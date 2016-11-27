/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.qpid.disttest.jms;

import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.apache.qpid.jms.Session.NO_ACKNOWLEDGE;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.apache.qpid.client.AMQDestination;
import org.apache.qpid.client.AMQSession;
import org.apache.qpid.disttest.DistributedTestException;
import org.apache.qpid.disttest.controller.config.QueueConfig;
import org.apache.qpid.framing.AMQShortString;
import org.apache.qpid.framing.FieldTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Qpid 0-8..0-10 specific class for creating/deleting queues using a non-JMS API.
 * Uses reflection to invoke methods to retain compatibility with Qpid versions {@literal <= 0.32}.
 */
public class QpidQueueCreator implements QueueCreator
{
    private static final Logger LOGGER = LoggerFactory.getLogger(QpidQueueCreator.class);

    private static final String CREATE_QUEUE = "createQueue";
    private static final String BIND_QUEUE = "bindQueue";
    // The Qpid AMQSession API currently makes the #deleteQueue method protected and the
    // raw protocol method public.  This should be changed then we should switch the below to
    // use deleteQueue.
    private static final String DELETE_QUEUE = "sendQueueDelete";

    private static final String GET_AMQQUEUE_NAME = "getAMQQueueName";
    private static final String GET_ROUTING_KEY = "getRoutingKey";
    private static final String GET_EXCHANGE_NAME = "getExchangeName";

    private static final Map<String,Object> EMPTY_QUEUE_BIND_ARGUMENTS = Collections.emptyMap();
    private static final int _drainPollTimeout = Integer.getInteger(QUEUE_CREATOR_DRAIN_POLL_TIMEOUT, 500);
    private static final boolean _drainQueueBeforeDelete = Boolean.parseBoolean(System.getProperty(
            QUEUE_CREATOR_DRAIN_QUEUE_BEFORE_DELETE, "true"));
    private static final int _threadPoolSize = Integer.getInteger(QUEUE_CREATOR_THREAD_POOL_SIZE, 4);

    // Methods on AMQSession
    private final Method _createQueue;
    private final Method _bindQueue;
    private final Method _deleteQueue;

    // Methods on AMQDestination
    private final Method _getAMQQueueName;
    private final Method _getRoutingKey;
    private final Method _getExchangeName;

    private final AtomicInteger _threadCount = new AtomicInteger();

    public QpidQueueCreator()
    {
        Method createQueue = getMethod(AMQSession.class, CREATE_QUEUE,
                                       String.class, Boolean.TYPE, Boolean.TYPE,
                                       Boolean.TYPE, Map.class);
        if (createQueue == null)
        {
            createQueue = getMethod(AMQSession.class, CREATE_QUEUE,
                                    AMQShortString.class, Boolean.TYPE, Boolean.TYPE,
                                    Boolean.TYPE, Map.class);
            if (createQueue == null)
            {
                throw new DistributedTestException("Failed to find method '" + CREATE_QUEUE + "' on class AMQSession");
            }
        }
        _createQueue = createQueue;

        Method bindQueue = getMethod(AMQSession.class, BIND_QUEUE,
                                       String.class, String.class, Map.class,
                                       String.class, AMQDestination.class);
        if (bindQueue == null)
        {
            bindQueue = getMethod(AMQSession.class, BIND_QUEUE,
                                  AMQShortString.class, AMQShortString.class, FieldTable.class,
                                  AMQShortString.class, AMQDestination.class);
            if (bindQueue == null)
            {
                throw new DistributedTestException("Failed to find method '" + BIND_QUEUE + "' on class AMQSession");
            }
        }
        _bindQueue = bindQueue;

        Method deleteQueue = getMethod(AMQSession.class, DELETE_QUEUE, String.class);
        if (deleteQueue == null)
        {
            deleteQueue = getMethod(AMQSession.class, DELETE_QUEUE, AMQShortString.class);
            if (deleteQueue == null)
            {
                throw new DistributedTestException("Failed to find method '" + DELETE_QUEUE + "' on class AMQSession");
            }
        }
        _deleteQueue = deleteQueue;

        _getAMQQueueName = getMethod(AMQDestination.class, GET_AMQQUEUE_NAME);
        if (_getAMQQueueName == null)
        {
            throw new DistributedTestException("Failed to find method '" + GET_AMQQUEUE_NAME + "' on class AMQDestination");
        }

        _getRoutingKey = getMethod(AMQDestination.class, GET_ROUTING_KEY);
        if (_getRoutingKey == null)
        {
            throw new DistributedTestException("Failed to find method '" + GET_ROUTING_KEY + "' on class AMQDestination");
        }

        _getExchangeName = getMethod(AMQDestination.class, GET_EXCHANGE_NAME);
        if (_getExchangeName == null)
        {
            throw new DistributedTestException("Failed to find method '" + GET_EXCHANGE_NAME + "' on class AMQDestination");
        }
    }

    @Override
    public void createQueues(final Connection connection, Session session, final List<QueueConfig> configs)
    {
        final Map<Thread, AMQSession<?,?>> sessionMap = new ConcurrentHashMap<>();
        ExecutorService executorService = newFixedThreadPool(_threadPoolSize, new JmsJobThreadFactory(connection, sessionMap));

        try
        {
            List<Future<Void>> createQueueFutures = new ArrayList<>(configs.size());

            for (final QueueConfig queueConfig : configs)
            {
                Future<Void> f = executorService.submit(new Callable<Void>()
                {
                    @Override
                    public Void call() throws Exception
                    {
                        AMQSession<?, ?> session = sessionMap.get(Thread.currentThread());
                        createQueue(session, queueConfig);
                        return null;
                    }
                });

                createQueueFutures.add(f);
            }

            for(Future<Void> queueCreateFuture : createQueueFutures)
            {
                queueCreateFuture.get();
            }

            closeAllSessions(sessionMap);
        }
        catch (InterruptedException e)
        {
            throw new DistributedTestException(e);
        }
        catch (ExecutionException e)
        {
            throw new DistributedTestException(e.getCause());
        }
        finally
        {
            executorService.shutdown();
        }
    }

    @Override
    public void deleteQueues(final Connection connection, Session session, List<QueueConfig> configs)
    {
        final Map<Thread, AMQSession<?,?>> sessionMap = new ConcurrentHashMap<>();
        ExecutorService executorService = newFixedThreadPool(_threadPoolSize,
                                                             new JmsJobThreadFactory(connection, sessionMap));

        try
        {
            List<Future<Void>> deleteQueueFutures = new ArrayList<>(configs.size());

            for (final QueueConfig queueConfig : configs)
            {

                Future<Void> f = executorService.submit(new Callable<Void>()
                {
                    @Override
                    public Void call() throws Exception
                    {
                        AMQSession<?, ?> session = sessionMap.get(Thread.currentThread());
                        AMQDestination destination = createAMQDestination(session, queueConfig);

                        // drainQueue method is added because deletion of queue with a lot
                        // of messages takes time and might cause the timeout exception
                        if (_drainQueueBeforeDelete)
                        {
                            drainQueue(session, destination);
                        }

                        deleteQueue(session, destination);
                        return null;
                    }
                });

                deleteQueueFutures.add(f);
            }

            for(Future<Void> queueCreateFuture : deleteQueueFutures)
            {
                queueCreateFuture.get();
            }

            closeAllSessions(sessionMap);
        }
        catch (InterruptedException e)
        {
            throw new DistributedTestException(e);
        }
        catch (ExecutionException e)
        {
            throw new DistributedTestException(e.getCause());
        }
        finally
        {
            executorService.shutdown();
        }
    }

    @Override
    public String getProtocolVersion(final Connection connection)
    {
        if (connection != null)
        {
            try
            {
                final Method method = connection.getClass().getMethod("getProtocolVersion"); // Qpid 0-8..0-10 method only
                Object version =  method.invoke(connection);
                return String.valueOf(version);
            }
            catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e)
            {
                return null;
            }
        }
        return null;
    }

    /**
     * We currently rely on the fact that the client version numbers follow the broker's.
     * This should be changed to use AMQP Management.
     *
     * @param connection
     * @return
     */
    @Override
    public String getProviderVersion(final Connection connection)
    {
        // Unfortunately, Qpid 0-8..0-10 does not define ConnectionMetaData#getProviderVersion in a useful way
        String qpidRelease = getQpidReleaseVersionByReflection("org.apache.qpid.configuration.CommonProperties");
        if (qpidRelease == null)
        {
            qpidRelease = getQpidReleaseVersionByReflection("org.apache.qpid.common.QpidProperties");  // < 0.32
        }
        return qpidRelease;
    }

    private String getQpidReleaseVersionByReflection(final String className)
    {
        try
        {
            Class clazz = Class.forName(className);
            Method method = clazz.getMethod("getReleaseVersion");
            Object version =  method.invoke(null);
            return String.valueOf(version);
        }
        catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException | IllegalAccessException e)
        {
            return null;
        }
    }

    private void closeAllSessions(final Map<Thread, AMQSession<?, ?>> sessionMap)
    {
        for(Session s : sessionMap.values())
        {
            try
            {
                s.close();
            }
            catch (JMSException e)
            {
                // Ignore
            }
        }
    }

    private AMQDestination createAMQDestination(AMQSession<?, ?> amqSession, QueueConfig queueConfig)
    {
        try
        {
            return (AMQDestination) amqSession.createQueue(queueConfig.getName());
        }
        catch (Exception e)
        {
            throw new DistributedTestException("Failed to create amq destination object:" + queueConfig, e);
        }
    }

    private long getQueueDepth(AMQSession<?, ?> amqSession, AMQDestination destination)
    {
        try
        {
            long queueDepth = amqSession.getQueueDepth(destination);
            return queueDepth;
        }
        catch (Exception e)
        {
            throw new DistributedTestException("Failed to query queue depth:" + destination, e);
        }
    }

    private void drainQueue(Session noAckSession, AMQDestination destination)
    {
        MessageConsumer messageConsumer = null;
        try
        {
            LOGGER.debug("About to drain the queue {}", destination.getQueueName());
            messageConsumer = noAckSession.createConsumer(destination);

            long currentQueueDepth = getQueueDepth((AMQSession<?,?>) noAckSession, destination);
            int counter = 0;
            while (currentQueueDepth > 0)
            {
                LOGGER.debug("Queue {} has {} message(s)", destination.getQueueName(), currentQueueDepth);

                while(messageConsumer.receive(_drainPollTimeout) != null)
                {
                    counter++;
                }

                currentQueueDepth = getQueueDepth((AMQSession<?,?>) noAckSession, destination);
            }
            LOGGER.info("Drained {} message(s) from queue {} ", counter, destination.getQueueName());
        }
        catch (Exception e)
        {
            throw new DistributedTestException("Failed to drain queue:" + destination, e);
        }
        finally
        {
            if (messageConsumer != null)
            {
                try
                {
                    messageConsumer.close();
                }
                catch (JMSException e)
                {
                }
            }
        }
    }

    private void createQueue(AMQSession<?, ?> session, QueueConfig queueConfig)
    {

        try
        {
            AMQDestination destination = (AMQDestination) session.createQueue(queueConfig.getName());
            boolean autoDelete = false;
            boolean exclusive = false;
            createQueueByReflection(session, destination, autoDelete,
                                    queueConfig.isDurable(), exclusive,
                                    queueConfig.getAttributes());
            bindQueueByReflections(session, destination);

            LOGGER.debug("Created queue {}", queueConfig);
        }
        catch (Exception e)
        {
            throw new DistributedTestException("Failed to create queue:" + queueConfig, e);
        }
    }

    private void deleteQueue(AMQSession<?, ?> session, AMQDestination destination)
    {
        try
        {
            Object amqQueueName = invokeByReflection(destination, _getAMQQueueName);
            invokeByReflection(session, _deleteQueue, amqQueueName);

            LOGGER.debug("Deleted queue {}", destination);
        }
        catch (Exception e)
        {
            throw new DistributedTestException("Failed to delete queue:" + destination, e);
        }
    }

    private void createQueueByReflection(AMQSession<?, ?> session,
                                         AMQDestination destination,
                                         boolean autoDelete,
                                         boolean durable, final boolean exclusive,
                                         Map<String, Object> attributes)
    {
        Object amqQueueName = invokeByReflection(destination, _getAMQQueueName);
        invokeByReflection(session, _createQueue, amqQueueName, autoDelete, durable, exclusive, attributes);
    }

    private void bindQueueByReflections(AMQSession<?, ?> session,
                                        AMQDestination destination)
    {
        Object amqQueueName = invokeByReflection(destination, _getAMQQueueName);
        Object routingKey = invokeByReflection(destination, _getRoutingKey);
        Object bindArguments = _bindQueue.getParameterTypes()[2].equals(Map.class) ?
                EMPTY_QUEUE_BIND_ARGUMENTS : FieldTable.convertToFieldTable(EMPTY_QUEUE_BIND_ARGUMENTS);
        Object exchangeName = invokeByReflection(destination, _getExchangeName);

        // Don't try and bind the queue to the default exchange
        if (exchangeName != null &&
            !"".equals(exchangeName) &&
            !AMQShortString.EMPTY_STRING.equals(exchangeName))
        {
            invokeByReflection(session, _bindQueue, amqQueueName, routingKey, bindArguments, exchangeName, destination);
        }
    }

    private Object invokeByReflection(Object target, Method method, Object... parameters)
    {
        try
        {
            return method.invoke(target, parameters);
        }
        catch (IllegalAccessException | InvocationTargetException e)
        {
            throw new DistributedTestException("Failed to invoke '" + method + "'", e);
        }
    }

    private Method getMethod(final Class<?> targetClass, final String methodName, final Class... parametersTypes)
    {
        try
        {
            return targetClass.getMethod(methodName, parametersTypes);
        }
        catch (NoSuchMethodException e)
        {
            return null;
        }
    }

    private static class JmsJobThreadFactory implements ThreadFactory
    {
        private final Connection _connection;
        private final Map<Thread, AMQSession<?, ?>> _sessionMap;

        public JmsJobThreadFactory(final Connection connection, final Map<Thread, AMQSession<?, ?>> sessionMap)
        {
            _connection = connection;
            _sessionMap = sessionMap;
        }

        @Override
        public Thread newThread(final Runnable runnable)
        {
            try
            {
                AMQSession<?, ?> session = (AMQSession<?, ?>) _connection.createSession(false, NO_ACKNOWLEDGE);
                Thread jobThread = new Thread(runnable);
                _sessionMap.put(jobThread, session);
                return jobThread;
            }
            catch (JMSException e)
            {
                throw new DistributedTestException("Failed to create session for queue creation", e);
            }
        }
    }
}
