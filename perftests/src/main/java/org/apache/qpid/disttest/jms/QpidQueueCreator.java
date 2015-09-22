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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
 * Uses reflection to invoke methods to retain compatibility with Qpid versions <= 0.32.
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
    private static int _drainPollTimeout = Integer.getInteger(QUEUE_CREATOR_DRAIN_POLL_TIMEOUT, 500);

    // Methods on AMQSession
    private final Method _createQueue;
    private final Method _bindQueue;
    private final Method _deleteQueue;

    // Methods on AMQDestination
    private final Method _getAMQQueueName;
    private final Method _getRoutingKey;
    private final Method _getExchangeName;

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
    public void createQueues(Connection connection, Session session, List<QueueConfig> configs)
    {
        AMQSession<?, ?> amqSession = (AMQSession<?, ?>)session;
        for (QueueConfig queueConfig : configs)
        {
            createQueue(amqSession, queueConfig);
        }
    }

    @Override
    public void deleteQueues(Connection connection, Session session, List<QueueConfig> configs)
    {
        AMQSession<?, ?> amqSession = (AMQSession<?, ?>)session;
        for (QueueConfig queueConfig : configs)
        {
            AMQDestination destination = createAMQDestination(amqSession, queueConfig);

            // drainQueue method is added because deletion of queue with a lot
            // of messages takes time and might cause the timeout exception
            drainQueue(connection, destination);

            deleteQueue(amqSession, destination);
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

    private void drainQueue(Connection connection, AMQDestination destination)
    {
        Session noAckSession = null;
        try
        {
            LOGGER.debug("About to drain the queue {}", destination.getQueueName());
            noAckSession = connection.createSession(false, org.apache.qpid.jms.Session.NO_ACKNOWLEDGE);
            MessageConsumer messageConsumer = noAckSession.createConsumer(destination);

            long currentQueueDepth = getQueueDepth((AMQSession<?,?>)noAckSession, destination);
            int counter = 0;
            while (currentQueueDepth > 0)
            {
                LOGGER.debug("Queue {} has {} message(s)", destination.getQueueName(), currentQueueDepth);

                while(messageConsumer.receive(_drainPollTimeout) != null)
                {
                    counter++;
                }

                currentQueueDepth = getQueueDepth((AMQSession<?,?>)noAckSession, destination);
            }
            LOGGER.info("Drained {} message(s) from queue {} ", counter, destination.getQueueName());
            messageConsumer.close();
        }
        catch (Exception e)
        {
            throw new DistributedTestException("Failed to drain queue:" + destination, e);
        }
        finally
        {
            if (noAckSession != null)
            {
                try
                {
                    noAckSession.close();
                }
                catch (JMSException e)
                {
                    throw new DistributedTestException("Failed to close n/a session:" + noAckSession, e);
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

        invokeByReflection(session, _bindQueue, amqQueueName, routingKey, bindArguments, exchangeName, destination);
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

}
