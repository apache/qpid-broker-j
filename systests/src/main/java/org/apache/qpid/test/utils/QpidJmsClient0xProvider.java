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
 *
 */

package org.apache.qpid.test.utils;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.security.AccessControlException;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;


public class QpidJmsClient0xProvider implements JmsProvider
{
    private final AmqpManagementFacade _managementFacade;

    public QpidJmsClient0xProvider(AmqpManagementFacade managementFacade)
    {
        _managementFacade = managementFacade;
    }

    @Override
    public ConnectionFactory getConnectionFactory() throws NamingException
    {
        return getConnectionBuilder().setTls(Boolean.getBoolean(QpidBrokerTestCase.PROFILE_USE_SSL))
                                     .buildConnectionFactory();
    }

    @Override
    public ConnectionFactory getConnectionFactory(final Map<String, String> options) throws NamingException
    {
        throw new UnsupportedOperationException();
    }

    private Connection getConnection() throws JMSException, NamingException
    {
        return getConnection(QpidBrokerTestCase.GUEST_USERNAME, QpidBrokerTestCase.GUEST_PASSWORD);
    }

    private Connection getConnection(String username, String password) throws JMSException, NamingException
    {
        return getConnectionBuilder().setUsername(username).setPassword(password).build();
    }

    @Override
    public Connection getConnection(String urlString) throws Exception
    {
        final Hashtable<Object, Object> initialContextEnvironment = new Hashtable<>();
        initialContextEnvironment.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.qpid.jndi.PropertiesFileInitialContextFactory");
        final String factoryName = "connectionFactory";
        initialContextEnvironment.put("connectionfactory." + factoryName, urlString);
        ConnectionFactory connectionFactory =
                (ConnectionFactory) new InitialContext(initialContextEnvironment).lookup(factoryName);
        return connectionFactory.createConnection();
    }


    @Override
    public Queue getTestQueue(final String testQueueName) throws NamingException
    {
        return createReflectively("org.apache.qpid.client.AMQQueue", "amq.direct", testQueueName);
    }

    @Override
    public Queue getQueueFromName(Session session, String name) throws JMSException
    {
        return createReflectively("org.apache.qpid.client.AMQQueue", "", name);
    }

    @Override
    public Queue createTestQueue(Session session, String queueName) throws JMSException
    {

        Queue amqQueue = null;
        try
        {
            amqQueue = getTestQueue(queueName);
        }
        catch (NamingException e)
        {
            throw new RuntimeException(e);
        }
        session.createConsumer(amqQueue).close();
        return amqQueue;
    }

    @Override
    public Topic getTestTopic(final String testQueueName)
    {
        return createReflectively("org.apache.qpid.client.AMQTopic", "amq.topic", testQueueName);
    }

    @Override
    public Topic createTopic(final Connection con, final String topicName) throws JMSException
    {
        return getTestTopic(topicName);
    }

    @Override
    public Topic createTopicOnDirect(final Connection con, String topicName) throws JMSException, URISyntaxException
    {
        return createReflectively("org.apache.qpid.client.AMQTopic",
                                  "direct://amq.direct/"
                                  + topicName
                                  + "/"
                                  + topicName
                                  + "?routingkey='"
                                  + topicName
                                  + "',exclusive='true',autodelete='true'");
    }

    private <T> T createReflectively(String className, Object ...args)
    {
        try
        {
            Class<?> topicClass = Class.forName(className);
            Class[] classes = new Class[args.length];
            for (int i = 0; i < args.length; ++i)
            {
                classes[i] = args[i].getClass();
            }
            Constructor<?> constructor = topicClass.getConstructor(classes);
            return (T) constructor.newInstance(args);
        }
        catch (IllegalAccessException | AccessControlException | InvocationTargetException | InstantiationException | NoSuchMethodException | ClassNotFoundException e)
        {
            throw new RuntimeException(e);
        }

    }

    @Override
    public Topic createTopicOnFanout(final Connection con, String topicName) throws JMSException, URISyntaxException
    {
        return createReflectively("org.apache.qpid.client.AMQTopic", "fanout://amq.fanout/"
                                                                     + topicName
                                                                     + "/"
                                                                     + topicName
                                                                     + "?routingkey='"
                                                                     + topicName
                                                                     + "',exclusive='true',autodelete='true'");
    }

    @Override
    public long getQueueDepth(final Queue destination) throws Exception
    {
        final String escapedName = destination.getQueueName().replaceAll("([/\\\\])", "\\\\$1");
        Connection connection = getConnection();
        try
        {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            try
            {
                Map<String, Object> arguments = Collections.singletonMap("statistics",
                                                                         Collections.singletonList("queueDepthMessages"));
                Object statistics = _managementFacade.performOperationUsingAmqpManagement(escapedName,
                                                                                             "getStatistics",
                                                                                             session,
                                                                                             "org.apache.qpid.Queue",
                                                                                             arguments);

                Map<String, Object> statisticsMap = (Map<String, Object>) statistics;
                return ((Number) statisticsMap.get("queueDepthMessages")).intValue();
            }
            finally
            {
                session.close();
            }
        }
        finally
        {
            connection.close();
        }
    }

    @Override
    public boolean isQueueExist(final Queue destination) throws Exception
    {
        final String escapedName = destination.getQueueName().replaceAll("([/\\\\])", "\\\\$1");
        Connection connection = getConnection();
        try
        {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            try
            {
                _managementFacade.performOperationUsingAmqpManagement(escapedName,
                                                                      "READ",
                                                                      session,
                                                                      "org.apache.qpid.Queue",
                                                                      Collections.emptyMap());
                return true;
            }
            catch (AmqpManagementFacade.OperationUnsuccessfulException e)
            {
                if (e.getStatusCode() == 404)
                {
                    return false;
                }
                else
                {
                    throw e;
                }
            }
            finally
            {
                session.close();
            }
        }
        finally
        {
            connection.close();
        }
    }

    @Override
    public String getBrokerDetailsFromDefaultConnectionUrl()
    {
        return getConnectionBuilder().getBrokerDetails();
    }

    @Override
    public QpidJmsClient0xConnectionBuilder getConnectionBuilder()
    {
        return new QpidJmsClient0xConnectionBuilder();
    }
}
