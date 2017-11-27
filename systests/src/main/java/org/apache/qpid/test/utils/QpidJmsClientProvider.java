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

import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;
import javax.naming.NamingException;

public class QpidJmsClientProvider implements JmsProvider
{
    private static final AtomicInteger CLIENTID_COUNTER = new AtomicInteger();
    private final AmqpManagementFacade _managementFacade;

    public QpidJmsClientProvider(AmqpManagementFacade managementFacade)
    {
        _managementFacade = managementFacade;
    }

    @Override
    public ConnectionFactory getConnectionFactory() throws NamingException
    {
        return getConnectionFactory(Collections.emptyMap());
    }

    @Override
    public ConnectionFactory getConnectionFactory(Map<String, String> options) throws NamingException
    {
        boolean useSsl = Boolean.getBoolean(QpidBrokerTestCase.PROFILE_USE_SSL);
        if (!options.containsKey("amqp.vhost"))
        {
            options = new HashMap<>(options);
            options.put("amqp.vhost", "test");
        }
        if (!options.containsKey("jms.clientID"))
        {
            options = new HashMap<>(options);
            options.put("jms.clientID", getNextClientId());
        }
        else if (options.get("jms.clientID") == null)
        {
            options.remove("jms.clientID");
        }
        if (!options.containsKey("amqp.forceSyncSend"))
        {
            options = new HashMap<>(options);
            options.put("jms.forceSyncSend", "true");
        }
        if (!options.containsKey("amqp.populateJMSXUserID"))
        {
            options = new HashMap<>(options);
            options.put("jms.populateJMSXUserID", "true");
        }

        return getConnectionBuilder().setTls(useSsl).setOptions(options).buildConnectionFactory();
    }

    private Connection getConnection() throws JMSException, NamingException
    {
        return getConnection(QpidBrokerTestCase.GUEST_USERNAME, QpidBrokerTestCase.GUEST_PASSWORD);
    }


    private Connection getConnection(String username, String password) throws JMSException, NamingException
    {
        return getConnectionFactory().createConnection(username, password);
    }

    @Override
    public Connection getConnection(String urlString) throws Exception
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Queue getTestQueue(final String testQueueName)
    {
        Connection con = null;
        try
        {
            con = getConnection();
            Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
            return session.createQueue(testQueueName);
        }
        catch (JMSException | NamingException e)
        {
            throw new RuntimeException("Failed to create a test queue name : " + testQueueName, e);
        }
        finally
        {
            if (con != null)
            {
                try
                {
                    con.close();
                }
                catch (JMSException e)
                {
                }
            }
        }
    }

    @Override
    public Queue getQueueFromName(Session session, String name) throws JMSException
    {
        return session.createQueue(name);
    }

    @Override
    public Queue createTestQueue(Session session, String queueName) throws JMSException
    {
        _managementFacade.createEntityUsingAmqpManagement(queueName, session, "org.apache.qpid.Queue");

        return session.createQueue(queueName);
    }

    @Override
    public Topic getTestTopic(final String testTopicName)
    {
        Connection con = null;
        try
        {
            con = getConnection();
            Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
            return session.createTopic(testTopicName);
        }
        catch (JMSException | NamingException e)
        {
            throw new RuntimeException("Failed to create a test topic name : " + testTopicName, e);
        }
        finally
        {
            if (con != null)
            {
                try
                {
                    con.close();
                }
                catch (JMSException e)
                {
                }
            }
        }
    }

    @Override
    public Topic createTopic(final Connection con, final String topicName) throws JMSException
    {
        Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        _managementFacade.createEntityUsingAmqpManagement(topicName, session, "org.apache.qpid.TopicExchange");

        return session.createTopic(topicName);
    }

    @Override
    public Topic createTopicOnDirect(final Connection con, String topicName) throws JMSException, URISyntaxException
    {
        Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        return session.createTopic("amq.direct/" + topicName);
    }

    @Override
    public Topic createTopicOnFanout(final Connection con, String topicName) throws JMSException, URISyntaxException
    {
        Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        return session.createTopic("amq.fanout/" + topicName);
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
        }
        finally
        {
            connection.close();
        }

    }

    @Override
    public String getBrokerDetailsFromDefaultConnectionUrl()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConnectionBuilder getConnectionBuilder()
    {
        return new QpidJmsClientConnectionBuilder();
    }

    private String getNextClientId()
    {
        return "clientid-" + CLIENTID_COUNTER.getAndIncrement();
    }
}
