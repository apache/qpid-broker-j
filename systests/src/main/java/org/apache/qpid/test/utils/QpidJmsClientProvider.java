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

import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.Topic;
import javax.naming.InitialContext;
import javax.naming.NamingException;

public class QpidJmsClientProvider implements JmsProvider
{
    private static final String CLIENTID = "clientid";
    private final AmqpManagementFacade _managementFacade;
    private final Hashtable<Object, Object> _initialContextEnvironment = new Hashtable<>();

    public QpidJmsClientProvider(AmqpManagementFacade managementFacade)
    {
        _managementFacade = managementFacade;
    }

    @Override
    public InitialContext getInitialContext() throws NamingException
    {
        return new InitialContext(_initialContextEnvironment);
    }

    @Override
    public ConnectionFactory getConnectionFactory() throws NamingException
    {
        return getConnectionFactory(Collections.<String, String>emptyMap());
    }

    @Override
    public ConnectionFactory getConnectionFactory(String factoryName) throws NamingException
    {
        return getConnectionFactory(factoryName, Collections.<String, String>emptyMap());
    }

    @Override
    public ConnectionFactory getConnectionFactory(Map<String, String> options) throws NamingException
    {

        if (Boolean.getBoolean(QpidBrokerTestCase.PROFILE_USE_SSL))
        {
            return getConnectionFactory("default.ssl", options);
        }
        else
        {
            return getConnectionFactory("default", options);
        }
    }

    @Override
    public ConnectionFactory getConnectionFactory(String factoryName, String vhost, String clientId) throws NamingException
    {
        return getConnectionFactory(factoryName, vhost, clientId, Collections.<String, String>emptyMap());
    }

    @Override
    public ConnectionFactory getConnectionFactory(String factoryName,
                                                  String vhost,
                                                  String clientId,
                                                  Map<String, String> options)
            throws NamingException
    {

        Map<String, String> actualOptions = new LinkedHashMap<>();
        actualOptions.put("amqp.vhost", vhost);
        actualOptions.put("jms.clientID", clientId);
        actualOptions.putAll(options);
        return getConnectionFactory(factoryName, actualOptions);
    }

    private ConnectionFactory getConnectionFactory(final String factoryName, Map<String, String> options)
            throws NamingException
    {

        if (!options.containsKey("amqp.vhost"))
        {
            options = new HashMap<>(options);
            options.put("amqp.vhost", "test");
        }
        if (!options.containsKey("amqp.clientID"))
        {
            options = new HashMap<>(options);
            options.put("jms.clientID", CLIENTID);
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


        if ("failover".equals(factoryName))
        {
            if (!options.containsKey("failover.maxReconnectAttempts"))
            {
                options.put("failover.maxReconnectAttempts", "2");
            }
            final StringBuilder stem = new StringBuilder("failover:(amqp://localhost:")
                    .append(System.getProperty("test.port"))
                    .append(",amqp://localhost:")
                    .append(System.getProperty("test.port.alt"))
                    .append(")");
            appendOptions(options, stem);

            _initialContextEnvironment.put("property.connectionfactory.failover.remoteURI",
                                           stem.toString());
        }
        else if ("default".equals(factoryName))
        {
            final StringBuilder stem =
                    new StringBuilder("amqp://localhost:").append(System.getProperty("test.port"));

            appendOptions(options, stem);

            _initialContextEnvironment.put("property.connectionfactory.default.remoteURI", stem.toString());
        }
        else if ("default.ssl".equals(factoryName))
        {

            final StringBuilder stem = new StringBuilder("amqps://localhost:").append(String.valueOf(System.getProperty("test.port.ssl")));
            appendOptions(options, stem);
            _initialContextEnvironment.put("connectionfactory.default.ssl", stem.toString());
        }
        return (ConnectionFactory) getInitialContext().lookup(factoryName);
    }

    @Override
    public Connection getConnection() throws JMSException, NamingException
    {
        return getConnection(QpidBrokerTestCase.GUEST_USERNAME, QpidBrokerTestCase.GUEST_PASSWORD);
    }

    @Override
    public Connection getConnection(String username, String password) throws JMSException, NamingException
    {
        Connection con = getConnectionFactory().createConnection(username, password);
        return con;
    }

    @Override
    public Connection getConnectionWithPrefetch(int prefetch) throws Exception
    {
        String factoryName = Boolean.getBoolean(QpidBrokerTestCase.PROFILE_USE_SSL) ? "default.ssl" : "default";

        final Map<String, String> options =
                Collections.singletonMap("jms.prefetchPolicy.all", String.valueOf(prefetch));
        final ConnectionFactory connectionFactory = getConnectionFactory(factoryName, "test", "clientid", options);
        return connectionFactory.createConnection(QpidBrokerTestCase.GUEST_USERNAME,
                                                  QpidBrokerTestCase.GUEST_PASSWORD);
    }

    @Override
    public Connection getConnectionWithOptions(Map<String, String> options) throws Exception
    {
        return getConnectionWithOptions("test", options);
    }

    @Override
    public Connection getConnectionWithOptions(String vhost, Map<String, String> options) throws Exception
    {
        return getConnectionFactory(Boolean.getBoolean(QpidBrokerTestCase.PROFILE_USE_SSL)
                                            ? "default.ssl"
                                            : "default",
                                    vhost,
                                    "clientId",
                                    options).createConnection(QpidBrokerTestCase.GUEST_USERNAME,
                                                              QpidBrokerTestCase.GUEST_PASSWORD);
    }

    @Override
    public Connection getConnectionForVHost(String vhost)
            throws Exception
    {
        return getConnectionForVHost(vhost, QpidBrokerTestCase.GUEST_USERNAME, QpidBrokerTestCase.GUEST_PASSWORD);
    }

    @Override
    public Connection getConnectionForVHost(String vhost, String username, String password)
            throws Exception
    {
        return getConnectionFactory(Boolean.getBoolean(QpidBrokerTestCase.PROFILE_USE_SSL)
                                            ? "default.ssl"
                                            : "default", vhost, "clientId").createConnection(username, password);
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
    public long getQueueDepth(final Connection con, final Queue destination) throws Exception
    {
        Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        try
        {

            MessageProducer producer = session.createProducer(session.createQueue("$management"));
            final TemporaryQueue responseQ = session.createTemporaryQueue();
            MessageConsumer consumer = session.createConsumer(responseQ);
            MapMessage message = session.createMapMessage();
            message.setStringProperty("index", "object-path");
            final String escapedName = destination.getQueueName().replaceAll("([/\\\\])", "\\\\$1");
            message.setStringProperty("key", escapedName);
            message.setStringProperty("type", "org.apache.qpid.Queue");
            message.setStringProperty("operation", "getStatistics");
            message.setStringProperty("statistics", "[\"queueDepthMessages\"]");

            message.setJMSReplyTo(responseQ);

            producer.send(message);

            Message response = consumer.receive();
            try
            {
                if (response instanceof MapMessage)
                {
                    return ((MapMessage) response).getLong("queueDepthMessages");
                }
                else if (response instanceof ObjectMessage)
                {
                    Object body = ((ObjectMessage) response).getObject();
                    if (body instanceof Map)
                    {
                        return Long.valueOf(((Map) body).get("queueDepthMessages").toString());
                    }
                }
                throw new IllegalArgumentException("Cannot parse the results from a management operation");
            }
            finally
            {
                consumer.close();
                responseQ.delete();
            }
        }
        finally
        {
            session.close();
        }
    }

    @Override
    public boolean isQueueExist(final Connection con, final Queue destination) throws Exception
    {
        Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        try
        {
            MessageProducer producer = session.createProducer(session.createQueue("$management"));
            final TemporaryQueue responseQ = session.createTemporaryQueue();
            MessageConsumer consumer = session.createConsumer(responseQ);
            MapMessage message = session.createMapMessage();
            message.setStringProperty("index", "object-path");
            final String escapedName = destination.getQueueName().replaceAll("([/\\\\])", "\\\\$1");
            message.setStringProperty("key", escapedName);
            message.setStringProperty("type", "org.apache.qpid.Queue");
            message.setStringProperty("operation", "READ");

            message.setJMSReplyTo(responseQ);

            producer.send(message);

            Message response = consumer.receive();
            try
            {
                int statusCode = response.getIntProperty("statusCode");
                switch(statusCode)
                {
                    case 200:
                        return true;
                    case 404:
                        return false;
                    default:
                        throw new RuntimeException(String.format("Unexpected response for queue query '%s' :  %d", destination.getQueueName(), statusCode));
                }
            }
            finally
            {
                consumer.close();
                responseQ.delete();
            }
        }
        finally
        {
            session.close();
        }
    }

    @Override
    public Connection getConnectionWithSyncPublishing() throws Exception
    {
        return getConnection();
    }

    @Override
    public Connection getClientConnection(String username, String password, String id)
            throws Exception
    {
        return getConnectionFactory("default", "test", id).createConnection(username, password);
    }

    @Override
    public String getBrokerDetailsFromDefaultConnectionUrl()
    {
        throw new UnsupportedOperationException();
    }

    private void appendOptions(final Map<String, String> actualOptions, final StringBuilder stem)
    {
        boolean first = true;
        for(Map.Entry<String, String> option : actualOptions.entrySet())
        {
            if(first)
            {
                stem.append('?');
                first = false;
            }
            else
            {
                stem.append('&');
            }
            try
            {
                stem.append(option.getKey()).append('=').append(URLEncoder.encode(option.getValue(), "UTF-8"));
            }
            catch (UnsupportedEncodingException e)
            {
                throw new RuntimeException(e);
            }
        }
    }
}