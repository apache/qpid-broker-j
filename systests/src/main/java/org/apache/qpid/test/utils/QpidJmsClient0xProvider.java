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

import org.apache.qpid.client.AMQConnectionFactory;
import org.apache.qpid.client.AMQConnectionURL;
import org.apache.qpid.client.AMQDestination;
import org.apache.qpid.client.AMQQueue;
import org.apache.qpid.client.AMQSession;
import org.apache.qpid.client.AMQTopic;
import org.apache.qpid.exchange.ExchangeDefaults;
import org.apache.qpid.jms.ConnectionURL;

public class QpidJmsClient0xProvider implements JmsProvider
{
    private static final String DEFAULT_INITIAL_CONTEXT = "org.apache.qpid.jndi.PropertiesFileInitialContextFactory";
    static
    {
        String initialContext = System.getProperty(Context.INITIAL_CONTEXT_FACTORY);

        if (initialContext == null || initialContext.length() == 0)
        {
            System.setProperty(Context.INITIAL_CONTEXT_FACTORY, DEFAULT_INITIAL_CONTEXT);
        }
    }

    private final Hashtable<Object, Object> _initialContextEnvironment = new Hashtable<>();
    private final AmqpManagementFacade _managementFacade;

    public QpidJmsClient0xProvider(AmqpManagementFacade managementFacade)
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
        if (Boolean.getBoolean(QpidBrokerTestCase.PROFILE_USE_SSL))
        {
            return getConnectionFactory("default.ssl");
        }
        else
        {
            return getConnectionFactory("default");
        }
    }

    @Override
    public ConnectionFactory getConnectionFactory(final Map<String, String> options) throws NamingException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConnectionFactory getConnectionFactory(String factoryName)
            throws NamingException
    {
        return getConnectionFactory(factoryName, "test", "clientid");
    }

    @Override
    public ConnectionFactory getConnectionFactory(String factoryName, String vhost, String clientId)
            throws NamingException
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
    public Connection getClientConnection(String username, String password, String id)
            throws Exception
    {
        Connection con = ((AMQConnectionFactory) getConnectionFactory()).createConnection(username,
                                                                                          password,
                                                                                          id);
        return con;
    }

    @Override
    public Connection getConnectionWithPrefetch(int prefetch) throws Exception
    {
        return getConnectionWithOptions(Collections.singletonMap("maxprefetch", String.valueOf(prefetch)));
    }

    @Override
    public Connection getConnectionWithOptions(Map<String, String> options) throws Exception
    {
        return getConnectionWithOptions("test", options);
    }

    @Override
    public Connection getConnectionWithOptions(String vhost, Map<String, String> options) throws Exception
    {
        ConnectionURL curl =
                new AMQConnectionURL(((AMQConnectionFactory) getConnectionFactory()).getConnectionURLString());
        for (Map.Entry<String, String> entry : options.entrySet())
        {
            curl.setOption(entry.getKey(), entry.getValue());
        }

        curl = new AMQConnectionURL(curl.toString());
        curl.setUsername(QpidBrokerTestCase.GUEST_USERNAME);
        curl.setPassword(QpidBrokerTestCase.GUEST_PASSWORD);
        curl.setVirtualHost(vhost);
        Connection connection = new AMQConnectionFactory(curl).createConnection(curl.getUsername(), curl.getPassword());

        return connection;
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
        ConnectionURL curl =
                new AMQConnectionURL(((AMQConnectionFactory) getConnectionFactory()).getConnectionURLString());
        curl.setVirtualHost("/" + vhost);
        curl = new AMQConnectionURL(curl.toString());

        curl.setUsername(username);
        curl.setPassword(password);
        Connection connection =
                new AMQConnectionFactory(curl).createConnection(curl.getUsername(), curl.getPassword());

        return connection;
    }

    @Override
    public Connection getConnection(String urlString) throws Exception
    {
        ConnectionURL url = new AMQConnectionURL(urlString);
        Connection connection = new AMQConnectionFactory(url).createConnection(url.getUsername(), url.getPassword());
        return connection;
    }

    @Override
    public Connection getConnectionWithSyncPublishing() throws Exception
    {
        Map<String, String> options = new HashMap<>();
        options.put(ConnectionURL.OPTIONS_SYNC_PUBLISH, "all");
        return getConnectionWithOptions(options);
    }


    @Override
    public Queue getTestQueue(final String testQueueName)
    {
        return new AMQQueue(ExchangeDefaults.DIRECT_EXCHANGE_NAME, testQueueName);
    }

    @Override
    public Queue getQueueFromName(Session session, String name) throws JMSException
    {
        return session.createQueue("ADDR: '" + name + "'");
    }

    @Override
    public Queue createTestQueue(Session session, String queueName) throws JMSException
    {

        Queue amqQueue = getTestQueue(queueName);
        session.createConsumer(amqQueue).close();
        return amqQueue;
    }

    @Override
    public Topic getTestTopic(final String testQueueName)
    {
        return new AMQTopic(ExchangeDefaults.TOPIC_EXCHANGE_NAME, testQueueName);
    }

    @Override
    public Topic createTopic(final Connection con, final String topicName) throws JMSException
    {
        return getTestTopic(topicName);
    }

    @Override
    public Topic createTopicOnDirect(final Connection con, String topicName) throws JMSException, URISyntaxException
    {
        return new AMQTopic(
                "direct://amq.direct/"
                + topicName
                + "/"
                + topicName
                + "?routingkey='"
                + topicName
                + "',exclusive='true',autodelete='true'");
    }

    @Override
    public Topic createTopicOnFanout(final Connection con, String topicName) throws JMSException, URISyntaxException
    {
        return new AMQTopic(
                "fanout://amq.fanout/"
                + topicName
                + "/"
                + topicName
                + "?routingkey='"
                + topicName
                + "',exclusive='true',autodelete='true'");
    }

    @Override
    public long getQueueDepth(final Connection con, final Queue destination) throws Exception
    {
        Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        try
        {
            return ((AMQSession<?, ?>) session).getQueueDepth((AMQDestination) destination);
        }
        finally
        {
            session.close();
        }
    }

    public String getBrokerDetailsFromDefaultConnectionUrl()
    {
        try
        {
            AMQConnectionFactory factory = (AMQConnectionFactory) getConnectionFactory();
            ConnectionURL connectionURL = factory.getConnectionURL();
            if (connectionURL.getBrokerCount() > 0)
            {
                return connectionURL
                              .getBrokerDetails(0)
                              .toString();
            }
            else
            {
                throw new RuntimeException("No broker details are available.");
            }
        }
        catch (NamingException e)
        {
            throw new RuntimeException("No broker details are available.", e);
        }
    }
}