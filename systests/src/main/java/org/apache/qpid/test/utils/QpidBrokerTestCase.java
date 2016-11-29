/* Licensed to the Apache Software Foundation (ASF) under one
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
package org.apache.qpid.test.utils;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import ch.qos.logback.classic.sift.SiftingAppender;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.FileAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import org.apache.qpid.QpidException;
import org.apache.qpid.client.AMQConnectionFactory;
import org.apache.qpid.client.AMQConnectionURL;
import org.apache.qpid.client.AMQDestination;
import org.apache.qpid.client.AMQQueue;
import org.apache.qpid.client.AMQSession;
import org.apache.qpid.client.AMQTopic;
import org.apache.qpid.client.BrokerDetails;
import org.apache.qpid.exchange.ExchangeDefaults;
import org.apache.qpid.jms.ConnectionURL;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.store.MemoryConfigurationStore;
import org.apache.qpid.url.URLSyntaxException;

/**
 * Qpid base class for system testing test cases.
 */
public class QpidBrokerTestCase extends QpidTestCase
{
    public static final int LOGBACK_REMOTE_PORT = LogbackSocketPortNumberDefiner.getLogbackSocketPortNumber();
    public static final String GUEST_USERNAME = "guest";
    public static final String GUEST_PASSWORD = "guest";
    public static final String PROFILE_USE_SSL = "profile.use_ssl";
    public static final String TEST_AMQP_PORT_PROTOCOLS_PROPERTY = "test.amqp_port_protocols";
    public static final int DEFAULT_PORT = Integer.getInteger("test.port", 0);
    public static final int FAILING_PORT = Integer.getInteger("test.port.alt", 0);
    public static final int DEFAULT_SSL_PORT = Integer.getInteger("test.port.ssl", 0);
    public static final String QUEUE = "queue";
    public static final String TOPIC = "topic";
    public static final String MANAGEMENT_MODE_PASSWORD = "mm_password";
    protected static final Logger _logger = LoggerFactory.getLogger(QpidBrokerTestCase.class);
    protected static final long RECEIVE_TIMEOUT = Long.getLong("qpid.test_receive_timeout", 1000L);
    protected static final String INDEX = "index";
    protected static final String CONTENT = "content";
    protected static final int DEFAULT_MESSAGE_SIZE = 1024;
    private static final String DEFAULT_INITIAL_CONTEXT = "org.apache.qpid.jndi.PropertiesFileInitialContextFactory";
    private static final String JAVA = "java";
    private static final String BROKER_LANGUAGE = System.getProperty("broker.language", JAVA);
    private static final BrokerHolder.BrokerType DEFAULT_BROKER_TYPE = BrokerHolder.BrokerType.valueOf(
            System.getProperty("broker.type", BrokerHolder.BrokerType.INTERNAL.name()).toUpperCase());
    private static final Boolean BROKER_CLEAN_BETWEEN_TESTS = Boolean.getBoolean("broker.clean.between.tests");
    private static final Boolean BROKER_PERSISTENT = Boolean.getBoolean("broker.persistent");
    private static final Protocol BROKER_PROTOCOL =
            Protocol.valueOf("AMQP_" + System.getProperty("broker.version", "v0_9").substring(1));
    private static List<BrokerHolder> _brokerList = new ArrayList<>();

    static
    {
        String initialContext = System.getProperty(Context.INITIAL_CONTEXT_FACTORY);

        if (initialContext == null || initialContext.length() == 0)
        {
            System.setProperty(Context.INITIAL_CONTEXT_FACTORY, DEFAULT_INITIAL_CONTEXT);
        }
    }

    private final Map<String, String> _propertiesSetForBroker = new HashMap<>();
    private final List<Connection> _connections = new ArrayList<>();
    protected ConnectionFactory _connectionFactory;
    private BrokerHolder _defaultBroker;
    private MessageType _messageType = MessageType.TEXT;
    private Hashtable _initialContextEnvironment = new Hashtable();

    @Override
    public void runBare() throws Throwable
    {
        try
        {
            _defaultBroker = new BrokerHolderFactory().create(DEFAULT_BROKER_TYPE, DEFAULT_PORT, this);
            super.runBare();
        }
        catch (Exception e)
        {
            _logger.error("exception", e);
            throw e;
        }
        finally
        {
            stopAllBrokers();

            // reset properties used in the test
            revertSystemProperties();

            _logger.info("==========  stop " + getTestName() + " ==========");
        }
    }

    public Logger getLogger()
    {
        return QpidBrokerTestCase._logger;
    }

    public File getOutputFile()
    {
        final ch.qos.logback.classic.Logger logger =
                (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);

        return getFileFromSiftingAppender(logger);
    }

    public BrokerHolder getDefaultBroker()
    {
        return _defaultBroker;
    }

    public void startDefaultBroker() throws Exception
    {
        startDefaultBroker(false);
    }

    public void startDefaultBroker(boolean managementMode) throws Exception
    {
        getDefaultBroker().start(managementMode);
        setTestSystemProperty("test.port", getDefaultBroker().getAmqpPort() + "");
    }

    public void stopDefaultBroker() throws Exception
    {
        getDefaultBroker().shutdown();
    }

    public TestBrokerConfiguration getDefaultBrokerConfiguration()
    {
        return getDefaultBroker().getConfiguration();
    }

    public BrokerHolder createSpawnedBroker() throws Exception
    {
        return createSpawnedBroker(0);
    }

    public BrokerHolder createSpawnedBroker(int amqpPort) throws Exception
    {
        return new BrokerHolderFactory().create(BrokerHolder.BrokerType.SPAWNED, amqpPort, this);
    }

    public void killDefaultBroker()
    {
        getDefaultBroker().kill();
    }

    /**
     * Check whether the broker is an 0.8
     *
     * @return true if the broker is an 0_8 version, false otherwise.
     */
    public boolean isBroker08()
    {
        return BROKER_PROTOCOL.equals(Protocol.AMQP_0_8);
    }

    public boolean isBroker010()
    {
        return BROKER_PROTOCOL.equals(Protocol.AMQP_0_10);
    }

    public boolean isBroker10()
    {
        return BROKER_PROTOCOL.equals(Protocol.AMQP_1_0);
    }

    public Protocol getBrokerProtocol()
    {
        return BROKER_PROTOCOL;
    }

    public void restartDefaultBroker() throws Exception
    {
        getDefaultBroker().restart();
    }

    /**
     * we assume that the environment is correctly set
     * i.e. -Djava.naming.provider.url="..//example010.properties"
     *
     * @return an initial context
     * @throws NamingException if there is an error getting the context
     */
    public InitialContext getInitialContext() throws NamingException
    {
        return new InitialContext(_initialContextEnvironment);
    }

    /**
     * Get the default connection factory for the currently used broker
     * Default factory is "local"
     *
     * @return A connection factory
     * @throws NamingException if there is an error getting the factory
     */
    public ConnectionFactory getConnectionFactory() throws NamingException
    {
        if (_connectionFactory == null)
        {
            if (Boolean.getBoolean(PROFILE_USE_SSL))
            {
                _connectionFactory = getConnectionFactory("default.ssl");
            }
            else
            {
                _connectionFactory = getConnectionFactory("default");
            }
        }
        return _connectionFactory;
    }

    /**
     * Get a connection factory for the currently used broker
     *
     * @param factoryName The factory name
     * @return A connection factory
     * @throws NamingException if there is an error getting the factory
     */
    public ConnectionFactory getConnectionFactory(String factoryName)
            throws NamingException
    {
        return getConnectionFactory(factoryName, "test", "clientid");
    }
    public ConnectionFactory getConnectionFactory(String factoryName, String vhost, String clientId)
            throws NamingException
    {
        return getConnectionFactory(factoryName, vhost, clientId, Collections.<String, String>emptyMap());
    }
    public ConnectionFactory getConnectionFactory(String factoryName, String vhost, String clientId, Map<String,String> options)
            throws NamingException
    {

        if(isBroker10())
        {
            Map<String,String> actualOptions = new LinkedHashMap<>();
            actualOptions.put("amqp.vhost", vhost);
            actualOptions.put("jms.clientID", clientId);
            actualOptions.put("jms.forceSyncSend", "true");
            actualOptions.putAll(options);
            if("failover".equals(factoryName))
            {
                if(!actualOptions.containsKey("failover.maxReconnectAttempts"))
                {
                    actualOptions.put("failover.maxReconnectAttempts", "2");
                }
                final StringBuilder stem = new StringBuilder("failover:(amqp://localhost:")
                                                  .append(System.getProperty("test.port"))
                                                  .append(",amqp://localhost:")
                                                  .append(System.getProperty("test.port.alt"))
                                                  .append(")");
                appendOptions(actualOptions, stem);

                _initialContextEnvironment.put("property.connectionfactory.failover.remoteURI",
                                               stem.toString());
            }
            else if("default".equals(factoryName))
            {
                final StringBuilder stem = new StringBuilder("amqp://localhost:").append(System.getProperty("test.port"));


                appendOptions(actualOptions, stem);

                _initialContextEnvironment.put("property.connectionfactory.default.remoteURI",
                                               stem.toString());

            }
        }
        return (ConnectionFactory) getInitialContext().lookup(factoryName);
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

    public Connection getConnection() throws JMSException, NamingException
    {
        return getConnection(GUEST_USERNAME, GUEST_PASSWORD);
    }

    public Connection getConnectionWithPrefetch(int prefetch) throws JMSException, NamingException, URLSyntaxException
    {
        if(isBroker10())
        {
            String factoryName = Boolean.getBoolean(PROFILE_USE_SSL) ? "default.ssl" : "default";

            final Map<String, String> options =
                    Collections.singletonMap("jms.prefetchPolicy.all", String.valueOf(prefetch));
            final ConnectionFactory connectionFactory = getConnectionFactory(factoryName, "test", "clientid", options);
            return connectionFactory.createConnection(GUEST_USERNAME, GUEST_PASSWORD);

        }
        else
        {
            return getConnectionWithOptions(Collections.singletonMap("maxprefetch", String.valueOf(prefetch)));
        }
    }

    public Connection getConnectionWithOptions(Map<String, String> options)
            throws URLSyntaxException, NamingException, JMSException
    {
        ConnectionURL curl = new AMQConnectionURL(((AMQConnectionFactory)getConnectionFactory()).getConnectionURLString());
        for (Map.Entry<String, String> entry : options.entrySet())
        {
            curl.setOption(entry.getKey(), entry.getValue());
        }
        curl = new AMQConnectionURL(curl.toString());

        curl.setUsername(GUEST_USERNAME);
        curl.setPassword(GUEST_PASSWORD);
        return getConnection(curl);
    }

    public Connection getConnectionForVHost(String vhost)
            throws URLSyntaxException, NamingException, JMSException
    {
        return getConnectionForVHost(vhost, GUEST_USERNAME, GUEST_PASSWORD);
    }

    public Connection getConnectionForVHost(String vhost, String username, String password)
            throws URLSyntaxException, NamingException, JMSException
    {

        if(isBroker10())
        {
            return getConnectionFactory(Boolean.getBoolean(PROFILE_USE_SSL) ? "default.ssl" : "default", vhost, "clientId").createConnection(username, password);
        }
        else
        {
            ConnectionURL curl =
                    new AMQConnectionURL(((AMQConnectionFactory) getConnectionFactory()).getConnectionURLString());
            curl.setVirtualHost("/"+vhost);
            curl = new AMQConnectionURL(curl.toString());

            curl.setUsername(username);
            curl.setPassword(password);
            return getConnection(curl);
        }
    }


    public Connection getConnection(ConnectionURL url) throws JMSException
    {
        _logger.debug("get connection for " + url.getURL());
        Connection connection = new AMQConnectionFactory(url).createConnection(url.getUsername(), url.getPassword());

        _connections.add(connection);

        return connection;
    }

    /**
     * Get a connection (remote or in-VM)
     *
     * @param username The user name
     * @param password The user password
     * @return a newly created connection
     * @throws JMSException NamingException if there is an error getting the connection
     */
    public Connection getConnection(String username, String password) throws JMSException, NamingException
    {
        _logger.debug("get connection for username " + username);
        Connection con = getConnectionFactory().createConnection(username, password);
        //add the connection in the list of connections
        _connections.add(con);
        return con;
    }

    /**
     * Return a Queue specific for this test.
     * Uses getTestQueueName() as the name of the queue
     */
    public Queue getTestQueue()
    {
        return new AMQQueue(ExchangeDefaults.DIRECT_EXCHANGE_NAME, getTestQueueName());
    }

    public Queue getQueueFromName(Session session, String name) throws JMSException
    {
        if(isBroker10())
        {
            return session.createQueue(name);
        }
        else
        {
            return session.createQueue("ADDR: '" + name + "'");
        }
    }

    public Queue createTestQueue(Session session) throws JMSException
    {
        return createTestQueue(session, getTestQueueName());
    }

    public Queue createTestQueue(Session session, String queueName) throws JMSException
    {
        if(isBroker10())
        {
            createEntityUsingAmqpManagement(queueName, session, "org.apache.qpid.Queue");

            return session.createQueue(queueName);
        }
        else
        {

            AMQQueue amqQueue = new AMQQueue(ExchangeDefaults.DIRECT_EXCHANGE_NAME, queueName);
            session.createConsumer(amqQueue).close();
            return amqQueue;
        }
    }

    /**
     * Return a Topic specific for this test.
     * Uses getTestQueueName() as the name of the topic
     */
    public Topic getTestTopic()
    {
        return new AMQTopic(ExchangeDefaults.TOPIC_EXCHANGE_NAME, getTestQueueName());
    }

    protected Topic createTopic(final Connection con, final String topicName) throws JMSException
    {
        if(isBroker10())
        {
            Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
            createEntityUsingAmqpManagement(topicName, session, "org.apache.qpid.TopicExchange");

            return session.createTopic(topicName);

        }
        else
        {
            return new AMQTopic(ExchangeDefaults.TOPIC_EXCHANGE_NAME, topicName);
        }
    }

    protected Topic createTopicOnDirect(final Connection con, String topicName) throws JMSException, URISyntaxException
    {
        if(isBroker10())
        {
            Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
            return session.createTopic("amq.direct/"+topicName);
        }
        else
        {
            return new AMQTopic(
                    "direct://amq.direct/"+topicName+"/"+topicName+"?routingkey='"+topicName+"',exclusive='true',autodelete='true'");
        }
    }


    protected Topic createTopicOnFanout(final Connection con, String topicName) throws JMSException, URISyntaxException
    {
        if(isBroker10())
        {
            Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
            return session.createTopic("amq.fanout/"+topicName);
        }
        else
        {
            return new AMQTopic(
                    "fanout://amq.fanout/"+topicName+"/"+topicName+"?routingkey='"+topicName+"',exclusive='true',autodelete='true'");
        }
    }

    protected void createEntityUsingAmqpManagement(final String name, final Session session, final String type)
            throws JMSException
    {
        createEntityUsingAmqpManagement(name, session, type, Collections.<String,Object>emptyMap());
    }

    protected void createEntityUsingAmqpManagement(final String name, final Session session, final String type, Map<String, Object> attributes)
            throws JMSException
    {
        MessageProducer producer = session.createProducer(session.createQueue(isBroker10() ? "$management" : "ADDR:$management"));

        MapMessage createMessage = session.createMapMessage();
        createMessage.setStringProperty("type", type);
        createMessage.setStringProperty("operation", "CREATE");
        createMessage.setString("name", name);
        createMessage.setString("object-path", name);
        for(Map.Entry<String,Object> entry : attributes.entrySet())
        {
            createMessage.setObject(entry.getKey(), entry.getValue());
        }
        producer.send(createMessage);
        if(session.getTransacted())
        {
            session.commit();
        }
    }

    protected void deleteEntityUsingAmqpManagement(final String name, final Session session, final String type)
            throws JMSException
    {
        MessageProducer producer = session.createProducer(session.createQueue(isBroker10() ? "$management" : "ADDR:$management"));

        MapMessage createMessage = session.createMapMessage();
        createMessage.setStringProperty("type", type);
        createMessage.setStringProperty("operation", "DELETE");
        createMessage.setStringProperty("index", "object-path");

        createMessage.setStringProperty("key", name);
        producer.send(createMessage);
        if(session.getTransacted())
        {
            session.commit();
        }
    }

    protected void performOperationUsingAmqpManagement(final String name, final String operation, final Session session, final String type, Map<String,Object> arguments)
            throws JMSException
    {
        MessageProducer producer = session.createProducer(session.createQueue(isBroker10() ? "$management" : "ADDR:$management"));

        MapMessage opMessage = session.createMapMessage();
        opMessage.setStringProperty("type", type);
        opMessage.setStringProperty("operation", operation);
        opMessage.setStringProperty("index", "object-path");

        opMessage.setStringProperty("key", name);
        for(Map.Entry<String,Object> argument : arguments.entrySet())
        {
            opMessage.setObjectProperty(argument.getKey(), argument.getValue());
        }

        producer.send(opMessage);
        if(session.getTransacted())
        {
            session.commit();
        }
    }


    protected List managementQueryObjects(final Session session, final String type) throws JMSException
    {
        MessageProducer producer = session.createProducer(session.createQueue("$management"));
        final TemporaryQueue responseQ = session.createTemporaryQueue();
        MessageConsumer consumer = session.createConsumer(responseQ);
        MapMessage message = session.createMapMessage();
        message.setStringProperty("identity", "self");
        message.setStringProperty("type", "org.amqp.management");
        message.setStringProperty("operation", "QUERY");
        message.setStringProperty("entityType", type);
        message.setString("attributeNames", "[]");
        message.setJMSReplyTo(responseQ);

        producer.send(message);

        Message response = consumer.receive();
        try
        {
            if (response instanceof MapMessage)
            {
                return (List) ((MapMessage) response).getObject("results");
            }
            else if (response instanceof ObjectMessage)
            {
                Object body = ((ObjectMessage) response).getObject();
                if (body instanceof Map)
                {
                    return (List) ((Map) body).get("results");
                }
            }
            throw new IllegalArgumentException("Cannot parse the results from a management query");
        }
        finally
        {
            consumer.close();
            responseQ.delete();
        }
    }

    public long getQueueDepth(final Connection con, final Queue destination) throws JMSException, QpidException
    {
        if(isBroker10())
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
        else
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
    }


    /**
     * Send messages to the given destination.
     * <p/>
     * If session is transacted then messages will be committed before returning
     *
     * @param session     the session to use for sending
     * @param destination where to send them to
     * @param count       no. of messages to send
     * @return the sent messages
     * @throws Exception
     */
    public List<Message> sendMessage(Session session, Destination destination,
                                     int count) throws Exception
    {
        return sendMessage(session, destination, count, 0, 0);
    }

    /**
     * Send messages to the given destination.
     * <p/>
     * If session is transacted then messages will be committed before returning
     *
     * @param session     the session to use for sending
     * @param destination where to send them to
     * @param count       no. of messages to send
     * @param batchSize   the batchSize in which to commit, 0 means no batching,
     *                    but a single commit at the end
     * @return the sent message
     * @throws Exception
     */
    public List<Message> sendMessage(Session session, Destination destination,
                                     int count, int batchSize) throws Exception
    {
        return sendMessage(session, destination, count, 0, batchSize);
    }

    /**
     * Send messages to the given destination.
     * <p/>
     * If session is transacted then messages will be committed before returning
     *
     * @param session     the session to use for sending
     * @param destination where to send them to
     * @param count       no. of messages to send
     * @param offset      offset allows the INDEX value of the message to be adjusted.
     * @param batchSize   the batchSize in which to commit, 0 means no batching,
     *                    but a single commit at the end
     * @return the sent message
     * @throws Exception
     */
    public List<Message> sendMessage(Session session, Destination destination,
                                     int count, int offset, int batchSize) throws Exception
    {
        List<Message> messages = new ArrayList<>(count);

        MessageProducer producer = session.createProducer(destination);

        int i = offset;
        for (; i < (count + offset); i++)
        {
            Message next = createNextMessage(session, i);

            producer.send(next);

            if (session.getTransacted() && batchSize > 0)
            {
                if (i % batchSize == 0)
                {
                    session.commit();
                }
            }

            messages.add(next);
        }

        // Ensure we commit the last messages
        // Commit the session if we are transacted and
        // we have no batchSize or
        // our count is not divible by batchSize.
        if (session.getTransacted() &&
            (batchSize == 0 || (i - 1) % batchSize != 0))
        {
            session.commit();
        }

        return messages;
    }

    public Message createNextMessage(Session session, int msgCount) throws JMSException
    {
        Message message = createMessage(session, DEFAULT_MESSAGE_SIZE);
        message.setIntProperty(INDEX, msgCount);

        return message;
    }

    public Message createMessage(Session session, int messageSize) throws JMSException
    {
        String payload = new String(new byte[messageSize]);

        Message message;

        switch (_messageType)
        {
            case BYTES:
                message = session.createBytesMessage();
                ((BytesMessage) message).writeUTF(payload);
                break;
            case MAP:
                message = session.createMapMessage();
                ((MapMessage) message).setString(CONTENT, payload);
                break;
            default: // To keep the compiler happy
            case TEXT:
                message = session.createTextMessage();
                ((TextMessage) message).setText(payload);
                break;
            case OBJECT:
                message = session.createObjectMessage();
                ((ObjectMessage) message).setObject(payload);
                break;
            case STREAM:
                message = session.createStreamMessage();
                ((StreamMessage) message).writeString(payload);
                break;
        }

        return message;
    }

    public BrokerDetails getBrokerDetailsFromDefaultConnectionUrl()
    {
        try
        {
            if (((AMQConnectionFactory)getConnectionFactory()).getConnectionURL().getBrokerCount() > 0)
            {
                return ((AMQConnectionFactory)getConnectionFactory()).getConnectionURL().getBrokerDetails(0);
            }
            else
            {
                fail("No broker details are available.");
            }
        }
        catch (NamingException e)
        {
            fail(e.getMessage());
        }

        //keep compiler happy
        return null;
    }

    /**
     * Tests that a connection is functional by producing and consuming a single message.
     * Will fail if failover interrupts either transaction.
     */
    public void assertProducingConsuming(final Connection connection) throws Exception
    {
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Destination destination = session.createQueue(getTestQueueName());
        MessageConsumer consumer = session.createConsumer(destination);
        sendMessage(session, destination, 1);
        session.commit();
        connection.start();
        Message m1 = consumer.receive(RECEIVE_TIMEOUT);
        assertNotNull("Message 1 is not received", m1);
        assertEquals("Unexpected first message received", 0, m1.getIntProperty(INDEX));
        session.commit();
        session.close();
    }

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
        startDefaultBroker();
    }

    @Override
    protected void tearDown() throws java.lang.Exception
    {
        _logger.debug("tearDown started");
        try
        {
            for (Connection c : _connections)
            {
                c.close();
            }
        }
        finally
        {
            try
            {
                _defaultBroker.shutdown();
            }
            finally
            {
                super.tearDown();
            }
        }
    }

    protected int getDefaultAmqpPort()
    {
        return getDefaultBroker().getAmqpPort();
    }

    protected boolean stopBrokerSafely(BrokerHolder broker)
    {
        boolean success = true;
        try
        {
            broker.shutdown();

            if (BROKER_CLEAN_BETWEEN_TESTS)
            {
                broker.cleanUp();
            }
        }
        catch (Exception e)
        {
            success = false;
            _logger.error("Failed to stop broker " + broker, e);
            if (broker != null)
            {
                // save the thread dump in case of dead locks
                try
                {
                    _logger.error("Broker " + broker + " thread dump:" + broker.dumpThreads());
                }
                finally
                {
                    try
                    {
                        broker.kill();
                    }
                    catch (Exception killException)
                    {
                        // ignore
                    }
                }
            }
        }
        return success;
    }

    protected void createTestVirtualHostNode(String virtualHostNodeName)
    {
        createTestVirtualHostNode(getDefaultBroker(), virtualHostNodeName, true);
    }

    protected void createTestVirtualHostNode(BrokerHolder broker, String virtualHostNodeName, boolean withBlueprint)
    {
        String storeType = getTestProfileVirtualHostNodeType();
        String storeDir = null;

        if (System.getProperty("profile", "").startsWith("java-dby-mem"))
        {
            storeDir = ":memory:";
        }
        else if (!MemoryConfigurationStore.TYPE.equals(storeType))
        {
            storeDir = "${qpid.work_dir}" + File.separator + virtualHostNodeName;
        }

        String blueprint = null;
        if (withBlueprint)
        {
            blueprint = getTestProfileVirtualHostNodeBlueprint();
        }

        broker.createVirtualHostNode(virtualHostNodeName, storeType, storeDir, blueprint);
    }

    /**
     * Set a System property for the duration of this test.
     * <p/>
     * When the test run is complete the value will be reverted.
     * <p/>
     * The values set using this method will also be propagated to the external
     * Apache Qpid Broker for Java via a -D value defined in QPID_OPTS.
     * <p/>
     * If the value should not be set on the broker then use
     * setTestClientSystemProperty().
     *
     * @param property the property to set
     * @param value    the new value to use
     */
    protected void setSystemProperty(String property, String value)
    {
        synchronized (_propertiesSetForBroker)
        {
            // Record the value for the external broker
            if (value == null)
            {
                _propertiesSetForBroker.remove(property);
            }
            else
            {
                _propertiesSetForBroker.put(property, value);
            }
        }
        //Set the value for the test client vm aswell.
        setTestClientSystemProperty(property, value);
    }

    /**
     * Set a System  property for the client (and broker if using the same vm) of this test.
     *
     * @param property The property to set
     * @param value    the value to set it to.
     */
    protected void setTestClientSystemProperty(String property, String value)
    {
        setTestSystemProperty(property, value);
    }

    /**
     * Restore the System property values that were set before this test run.
     */
    protected void revertSystemProperties()
    {
        revertTestSystemProperties();

        // We don't change the current VMs settings for Broker only properties
        // so we can just clear this map
        _propertiesSetForBroker.clear();
    }

    protected boolean isJavaBroker()
    {
        return BROKER_LANGUAGE.equals("java");
    }

    protected boolean isCppBroker()
    {
        return BROKER_LANGUAGE.equals("cpp");
    }

    protected boolean isExternalBroker()
    {
        return !isInternalBroker();
    }

    protected boolean isInternalBroker()
    {
        return DEFAULT_BROKER_TYPE.equals(BrokerHolder.BrokerType.INTERNAL);
    }

    protected boolean isBrokerStorePersistent()
    {
        return BROKER_PERSISTENT;
    }

    protected Connection getConnectionWithSyncPublishing() throws URLSyntaxException, NamingException, JMSException
    {
        Map<String, String> options = new HashMap<>();
        options.put(ConnectionURL.OPTIONS_SYNC_PUBLISH, "all");
        return getConnectionWithOptions(options);
    }

    protected Connection getClientConnection(String username, String password, String id)
            throws JMSException, URLSyntaxException,
                   QpidException, NamingException
    {
        _logger.debug("get connection for id " + id);
        Connection con;
        if(isBroker10())
        {
            con = getConnectionFactory("default", "test", id).createConnection(username, password);
        }
        else
        {
            con = ((AMQConnectionFactory)getConnectionFactory()).createConnection(username, password, id);
        }
        //add the connection in the list of connections
        _connections.add(con);
        return con;
    }

    /**
     * Return a uniqueName for this test.
     * In this case it returns a queue Named by the TestCase and TestName
     *
     * @return String name for a queue
     */
    protected String  getTestQueueName()
    {
        return getClass().getSimpleName() + "-" + getName();
    }

    protected int getFailingPort()
    {
        return FAILING_PORT;
    }

    @Override
    protected void setTestOverriddenProperties(Properties properties)
    {
        for (String propertyName : properties.stringPropertyNames())
        {
            setSystemProperty(propertyName, properties.getProperty(propertyName));
        }
    }

    protected long getReceiveTimeout()
    {
        return Long.getLong("qpid.test_receive_timeout", 1000L);
    }

    protected long getLongReceiveTimeout()
    {
        return Long.getLong("qpid.test_receive_long_timeout", 5000L);
    }

    protected long getShortReceiveTimeout()
    {
        return Long.getLong("qpid.test_receive_short_timeout", 500L);
    }

    private File getFileFromSiftingAppender(final ch.qos.logback.classic.Logger logger)
    {
        String key = MDC.get(QpidTestCase.CLASS_QUALIFIED_TEST_NAME);

        for (Iterator<Appender<ILoggingEvent>> index = logger.iteratorForAppenders(); index.hasNext(); /* do nothing */)
        {
            Appender<ILoggingEvent> appender = index.next();
            if (appender instanceof SiftingAppender)
            {
                SiftingAppender siftingAppender = (SiftingAppender) appender;
                Appender subAppender = siftingAppender.getAppenderTracker().find(key);
                if (subAppender instanceof FileAppender)
                {
                    return new File(((FileAppender) subAppender).getFile());
                }
            }
        }
        return null;
    }

    private boolean existingInternalBroker()
    {
        for (BrokerHolder holder : _brokerList)
        {
            if (holder instanceof InternalBrokerHolder)
            {
                return true;
            }
        }

        return false;
    }

    private void stopAllBrokers()
    {
        boolean exceptionOccurred = false;
        for (BrokerHolder brokerHolder : _brokerList)
        {
            if (!stopBrokerSafely(brokerHolder))
            {
                exceptionOccurred = true;
            }
        }
        _brokerList.clear();
        if (exceptionOccurred)
        {
            throw new RuntimeException("Exception occurred on stopping of test broker. Please, examine logs for details");
        }
    }

    private Map<String, String> getJvmProperties()
    {
        Map<String, String> jvmOptions = new HashMap<>();
        synchronized (_propertiesSetForBroker)
        {
            jvmOptions.putAll(_propertiesSetForBroker);

            copySystemProperty("test.port", jvmOptions);
            copySystemProperty("test.hport", jvmOptions);
            copySystemProperty("test.port.ssl", jvmOptions);
            copySystemProperty("test.port.alt", jvmOptions);
            copySystemProperty("test.port.alt.ssl", jvmOptions);
            copySystemProperty("test.amqp_port_protocols", jvmOptions);

            copySystemProperty("qpid.globalAddressDomains", jvmOptions);

            copySystemProperty("virtualhostnode.type", jvmOptions);
            copySystemProperty("virtualhostnode.context.blueprint", jvmOptions);
        }
        return jvmOptions;
    }

    private void copySystemProperty(String name, Map<String, String> jvmOptions)
    {
        String value = System.getProperty(name);
        if (value != null)
        {
            jvmOptions.put(name, value);
        }
    }

    /**
     * Type of message
     */
    protected enum MessageType
    {
        BYTES,
        MAP,
        OBJECT,
        STREAM,
        TEXT
    }

    public static class BrokerHolderFactory
    {
        public BrokerHolder create(BrokerHolder.BrokerType brokerType, int port, QpidBrokerTestCase testCase)
        {
            // This will force the creation of the file appender
            _logger.debug("Creating BrokerHolder");

            final File logFile = testCase.getOutputFile();
            final String classQualifiedTestName = testCase.getClassQualifiedTestName();
            BrokerHolder holder = null;
            if (brokerType.equals(BrokerHolder.BrokerType.INTERNAL) && !testCase.existingInternalBroker())
            {
                holder = new InternalBrokerHolder(port, classQualifiedTestName, logFile);
            }
            else if (!brokerType.equals(BrokerHolder.BrokerType.EXTERNAL))
            {
                Map<String, String> jvmOptions = testCase.getJvmProperties();
                Map<String, String> environmentProperties = new HashMap<>(testCase._propertiesSetForBroker);

                holder = new SpawnedBrokerHolder(port, classQualifiedTestName, logFile,
                                                 jvmOptions, environmentProperties);
            }
            _brokerList.add(holder);
            return holder;
        }
    }

}
