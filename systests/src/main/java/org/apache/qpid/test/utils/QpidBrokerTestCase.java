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

import static org.apache.qpid.systests.Utils.getAmqpManagementFacade;
import static org.apache.qpid.systests.Utils.getProtocol;

import java.io.File;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.naming.NamingException;

import ch.qos.logback.classic.sift.SiftingAppender;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.FileAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.store.MemoryConfigurationStore;
import org.apache.qpid.systests.AmqpManagementFacade;
import org.apache.qpid.systests.ConnectionBuilder;
import org.apache.qpid.systests.JmsProvider;
import org.apache.qpid.systests.Utils;

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
    private static final Logger LOGGER = LoggerFactory.getLogger(QpidBrokerTestCase.class);
    protected static final long RECEIVE_TIMEOUT = Long.getLong("qpid.test_receive_timeout", 1000L);
    protected static final String INDEX = "index";
    protected static final String CONTENT = "content";
    protected static final int DEFAULT_MESSAGE_SIZE = 1024;
    private static final String JAVA = "java";
    private static final String BROKER_LANGUAGE = System.getProperty("broker.language", JAVA);
    private static final BrokerHolder.BrokerType DEFAULT_BROKER_TYPE = BrokerHolder.BrokerType.valueOf(
            System.getProperty("broker.type", BrokerHolder.BrokerType.INTERNAL.name()).toUpperCase());
    private static final Boolean BROKER_CLEAN_BETWEEN_TESTS = Boolean.getBoolean("broker.clean.between.tests");
    private static final Boolean BROKER_PERSISTENT = Boolean.getBoolean("broker.persistent");
    private static final Protocol BROKER_PROTOCOL = getProtocol();
    private static List<BrokerHolder> _brokerList = new ArrayList<>();

    private final Map<String, String> _propertiesSetForBroker = new HashMap<>();
    private final List<Connection> _connections = new ArrayList<>();
    private AmqpManagementFacade _managementFacade;
    private BrokerHolder _defaultBroker;
    private MessageType _messageType = MessageType.TEXT;

    private JmsProvider _jmsProvider;

    @Override
    public void runBare() throws Throwable
    {
        try
        {
            _managementFacade = getAmqpManagementFacade();
            _jmsProvider = Utils.getJmsProvider();

            _defaultBroker = new BrokerHolderFactory().create(DEFAULT_BROKER_TYPE, DEFAULT_PORT, this);
            super.runBare();
        }
        catch (Exception e)
        {
            LOGGER.error("exception", e);
            throw e;
        }
        finally
        {
            stopAllBrokers();

            // reset properties used in the test
            revertSystemProperties();

            LOGGER.info("==========  stop " + getTestName() + " ==========");
        }
    }

    public Logger getLogger()
    {
        return QpidBrokerTestCase.LOGGER;
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

    public boolean isBrokerPre010()
    {
        return EnumSet.of(Protocol.AMQP_0_8, Protocol.AMQP_0_9, Protocol.AMQP_0_9_1).contains(BROKER_PROTOCOL);
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

    public JmsProvider getJmsProvider()
    {
        return _jmsProvider;
    }

    public ConnectionBuilder getConnectionBuilder()
    {
        final ConnectionBuilder connectionBuilder = _jmsProvider.getConnectionBuilder()
                                                                .setPort(Integer.getInteger("test.port"))
                                                                .setSslPort(Integer.getInteger("test.port.ssl"))
                                                                .setVirtualHost("test")
                                                                .setTls(Boolean.getBoolean(PROFILE_USE_SSL))
                                                                .setPopulateJMSXUserID(true)
                                                                .setUsername(GUEST_USERNAME)
                                                                .setPassword(GUEST_PASSWORD);

        return (ConnectionBuilder) Proxy.newProxyInstance(getClass().getClassLoader(),
                                                          new Class<?>[]{ConnectionBuilder.class},
                                                          new ConectionBuilderHandler(connectionBuilder, _connections));
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
        return getConnectionFactory(Collections.emptyMap());
    }

    public ConnectionFactory getConnectionFactory(final Map<String, String> options) throws NamingException
    {
        return getConnectionBuilder().setOptions(options).buildConnectionFactory();
    }

    public Connection getConnection() throws JMSException, NamingException
    {
        return getConnection(GUEST_USERNAME, GUEST_PASSWORD);
    }

    public Connection getConnection(String username, String password) throws JMSException, NamingException
    {
        return getConnectionBuilder().setUsername(username).setPassword(password).build();
    }

    public Connection getConnectionWithPrefetch(int prefetch) throws Exception
    {
        return getConnectionBuilder().setPrefetch(prefetch).build();
    }

    public Connection getConnectionWithOptions(Map<String, String> options) throws Exception
    {
        return getConnectionBuilder().setOptions(options).build();
    }

    public Connection getConnectionWithOptions(String vhost, Map<String, String> options) throws Exception
    {
        return getConnectionBuilder().setOptions(options)
                                     .setVirtualHost(vhost)
                                     .build();
    }

    public Connection getConnectionForVHost(String vhost) throws Exception
    {
        return getConnectionBuilder().setVirtualHost(vhost).build();
    }

    public Connection getConnection(String urlString) throws Exception
    {
        Connection connection = _jmsProvider.getConnection(urlString);
        _connections.add(connection);
        return connection;
    }

    public Queue getTestQueue() throws NamingException
    {
        return _jmsProvider.getTestQueue(getTestQueueName());
    }

    public Queue getQueueFromName(Session session, String name) throws JMSException
    {
        return _jmsProvider.getQueueFromName(session, name);
    }

    public Queue createTestQueue(Session session) throws JMSException
    {
        return _jmsProvider.createQueue(session, getTestQueueName());
    }

    public Queue createTestQueue(Session session, String queueName) throws JMSException
    {
        return _jmsProvider.createQueue(session, queueName);
    }

    /**
     * Return a Topic specific for this test.
     * Uses getTestQueueName() as the name of the topic
     */
    public Topic getTestTopic() throws NamingException
    {
        return _jmsProvider.getTestTopic(getTestQueueName());
    }

    protected Topic createTopic(final Connection con, final String topicName) throws JMSException
    {
        return _jmsProvider.createTopic(con, topicName);
    }

    protected Topic createTopicOnDirect(final Connection con, String topicName) throws JMSException, URISyntaxException
    {
        return _jmsProvider.createTopicOnDirect(con, topicName);
    }

    protected Topic createTopicOnFanout(final Connection con, String topicName) throws JMSException, URISyntaxException
    {
        return _jmsProvider.createTopicOnFanout(con, topicName);
    }

    protected void createEntityUsingAmqpManagement(final String name, final Session session, final String type)
            throws JMSException
    {
        _managementFacade.createEntityUsingAmqpManagement(name, session, type);
    }

    protected void createEntityUsingAmqpManagement(final String name, final Session session, final String type, Map<String, Object> attributes)
            throws JMSException
    {

        _managementFacade.createEntityUsingAmqpManagement(name, session, type, attributes);
    }

    protected void updatenEntityUsingAmqpManagement(final String name, final Session session, final String type, Map<String, Object> attributes)
            throws JMSException
    {
        _managementFacade.updateEntityUsingAmqpManagement(name, session, type, attributes);
    }

    protected void deleteEntityUsingAmqpManagement(final String name, final Session session, final String type)
            throws JMSException
    {

        _managementFacade.deleteEntityUsingAmqpManagement(name, session, type);
    }

    protected Object performOperationUsingAmqpManagement(final String name, final String operation, final Session session, final String type, Map<String,Object> arguments)
            throws JMSException
    {
        return _managementFacade.performOperationUsingAmqpManagement(name, operation, session, type, arguments);
    }

    protected List<Map<String, Object>> managementQueryObjects(final Session session, final String type) throws JMSException
    {

        return _managementFacade.managementQueryObjects(session, type);
    }

    protected Map<String, Object> managementReadObject(Session session, String type, String name, boolean actuals) throws JMSException
    {
        return _managementFacade.readEntityUsingAmqpManagement(session, type, name, actuals);
    }

    public long getQueueDepth(final Connection con, final Queue destination) throws Exception
    {
        Connection connection = getConnection();
        try
        {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            try
            {
                return _managementFacade.getQueueDepth(destination, session);
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

    public boolean isQueueExist(final Connection con, final Queue destination) throws Exception
    {
        Connection connection = getConnection();
        try
        {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            try
            {
                return _managementFacade.isQueueExist(destination, session);
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

    public String getBrokerDetailsFromDefaultConnectionUrl()
    {
        return "tcp://localhost:" + (getDefaultBroker().getAmqpTlsPort() > 0
                ? getDefaultBroker().getAmqpTlsPort()
                : getDefaultBroker().getAmqpPort());
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
        Message m1 = consumer.receive(getReceiveTimeout());
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
        LOGGER.debug("tearDown started");
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
            LOGGER.error("Failed to stop broker " + broker, e);
            if (broker != null)
            {
                // save the thread dump in case of dead locks
                try
                {
                    LOGGER.error("Broker " + broker + " thread dump:" + broker.dumpThreads());
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
     * Apache Qpid Broker-J via a -D value defined in QPID_OPTS.
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

    protected Connection getConnectionWithSyncPublishing() throws Exception
    {
        return getConnectionBuilder().setSyncPublish(true).build();
    }

    protected Connection getClientConnection(String username, String password, String id)
            throws Exception
    {
        return getConnectionBuilder().setClientId(id).setUsername(username).setPassword(password).build();
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
        String key = logger.getLoggerContext().getProperty(LogbackPropertyValueDiscriminator.CLASS_QUALIFIED_TEST_NAME);

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
            LOGGER.debug("Creating BrokerHolder");

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

    private static class ConectionBuilderHandler implements InvocationHandler
    {
        private final ConnectionBuilder _connectionBuilder;
        private final List<Connection> _connections;

        public ConectionBuilderHandler(final ConnectionBuilder connectionBuilder,
                                       final List<Connection> connections)
        {
            _connectionBuilder = connectionBuilder;
            _connections = connections;
        }

        @Override
        public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable
        {
            if (method.getName().equals("build"))
            {
                Connection connection = _connectionBuilder.build();
                _connections.add(connection);
                return connection;
            }
            else if (method.getName().equals("buildConnectionFactory"))
            {
                return _connectionBuilder.buildConnectionFactory();
            }
            else
            {
                method.invoke(_connectionBuilder, args);
                return proxy;
            }
        }
    }
}
