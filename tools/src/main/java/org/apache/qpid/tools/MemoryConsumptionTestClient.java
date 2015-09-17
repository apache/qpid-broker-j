/*
 *
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
package org.apache.qpid.tools;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.management.MBeanServerConnection;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import com.sun.management.GarbageCollectionNotificationInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.QpidException;
import org.apache.qpid.client.AMQDestination;
import org.apache.qpid.client.AMQSession;


public class MemoryConsumptionTestClient
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MemoryConsumptionTestClient.class);

    private static final String QUEUE_NAME_PREFIX = "BURL:direct://amq.direct//memory-test-queue";
    private static final String DURABLE_SUFFIX = "?durable='true'";

    public static final String CONNECTIONS_ARG = "connections";
    public static final String SESSIONS_ARG = "sessions";
    public static final String PRODUCERS_ARG = "producers";
    public static final String MESSAGE_COUNT_ARG = "messagecount";
    public static final String MESSAGE_SIZE_ARG = "size";
    public static final String PERSISTENT_ARG = "persistent";
    public static final String TIMEOUT_ARG = "timeout";
    public static final String TRANSACTED_ARG = "transacted";
    public static final String JMX_HOST_ARG = "jmxhost";
    public static final String JMX_PORT_ARG = "jmxport";
    public static final String JMX_USER_ARG = "jmxuser";
    public static final String JMX_USER_PASSWORD_ARG = "jmxpassword";

    public static final String CONNECTIONS_DEFAULT = "1";
    public static final String SESSIONS_DEFAULT = "1";
    public static final String PRODUCERS_DEFAULT = "1";
    public static final String MESSAGE_COUNT_DEFAULT = "1";
    public static final String MESSAGE_SIZE_DEFAULT = "256";
    public static final String PERSISTENT_DEFAULT = "false";
    public static final String TIMEOUT_DEFAULT = "1000";
    public static final String TRANSACTED_DEFAULT = "false";

    private static final String JMX_HOST_DEFAULT = "localhost";
    private static final String JMX_PORT_DEFAULT = "8999";
    private static final String JMX_GARBAGE_COLLECTOR_MBEAN = "gc";

    public static void main(String[] args)
    {
        Map<String,String> options = new HashMap<>();
        options.put(CONNECTIONS_ARG, CONNECTIONS_DEFAULT);
        options.put(SESSIONS_ARG, SESSIONS_DEFAULT);
        options.put(PRODUCERS_ARG, PRODUCERS_DEFAULT);
        options.put(MESSAGE_COUNT_ARG, MESSAGE_COUNT_DEFAULT);
        options.put(MESSAGE_SIZE_ARG, MESSAGE_SIZE_DEFAULT);
        options.put(PERSISTENT_ARG, PERSISTENT_DEFAULT);
        options.put(TIMEOUT_ARG, TIMEOUT_DEFAULT);
        options.put(TRANSACTED_ARG, TRANSACTED_DEFAULT);
        options.put(JMX_HOST_ARG, JMX_HOST_DEFAULT);
        options.put(JMX_PORT_ARG, JMX_PORT_DEFAULT);
        options.put(JMX_USER_ARG, "");
        options.put(JMX_USER_PASSWORD_ARG, "");
        options.put(JMX_GARBAGE_COLLECTOR_MBEAN, "java.lang:type=GarbageCollector,name=ConcurrentMarkSweep");

        if(args.length == 1 &&
                (args[0].equals("-h") || args[0].equals("--help") || args[0].equals("help")))
        {
            System.out.println("arg=value options: \n" + options.keySet());
            return;
        }

        parseArgumentsIntoConfig(options, args);

        MemoryConsumptionTestClient testClient = new MemoryConsumptionTestClient();
        testClient.runTest(options);
    }

    public static void parseArgumentsIntoConfig(Map<String, String> initialValues, String[] args)
    {
        for(String arg: args)
        {
            int equalPos = arg.indexOf('=');
            if(equalPos == -1)
            {
                throw new IllegalArgumentException("arguments must have format <name>=<value>: " + arg);
            }

            if(initialValues.put(arg.substring(0, equalPos), arg.substring(equalPos + 1)) == null)
            {
                throw new IllegalArgumentException("not a valid configuration property: " + arg);
            }
        }
    }


    private void runTest(Map<String,String> options)
    {
        int numConnections = Integer.parseInt(options.get(CONNECTIONS_ARG));
        int numSessions = Integer.parseInt(options.get(SESSIONS_ARG));
        int numProducers = Integer.parseInt(options.get(PRODUCERS_ARG));
        int numMessage = Integer.parseInt(options.get(MESSAGE_COUNT_ARG));
        int messageSize = Integer.parseInt(options.get(MESSAGE_SIZE_ARG));
        String queueString = QUEUE_NAME_PREFIX + DURABLE_SUFFIX;
        int deliveryMode = Boolean.valueOf(options.get(PERSISTENT_ARG)) ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT;
        long receiveTimeout = Long.parseLong(options.get(TIMEOUT_ARG));
        boolean transacted = Boolean.valueOf(options.get(TRANSACTED_ARG));

        LOGGER.info("Using options: " + options);

        try
        {
            // Load JNDI properties
            Properties properties = new Properties();
            try(InputStream is = this.getClass().getClassLoader().getResourceAsStream("stress-test-client.properties"))
            {
                properties.load(is);
            }

            ConnectionFactory conFac = createConnectionFactory(properties);
            Destination destination = ensureQueueCreated(queueString, conFac);
            Map<Connection, List<Session>> connectionsAndSessions = openConnectionsAndSessions(numConnections, numSessions, transacted, conFac);
            publish(numMessage, messageSize, numProducers, deliveryMode, destination, connectionsAndSessions);
            MemoryStatistic memoryStatistics = collectMemoryStatistics(options);
            generateCSV(memoryStatistics, numConnections, numSessions, transacted, numMessage, messageSize, numProducers, deliveryMode);
            purgeQueue(conFac, queueString, receiveTimeout);
            closeConnections(connectionsAndSessions.keySet());
        }
        catch (Exception e)
        {
            LOGGER.error("Exception on running the test", e);
        }
    }

    private void generateCSV(MemoryStatistic memoryStatistics, int numConnections, int numSessions, boolean transacted, int numMessage, int messageSize, int numProducers, int deliveryMode)
    {
        System.out.println(memoryStatistics.getHeapUsage() + "," + memoryStatistics.getDirectMemoryUsage()
                + "," + numConnections  + "," + numSessions  + "," + numProducers + "," + transacted + "," + numMessage
                + "," + messageSize + "," + deliveryMode + "," + toUserFriendlyName(memoryStatistics.getHeapUsage())
                + "," + toUserFriendlyName(memoryStatistics.getDirectMemoryUsage()));
    }

    private void publish(int numberOfMessages, int messageSize, int numberOfProducers, int deliveryMode,
                         Destination destination, Map<Connection, List<Session>> connectionsAndSessions) throws JMSException
    {
        byte[] messageBytes = generateMessage(messageSize);
        for (List<Session> sessions : connectionsAndSessions.values())
        {
            for (Session session: sessions)
            {
                BytesMessage message = session.createBytesMessage();

                if (messageSize > 0)
                {
                    message.writeBytes(messageBytes);
                }

                for(int i = 0; i < numberOfProducers ; i++)
                {
                    MessageProducer prod = session.createProducer(destination);
                    for(int j = 0; j < numberOfMessages ; j++)
                    {
                        prod.send(message, deliveryMode, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                        if(session.getTransacted())
                        {
                            session.commit();
                        }
                    }
                }
            }
        }
    }

    private Map<Connection, List<Session>> openConnectionsAndSessions(int numConnections, int numSessions, boolean transacted, ConnectionFactory conFac) throws JMSException
    {
        Map<Connection, List<Session>> connectionAndSessions = new HashMap<>();
        for (int i= 0; i < numConnections ; i++)
        {
            Connection connection = conFac.createConnection();
            connection.setExceptionListener(new ExceptionListener()
            {
                public void onException(JMSException jmse)
                {
                    LOGGER.error("The sample received an exception through the ExceptionListener", jmse);
                    System.exit(1);
                }
            });

            List<Session> sessions = new ArrayList<>();
            connectionAndSessions.put(connection, sessions);
            connection.start();
            for (int s= 0; s < numSessions ; s++)
            {
                Session session = connection.createSession(transacted, transacted?Session.SESSION_TRANSACTED:Session.AUTO_ACKNOWLEDGE);
                sessions.add(session);
            }
        }
        return connectionAndSessions;
    }

    private Destination ensureQueueCreated(String queueURL, ConnectionFactory connectionFactory) throws JMSException
    {
        Connection connection = connectionFactory.createConnection();
        Destination destination = null;
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            destination = session.createQueue(queueURL);
            MessageConsumer consumer = session.createConsumer(destination);
            consumer.close();
            session.close();
        }
        finally
        {
            connection.close();
        }
        return destination;
    }

    private ConnectionFactory createConnectionFactory(Properties properties) throws NamingException
    {
        ConnectionFactory connectionFactory = null;
        Context ctx = new InitialContext(properties);
        try
        {
            connectionFactory = (ConnectionFactory) ctx.lookup("qpidConnectionfactory");
        }
        finally
        {
            ctx.close();
        }
        return connectionFactory;
    }

    private void closeConnections(Collection<Connection> connections) throws JMSException, NamingException
    {
        for (Connection c: connections)
        {
            c.close();
        }
    }

    private void purgeQueue(ConnectionFactory connectionFactory, String queueString, long receiveTimeout) throws JMSException, QpidException
    {
        LOGGER.debug("Consuming left over messages, using receive timeout:" + receiveTimeout);

        Connection connection = connectionFactory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createQueue(queueString);
        MessageConsumer consumer = session.createConsumer(destination);
        connection.start();

        int count = 0;
        while(true)
        {
            BytesMessage msg = (BytesMessage) consumer.receive(receiveTimeout);

            if(msg == null)
            {
                long queueDepth = ((AMQSession<?,?>)session).getQueueDepth((AMQDestination)destination);
                if (queueDepth == 0)
                {
                    LOGGER.debug("Received " + count + " messages");
                    break;
                }
            }
            else
            {
                count++;
            }
        }

        consumer.close();
        session.close();
        connection.close();
    }

    private MemoryStatistic collectMemoryStatistics(Map<String, String> options) throws Exception
    {
        String host = options.get(JMX_HOST_ARG);
        String port = options.get(JMX_PORT_ARG);
        String user = options.get(JMX_USER_ARG);
        String password = options.get(JMX_USER_PASSWORD_ARG);

        if (!"".equals(host) && !"".equals(port) && !"".equals(user) && !"".equals(password))
        {
            final MemoryStatistic memoryStatistics = new MemoryStatistic();
            Map<String, Object> environment = Collections.<String, Object>singletonMap(JMXConnector.CREDENTIALS, new String[]{user, password});
            JMXConnector jmxConnector = JMXConnectorFactory.newJMXConnector(new JMXServiceURL("rmi", "", 0, "/jndi/rmi://" + host + ":" + port + "/jmxrmi"), environment);
            jmxConnector.connect();
            try
            {
                final MBeanServerConnection mBeanServerConnection = jmxConnector.getMBeanServerConnection();
                final ObjectName memoryMBean = new ObjectName("java.lang:type=Memory");
                ObjectName gcMBean = new ObjectName(options.get(JMX_GARBAGE_COLLECTOR_MBEAN));
                final CountDownLatch notificationReceived = new CountDownLatch(1);
                mBeanServerConnection.addNotificationListener(gcMBean, new NotificationListener()
                {
                    @Override
                    public void handleNotification(Notification notification, Object handback)
                    {
                        if (notification.getType().equals(GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION))
                        {
                            GarbageCollectionNotificationInfo info = GarbageCollectionNotificationInfo.from((CompositeData) notification.getUserData());
                            String gcCause = info.getGcCause();
                            if (gcCause.equals("System.gc()") && info.getGcAction().contains("end of major GC"))
                            {
                                try
                                {
                                    Object heapMemoryUsage = mBeanServerConnection.getAttribute(memoryMBean, "HeapMemoryUsage");
                                    Object used = ((CompositeData) heapMemoryUsage).get("used");
                                    Object directMemoryTotalCapacity = mBeanServerConnection.getAttribute(new ObjectName("java.nio:type=BufferPool,name=direct"), "TotalCapacity");

                                    memoryStatistics.setHeapUsage(Long.parseLong(String.valueOf(used)));
                                    memoryStatistics.setDirectMemoryUsage(Long.parseLong(String.valueOf(directMemoryTotalCapacity)));
                                }
                                catch (Exception e)
                                {
                                    e.printStackTrace();
                                }
                                finally
                                {
                                    notificationReceived.countDown();
                                }
                            }
                        }
                    }
                }, null, null );

                mBeanServerConnection.invoke(memoryMBean, "gc", null, null);
                notificationReceived.await(5, TimeUnit.SECONDS);

            }
            finally
            {
                jmxConnector.close();
            }

            return memoryStatistics;
        }
        else
        {
            return null;
        }
    }

    private String toUserFriendlyName(Object intValue)
    {
        long value = Long.parseLong(String.valueOf(intValue));
        if (value <= 1024)
        {
            return String.valueOf(value) + "B";
        }
        else if (value <= 1024 * 1024)
        {
            return String.valueOf(value/1024) + "kB";
        }
        else if (value <= 1024l * 1024l * 1024l)
        {
            return String.valueOf(value/1024l/1024l) + "MB";
        }
        else
        {
            return String.valueOf(value/1024l/1024l/1024l) + "GB";
        }
    }


    private byte[] generateMessage(int messageSize)
    {
        byte[] sentBytes = new byte[messageSize];
        for(int r = 0 ; r < messageSize ; r++)
        {
            sentBytes[r] = (byte) (48 + (r % 10));
        }
        return sentBytes;
    }

    private class MemoryStatistic
    {
        private long heapUsage;
        private long directMemoryUsage;

        public long getHeapUsage()
        {
            return heapUsage;
        }

        public void setHeapUsage(long heapUsage)
        {
            this.heapUsage = heapUsage;
        }

        public long getDirectMemoryUsage()
        {
            return directMemoryUsage;
        }

        public void setDirectMemoryUsage(long directMemoryUsage)
        {
            this.directMemoryUsage = directMemoryUsage;
        }
    }
}
