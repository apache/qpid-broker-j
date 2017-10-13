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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

public class StressTestClient
{
    private static final String JNDI_PROPERTIES_ARG = "jndiProperties";
    private static final String JNDI_DESTINATION_ARG = "jndiDestination";
    private static final String JNDI_CONNECTION_FACTORY_ARG = "jndiConnectionFactory";
    private static final String CONNECTIONS_ARG = "connections";
    private static final String SESSIONS_ARG = "sessions";
    private static final String CONSUME_IMMEDIATELY_ARG = "consumeImmediately";
    private static final String CONSUMERS_ARG = "consumers";
    private static final String CLOSE_CONSUMERS_ARG = "closeconsumers";
    private static final String DISABLE_MESSAGE_ID_ARG = "disableMessageId";
    private static final String DISABLE_MESSAGE_TIMESTAMP_ARG = "disableMessageTimestamp";
    private static final String PRODUCERS_ARG = "producers";
    private static final String MESSAGE_COUNT_ARG = "messagecount";
    private static final String MESSAGE_SIZE_ARG = "size";
    private static final String REPETITIONS_ARG = "repetitions";
    private static final String PERSISTENT_ARG = "persistent";
    private static final String RANDOM_ARG = "random";
    private static final String TIMEOUT_ARG = "timeout";
    private static final String DELAYCLOSE_ARG = "delayclose";
    private static final String REPORT_MOD_ARG = "reportmod";
    private static final String TRANSACTED_ARG = "transacted";
    private static final String TX_BATCH_ARG = "txbatch";
    private static final String CLOSE_SESSION_ARG = "closeSession";
    private static final String PAUSE_AFTER_CONNECTION_OPEN_ARG = "pauseAfterConnectionOpen";
    private static final String PAUSE_BEFORE_CONNECTION_CLOSE_ARG = "pauseBeforeConnectionClose";
    private static final String CLOSE_PRODUCERS_ARG = "closeProducers";
    private static final String PAUSE_AFTER_SESSION_CREATE_ARG = "pauseAfterSessionCreate";
    private static final String PAUSE_BEFORE_SESSION_CLOSE_ARG = "pauseBeforeSessionClose";
    private static final String SESSION_ITERATIONS_ARG = "sessionIterations";
    private static final String PAUSE_BEFORE_MESSAGING_ARG = "pauseBeforeMessaging";
    private static final String PAUSE_AFTER_MESSAGING_ARG = "pauseAfterMessaging";
    private static final String MESSAGING_ITERATIONS_ARG = "messagingIterations";
    private static final String CONSUMER_MESSAGE_COUNT = "consumerMessageCount";
    private static final String CONSUMER_SELECTOR = "selector";

    private static final String JNDI_PROPERTIES_DEFAULT = "stress-test-client-qpid-jms-client-0-x.properties";
    private static final String JNDI_DESTINATION_DEFAULT = "stressTestQueue";
    private static final String JNDI_CONNECTION_FACTORY_DEFAULT = "qpidConnectionFactory";
    private static final String CONNECTIONS_DEFAULT = "1";
    private static final String SESSIONS_DEFAULT = "1";
    private static final String CONSUME_IMMEDIATELY_DEFAULT = "true";
    private static final String CLOSE_CONSUMERS_DEFAULT = "true";
    private static final String PRODUCERS_DEFAULT = "1";
    private static final String CONSUMERS_DEFAULT = "1";
    private static final String MESSAGE_COUNT_DEFAULT = "1";
    private static final String MESSAGE_SIZE_DEFAULT = "256";
    private static final String REPETITIONS_DEFAULT = "1";
    private static final String PERSISTENT_DEFAULT = "false";
    private static final String RANDOM_DEFAULT = "true";
    private static final String TIMEOUT_DEFAULT = "30000";
    private static final String DELAYCLOSE_DEFAULT = "0";
    private static final String REPORT_MOD_DEFAULT = "1";
    private static final String TRANSACTED_DEFAULT = "false";
    private static final String TX_BATCH_DEFAULT = "1";
    private static final String CLOSE_SESSION_DEFAULT = "false";
    private static final String PAUSE_AFTER_CONNECTION_OPEN_DEFAULT = "false";
    private static final String PAUSE_BEFORE_CONNECTION_CLOSE_DEFAULT = "false";
    private static final String CLOSE_PRODUCERS_DEFAULT = "false";
    private static final String PAUSE_AFTER_SESSION_CREATE_DEFAULT = "false";
    private static final String PAUSE_BEFORE_SESSION_CLOSE_DEFAULT = "false";
    private static final String SESSION_ITERATIONS_DEFAULT = "1";
    private static final String PAUSE_BEFORE_MESSAGING_DEFAULT= "false";
    private static final String PAUSE_AFTER_MESSAGING_DEFAULT= "false";
    private static final String MESSAGING_ITERATIONS_DEFAULT = "1";
    private static final String CONSUMERS_SELECTOR_DEFAULT = "";
    private static final String CLASS = "StressTestClient";
    private static final String DISABLE_MESSAGE_ID_DEFAULT = Boolean.FALSE.toString();
    private static final String DISABLE_MESSAGE_TIMESTAMP_DEFAULT = Boolean.FALSE.toString();


    public static void main(String[] args)
    {
        Map<String,String> options = new HashMap<>();
        options.put(JNDI_PROPERTIES_ARG, JNDI_PROPERTIES_DEFAULT);
        options.put(JNDI_DESTINATION_ARG, JNDI_DESTINATION_DEFAULT);
        options.put(JNDI_CONNECTION_FACTORY_ARG, JNDI_CONNECTION_FACTORY_DEFAULT);
        options.put(CONNECTIONS_ARG, CONNECTIONS_DEFAULT);
        options.put(SESSIONS_ARG, SESSIONS_DEFAULT);
        options.put(CONSUME_IMMEDIATELY_ARG, CONSUME_IMMEDIATELY_DEFAULT);
        options.put(PRODUCERS_ARG, PRODUCERS_DEFAULT);
        options.put(CONSUMERS_ARG, CONSUMERS_DEFAULT);
        options.put(DISABLE_MESSAGE_ID_ARG, DISABLE_MESSAGE_ID_DEFAULT);
        options.put(DISABLE_MESSAGE_TIMESTAMP_ARG, DISABLE_MESSAGE_TIMESTAMP_DEFAULT);
        options.put(CLOSE_CONSUMERS_ARG, CLOSE_CONSUMERS_DEFAULT);
        options.put(MESSAGE_COUNT_ARG, MESSAGE_COUNT_DEFAULT);
        options.put(MESSAGE_SIZE_ARG, MESSAGE_SIZE_DEFAULT);
        options.put(REPETITIONS_ARG, REPETITIONS_DEFAULT);
        options.put(PERSISTENT_ARG, PERSISTENT_DEFAULT);
        options.put(RANDOM_ARG, RANDOM_DEFAULT);
        options.put(TIMEOUT_ARG, TIMEOUT_DEFAULT);
        options.put(DELAYCLOSE_ARG, DELAYCLOSE_DEFAULT);
        options.put(REPORT_MOD_ARG, REPORT_MOD_DEFAULT);
        options.put(TRANSACTED_ARG, TRANSACTED_DEFAULT);
        options.put(TX_BATCH_ARG, TX_BATCH_DEFAULT);
        options.put(CLOSE_SESSION_ARG, CLOSE_SESSION_DEFAULT);
        options.put(PAUSE_AFTER_CONNECTION_OPEN_ARG, PAUSE_AFTER_CONNECTION_OPEN_DEFAULT);
        options.put(PAUSE_BEFORE_CONNECTION_CLOSE_ARG, PAUSE_BEFORE_CONNECTION_CLOSE_DEFAULT);
        options.put(CLOSE_PRODUCERS_ARG, CLOSE_PRODUCERS_DEFAULT);
        options.put(PAUSE_AFTER_SESSION_CREATE_ARG, PAUSE_AFTER_SESSION_CREATE_DEFAULT);
        options.put(PAUSE_BEFORE_SESSION_CLOSE_ARG, PAUSE_BEFORE_SESSION_CLOSE_DEFAULT);
        options.put(SESSION_ITERATIONS_ARG, SESSION_ITERATIONS_DEFAULT);
        options.put(PAUSE_BEFORE_MESSAGING_ARG, PAUSE_BEFORE_MESSAGING_DEFAULT);
        options.put(PAUSE_AFTER_MESSAGING_ARG, PAUSE_AFTER_MESSAGING_DEFAULT);
        options.put(MESSAGING_ITERATIONS_ARG, MESSAGING_ITERATIONS_DEFAULT);
        options.put(CONSUMER_SELECTOR, CONSUMERS_SELECTOR_DEFAULT);
        options.put(CONSUMER_MESSAGE_COUNT, "");

        if(args.length == 1 &&
                (args[0].equals("-h") || args[0].equals("--help") || args[0].equals("help")))
        {
            System.out.println("arg=value options: \n" + options.keySet());
            return;
        }

        parseArgumentsIntoConfig(options, args);

        StressTestClient testClient = new StressTestClient();
        testClient.runTest(options);
    }

    private static void parseArgumentsIntoConfig(Map<String, String> initialValues, String[] args)
    {
        for(String arg: args)
        {
            String[] splitArg = arg.split("=");
            if(splitArg.length != 2)
            {
                throw new IllegalArgumentException("arguments must have format <name>=<value>: " + arg);
            }

            if(initialValues.put(splitArg[0], splitArg[1]) == null)
            {
                throw new IllegalArgumentException("not a valid configuration property: " + arg);
            }
        }
    }

    private void runTest(Map<String,String> options)
    {
        String jndiProperties = options.get(JNDI_PROPERTIES_ARG);
        int numConnections = Integer.parseInt(options.get(CONNECTIONS_ARG));
        int numSessions = Integer.parseInt(options.get(SESSIONS_ARG));
        int numProducers = Integer.parseInt(options.get(PRODUCERS_ARG));
        int numConsumers = Integer.parseInt(options.get(CONSUMERS_ARG));
        boolean closeConsumers = Boolean.valueOf(options.get(CLOSE_CONSUMERS_ARG));
        boolean disableMessageTimestamp = Boolean.valueOf(options.get(DISABLE_MESSAGE_TIMESTAMP_ARG));
        boolean disableMessageID = Boolean.valueOf(options.get(DISABLE_MESSAGE_ID_ARG));
        boolean consumeImmediately = Boolean.valueOf(options.get(CONSUME_IMMEDIATELY_ARG));
        int numMessage = Integer.parseInt(options.get(MESSAGE_COUNT_ARG));
        int messageSize = Integer.parseInt(options.get(MESSAGE_SIZE_ARG));
        int repetitions = Integer.parseInt(options.get(REPETITIONS_ARG));
        String destinationString = options.get(JNDI_DESTINATION_ARG);
        String connectionFactoryString = options.get(JNDI_CONNECTION_FACTORY_ARG);
        int deliveryMode = Boolean.valueOf(options.get(PERSISTENT_ARG)) ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT;
        boolean random = Boolean.valueOf(options.get(RANDOM_ARG));
        long recieveTimeout = Long.parseLong(options.get(TIMEOUT_ARG));
        long delayClose = Long.parseLong(options.get(DELAYCLOSE_ARG));
        int reportingMod = Integer.parseInt(options.get(REPORT_MOD_ARG));
        boolean transacted = Boolean.valueOf(options.get(TRANSACTED_ARG));
        int txBatch = Integer.parseInt(options.get(TX_BATCH_ARG));
        boolean closeSession = Boolean.valueOf(options.get(CLOSE_SESSION_ARG));
        boolean pauseAfterConnectionOpen = Boolean.valueOf(options.get(PAUSE_AFTER_CONNECTION_OPEN_ARG));
        boolean pauseBeforeConnectionClose = Boolean.valueOf(options.get(PAUSE_BEFORE_CONNECTION_CLOSE_ARG));
        boolean closeProducers = Boolean.valueOf(options.get(CLOSE_PRODUCERS_ARG));
        boolean pauseAfterSessionCreate = Boolean.valueOf(options.get(PAUSE_AFTER_SESSION_CREATE_ARG));
        boolean pauseBeforeSessionClose = Boolean.valueOf(options.get(PAUSE_BEFORE_SESSION_CLOSE_ARG));
        int sessionIterations = Integer.parseInt(options.get(SESSION_ITERATIONS_ARG));
        boolean pauseBeforeMessaging = Boolean.valueOf(options.get(PAUSE_BEFORE_MESSAGING_ARG));
        boolean pauseAfterMessaging = Boolean.valueOf(options.get(PAUSE_AFTER_MESSAGING_ARG));
        int messagingIterations = Integer.parseInt(options.get(MESSAGING_ITERATIONS_ARG));
        String consumerSelector =  options.get(CONSUMER_SELECTOR);
        int consumerMessageCount = !"".equals(options.get(CONSUMER_MESSAGE_COUNT)) ?
                Integer.parseInt(options.get(CONSUMER_MESSAGE_COUNT)) : numMessage;

        System.out.println(CLASS + ": Using options: " + options);

        System.out.println(CLASS + ": Creating message payload of " + messageSize + " (bytes)");
        byte[] sentBytes = generateMessage(random, messageSize);

        try
        {
            // Load JNDI properties
            Context ctx = getInitialContext(jndiProperties);
            final ConnectionFactory conFac = (ConnectionFactory) ctx.lookup(connectionFactoryString);

            //ensure the queue to be used exists and is bound
            Destination destination = (Destination) ctx.lookup(destinationString);

            System.out.println(CLASS + ": Destination: " + destination);

            Connection startupConn = null;
            try
            {
                startupConn = conFac.createConnection();
                Session startupSess = startupConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
                MessageConsumer startupConsumer = startupSess.createConsumer(destination);
                startupConsumer.close();
                startupSess.close();
            }
            finally
            {
                if (startupConn != null)
                {
                    startupConn.close();
                }
            }

            for(int rep = 1 ; rep <= repetitions; rep++)
            {
                List<Connection> connectionList = new ArrayList<>();

                for (int co= 1; co<= numConnections ; co++)
                {
                    if( co % reportingMod == 0)
                    {
                        System.out.println(CLASS + ": Creating connection " + co);
                    }
                    Connection conn = conFac.createConnection();
                    conn.setExceptionListener(jmse ->
                                              {
                                                  System.err.println(CLASS + ": The sample received an exception through the ExceptionListener");
                                                  jmse.printStackTrace();
                                                  System.exit(0);
                                              });

                    connectionList.add(conn);
                    conn.start();

                    if (pauseAfterConnectionOpen)
                    {
                        System.out.println(String.format("Connection %d is open. Press any key to continue...", co));
                        System.in.read();
                    }

                    for (int se= 1; se<= numSessions ; se++)
                    {
                        if( se % reportingMod == 0)
                        {
                            System.out.println(CLASS + ": Creating Session " + se);
                        }
                        try
                        {
                            Session sess;
                            if(transacted)
                            {
                                sess = conn.createSession(true, Session.SESSION_TRANSACTED);
                            }
                            else
                            {
                                sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
                            }

                            if (pauseAfterSessionCreate)
                            {
                                System.out.println(String.format(
                                        "Session %d is created on connection %d. Press any key to continue...",
                                        se,
                                        co));
                                System.in.read();
                            }

                            final Message message;
                            if (messageSize > 0)
                            {
                                message = sess.createBytesMessage();
                                ((BytesMessage) message).writeBytes(sentBytes);
                            }
                            else
                            {
                                message = sess.createMessage();
                            }


                            if(!random && numMessage == 1 && numSessions == 1 && numConnections == 1 && repetitions == 1)
                            {
                                //null the array to save memory
                                sentBytes = null;
                            }

                            for (int sessionIteration = 1; sessionIteration <= sessionIterations; sessionIteration++)
                            {
                                if (sessionIterations > 1 && sessionIteration % reportingMod == 0)
                                {
                                    System.out.println(CLASS + ": Session iteration " + sessionIteration);
                                }

                                MessageConsumer consumer = null;
                                MessageConsumer[] consumers = new MessageConsumer[numConsumers];
                                for (int cns = 1; cns <= numConsumers; cns++)
                                {
                                    if (cns % reportingMod == 0)
                                    {
                                        System.out.println(CLASS + ": Creating Consumer " + cns);
                                    }
                                    consumer = sess.createConsumer(destination, consumerSelector);
                                    consumers[cns - 1] = consumer;
                                }

                                MessageProducer[] producers = new MessageProducer[numProducers];
                                for (int pr = 1; pr <= numProducers; pr++)
                                {
                                    if (pr % reportingMod == 0)
                                    {
                                        System.out.println(CLASS + ": Creating Producer " + pr);
                                    }
                                    producers[pr - 1] = sess.createProducer(destination);

                                    if (disableMessageID)
                                    {
                                        producers[pr - 1].setDisableMessageID(true);
                                    }

                                    if (disableMessageTimestamp)
                                    {
                                        producers[pr - 1].setDisableMessageTimestamp(true);
                                    }
                                }

                                if (pauseBeforeMessaging)
                                {
                                    System.out.println("Consumer(s)/Producer(s) created. Press any key to continue...");
                                    System.in.read();
                                }

                                for (int iteration = 1; iteration <= messagingIterations; iteration++)
                                {
                                    if (messagingIterations > 1 && iteration % reportingMod == 0)
                                    {
                                        System.out.println(CLASS + ": Iteration " + iteration);
                                    }

                                    for (int pr = 1; pr <= numProducers; pr++)
                                    {
                                        MessageProducer prod = producers[pr - 1];
                                        for (int me = 1; me <= numMessage; me++)
                                        {
                                            int messageNumber = (iteration - 1) * numProducers * numMessage
                                                                + (pr - 1) * numMessage + (me - 1);
                                            if (messageNumber % reportingMod == 0)
                                            {
                                                System.out.println(CLASS + ": Sending Message " + messageNumber);
                                            }
                                            message.setIntProperty("index", me - 1);
                                            prod.send(message, deliveryMode,
                                                      Message.DEFAULT_PRIORITY,
                                                      Message.DEFAULT_TIME_TO_LIVE);
                                            if (sess.getTransacted() && me % txBatch == 0)
                                            {
                                                sess.commit();
                                            }
                                        }
                                    }

                                    if (numConsumers == 1 && consumeImmediately)
                                    {
                                        for (int cs = 1; cs <= consumerMessageCount; cs++)
                                        {
                                            if (cs % reportingMod == 0)
                                            {
                                                System.out.println(CLASS + ": Consuming Message " + cs);
                                            }
                                            Message msg = consumer.receive(recieveTimeout);

                                            if (sess.getTransacted() && cs % txBatch == 0)
                                            {
                                                sess.commit();
                                            }

                                            if (msg == null)
                                            {
                                                throw new RuntimeException(
                                                        "Expected message not received in allowed time: "
                                                        + recieveTimeout);
                                            }

                                            if (messageSize > 0)
                                            {
                                                validateReceivedMessageContent(sentBytes,
                                                                               (BytesMessage) msg, random, messageSize);
                                            }
                                        }
                                    }
                                }

                                if (pauseAfterMessaging)
                                {
                                    System.out.println("Messaging operations are completed. Press any key to continue...");
                                    System.in.read();
                                }

                                if (closeProducers)
                                {
                                    for (MessageProducer messageProducer : producers)
                                    {
                                        messageProducer.close();
                                    }
                                }

                                if (closeConsumers)
                                {
                                    for (MessageConsumer messageConsumer : consumers)
                                    {
                                        messageConsumer.close();
                                    }
                                }

                            }

                            if (pauseBeforeSessionClose)
                            {
                                System.out.println(String.format(
                                        "Session %d on connection %d is about to be closed. Press any key to continue...",
                                        se,
                                        co));
                                System.in.read();
                            }

                            if (closeSession)
                            {
                                sess.close();
                            }
                        }
                        catch (Exception exp)
                        {
                            System.err.println(CLASS + ": Caught an Exception: " + exp);
                            exp.printStackTrace();
                        }

                    }
                }

                if(numConsumers == -1 && !consumeImmediately)
                {
                    System.out.println(CLASS + ": Consuming left over messages, using receive timeout:" + recieveTimeout);

                    Connection conn = conFac.createConnection();
                    Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    MessageConsumer consumer = sess.createConsumer(destination);
                    conn.start();

                    int count = 0;
                    while(true)
                    {
                        Message msg = consumer.receive(recieveTimeout);

                        if(msg == null)
                        {
                            System.out.println(CLASS + ": Received " + count + " messages");
                            break;
                        }
                        else
                        {
                            count++;
                        }

                        if (messageSize > 0)
                        {
                            validateReceivedMessageContent(sentBytes, (BytesMessage) msg, random, messageSize);
                        }
                    }

                    consumer.close();
                    sess.close();
                    conn.close();
                }

                if(delayClose > 0)
                {
                    System.out.println(CLASS + ": Delaying closing connections: " + delayClose);
                    Thread.sleep(delayClose);
                }

                // Close the connections to the server
                System.out.println(CLASS + ": Closing connections");

                for(int connection = 0 ; connection < connectionList.size() ; connection++)
                {
                    if( (connection+1) % reportingMod == 0)
                    {
                        System.out.println(CLASS + ": Closing connection " + (connection+1));
                    }
                    Connection c = connectionList.get(connection);

                    if (pauseBeforeConnectionClose)
                    {
                        System.out.println(String.format(
                                "Connection %d is about to be closed. Press any key to continue...",
                                connection));
                        System.in.read();
                    }
                    c.close();
                }

                // Close the JNDI reference
                System.out.println(CLASS + ": Closing JNDI context");
                ctx.close();
            }
        }
        catch (Exception exp)
        {
            System.err.println(CLASS + ": Caught an Exception: " + exp);
            exp.printStackTrace();
        }
    }

    private Context getInitialContext(final String jndiProperties) throws IOException, NamingException
    {
        Properties properties = new Properties();
        try(InputStream is = this.getClass().getClassLoader().getResourceAsStream(jndiProperties))
        {
            if (is != null)
            {
                properties.load(is);
                return new InitialContext(properties);
            }
        }

        System.out.printf(CLASS + ": Failed to find '%s' on classpath, using fallback\n", jndiProperties);
        return new InitialContext();
    }


    private byte[] generateMessage(boolean random, int messageSize)
    {
        byte[] sentBytes = new byte[messageSize];
        if(random)
        {
            //fill the array with numbers from 0-9
            Random rand = new Random(System.currentTimeMillis());
            for(int r = 0 ; r < messageSize ; r++)
            {
                sentBytes[r] = (byte) (48 + rand.nextInt(10));
            }
        }
        else
        {
            //use sequential numbers from 0-9
            for(int r = 0 ; r < messageSize ; r++)
            {
                sentBytes[r] = (byte) (48 + (r % 10));
            }
        }
        return sentBytes;
    }


    private void validateReceivedMessageContent(byte[] sentBytes,
            BytesMessage msg, boolean random, int messageSize) throws JMSException
    {
        Long length = msg.getBodyLength();

        if(length != messageSize)
        {
            throw new RuntimeException("Incorrect number of bytes received");
        }

        byte[] recievedBytes = new byte[length.intValue()];
        msg.readBytes(recievedBytes);

        if(random)
        {
            if(!Arrays.equals(sentBytes, recievedBytes))
            {
                throw new RuntimeException("Incorrect value of bytes received");
            }
        }
        else
        {
            for(int r = 0 ; r < messageSize ; r++)
            {
                if(! (recievedBytes[r] == (byte) (48 + (r % 10))))
                {
                    throw new RuntimeException("Incorrect value of bytes received");
                }
            }
        }
    }
}

