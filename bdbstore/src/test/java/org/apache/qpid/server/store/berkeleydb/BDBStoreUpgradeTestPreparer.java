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
package org.apache.qpid.server.store.berkeleydb;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.queue.QueueArgumentsConverter;

/**
 * Prepares an older version brokers BDB store with the required
 * contents for use in the BDBStoreUpgradeTest.
 *
 * NOTE: Must be used with the equivalent older version client!
 *
 * The store will then be used to verify that the upgraded is
 * completed properly and that once upgraded it functions as
 * expected with the new broker.
 *
 */
public class BDBStoreUpgradeTestPreparer
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BDBStoreUpgradeTestPreparer.class);

    public static final String TOPIC_NAME="myUpgradeTopic";
    public static final String SUB_NAME="myDurSubName";
    public static final String SELECTOR_SUB_NAME="mySelectorDurSubName";
    public static final String SELECTOR_TOPIC_NAME="mySelectorUpgradeTopic";
    public static final String QUEUE_NAME="myUpgradeQueue";
    public static final String NON_DURABLE_QUEUE_NAME="queue-non-durable";

    public static final String PRIORITY_QUEUE_NAME="myPriorityQueue";
    public static final String QUEUE_WITH_DLQ_NAME="myQueueWithDLQ";
    public static final String NONEXCLUSIVE_WITH_ERRONEOUS_OWNER = "nonexclusive-with-erroneous-owner";
    private static final String SORTED_QUEUE_NAME = "mySortedQueue";
    private static final String SORT_KEY = "mySortKey";
    private static final String TEST_EXCHANGE_NAME = "myCustomExchange";
    private static final String TEST_QUEUE_NAME = "myCustomQueue";

    private static ConnectionFactory _connFac;
    private static TopicConnectionFactory _topciConnFac;

    /**
     * Create a BDBStoreUpgradeTestPreparer instance
     */
    public BDBStoreUpgradeTestPreparer () throws Exception
    {
        // The configuration for the Qpid InitialContextFactory has been supplied in
        // a jndi.properties file in the classpath, which results in it being picked
        // up automatically by the InitialContext constructor.
        Context context = new InitialContext();

        _connFac = (ConnectionFactory) context.lookup("myConnFactory");
        _topciConnFac = (TopicConnectionFactory) context.lookup("myTopicConnFactory");
    }

    private void prepareBroker() throws Exception
    {
        prepareQueues();
        prepareNonDurableQueue();
        prepareDurableSubscriptionWithSelector();
        prepareDurableSubscriptionWithoutSelector();
    }

    private void prepareNonDurableQueue() throws Exception
    {
        Connection connection = _connFac.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createQueue(String.format(
                "ADDR: %s; {create:always, node: {type: queue, durable: false, x-bindings:[{exchange: '%s', key: %s}]}}",
                NON_DURABLE_QUEUE_NAME,
                "amq.direct",
                NON_DURABLE_QUEUE_NAME));

        session.createConsumer(destination).close();
        MessageProducer messageProducer = session.createProducer(destination);
        sendMessages(session, messageProducer, destination, DeliveryMode.PERSISTENT, 1024, 3);
        connection.close();
    }

    /**
     * Prepare a queue for use in testing message and binding recovery
     * after the upgrade is performed.
     *
     * - Create a transacted session on the connection.
     * - Use a consumer to create the (durable by default) queue.
     * - Send 5 large messages to test (multi-frame) content recovery.
     * - Send 1 small message to test (single-frame) content recovery.
     * - Commit the session.
     * - Send 5 small messages to test that uncommitted messages are not recovered.
     *   following the upgrade.
     * - Close the session.
     */
    private void prepareQueues() throws Exception
    {
        // Create a connection
        Connection connection = _connFac.createConnection();
        connection.start();
        connection.setExceptionListener(e -> LOGGER.error("Error setting exception listener for connection", e));
        // Create a session on the connection, transacted to confirm delivery
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Queue queue = session.createQueue(String.format(
                "ADDR: %s; {create:always, node: {type: queue, durable: true, x-bindings:[{exchange: '%s', key: %s}]}}",
                QUEUE_NAME,
                "amq.direct",
                QUEUE_NAME));
        // Create a consumer to ensure the queue gets created
        // (and enter it into the store, as queues are made durable by default)
        MessageConsumer messageConsumer = session.createConsumer(queue);
        messageConsumer.close();

        // Create a Message priorityQueueProducer
        MessageProducer messageProducer = session.createProducer(queue);

        // Publish 5 persistent messages, 256k chars to ensure they are multi-frame
        sendMessages(session, messageProducer, queue, DeliveryMode.PERSISTENT, 256*1024, 5);
        // Publish 5 persistent messages, 1k chars to ensure they are single-frame
        sendMessages(session, messageProducer, queue, DeliveryMode.PERSISTENT, 1*1024, 5);

        session.commit();

        // Publish 5 persistent messages which will NOT be committed and so should be 'lost'
        sendMessages(session, messageProducer, queue, DeliveryMode.PERSISTENT, 1*1024, 5);
        messageProducer.close();
        session.close();

        session = connection.createSession(true,  Session.SESSION_TRANSACTED);
        // Create a priority queue on broker
        final Map<String,Object> priorityQueueArguments = new HashMap<String, Object>();
        priorityQueueArguments.put(QueueArgumentsConverter.X_QPID_PRIORITIES,10);
        Queue priorityQueue = createAndBindQueueOnBroker(session, PRIORITY_QUEUE_NAME, priorityQueueArguments);
        MessageProducer priorityQueueProducer = session.createProducer(priorityQueue);

        for (int msg = 0; msg < 5; msg++)
        {
            priorityQueueProducer.setPriority(msg % 10);
            Message message = session.createTextMessage(generateString(256*1024));
            message.setIntProperty("ID", msg);
            priorityQueueProducer.send(message);
        }
        session.commit();
        priorityQueueProducer.close();

        // Create a queue that has a DLQ
        final Map<String,Object> queueWithDLQArguments = new HashMap<String, Object>();
        queueWithDLQArguments.put("x-qpid-dlq-enabled", true);
        queueWithDLQArguments.put("x-qpid-maximum-delivery-count", 2);
        createAndBindQueueOnBroker(session, QUEUE_WITH_DLQ_NAME, queueWithDLQArguments);

        // Send message to the DLQ
        Queue dlq = session.createQueue("BURL:fanout://" + QUEUE_WITH_DLQ_NAME + "_DLE//does-not-matter");
        MessageProducer dlqMessageProducer = session.createProducer(dlq);
        sendMessages(session, dlqMessageProducer, dlq, DeliveryMode.PERSISTENT, 1*1024, 1);
        session.commit();

        session.createProducer(session.createTopic(
                String.format("BURL:direct://%s//?durable='true'", TEST_EXCHANGE_NAME))).close();

        Queue customQueue = createAndBindQueueOnBroker(session,
                                                       TEST_QUEUE_NAME,
                                                       Collections.emptyMap(),
                                                       TEST_EXCHANGE_NAME
                                                      );
        MessageProducer customQueueMessageProducer = session.createProducer(customQueue);
        sendMessages(session, customQueueMessageProducer, customQueue, DeliveryMode.PERSISTENT, 1*1024, 1);
        session.commit();
        customQueueMessageProducer.close();

        prepareSortedQueue(session, SORTED_QUEUE_NAME, SORT_KEY);

        session.close();
        connection.close();
    }

    private Queue createAndBindQueueOnBroker(Session session, String queueName, final Map<String, Object> arguments) throws Exception
    {
        return createAndBindQueueOnBroker(session, queueName, arguments, "amq.direct");
    }

    private Queue createAndBindQueueOnBroker(Session session,
                                             String queueName,
                                             final Map<String, Object> arguments,
                                             String exchangeName) throws Exception
    {
        final String declareArgs = arguments.entrySet()
                                            .stream()
                                            .map(entry -> String.format("'%s' : %s", entry.getKey(), entry.getValue()))
                                            .collect(Collectors.joining(",", "{", "}"));

        Queue queue = session.createQueue(String.format(
                "ADDR: %s; {create:always, node: {type: queue, x-bindings:[{exchange: '%s', key: %s}], x-declare: {arguments:%s}}}",
                queueName,
                exchangeName,
                queueName,
                declareArgs));
        return queue;
    }

    private void prepareSortedQueue(Session session, String queueName, String sortKey) throws Exception
    {
        final Map<String, Object> arguments = new HashMap<>();
        arguments.put("qpid.queue_sort_key", sortKey);
        Queue sortedQueue = createAndBindQueueOnBroker(session, queueName, arguments);

        MessageProducer messageProducer2 = session.createProducer(sortedQueue);

        String[] sortKeys = {"c", "b", "e", "a", "d"};
        for (int i = 1; i <= sortKeys.length; i++)
        {
            Message message = session.createTextMessage(generateString(256*1024));
            message.setIntProperty("ID", i);
            message.setStringProperty(sortKey, sortKeys[i - 1]);
            messageProducer2.send(message);
        }
        session.commit();
    }

    /**
     * Prepare a DurableSubscription backing queue for use in testing selector
     * recovery and queue exclusivity marking during the upgrade process.
     *
     * - Create a transacted session on the connection.
     * - Open and close a DurableSubscription with selector to create the backing queue.
     * - Send a message which matches the selector.
     * - Send a message which does not match the selector.
     * - Send a message which matches the selector but will remain uncommitted.
     * - Close the session.
     */
    private void prepareDurableSubscriptionWithSelector() throws Exception
    {

        // Create a connection
        TopicConnection connection = _topciConnFac.createTopicConnection();
        connection.start();
        connection.setExceptionListener(e -> LOGGER.error("Error setting exception listener for connection", e));
        // Create a session on the connection, transacted to confirm delivery
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Topic topic = session.createTopic(SELECTOR_TOPIC_NAME);

        // Create and register a durable subscriber with selector and then close it
        TopicSubscriber durSub1 = session.createDurableSubscriber(topic, SELECTOR_SUB_NAME,"testprop='true'", false);
        durSub1.close();

        // Create a publisher and send a persistent message which matches the selector
        // followed by one that does not match, and another which matches but is not
        // committed and so should be 'lost'
        TopicSession pubSession = connection.createTopicSession(true, Session.SESSION_TRANSACTED);
        TopicPublisher publisher = pubSession.createPublisher(topic);

        publishMessages(pubSession, publisher, topic, DeliveryMode.PERSISTENT, 1*1024, 1, "true");
        publishMessages(pubSession, publisher, topic, DeliveryMode.PERSISTENT, 1*1024, 1, "false");
        pubSession.commit();
        publishMessages(pubSession, publisher, topic, DeliveryMode.PERSISTENT, 1*1024, 1, "true");

        publisher.close();
        pubSession.close();
        connection.close();
    }

    /**
     * Prepare a DurableSubscription backing queue for use in testing use of
     * DurableSubscriptions without selectors following the upgrade process.
     *
     * - Create a transacted session on the connection.
     * - Open and close a DurableSubscription without selector to create the backing queue.
     * - Send a message which matches the subscription and commit session.
     * - Close the session.
     */
    private void prepareDurableSubscriptionWithoutSelector() throws Exception
    {
        // Create a connection
        TopicConnection connection = _topciConnFac.createTopicConnection();
        connection.start();
        connection.setExceptionListener(e -> LOGGER.error("Error setting exception listener for connection", e));
        // Create a session on the connection, transacted to confirm delivery
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Topic topic = session.createTopic(TOPIC_NAME);

        // Create and register a durable subscriber without selector and then close it
        TopicSubscriber durSub1 = session.createDurableSubscriber(topic, SUB_NAME);
        durSub1.close();

        // Create a publisher and send a persistent message which matches the subscription
        TopicSession pubSession = connection.createTopicSession(true, Session.SESSION_TRANSACTED);
        TopicPublisher publisher = pubSession.createPublisher(topic);

        publishMessages(pubSession, publisher, topic, DeliveryMode.PERSISTENT, 1*1024, 1, "indifferent");
        pubSession.commit();

        publisher.close();
        pubSession.close();
        connection.close();
    }

    private static void sendMessages(Session session, MessageProducer messageProducer,
            Destination dest, int deliveryMode, int length, int numMesages) throws JMSException
    {
        for (int i = 1; i <= numMesages; i++)
        {
            Message message = session.createTextMessage(generateString(length));
            message.setIntProperty("ID", i);
            messageProducer.send(message, deliveryMode, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        }
    }

    private static void publishMessages(Session session, TopicPublisher publisher,
            Destination dest, int deliveryMode, int length, int numMesages, String selectorProperty) throws JMSException
    {
        for (int i = 1; i <= numMesages; i++)
        {
            Message message = session.createTextMessage(generateString(length));
            message.setIntProperty("ID", i);
            message.setStringProperty("testprop", selectorProperty);
            publisher.publish(message, deliveryMode, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        }
    }

    /**
     * Generates a string of a given length consisting of the sequence 0,1,2,..,9,0,1,2.
     *
     * @param length number of characters in the string
     * @return string sequence of the given length
     */
    private static String generateString(int length)
    {
        char[] base_chars = new char[]{'0','1','2','3','4','5','6','7','8','9'};
        char[] chars = new char[length];
        for (int i = 0; i < (length); i++)
        {
            chars[i] = base_chars[i % 10];
        }
        return new String(chars);
    }

    /**
     * Run the preparation tool.
     * @param args Command line arguments.
     */
    public static void main(String[] args) throws Exception
    {
        BDBStoreUpgradeTestPreparer producer = new BDBStoreUpgradeTestPreparer();
        producer.prepareBroker();
    }
}
