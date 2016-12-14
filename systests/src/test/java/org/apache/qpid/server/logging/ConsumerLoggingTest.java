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
package org.apache.qpid.server.logging;

import java.io.IOException;
import java.util.List;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.qpid.server.model.Consumer;

/**
 * Subscription
 *
 * The Subscription test suite validates that the follow log messages as specified in the Functional Specification.
 *
 * This suite of tests validate that the Subscription messages occur correctly and according to the following format:
 *
 * SUB-1001 : Create : [Durable] [Arguments : <key=value>]
 * SUB-1002 : Close
 * SUB-1003 : State : <state>
 */
public class ConsumerLoggingTest extends AbstractTestLogging
{
    static final String SUB_PREFIX = "SUB-";

    private Connection _connection;
    private Session _session;
    private Queue _queue;
    private Topic _topic;

    @Override
    public void setUp() throws Exception
    {
        setSystemProperty(Consumer.SUSPEND_NOTIFICATION_PERIOD, "100");
        super.setUp();

        _connection = getConnection();

        _session = _connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        final String queueName = getTestQueueName() + "Queue";
        final String topicName = getTestQueueName() + "Topic";
        _queue = createTestQueue(_session, queueName);
        _topic = createTopic(_connection, topicName);

        //Remove broker startup logging messages
        _monitor.markDiscardPoint();
    }

    /**
     * Description:
     * When a Subscription is created it will be logged. This test validates that Subscribing to a transient queue is correctly logged.
     * Input:
     *
     * 1. Running Broker
     * 2. Create a new Subscription to a transient queue/topic.
     * Output:          6
     *
     * <date> SUB-1001 : Create
     *
     * Validation Steps:
     * 3. The SUB ID is correct
     *
     * @throws java.io.IOException    - if there is a problem getting the matches
     * @throws javax.jms.JMSException - if there is a problem creating the consumer
     */
    public void testSubscriptionCreate() throws JMSException, IOException
    {
        _session.createConsumer(_queue);

        //Validate

        //Ensure that we wait for the SUB log message
        waitAndFindMatches("SUB-1001");

        List<String> results = findMatches(SUB_PREFIX);

        assertEquals("Result set larger than expected.", 1, results.size());

        String log = getLogMessage(results, 0);

        validateMessageID("SUB-1001", log);

        assertEquals("Log Message not as expected", "Create", getMessageString(fromMessage(log)));
    }

    /**
     * Description:
     * The creation of a Durable Subscription, such as a JMS DurableTopicSubscriber will result in an extra Durable tag being included in the Create log message
     * Input:
     *
     * 1. Running Broker
     * 2. Creation of a JMS DurableTopicSubiber
     * Output:
     *
     * <date> SUB-1001 : Create : Durable
     *
     * Validation Steps:
     * 3. The SUB ID is correct
     * 4. The Durable tag is present in the message
     * NOTE: A Subscription is not Durable, the queue it consumes from is.
     *
     * @throws java.io.IOException    - if there is a problem getting the matches
     * @throws javax.jms.JMSException - if there is a problem creating the consumer
     */
    public void testSubscriptionCreateDurable() throws JMSException, IOException
    {
        _session.createDurableSubscriber(_topic, getName());

        //Validate
        //Ensure that we wait for the SUB log message
        waitAndFindMatches("SUB-1001");

        List<String> results = findMatches(SUB_PREFIX);

        assertEquals("Result set not as expected.", 1, results.size());

        String log = getLogMessage(results, 0);

        validateMessageID("SUB-1001", log);

        String message = getMessageString(fromMessage(log));
        assertTrue("Durable not on log message:" + message, message.contains("Durable"));
    }

    /**
     * Description:
     * The creation of a Subscriber with a JMS Selector will result in the Argument field being populated. These argument key/value pairs are then shown in the log message.
     * Input:
     *
     * 1. Running Broker
     * 2. Subscriber created with a JMS Selector.
     * Output:
     *
     * <date> SUB-1001 : Create : Arguments : <key=value>
     *
     * Validation Steps:
     * 3. The SUB ID is correct
     * 4. Argument tag is present in the message
     *
     * @throws java.io.IOException    - if there is a problem getting the matches
     * @throws javax.jms.JMSException - if there is a problem creating the consumer
     */
    public void testSubscriptionCreateWithArguments() throws JMSException, IOException
    {
        final String SELECTOR = "Selector='True'";
        _session.createConsumer(_queue, SELECTOR);

        //Validate

        //Ensure that we wait for the SUB log message
        waitAndFindMatches("SUB-1001");

        List<String> results = findMatches(SUB_PREFIX);

        assertEquals("Result set larger than expected.", 1, results.size());

        String log = getLogMessage(results, 0);

        validateMessageID("SUB-1001", log);

        String message = getMessageString(fromMessage(log));
        assertTrue("Selector not on log message:" + message, message.contains(SELECTOR));
    }

    /**
     * Description:
     * The final combination of SUB-1001 Create messages involves the creation of a Durable Subscription that also contains a set of Arguments, such as those provided via a JMS Selector.
     * Input:
     *
     * 1. Running Broker
     * 2. Java Client creates a Durable Subscription with Selector
     * Output:
     *
     * <date> SUB-1001 : Create : Durable Arguments : <key=value>
     *
     * Validation Steps:
     * 3. The SUB ID is correct
     * 4. The tag Durable is present in the message
     * 5. The Arguments are present in the message
     *
     * @throws java.io.IOException    - if there is a problem getting the matches
     * @throws javax.jms.JMSException - if there is a problem creating the consumer
     */
    public void testSubscriptionCreateDurableWithArguments() throws JMSException, IOException
    {
        final String SELECTOR = "Selector='True'";
        _session.createDurableSubscriber(_topic, getName(), SELECTOR, false);

        //Validate

        //Ensure that we wait for the SUB log message
        waitAndFindMatches("SUB-1001");

        List<String> results = findMatches(SUB_PREFIX);

        assertEquals("Result set larger than expected.", 1, results.size());

        String log = getLogMessage(results, 0);

        validateMessageID("SUB-1001", log);

        String message = getMessageString(fromMessage(log));
        assertTrue("Durable not on log message:" + message, message.contains("Durable"));
        assertTrue("Selector not on log message:" + message, message.contains(SELECTOR));
    }

    /**
     * Description:
     * When a Subscription is closed it will log this so that it can be correlated with the Create.
     * Input:
     *
     * 1. Running Broker
     * 2. Client with a subscription.
     * 3. The subscription is then closed.
     * Output:
     *
     * <date> SUB-1002 : Close
     *
     * Validation Steps:
     * 1. The SUB ID is correct
     * 2. There must be a SUB-1001 Create message preceding this message
     * 3. This must be the last message from the given Subscription
     *
     * @throws java.io.IOException    - if there is a problem getting the matches
     * @throws javax.jms.JMSException - if there is a problem creating the consumer
     */
    public void testSubscriptionClose() throws JMSException, IOException
    {
        _session.createConsumer(_queue).close();

        //Validate
        //Ensure that we wait for the SUB log message
        waitAndFindMatches("SUB-1002");

        List<String> results = findMatches(SUB_PREFIX);

        //3
        assertEquals("Result set larger than expected.", 2, results.size());

        // 2
        String log = getLogMessage(results, 0);
        validateMessageID("SUB-1001", log);
        // 1
        log = getLogMessage(results, 1);
        validateMessageID("SUB-1002", log);

        String message = getMessageString(fromMessage(log));
        assertEquals("Log message is not close", "Close", message);

    }

    /**
     * Description:
     * When a Subscription fills its prefetch it will become suspended. This
     * will be logged as a SUB-1003 message.
     * Input:
     *
     * 1. Running broker
     * 2. Message Producer to put more data on the queue than the client's prefetch
     * 3. Client that ensures that its prefetch becomes full
     * Output:
     *
     * <date> SUB-1003 : State : <state>
     *
     * Validation Steps:
     * 1. The SUB ID is correct
     * 2. The state is correct
     *
     * @throws java.io.IOException    - if there is a problem getting the matches
     * @throws javax.jms.JMSException - if there is a problem creating the consumer
     */
    public void testSubscriptionSuspend() throws Exception, IOException
    {
        //Close session with large prefetch
        _connection.close();
        int PREFETCH = 15;
        _connection = getConnectionWithPrefetch(PREFETCH);
        _session = _connection.createSession(true, Session.SESSION_TRANSACTED);


        MessageConsumer consumer = _session.createConsumer(_queue);

        _connection.start();

        //Start the dispatcher & Unflow the channel.
        consumer.receiveNoWait();

        //Fill the prefetch and two extra so that our receive bellow allows the
        // subscription to become active
        // Previously we set this to 17 so that it would return to a suspended
        // state. However, testing has shown that the state change can occur
        // sufficiently quickly that logging does not occur consistently enough
        // for testing.
        int SEND_COUNT = 16;
        sendMessage(_session, _queue, SEND_COUNT);
        _session.commit();

        Thread.sleep(2500l);

        _logger.debug("Looking for SUB-1003s");
        List<String> results = waitAndFindMatches("SUB-1003");

        assertTrue("Expected at least two suspension messages, but got " + results.size(), results.size() >= 2);

        // Retrieve the first message, and start the flow of messages
        Message msg = consumer.receive(1000);
        assertNotNull("Message not retrieved", msg);
        _session.commit();
        msg = consumer.receive(1000);
        assertNotNull("Message not retrieved", msg);
        _session.commit();

        int count = waitAndFindMatches("SUB-1003").size();
        Thread.sleep(2000l);
        assertEquals("More suspension messages were received unexpectedly", count, waitAndFindMatches("SUB-1003").size());

        _connection.close();
    }

}
