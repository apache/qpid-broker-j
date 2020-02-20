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

import static org.apache.qpid.systests.JmsTestBase.DEFAULT_BROKER_CONFIG;
import static org.apache.qpid.systests.Utils.INDEX;
import static org.apache.qpid.systests.Utils.sendMessages;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.qpid.server.model.AlternateBinding;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.ExclusivityPolicy;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.systests.AmqpManagementFacade;
import org.apache.qpid.tests.utils.ConfigItem;

/**
 * Tests upgrading a BDB store on broker startup.
 * The store will then be used to verify that the upgrade is completed
 * properly and that once upgraded it functions as expected.
 * <p>
 * Store prepared using old client/broker with BDBStoreUpgradeTestPreparer.
 */
@ConfigItem(name = "qpid.initialConfigurationLocation", value = DEFAULT_BROKER_CONFIG )
public class BDBUpgradeTest extends UpgradeTestBase
{

    private static final String STRING_1024 = generateString(1024);
    private static final String STRING_1024_256 = generateString(1024 * 256);

    private static final String TOPIC_NAME = "myUpgradeTopic";
    private static final String SUB_NAME = "myDurSubName";
    private static final String SELECTOR_SUB_NAME = "mySelectorDurSubName";
    private static final String SELECTOR_TOPIC_NAME = "mySelectorUpgradeTopic";
    private static final String QUEUE_NAME = "myUpgradeQueue";
    private static final String PRIORITY_QUEUE_NAME = "myPriorityQueue";
    private static final String QUEUE_WITH_DLQ_NAME = "myQueueWithDLQ";

    @BeforeClass
    public static void verifyClient()
    {
        assumeThat(System.getProperty("virtualhostnode.type", "BDB"), is(equalTo("BDB")));
        assumeThat(getProtocol(), is(not(equalTo(Protocol.AMQP_1_0))));
    }

    /**
     * Test that the selector applied to the DurableSubscription was successfully
     * transferred to the new store, and functions as expected with continued use
     * by monitoring message count while sending new messages to the topic and then
     * consuming them.
     */
    @Test
    public void testSelectorDurability() throws Exception
    {
        TopicConnection connection = getTopicConnection();
        try
        {
            connection.start();

            TopicSession session = connection.createTopicSession(true, Session.SESSION_TRANSACTED);
            Topic topic = session.createTopic(SELECTOR_TOPIC_NAME);
            TopicPublisher publisher = session.createPublisher(topic);

            int index = ThreadLocalRandom.current().nextInt();
            Message messageA = session.createTextMessage("A");
            messageA.setIntProperty("ID", index);
            messageA.setStringProperty("testprop", "false");
            publisher.publish(messageA);

            Message messageB = session.createTextMessage("B");
            messageB.setIntProperty("ID", index);
            messageB.setStringProperty("testprop", "true");
            publisher.publish(messageB);

            session.commit();

            TopicSubscriber subscriber =
                    session.createDurableSubscriber(topic, SELECTOR_SUB_NAME, "testprop='true'", false);
            Message migrated = subscriber.receive(getReceiveTimeout());
            assertThat("Failed to receive migrated message", migrated, is(notNullValue()));

            Message received = subscriber.receive(getReceiveTimeout());
            session.commit();
            assertThat("Failed to receive published message", received, is(notNullValue()));
            assertThat("Message is not Text message", received, is(instanceOf(TextMessage.class)));
            assertThat("Unexpected text", ((TextMessage) received).getText(), is(equalTo("B")));
            assertThat("Unexpected index", received.getIntProperty("ID"), is(equalTo(index)));

            session.close();
        }
        finally
        {
            connection.close();
        }
    }

    /**
     * Test that the DurableSubscription without selector was successfully
     * transfered to the new store, and functions as expected with continued use.
     */
    @Test
    public void testDurableSubscriptionWithoutSelector() throws Exception
    {
        TopicConnection connection = getTopicConnection();
        try
        {
            connection.start();

            TopicSession session = connection.createTopicSession(true, Session.SESSION_TRANSACTED);

            Topic topic = session.createTopic(TOPIC_NAME);
            TopicPublisher publisher = session.createPublisher(topic);

            int index = ThreadLocalRandom.current().nextInt();
            Message messageA = session.createTextMessage("A");
            messageA.setIntProperty("ID", index);
            messageA.setStringProperty("testprop", "false");
            publisher.publish(messageA);

            Message messageB = session.createTextMessage("B");
            messageB.setIntProperty("ID", index);
            messageB.setStringProperty("testprop", "true");
            publisher.publish(messageB);

            session.commit();

            TopicSubscriber subscriber = session.createDurableSubscriber(topic, SUB_NAME);
            Message migrated = subscriber.receive(getReceiveTimeout());
            assertThat("Failed to receive migrated message", migrated, is(notNullValue()));

            Message receivedA = subscriber.receive(getReceiveTimeout());
            session.commit();
            assertThat("Failed to receive published message A", receivedA, is(notNullValue()));
            assertThat("Message A is not Text message", receivedA, is(instanceOf(TextMessage.class)));
            assertThat("Unexpected text for A", ((TextMessage) receivedA).getText(), is(equalTo("A")));
            assertThat("Unexpected index", receivedA.getIntProperty("ID"), is(equalTo(index)));

            Message receivedB = subscriber.receive(getReceiveTimeout());
            session.commit();
            assertThat("Failed to receive published message B", receivedB, is(notNullValue()));
            assertThat("Message B is not Text message", receivedB, is(instanceOf(TextMessage.class)));
            assertThat("Unexpected text for B", ((TextMessage) receivedB).getText(), is(equalTo("B")));
            assertThat("Unexpected index  for B", receivedB.getIntProperty("ID"), is(equalTo(index)));

            session.commit();
            session.close();
        }
        finally
        {
            connection.close();
        }
    }

    /**
     * Test that the backing queue for the durable subscription created was successfully
     * detected and set as being exclusive during the upgrade process, and that the
     * regular queue was not.
     */
    @Test
    public void testQueueExclusivity() throws Exception
    {
        Map<String, Object> result = getQueueAttributes(QUEUE_NAME);
        assertThat("Exclusive policy attribute is not found",
                   result.get(org.apache.qpid.server.model.Queue.EXCLUSIVE),
                   is(notNullValue()));
        assertThat("Queue should not have been marked as Exclusive during upgrade",
                   ExclusivityPolicy.valueOf(String.valueOf(result.get(org.apache.qpid.server.model.Queue.EXCLUSIVE))),
                   is(equalTo(ExclusivityPolicy.NONE)));

        result = getQueueAttributes("clientid" + ":" + SUB_NAME);
        assertThat("Exclusive policy attribute is not found",
                   result.get(org.apache.qpid.server.model.Queue.EXCLUSIVE),
                   is(notNullValue()));
        assertThat("DurableSubscription backing queue should have been marked as Exclusive during upgrade",
                   ExclusivityPolicy.valueOf(String.valueOf(result.get(org.apache.qpid.server.model.Queue.EXCLUSIVE))),
                   is(not(equalTo(ExclusivityPolicy.NONE))));
    }

    /**
     * Test that the upgraded queue continues to function properly when used
     * for persistent messaging and restarting the broker.
     * <p>
     * Sends the new messages to the queue BEFORE consuming those which were
     * sent before the upgrade. In doing so, this also serves to test that
     * the queue bindings were successfully transitioned during the upgrade.
     */
    @Test
    public void testBindingAndMessageDurability() throws Exception
    {
        Connection connection = getConnection();
        try
        {
            connection.start();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(QUEUE_NAME);

            sendMessages(connection, queue, 1);

            session.close();

            // Restart
            getBrokerAdmin().restart();

            // Drain the queue of all messages
            connection = getConnection();
            connection.start();
            consumeQueueMessages(connection, true);
        }
        finally
        {
            connection.close();
        }
    }

    /**
     * Test that all of the committed persistent messages previously sent to
     * the broker are properly received following update of the MetaData and
     * Content entries during the store upgrade process.
     */
    @Test
    public void testConsumptionOfUpgradedMessages() throws Exception
    {
        // Create a connection and start it
        Connection connection = getConnection();
        try
        {
            connection.start();

            consumeDurableSubscriptionMessages(connection, true);
            consumeDurableSubscriptionMessages(connection, false);
            consumeQueueMessages(connection, false);
        }
        finally
        {
            connection.close();
        }
    }

    /**
     * Tests store upgrade has maintained the priority queue configuration,
     * such that sending messages with priorities out-of-order and then consuming
     * them gets the messages back in priority order.
     */
    @Test
    public void testPriorityQueue() throws Exception
    {
        Connection connection = getConnection();
        try
        {
            connection.start();

            // send some messages to the priority queue
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(PRIORITY_QUEUE_NAME);
            MessageProducer producer = session.createProducer(queue);

            producer.send(session.createTextMessage("A"), DeliveryMode.PERSISTENT, 4, -1);
            producer.send(session.createTextMessage("B"), DeliveryMode.PERSISTENT, 1, -1);
            producer.send(session.createTextMessage("C"), DeliveryMode.PERSISTENT, 9, -1);
            session.close();

            //consume the messages, expected order: C, A, B.
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(queue);

            Message message1 = consumer.receive(getReceiveTimeout());
            assertThat("expected message was not received", message1, is(instanceOf(TextMessage.class)));
            assertThat(((TextMessage) message1).getText(), is(equalTo("C")));
            Message message2 = consumer.receive(getReceiveTimeout());
            assertThat("expected message was not received", message2, is(instanceOf(TextMessage.class)));
            assertThat(((TextMessage) message2).getText(), is(equalTo("A")));
            Message message3 = consumer.receive(getReceiveTimeout());
            assertThat("expected message was not received", message3, is(instanceOf(TextMessage.class)));
            assertThat(((TextMessage) message3).getText(), is(equalTo("B")));
        }
        finally
        {
            connection.close();
        }
    }

    /**
     * Test that the queue configured to have a DLQ was recovered and has the alternate exchange
     * and max delivery count, the DLE exists, the DLQ exists with no max delivery count, the
     * DLQ is bound to the DLE, and that the DLQ does not itself have a DLQ.
     * <p>
     * DLQs are NOT enabled at the virtualhost level, we are testing recovery of the arguments
     * that turned it on for this specific queue.
     */
    @Test
    public void testRecoveryOfQueueWithDLQ() throws Exception
    {
        //verify the DLE exchange exists, has the expected type, and a single binding for the DLQ
        Map<String, Object> exchangeAttributes = getExchangeAttributes(QUEUE_WITH_DLQ_NAME + "_DLE");
        assertThat("Wrong exchange type",
                   exchangeAttributes.get(Exchange.TYPE),
                   is(equalTo("org.apache.qpid.FanoutExchange")));

        @SuppressWarnings("unchecked")
        Collection<Map<String, Object>> bindings = (Collection<Map<String, Object>>) exchangeAttributes.get("bindings");
        assertThat(bindings.size(), is(equalTo(1)));
        for (Map<String, Object> binding : bindings)
        {
            String bindingKey = (String) binding.get("bindingKey");
            String queueName = (String) binding.get("destination");

            //Because its a fanout exchange, we just return a single '*' key with all bound queues
            assertThat("unexpected binding key", bindingKey, is(equalTo("dlq")));
            assertThat("unexpected queue name", queueName, is(equalTo(QUEUE_WITH_DLQ_NAME + "_DLQ")));
        }

        //verify the queue exists, has the expected alternate exchange and max delivery count
        Map<String, Object> queueAttributes = getQueueAttributes(QUEUE_WITH_DLQ_NAME);
        assertThat("Queue does not have the expected AlternateExchange",
                   queueAttributes.get(Exchange.ALTERNATE_BINDING),
                   is(equalTo(Collections.singletonMap(AlternateBinding.DESTINATION, QUEUE_WITH_DLQ_NAME + "_DLE"))));

        assertThat("Unexpected maximum delivery count",
                   ((Number) queueAttributes.get(org.apache.qpid.server.model.Queue.MAXIMUM_DELIVERY_ATTEMPTS)).intValue(),
                   is(equalTo(2)));

        Map<String, Object> dlQueueAttributes = getQueueAttributes(QUEUE_WITH_DLQ_NAME + "_DLQ");
        assertThat("Queue should not have an AlternateExchange",
                   dlQueueAttributes.get(org.apache.qpid.server.model.Queue.ALTERNATE_BINDING), is(nullValue()));
        assertThat("Unexpected maximum delivery count",
                   ((Number) dlQueueAttributes.get(org.apache.qpid.server.model.Queue.MAXIMUM_DELIVERY_ATTEMPTS)).intValue(),
                   is(equalTo(0)));

        try
        {
            String queueName = QUEUE_WITH_DLQ_NAME + "_DLQ_DLQ";
            getQueueAttributes(queueName);
            fail("A DLQ should not exist for the DLQ itself");
        }
        catch (AmqpManagementFacade.OperationUnsuccessfulException e)
        {
            assertThat(e.getStatusCode(), is(equalTo(404)));
        }
    }

    @Override
    String getOldStoreResourcePath()
    {
        return "upgrade/bdbstore-v4/test-store/00000000.jdb";
    }

    private Map<String, Object> getExchangeAttributes(final String exchangeName) throws Exception
    {
        return readEntityUsingAmqpManagement(exchangeName, "org.apache.qpid.Exchange", false);
    }

    private Map<String, Object> getQueueAttributes(final String queueName) throws Exception
    {
        return readEntityUsingAmqpManagement(queueName, "org.apache.qpid.Queue", false);
    }

    private void consumeDurableSubscriptionMessages(Connection connection, boolean selector) throws Exception
    {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = null;
        TopicSubscriber durSub = null;

        if (selector)
        {
            topic = session.createTopic(SELECTOR_TOPIC_NAME);
            durSub = session.createDurableSubscriber(topic, SELECTOR_SUB_NAME, "testprop='true'", false);
        }
        else
        {
            topic = session.createTopic(TOPIC_NAME);
            durSub = session.createDurableSubscriber(topic, SUB_NAME);
        }

        // Retrieve the matching message
        Message m = durSub.receive(getReceiveTimeout());
        assertThat("Failed to receive an expected message", m, is(notNullValue()));
        if (selector)
        {
            assertThat("Selector property did not match", m.getStringProperty("testprop"), is(equalTo("true")));
        }
        assertThat("ID property did not match", m.getIntProperty("ID"), is(equalTo(1)));
        assertThat("Message content was not as expected",
                   ((TextMessage) m).getText(),
                   is(equalTo(generateString(1024))));

        // Verify that no more messages are received
        m = durSub.receive(getReceiveTimeout());
        assertThat("No more messages should have been recieved", m, is(nullValue()));

        durSub.close();
        session.close();
    }

    private void consumeQueueMessages(Connection connection, boolean extraMessage) throws Exception
    {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(QUEUE_NAME);

        MessageConsumer consumer = session.createConsumer(queue);
        Message m;

        // Retrieve the initial pre-upgrade messages
        for (int i = 1; i <= 5; i++)
        {
            m = consumer.receive(getReceiveTimeout());
            assertThat("Failed to receive an expected message", m, is(notNullValue()));
            assertThat("ID property did not match", m.getIntProperty("ID"), is(equalTo(i)));
            assertThat("Message content was not as expected",
                       ((TextMessage) m).getText(),
                       is(equalTo(STRING_1024_256)));
        }
        for (int i = 1; i <= 5; i++)
        {
            m = consumer.receive(getReceiveTimeout());
            assertThat("Failed to receive an expected message", m, is(notNullValue()));
            assertThat("ID property did not match", m.getIntProperty("ID"), is(equalTo(i)));
            assertThat("Message content was not as expected", ((TextMessage) m).getText(), is((equalTo(STRING_1024))));
        }

        if (extraMessage)
        {
            //verify that the extra message is received
            m = consumer.receive(getReceiveTimeout());
            assertThat("Failed to receive an expected message", m, is(notNullValue()));
            assertThat("ID property did not match", m.getIntProperty(INDEX), is(equalTo(0)));
        }
        else
        {
            // Verify that no more messages are received
            m = consumer.receive(getReceiveTimeout());
            assertThat("No more messages should have been recieved", m, is(nullValue()));
        }

        consumer.close();
        session.close();
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

}
