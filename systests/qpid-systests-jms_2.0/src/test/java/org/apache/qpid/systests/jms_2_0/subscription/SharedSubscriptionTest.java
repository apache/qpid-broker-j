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
 */

package org.apache.qpid.systests.jms_2_0.subscription;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.Topic;

import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.JmsTestBase;
import org.apache.qpid.systests.Utils;

public class SharedSubscriptionTest extends JmsTestBase
{
    @Test
    public void testSharedNonDurableSubscription() throws Exception
    {
        try (Connection connection = getConnectionBuilder().setPrefetch(0).build())
        {
            Session publishingSession = connection.createSession();
            Session subscriber1Session = connection.createSession();
            Session subscriber2Session = connection.createSession();

            String topicName = getTestName();
            Topic topic = publishingSession.createTopic("amq.direct/" + topicName);

            MessageConsumer consumer1 = subscriber1Session.createSharedConsumer(topic, "subscription");
            MessageConsumer consumer2 = subscriber2Session.createSharedConsumer(topic, "subscription");

            Utils.sendMessages(publishingSession, topic, 2);

            connection.start();

            Message message1 = consumer1.receive(getReceiveTimeout());
            Message message2 = consumer2.receive(getReceiveTimeout());

            assertNotNull(message1, "Message 1 was not received");
            assertNotNull(message2, "Message 2 was not received");

            assertEquals(0, message1.getIntProperty(Utils.INDEX), "Unexpected index for message 1");
            assertEquals(1, message2.getIntProperty(Utils.INDEX), "Unexpected index for message 2");

            Message message3 = consumer1.receive(getReceiveTimeout());
            Message message4 = consumer2.receive(getReceiveTimeout());

            assertNull(message3, "Unexpected message received by first shared consumer");
            assertNull(message4, "Unexpected message received by second shared consumer");
        }
    }

    @Test
    public void testSharedDurableSubscription() throws Exception
    {
        String topicName = getTestName();
        try (Connection connection = getConnectionBuilder().setPrefetch(0).setClientId("myClientId").build())
        {

            Session publishingSession = connection.createSession();
            Session subscriber1Session = connection.createSession();
            Session subscriber2Session = connection.createSession();

            Topic topic = publishingSession.createTopic("amq.direct/" + topicName);

            MessageConsumer consumer1 = subscriber1Session.createSharedDurableConsumer(topic, "subscription");
            MessageConsumer consumer2 = subscriber2Session.createSharedDurableConsumer(topic, "subscription");

            Utils.sendMessages(publishingSession, topic, 4);

            connection.start();

            Message message1 = consumer1.receive(getReceiveTimeout());
            Message message2 = consumer2.receive(getReceiveTimeout());

            assertNotNull(message1, "Message 1 was not received");
            assertNotNull(message2, "Message 2 was not received");

            assertEquals(0, message1.getIntProperty(Utils.INDEX), "Unexpected index for message 1");
            assertEquals(1, message2.getIntProperty(Utils.INDEX), "Unexpected index for message 2");
        }

        if (getBrokerAdmin().supportsRestart())
        {
            getBrokerAdmin().restart();
        }

        try (Connection connection = getConnectionBuilder().setPrefetch(0).setClientId("myClientId").build())
        {
            Session subscriber1Session = connection.createSession();
            Session subscriber2Session = connection.createSession();
            Topic topic = subscriber1Session.createTopic("amq.direct/" + topicName);
            MessageConsumer consumer1 = subscriber1Session.createSharedDurableConsumer(topic, "subscription");
            MessageConsumer consumer2 = subscriber2Session.createSharedDurableConsumer(topic, "subscription");

            connection.start();

            Message message3 = consumer1.receive(getReceiveTimeout());
            Message message4 = consumer2.receive(getReceiveTimeout());

            assertNotNull(message3, "Message 3 was not received");
            assertNotNull(message4, "Message 4 was not received");

            assertEquals(2, message3.getIntProperty(Utils.INDEX), "Unexpected index for message 3");
            assertEquals(3, message4.getIntProperty(Utils.INDEX), "Unexpected index for message 4");

            Message message5 = consumer1.receive(getReceiveTimeout());
            Message message6 = consumer2.receive(getReceiveTimeout());

            assertNull(message5, "Unexpected message received by first shared consumer");
            assertNull(message6, "Unexpected message received by second shared consumer");
        }
    }

    @Test
    public void testUnsubscribe() throws Exception
    {
        sharedDurableSubscriptionUnsubscribeTest("myClientId");
    }


    @Test
    public void testUnsubscribeForGlobalSharedDurableSubscription() throws Exception
    {
        sharedDurableSubscriptionUnsubscribeTest(null);
    }

    private void sharedDurableSubscriptionUnsubscribeTest(final String clientId) throws Exception
    {
        String subscriptionName = "testSharedSubscription";
        int numberOfQueuesBeforeTest = getQueueCount();
        String topicName = getTestName();
        try (Connection connection = getConnectionBuilder().setPrefetch(0).setClientId(clientId).build())
        {
            Session session = connection.createSession();

            connection.start();

            Topic topic = session.createTopic("amq.direct/" + topicName);
            MessageConsumer consumer = session.createSharedDurableConsumer(topic, subscriptionName);

            int numberOfQueuesBeforeUnsubscribe = getQueueCount();
            assertEquals(numberOfQueuesBeforeTest + 1, numberOfQueuesBeforeUnsubscribe,
                    "Unexpected number of Queues");

            consumer.close();
            session.close();
        }

        if (getBrokerAdmin().supportsRestart())
        {
            getBrokerAdmin().restart();
        }

        try (Connection connection = getConnectionBuilder().setPrefetch(0).setClientId(clientId).build())
        {
            final Session session = connection.createSession();
            session.unsubscribe(subscriptionName);

            int numberOfQueuesAfterUnsubscribe = getQueueCount();
            assertEquals(numberOfQueuesBeforeTest, numberOfQueuesAfterUnsubscribe, "Queue should be deleted");
        }
    }

    @Test
    public void testDurableSharedAndNonDurableSharedCanUseTheSameSubscriptionName() throws Exception
    {
        try (Connection connection = getConnectionBuilder().setPrefetch(0).build())
        {
            Session publishingSession = connection.createSession();
            Session subscriberSession = connection.createSession();

            String topicName = getTestName();
            Topic topic = publishingSession.createTopic("amq.direct/" + topicName);
            MessageConsumer consumer1 = subscriberSession.createSharedDurableConsumer(topic, "testSharedSubscription");
            MessageConsumer consumer2 = subscriberSession.createSharedConsumer(topic, "testSharedSubscription");
            connection.start();

            Utils.sendMessages(publishingSession, topic, 1);

            Message message1 = consumer1.receive(getReceiveTimeout());
            Message message2 = consumer2.receive(getReceiveTimeout());

            assertNotNull(message1, "Message 1 was not received");
            assertNotNull(message2, "Message 2 was not received");

            assertEquals(0, message1.getIntProperty(Utils.INDEX), "Unexpected index for message 1");
            assertEquals(0, message2.getIntProperty(Utils.INDEX), "Unexpected index for message 2");
        }
    }

    @Test
    public void testGlobalAndNotGlobalCanUseTheSameSubscriptionName() throws Exception
    {
        try (Connection connection =  getConnectionBuilder().setClientId("testClientId").build();
             Connection connection2 = getConnectionBuilder().setClientId(null).build())
        {
            Session publishingSession = connection.createSession();
            Session subscriber1Session = connection.createSession();
            Session subscriber2Session = connection2.createSession();

            String topicName = getTestName();
            Topic topic = publishingSession.createTopic("amq.direct/" + topicName);
            MessageConsumer consumer1 = subscriber1Session.createSharedConsumer(topic, "testSharedSubscription");
            MessageConsumer consumer2 = subscriber2Session.createSharedConsumer(topic, "testSharedSubscription");
            connection.start();
            connection2.start();

            Utils.sendMessages(publishingSession, topic, 1);

            Message message1 = consumer1.receive(getReceiveTimeout());
            Message message2 = consumer2.receive(getReceiveTimeout());

            assertNotNull(message1, "Message 1 was not received");
            assertNotNull(message2, "Message 2 was not received");

            assertEquals(0, message1.getIntProperty(Utils.INDEX), "Unexpected index for message 1");
            assertEquals(0, message2.getIntProperty(Utils.INDEX), "Unexpected index for message 2");
        }
    }

    @Test
    public void testTopicOrSelectorChange() throws Exception
    {
        try (Connection connection =  getConnectionBuilder().setPrefetch(0).setClientId(null).build();
             Connection connection2 = getConnectionBuilder().setPrefetch(0).setClientId(null).build())
        {
            Session publishingSession = connection.createSession();
            Session subscriber1Session = connection.createSession();
            Session subscriber2Session = connection2.createSession();

            String topicName = getTestName();
            Topic topic = publishingSession.createTopic("amq.direct/" + topicName);

            MessageConsumer consumer1 =
                    subscriber1Session.createSharedDurableConsumer(topic, "subscription", "index>1");

            Utils.sendMessages(publishingSession, topic, 4);

            connection.start();
            connection2.start();

            Message message1 = consumer1.receive(getReceiveTimeout());
            assertNotNull(message1, "Message 1 was not received");
            assertEquals(2, message1.getIntProperty(Utils.INDEX), "Unexpected index for message 1");

            try
            {
                subscriber2Session.createSharedDurableConsumer(topic, "subscription", "index>2");
                fail("Consumer should not be allowed to join shared subscription with different filter when there is an active subscriber");
            }
            catch (JMSException e)
            {
                // pass
            }
            Topic topic2 = publishingSession.createTopic("amq.direct/" + topicName + "2");
            try
            {
                subscriber2Session.createSharedDurableConsumer(topic2, "subscription", "index>1");
                fail("Consumer should not be allowed to join shared subscription with different topic when there is an active subscriber");
            }
            catch (JMSException e)
            {
                // pass
            }
            consumer1.close();
            MessageConsumer consumer2 =
                    subscriber2Session.createSharedDurableConsumer(topic, "subscription", "index>2");

            Message message2 = consumer2.receive(getReceiveTimeout());
            assertNull(message2,
                       "No message should be received as re-subscribing with different topic or selector is equivalent to unsubscribe/subscribe");

            Utils.sendMessages(publishingSession, topic, 4);

            Message message3 = consumer2.receive(getReceiveTimeout());
            assertNotNull(message3, "Should receive message 3");
            assertEquals(3, message3.getIntProperty(Utils.INDEX), "Unexpected index for message 3");
            consumer2.close();

            MessageConsumer consumer3 =
                    subscriber2Session.createSharedDurableConsumer(topic2, "subscription", "index>2");
            Message message4 = consumer3.receive(getReceiveTimeout());

            assertNull(message4,
                    "No message should be received as re-subscribing with different topic or selector is equivalent to unsubscribe/subscribe");

            Utils.sendMessages(publishingSession, topic2, 4);

            Message message5 = consumer3.receive(getReceiveTimeout());
            assertEquals(3, message5.getIntProperty(Utils.INDEX), "Unexpected index for message 5");
        }
    }

}
