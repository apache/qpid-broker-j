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

import java.util.HashMap;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.qpid.systest.rest.RestTestHelper;
import org.apache.qpid.test.utils.QpidBrokerTestCase;

public class SharedSubscriptionTest extends QpidBrokerTestCase
{

    private RestTestHelper _restTestHelper;

    @Override
    public void setUp() throws Exception
    {
        getDefaultBrokerConfiguration().addHttpManagementConfiguration();
        super.setUp();
        _restTestHelper = new RestTestHelper(getDefaultBroker().getHttpPort());
    }

    public void testSharedNonDurableSubscription() throws Exception
    {
        Connection connection = getConnectionWithPrefetch(0);

        Session publishingSession = connection.createSession();
        Session subscriber1Session = connection.createSession();
        Session subscriber2Session = connection.createSession();

        String topicName = getTestName();
        Topic topic = publishingSession.createTopic("amq.direct/" + topicName);

        MessageConsumer consumer1 = subscriber1Session.createSharedConsumer(topic, "subscription");
        MessageConsumer consumer2 = subscriber2Session.createSharedConsumer(topic, "subscription");

        MessageProducer producer = publishingSession.createProducer(topic);

        sendMessage(publishingSession, topic, 2);

        connection.start();

        Message message1 = consumer1.receive(RECEIVE_TIMEOUT);
        Message message2 = consumer2.receive(RECEIVE_TIMEOUT);

        assertNotNull("Message 1 was not received", message1);
        assertNotNull("Message 2 was not received", message2);

        assertEquals("Unexpected index for message 1", 0, message1.getIntProperty(INDEX));
        assertEquals("Unexpected index for message 2", 1, message2.getIntProperty(INDEX));

        Message message3 = consumer1.receive(RECEIVE_TIMEOUT);
        Message message4 = consumer2.receive(RECEIVE_TIMEOUT);

        assertNull("Unexpected message received by first shared consumer", message3);
        assertNull("Unexpected message received by second shared consumer", message4);
    }

    public void testSharedDurableSubscription() throws Exception
    {
        Connection connection = getConnectionBuilder().setPrefetch(0).setClientId("myClientId").build();

        Session publishingSession = connection.createSession();
        Session subscriber1Session = connection.createSession();
        Session subscriber2Session = connection.createSession();

        String topicName = getTestName();
        Topic topic = publishingSession.createTopic("amq.direct/" + topicName);

        MessageConsumer consumer1 = subscriber1Session.createSharedDurableConsumer(topic, "subscription");
        MessageConsumer consumer2 = subscriber2Session.createSharedDurableConsumer(topic, "subscription");

        MessageProducer producer = publishingSession.createProducer(topic);

        sendMessage(publishingSession, topic, 4);

        connection.start();

        Message message1 = consumer1.receive(RECEIVE_TIMEOUT);
        Message message2 = consumer2.receive(RECEIVE_TIMEOUT);

        assertNotNull("Message 1 was not received", message1);
        assertNotNull("Message 2 was not received", message2);

        assertEquals("Unexpected index for message 1", 0, message1.getIntProperty(INDEX));
        assertEquals("Unexpected index for message 2", 1, message2.getIntProperty(INDEX));

        connection.close();

        if (isBrokerStorePersistent())
        {
            restartDefaultBroker();
        }

        connection = getConnectionBuilder().setPrefetch(0).setClientId("myClientId").build();
        subscriber1Session = connection.createSession();
        subscriber2Session = connection.createSession();

        consumer1 = subscriber1Session.createSharedDurableConsumer(topic, "subscription");
        consumer2 = subscriber2Session.createSharedDurableConsumer(topic, "subscription");

        connection.start();

        Message message3 = consumer1.receive(RECEIVE_TIMEOUT);
        Message message4 = consumer2.receive(RECEIVE_TIMEOUT);

        assertNotNull("Message 3 was not received", message3);
        assertNotNull("Message 4 was not received", message4);

        assertEquals("Unexpected index for message 3", 2, message3.getIntProperty(INDEX));
        assertEquals("Unexpected index for message 4", 3, message4.getIntProperty(INDEX));

        Message message5 = consumer1.receive(RECEIVE_TIMEOUT);
        Message message6 = consumer2.receive(RECEIVE_TIMEOUT);

        assertNull("Unexpected message received by first shared consumer", message5);
        assertNull("Unexpected message received by second shared consumer", message6);

    }

    public void testUnsubscribe() throws Exception
    {
        sharedDurableSubscriptionUnsubscribeTest("myClientId");
    }


    public void testUnsubscribeForGlobalSharedDurableSubscription() throws Exception
    {
        sharedDurableSubscriptionUnsubscribeTest(null);
    }

    private void sharedDurableSubscriptionUnsubscribeTest(final String clientId) throws Exception
    {
        Connection connection = getConnectionBuilder().setPrefetch(0).setClientId(clientId).build();
        Session session = connection.createSession();

        connection.start();

        String topicName = getTestName();
        Topic topic = session.createTopic("amq.direct/" + topicName);
        String subscriptionName = "testSharedSubscription";
        MessageConsumer consumer = session.createSharedDurableConsumer(topic, subscriptionName);

        Map<String, Object>
                statistics = _restTestHelper.getJsonAsMap("virtualhost/test/test/getStatistics?statistics=[\"queueCount\"]");
        int numberOfQueuesBeforeUnsubscribe = (int) statistics.get("queueCount");
        assertEquals("Unexpected number of Queues", 1, numberOfQueuesBeforeUnsubscribe);

        consumer.close();
        session.close();
        connection.close();

        if (isBrokerStorePersistent())
        {
            restartDefaultBroker();
        }

        connection = getConnectionBuilder().setPrefetch(0).setClientId(clientId).build();
        session = connection.createSession();
        session.unsubscribe(subscriptionName);

        statistics = _restTestHelper.getJsonAsMap("virtualhost/test/test/getStatistics?statistics=[\"queueCount\"]");
        int numberOfQueuesAfterUnsubscribe = (int) statistics.get("queueCount");
        assertEquals("Queue should be deleted", 0, numberOfQueuesAfterUnsubscribe);
    }

    public void testDurableSharedAndNonDurableSharedCanUseTheSameSubscriptionName() throws Exception
    {
        Connection connection = getConnectionWithPrefetch(0);

        Session publishingSession = connection.createSession();
        Session subscriberSession = connection.createSession();

        String topicName = getTestName();
        Topic topic = publishingSession.createTopic("amq.direct/" + topicName);
        MessageConsumer consumer1 = subscriberSession.createSharedDurableConsumer(topic, "testSharedSubscription");
        MessageConsumer consumer2 = subscriberSession.createSharedConsumer(topic, "testSharedSubscription");
        connection.start();

        sendMessage(publishingSession, topic, 1);

        Message message1 = consumer1.receive(RECEIVE_TIMEOUT);
        Message message2 = consumer2.receive(RECEIVE_TIMEOUT);

        assertNotNull("Message 1 was not received", message1);
        assertNotNull("Message 2 was not received", message2);

        assertEquals("Unexpected index for message 1", 0, message1.getIntProperty(INDEX));
        assertEquals("Unexpected index for message 2", 0, message2.getIntProperty(INDEX));
    }

    public void testGlobalAndNotGlobalCanUseTheSameSubscriptionName() throws Exception
    {
        Connection connection = getClientConnection(GUEST_USERNAME,  GUEST_PASSWORD, "testClientId");
        Connection connection2 = getClientConnection(GUEST_USERNAME,  GUEST_PASSWORD, null);

        Session publishingSession = connection.createSession();
        Session subscriber1Session = connection.createSession();
        Session subscriber2Session = connection2.createSession();

        String topicName = getTestName();
        Topic topic = publishingSession.createTopic("amq.direct/" + topicName);
        MessageConsumer consumer1 = subscriber1Session.createSharedConsumer(topic, "testSharedSubscription");
        MessageConsumer consumer2 = subscriber2Session.createSharedConsumer(topic, "testSharedSubscription");
        connection.start();
        connection2.start();

        sendMessage(publishingSession, topic, 1);

        Message message1 = consumer1.receive(RECEIVE_TIMEOUT);
        Message message2 = consumer2.receive(RECEIVE_TIMEOUT);

        assertNotNull("Message 1 was not received", message1);
        assertNotNull("Message 2 was not received", message2);

        assertEquals("Unexpected index for message 1", 0, message1.getIntProperty(INDEX));
        assertEquals("Unexpected index for message 2", 0, message2.getIntProperty(INDEX));
    }

    public void testTopicOrSelectorChange() throws Exception
    {
        final Map<String, String> options = new HashMap<>();
        options.put("jms.prefetchPolicy.all", "0");
        options.put("jms.clientID", null);
        Connection connection = getConnectionWithOptions(options);
        Connection connection2 = getConnectionWithOptions(options);

        Session publishingSession = connection.createSession();
        Session subscriber1Session = connection.createSession();
        Session subscriber2Session = connection2.createSession();

        String topicName = getTestName();
        Topic topic = publishingSession.createTopic("amq.direct/" + topicName);

        MessageConsumer consumer1 = subscriber1Session.createSharedDurableConsumer(topic, "subscription", "index>1");

        MessageProducer producer = publishingSession.createProducer(topic);

        sendMessage(publishingSession, topic, 4);

        connection.start();
        connection2.start();

        Message message1 = consumer1.receive(RECEIVE_TIMEOUT);
        assertNotNull("Message 1 was not received", message1);
        assertEquals("Unexpected index for message 1", 2, message1.getIntProperty(INDEX));

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
        MessageConsumer consumer2 = subscriber2Session.createSharedDurableConsumer(topic, "subscription", "index>2");

        Message message2 = consumer2.receive(RECEIVE_TIMEOUT);
        assertNull("No message should be received as re-subscribing with different topic or selector is equivalent to unsubscribe/subscribe", message2);

        sendMessage(publishingSession, topic, 4);

        Message message3 = consumer2.receive(RECEIVE_TIMEOUT);
        assertNotNull("Should receive message 3", message3);
        assertEquals("Unexpected index for message 3", 3, message3.getIntProperty(INDEX));
        consumer2.close();

        MessageConsumer consumer3 = subscriber2Session.createSharedDurableConsumer(topic2, "subscription", "index>2");
        Message message4 = consumer3.receive(RECEIVE_TIMEOUT);

        assertNull("No message should be received as re-subscribing with different topic or selector is equivalent to unsubscribe/subscribe", message4);

        sendMessage(publishingSession, topic2, 4);

        Message message5 = consumer3.receive(RECEIVE_TIMEOUT);
        assertEquals("Unexpected index for message 5", 3, message5.getIntProperty(INDEX));

    }

}
