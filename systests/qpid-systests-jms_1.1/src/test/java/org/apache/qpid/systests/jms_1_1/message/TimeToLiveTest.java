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

package org.apache.qpid.systests.jms_1_1.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicSubscriber;

import org.junit.Test;

import org.apache.qpid.systests.JmsTestBase;

public class TimeToLiveTest extends JmsTestBase
{
    @Test
    public void testPassiveTTL() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        long timeToLiveMillis = getReceiveTimeout();
        try
        {
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageProducer producer = session.createProducer(queue);
            producer.setTimeToLive(timeToLiveMillis);
            producer.send(session.createTextMessage("A"));
            producer.setTimeToLive(0);
            producer.send(session.createTextMessage("B"));
            session.commit();

            Thread.sleep(timeToLiveMillis);

            MessageConsumer consumer = session.createConsumer(queue);
            connection.start();
            Message message = consumer.receive(getReceiveTimeout());

            assertTrue("TextMessage should be received", message instanceof TextMessage);
            assertEquals("Unexpected message received", "B", ((TextMessage)message).getText());
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testActiveTTL() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        long timeToLiveMillis = getReceiveTimeout() * 2;
        try
        {
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageProducer producer = session.createProducer(queue);
            producer.setTimeToLive(timeToLiveMillis);
            producer.send(session.createTextMessage("A"));
            producer.setTimeToLive(0);
            producer.send(session.createTextMessage("B"));
            session.commit();

            MessageConsumer consumer = session.createConsumer(queue);
            connection.start();
            Message message = consumer.receive(getReceiveTimeout());

            assertTrue("TextMessage should be received", message instanceof TextMessage);
            assertEquals("Unexpected message received", "A", ((TextMessage) message).getText());

            Thread.sleep(timeToLiveMillis);

            session.rollback();
            message = consumer.receive(getReceiveTimeout());

            assertTrue("TextMessage should be received after waiting for TTL", message instanceof TextMessage);
            assertEquals("Unexpected message received after waiting for TTL", "B", ((TextMessage) message).getText());
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testPassiveTTLWithDurableSubscription() throws Exception
    {
        long timeToLiveMillis = getReceiveTimeout() * 2;
        String subscriptionName = getTestName() + "_sub";
        Topic topic = createTopic(getTestName());
        TopicConnection connection = getTopicConnection();
        try
        {
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            TopicSubscriber durableSubscriber = session.createDurableSubscriber(topic, subscriptionName);
            MessageProducer producer = session.createProducer(topic);
            producer.setTimeToLive(timeToLiveMillis);
            producer.send(session.createTextMessage("A"));
            producer.setTimeToLive(0);
            producer.send(session.createTextMessage("B"));
            session.commit();

            connection.start();
            Message message = durableSubscriber.receive(getReceiveTimeout());

            assertTrue("TextMessage should be received", message instanceof TextMessage);
            assertEquals("Unexpected message received", "A", ((TextMessage)message).getText());

            Thread.sleep(timeToLiveMillis);

            session.rollback();
            message = durableSubscriber.receive(getReceiveTimeout());

            assertTrue("TextMessage should be received after waiting for TTL", message instanceof TextMessage);
            assertEquals("Unexpected message received after waiting for TTL", "B", ((TextMessage) message).getText());
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testActiveTTLWithDurableSubscription() throws Exception
    {
        long timeToLiveMillis = getReceiveTimeout();
        String subscriptionName = getTestName() + "_sub";
        Topic topic = createTopic(getTestName());
        TopicConnection connection = getTopicConnection();
        try
        {
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            TopicSubscriber durableSubscriber = session.createDurableSubscriber(topic, subscriptionName);
            MessageProducer producer = session.createProducer(topic);
            producer.setTimeToLive(timeToLiveMillis);
            producer.send(session.createTextMessage("A"));
            producer.setTimeToLive(0);
            producer.send(session.createTextMessage("B"));
            session.commit();

            Thread.sleep(timeToLiveMillis);

            connection.start();
            Message message = durableSubscriber.receive(getReceiveTimeout());

            assertTrue("TextMessage should be received", message instanceof TextMessage);
            assertEquals("Unexpected message received", "B", ((TextMessage)message).getText());
        }
        finally
        {
            connection.close();
        }
    }

}
