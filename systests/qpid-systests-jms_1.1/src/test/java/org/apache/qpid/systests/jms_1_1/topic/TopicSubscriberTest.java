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
package org.apache.qpid.systests.jms_1_1.topic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.JmsTestBase;

public class TopicSubscriberTest extends JmsTestBase
{
    @Test
    public void messageDeliveredToAllSubscribers() throws Exception
    {
        Topic topic = createTopic(getTestName());
        final TopicConnection connection = getTopicConnection();
        try
        {
            final TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            final TopicPublisher producer = session.createPublisher(topic);
            final TopicSubscriber subscriber1 = session.createSubscriber(topic);
            assertEquals(topic.getTopicName(), subscriber1.getTopic().getTopicName(), "Unexpected subscriber1 topic");
            final TopicSubscriber subscriber2 = session.createSubscriber(topic);
            assertEquals(topic.getTopicName(), subscriber2.getTopic().getTopicName(), "Unexpected subscriber2 topic");

            connection.start();
            String messageText = "Test Message";
            producer.send(session.createTextMessage(messageText));

            final Message subscriber1Message = subscriber1.receive(getReceiveTimeout());
            final Message subscriber2Message = subscriber2.receive(getReceiveTimeout());

            assertTrue(subscriber1Message instanceof TextMessage, "TextMessage should be received  by subscriber1");
            assertEquals(messageText, ((TextMessage) subscriber1Message).getText());
            assertTrue(subscriber2Message instanceof TextMessage, "TextMessage should be received  by subscriber2");
            assertEquals(messageText, ((TextMessage) subscriber2Message).getText());
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void publishedMessageIsLostWhenSubscriberDisconnected() throws Exception
    {
        Topic topic = createTopic(getTestName());
        final Connection connection = getConnection();
        try
        {
            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final MessageProducer producer = session.createProducer(topic);
            final MessageConsumer subscriber = session.createConsumer(topic);
            connection.start();
            producer.send(session.createTextMessage("A"));

            final Message message1 = subscriber.receive(getReceiveTimeout());
            assertTrue(message1 instanceof TextMessage, "TextMessage should be received");
            assertEquals("A", ((TextMessage) message1).getText(), "Unexpected message received");

            subscriber.close();

            producer.send(session.createTextMessage("B"));
            final MessageConsumer subscriber2 = session.createConsumer(topic);
            producer.send(session.createTextMessage("C"));

            final Message message2 = subscriber2.receive(getReceiveTimeout());
            assertTrue(message2 instanceof TextMessage, "TextMessage should be received");
            assertEquals("C", ((TextMessage) message2).getText(), "Unexpected message received");
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void selector() throws Exception
    {
        Topic topic = createTopic(getTestName());
        final Connection connection = getConnection();
        try
        {
            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final MessageConsumer subscriber = session.createConsumer(topic, "id='B'");
            final MessageProducer producer = session.createProducer(topic);
            Message message1 = session.createMessage();
            message1.setStringProperty("id", "A");
            producer.send(message1);
            Message message2 = session.createMessage();
            message2.setStringProperty("id", "B");
            producer.send(message2);

            connection.start();
            final Message receivedMessage = subscriber.receive(getReceiveTimeout());
            assertNotNull(receivedMessage, "Message not received");
            assertEquals("B", receivedMessage.getStringProperty("id"), "Unexpected message received");
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void noLocal() throws Exception
    {
        Topic topic = createTopic(getTestName());
        final Connection connection = getConnection();
        try
        {
            final Connection connection2 = getConnection();
            try
            {
                final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                final MessageConsumer subscriber = session.createConsumer(topic, null, true);

                final Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
                final MessageProducer producer1 = session.createProducer(topic);
                final MessageProducer producer2 = session2.createProducer(topic);
                producer1.send(session.createTextMessage("A"));
                producer2.send(session2.createTextMessage("B"));

                connection.start();
                final Message receivedMessage = subscriber.receive(getReceiveTimeout());
                assertTrue(receivedMessage instanceof TextMessage, "TextMessage should be received");
                assertEquals("B", ((TextMessage) receivedMessage).getText(), "Unexpected message received");
            }
            finally
            {
                connection2.close();
            }
        }
        finally
        {
            connection.close();
        }
    }
}
