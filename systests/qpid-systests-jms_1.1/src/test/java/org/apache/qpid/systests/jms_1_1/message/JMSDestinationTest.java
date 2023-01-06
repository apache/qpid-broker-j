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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;

import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.JmsTestBase;
import org.apache.qpid.systests.Utils;

public class JMSDestinationTest extends JmsTestBase
{
    @Test
    public void messageSentToQueueComesBackWithTheSameJMSDestination() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(queue);

            Utils.sendMessages(session, queue, 1);

            connection.start();

            Message receivedMessage = consumer.receive(getReceiveTimeout());
            assertNotNull(receivedMessage, "Message should not be null");

            Destination receivedDestination = receivedMessage.getJMSDestination();

            assertNotNull(receivedDestination, "JMSDestination should not be null");
            assertTrue(receivedDestination instanceof Queue, "Unexpected destination type");
            assertEquals(queue.getQueueName(), ((Queue) receivedDestination).getQueueName(),
                    "Unexpected destination name");
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void messageSentToTopicComesBackWithTheSameJMSDestination() throws Exception
    {
        Topic topic = createTopic(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(topic);

            Utils.sendMessages(session, topic, 1);

            connection.start();

            Message receivedMessage = consumer.receive(getReceiveTimeout());
            assertNotNull(receivedMessage, "Message should not be null");

            Destination receivedDestination = receivedMessage.getJMSDestination();

            assertNotNull(receivedDestination, "JMSDestination should not be null");
            assertTrue(receivedDestination instanceof Topic, "Unexpected destination type");
            assertEquals(topic.getTopicName(), ((Topic) receivedDestination).getTopicName(),
                    "Unexpected destination name");
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void messageSentToQueueComesBackWithTheSameJMSDestinationWhenReceivedAsynchronously() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(queue);

            Utils.sendMessages(session, queue, 1);

            connection.start();

            CountDownLatch receiveLatch = new CountDownLatch(1);
            AtomicReference<Message> messageHolder = new AtomicReference<>();
            consumer.setMessageListener(message -> {
                messageHolder.set(message);
                receiveLatch.countDown();
            });
            assertTrue(receiveLatch.await(getReceiveTimeout(), TimeUnit.MILLISECONDS),
                    "Timed out waiting for message to be received ");
            assertNotNull(messageHolder.get(), "Message should not be null");

            Destination receivedDestination = messageHolder.get().getJMSDestination();

            assertNotNull(receivedDestination, "JMSDestination should not be null");
            assertTrue(receivedDestination instanceof Queue, "Unexpected destination type");
            assertEquals(queue.getQueueName(), ((Queue) receivedDestination).getQueueName(),
                    "Unexpected destination name");
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testReceiveResend() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(queue);

            Utils.sendMessages(session, queue, 1);

            connection.start();

            Message receivedMessage = consumer.receive(getReceiveTimeout());
            assertNotNull(receivedMessage, "Message should not be null");

            Destination receivedDestination = receivedMessage.getJMSDestination();

            assertNotNull(receivedDestination, "JMSDestination should not be null");
            assertTrue(receivedDestination instanceof Queue, "Unexpected destination type");
            assertEquals(queue.getQueueName(), ((Queue) receivedDestination).getQueueName(),
                    "Unexpected destination name");

            MessageProducer producer = session.createProducer(queue);
            producer.send(receivedMessage);

            Message message = consumer.receive(getReceiveTimeout());
            assertNotNull(message, "Message should not be null");

            Destination destination = message.getJMSDestination();

            assertNotNull(destination, "JMSDestination should not be null");
            assertTrue(destination instanceof Queue, "Unexpected destination type");
            assertEquals(queue.getQueueName(), ((Queue) destination).getQueueName(),
                    "Unexpected destination name");
        }
        finally
        {
            connection.close();
        }
    }
}
