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
package org.apache.qpid.systests.jms_1_1.extensions.consumerpriority;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.systests.JmsTestBase;


public class ConsumerPriorityTest extends JmsTestBase
{
    private static final String LEGACY_BINDING_URL = "BURL:direct://amq.direct/%s/%s?x-priority='%d'";
    private static final String LEGACY_ADDRESS_URL = "ADDR:%s; { create: always, node: { type: queue },"
                                                     + "link : { x-subscribe: { arguments : { x-priority : '%d' } } } }";

    @Test
    public void testLowPriorityConsumerReceivesMessages() throws Exception
    {
        assumeTrue(is(not(equalTo(Protocol.AMQP_1_0))).matches(getProtocol()), "Only legacy client implements this feature");

        final String queueName = getTestName();
        final Queue queue = createQueue(queueName);
        final Connection consumingConnection = getConnection();
        try
        {
            final Connection producingConnection = getConnectionBuilder().setSyncPublish(true).build();
            try
            {
                final Session consumingSession = consumingConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                consumingConnection.start();

                final Queue consumerDestination =
                        consumingSession.createQueue(String.format(LEGACY_BINDING_URL, queueName, queueName, 10));
                final MessageConsumer consumer = consumingSession.createConsumer(consumerDestination);
                assertNull(consumer.receive(getShortReceiveTimeout()),
                        "There should be no messages in the queue");

                Session producingSession = producingConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                final MessageProducer producer = producingSession.createProducer(queue);
                producer.send(producingSession.createTextMessage(getTestName()));
                assertNotNull(consumer.receive(getReceiveTimeout()), "Expected message is not received");
            }
            finally
            {
                producingConnection.close();
            }
        }
        finally
        {
            consumingConnection.close();
        }
    }

    @Test
    public void testLowPriorityConsumerDoesNotReceiveMessagesIfHigherPriorityConsumerAvailable() throws Exception
    {
        assumeTrue(is(not(equalTo(Protocol.AMQP_1_0))).matches(getProtocol()), "Only legacy client implements this feature");

        final String queueName = getTestName();
        final String consumerQueue = String.format(LEGACY_BINDING_URL, queueName, queueName, 10);
        doTestLowPriorityConsumerDoesNotReceiveMessagesIfHigherPriorityAvailable(queueName, consumerQueue);
    }

    @Test
    public void testLowPriorityConsumerDoesNotReceiveMessagesIfHigherPriorityConsumerAvailableUsingADDR()
            throws Exception
    {
        assumeTrue(is(not(equalTo(Protocol.AMQP_1_0))).matches(getProtocol()), "Only legacy client implements this feature");

        final String queueName = getTestName();
        String consumerQueue = String.format(LEGACY_ADDRESS_URL, queueName, 10);
        doTestLowPriorityConsumerDoesNotReceiveMessagesIfHigherPriorityAvailable(queueName, consumerQueue);
    }

    private void doTestLowPriorityConsumerDoesNotReceiveMessagesIfHigherPriorityAvailable(final String queueName,
                                                                                          final String consumerQueue)
            throws Exception
    {
        final Queue queue = createQueue(queueName);
        final Connection consumingConnection = getConnection();
        try
        {
            final Connection producingConnection = getConnectionBuilder().setSyncPublish(true).build();
            try
            {
                final Session consumingSession = consumingConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                consumingConnection.start();

                final MessageConsumer consumer =
                        consumingSession.createConsumer(consumingSession.createQueue(consumerQueue));
                assertNull(consumer.receive(getShortReceiveTimeout()), "There should be no messages in the queue");

                final Connection secondConsumingConnection = getConnection();
                try
                {
                    final Session secondConsumingSession =
                            secondConsumingConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
                    secondConsumingConnection.start();

                    final MessageConsumer standardPriorityConsumer = secondConsumingSession.createConsumer(queue);
                    assertNull(standardPriorityConsumer.receive(getShortReceiveTimeout()),
                            "There should be no messages in the queue");

                    final Session producingSession = producingConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    final MessageProducer producer = producingSession.createProducer(queue);
                    producer.send(producingSession.createTextMessage(getTestName()));
                    assertNull(consumer.receive(getShortReceiveTimeout()),
                            "Message should not go to the low priority consumer");
                    producer.send(producingSession.createTextMessage(getTestName() + " 2"));
                    assertNull(consumer.receive(getShortReceiveTimeout()),
                            "Message should not go to the low priority consumer");

                    assertNotNull(standardPriorityConsumer.receive(getReceiveTimeout()));
                    assertNotNull(standardPriorityConsumer.receive(getReceiveTimeout()));
                }
                finally
                {
                    secondConsumingConnection.close();
                }
            }
            finally
            {
                producingConnection.close();
            }
        }
        finally
        {
            consumingConnection.close();
        }
    }

    @Test
    public void testLowPriorityConsumerReceiveMessagesIfHigherPriorityConsumerHasNoCredit() throws Exception
    {
        assumeTrue(is(not(equalTo(Protocol.AMQP_1_0))).matches(getProtocol()), "Only legacy client implements this feature");

        final String queueName = getTestName();
        final Queue queue = createQueue(queueName);
        final Connection consumingConnection = getConnection();
        try
        {
            final Connection producingConnection = getConnectionBuilder().setSyncPublish(true).build();
            try
            {
                final Session consumingSession = consumingConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                consumingConnection.start();
                final Queue consumerDestination =
                        consumingSession.createQueue(String.format(LEGACY_BINDING_URL, queueName, queueName, 10));
                final MessageConsumer consumer = consumingSession.createConsumer(consumerDestination);
                assertNull(consumer.receive(getShortReceiveTimeout()),
                        "There should be no messages in the queue");

                final Connection secondConsumingConnection = getConnectionBuilder().setPrefetch(2).build();
                try
                {
                    final Session secondConsumingSession =
                            secondConsumingConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    secondConsumingConnection.start();
                    final MessageConsumer standardPriorityConsumer = secondConsumingSession.createConsumer(queue);
                    assertNull(standardPriorityConsumer.receive(getShortReceiveTimeout()),
                            "There should be no messages in the queue");

                    final Session producingSession = producingConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    final MessageProducer producer = producingSession.createProducer(queue);

                    producer.send(createTextMessage(1, producingSession));
                    assertNull(consumer.receive(getShortReceiveTimeout()),
                            "Message should not go to the low priority consumer");
                    producer.send(createTextMessage(2, producingSession));
                    assertNull(consumer.receive(getShortReceiveTimeout()),
                            "Message should not go to the low priority consumer");
                    producer.send(createTextMessage(3, producingSession));
                    final Message message = consumer.receive(getReceiveTimeout());
                    assertNotNull(message,
                            "Message should go to the low priority consumer as standard priority consumer has no credit");
                    assertTrue(message instanceof TextMessage, "Message is not a text message");
                    assertEquals(getTestName() + " 3", ((TextMessage) message).getText());
                }
                finally
                {
                    secondConsumingConnection.close();
                }
            }
            finally
            {
                producingConnection.close();
            }
        }
        finally
        {
            consumingConnection.close();
        }
    }

    @Test
    public void testLowPriorityConsumerReceiveMessagesIfHigherPriorityConsumerDoesNotSelect() throws Exception
    {
        assumeTrue(is(not(equalTo(Protocol.AMQP_1_0))).matches(getProtocol()), "Only legacy client implements this feature");

        final String queueName = getTestName();
        final Queue queue = createQueue(queueName);
        Connection consumingConnection = getConnection();
        try
        {
            Connection producingConnection = getConnectionBuilder().setSyncPublish(true).build();
            try
            {
                Session consumingSession = consumingConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                consumingConnection.start();

                Session producingSession = producingConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

                final Queue consumerDestination =
                        consumingSession.createQueue(String.format(LEGACY_BINDING_URL, queueName, queueName, 10));
                final MessageConsumer consumer = consumingSession.createConsumer(consumerDestination);
                assertNull(consumer.receive(getShortReceiveTimeout()), "There should be no messages in the queue");

                final Connection secondConsumingConnection = getConnection();
                try
                {
                    final Session secondConsumingSession =
                            secondConsumingConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    secondConsumingConnection.start();
                    final MessageConsumer standardPriorityConsumer =
                            secondConsumingSession.createConsumer(queue,
                                                                  "msg <> 2");
                    assertNull(standardPriorityConsumer.receive(
                            getShortReceiveTimeout()), "There should be no messages in the queue");

                    final MessageProducer producer = producingSession.createProducer(queue);

                    producer.send(createTextMessage(1, producingSession));
                    assertNull(consumer.receive(getShortReceiveTimeout()),
                            "Message should not go to the low priority consumer");
                    producer.send(createTextMessage(2, producingSession));
                    Message message = consumer.receive(getReceiveTimeout());
                    assertNotNull(message,
                            "Message should go to the low priority consumer as standard priority consumer is not interested");
                    assertTrue(message instanceof TextMessage, "Message is not a text message");
                    assertEquals(getTestName() + " 2", ((TextMessage) message).getText());
                }
                finally
                {
                    secondConsumingConnection.close();
                }
            }
            finally
            {
                producingConnection.close();
            }
        }
        finally
        {
            consumingConnection.close();
        }
    }

    private long getShortReceiveTimeout()
    {
        return getReceiveTimeout() / 4;
    }


    private TextMessage createTextMessage(final int msgId, final Session producingSession) throws JMSException
    {
        TextMessage textMessage = producingSession.createTextMessage(getTestName() + " " + msgId);
        textMessage.setIntProperty("msg", msgId);
        return textMessage;
    }
}
