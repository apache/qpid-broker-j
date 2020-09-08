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
package org.apache.qpid.systests.jms_1_1.extensions.queue;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeThat;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.junit.Test;

import org.apache.qpid.server.model.OverflowPolicy;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.systests.Utils;

public class ProducerFlowControlTest extends OverflowPolicyTestBase
{

    @Test
    public void testCapacityExceededCausesBlock() throws Exception
    {
        String queueName = getTestName();
        int messageSize = evaluateMessageSize();
        int capacity = messageSize * 3 + messageSize / 2;
        int resumeCapacity = messageSize * 2;

        Queue queue = createAndBindQueueWithFlowControlEnabled(queueName, capacity, resumeCapacity);

        Connection producerConnection = getConnectionBuilder().setSyncPublish(true).build();
        try
        {
            Session producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = producerSession.createProducer(queue);

            // try to send 5 messages (should block after 4)
            MessageSender messageSender = sendMessagesAsync(producer, producerSession, 5);

            assertTrue("Flow is not stopped", awaitAttributeValue(queueName, "queueFlowStopped", true, 5000));
            assertEquals("Incorrect number of message sent before blocking",
                         4,
                         messageSender.getNumberOfSentMessages());

            Connection consumerConnection = getConnection();
            try
            {
                Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                MessageConsumer consumer = consumerSession.createConsumer(queue);
                consumerConnection.start();

                Message message = consumer.receive(getReceiveTimeout());
                assertNotNull("Message is not received", message);

                assertFalse("Flow is not stopped", awaitAttributeValue(queueName, "queueFlowStopped", false, 1000));

                assertEquals("Message incorrectly sent after one message received",
                             4,
                             messageSender.getNumberOfSentMessages());

                Message message2 = consumer.receive(getReceiveTimeout());
                assertNotNull("Message is not received", message2);
                assertTrue("Message sending is not finished", messageSender.getSendLatch()
                                                                           .await(1000, TimeUnit.MILLISECONDS));
                assertEquals("Message not sent after two messages received",
                             5,
                             messageSender.getNumberOfSentMessages());
            }
            finally
            {
                consumerConnection.close();
            }
        }
        finally
        {
            producerConnection.close();
        }
    }

    @Test
    public void testFlowControlOnCapacityResumeEqual() throws Exception
    {
        String queueName = getTestName();
        int messageSize = evaluateMessageSize();
        int capacity = messageSize * 3 + messageSize / 2;
        Queue queue = createAndBindQueueWithFlowControlEnabled(queueName, capacity, capacity);

        Connection producerConnection = getConnectionBuilder().setSyncPublish(true).build();
        try
        {
            Session producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = producerSession.createProducer(queue);

            // try to send 5 messages (should block after 4)
            MessageSender messageSender = sendMessagesAsync(producer, producerSession, 5);

            assertTrue("Flow is not stopped", awaitAttributeValue(queueName, "queueFlowStopped", true, 5000));

            assertEquals("Incorrect number of message sent before blocking",
                         4,
                         messageSender.getNumberOfSentMessages());

            Connection consumerConnection = getConnection();
            try
            {
                Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                MessageConsumer consumer = consumerSession.createConsumer(queue);
                consumerConnection.start();

                Message message = consumer.receive(getReceiveTimeout());
                assertNotNull("Message is not received", message);

                assertTrue("Message sending is not finished",
                           messageSender.getSendLatch().await(1000, TimeUnit.MILLISECONDS));

                assertEquals("Message incorrectly sent after one message received",
                             5,
                             messageSender.getNumberOfSentMessages());
            }
            finally
            {
                consumerConnection.close();
            }
        }
        finally
        {
            producerConnection.close();
        }
    }

    @Test
    public void testFlowControlAttributeModificationViaManagement() throws Exception
    {
        final String queueName = getTestName();
        int messageSize = evaluateMessageSize();
        final Queue queue = createAndBindQueueWithFlowControlEnabled(queueName, 0, 0);

        //set new values that will cause flow control to be active, and the queue to become overfull after 1 message is sent
        setFlowLimits(queueName, messageSize / 2, messageSize / 2);
        assertFalse("Queue should not be overfull", isFlowStopped(queueName));

        Connection producerConnection = getConnectionBuilder().setSyncPublish(true).build();
        try
        {
            Session producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = producerSession.createProducer(queue);

            // try to send 2 messages (should block after 1)
            final MessageSender sender = sendMessagesAsync(producer, producerSession, 2);

            assertTrue("Flow is not stopped", awaitAttributeValue(queueName, "queueFlowStopped", true, 2000));

            assertEquals("Incorrect number of message sent before blocking", 1, sender.getNumberOfSentMessages());

            int queueDepthBytes = getQueueDepthBytes(queueName);
            //raise the attribute values, causing the queue to become underfull and allow the second message to be sent.
            setFlowLimits(queueName, queueDepthBytes * 2 + queueDepthBytes / 2, queueDepthBytes + queueDepthBytes / 2);

            assertTrue("Flow is stopped", awaitAttributeValue(queueName, "queueFlowStopped", false, 2000));
            assertTrue("Second message was not sent after changing limits",
                       awaitStatisticsValue(queueName, "queueDepthMessages", 2, 2000));
            assertFalse("Queue should not be overfull", isFlowStopped(queueName));

            // try to send another message to block flow
            final MessageSender sender2 = sendMessagesAsync(producer, producerSession, 1);

            assertTrue("Flow is stopped", awaitAttributeValue(queueName, "queueFlowStopped", true, 2000));
            assertEquals("Incorrect number of message sent before blocking", 1, sender2.getNumberOfSentMessages());
            assertTrue("Queue should be overfull", isFlowStopped(queueName));

            Connection consumerConnection = getConnection();
            try
            {
                Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                MessageConsumer consumer = consumerSession.createConsumer(queue);
                consumerConnection.start();

                Message message = consumer.receive(getReceiveTimeout());
                assertNotNull("Message is not received", message);

                message = consumer.receive(getReceiveTimeout());
                assertNotNull("Second message is not received", message);

                assertTrue("Flow is stopped", awaitAttributeValue(queueName, "queueFlowStopped", false, 2000));

                assertNotNull("Should have received second message", consumer.receive(getReceiveTimeout()));
            }
            finally
            {
                consumerConnection.close();
            }
        }
        finally
        {
            producerConnection.close();
        }
    }

    @Test
    public void testProducerFlowControlIsTriggeredOnEnqueue() throws Exception
    {
        final String queueName = getTestName();
        final Queue queue = createAndBindQueueWithFlowControlEnabled(queueName, 1, 0);

        Connection producerConnection = getConnectionBuilder().setSyncPublish(true).build();
        try
        {
            Session producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = producerSession.createProducer(queue);

            producer.send(nextMessage(0, producerSession));

            // try to send 2 messages (should block after 1)
            sendMessagesAsync(producer, producerSession, 2);

            assertTrue("Flow is not stopped", awaitAttributeValue(queueName, "queueFlowStopped", true, 2000));

            Connection consumerConnection = getConnection();
            try
            {
                Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                MessageConsumer consumer = consumerSession.createConsumer(queue);
                consumerConnection.start();

                Message message = consumer.receive(getReceiveTimeout());
                assertNotNull("Message is not received", message);

                message = consumer.receive(getReceiveTimeout());
                assertNotNull("Second message is not received", message);
            }
            finally
            {
                consumerConnection.close();
            }
        }
        finally
        {
            producerConnection.close();
        }
    }

    @Test
    public void testQueueDeleteWithBlockedFlow() throws Exception
    {
        final String queueName = getTestName();
        final int messageSize = evaluateMessageSize();
        final int capacity = messageSize * 3 + messageSize / 2;
        final int resumeCapacity = messageSize * 2;

        final Queue queue = createAndBindQueueWithFlowControlEnabled(queueName, capacity, resumeCapacity);

        final Connection producerConnection = getConnectionBuilder().setSyncPublish(true).build();
        try
        {
            final Session producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final MessageProducer producer = producerSession.createProducer(queue);

            // try to send 5 messages (should block after 4)
            final MessageSender sender = sendMessagesAsync(producer, producerSession, 5);

            assertTrue("Flow is not stopped", awaitAttributeValue(queueName, "queueFlowStopped", true, 5000));

            assertEquals("Incorrect number of message sent before blocking", 4, sender.getNumberOfSentMessages());

            deleteEntityUsingAmqpManagement(queueName, "org.apache.qpid.Queue");

            createQueue(queueName);

            assertEquals("Unexpected queue depth", 0, getQueueDepthBytes(queueName));
        }
        finally
        {
            producerConnection.close();
        }
    }

    @Test
    public void testEnforceFlowControlOnNewConnection() throws Exception
    {
        assumeThat(getProtocol(), is(equalTo(Protocol.AMQP_1_0)));
        final Queue testQueue = createQueueWithOverflowPolicy(getTestName(), OverflowPolicy.PRODUCER_FLOW_CONTROL, 0, 1, 0);
        final Connection producerConnection1 = getConnectionBuilder().setSyncPublish(true).build();
        try
        {
            Utils.sendMessages(producerConnection1, testQueue, 2);
            assertTrue("Flow is not stopped", awaitAttributeValue(testQueue.getQueueName(), "queueFlowStopped", true, 5000));
        }
        finally
        {
            producerConnection1.close();
        }

        final Connection producerConnection2 = getConnectionBuilder().setSyncPublish(true).build();
        try
        {
            final Session producerSession = producerConnection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final MessageProducer producer = producerSession.createProducer(testQueue);
            final MessageSender messageSender = sendMessagesAsync(producer, producerSession, 1);

            assertTrue("Flow is not stopped", awaitAttributeValue(testQueue.getQueueName(), "queueFlowStopped", true, 5000));
            assertEquals("Incorrect number of message sent before blocking",
                         0,
                         messageSender.getNumberOfSentMessages());

            final Connection consumerConnection = getConnection();
            try
            {
                Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                MessageConsumer consumer = consumerSession.createConsumer(testQueue);
                consumerConnection.start();

                Message message = consumer.receive(getReceiveTimeout());
                assertNotNull("Message is not received", message);

                Message message2 = consumer.receive(getReceiveTimeout());
                assertNotNull("Message is not received", message2);
                assertTrue("Message sending is not finished", messageSender.getSendLatch()
                                                                           .await(getReceiveTimeout(), TimeUnit.MILLISECONDS));
            }
            finally
            {
                consumerConnection.close();
            }
        }
        finally
        {
            producerConnection2.close();
        }
    }

    private void setFlowLimits(final String queueName, final int blockValue, final int resumeValue) throws Exception
    {
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put(org.apache.qpid.server.model.Queue.MAXIMUM_QUEUE_DEPTH_BYTES, blockValue);
        attributes.put(org.apache.qpid.server.model.Queue.OVERFLOW_POLICY, OverflowPolicy.PRODUCER_FLOW_CONTROL.name());
        String resumeLimit = getFlowResumeLimit(blockValue, resumeValue);
        String context = String.format("{\"%s\": %s}",
                                       org.apache.qpid.server.model.Queue.QUEUE_FLOW_RESUME_LIMIT,
                                       resumeLimit);
        attributes.put(org.apache.qpid.server.model.Queue.CONTEXT, context);
        updateEntityUsingAmqpManagement(queueName, "org.apache.qpid.Queue", attributes);
    }

    private boolean isFlowStopped(final String queueName) throws Exception
    {
        Map<String, Object> attributes = readEntityUsingAmqpManagement(queueName, "org.apache.qpid.Queue", false);
        return Boolean.TRUE.equals(attributes.get("queueFlowStopped"));
    }


    private Queue createAndBindQueueWithFlowControlEnabled(String queueName,
                                                           int capacity,
                                                           int resumeCapacity) throws Exception
    {

       return createQueueWithOverflowPolicy(queueName, OverflowPolicy.PRODUCER_FLOW_CONTROL, capacity, -1, resumeCapacity);
    }

    private MessageSender sendMessagesAsync(final MessageProducer producer,
                                            final Session producerSession,
                                            final int numMessages)
    {
        return sendMessagesAsync(producer, producerSession, numMessages, null);
    }

    private MessageSender sendMessagesAsync(final MessageProducer producer,
                                            final Session producerSession,
                                            final int numMessages,
                                            final AtomicInteger messageCounter)
    {
        MessageSender sender = new MessageSender(producer, producerSession, numMessages, messageCounter);
        new Thread(sender).start();
        return sender;
    }

    private boolean awaitStatisticsValue(String queueName, String statisticsName, Number expectedValue, long timeout)
            throws Exception
    {
        return await(timeout, expectedValue, () -> {
            try
            {
                return getStatistics(queueName, statisticsName);
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        });
    }


    private boolean awaitAttributeValue(String queueName, String attributeName, Object expectedValue, long timeout)
            throws Exception
    {
        return await(timeout, expectedValue, () -> {
            try
            {
                Map<String, Object> attributes =
                        readEntityUsingAmqpManagement(queueName, "org.apache.qpid.Queue", false);
                return attributes.get(attributeName);
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        });
    }

    private boolean await(long timeout, Object expectedValue, final Supplier<Object> supplier)
            throws Exception
    {
        long startTime = System.currentTimeMillis();
        long endTime = startTime + timeout;
        boolean found = false;
        do
        {

            Object actualValue = supplier.get();
            if (expectedValue == null)
            {
                found = actualValue == null;
            }
            else if (actualValue != null)
            {
                if (actualValue.getClass() == expectedValue.getClass())
                {
                    found = expectedValue.equals(actualValue);
                }
                else
                {
                    found = String.valueOf(expectedValue).equals(String.valueOf(actualValue));
                }
            }

            if (!found)
            {
                Thread.sleep(50);
            }
        } while (!found && System.currentTimeMillis() <= endTime);
        return found;
    }

    private class MessageSender implements Runnable
    {
        private final AtomicInteger _sentMessages;
        private final MessageProducer _senderProducer;
        private final Session _senderSession;
        private final int _numMessages;
        private volatile Exception _exception;
        private CountDownLatch _sendLatch = new CountDownLatch(1);

        MessageSender(MessageProducer producer, Session producerSession, int numMessages, AtomicInteger messageCounter)
        {
            _senderProducer = producer;
            _senderSession = producerSession;
            _numMessages = numMessages;
            _sentMessages = messageCounter == null ? new AtomicInteger(0) : messageCounter;
        }

        @Override
        public void run()
        {
            try
            {
                sendMessages(_senderProducer, _senderSession, _numMessages);
            }
            catch (Exception e)
            {
                _exception = e;
            }
            finally
            {
                _sendLatch.countDown();
            }
        }

        CountDownLatch getSendLatch()
        {
            return _sendLatch;
        }

        int getNumberOfSentMessages()
        {
            return _sentMessages.get();
        }

        Exception getException()
        {
            return _exception;
        }

        private void sendMessages(MessageProducer producer, Session producerSession, int numMessages)
                throws JMSException
        {

            for (int msg = 0; msg < numMessages; msg++)
            {
                producer.send(nextMessage(msg, producerSession));
                _sentMessages.incrementAndGet();

                // Cause work that causes a synchronous interaction on the wire.  We need to be
                // sure that the client has received the flow/message.stop etc.
                producerSession.createTemporaryQueue().delete();
            }
        }
    }


}
