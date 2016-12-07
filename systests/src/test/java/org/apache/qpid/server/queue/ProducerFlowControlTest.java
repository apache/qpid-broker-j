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
package org.apache.qpid.server.queue;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.QpidException;
import org.apache.qpid.client.AMQDestination;
import org.apache.qpid.client.AMQSession;
import org.apache.qpid.server.logging.AbstractTestLogging;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.LifetimePolicy;
import org.apache.qpid.systest.rest.RestTestHelper;
import org.apache.qpid.test.utils.TestBrokerConfiguration;

public class ProducerFlowControlTest extends AbstractTestLogging
{
    private static final Logger _logger = LoggerFactory.getLogger(ProducerFlowControlTest.class);

    private static final int TIMEOUT = 10000;

    private Connection _producerConnection;
    private Connection _consumerConnection;
    private Session _producerSession;
    private Session _consumerSession;
    private MessageProducer _producer;
    private MessageConsumer _consumer;
    private Queue _queue;
    private RestTestHelper _restTestHelper;

    private final AtomicInteger _sentMessages = new AtomicInteger(0);

    public void setUp() throws Exception
    {
        getDefaultBrokerConfiguration().addHttpManagementConfiguration();
        super.setUp();

        _restTestHelper = new RestTestHelper(getDefaultBroker().getHttpPort());
        _monitor.markDiscardPoint();

        _producerConnection = getConnection();
        _producerSession = _producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        _producerConnection.start();

        _consumerConnection = getConnection();
        _consumerSession = _consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

    }

    public void tearDown() throws Exception
    {
        try
        {
            try
            {
                _producerConnection.close();
                _consumerConnection.close();
            }
            finally
            {
                _restTestHelper.tearDown();
            }
        }
        finally
        {
            super.tearDown();
        }
    }

    public void testCapacityExceededCausesBlock() throws Exception
    {
        String queueName = getTestQueueName();

        createAndBindQueueWithFlowControlEnabled(_producerSession, queueName, 1000, 800);
        _producer = _producerSession.createProducer(_queue);

        // try to send 5 messages (should block after 4)
        sendMessagesAsync(_producer, _producerSession, 5, 50L);

        Thread.sleep(5000);

        assertEquals("Incorrect number of message sent before blocking", 4, _sentMessages.get());

        _consumer = _consumerSession.createConsumer(_queue);
        _consumerConnection.start();


        _consumer.receive();

        Thread.sleep(1000);

        assertEquals("Message incorrectly sent after one message received", 4, _sentMessages.get());


        _consumer.receive();

        Thread.sleep(1000);

        assertEquals("Message not sent after two messages received", 5, _sentMessages.get());

    }


    public void testBrokerLogMessages() throws Exception
    {
        String queueName = getTestQueueName();
        
        createAndBindQueueWithFlowControlEnabled(_producerSession, queueName, 1000, 800);
        _producer = _producerSession.createProducer(_queue);

        // try to send 5 messages (should block after 4)
        sendMessagesAsync(_producer, _producerSession, 5, 50L);

        List<String> results = waitAndFindMatches("QUE-1003", 7000);

        assertEquals("Did not find correct number of QUE-1003 queue overfull messages", 1, results.size());

        _consumer = _consumerSession.createConsumer(_queue);
        _consumerConnection.start();


        while(_consumer.receive(1000) != null) {};

        results = waitAndFindMatches("QUE-1004");

        assertEquals("Did not find correct number of UNDERFULL queue underfull messages", 1, results.size());
    }


    public void testClientLogMessages() throws Exception
    {
        String queueName = getTestQueueName();

        setTestClientSystemProperty("qpid.flow_control_wait_failure","3000");
        setTestClientSystemProperty("qpid.flow_control_wait_notify_period","1000");

        Session session = _producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        createAndBindQueueWithFlowControlEnabled(session, queueName, 1000, 800);
        _producer = session.createProducer(_queue);

        // try to send 5 messages (should block after 4)
        MessageSender sender = sendMessagesAsync(_producer, session, 5, 50L);

        List<String> results = waitAndFindMatches("Message send delayed by", TIMEOUT);
        assertTrue("No delay messages logged by client",results.size()!=0);

        List<String> failedMessages = waitAndFindMatches("Message send failed due to timeout waiting on broker enforced"
                                                  + " flow control", TIMEOUT);
        assertEquals("Incorrect number of send failure messages logged by client (got " + results.size() + " delay "
                     + "messages)",1,failedMessages.size());
    }


    public void testFlowControlOnCapacityResumeEqual() throws Exception
    {
        String queueName = getTestQueueName();
        
        createAndBindQueueWithFlowControlEnabled(_producerSession, queueName, 1000, 1000);
        _producer = _producerSession.createProducer(_queue);


        // try to send 5 messages (should block after 4)
        sendMessagesAsync(_producer, _producerSession, 5, 50L);

        Thread.sleep(5000);

        assertEquals("Incorrect number of message sent before blocking", 4, _sentMessages.get());

        _consumer = _consumerSession.createConsumer(_queue);
        _consumerConnection.start();

        _consumer.receive();

        Thread.sleep(1000);

        assertEquals("Message incorrectly sent after one message received", 5, _sentMessages.get());
        

    }


    public void testFlowControlSoak() throws Exception
    {
        String queueName = getTestQueueName();
        

        final int numProducers = 10;
        final int numMessages = 100;

        createAndBindQueueWithFlowControlEnabled(_producerSession, queueName, 6000, 3000);

        _consumerConnection.start();

        Connection[] producers = new Connection[numProducers];
        for(int i = 0 ; i < numProducers; i ++)
        {

            producers[i] = getConnection();
            producers[i].start();
            Session session = producers[i].createSession(false, Session.AUTO_ACKNOWLEDGE);

            MessageProducer myproducer = session.createProducer(_queue);
            MessageSender sender = sendMessagesAsync(myproducer, session, numMessages, 50L);
        }

        _consumer = _consumerSession.createConsumer(_queue);
        _consumerConnection.start();

        for(int j = 0; j < numProducers * numMessages; j++)
        {
        
            Message msg = _consumer.receive(5000);
            Thread.sleep(50L);
            assertNotNull("Message not received("+j+"), sent: "+_sentMessages.get(), msg);

        }



        Message msg = _consumer.receive(500);
        assertNull("extra message received", msg);


        for(int i = 0; i < numProducers; i++)
        {
            producers[i].close();
        }

    }

    public void testSendTimeout() throws Exception
    {
        String queueName = getTestQueueName();
        final String expectedMsg = isBroker010() ? "Exception when sending message:timed out waiting for message credit"
                : "Unable to send message for 3 seconds due to broker enforced flow control";

        setTestClientSystemProperty("qpid.flow_control_wait_failure","3000");
        Session session = _producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        createAndBindQueueWithFlowControlEnabled(_producerSession, queueName, 1000, 800);
        _producer = session.createProducer(_queue);

        // try to send 5 messages (should block after 4)
        MessageSender sender = sendMessagesAsync(_producer, session, 5, 100L);

        Exception e = sender.awaitSenderException(10000);

        assertNotNull("No timeout exception on sending", e);


        assertEquals("Unexpected exception reason", expectedMsg, e.getMessage());

    }

    public void testFlowControlAttributeModificationViaREST() throws Exception
    {
        String queueName = getTestQueueName();

        createAndBindQueueWithFlowControlEnabled(_producerSession, queueName, 0, 0);
        _producer = _producerSession.createProducer(_queue);
        
        String queueUrl = String.format("queue/%1$s/%1$s/%2$s", TestBrokerConfiguration.ENTRY_NAME_VIRTUAL_HOST, queueName);

        //check current attribute values are 0 as expected
        Map<String, Object> queueAttributes = _restTestHelper.getJsonAsSingletonList(queueUrl);
        assertEquals("Capacity was not the expected value", 0,
                     ((Number) queueAttributes.get(org.apache.qpid.server.model.Queue.QUEUE_FLOW_CONTROL_SIZE_BYTES)).intValue());
        assertEquals("FlowResumeCapacity was not the expected value", 0,
                     ((Number) queueAttributes.get(org.apache.qpid.server.model.Queue.QUEUE_FLOW_RESUME_SIZE_BYTES)).intValue());
        
        //set new values that will cause flow control to be active, and the queue to become overfull after 1 message is sent
        setFlowLimits(queueUrl, 250, 250);
        assertFalse("Queue should not be overfull", isFlowStopped(queueUrl));
        
        // try to send 2 messages (should block after 1)
        sendMessagesAsync(_producer, _producerSession, 2, 50L);

        waitForFlowControlAndMessageCount(queueUrl, 1, 2000);

        //check only 1 message was sent, and queue is overfull
        assertEquals("Incorrect number of message sent before blocking", 1, _sentMessages.get());
        assertTrue("Queue should be overfull", isFlowStopped(queueUrl));
        
        //raise the attribute values, causing the queue to become underfull and allow the second message to be sent.
        setFlowLimits(queueUrl, 300, 300);

        waitForFlowControlAndMessageCount(queueUrl, 2, 2000);

        //check second message was sent, and caused the queue to become overfull again
        assertEquals("Second message was not sent after lifting FlowResumeCapacity", 2, _sentMessages.get());
        assertTrue("Queue should be overfull", isFlowStopped(queueUrl));

        //raise capacity above queue depth, check queue remains overfull as FlowResumeCapacity still exceeded
        setFlowLimits(queueUrl, 700, 300);
        assertTrue("Queue should be overfull", isFlowStopped(queueUrl));

        //receive a message, check queue becomes underfull
        
        _consumer = _consumerSession.createConsumer(_queue);
        _consumerConnection.start();
        
        _consumer.receive();

        if(!isBroker10())
        {
            //perform a synchronous op on the connection
            ((AMQSession<?, ?>) _consumerSession).sync();
        }

        assertFalse("Queue should not be overfull", isFlowStopped(queueUrl));

        _consumer.receive();
    }

    private void waitForFlowControlAndMessageCount(final String queueUrl, final int messageCount, final int timeout) throws InterruptedException, IOException
    {
        int timeWaited = 0;
        while (timeWaited < timeout && (!isFlowStopped(queueUrl) || _sentMessages.get() != messageCount))
        {
            Thread.sleep(50);
            timeWaited += 50;
        }
    }

    private void setFlowLimits(final String queueUrl, final int blockValue, final int resumeValue) throws IOException
    {
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put(org.apache.qpid.server.model.Queue.QUEUE_FLOW_CONTROL_SIZE_BYTES, blockValue);
        attributes.put(org.apache.qpid.server.model.Queue.QUEUE_FLOW_RESUME_SIZE_BYTES, resumeValue);
        _restTestHelper.submitRequest(queueUrl, "PUT", attributes);
    }

    private boolean isFlowStopped(final String queueUrl) throws IOException
    {
        Map<String, Object> queueAttributes2 = _restTestHelper.getJsonAsSingletonList(queueUrl);
        return (boolean) queueAttributes2.get(org.apache.qpid.server.model.Queue.QUEUE_FLOW_STOPPED);
    }

    public void testQueueDeleteWithBlockedFlow() throws Exception
    {
        String queueName = getTestQueueName();
        createAndBindQueueWithFlowControlEnabled(_producerSession, queueName, 1000, 800, true, false);

        _producer = _producerSession.createProducer(_queue);

        // try to send 5 messages (should block after 4)
        sendMessagesAsync(_producer, _producerSession, 5, 50L);

        Thread.sleep(5000);

        assertEquals("Incorrect number of message sent before blocking", 4, _sentMessages.get());

        // close blocked producer session and connection
        _producerConnection.close();

        if(!isBroker10())
        {
            // delete queue with a consumer session
            ((AMQSession<?, ?>) _consumerSession).sendQueueDelete(queueName);

            _consumer = _consumerSession.createConsumer(_queue);
        }
        else
        {
            deleteEntityUsingAmqpManagement(getTestQueueName(), _consumerSession, "org.apache.qpid.Queue");
            createTestQueue(_consumerSession);
        }
        _consumerConnection.start();

        Message message = _consumer.receive(1000l);
        assertNull("Unexpected message", message);
    }

    private void createAndBindQueueWithFlowControlEnabled(Session session, String queueName, int capacity, int resumeCapacity) throws Exception
    {
        createAndBindQueueWithFlowControlEnabled(session, queueName, capacity, resumeCapacity, false, true);
    }

    private void createAndBindQueueWithFlowControlEnabled(Session session, String queueName, int capacity, int resumeCapacity, boolean durable, boolean autoDelete) throws Exception
    {
        if(isBroker10())
        {
            final Map<String, Object> attributes = new HashMap<>();
            attributes.put(org.apache.qpid.server.model.Queue.QUEUE_FLOW_CONTROL_SIZE_BYTES, capacity);
            attributes.put(org.apache.qpid.server.model.Queue.QUEUE_FLOW_RESUME_SIZE_BYTES, resumeCapacity);
            attributes.put(org.apache.qpid.server.model.Queue.DURABLE, durable);
            attributes.put(ConfiguredObject.LIFETIME_POLICY, autoDelete ? LifetimePolicy.DELETE_ON_NO_OUTBOUND_LINKS.name() : LifetimePolicy.PERMANENT.name());
            createEntityUsingAmqpManagement(getTestQueueName(), session, "org.apache.qpid.Queue", attributes);
            _queue = session.createQueue(queueName);
        }
        else
        {
            final Map<String, Object> arguments = new HashMap<String, Object>();
            arguments.put("x-qpid-capacity", capacity);
            arguments.put("x-qpid-flow-resume-capacity", resumeCapacity);
            ((AMQSession<?, ?>) session).createQueue(queueName, autoDelete, durable, false, arguments);
            _queue = session.createQueue("direct://amq.direct/"
                                         + queueName
                                         + "/"
                                         + queueName
                                         + "?durable='"
                                         + durable
                                         + "'&autodelete='"
                                         + autoDelete
                                         + "'");
            ((AMQSession<?, ?>) session).declareAndBind((AMQDestination) _queue);
        }
    }

    private MessageSender sendMessagesAsync(final MessageProducer producer,
                                            final Session producerSession,
                                            final int numMessages,
                                            long sleepPeriod)
    {
        MessageSender sender = new MessageSender(producer, producerSession, numMessages,sleepPeriod);
        new Thread(sender).start();
        return sender;
    }

    private void sendMessages(MessageProducer producer, Session producerSession, int numMessages, long sleepPeriod)
            throws JMSException
    {

        for (int msg = 0; msg < numMessages; msg++)
        {
            producer.send(nextMessage(msg, producerSession));
            _sentMessages.incrementAndGet();


            try
            {
                if(!isBroker10())
                {
                    ((AMQSession<?,?>)producerSession).sync();
                    // TODO: sync a second time in order to ensure that the client has received the flow command
                    // before continuing with the next message.  This is required because the Broker may legally
                    // send the flow command after the sync response. By sync'ing a second time we ensure that
                    // the client will has seen/acted on the flow command.  The test really ought not have this
                    // level of information.
                    ((AMQSession<?,?>)producerSession).sync();
                }
                else
                {
                    producerSession.createTemporaryQueue().delete();
                }
            }
            catch (QpidException e)
            {
                _logger.error("Error performing sync", e);
                throw new RuntimeException(e);
            }

            try
            {
                Thread.sleep(sleepPeriod);
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    private static final byte[] BYTE_300 = new byte[300];

    private Message nextMessage(int msg, Session producerSession) throws JMSException
    {
        BytesMessage send = producerSession.createBytesMessage();
        send.writeBytes(BYTE_300);
        send.setIntProperty("msg", msg);

        return send;
    }

    private class MessageSender implements Runnable
    {
        private final MessageProducer _senderProducer;
        private final Session _senderSession;
        private final int _numMessages;
        private volatile JMSException _exception;
        private CountDownLatch _exceptionThrownLatch = new CountDownLatch(1);
        private long _sleepPeriod;

        public MessageSender(MessageProducer producer, Session producerSession, int numMessages, long sleepPeriod)
        {
            _senderProducer = producer;
            _senderSession = producerSession;
            _numMessages = numMessages;
            _sleepPeriod = sleepPeriod;
        }

        public void run()
        {
            try
            {
                sendMessages(_senderProducer, _senderSession, _numMessages, _sleepPeriod);
            }
            catch (JMSException e)
            {
                _exception = e;
                _exceptionThrownLatch.countDown();
            }
        }

        public Exception awaitSenderException(long timeout) throws InterruptedException
        {
            _exceptionThrownLatch.await(timeout, TimeUnit.MILLISECONDS);
            return _exception;
        }
    }
}
