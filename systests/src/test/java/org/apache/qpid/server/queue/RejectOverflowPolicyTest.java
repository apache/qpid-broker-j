/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.qpid.server.queue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.qpid.server.model.OverflowPolicy;
import org.apache.qpid.systest.rest.RestTestHelper;
import org.apache.qpid.test.utils.QpidBrokerTestCase;
import org.apache.qpid.test.utils.TestBrokerConfiguration;

public class RejectOverflowPolicyTest extends QpidBrokerTestCase
{
    private final byte[] BYTE_300 = new byte[300];
    private RestTestHelper _restTestHelper;

    @Override
    public void setUp() throws Exception
    {
        getDefaultBrokerConfiguration().addHttpManagementConfiguration();
        super.setUp();
        _restTestHelper = new RestTestHelper(getDefaultBroker().getHttpPort());

        if (!isBroker10())
        {
            setSystemProperty("sync_publish", "all");
        }

    }



    public void testMaximumQueueDepthBytesExceeded() throws Exception
    {
        final int messageSize = evaluateMessageSize();
        final int maximumQueueDepthBytes = messageSize + messageSize / 2;
        createQueueWithOverflowPolicy(getTestName(), OverflowPolicy.REJECT, maximumQueueDepthBytes, -1);
        verifyOverflowPolicyRejectingSecondMessage(getTestName());
    }

    public void testMaximumQueueDepthMessagesExceeded() throws Exception
    {
        createQueueWithOverflowPolicy(getTestName(), OverflowPolicy.REJECT, -1, 1);
        verifyOverflowPolicyRejectingSecondMessage(getTestName());
    }

    private void createQueueWithOverflowPolicy(final String queueName,
                                               final OverflowPolicy overflowPolicy,
                                               final int maxQueueDepthBytes,
                                               final int maxQueueDepthMessages) throws Exception
    {
        final Map<String, Object> attributes = new HashMap<>();
        if (maxQueueDepthBytes>0)
        {
            attributes.put(org.apache.qpid.server.model.Queue.MAXIMUM_QUEUE_DEPTH_BYTES, maxQueueDepthBytes);
        }
        if (maxQueueDepthMessages > 0)
        {
            attributes.put(org.apache.qpid.server.model.Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES, maxQueueDepthMessages);
        }
        attributes.put(org.apache.qpid.server.model.Queue.OVERFLOW_POLICY, overflowPolicy.name());
        final String queueUrl = String.format("queue/%1$s/%1$s/%2$s", TestBrokerConfiguration.ENTRY_NAME_VIRTUAL_HOST, queueName);
        _restTestHelper.submitRequest(queueUrl, "PUT", attributes, 201);

        final String bindUrl = String.format("exchange/%1$s/%1$s/amq.direct/bind", TestBrokerConfiguration.ENTRY_NAME_VIRTUAL_HOST);
        final Map<String, Object> bindAttributes = new HashMap<>();
        bindAttributes.put("destination", queueName);
        bindAttributes.put("bindingKey", queueName);
        _restTestHelper.submitRequest(bindUrl, "POST", bindAttributes, 200);
    }

    private void verifyOverflowPolicyRejectingSecondMessage(final String queueName) throws Exception
    {
        final Connection producerConnection = getConnectionBuilder().setSyncPublish(true).build();
        try
        {
            final Session producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final Destination queue = producerSession.createQueue(queueName);

            final Message firstMessage = nextMessage(0, producerSession);
            final Message secondMessage = nextMessage(1, producerSession);

            try
            {
                final MessageProducer producer = producerSession.createProducer(queue);
                producer.send(firstMessage);
                try
                {
                    producer.send(secondMessage);
                    fail("Message send should fail due to reject policy");
                }
                catch (JMSException e)
                {
                    // pass
                }
            }
            finally
            {
                producerSession.close();
            }

            final Session producerSession2 = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final MessageProducer producer2 = producerSession2.createProducer(queue);
            final Connection consumerConnection = getConnection();
            try
            {
                Session consumerSession = consumerConnection.createSession(true, Session.SESSION_TRANSACTED);
                MessageConsumer consumer = consumerSession.createConsumer(queue);
                consumerConnection.start();

                Message message = consumer.receive(getReceiveTimeout());
                assertNotNull("Message is not received", message);
                assertEquals(0, message.getIntProperty(INDEX));

                consumerSession.commit();

                producer2.send(secondMessage);

                Message message2 = consumer.receive(getReceiveTimeout());
                assertNotNull("Message is not received", message2);
                assertEquals(1, message2.getIntProperty(INDEX));

                consumerSession.commit();
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

    private int evaluateMessageSize() throws Exception
    {

        final Connection connection = getConnection();
        try
        {
            connection.start();
            final Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            final String tmpQueueName = getTestQueueName() + "_Tmp";
            final Queue tmpQueue = createTestQueue(session, tmpQueueName);
            final MessageProducer producer = session.createProducer(tmpQueue);
            producer.send(nextMessage(0, session));
            session.commit();
            return getQueueDepthBytes(tmpQueueName);
        }
        finally
        {
            connection.close();
        }
    }

    private int getQueueDepthBytes(final String queueName) throws IOException
    {
        // On AMQP 1.0 the size of the message on the broker is not necessarily the size of the message we sent. Therefore, get the actual size from the broker
        final String requestUrl = String.format("queue/%1$s/%1$s/%2$s/getStatistics?statistics=[\"queueDepthBytes\"]", TestBrokerConfiguration.ENTRY_NAME_VIRTUAL_HOST, queueName);
        final Map<String, Object> queueAttributes = _restTestHelper.getJsonAsMap(requestUrl);
        return ((Number) queueAttributes.get("queueDepthBytes")).intValue();
    }

    private Message nextMessage(int index, Session producerSession) throws JMSException
    {
        BytesMessage send = producerSession.createBytesMessage();
        send.writeBytes(BYTE_300);
        send.setIntProperty(INDEX, index);
        return send;
    }
}
