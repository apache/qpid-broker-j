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
package org.apache.qpid.systests.jms_1_1.extensions.queue;

import static org.apache.qpid.systests.Utils.INDEX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.naming.NamingException;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.OverflowPolicy;

public class RejectOverflowPolicyTest extends OverflowPolicyTestBase
{
    @Test
    public void testMaximumQueueDepthBytesExceeded() throws Exception
    {
        final int messageSize = evaluateMessageSize();
        final int maximumQueueDepthBytes = messageSize + messageSize / 2;
        final Queue queue = createQueueWithOverflowPolicy(getTestName(), OverflowPolicy.REJECT, maximumQueueDepthBytes, -1, -1);
        verifyOverflowPolicyRejectingSecondMessage(queue);
    }

    @Test
    public void testMaximumQueueDepthMessagesExceeded() throws Exception
    {
        final Queue queue = createQueueWithOverflowPolicy(getTestName(), OverflowPolicy.REJECT, -1, 1, -1);
        verifyOverflowPolicyRejectingSecondMessage(queue);
    }

    private void verifyOverflowPolicyRejectingSecondMessage(final Queue queue) throws NamingException, JMSException
    {
        final Connection producerConnection = getConnectionBuilder().setSyncPublish(true).build();
        try
        {
            final Session producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
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
                assertNotNull(message, "Message is not received");
                assertEquals(0, message.getIntProperty(INDEX));

                consumerSession.commit();

                producer2.send(secondMessage);

                Message message2 = consumer.receive(getReceiveTimeout());
                assertNotNull(message2, "Message is not received");
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
}
