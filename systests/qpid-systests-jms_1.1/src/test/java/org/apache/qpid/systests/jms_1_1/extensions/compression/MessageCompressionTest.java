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
package org.apache.qpid.systests.jms_1_1.extensions.compression;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.Collections;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.systests.JmsTestBase;


public class MessageCompressionTest extends JmsTestBase
{
    private static final int MIN_MESSAGE_PAYLOAD_SIZE = 2048 * 1024;

    @BeforeEach
    public void setUp()
    {
        assumeTrue(is(not(equalTo(Protocol.AMQP_1_0))).matches(getProtocol()), "AMQP 1.0 client does not support compression yet");
    }

    @Test
    public void testSenderCompressesReceiverUncompresses() throws Exception
    {
        doTestCompression(true, true, true);
    }

    @Test
    public void testSenderCompressesOnly() throws Exception
    {
        doTestCompression(true, false, true);
    }

    @Test
    public void testReceiverUncompressesOnly() throws Exception
    {
        doTestCompression(false, true, true);
    }

    @Test
    public void testNoCompression() throws Exception
    {
        doTestCompression(false, false, true);
    }

    @Test
    public void testDisablingCompressionAtBroker() throws Exception
    {
        enableMessageCompression(false);
        try
        {
            doTestCompression(true, true, false);
        }
        finally
        {
            enableMessageCompression(true);
        }
    }


    private void doTestCompression(final boolean senderCompresses,
                                   final boolean receiverUncompresses,
                                   final boolean brokerCompressionEnabled) throws Exception
    {

        String messageText = createMessageText();
        Connection senderConnection = getConnectionBuilder().setCompress(senderCompresses).build();
        String queueName = getTestName();
        Queue testQueue = createQueue(queueName);
        try
        {
            publishMessage(senderConnection, messageText, testQueue);

            Map<String, Object> statistics = getVirtualHostStatistics("bytesIn");
            int bytesIn = ((Number) statistics.get("bytesIn")).intValue();

            if (senderCompresses && brokerCompressionEnabled)
            {
                assertTrue(bytesIn < messageText.length(), "Message was not sent compressed");
            }
            else
            {
                assertFalse(bytesIn < messageText.length(), "Message was incorrectly sent compressed");
            }
        }
        finally
        {
            senderConnection.close();
        }

        // receive the message
        Connection consumerConnection = getConnectionBuilder().setCompress(receiverUncompresses).build();
        try
        {
            Session session = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(testQueue);
            consumerConnection.start();

            TextMessage message = (TextMessage) consumer.receive(getReceiveTimeout() * 2);
            assertNotNull(message, "Message was not received");
            assertEquals(messageText, message.getText(), "Message was corrupted");
            assertEquals("foo", message.getStringProperty("bar"), "Header was corrupted");

            Map<String, Object> statistics = getVirtualHostStatistics("bytesOut");
            int bytesOut = ((Number) statistics.get("bytesOut")).intValue();

            if (receiverUncompresses && brokerCompressionEnabled)
            {
                assertTrue(bytesOut < messageText.length(), "Message was not received compressed");
            }
            else
            {
                assertFalse(bytesOut < messageText.length(), "Message was incorrectly received compressed");
            }
        }
        finally
        {
            consumerConnection.close();
        }
    }

    private void publishMessage(final Connection senderConnection, final String messageText, final Queue testQueue)
            throws JMSException
    {
        Session session = senderConnection.createSession(true, Session.SESSION_TRANSACTED);

        MessageProducer producer = session.createProducer(testQueue);
        TextMessage sentMessage = session.createTextMessage(messageText);
        sentMessage.setStringProperty("bar", "foo");

        producer.send(sentMessage);
        session.commit();
    }

    private String createMessageText()
    {
        StringBuilder stringBuilder = new StringBuilder();
        while (stringBuilder.length() < MIN_MESSAGE_PAYLOAD_SIZE)
        {
            stringBuilder.append("This should compress easily. ");
        }
        return stringBuilder.toString();
    }

    private void enableMessageCompression(final boolean value) throws Exception
    {
        Connection connection = getConnectionBuilder().setVirtualHost("$management").build();
        try
        {
            connection.start();
            final Map<String, Object> attributes =
                    Collections.singletonMap("messageCompressionEnabled", value);
            updateEntityUsingAmqpManagement("Broker",
                                            "org.apache.qpid.Broker",
                                             attributes,
                                            connection);

        }
        finally
        {
            connection.close();
        }
    }
}
