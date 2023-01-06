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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.JmsTestBase;

public class BytesMessageTest extends JmsTestBase
{
    @Test
    public void sendAndReceiveEmpty() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            MessageProducer producer = session.createProducer(queue);

            BytesMessage message = session.createBytesMessage();
            producer.send(message);

            MessageConsumer consumer = session.createConsumer(queue);
            connection.start();
            Message receivedMessage = consumer.receive(getReceiveTimeout());

            assertTrue(receivedMessage instanceof BytesMessage, "BytesMessage should be received");
            assertEquals(0, ((BytesMessage) receivedMessage).getBodyLength(), "Unexpected body length");
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void sendAndReceiveBody() throws Exception
    {
        final byte[] text = "euler".getBytes(StandardCharsets.US_ASCII);
        final double value = 2.71828;

        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            MessageProducer producer = session.createProducer(queue);

            BytesMessage message = session.createBytesMessage();

            message.writeBytes(text);
            message.writeDouble(value);

            producer.send(message);

            MessageConsumer consumer = session.createConsumer(queue);
            connection.start();
            Message receivedMessage = consumer.receive(getReceiveTimeout());

            assertTrue(receivedMessage instanceof BytesMessage, "BytesMessage should be received");

            byte[] receivedBytes = new byte[text.length];
            BytesMessage receivedBytesMessage = (BytesMessage) receivedMessage;
            receivedBytesMessage.readBytes(receivedBytes);

            assertArrayEquals(receivedBytes, text, "Unexpected bytes");
            assertEquals(value, receivedBytesMessage.readDouble(), 0, "Unexpected double");
        }
        finally
        {
            connection.close();
        }
    }
}
