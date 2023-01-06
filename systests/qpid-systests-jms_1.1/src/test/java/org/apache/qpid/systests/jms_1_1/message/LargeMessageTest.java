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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.stream.IntStream;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.JmsTestBase;

public class LargeMessageTest extends JmsTestBase
{
    @Test
    public void message256k() throws Exception
    {
        checkLargeMessage(256 * 1024);
    }

    @Test
    public void message512k() throws Exception
    {
        checkLargeMessage(512 * 1024);
    }

    @Test
    public void message1024k() throws Exception
    {
        checkLargeMessage(1024 * 1024);
    }

    public void checkLargeMessage(final int messageSize) throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            final String messageText = buildLargeMessage(messageSize);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            MessageProducer producer = session.createProducer(queue);

            TextMessage message = session.createTextMessage(messageText);

            producer.send(message);

            MessageConsumer consumer = session.createConsumer(queue);
            connection.start();
            Message receivedMessage = consumer.receive(getReceiveTimeout());

            assertTrue(receivedMessage instanceof TextMessage, "TextMessage should be received");
            String receivedMessageText = ((TextMessage) receivedMessage).getText();
            assertEquals(receivedMessageText, messageText,
                    String.format("Unexpected large message content for size : %d", messageSize));
        }
        finally
        {
            connection.close();
        }
    }

    private String buildLargeMessage(int size)
    {
        return IntStream.range(0, size)
                        .map(operand -> (char)(operand % 26) + 'A')
                        .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString();
    }
}
