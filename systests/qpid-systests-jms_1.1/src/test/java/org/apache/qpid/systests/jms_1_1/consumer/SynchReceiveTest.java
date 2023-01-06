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
 *
 *
 */
package org.apache.qpid.systests.jms_1_1.consumer;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.Instant;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;

import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.JmsTestBase;
import org.apache.qpid.systests.Utils;

public class SynchReceiveTest extends JmsTestBase
{
    @Test
    public void receiveWithTimeout() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();

        try
        {
            final int numberOfMessages = 3;
            connection.start();
            Utils.sendMessages(connection, queue, numberOfMessages);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(queue);

            for (int num = 0; num < numberOfMessages; num++)
            {
                assertNotNull(consumer.receive(getReceiveTimeout()),
                        String.format("Expected message (%d) not received", num));
            }

            assertNull(consumer.receive(getReceiveTimeout()), "Received too many messages");

        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void receiveNoWait() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();

        try
        {
            connection.start();
            Utils.sendMessages(connection, queue, 1);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(queue);

            final Instant timeout = Instant.now().plusMillis(getReceiveTimeout());
            Message msg;
            do
            {
                msg = consumer.receiveNoWait();
            }
            while(msg == null && Instant.now().isBefore(timeout));

            assertNotNull(msg, "Expected message not received within timeout");
            assertNull(consumer.receiveNoWait(), "Received too many messages");
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void twoConsumersInterleaved() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnectionBuilder().setPrefetch(0).build();

        try
        {
            final int numberOfReceiveLoops = 3;
            final int numberOfMessages = numberOfReceiveLoops * 2;

            connection.start();
            Utils.sendMessages(connection, queue, numberOfMessages);

            Session consumerSession1 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer1 = consumerSession1.createConsumer(queue);

            Session consumerSession2 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer2 = consumerSession2.createConsumer(queue);

            for(int i = 0; i < numberOfReceiveLoops; i++)
            {
                final Message receive1 = consumer1.receive(getReceiveTimeout());
                assertNotNull(receive1, "Expected message not received from consumer1 within timeout");

                final Message receive2 = consumer2.receive(getReceiveTimeout());
                assertNotNull(receive2, "Expected message not received from consumer1 within timeout");
            }
        }
        finally
        {
            connection.close();
        }
    }
}
