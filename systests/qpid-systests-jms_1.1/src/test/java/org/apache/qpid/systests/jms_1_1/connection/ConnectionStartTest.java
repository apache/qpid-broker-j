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
package org.apache.qpid.systests.jms_1_1.connection;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;

import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.JmsTestBase;
import org.apache.qpid.systests.Utils;

public class ConnectionStartTest extends JmsTestBase
{
    @Test
    public void testConsumerCanReceiveMessageAfterConnectionStart() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Utils.sendMessages(connection, queue, 1);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(queue);
            assertNull(consumer.receive(getReceiveTimeout() / 2),
                    "No messages should be delivered when the connection is stopped");
            connection.start();
            assertNotNull(consumer.receive(getReceiveTimeout()),
                    "There should be messages waiting for the consumer");
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testMessageListenerCanReceiveMessageAfterConnectionStart() throws Exception
    {

        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Utils.sendMessages(connection, queue, 1);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(queue);

            final CountDownLatch awaitMessage = new CountDownLatch(1);
            final AtomicLong deliveryTime = new AtomicLong();
            consumer.setMessageListener(message -> {
                try
                {
                    deliveryTime.set(System.currentTimeMillis());
                }
                finally
                {
                    awaitMessage.countDown();
                }
            });

            long beforeStartTime = System.currentTimeMillis();
            connection.start();

            assertTrue(awaitMessage.await(getReceiveTimeout(), TimeUnit.MILLISECONDS),
                    "Message is not received in timely manner");
            assertTrue(deliveryTime.get() >= beforeStartTime, "Message received before connection start");
        }
        finally
        {
            connection.close();
        }
    }
}
