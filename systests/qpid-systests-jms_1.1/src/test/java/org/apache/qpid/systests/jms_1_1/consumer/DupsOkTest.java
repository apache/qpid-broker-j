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
package org.apache.qpid.systests.jms_1_1.consumer;

import static org.apache.qpid.systests.Utils.INDEX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;

import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.JmsTestBase;
import org.apache.qpid.systests.Utils;

public class DupsOkTest extends JmsTestBase
{
    @Test
    public void synchronousReceive() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        final int numberOfMessages = 3;
        try
        {
            connection.start();
            Utils.sendMessages(connection, queue, numberOfMessages);

            Session session = connection.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(queue);

            for (int i = 0; i < numberOfMessages; i++)
            {
                Message received = consumer.receive(getReceiveTimeout());
                assertNotNull(received, String.format("Expected message (%d) not received", i));
                assertEquals(i, received.getIntProperty(INDEX), "Unexpected message received");
            }

            assertNull(consumer.receive(getReceiveTimeout() / 4), "Received too many messages");

        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void asynchronousReceive() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        final int numberOfMessages = 3;
        try
        {
            connection.start();
            Utils.sendMessages(connection, queue, numberOfMessages);

            Session session = connection.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(queue);

            AtomicReference<Throwable> exception = new AtomicReference<>();
            CountDownLatch completionLatch = new CountDownLatch(numberOfMessages);
            AtomicInteger expectedIndex = new AtomicInteger();

            consumer.setMessageListener(message -> {
                try
                {
                    Object index = message.getObjectProperty(INDEX);
                    assertEquals(expectedIndex.getAndIncrement(), message.getIntProperty(INDEX), "Unexpected message received");
                }
                catch (Throwable e)
                {
                    exception.set(e);
                }
                finally
                {
                    completionLatch.countDown();
                }
            });

            boolean completed = completionLatch.await(getReceiveTimeout() * numberOfMessages, TimeUnit.MILLISECONDS);
            assertTrue(completed, "Message listener did not receive all messages within expected");
            assertNull(exception.get(), "Message listener encountered unexpected exception");
        }
        finally
        {
            connection.close();
        }
    }
}
