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
package org.apache.qpid.systests.jms_1_1.extensions.limitliveconsumers;

import static org.apache.qpid.systests.Utils.INDEX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Collections;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.JmsTestBase;
import org.apache.qpid.systests.Utils;

public class LimitLiveConsumersTest extends JmsTestBase
{
    @Test
    public void testLimitLiveConsumers() throws Exception
    {
        String queueName = getTestName();
        createEntityUsingAmqpManagement(queueName, "org.apache.qpid.Queue",
                                        Collections.singletonMap("maximumLiveConsumers", 1));
        Queue queue = createQueue(queueName);
        int numberOfMessages = 5;
        Connection connection = getConnectionBuilder().setSyncPublish(true).build();
        try
        {
            Session session1 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            connection.start();


            MessageConsumer consumer1 = session1.createConsumer(queue);

            Session session2 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            MessageConsumer consumer2 = session2.createConsumer(queue);


            Utils.sendMessages(session1, queue, numberOfMessages);


            for (int i = 0; i < 3; i++)
            {
                Message receivedMsg = consumer1.receive(getReceiveTimeout());
                assertNotNull(receivedMsg, "Message " + i + " not received");
                assertEquals(i, receivedMsg.getIntProperty(INDEX), "Unexpected message");
            }

            assertNull(consumer2.receive(getShortReceiveTimeout()), "Unexpected message arrived");

            consumer1.close();
            session1.close();

            for (int i = 3; i < numberOfMessages; i++)
            {
                Message receivedMsg = consumer2.receive(getReceiveTimeout());
                assertNotNull(receivedMsg, "Message " + i + " not received");
                assertEquals(i, receivedMsg.getIntProperty(INDEX), "Unexpected message");
            }

            assertNull(consumer2.receive(getShortReceiveTimeout()), "Unexpected message arrived");

            Session session3 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            MessageConsumer consumer3 = session3.createConsumer(queue);

            MessageProducer producer = session3.createProducer(queue);
            producer.send(Utils.createNextMessage(session3, 6));
            producer.send(Utils.createNextMessage(session3, 7));


            assertNotNull(consumer2.receive(getReceiveTimeout()), "Message not received on second consumer");
            assertNull(consumer3.receive(getShortReceiveTimeout()),
                    "Message unexpectedly received on third consumer");

            Session session4 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            MessageConsumer consumer4 = session4.createConsumer(queue);

            assertNull(consumer4.receive(getShortReceiveTimeout()),
                    "Message unexpectedly received on fourth consumer");
            consumer3.close();
            session3.close();

            assertNull(consumer4.receive(getShortReceiveTimeout()),
                    "Message unexpectedly received on fourth consumer");
            consumer2.close();
            session2.close();

            assertNotNull(consumer4.receive(getReceiveTimeout()), "Message not received on fourth consumer");


            assertNull(consumer4.receive(getShortReceiveTimeout()), "Unexpected message arrived");
        }
        finally
        {
            connection.close();
        }
    }

    private long getShortReceiveTimeout()
    {
        return getReceiveTimeout() / 4;
    }
}
