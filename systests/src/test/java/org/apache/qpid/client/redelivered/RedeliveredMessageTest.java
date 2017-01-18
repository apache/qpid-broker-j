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
package org.apache.qpid.client.redelivered;

import org.apache.qpid.test.utils.QpidBrokerTestCase;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;

public class RedeliveredMessageTest extends QpidBrokerTestCase
{
    public void testRedeliveredFlagSetAfterRedelivery() throws Exception
    {
        Connection connection = getConnectionWithPrefetch(0);
        connection.start();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Destination destination = createTestQueue(session);
        MessageConsumer consumer = session.createConsumer(destination);

        final int numberOfMessages = 3;
        sendMessage(session, destination, numberOfMessages);

        // Receive only the first message. Leave messages 2 and 3 unseen.
        receiveAssertingJmsDelivery(consumer, 0, false);

        session.rollback();
        connection.close();

        connection =  getConnectionWithPrefetch(0);
        connection.start();
        session = connection.createSession(true, Session.SESSION_TRANSACTED);
        consumer = session.createConsumer(destination);

        receiveAssertingJmsDelivery(consumer, 0, true);
        receiveAssertingJmsDelivery(consumer, 1, false);
        receiveAssertingJmsDelivery(consumer, 2, false);

    }

    private void receiveAssertingJmsDelivery(MessageConsumer consumer,
                                             int expectedIndex,
                                             boolean expectedJMSRedelivered) throws Exception
    {
        Message m = consumer.receive(getReceiveTimeout());
        assertNotNull("Message is not received" , m);
        assertEquals("Unexpected message expectedIndex" , expectedIndex, m.getIntProperty(INDEX));

        assertEquals("Redelivered should be not set on message expectedIndex " + expectedIndex, expectedJMSRedelivered, m.getJMSRedelivered());
    }
}
