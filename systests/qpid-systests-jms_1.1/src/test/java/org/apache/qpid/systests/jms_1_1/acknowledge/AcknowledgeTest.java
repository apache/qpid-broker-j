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
package org.apache.qpid.systests.jms_1_1.acknowledge;

import static org.apache.qpid.systests.Utils.INDEX;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeThat;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;


import org.junit.Test;

import org.apache.qpid.systests.JmsTestBase;
import org.apache.qpid.systests.Utils;

public class AcknowledgeTest extends JmsTestBase
{

    @Test
    public void testClientAckWithLargeFlusherPeriod() throws Exception
    {
        assumeThat(getBrokerAdmin().supportsRestart(), is(true));

        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(queue);
            connection.start();

            Utils.sendMessages(session, queue, 2);

            Message message = consumer.receive(getReceiveTimeout());
            assertNotNull("Message has not been received", message);
            assertEquals("Unexpected message is received", 0, message.getIntProperty(INDEX));
            message.acknowledge();

            message = consumer.receive(getReceiveTimeout());
            assertNotNull("Second message has not been received", message);
            assertEquals("Unexpected message is received", 1, message.getIntProperty(INDEX));
        }
        finally
        {
            connection.close();
        }

        getBrokerAdmin().restart();

        Connection connection2 = getConnection();
        try
        {
            connection2.start();
            Session session = connection2.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(queue);
            Message message = consumer.receive(getReceiveTimeout());
            assertNotNull("Message has not been received after restart", message);
            assertEquals("Unexpected message is received after restart", 1, message.getIntProperty(INDEX));
        }
        finally
        {
            connection2.close();
        }
    }
}
