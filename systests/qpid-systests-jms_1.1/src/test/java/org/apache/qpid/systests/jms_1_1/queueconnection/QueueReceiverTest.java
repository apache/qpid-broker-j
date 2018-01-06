/*
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

package org.apache.qpid.systests.jms_1_1.queueconnection;

import static junit.framework.TestCase.assertNotNull;
import static org.apache.qpid.systests.Utils.INDEX;
import static org.junit.Assert.assertEquals;

import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueReceiver;
import javax.jms.QueueSession;
import javax.jms.Session;

import org.junit.Test;

import org.apache.qpid.systests.JmsTestBase;
import org.apache.qpid.systests.Utils;

public class QueueReceiverTest extends JmsTestBase
{

    @Test
    public void createReceiver() throws Exception
    {
        Queue queue = createQueue(getTestName());
        QueueConnection queueConnection = getQueueConnection();
        try
        {
            queueConnection.start();
            Utils.sendMessages(queueConnection, queue, 3);

            QueueSession session = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
            QueueReceiver receiver = session.createReceiver(queue, String.format("%s=2", INDEX));
            assertEquals("Queue names should match from QueueReceiver", queue.getQueueName(), receiver.getQueue().getQueueName());

            Message received = receiver.receive(getReceiveTimeout());
            assertNotNull("Message is not received", received);
            assertEquals("Unexpected message is received", 2, received.getIntProperty(INDEX));
        }
        finally
        {
            queueConnection.close();
        }
    }

}
