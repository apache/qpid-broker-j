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

package org.apache.qpid.test.client.queue;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueReceiver;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

import org.apache.qpid.client.AMQSession;
import org.apache.qpid.test.utils.QpidBrokerTestCase;

public class QueueReceiverTest extends QpidBrokerTestCase
{
    private Connection _connection;
    private Session _session;
    private Queue _queue;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        _connection = getConnection();
        _session = _connection.createSession(true, Session.SESSION_TRANSACTED);
        _queue = createTestQueue(_session);
    }

    public void testCreateReceiver() throws JMSException
    {
        QueueSession session = (QueueSession) _session;

        QueueReceiver receiver = session.createReceiver(_queue);
        assertEquals("Queue names should match from QueueReceiver", _queue.getQueueName(), receiver.getQueue().getQueueName());

        receiver = session.createReceiver(_queue, "abc");
        assertEquals("Queue names should match from QueueReceiver with selector", _queue.getQueueName(), receiver.getQueue().getQueueName());
    }
}
