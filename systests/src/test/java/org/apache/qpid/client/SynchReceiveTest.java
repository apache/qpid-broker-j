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
package org.apache.qpid.client;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.qpid.test.utils.QpidBrokerTestCase;

public class SynchReceiveTest extends QpidBrokerTestCase
{
    private static final int MSG_COUNT = 10;
    private Connection _consumerConnection;
    private MessageConsumer _consumer;
    private Queue _queue;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        _consumerConnection = getConnection();
        _consumerConnection.start();
        Session consumerSession = _consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        _queue = createTestQueue(consumerSession);
        _consumer = consumerSession.createConsumer(_queue);

        // Populate queue
        Connection producerConnection = getConnection();
        Session producerSession = producerConnection.createSession(true, Session.SESSION_TRANSACTED);
        sendMessage(producerSession, _queue, MSG_COUNT);
        producerConnection.close();
    }

    public void testReceiveWithTimeout() throws Exception
    {
        for (int msg = 0; msg < MSG_COUNT; msg++)
        {
            assertNotNull("Expected message number " + msg, _consumer.receive(getReceiveTimeout()));
        }

        assertNull("Received too many messages", _consumer.receive(getShortReceiveTimeout()));
    }

    public void testReceiveNoWait() throws Exception
    {
        for (int msg = 0; msg < MSG_COUNT; msg++)
        {
            assertNotNull("Expected message number " + msg, _consumer.receiveNoWait());
        }

        assertNull("Received too many messages", _consumer.receive(getShortReceiveTimeout()));
    }

    public void testTwoConsumersInterleaved() throws Exception
    {
        _consumerConnection.close();

        _consumerConnection = getConnectionWithPrefetch(0);
        _consumerConnection.start();
        Session consumerSession1 = _consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer1 = consumerSession1.createConsumer(_queue);

        Session consumerSession2 = _consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer2 = consumerSession2.createConsumer(_queue);

        final int maxLoops = MSG_COUNT * 2;
        int msg = 0;
        int loops = 0;
        while(msg < MSG_COUNT && loops < maxLoops)
        {
            if (consumer1.receive(getReceiveTimeout()) != null)
            {
                msg++;
            }

            if (consumer2.receive(getReceiveTimeout()) != null)
            {
                msg++;
            }

            loops++;
        }

        assertEquals("Not all messages received.", MSG_COUNT, msg);
        assertNull("Received too many messages", consumer1.receive(getShortReceiveTimeout()));
        assertNull("Received too many messages", consumer2.receive(getShortReceiveTimeout()));
    }

    public void testIdleSecondConsumer() throws Exception
    {
        Session idleSession = _consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        @SuppressWarnings("unused")
        MessageConsumer idleConsumerOnSameQueue = idleSession.createConsumer(_queue);

        // Since we don't call receive on the idle consumer, all messages will flow to other

        for (int msg = 0; msg < MSG_COUNT; msg++)
        {
            assertNotNull("Expected message number " + msg, _consumer.receive(getReceiveTimeout()));
        }

        assertNull("Received too many messages", _consumer.receive(getShortReceiveTimeout()));
    }


}
