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
package org.apache.qpid.test.unit.client;

import javax.jms.Connection;
import javax.jms.Session;

import org.apache.qpid.AMQChannelClosedException;
import org.apache.qpid.client.AMQDestination;
import org.apache.qpid.client.AMQSession;
import org.apache.qpid.configuration.ClientProperties;
import org.apache.qpid.protocol.ErrorCodes;
import org.apache.qpid.test.utils.QpidBrokerTestCase;


public class AMQSessionTest extends QpidBrokerTestCase
{
    private static Session _session;
    private static Connection _connection;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        _connection = getConnection();
        _session = _connection.createSession(true, AMQSession.SESSION_TRANSACTED);
    }


    public void testQueueDepthForQueueWithDepth() throws Exception
    {
        AMQDestination dest = (AMQDestination) _session.createQueue(getTestQueueName());
        AMQSession amqSession = (AMQSession)_session;
        _session.createConsumer(dest).close();
        long depth = amqSession.getQueueDepth(dest);
        assertEquals("Unexpected queue depth for empty queue", 0 , depth);

        sendMessage(_session, dest, 1);

        depth = amqSession.getQueueDepth(dest);
        assertEquals("Unexpected queue depth for empty queue", 1, depth);
    }

    public void testQueueDepthForQueueThatDoesNotExist() throws Exception
    {
        AMQDestination dest = (AMQDestination) _session.createQueue(getTestQueueName());
        AMQSession amqSession = (AMQSession)_session;

        long depth = amqSession.getQueueDepth(dest);
        assertEquals("Unexpected queue depth for non-existent queue", 0 , depth);
    }

    public void testQueueDepthForQueueThatDoesNotExistLegacyBehaviour_08_091() throws Exception
    {
        _session.close();

        setTestClientSystemProperty(ClientProperties.QPID_USE_LEGACY_GETQUEUEDEPTH_BEHAVIOUR, "true");
        _session = _connection.createSession(true, AMQSession.SESSION_TRANSACTED);

        AMQSession amqSession = (AMQSession)_session;
        AMQDestination dest = (AMQDestination) _session.createQueue(getTestQueueName());

        try
        {
            amqSession.getQueueDepth(dest);
            fail("Exception not thrown");
        }
        catch(AMQChannelClosedException cce)
        {
            assertEquals(ErrorCodes.NOT_FOUND, cce.getErrorCode());
        }
    }

}
