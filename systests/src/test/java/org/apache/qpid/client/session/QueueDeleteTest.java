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
package org.apache.qpid.client.session;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.apache.qpid.client.AMQDestination;
import org.apache.qpid.client.AMQSession;
import org.apache.qpid.configuration.ClientProperties;
import org.apache.qpid.protocol.ErrorCodes;
import org.apache.qpid.test.utils.QpidBrokerTestCase;

public class QueueDeleteTest extends QpidBrokerTestCase
{
    private Connection _connection;
    private AMQSession<?, ?> _session;
    protected void setUp() throws Exception
    {
        super.setUp();

        // Turn off queue declare side effect of creating consumer
        setTestClientSystemProperty(ClientProperties.QPID_DECLARE_QUEUES_PROP_NAME, "false");
        setTestClientSystemProperty(ClientProperties.QPID_DECLARE_EXCHANGES_PROP_NAME, "false");
        setTestClientSystemProperty(ClientProperties.QPID_BIND_QUEUES_PROP_NAME, "false");

        _connection = getConnection();
        _connection.start();
        _session = (AMQSession<?, ?>) _connection.createSession(true, Session.SESSION_TRANSACTED);
    }

    public void testDeleteQueue() throws Exception
    {
        AMQDestination destination = (AMQDestination) _session.createQueue(String.format("direct://amq.direct//%s", getTestQueueName()));

        _session.declareAndBind(destination);

        sendMessage(_session, destination, 2);

        receiveMessage(destination);

        _session.deleteQueue(destination.getQueueName());

        // Trying to consume from a queue that does not exist will cause an exception
        try
        {
            _session.createConsumer(destination);
            fail("Exception not thrown");
        }
        catch (JMSException e)
        {
            // PASS
            assertEquals("Expecting queue not found",
                         String.valueOf(ErrorCodes.NOT_FOUND),
                         e.getErrorCode());
        }

        assertTrue("Session expected to be closed", _session.isClosed());
    }

    public void testDeleteNonExistentQueue() throws Exception
    {
        AMQDestination destination = (AMQDestination) _session.createQueue(String.format("direct://amq.direct//%s", getTestQueueName()));

        try
        {
            _session.deleteQueue(destination.getQueueName());
            fail("Exception not thrown");
        }
        catch (JMSException e)
        {
            // PASS
            assertEquals("Expecting queue not found",
                         String.valueOf(ErrorCodes.NOT_FOUND),
                         e.getErrorCode());
        }

        assertTrue("Session expected to be closed", _session.isClosed());
    }


    private void receiveMessage(final Destination destination) throws Exception
    {
        MessageConsumer consumer = _session.createConsumer(destination);
        Message message = consumer.receive(RECEIVE_TIMEOUT);
        assertNotNull("Message not received", message);
        _session.commit();
        consumer.close();
    }
}
