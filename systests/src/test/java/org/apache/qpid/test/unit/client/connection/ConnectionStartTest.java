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
package org.apache.qpid.test.unit.client.connection;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.qpid.client.AMQSession;
import org.apache.qpid.test.utils.QpidBrokerTestCase;

public class ConnectionStartTest extends QpidBrokerTestCase
{

    private Connection _connection;
    private Session _consumerSess;
    private MessageConsumer _consumer;

    protected void setUp() throws Exception
    {
        super.setUp();

        Connection pubCon = getConnection();

        Session pubSess = pubCon.createSession(false, AMQSession.AUTO_ACKNOWLEDGE);
        Queue queue = createTestQueue(pubSess);


        MessageProducer pub = pubSess.createProducer(queue);

        _connection = getConnection();

        _consumerSess = _connection.createSession(false, AMQSession.AUTO_ACKNOWLEDGE);

        _consumer = _consumerSess.createConsumer(queue);

        //publish after queue is created to ensure it can be routed as expected
        pub.send(pubSess.createTextMessage("Initial Message"));

        pubCon.close();


    }

    protected void tearDown() throws Exception
    {
        _connection.close();
        super.tearDown();
    }

    public void testSimpleReceiveConnection()
    {
        try
        {
            assertNull("No messages should be delivered when the connection is stopped", _consumer.receive(500));
            _connection.start();
            assertTrue("There should be messages waiting for the consumer", _consumer.receive(getReceiveTimeout()) != null);

        }
        catch (JMSException e)
        {
            fail("An error occured during test because:" + e);
        }

    }

    public void testMessageListenerConnection() throws Exception
    {
        final CountDownLatch _gotMessage = new CountDownLatch(1);

        try
        {
            _consumer.setMessageListener(new MessageListener()
            {
                public void onMessage(Message message)
                {
                    try
                    {
                        assertEquals("Mesage Received", "Initial Message", ((TextMessage) message).getText());
                        _gotMessage.countDown();
                    }
                    catch (JMSException e)
                    {
                        fail("Couldn't get message text because:" + e.getCause());
                    }
                }
            });
            Thread.sleep(500);
            assertEquals("No messages should be delivered before connection start", 1L,_gotMessage.getCount());
            _connection.start();

            try
            {
                assertTrue("Listener was never called", _gotMessage.await(10 * 1000, TimeUnit.MILLISECONDS));
            }
            catch (InterruptedException e)
            {
                fail("Timed out awaiting message via onMessage");
            }

        }
        catch (JMSException e)
        {
            fail("Failed because:" + e.getCause());
        }

    }


    public static junit.framework.Test suite()
    {
        return new junit.framework.TestSuite(ConnectionStartTest.class);
    }
}
