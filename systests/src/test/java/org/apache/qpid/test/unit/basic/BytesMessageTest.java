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
package org.apache.qpid.test.unit.basic;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageNotReadableException;
import javax.jms.MessageNotWriteableException;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.test.utils.QpidBrokerTestCase;
import org.apache.qpid.transport.util.Waiter;

public class BytesMessageTest extends QpidBrokerTestCase implements MessageListener
{
    private static final Logger _logger = LoggerFactory.getLogger(BytesMessageTest.class);

    private Connection _connection;
    private Destination _destination;
    private Session _session;
    private final List<Message> received = new ArrayList<>();
    private final List<byte[]> messages = new ArrayList<byte[]>();
    private int _count = 100;

    protected void setUp() throws Exception
    {
        super.setUp();
        _connection = getConnection("guest", "guest");
        _session = _connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        _destination = createTestQueue(_session);

        // Set up a slow consumer.
        _session.createConsumer(_destination).setMessageListener(this);
        _connection.start();
    }

    protected void tearDown() throws Exception
    {
        super.tearDown();
    }

    public void test() throws Exception
    {
        try
        {
            send(_count);
            waitFor(_count);
            check();
            _logger.info("Completed without failure");
        }
        finally
        {
            _connection.close();
        }
    }

    void send(int count) throws JMSException
    {
        // create a publisher
        MessageProducer producer = _session.createProducer(_destination);
        for (int i = 0; i < count; i++)
        {
            BytesMessage msg = _session.createBytesMessage();

            try
            {
                msg.readFloat();
                Assert.fail("Message should not be readable");
            }
            catch (MessageNotReadableException mnwe)
            {
                // normal execution
            }

            byte[] data = ("Message " + i).getBytes();
            msg.writeBytes(data);
            messages.add(data);
            producer.send(msg);
        }
    }

    void waitFor(int count) throws InterruptedException
    {
        synchronized (received)
        {
            Waiter w = new Waiter(received, 30000);
            while (received.size() < count && w.hasTime())
            {
                w.await();
            }
        }
    }

    void check() throws JMSException
    {
        List<byte[]> actual = new ArrayList<>();
        for (Message message : received)
        {

            BytesMessage bytesMessage = (BytesMessage)message;

            byte[] data = new byte[(int) bytesMessage.getBodyLength()];
            bytesMessage.readBytes(data);
            actual.add(data);

            // Check Body Write Status
            try
            {
                bytesMessage.writeBoolean(true);
                Assert.fail("Message should not be writeable");
            }
            catch (MessageNotWriteableException mnwe)
            {
                // normal execution
            }

            bytesMessage.clearBody();

            try
            {
                bytesMessage.writeBoolean(true);
            }
            catch (MessageNotWriteableException mnwe)
            {
                Assert.fail("Message should be writeable");
            }

            // Check property write status
            try
            {
                bytesMessage.setStringProperty("test", "test");
                Assert.fail("Message should not be writeable");
            }
            catch (MessageNotWriteableException mnwe)
            {
                // normal execution
            }

            bytesMessage.clearProperties();

            try
            {
                bytesMessage.setStringProperty("test", "test");
            }
            catch (MessageNotWriteableException mnwe)
            {
                Assert.fail("Message should be writeable");
            }

        }

        assertEqual(messages.iterator(), actual.iterator());
    }

    private static void assertEqual(Iterator expected, Iterator actual)
    {
        List<String> errors = new ArrayList<String>();
        while (expected.hasNext() && actual.hasNext())
        {
            try
            {
                assertEquivalent((byte[]) expected.next(), (byte[]) actual.next());
            }
            catch (Exception e)
            {
                errors.add(e.getMessage());
            }
        }
        while (expected.hasNext())
        {
            errors.add("Expected " + expected.next() + " but no more actual values.");
        }
        while (actual.hasNext())
        {
            errors.add("Found " + actual.next() + " but no more expected values.");
        }

        if (!errors.isEmpty())
        {
            throw new RuntimeException(errors.toString());
        }
    }

    private static void assertEquivalent(byte[] expected, byte[] actual)
    {
        if (expected.length != actual.length)
        {
            throw new RuntimeException("Expected length " + expected.length + " got " + actual.length);
        }

        for (int i = 0; i < expected.length; i++)
        {
            if (expected[i] != actual[i])
            {
                throw new RuntimeException("Failed on byte " + i + " of " + expected.length);
            }
        }
    }

    public void onMessage(Message message)
    {
        synchronized (received)
        {
            received.add(message);
            received.notify();
        }
    }



    public void testModificationAfterSend() throws Exception
    {
        Connection connection = getConnection();
        Session producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        BytesMessage jmsMsg = producerSession.createBytesMessage();
        Destination destination = createTestQueue(producerSession, "testQ");
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        /* Set the constant message contents. */

        jmsMsg.setStringProperty("foo", "test");

        /* Pre-populate the message body buffer to the target size. */
        byte[] jmsMsgBodyBuffer = new byte[1024];

        connection.start();

        /* Send messages. */
        MessageProducer producer = producerSession.createProducer(destination);

        MessageConsumer consumer = session.createConsumer(destination);
        
        for(int writtenMsgCount = 0; writtenMsgCount < 10; writtenMsgCount++)
        {
            /* Set the per send message contents. */
            jmsMsgBodyBuffer[0] = (byte) writtenMsgCount;
            jmsMsg.writeBytes(jmsMsgBodyBuffer, 0, jmsMsgBodyBuffer.length);
            /** Try to write a message. */
            producer.send(jmsMsg);
        }

        producerSession.close();

        for(int writtenMsgCount = 0; writtenMsgCount < 10; writtenMsgCount++)
        {
            BytesMessage recvdMsg = (BytesMessage) consumer.receive(1000L);
            assertNotNull("Expected to receive message " + writtenMsgCount + " but did not", recvdMsg);
            assertEquals("Message "+writtenMsgCount+" not of expected size", (long) ((writtenMsgCount + 1)*1024),
                         recvdMsg.getBodyLength());

        }

        connection.close();
    }

}
