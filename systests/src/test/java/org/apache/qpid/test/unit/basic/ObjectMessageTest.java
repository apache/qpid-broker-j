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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.MessageNotWriteableException;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.test.utils.QpidBrokerTestCase;

public class ObjectMessageTest extends QpidBrokerTestCase implements MessageListener
{
    private static final Logger _logger = LoggerFactory.getLogger(ObjectMessageTest.class);

    private Connection _connection;
    private Destination _destination;
    private Session _session;
    private final List<Message> received = new ArrayList<>();
    private final List<Payload> messages = new ArrayList<>();
    private int _count = 100;
    public String _connectionString = "vm://:1";

    protected void setUp() throws Exception
    {
        super.setUp();
        _connection = getConnection("guest", "guest");
        _session = _connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        _destination = createTestQueue(_session);

        // set up a slow consumer
        _session.createConsumer(_destination).setMessageListener(this);
        _connection.start();
    }

    public void test() throws Exception
    {
        int count = _count;
        send(count);
        waitFor(count);
        check();
        _logger.info("Completed without failure");
        _connection.close();
    }

    void send(int count) throws JMSException
    {
        // create a publisher
        MessageProducer producer = _session.createProducer(_destination);
        for (int i = 0; i < count; i++)
        {
            Payload payload = new Payload("Message " + i);
            messages.add(payload);
            producer.send(_session.createObjectMessage(payload));
        }
    }

    void waitFor(int count) throws InterruptedException
    {
        synchronized (received)
        {
            long endTime = System.currentTimeMillis() + 30000L;
            while (received.size() < count)
            {
                received.wait(30000);
                if(received.size() < count && System.currentTimeMillis() > endTime)
                {
                    throw new RuntimeException("Only received " + received.size() + " messages, was expecting " + count);
                }
            }
        }
    }

    void check() throws JMSException
    {
        List<Object> actual = new ArrayList<Object>();
        for (Message message : received)
        {
            assertTrue(message instanceof ObjectMessage);

            ObjectMessage objectMessage = (ObjectMessage)message;
            actual.add(objectMessage.getObject());

            try
            {
                objectMessage.setObject("Test text");
                Assert.fail("Message should not be writeable");
            }
            catch (MessageNotWriteableException mnwe)
            {
                // normal execution
            }

            objectMessage.clearBody();

            try
            {
                objectMessage.setObject("Test text");
            }
            catch (MessageNotWriteableException mnwe)
            {
                Assert.fail("Message should be writeable");
            }

            // Check property write status
            try
            {
                objectMessage.setStringProperty("test", "test");
                Assert.fail("Message should not be writeable");
            }
            catch (MessageNotWriteableException mnwe)
            {
                // normal execution
            }

            objectMessage.clearProperties();

            try
            {
                objectMessage.setStringProperty("test", "test");
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
                assertEqual(expected.next(), actual.next());
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

    private static void assertEqual(Object expected, Object actual)
    {
        if (!expected.equals(actual))
        {
            throw new RuntimeException("Expected '" + expected + "' found '" + actual + "'");
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

    private static class Payload implements Serializable
    {
        private final String data;

        Payload(String data)
        {
            this.data = data;
        }

        public int hashCode()
        {
            return data.hashCode();
        }

        public boolean equals(Object o)
        {
            return (o instanceof Payload) && ((Payload) o).data.equals(data);
        }

        public String toString()
        {
            return "Payload[" + data + "]";
        }
    }
}
