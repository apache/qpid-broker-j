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
package org.apache.qpid.test.unit.client.message;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.test.utils.QpidBrokerTestCase;

public class ObjectMessageTest extends QpidBrokerTestCase implements MessageListener
{
    private static final Logger _logger = LoggerFactory.getLogger(ObjectMessageTest.class);

    private Connection connection;
    private Destination destination;
    private Session session;
    private MessageProducer producer;
    private Serializable[] data;
    private volatile boolean waiting;
    private int received;
    private final ArrayList items = new ArrayList();

    protected void setUp() throws Exception
    {
        super.setUp();
        connection =  getConnection("guest", "guest");
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        destination = createTestQueue(session);

        // set up a consumer
        session.createConsumer(destination).setMessageListener(this);
        connection.start();

        // create a publisher
        producer = session.createProducer(destination);
        A a1 = new A(1, "A");
        A a2 = new A(2, "a");
        B b = new B(1, "B");
        C c = new C();
        c.put("A1", a1);
        c.put("a2", a2);
        c.put("B", b);
        c.put("String", "String");

        data = new Serializable[] { a1, a2, b, c, "Hello World!", new Integer(1001) };
    }

    protected void tearDown() throws Exception
    {
        close();
        super.tearDown();
    }


    public void testSendAndReceive() throws Exception
    {
        try
        {
            send();
            waitUntilReceived(data.length);
            check();
            _logger.info("All " + data.length + " items matched.");
        }
        catch (Exception e)
        {
            _logger.error("This Test should succeed but failed", e);
            fail("This Test should succeed but failed due to: " + e);
        }
    }

    public void testSetObjectPropertyForString() throws Exception
    {
        String testStringProperty = "TestStringProperty";
        ObjectMessage msg = session.createObjectMessage(data[0]);
        msg.setObjectProperty("TestStringProperty", testStringProperty);
        assertEquals(testStringProperty, msg.getObjectProperty("TestStringProperty"));
    }

    public void testSetObjectPropertyForBoolean() throws Exception
    {
        ObjectMessage msg = session.createObjectMessage(data[0]);
        msg.setObjectProperty("TestBooleanProperty", Boolean.TRUE);
        assertEquals(Boolean.TRUE, msg.getObjectProperty("TestBooleanProperty"));
    }

    public void testSetObjectPropertyForByte() throws Exception
    {
        ObjectMessage msg = session.createObjectMessage(data[0]);
        msg.setObjectProperty("TestByteProperty", Byte.MAX_VALUE);
        assertEquals(Byte.MAX_VALUE, msg.getObjectProperty("TestByteProperty"));
    }

    public void testSetObjectPropertyForShort() throws Exception
    {
        ObjectMessage msg = session.createObjectMessage(data[0]);
        msg.setObjectProperty("TestShortProperty", Short.MAX_VALUE);
        assertEquals(Short.MAX_VALUE, msg.getObjectProperty("TestShortProperty"));
    }

    public void testSetObjectPropertyForInteger() throws Exception
    {
        ObjectMessage msg = session.createObjectMessage(data[0]);
        msg.setObjectProperty("TestIntegerProperty", Integer.MAX_VALUE);
        assertEquals(Integer.MAX_VALUE, msg.getObjectProperty("TestIntegerProperty"));
    }

    public void testSetObjectPropertyForDouble() throws Exception
    {
        ObjectMessage msg = session.createObjectMessage(data[0]);
        msg.setObjectProperty("TestDoubleProperty", Double.MAX_VALUE);
        assertEquals(Double.MAX_VALUE, msg.getObjectProperty("TestDoubleProperty"));
    }

    public void testSetObjectPropertyForFloat() throws Exception
    {
        ObjectMessage msg = session.createObjectMessage(data[0]);
        msg.setObjectProperty("TestFloatProperty", Float.MAX_VALUE);
        assertEquals(Float.MAX_VALUE, msg.getObjectProperty("TestFloatProperty"));
    }

    public void testSetObjectForNull() throws Exception
    {
        ObjectMessage msg = session.createObjectMessage();
        msg.setObject(null);
        assertNull(msg.getObject());
    }

    private void send() throws Exception
    {
        for (int i = 0; i < data.length; i++)
        {
            ObjectMessage msg;
            if ((i % 2) == 0)
            {
                msg = session.createObjectMessage(data[i]);
            }
            else
            {
                msg = session.createObjectMessage();
                msg.setObject(data[i]);
            }

            producer.send(msg);
        }
    }

    public void check() throws Exception
    {
        Object[] actual = items.toArray();
        if (actual.length != data.length)
        {
            throw new Exception("Expected " + data.length + " objects, got " + actual.length);
        }

        for (int i = 0; i < data.length; i++)
        {
            if (actual[i] instanceof Exception)
            {
                throw new Exception("Error on receive of " + data[i], ((Exception) actual[i]));
            }

            if (actual[i] == null)
            {
                throw new Exception("Expected " + data[i] + " got null");
            }

            if (!data[i].equals(actual[i]))
            {
                throw new Exception("Expected " + data[i] + " got " + actual[i]);
            }
        }
    }

    private void close() throws Exception
    {
        session.close();
        connection.close();
    }

    private synchronized void waitUntilReceived(int count) throws InterruptedException
    {
        waiting = true;
        while (received < count)
        {
            wait();
        }

        waiting = false;
    }

    public void onMessage(Message message)
    {

        try
        {
            if (message instanceof ObjectMessage)
            {
                items.add(((ObjectMessage) message).getObject());
            }
            else
            {
                _logger.error("ERROR: Got " + message.getClass().getName() + " not ObjectMessage");
                items.add(message);
            }
        }
        catch (JMSException e)
        {
            _logger.error("Error getting object from message", e);
            items.add(e);
        }

        synchronized (this)
        {
            received++;
            notify();
        }
    }

    private static class A implements Serializable
    {
        private String sValue;
        private int iValue;

        A(int i, String s)
        {
            sValue = s;
            iValue = i;
        }

        public int hashCode()
        {
            return iValue;
        }

        public boolean equals(Object o)
        {
            return (o instanceof A) && equals((A) o);
        }

        protected boolean equals(A a)
        {
            return areEqual(a.sValue, sValue) && (a.iValue == iValue);
        }
    }

    private static class B extends A
    {
        private long time;

        B(int i, String s)
        {
            super(i, s);
            time = System.currentTimeMillis();
        }

        protected boolean equals(A a)
        {
            return super.equals(a) && (a instanceof B) && (time == ((B) a).time);
        }
    }

    private static class C extends HashMap implements Serializable
    { }

    private static boolean areEqual(Object a, Object b)
    {
        return (a == null) ? (b == null) : a.equals(b);
    }

    private static String randomize(String in)
    {
        return in + System.currentTimeMillis();
    }
}
