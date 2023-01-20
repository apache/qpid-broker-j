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
package org.apache.qpid.systests.jms_1_1.message;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.UUID;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageNotWriteableException;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;

import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.JmsTestBase;

public class ObjectMessageTest extends JmsTestBase
{
    @Test
    public void sendAndReceive() throws Exception
    {
        UUID test = UUID.randomUUID();
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            ObjectMessage testMessage = session.createObjectMessage(test);
            Object o = testMessage.getObject();

            assertNotNull(o, "Object was null");
            assertNotNull(testMessage.toString(), "toString returned null");

            MessageProducer producer = session.createProducer(queue);
            producer.send(testMessage);

            MessageConsumer consumer = session.createConsumer(queue);
            connection.start();
            Message receivedMessage = consumer.receive(getReceiveTimeout());

            assertTrue(receivedMessage instanceof ObjectMessage, "ObjectMessage should be received");

            Object result = ((ObjectMessage) receivedMessage).getObject();

            assertEquals(test, result, "First read: UUIDs were not equal");

            result = ((ObjectMessage) receivedMessage).getObject();

            assertEquals(test, result, "Second read: UUIDs were not equal");
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void sendAndReceiveNull() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            ObjectMessage testMessage = session.createObjectMessage(null);
            Object o = testMessage.getObject();

            assertNull(o, "Object was not null");
            assertNotNull(testMessage.toString(), "toString returned null");

            MessageProducer producer = session.createProducer(queue);
            producer.send(testMessage);

            MessageConsumer consumer = session.createConsumer(queue);
            connection.start();
            Message receivedMessage = consumer.receive(getReceiveTimeout());

            assertTrue(receivedMessage instanceof ObjectMessage, "ObjectMessage should be received");

            Object result = ((ObjectMessage) receivedMessage).getObject();

            assertNull(result, "First read: UUIDs were not equal");

            result = ((ObjectMessage) receivedMessage).getObject();

            assertNull(result, "Second read: UUIDs were not equal");
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void sendEmptyObjectMessage() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            ObjectMessage testMessage = session.createObjectMessage();

            MessageProducer producer = session.createProducer(queue);
            producer.send(testMessage);

            MessageConsumer consumer = session.createConsumer(queue);
            connection.start();
            Message receivedMessage = consumer.receive(getReceiveTimeout());

            assertTrue(receivedMessage instanceof ObjectMessage, "ObjectMessage should be received");

            Object result = ((ObjectMessage) receivedMessage).getObject();

            assertNull(result, "First read: unexpected object received");

            result = ((ObjectMessage) receivedMessage).getObject();

            assertNull(result, "Second read: unexpected object received");
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void sendAndReceiveObject() throws Exception
    {
        A a1 = new A(1, "A");
        A a2 = new A(2, "a");
        B b = new B(1, "B");
        C<String, Object> c = new C<>();
        c.put("A1", a1);
        c.put("a2", a2);
        c.put("B", b);
        c.put("String", "String");
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            ObjectMessage testMessage = session.createObjectMessage(c);

            MessageProducer producer = session.createProducer(queue);
            producer.send(testMessage);

            MessageConsumer consumer = session.createConsumer(queue);
            connection.start();
            Message receivedMessage = consumer.receive(getReceiveTimeout());

            assertTrue(receivedMessage instanceof ObjectMessage, "ObjectMessage should be received");

            Object result = ((ObjectMessage) receivedMessage).getObject();

            assertTrue(result instanceof C, "Unexpected object received");

            @SuppressWarnings("unchecked") final C<String, Object> received = (C) result;
            assertEquals(c.size(), received.size(), "Unexpected size");
            assertEquals(new HashSet<>(c.keySet()), new HashSet<>(received.keySet()), "Unexpected keys");

            for (String key : c.keySet())
            {
                assertEquals(c.get(key), received.get(key), "Unexpected value for " + key);
            }
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testSetObjectPropertyForString() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        String testStringProperty = "TestStringProperty";
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            ObjectMessage msg = session.createObjectMessage("test");
            msg.setObjectProperty("TestStringProperty", testStringProperty);
            assertEquals(testStringProperty, msg.getObjectProperty("TestStringProperty"));

            MessageProducer producer = session.createProducer(queue);
            producer.send(msg);

            MessageConsumer consumer = session.createConsumer(queue);
            connection.start();
            Message receivedMessage = consumer.receive(getReceiveTimeout());

            assertTrue(receivedMessage instanceof ObjectMessage, "ObjectMessage should be received");
            assertEquals(testStringProperty, receivedMessage.getObjectProperty("TestStringProperty"),
                    "Unexpected received property");
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testSetObjectPropertyForBoolean() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            ObjectMessage msg = session.createObjectMessage("test");
            msg.setObjectProperty("TestBooleanProperty", Boolean.TRUE);
            assertEquals(Boolean.TRUE, msg.getObjectProperty("TestBooleanProperty"));

            MessageProducer producer = session.createProducer(queue);
            producer.send(msg);

            MessageConsumer consumer = session.createConsumer(queue);
            connection.start();
            Message receivedMessage = consumer.receive(getReceiveTimeout());

            assertTrue(receivedMessage instanceof ObjectMessage, "ObjectMessage should be received");
            assertEquals(Boolean.TRUE, receivedMessage.getObjectProperty("TestBooleanProperty"),
                    "Unexpected received property");
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testSetObjectPropertyForByte() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            ObjectMessage msg = session.createObjectMessage("test");
            msg.setObjectProperty("TestByteProperty", Byte.MAX_VALUE);
            assertEquals(Byte.MAX_VALUE, msg.getObjectProperty("TestByteProperty"));

            MessageProducer producer = session.createProducer(queue);
            producer.send(msg);

            MessageConsumer consumer = session.createConsumer(queue);
            connection.start();
            Message receivedMessage = consumer.receive(getReceiveTimeout());

            assertTrue(receivedMessage instanceof ObjectMessage, "ObjectMessage should be received");
            assertEquals(Byte.MAX_VALUE, receivedMessage.getObjectProperty("TestByteProperty"),
                    "Unexpected received property");
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testSetObjectPropertyForShort() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            ObjectMessage msg = session.createObjectMessage("test");
            msg.setObjectProperty("TestShortProperty", Short.MAX_VALUE);
            assertEquals(Short.MAX_VALUE, msg.getObjectProperty("TestShortProperty"));
            MessageProducer producer = session.createProducer(queue);
            producer.send(msg);

            MessageConsumer consumer = session.createConsumer(queue);
            connection.start();
            Message receivedMessage = consumer.receive(getReceiveTimeout());

            assertTrue(receivedMessage instanceof ObjectMessage, "ObjectMessage should be received");
            assertEquals(Short.MAX_VALUE, receivedMessage.getObjectProperty("TestShortProperty"),
                    "Unexpected received property");
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testSetObjectPropertyForInteger() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            ObjectMessage msg = session.createObjectMessage("test");
            msg.setObjectProperty("TestIntegerProperty", Integer.MAX_VALUE);
            assertEquals(Integer.MAX_VALUE, msg.getObjectProperty("TestIntegerProperty"));
            MessageProducer producer = session.createProducer(queue);
            producer.send(msg);

            MessageConsumer consumer = session.createConsumer(queue);
            connection.start();
            Message receivedMessage = consumer.receive(getReceiveTimeout());

            assertTrue(receivedMessage instanceof ObjectMessage, "ObjectMessage should be received");
            assertEquals(Integer.MAX_VALUE, receivedMessage.getObjectProperty("TestIntegerProperty"),
                    "Unexpected received property");
        }
        finally
        {
            connection.close();
        }
    }


    @Test
    public void testSetObjectPropertyForDouble() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            ObjectMessage msg = session.createObjectMessage("test");
            msg.setObjectProperty("TestDoubleProperty", Double.MAX_VALUE);
            assertEquals(Double.MAX_VALUE, msg.getObjectProperty("TestDoubleProperty"));

            MessageProducer producer = session.createProducer(queue);
            producer.send(msg);

            MessageConsumer consumer = session.createConsumer(queue);
            connection.start();
            Message receivedMessage = consumer.receive(getReceiveTimeout());

            assertTrue(receivedMessage instanceof ObjectMessage, "ObjectMessage should be received");
            assertEquals(Double.MAX_VALUE, receivedMessage.getObjectProperty("TestDoubleProperty"),
                    "Unexpected received property");
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testSetObjectPropertyForFloat() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            ObjectMessage msg = session.createObjectMessage("test");
            msg.setObjectProperty("TestFloatProperty", Float.MAX_VALUE);
            assertEquals(Float.MAX_VALUE, msg.getObjectProperty("TestFloatProperty"));

            MessageProducer producer = session.createProducer(queue);
            producer.send(msg);

            MessageConsumer consumer = session.createConsumer(queue);
            connection.start();
            Message receivedMessage = consumer.receive(getReceiveTimeout());

            assertTrue(receivedMessage instanceof ObjectMessage, "ObjectMessage should be received");
            assertEquals(Float.MAX_VALUE, receivedMessage.getObjectProperty("TestFloatProperty"),
                    "Unexpected received property");
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testClearBodyAndProperties() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            A object = new A(1, "test");
            ObjectMessage msg = session.createObjectMessage(object);
            msg.setStringProperty("testProperty", "testValue");

            MessageProducer producer = session.createProducer(queue);
            producer.send(msg);

            MessageConsumer consumer = session.createConsumer(queue);
            connection.start();
            Message receivedMessage = consumer.receive(getReceiveTimeout());

            assertTrue(receivedMessage instanceof ObjectMessage, "ObjectMessage should be received");
            ObjectMessage objectMessage = (ObjectMessage) receivedMessage;
            Object received = objectMessage.getObject();
            assertTrue(received instanceof A, "Unexpected object type received");
            assertEquals(object, received, "Unexpected object received");
            assertEquals("testValue",   receivedMessage.getStringProperty("testProperty"),
                    "Unexpected property value");

            try
            {
                objectMessage.setObject("Test text");
                fail("Message should not be writable");
            }
            catch (MessageNotWriteableException e)
            {
                // pass
            }

            objectMessage.clearBody();

            try
            {
                objectMessage.setObject("Test text");
            }
            catch (MessageNotWriteableException e)
            {
                fail("Message should be writable");
            }

            try
            {
                objectMessage.setStringProperty("test", "test");
                fail("Message should not be writable");
            }
            catch (MessageNotWriteableException mnwe)
            {
                // pass
            }

            objectMessage.clearProperties();

            try
            {
                objectMessage.setStringProperty("test", "test");
            }
            catch (MessageNotWriteableException mnwe)
            {
                fail("Message should be writable");
            }
        }
        finally
        {
            connection.close();
        }
    }

    private static class A implements Serializable
    {
        private final String sValue;
        private final int iValue;

        A(int i, String s)
        {
            sValue = s;
            iValue = i;
        }

        @Override
        public int hashCode()
        {
            return iValue;
        }

        @Override
        public boolean equals(Object o)
        {
            return (o instanceof A) && equals((A) o);
        }

        protected boolean equals(A a)
        {
            return ((a.sValue == null) ? (sValue == null) : a.sValue.equals(sValue)) && (a.iValue == iValue);
        }
    }

    private static class B extends A
    {
        private final long time;

        B(int i, String s)
        {
            super(i, s);
            time = System.currentTimeMillis();
        }

        @Override
        protected boolean equals(A a)
        {
            return super.equals(a) && (a instanceof B) && (time == ((B) a).time);
        }
    }

    private static class C<X, Y> extends HashMap<X, Y> implements Serializable
    {
    }
}
