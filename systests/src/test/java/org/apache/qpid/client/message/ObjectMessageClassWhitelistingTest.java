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
 */

package org.apache.qpid.client.message;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageFormatException;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.qpid.configuration.CommonProperties;
import org.apache.qpid.jms.ConnectionURL;
import org.apache.qpid.test.utils.QpidBrokerTestCase;

public class ObjectMessageClassWhitelistingTest extends QpidBrokerTestCase
{
    public static final int TIMEOUT = 10000;
    public static final int TEST_VALUE = 37;
    private MessageConsumer _consumer;
    private MessageProducer _producer;

    public void testObjectMessage() throws Exception
    {
        Map<String, String> connectionOptions = new HashMap<>();
        connectionOptions.put(ConnectionURL.OPTIONS_OBJECT_MESSAGE_CLASS_HIERARCHY_WHITE_LIST, "*");
        final Connection c = getConnectionWithOptions(connectionOptions);
        c.start();
        Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue destination = getTestQueue();
        _consumer = s.createConsumer(destination);
        _producer = s.createProducer(destination);

        sendTestObjectMessage(s, _producer);
        Message receivedMessage = _consumer.receive(TIMEOUT);
        assertNotNull("did not receive message within timeout", receivedMessage);
        assertTrue("message is of wrong type", receivedMessage instanceof ObjectMessage);
        ObjectMessage receivedObjectMessage = (ObjectMessage) receivedMessage;
        Object payloadObject = receivedObjectMessage.getObject();
        assertTrue("payload is of wrong type", payloadObject instanceof HashMap);
        HashMap<String,Integer> payload = (HashMap<String, Integer>) payloadObject;
        assertEquals("payload has wrong value", (Integer)TEST_VALUE, payload.get("value"));
    }

    public void testNotWhiteListedByConnectionUrlObjectMessage() throws Exception
    {
        Map<String, String> connectionOptions = new HashMap<>();
        connectionOptions.put(ConnectionURL.OPTIONS_OBJECT_MESSAGE_CLASS_HIERARCHY_WHITE_LIST, "org.apache.qpid");
        final Connection c = getConnectionWithOptions(connectionOptions);
        c.start();
        Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue destination = getTestQueue();
        MessageConsumer consumer = s.createConsumer(destination);
        MessageProducer producer = s.createProducer(destination);

        sendTestObjectMessage(s, producer);
        Message receivedMessage = consumer.receive(TIMEOUT);
        assertNotNull("did not receive message within timeout", receivedMessage);
        assertTrue("message is of wrong type", receivedMessage instanceof ObjectMessage);
        ObjectMessage receivedObjectMessage = (ObjectMessage) receivedMessage;
        try
        {
            receivedObjectMessage.getObject();
            fail("should not deserialize class");
        }
        catch (MessageFormatException e)
        {
            // pass
        }
    }

    public void testWhiteListedClassByConnectionUrlObjectMessage() throws Exception
    {
        Map<String, String> connectionOptions = new HashMap<>();
        connectionOptions.put(ConnectionURL.OPTIONS_OBJECT_MESSAGE_CLASS_HIERARCHY_WHITE_LIST, "java.util.HashMap,java.lang");
        final Connection c = getConnectionWithOptions(connectionOptions);
        c.start();
        Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue destination = getTestQueue();
        MessageConsumer consumer = s.createConsumer(destination);
        MessageProducer producer = s.createProducer(destination);

        sendTestObjectMessage(s, producer);
        Message receivedMessage = consumer.receive(TIMEOUT);
        assertNotNull("did not receive message within timeout", receivedMessage);
        assertTrue("message is of wrong type", receivedMessage instanceof ObjectMessage);
        ObjectMessage receivedObjectMessage = (ObjectMessage) receivedMessage;
        HashMap<String, Integer> object = (HashMap<String, Integer>) receivedObjectMessage.getObject();
        assertEquals("Unexpected value", (Integer) TEST_VALUE, object.get("value"));
    }

    public void testBlackListedClassByConnectionUrlObjectMessage() throws Exception
    {
        Map<String, String> connectionOptions = new HashMap<>();
        connectionOptions.put(ConnectionURL.OPTIONS_OBJECT_MESSAGE_CLASS_HIERARCHY_WHITE_LIST, "java");
        connectionOptions.put(ConnectionURL.OPTIONS_OBJECT_MESSAGE_CLASS_HIERARCHY_BLACK_LIST, "java.lang.Integer");
        final Connection c = getConnectionWithOptions(connectionOptions);
        c.start();
        Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue destination = getTestQueue();
        MessageConsumer consumer = s.createConsumer(destination);
        MessageProducer producer = s.createProducer(destination);

        sendTestObjectMessage(s, producer);
        Message receivedMessage = consumer.receive(TIMEOUT);
        assertNotNull("did not receive message within timeout", receivedMessage);
        assertTrue("message is of wrong type", receivedMessage instanceof ObjectMessage);
        ObjectMessage receivedObjectMessage = (ObjectMessage) receivedMessage;

        try
        {
            receivedObjectMessage.getObject();
            fail("Should not be allowed to deserialize black listed class");
        }
        catch (JMSException e)
        {
            // pass
        }
    }

    public void testNonWhiteListedBySystemPropertyObjectMessage() throws Exception
    {
        setSystemProperty(CommonProperties.QPID_SECURITY_OBJECT_MESSAGE_CLASS_HIERARCHY_WHITE_LIST, "org.apache.qpid");
        final Connection c = getConnection();
        c.start();
        Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue destination = getTestQueue();
        MessageConsumer consumer = s.createConsumer(destination);
        MessageProducer producer = s.createProducer(destination);

        sendTestObjectMessage(s, producer);
        Message receivedMessage = consumer.receive(TIMEOUT);
        assertNotNull("did not receive message within timeout", receivedMessage);
        assertTrue("message is of wrong type", receivedMessage instanceof ObjectMessage);
        ObjectMessage receivedObjectMessage = (ObjectMessage) receivedMessage;
        try
        {
            receivedObjectMessage.getObject();
            fail("should not deserialize class");
        }
        catch (MessageFormatException e)
        {
            // pass
        }
    }

    public void testWhiteListedAnonymousClassByConnectionUrlObjectMessage() throws Exception
    {
        Map<String, String> connectionOptions = new HashMap<>();
        connectionOptions.put(ConnectionURL.OPTIONS_OBJECT_MESSAGE_CLASS_HIERARCHY_WHITE_LIST, ObjectMessageClassWhitelistingTest.class.getCanonicalName());
        final Connection c = getConnectionWithOptions(connectionOptions);
        doTestWhiteListedEnclosedClassTest(c, createAnonymousObject(TEST_VALUE));
    }

    public void testBlackListedAnonymousClassByConnectionUrlObjectMessage() throws Exception
    {
        Map<String, String> connectionOptions = new HashMap<>();
        connectionOptions.put(ConnectionURL.OPTIONS_OBJECT_MESSAGE_CLASS_HIERARCHY_WHITE_LIST, ObjectMessageClassWhitelistingTest.class.getPackage().getName());
        connectionOptions.put(ConnectionURL.OPTIONS_OBJECT_MESSAGE_CLASS_HIERARCHY_BLACK_LIST, ObjectMessageClassWhitelistingTest.class.getCanonicalName());

        final Connection c = getConnectionWithOptions(connectionOptions);
        doTestBlackListedEnclosedClassTest(c, createAnonymousObject(TEST_VALUE));
    }

    public void testWhiteListedNestedClassByConnectionUrlObjectMessage() throws Exception
    {
        Map<String, String> connectionOptions = new HashMap<>();
        connectionOptions.put(ConnectionURL.OPTIONS_OBJECT_MESSAGE_CLASS_HIERARCHY_WHITE_LIST, ObjectMessageClassWhitelistingTest.NestedClass.class.getCanonicalName());
        final Connection c = getConnectionWithOptions(connectionOptions);
        doTestWhiteListedEnclosedClassTest(c, new NestedClass(TEST_VALUE));
    }

    public void testBlackListedNestedClassByConnectionUrlObjectMessage() throws Exception
    {
        Map<String, String> connectionOptions = new HashMap<>();
        connectionOptions.put(ConnectionURL.OPTIONS_OBJECT_MESSAGE_CLASS_HIERARCHY_WHITE_LIST, ObjectMessageClassWhitelistingTest.class.getCanonicalName());
        connectionOptions.put(ConnectionURL.OPTIONS_OBJECT_MESSAGE_CLASS_HIERARCHY_BLACK_LIST, NestedClass.class.getCanonicalName());

        final Connection c = getConnectionWithOptions(connectionOptions);
        doTestBlackListedEnclosedClassTest(c, new NestedClass(TEST_VALUE));
    }

    private void doTestWhiteListedEnclosedClassTest(Connection c, Serializable content) throws JMSException
    {
        c.start();
        Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue destination = getTestQueue();
        MessageConsumer consumer = s.createConsumer(destination);
        MessageProducer producer = s.createProducer(destination);

        final ObjectMessage sendMessage = s.createObjectMessage();
        sendMessage.setObject(content);
        producer.send(sendMessage);

        Message receivedMessage = consumer.receive(TIMEOUT);
        assertNotNull("did not receive message within timeout", receivedMessage);
        assertTrue("message is of wrong type", receivedMessage instanceof ObjectMessage);
        Object receivedObject = ((ObjectMessage)receivedMessage).getObject();
        assertEquals("Received object has unexpected class", content.getClass(), receivedObject.getClass());
        assertEquals("Received object has unexpected content", content, receivedObject);
    }

    private void doTestBlackListedEnclosedClassTest(final Connection c, final Serializable content) throws JMSException
    {
        c.start();
        Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue destination = getTestQueue();
        MessageConsumer consumer = s.createConsumer(destination);
        MessageProducer producer = s.createProducer(destination);

        final ObjectMessage sendMessage = s.createObjectMessage();
        sendMessage.setObject(content);
        producer.send(sendMessage);

        Message receivedMessage = consumer.receive(TIMEOUT);
        assertNotNull("did not receive message within timeout", receivedMessage);
        assertTrue("message is of wrong type", receivedMessage instanceof ObjectMessage);
        try
        {
            ((ObjectMessage)receivedMessage).getObject();
            fail("Exception not thrown");
        }
        catch (MessageFormatException e)
        {
            // pass
        }
    }

    private void sendTestObjectMessage(final Session s, final MessageProducer producer) throws JMSException
    {
        HashMap<String, Integer> messageContent = new HashMap<>();
        messageContent.put("value", TEST_VALUE);
        Message objectMessage =  s.createObjectMessage(messageContent);
        producer.send(objectMessage);
    }

    public static Serializable createAnonymousObject(final int field)
    {
        return new Serializable()
        {
            private int _field = field;

            @Override
            public int hashCode()
            {
                return _field;
            }

            @Override
            public boolean equals(final Object o)
            {
                if (this == o)
                {
                    return true;
                }
                if (o == null || getClass() != o.getClass())
                {
                    return false;
                }

                final Serializable that = (Serializable) o;

                return getFieldValueByReflection(that).equals(_field);
            }

            private Object getFieldValueByReflection(final Serializable that)
            {
                try
                {
                    final Field f = that.getClass().getDeclaredField("_field");
                    f.setAccessible(true);
                    return f.get(that);
                }
                catch (NoSuchFieldException | IllegalAccessException e)
                {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    public static class NestedClass implements Serializable
    {
        private final int _field;

        public NestedClass(final int field)
        {
            _field = field;
        }

        @Override
        public boolean equals(final Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || getClass() != o.getClass())
            {
                return false;
            }

            final NestedClass that = (NestedClass) o;

            return _field == that._field;

        }

        @Override
        public int hashCode()
        {
            return _field;
        }
    }
}
