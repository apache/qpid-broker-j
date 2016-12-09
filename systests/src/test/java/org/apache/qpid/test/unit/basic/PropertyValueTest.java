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
package org.apache.qpid.test.unit.basic;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageFormatException;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import com.google.common.base.Strings;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.client.AMQConnection;
import org.apache.qpid.test.utils.QpidBrokerTestCase;

public class PropertyValueTest extends QpidBrokerTestCase implements MessageListener
{
    private static final Logger _logger = LoggerFactory.getLogger(PropertyValueTest.class);

    private Connection _connection;
    private Destination _destination;
    private Session _session;
    private final List<TextMessage> received = new ArrayList<>();
    private final List<String> messages = new ArrayList<String>();
    private Map<String, Destination> _replyToDestinations;
    private int _count = 1;

    protected void setUp() throws Exception
    {
        _replyToDestinations = new HashMap<String, Destination>();
        super.setUp();
    }

    protected void tearDown() throws Exception
    {
        super.tearDown();
    }

    private void init(Connection connection) throws Exception
    {
        _connection = connection;
        _session = connection.createSession(true, Session.SESSION_TRANSACTED);
        _destination = createTestQueue(_session);

        // set up a slow consumer
        connection.start();
        _session.createConsumer(_destination).setMessageListener(this);
    }

    private Message getTestMessage() throws Exception
    {
        Connection conn = getConnection();
        Session ssn = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        return ssn.createTextMessage();
    }

    public void testGetNonexistent() throws Exception
    {
        Message m = getTestMessage();
        String s = m.getStringProperty("nonexistent");
        assertNull(s);
    }

    private static final String[] NAMES = {
        "setBooleanProperty", "setByteProperty", "setShortProperty",
        "setIntProperty", "setLongProperty", "setFloatProperty",
        "setDoubleProperty", "setObjectProperty"
    };

    private static final Class[] TYPES = {
        boolean.class, byte.class, short.class, int.class, long.class,
        float.class, double.class, Object.class
    };

    private static final Object[] VALUES = {
        true, (byte) 0, (short) 0, 0, (long) 0, (float) 0, (double) 0,
        new Object()
    };

    public void testSetEmptyPropertyName() throws Exception
    {
        Message m = getTestMessage();

        for (int i = 0; i < NAMES.length; i++)
        {
            Method meth = m.getClass().getMethod(NAMES[i], String.class, TYPES[i]);
            try
            {
                meth.invoke(m, "", VALUES[i]);
                fail("expected illegal argument exception");
            }
            catch (InvocationTargetException e)
            {
                assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
            }
        }
    }

    public void testSetDisallowedClass() throws Exception
    {
        Message m = getTestMessage();
        try
        {
            m.setObjectProperty("foo", new Object());
            fail("expected a MessageFormatException");
        }
        catch (MessageFormatException e)
        {
            // pass
        }
    }

    public void testOnce()
    {
        runBatch(1);
    }

    public void test50()
    {
        runBatch(50);
    }


    /**
     * This fails because of QPID-6793
     */
    /*
    public void testLargeHeader_010_HeaderLargerThan16Bit() throws Exception
    {
        _connection = (AMQConnection) getConnection();
        int maximumFrameSize = (int) _connection.getMaximumFrameSize();
        String propertyName = "string";
        String propertyValue = generateLongString(1<<16);
        sendReceiveMessageWithHeader(_connection, propertyName, propertyValue);
    }
    */

    /**
     * Test QPID-6786
     */
    public void testLargeHeader_010_HeadersFillContentHeaderFrame() throws Exception
    {
        _connection = getConnection();
        int maximumFrameSize = (int) ((AMQConnection)_connection).getMaximumFrameSize();
        Map<String, String> headerProperties = new HashMap<>();
        int headerPropertySize = ((1<<16) - 1);
        int i = 0;
        do
        {
            String propertyName = "string_" + i;
            String propertyValue = generateLongString((1<<16) - 1);
            headerProperties.put(propertyName, propertyValue);
            ++i;
        }
        while (headerProperties.size() * headerPropertySize < 2 * maximumFrameSize);

        _connection.start();
        Session session = _connection.createSession(true, Session.SESSION_TRANSACTED);

        Destination destination = session.createQueue(getTestQueueName());

        Message message = session.createMessage();
        for (Map.Entry<String, String> propertyEntry : headerProperties.entrySet())
        {
            message.setStringProperty(propertyEntry.getKey(), propertyEntry.getValue());
        }

        MessageConsumer consumer = session.createConsumer(destination);

        MessageProducer producer = session.createProducer(destination);
        producer.setDisableMessageID(true);
        producer.setDisableMessageTimestamp(true);
        producer.send(message);
        session.commit();

        Message receivedMessage = consumer.receive(1000);
        assertNotNull("Message not received", receivedMessage);
        for (Map.Entry<String, String> propertyEntry : headerProperties.entrySet())
        {
            assertEquals("Message has unexpected property value",
                         propertyEntry.getValue(),
                         receivedMessage.getStringProperty(propertyEntry.getKey()));
        }
        session.commit();

    }

    public void testLargeHeader_08091_HeadersFillContentHeaderFrame() throws Exception
    {
        _connection =  getConnection();
        int maximumFrameSize = (int) ((AMQConnection)_connection).getMaximumFrameSize();
        String propertyName = "string";
        int overhead = calculateOverHead_08091_FrameWithHeader(propertyName);

        String propertyValue = generateLongString(maximumFrameSize - overhead);
        sendReceiveMessageWithHeader(_connection, propertyName, propertyValue);
    }

    public void testOverlyLargeHeaderRejected_08091() throws Exception
    {
        _connection = (AMQConnection) getConnection();
        _session = _connection.createSession(true, Session.SESSION_TRANSACTED);
        MessageProducer producer = _session.createProducer(_session.createQueue(getTestQueueName()));

        int maximumFrameSize = (int) ((AMQConnection)_connection).getMaximumFrameSize();
        String propertyName = "string";
        int overhead = calculateOverHead_08091_FrameWithHeader(propertyName);

        String propertyValue = generateLongString(maximumFrameSize - overhead + 1);
        try
        {
            Message m = _session.createMessage();
            m.setStringProperty(propertyName, propertyValue);
            producer.send(m);
            fail("Exception not thrown");
        }
        catch (JMSException je)
        {
            assertTrue("Unexpected message " + je.getMessage(), je.getMessage().contains("Unable to send message as the headers are too large"));
            // PASS
        }
    }


    private int calculateOverHead_08091_FrameWithHeader(final String propertyName)
    {
        int frame = 1 + 2 + 4 + 1;
        int body = 2 + 2 + 8 + 2;
        int properties = GUEST_USERNAME.length() + 1  // Username + length
                         + 1 // DeliveryMode byte
                         + 1 // Priority byte
                         + "application/octet-stream".length() + 1 // Encoding + length
                         + 4  // Headers field table
                         + "JMS_QPID_DESTTYPE".length() + 1 + 1 + 4
                         + propertyName.length() + 1 + 1 + 4;
        return frame + body + properties;
    }

    private String generateLongString(final int count)
    {
        String pattern = "abcde";
        String str = Strings.repeat(pattern, count / pattern.length()) + pattern.substring(0, count % pattern.length());
        assertEquals(count, str.length());
        return str;
    }

    private void sendReceiveMessageWithHeader(Connection connection,
                                              final String propName, final String propValue) throws Exception
    {
        connection.start();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

        Destination destination = session.createQueue(getTestQueueName());

        Message message = session.createMessage();
        message.setStringProperty(propName, propValue);

        MessageConsumer consumer = session.createConsumer(destination);

        MessageProducer producer = session.createProducer(destination);
        producer.setDisableMessageID(true);
        producer.setDisableMessageTimestamp(true);
        producer.send(message);
        session.commit();

        Message receivedMessage = consumer.receive(1000);
        assertNotNull("Message not received", receivedMessage);
        assertEquals("Message has unexpected property value", propValue, receivedMessage.getStringProperty(propName));
        session.commit();
    }

    private void runBatch(int runSize)
    {
        try
        {
            int run = 0;
            while (run < runSize)
            {
                _logger.error("Run Number:" + run++);
                try
                {
                    init(getConnection());
                }
                catch (Exception e)
                {
                    _logger.error("exception:", e);
                    fail("Unable to initialise connection: " + e);
                }

                int count = _count;
                send(count, run);
                waitFor(count);
                check();
                _logger.info("Completed without failure");

                _connection.close();

                _logger.error("End Run Number:" + (run - 1));
            }
        }
        catch (Exception e)
        {
            _logger.error(e.getMessage(), e);
        }
    }

    void send( int count, final int iteration) throws JMSException
    {
        // create a publisher
        MessageProducer producer = _session.createProducer(_destination);
        for (int i = 0; i < count; i++)
        {
            String text = "Message " + iteration;
            messages.add(text);
            Message m = _session.createTextMessage(text);

            m.setBooleanProperty("Bool", true);

            m.setByteProperty("Byte", (byte) Byte.MAX_VALUE);
            m.setDoubleProperty("Double", (double) Double.MAX_VALUE);
            m.setFloatProperty("Float", (float) Float.MAX_VALUE);
            m.setIntProperty("Int", (int) Integer.MAX_VALUE);

            m.setJMSCorrelationID("Correlation");
            // fixme the m.setJMSMessage has no effect
            producer.setPriority(8);
            m.setJMSPriority(3);

            // Queue
            Queue q;

            if ((i / 2) == 0)
            {
                q = _session.createTemporaryQueue();
            }
            else
            {
                q = _session.createQueue("TestReply");
            }

            m.setJMSReplyTo(q);

            m.setStringProperty("ReplyToIndex", String.valueOf(i));
            _replyToDestinations.put(String.valueOf(i), q);

            _logger.debug("Message:" + m);

            m.setJMSType("Test");
            m.setLongProperty("UnsignedInt", (long) 4294967295L);
            m.setLongProperty("Long", (long) Long.MAX_VALUE);

            m.setShortProperty("Short", (short) Short.MAX_VALUE);
            m.setStringProperty("String", "Test");

            _logger.debug("Sending Msg:" + m);
            producer.send(m);
            _session.commit();
        }
    }

    void waitFor(int count) throws Exception
    {
        synchronized (received)
        {
            while (received.size() < count)
            {
                received.wait();
            }
        }
        _session.commit();
    }

    void check() throws JMSException, URISyntaxException
    {
        List<String> actual = new ArrayList<>();
        for (TextMessage m : received)
        {
            getLogger().debug("Checking : message : {} text payload: {}", m, m.getText());
            actual.add(m.getText());

            // Check Properties

            Assert.assertEquals("Check Boolean properties are correctly transported", true, m.getBooleanProperty("Bool"));
            Assert.assertEquals("Check Byte properties are correctly transported", Byte.MAX_VALUE,
                m.getByteProperty("Byte"));
            Assert.assertEquals("Check Double properties are correctly transported", Double.MAX_VALUE,
                m.getDoubleProperty("Double"), 0d);
            Assert.assertEquals("Check Float properties are correctly transported", Float.MAX_VALUE,
                m.getFloatProperty("Float"), 0f);
            Assert.assertEquals("Check Int properties are correctly transported", Integer.MAX_VALUE,
                m.getIntProperty("Int"));
            Assert.assertEquals("Check CorrelationID properties are correctly transported", "Correlation",
                m.getJMSCorrelationID());
            Assert.assertEquals("Check Priority properties are correctly transported", 8, m.getJMSPriority());

            // Queue
            String replyToIndex = m.getStringProperty("ReplyToIndex");
            Assert.assertEquals("Check ReplyTo properties are correctly transported :" + replyToIndex + " : " + _replyToDestinations, _replyToDestinations.get(replyToIndex), m.getJMSReplyTo());

            Assert.assertEquals("Check Type properties are correctly transported", "Test", m.getJMSType());

            Assert.assertEquals("Check Short properties are correctly transported", (short) Short.MAX_VALUE,
                m.getShortProperty("Short"));
            Assert.assertEquals("Check UnsignedInt properties are correctly transported", (long) 4294967295L,
                m.getLongProperty("UnsignedInt"));
            Assert.assertEquals("Check Long properties are correctly transported", (long) Long.MAX_VALUE,
                m.getLongProperty("Long"));
            Assert.assertEquals("Check String properties are correctly transported", "Test", m.getStringProperty("String"));

            //JMSXUserID
            if (m.getStringProperty("JMSXUserID") != null)
            {
                Assert.assertEquals("Check 'JMSXUserID' is supported ", QpidBrokerTestCase.GUEST_USERNAME,
                                    m.getStringProperty("JMSXUserID"));
            }
        }

        received.clear();

        assertEqual(messages.iterator(), actual.iterator());

        messages.clear();
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
            received.add((TextMessage) message);
            received.notify();
        }
    }


}
