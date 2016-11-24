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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageNotWriteableException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.test.utils.QpidBrokerTestCase;

public class TextMessageTest extends QpidBrokerTestCase implements MessageListener
{
    private static final Logger _logger = LoggerFactory.getLogger(TextMessageTest.class);

    private Connection _connection;
    private Destination _destination;
    private Session _session;
    private final List<Message> received = new ArrayList<>();
    private final List<String> messages = new ArrayList<String>();
    private int _count = 100;
    private CountDownLatch _waitForCompletion;
    private MessageConsumer _consumer;

    protected void setUp() throws Exception
    {
        super.setUp();
        try
        {
            init(getConnection("guest", "guest"));
        }
        catch (Exception e)
        {
            fail("Unable to initialilse connection: " + e);
        }
    }

    protected void tearDown() throws Exception
    {
        super.tearDown();
    }

    private void init(Connection connection) throws Exception
    {
        _connection = connection;
        connection.start();
        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = createTestQueue(session);
        session.close();
        _session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        _destination = destination;

        // set up a slow consumer
        try
        {
            _consumer = _session.createConsumer(destination);
            _consumer.setMessageListener(this);
        }
        catch (Throwable  e)
        {
            _logger.error("Error creating consumer", e);
        }
    }

    public void test() throws Exception
    {
        int count = _count;
        _waitForCompletion = new CountDownLatch(_count);
        send(count);
        _waitForCompletion.await(20, TimeUnit.SECONDS);
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
            String text = "Message " + i;
            messages.add(text);
            Message m = _session.createTextMessage(text);
            //m.setStringProperty("String", "hello");

            _logger.info("Sending Msg:" + m);
            producer.send(m);
        }
        _logger.info("sent " + count  + " mesages");
    }


    void check() throws JMSException
    {
        List<String> actual = new ArrayList<String>();
        for (Message message : received)
        {
            assertTrue("Message is not a text message", message instanceof TextMessage);
            TextMessage textMessage = (TextMessage)message;
            actual.add(textMessage.getText());

            // Check body write status
            try
            {
                textMessage.setText("Test text");
                Assert.fail("Message should not be writeable");
            }
            catch (MessageNotWriteableException mnwe)
            {
                // normal execution
            }

            textMessage.clearBody();

            try
            {
                textMessage.setText("Test text");
            }
            catch (MessageNotWriteableException mnwe)
            {
                Assert.fail("Message should be writeable");
            }

            // Check property write status
            try
            {
                textMessage.setStringProperty("test", "test");
                Assert.fail("Message should not be writeable");
            }
            catch (MessageNotWriteableException mnwe)
            {
                // normal execution
            }

            textMessage.clearProperties();

            try
            {
                textMessage.setStringProperty("test", "test");
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
            _logger.info("===== received one message");
            received.add(message);
            _waitForCompletion.countDown();
        }
    }

}
