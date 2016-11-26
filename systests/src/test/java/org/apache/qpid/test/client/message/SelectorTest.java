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
package org.apache.qpid.test.client.message;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.test.utils.QpidBrokerTestCase;

public class SelectorTest extends QpidBrokerTestCase implements MessageListener
{
    private static final Logger _logger = LoggerFactory.getLogger(SelectorTest.class);

    private Connection _connection;
    private Queue _destination;
    private int count;
    private static final String INVALID_SELECTOR = "Cost LIKE 5";
    CountDownLatch _responseLatch = new CountDownLatch(1);

    private static final String BAD_MATHS_SELECTOR = " 1 % 5";

    protected void setUp() throws Exception
    {
        super.setUp();
        _connection = getConnection();
        Session session = _connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        _destination = createTestQueue(session);
        session.close();
        _connection.start();
    }

    public void onMessage(Message message)
    {
        count++;
        _logger.info("Got Message:" + message);
        _responseLatch.countDown();
    }

    public void testUsingOnMessage() throws Exception
    {
        String selector = "Cost = 2 AND foo = 'wibble'";
        Session session = _connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createConsumer(_destination, selector).setMessageListener(this);

        Message msg = session.createTextMessage("Message");
        msg.setJMSPriority(1);
        msg.setIntProperty("Cost", 2);
        msg.setStringProperty("foo", "wibble");
        msg.setJMSType("Special");

        _logger.info("Sending Message:" + msg);

        final MessageProducer producer = session.createProducer(_destination);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        producer.send(msg);
        _logger.info("Message sent, waiting for response...");

        _responseLatch.await(getReceiveTimeout(), TimeUnit.MILLISECONDS);

        if (count > 0)
        {
            _logger.info("Got message");
        }

        if (count == 0)
        {
            fail("Did not get message!");
            // throw new RuntimeException("Did not get message!");
        }

    }

    public void testUnparsableSelectors() throws Exception
    {
        Session session = _connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        boolean caught = false;

        //Try Creating a Browser
        try
        {
            session.createBrowser(session.createQueue("Ping"), INVALID_SELECTOR);
        }
        catch (JMSException e)
        {
            _logger.debug("JMS:" + e.getClass().getSimpleName() + ":" + e.getMessage());
            if (!(e instanceof InvalidSelectorException))
            {
                fail("Wrong exception:" + e.getMessage());
            }
            caught = true;
        }
        assertTrue("No exception thrown!", caught);
        caught = false;

        //Try Creating a Consumer
        try
        {
            session.createConsumer(session.createQueue("Ping"), INVALID_SELECTOR);
        }
        catch (JMSException e)
        {
            _logger.debug("JMS:" + e.getClass().getSimpleName() + ":" + e.getMessage());
            if (!(e instanceof InvalidSelectorException))
            {
                fail("Wrong exception:" + e.getMessage());
            }
            caught = true;
        }
        assertTrue("No exception thrown!", caught);
        caught = false;

        //Try Creating a Receiever
        try
        {
            session.createConsumer(session.createQueue("Ping"), INVALID_SELECTOR);
        }
        catch (JMSException e)
        {
            _logger.debug("JMS:" + e.getClass().getSimpleName() + ":" + e.getMessage());
            if (!(e instanceof InvalidSelectorException))
            {
                fail("Wrong exception:" + e.getMessage());
            }
            caught = true;
        }
        assertTrue("No exception thrown!", caught);
        caught = false;

        try
        {
            session.createConsumer(session.createQueue("Ping"), BAD_MATHS_SELECTOR);
        }
        catch (JMSException e)
        {
            _logger.debug("JMS:" + e.getClass().getSimpleName() + ":" + e.getMessage());
            if (!(e instanceof InvalidSelectorException))
            {
                fail("Wrong exception:" + e.getMessage());
            }
            caught = true;
        }
        assertTrue("No exception thrown!", caught);

    }

    public void testRuntimeSelectorError() throws JMSException
    {
        Session session = _connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(_destination , "testproperty % 5 = 1");
        MessageProducer producer = session.createProducer(_destination);
        Message sentMsg = session.createTextMessage();

        sentMsg.setIntProperty("testproperty", 1); // 1 % 5
        producer.send(sentMsg);
        Message recvd = consumer.receive(RECEIVE_TIMEOUT);
        assertNotNull("Message matching selector should be received", recvd);

        sentMsg.setStringProperty("testproperty", "hello"); // "hello" % 5 makes no sense
        producer.send(sentMsg);
        recvd = consumer.receive(RECEIVE_TIMEOUT);
        assertNull("Message causing runtime selector error should be received", recvd);

    }

    public void testSelectorWithJMSMessageID() throws Exception
    {
        Session session = _connection.createSession(true, Session.SESSION_TRANSACTED);

        MessageProducer prod = session.createProducer(_destination);
        MessageConsumer consumer = session.createConsumer(_destination,"JMSMessageID IS NOT NULL");

        for (int i=0; i<2; i++)
        {
            Message msg = session.createTextMessage("Msg" + String.valueOf(i));
            prod.send(msg);
        }
        session.commit();

        Message msg1 = consumer.receive(1000);
        Message msg2 = consumer.receive(1000);

        Assert.assertNotNull("Msg1 should not be null", msg1);
        Assert.assertNotNull("Msg2 should not be null", msg2);

        session.commit();

        prod.setDisableMessageID(true);

        for (int i=2; i<4; i++)
        {
            Message msg = session.createTextMessage("Msg" + String.valueOf(i));
            prod.send(msg);
        }

        session.commit();
        Message msg3 = consumer.receive(1000);
        Assert.assertNull("Msg3 should be null", msg3);
        session.commit();
        consumer = session.createConsumer(_destination,"JMSMessageID IS NULL");

        Message msg4 = consumer.receive(1000);
        Message msg5 = consumer.receive(1000);
        session.commit();
        Assert.assertNotNull("Msg4 should not be null", msg4);
        Assert.assertNotNull("Msg5 should not be null", msg5);
    }

    public void testSelectorWithJMSDeliveryMode() throws Exception
    {
        Session session = _connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Destination dest1 = createTopic(_connection, "test1");
        Destination dest2 = createTopic(_connection, "test2");

        MessageProducer prod1 = session.createProducer(dest1);
        MessageProducer prod2 = session.createProducer(dest2);
        MessageConsumer consumer1 = session.createConsumer(dest1,"JMSDeliveryMode = 'PERSISTENT'");
        MessageConsumer consumer2 = session.createConsumer(dest2,"JMSDeliveryMode = 'NON_PERSISTENT'");

        Message msg1 = session.createTextMessage("Persistent");
        prod1.send(msg1);
        prod2.send(msg1);

        prod1.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        prod2.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        Message msg2 = session.createTextMessage("Non_Persistent");
        prod1.send(msg2);
        prod2.send(msg2);

        TextMessage m1 = (TextMessage)consumer1.receive(1000);
        assertEquals("Consumer1 should receive the persistent message","Persistent",m1.getText());
        assertNull("Consumer1 should not receiver another message",consumer1.receive(1000));

        TextMessage m2 = (TextMessage)consumer2.receive(1000);
        assertEquals("Consumer2 should receive the non persistent message","Non_Persistent",m2.getText());
        assertNull("Consumer2 should not receiver another message",consumer2.receive(1000));
    }

}
