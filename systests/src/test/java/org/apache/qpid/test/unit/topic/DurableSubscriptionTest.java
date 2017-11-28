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
package org.apache.qpid.test.unit.topic;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.InvalidDestinationException;
import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicSubscriber;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.client.AMQConnection;
import org.apache.qpid.client.AMQNoRouteException;
import org.apache.qpid.client.AMQQueue;
import org.apache.qpid.client.AMQSession;
import org.apache.qpid.test.utils.QpidBrokerTestCase;

/**
 *  The tests in the suite only test 0-x client specific behaviour.
 *  The tests should be moved into client or removed
 */
public class DurableSubscriptionTest extends QpidBrokerTestCase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DurableSubscriptionTest.class);

    private static final String MY_TOPIC = "MyTopic";

    private static final String MY_SUBSCRIPTION = "MySubscription";

    /**
     * Specifically uses a subscriber with a selector because QPID-4731 found that selectors
     * can prevent queue removal.
     */
    public void testUnsubscribeWhenUsingSelectorMakesTopicUnreachable() throws Exception
    {
        setTestClientSystemProperty("qpid.default_mandatory_topic","true");

        // set up subscription
        AMQConnection connection = (AMQConnection) getConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = createTopic(connection, MY_TOPIC);
        MessageProducer producer = session.createProducer(topic);

        TopicSubscriber subscriber = session.createDurableSubscriber(topic, MY_SUBSCRIPTION, "1 = 1", false);
        StoringExceptionListener exceptionListener = new StoringExceptionListener();
        connection.setExceptionListener(exceptionListener);

        // send message and verify it was consumed
        producer.send(session.createTextMessage("message1"));
        assertNotNull("Message should have been successfully received", subscriber.receive(getReceiveTimeout()));
        assertEquals(null, exceptionListener.getException());
        session.unsubscribe(MY_SUBSCRIPTION);

        // send another message and verify that the connection exception listener was fired.
        StoringExceptionListener exceptionListener2 = new StoringExceptionListener();
        connection.setExceptionListener(exceptionListener2);

        producer.send(session.createTextMessage("message that should be unroutable"));
        ((AMQSession<?, ?>) session).sync();

        JMSException exception = exceptionListener2.awaitException();
        assertNotNull("Expected exception as message should no longer be routable", exception);

        Throwable linkedException = exception.getLinkedException();
        assertNotNull("The linked exception of " + exception + " should be the 'no route' exception", linkedException);
        assertEquals(AMQNoRouteException.class, linkedException.getClass());
    }

    private final class StoringExceptionListener implements ExceptionListener
    {
        private volatile JMSException _exception;
        private CountDownLatch _latch = new CountDownLatch(1);

        @Override
        public void onException(JMSException exception)
        {
            _exception = exception;
            LOGGER.info("Exception listener received: " + exception);
            _latch.countDown();
        }

        public JMSException awaitException() throws InterruptedException
        {
            _latch.await(getReceiveTimeout(), TimeUnit.MILLISECONDS);
            return _exception;
        }

        public JMSException getException()
        {
            return _exception;
        }
    }

    public void testDurabilityNOACK() throws Exception
    {
        durabilityImpl(AMQSession.NO_ACKNOWLEDGE, false);
    }

    public void testDurabilityAUTOACK() throws Exception
    {
        durabilityImpl(Session.AUTO_ACKNOWLEDGE, false);
    }
    
    public void testDurabilityAUTOACKwithRestartIfPersistent() throws Exception
    {
        if(!isBrokerStorePersistent())
        {
            LOGGER.warn("The broker store is not persistent, skipping this test");
            return;
        }
        
        durabilityImpl(Session.AUTO_ACKNOWLEDGE, true);
    }

    public void testDurabilityNOACKSessionPerConnection() throws Exception
    {
        durabilityImplSessionPerConnection(AMQSession.NO_ACKNOWLEDGE);
    }

    public void testDurabilityAUTOACKSessionPerConnection() throws Exception
    {
        durabilityImplSessionPerConnection(Session.AUTO_ACKNOWLEDGE);
    }

    private void durabilityImpl(int ackMode, boolean restartBroker) throws Exception
    {        
        TopicConnection con = (TopicConnection) getConnection();
        Topic topic = createTopic(con, MY_TOPIC);
        Session session1 = con.createSession(false, ackMode);
        MessageConsumer consumer1 = session1.createConsumer(topic);

        Session sessionProd = con.createSession(false, ackMode);
        MessageProducer producer = sessionProd.createProducer(topic);

        Session session2 = con.createSession(false, ackMode);
        TopicSubscriber consumer2 = session2.createDurableSubscriber(topic, MY_SUBSCRIPTION);

        con.start();

        //send message A and check both consumers receive
        producer.send(session1.createTextMessage("A"));

        Message msg;
        LOGGER.info("Receive message on consumer 1 :expecting A");
        msg = consumer1.receive(getReceiveTimeout());
        assertNotNull("Message should have been received",msg);
        assertEquals("A", ((TextMessage) msg).getText());
        msg = consumer1.receive(getShortReceiveTimeout());
        assertEquals(null, msg);

        LOGGER.info("Receive message on consumer 2 :expecting A");
        msg = consumer2.receive(getReceiveTimeout());
        assertNotNull("Message should have been received",msg);
        assertEquals("A", ((TextMessage) msg).getText());
        msg = consumer2.receive(getShortReceiveTimeout());
        assertEquals(null, msg);

        //send message B, receive with consumer 1, and disconnect consumer 2 to leave the message behind (if not NO_ACK)
        producer.send(session1.createTextMessage("B"));

        LOGGER.info("Receive message on consumer 1 :expecting B");
        msg = consumer1.receive(getReceiveTimeout());
        assertNotNull("Consumer 1 should get message 'B'.", msg);
        assertEquals("Incorrect Message received on consumer1.", "B", ((TextMessage) msg).getText());
        LOGGER.info("Receive message on consumer 1 :expecting null");
        msg = consumer1.receive(getShortReceiveTimeout());
        assertNull("There should be no more messages for consumption on consumer1.", msg);

        consumer2.close();
        session2.close();

        //Send message C, then connect consumer 3 to durable subscription and get
        //message B if not using NO_ACK, then receive C with consumer 1 and 3
        producer.send(session1.createTextMessage("C"));

        Session session3 = con.createSession(false, ackMode);
        MessageConsumer consumer3 = session3.createDurableSubscriber(topic, MY_SUBSCRIPTION);

        if(ackMode == AMQSession.NO_ACKNOWLEDGE)
        {
            //Do nothing if NO_ACK was used, as prefetch means the message was dropped
            //when we didn't call receive() to get it before closing consumer 2
        }
        else
        {
            LOGGER.info("Receive message on consumer 3 :expecting B");
            msg = consumer3.receive(getReceiveTimeout());
            assertNotNull("Consumer 3 should get message 'B'.", msg);
            assertEquals("Incorrect Message received on consumer3.", "B", ((TextMessage) msg).getText());
        }

        LOGGER.info("Receive message on consumer 1 :expecting C");
        msg = consumer1.receive(getReceiveTimeout());
        assertNotNull("Consumer 1 should get message 'C'.", msg);
        assertEquals("Incorrect Message received on consumer1.", "C", ((TextMessage) msg).getText());
        LOGGER.info("Receive message on consumer 1 :expecting null");
        msg = consumer1.receive(getShortReceiveTimeout());
        assertNull("There should be no more messages for consumption on consumer1.", msg);

        LOGGER.info("Receive message on consumer 3 :expecting C");
        msg = consumer3.receive(getReceiveTimeout());
        assertNotNull("Consumer 3 should get message 'C'.", msg);
        assertEquals("Incorrect Message received on consumer3.", "C", ((TextMessage) msg).getText());
        LOGGER.info("Receive message on consumer 3 :expecting null");
        msg = consumer3.receive(getShortReceiveTimeout());
        assertNull("There should be no more messages for consumption on consumer3.", msg);

        consumer1.close();
        consumer3.close();

        session3.unsubscribe(MY_SUBSCRIPTION);

        con.close();
        
        if(restartBroker)
        {
            try
            {
                restartDefaultBroker();
            }
            catch (Exception e)
            {
                fail("Error restarting the broker");
            }
        }
    }

    private void durabilityImplSessionPerConnection(int ackMode) throws Exception
    {
        Message msg;
        // Create producer.
        TopicConnection con0 = (TopicConnection) getConnection();
        con0.start();
        Session session0 = con0.createSession(false, ackMode);

        Topic topic = createTopic(con0, MY_TOPIC);

        Session sessionProd = con0.createSession(false, ackMode);
        MessageProducer producer = sessionProd.createProducer(topic);

        // Create consumer 1.
        Connection con1 = getConnection();
        con1.start();
        Session session1 = con1.createSession(false, ackMode);

        MessageConsumer consumer1 = session1.createConsumer(topic);

        // Create consumer 2.
        Connection con2 = getConnection();
        con2.start();
        Session session2 = con2.createSession(false, ackMode);

        TopicSubscriber consumer2 = session2.createDurableSubscriber(topic, MY_SUBSCRIPTION);

        // Send message and check that both consumers get it and only it.
        producer.send(session0.createTextMessage("A"));

        msg = consumer1.receive(getReceiveTimeout());
        assertNotNull("Message should be available", msg);
        assertEquals("Message Text doesn't match", "A", ((TextMessage) msg).getText());
        msg = consumer1.receive(getShortReceiveTimeout());
        assertNull("There should be no more messages for consumption on consumer1.", msg);

        msg = consumer2.receive(getReceiveTimeout());
        assertNotNull("Message should have been received",msg);
        assertEquals("Consumer 2 should also received the first msg.", "A", ((TextMessage) msg).getText());
        msg = consumer2.receive(getShortReceiveTimeout());
        assertNull("There should be no more messages for consumption on consumer2.", msg);

        // Send message and receive on consumer 1.
        producer.send(session0.createTextMessage("B"));

        LOGGER.info("Receive message on consumer 1 :expecting B");
        msg = consumer1.receive(getReceiveTimeout());
        assertEquals("B", ((TextMessage) msg).getText());
        LOGGER.info("Receive message on consumer 1 :expecting null");
        msg = consumer1.receive(getShortReceiveTimeout());
        assertEquals(null, msg);
        
        // Detach the durable subscriber.
        consumer2.close();
        session2.close();
        con2.close();
        
        // Send message C and receive on consumer 1
        producer.send(session0.createTextMessage("C"));

        LOGGER.info("Receive message on consumer 1 :expecting C");
        msg = consumer1.receive(getReceiveTimeout());
        assertEquals("C", ((TextMessage) msg).getText());
        LOGGER.info("Receive message on consumer 1 :expecting null");
        msg = consumer1.receive(getShortReceiveTimeout());
        assertEquals(null, msg);

        // Re-attach a new consumer to the durable subscription, and check that it gets message B it left (if not NO_ACK)
        // and also gets message C sent after it was disconnected.
        AMQConnection con3 = (AMQConnection) getConnection();
        con3.start();
        Session session3 = con3.createSession(false, ackMode);

        TopicSubscriber consumer3 = session3.createDurableSubscriber(topic, MY_SUBSCRIPTION);

        if(ackMode == AMQSession.NO_ACKNOWLEDGE)
        {
            //Do nothing if NO_ACK was used, as prefetch means the message was dropped
            //when we didn't call receive() to get it before closing consumer 2
        }
        else
        {
            LOGGER.info("Receive message on consumer 3 :expecting B");
            msg = consumer3.receive(getReceiveTimeout());
            assertNotNull(msg);
            assertEquals("B", ((TextMessage) msg).getText());
        }
        
        LOGGER.info("Receive message on consumer 3 :expecting C");
        msg = consumer3.receive(getReceiveTimeout());
        assertNotNull("Consumer 3 should get message 'C'.", msg);
        assertEquals("Incorrect Message recevied on consumer3.", "C", ((TextMessage) msg).getText());
        LOGGER.info("Receive message on consumer 3 :expecting null");
        msg = consumer3.receive(getShortReceiveTimeout());
        assertNull("There should be no more messages for consumption on consumer3.", msg);

        consumer1.close();
        consumer3.close();

        session3.unsubscribe(MY_SUBSCRIPTION);

        con0.close();
        con1.close();
        con3.close();
    }

    /**
     * This tests the fix for QPID-1085
     * Creates a durable subscriber with an invalid selector, checks that the
     * exception is thrown correctly and that the subscription is not created. 
     * @throws Exception 
     */
    public void testDurableWithInvalidSelector() throws Exception
    {
    	Connection conn = getConnection();
    	conn.start();
    	Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
    	Topic topic = createTopic(conn, "MyTestDurableWithInvalidSelectorTopic");
    	MessageProducer producer = session.createProducer(topic);
    	producer.send(session.createTextMessage("testDurableWithInvalidSelector1"));
    	try 
    	{
    		TopicSubscriber deadSubscriber = session.createDurableSubscriber(topic, "testDurableWithInvalidSelectorSub",
																	 		 "=TEST 'test", true);
    		assertNull("Subscriber should not have been created", deadSubscriber);
    	} 
    	catch (JMSException e)
    	{
    		assertTrue("Wrong type of exception thrown", e instanceof InvalidSelectorException);
    	}
    	TopicSubscriber liveSubscriber = session.createDurableSubscriber(topic, "testDurableWithInvalidSelectorSub");
    	assertNotNull("Subscriber should have been created", liveSubscriber);

    	producer.send(session.createTextMessage("testDurableWithInvalidSelector2"));
    	
    	Message msg = liveSubscriber.receive(getReceiveTimeout());
    	assertNotNull ("Message should have been received", msg);
    	assertEquals ("testDurableWithInvalidSelector2", ((TextMessage) msg).getText());
    	assertNull("Should not receive subsequent message", liveSubscriber.receive(getShortReceiveTimeout()));
        liveSubscriber.close();
        session.unsubscribe("testDurableWithInvalidSelectorSub");
    }
    
    /**
     * This tests the fix for QPID-1085
     * Creates a durable subscriber with an invalid destination, checks that the
     * exception is thrown correctly and that the subscription is not created. 
     * @throws Exception 
     */
    public void testDurableWithInvalidDestination() throws Exception
    {
    	Connection conn = getConnection();
    	conn.start();
    	Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
    	Topic topic = createTopic(conn, "testDurableWithInvalidDestinationTopic");
    	try 
    	{
    		TopicSubscriber deadSubscriber = session.createDurableSubscriber(null, "testDurableWithInvalidDestinationsub");
    		assertNull("Subscriber should not have been created", deadSubscriber);
    	} 
    	catch (InvalidDestinationException e)
    	{
    		// This was expected
    	}
    	MessageProducer producer = session.createProducer(topic);    	
    	producer.send(session.createTextMessage("testDurableWithInvalidSelector1"));
    	
    	TopicSubscriber liveSubscriber = session.createDurableSubscriber(topic, "testDurableWithInvalidDestinationsub");
    	assertNotNull("Subscriber should have been created", liveSubscriber);
    	
    	producer.send(session.createTextMessage("testDurableWithInvalidSelector2"));
    	Message msg = liveSubscriber.receive(getReceiveTimeout());
    	assertNotNull ("Message should have been received", msg);
    	assertEquals ("testDurableWithInvalidSelector2", ((TextMessage) msg).getText());
    	assertNull("Should not receive subsequent message", liveSubscriber.receive(getShortReceiveTimeout()));

        session.unsubscribe("testDurableWithInvalidDestinationsub");
    }

    /**
     * <ul>
     * <li>create and register a durable subscriber with a message selector
     * <li>create another durable subscriber with a different selector and same name
     * <li>check first subscriber is now closed
     * <li>create a publisher and send messages
     * <li>check messages are received correctly
     * </ul>
     * <p>
     * QPID-2418
     *
     * TODO: it seems that client behaves in not jms spec compliant:
     * the client allows subscription recreation with a new selector whilst an active subscriber is connected
     */
    public void testResubscribeWithChangedSelectorNoClose() throws Exception
    {
        Connection conn = getConnection();
        conn.start();
        Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = createTopic(conn, "testResubscribeWithChangedSelectorNoClose");
        
        // Create durable subscriber that matches A
        TopicSubscriber subA = session.createDurableSubscriber(topic, 
                "testResubscribeWithChangedSelectorNoClose",
                "Match = True", false);
        
        // Reconnect with new selector that matches B
        TopicSubscriber subB = session.createDurableSubscriber(topic, 
                "testResubscribeWithChangedSelectorNoClose",
                "Match = false", false);
        
        // First subscription has been closed
        try
        {
            subA.receive(getShortReceiveTimeout());
            fail("First subscription was not closed");
        }
        catch (Exception e)
        {
            LOGGER.error("Receive error",e);
        }

        conn.stop();
        
        // Send 1 matching message and 1 non-matching message
        MessageProducer producer = session.createProducer(topic);
        TextMessage msg = session.createTextMessage("testResubscribeWithChangedSelectorAndRestart1");
        msg.setBooleanProperty("Match", true);
        producer.send(msg);
        msg = session.createTextMessage("testResubscribeWithChangedSelectorAndRestart2");
        msg.setBooleanProperty("Match", false);
        producer.send(msg);

        // should be 1 or 2 messages on queue now
        // (1 for the Apache Qpid Broker-J due to use of server side selectors, and 2 for the cpp broker due to client side selectors only)
        AMQQueue queue = new AMQQueue("amq.topic", "clientid" + ":" + "testResubscribeWithChangedSelectorNoClose");
        assertEquals("Queue depth is wrong", isJavaBroker() ? 1 : 2, ((AMQSession<?, ?>) session).getQueueDepth(queue, true));

        conn.start();
        
        Message rMsg = subB.receive(getReceiveTimeout());
        assertNotNull(rMsg);
        assertEquals("Content was wrong", 
                     "testResubscribeWithChangedSelectorAndRestart2",
                     ((TextMessage) rMsg).getText());
        
        rMsg = subB.receive(getShortReceiveTimeout());
        assertNull(rMsg);
        
        // Check queue has no messages
        assertEquals("Queue should be empty", 0, ((AMQSession<?, ?>) session).getQueueDepth(queue, true));
        
        conn.close();
    }

    /**
     * <ul>
     * <li>create and register a durable subscriber with no message selector
     * <li>create another durable subscriber with a selector and same name
     * <li>check first subscriber is now closed
     * <li>create a publisher and send  messages
     * <li>check messages are received correctly
     * </ul>
     * <p>
     * QPID-2418
     *
     * TODO: it seems that client behaves in not jms spec compliant:
     * the client allows subscription recreation with a new selector whilst active subscriber is connected
     */
    public void testDurSubAddMessageSelectorNoClose() throws Exception
    {        
        Connection conn = getConnection();
        conn.start();
        Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = createTopic(conn, "subscriptionName");
                
        // create and register a durable subscriber with no message selector
        TopicSubscriber subOne = session.createDurableSubscriber(topic, "subscriptionName", null, false);

        // now create a durable subscriber with a selector
        TopicSubscriber subTwo = session.createDurableSubscriber(topic, "subscriptionName", "testprop = TRUE", false);

        // First subscription has been closed
        try
        {
            subOne.receive(getShortReceiveTimeout());
            fail("First subscription was not closed");
        }
        catch (Exception e)
        {
            LOGGER.error("Receive error",e);
        }

        conn.stop();
        
        // Send 1 matching message and 1 non-matching message
        MessageProducer producer = session.createProducer(topic);
        TextMessage msg = session.createTextMessage("testResubscribeWithChangedSelectorAndRestart1");
        msg.setBooleanProperty("testprop", true);
        producer.send(msg);
        msg = session.createTextMessage("testResubscribeWithChangedSelectorAndRestart2");
        msg.setBooleanProperty("testprop", false);
        producer.send(msg);

        // should be 1 or 2 messages on queue now
        // (1 for the Apache Qpid Broker-J due to use of server side selectors, and 2 for the cpp broker due to client side selectors only)
        AMQQueue queue = new AMQQueue("amq.topic", "clientid" + ":" + "subscriptionName");
        assertEquals("Queue depth is wrong", isJavaBroker() ? 1 : 2, ((AMQSession<?, ?>) session).getQueueDepth(queue, true));
        
        conn.start();
        
        Message rMsg = subTwo.receive(getReceiveTimeout());
        assertNotNull(rMsg);
        assertEquals("Content was wrong", 
                     "testResubscribeWithChangedSelectorAndRestart1",
                     ((TextMessage) rMsg).getText());
        
        rMsg = subTwo.receive(getShortReceiveTimeout());
        assertNull(rMsg);
        
        // Check queue has no messages
        assertEquals("Queue should be empty", 0, ((AMQSession<?, ?>) session).getQueueDepth(queue, true));
        
        conn.close();
    }

}
