/* Licensed to the Apache Software Foundation (ASF) under one
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
package org.apache.qpid.test.unit.ct;

import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

import org.apache.qpid.client.AMQQueue;
import org.apache.qpid.client.AMQSession;
import org.apache.qpid.test.utils.QpidBrokerTestCase;

/**
 *   Crash Recovery tests for durable subscription
 *
 */
public class DurableSubscriberTest extends QpidBrokerTestCase
{
    private final String _topicName = "durableSubscriberTopic";

    /**
     * create and register a durable subscriber with a message selector and then close it
     * crash the broker
     * create a publisher and send  5 right messages and 5 wrong messages
     * recreate the durable subscriber and check we receive the 5 expected messages
     */
    public void testDurSubRestoresMessageSelector() throws Exception
    {
        if (isBrokerStorePersistent())
        {
            //create and register a durable subscriber with a message selector and then close it
            TopicConnection durConnection = (TopicConnection) getConnection();
            final Topic topic = createTopic(durConnection, _topicName);
            TopicSession durSession = durConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            TopicSubscriber durSub1 = durSession.createDurableSubscriber(topic, "dursub", "testprop='true'", false);
            durConnection.start();
            durSub1.close();
            durSession.close();
            durConnection.stop();
            //now stop the server
            try
            {
                restartDefaultBroker();
            }
            catch (Exception e)
            {
                _logger.error("problems restarting broker: " + e);
                throw e;
            }
            TopicConnection pubConnection = (TopicConnection) getConnection();
            TopicSession pubSession = pubConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            TopicPublisher publisher = pubSession.createPublisher(topic);
            for (int i = 0; i < 5; i++)
            {
                Message message = pubSession.createMessage();
                message.setStringProperty("testprop", "true");
                publisher.publish(message);
                message = pubSession.createMessage();
                message.setStringProperty("testprop", "false");
                publisher.publish(message);
            }
            publisher.close();
            pubSession.close();

            //now recreate the durable subscriber and check the received messages
            TopicConnection durConnection2 = (TopicConnection) getConnection();
            TopicSession durSession2 = durConnection2.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            TopicSubscriber durSub2 = durSession2.createDurableSubscriber(topic, "dursub", "testprop='true'", false);
            durConnection2.start();
            for (int i = 0; i < 5; i++)
            {
                Message message = durSub2.receive(1000);
                if (message == null)
                {
                    assertTrue("testDurSubRestoresMessageSelector test failed. no message was returned", false);
                }
                else
                {
                    assertTrue("testDurSubRestoresMessageSelector test failed. message selector not reset",
                               message.getStringProperty("testprop").equals("true"));
                }
            }
            durSub2.close();
            durSession2.unsubscribe("dursub");
            durConnection2.close();
        }
    }
    
    /**
     * create and register a durable subscriber without a message selector and then unsubscribe it
     * create and register a durable subscriber with a message selector and then close it
     * restart the broker
     * send matching and non matching messages
     * recreate and register the durable subscriber with a message selector
     * verify only the matching messages are received
     */
    public void testDurSubChangedToHaveSelectorThenRestart() throws Exception
    {
        if (! isBrokerStorePersistent())
        {
            _logger.warn("Test skipped due to requirement of a persistent store");
            return;
        }
        
        final String SUB_NAME=getTestQueueName();
        
        //create and register a durable subscriber then unsubscribe it
        TopicConnection durConnection = (TopicConnection) getConnection();
        Topic topic = createTopic(durConnection, _topicName);
        TopicSession durSession = durConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        TopicSubscriber durSub1 = durSession.createDurableSubscriber(topic, SUB_NAME);
        durConnection.start();
        durSub1.close();
        durSession.unsubscribe(SUB_NAME);
        durSession.close();
        durConnection.close();

        //create and register a durable subscriber with a message selector and then close it
        TopicConnection durConnection2 = (TopicConnection) getConnection();
        TopicSession durSession2 = durConnection2.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        TopicSubscriber durSub2 = durSession2.createDurableSubscriber(topic, SUB_NAME, "testprop='true'", false);
        durConnection2.start();
        durSub2.close();
        durSession2.close();
        durConnection2.close();
        
        //now restart the server
        try
        {
            restartDefaultBroker();
        }
        catch (Exception e)
        {
            _logger.error("problems restarting broker: " + e);
            throw e;
        }
        
        //send messages matching and not matching the selector
        TopicConnection pubConnection = (TopicConnection) getConnection();
        TopicSession pubSession = pubConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        TopicPublisher publisher = pubSession.createPublisher(topic);
        for (int i = 0; i < 5; i++)
        {
            Message message = pubSession.createMessage();
            message.setStringProperty("testprop", "true");
            publisher.publish(message);
            message = pubSession.createMessage();
            message.setStringProperty("testprop", "false");
            publisher.publish(message);
        }
        publisher.close();
        pubSession.close();

        //now recreate the durable subscriber with selector to check there are no exceptions generated
        //and then verify the messages are received correctly
        TopicConnection durConnection3 = (TopicConnection) getConnection();
        TopicSession durSession3 = (TopicSession) durConnection3.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TopicSubscriber durSub3 = durSession3.createDurableSubscriber(topic, SUB_NAME, "testprop='true'", false);
        durConnection3.start();
        
        for (int i = 0; i < 5; i++)
        {
            Message message = durSub3.receive(2000);
            if (message == null)
            {
                fail("testDurSubChangedToHaveSelectorThenRestart test failed. Expected message " + i + " was not returned");
            }
            else
            {
                assertTrue("testDurSubChangedToHaveSelectorThenRestart test failed. Got message not matching selector",
                           message.getStringProperty("testprop").equals("true"));
            }
        }

        durSub3.close();
        durSession3.unsubscribe(SUB_NAME);
        durSession3.close();
        durConnection3.close();
    }

    
    /**
     * create and register a durable subscriber with a message selector and then unsubscribe it
     * create and register a durable subscriber without a message selector and then close it
     * restart the broker
     * send matching and non matching messages
     * recreate and register the durable subscriber without a message selector
     * verify ALL the sent messages are received
     */
    public void testDurSubChangedToNotHaveSelectorThenRestart() throws Exception
    {
        if (! isBrokerStorePersistent())
        {
            _logger.warn("Test skipped due to requirement of a persistent store");
            return;
        }
        
        final String SUB_NAME=getTestQueueName();

        //create and register a durable subscriber with selector then unsubscribe it
        TopicConnection durConnection = (TopicConnection) getConnection();
        Topic topic = createTopic(durConnection, _topicName);
        TopicSession durSession = durConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        TopicSubscriber durSub1 = durSession.createDurableSubscriber(topic, SUB_NAME, "testprop='true'", false);
        durConnection.start();
        durSub1.close();
        durSession.unsubscribe(SUB_NAME);
        durSession.close();
        durConnection.close();

        //create and register a durable subscriber without the message selector and then close it
        TopicConnection durConnection2 = (TopicConnection) getConnection();
        TopicSession durSession2 = durConnection2.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        TopicSubscriber durSub2 = durSession2.createDurableSubscriber(topic, SUB_NAME);
        durConnection2.start();
        durSub2.close();
        durSession2.close();
        durConnection2.close();
        
        //now restart the server
        try
        {
            restartDefaultBroker();
        }
        catch (Exception e)
        {
            _logger.error("problems restarting broker: " + e);
            throw e;
        }
        
        //send messages matching and not matching the original used selector
        TopicConnection pubConnection = (TopicConnection) getConnection();
        TopicSession pubSession = pubConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        TopicPublisher publisher = pubSession.createPublisher(topic);
        for (int i = 1; i <= 5; i++)
        {
            Message message = pubSession.createMessage();
            message.setStringProperty("testprop", "true");
            publisher.publish(message);
            message = pubSession.createMessage();
            message.setStringProperty("testprop", "false");
            publisher.publish(message);
        }
        publisher.close();
        pubSession.close();

        //now recreate the durable subscriber without selector to check there are no exceptions generated
        //then verify ALL messages sent are received
        TopicConnection durConnection3 = (TopicConnection) getConnection();
        TopicSession durSession3 = (TopicSession) durConnection3.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TopicSubscriber durSub3 = durSession3.createDurableSubscriber(topic, SUB_NAME);
        durConnection3.start();
        
        for (int i = 1; i <= 10; i++)
        {
            Message message = durSub3.receive(2000);
            if (message == null)
            {
                fail("testDurSubChangedToNotHaveSelectorThenRestart test failed. Expected message " + i + " was not received");
            }
        }
        
        durSub3.close();
        durSession3.unsubscribe(SUB_NAME);
        durSession3.close();
        durConnection3.close();
    }
    
    
    public void testResubscribeWithChangedSelectorAndRestart() throws Exception
    {
        if (! isBrokerStorePersistent())
        {
            _logger.warn("Test skipped due to requirement of a persistent store");
            return;
        }
        
        TopicConnection conn = (TopicConnection) getConnection();
        conn.start();
        Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = createTopic(conn, "testResubscribeWithChangedSelectorAndRestart");
        MessageProducer producer = session.createProducer(topic);
        
        // Create durable subscriber that matches A
        TopicSubscriber subA = session.createDurableSubscriber(topic, 
                "testResubscribeWithChangedSelectorAndRestart",
                "Match = True", false);

        // Send 1 matching message and 1 non-matching message
        TextMessage msg = session.createTextMessage("testResubscribeWithChangedSelectorAndRestart1");
        msg.setBooleanProperty("Match", true);
        producer.send(msg);
        msg = session.createTextMessage("testResubscribeWithChangedSelectorAndRestart2");
        msg.setBooleanProperty("Match", false);
        producer.send(msg);

        Message rMsg = subA.receive(getReceiveTimeout());
        assertNotNull(rMsg);
        assertEquals("Content was wrong", 
                     "testResubscribeWithChangedSelectorAndRestart1",
                     ((TextMessage) rMsg).getText());

        // Queue has no messages left
        rMsg = subA.receive(getReceiveTimeout());
        assertNull(rMsg);
        
        // Send another 1 matching message and 1 non-matching message
        msg = session.createTextMessage("testResubscribeWithChangedSelectorAndRestart1");
        msg.setBooleanProperty("Match", true);
        producer.send(msg);
        msg = session.createTextMessage("testResubscribeWithChangedSelectorAndRestart2");
        msg.setBooleanProperty("Match", false);
        producer.send(msg);
        
        // Disconnect subscriber without receiving the message to 
        //leave it on the underlying queue
        subA.close();
        
        // Reconnect with new selector that matches B
        TopicSubscriber subB = session.createDurableSubscriber(topic, 
                "testResubscribeWithChangedSelectorAndRestart",
                "Match = false", false);
        
        //verify no messages are now present on the queue as changing selector should have issued
        //an unsubscribe and thus deleted the previous durable backing queue for the subscription.
        //check the dur sub's underlying queue now has msg count 0
        rMsg = subB.receive(getReceiveTimeout());
        assertNull(rMsg);
        
        // Check that new messages are received properly
        msg = session.createTextMessage("testResubscribeWithChangedSelectorAndRestart1");
        msg.setBooleanProperty("Match", true);
        producer.send(msg);
        msg = session.createTextMessage("testResubscribeWithChangedSelectorAndRestart2");
        msg.setBooleanProperty("Match", false);
        producer.send(msg);
        
        rMsg = subB.receive(getReceiveTimeout());
        assertNotNull(rMsg);
        assertEquals("Content was wrong", 
                     "testResubscribeWithChangedSelectorAndRestart2",
                     ((TextMessage) rMsg).getText());

        //check the dur sub's underlying queue now has msg count 0
        rMsg = subB.receive(getReceiveTimeout());
        assertNull(rMsg);

        conn.close();

        //now restart the server
        try
        {
            restartDefaultBroker();
        }
        catch (Exception e)
        {
            _logger.error("problems restarting broker: " + e);
            throw e;
        }
        
        // Reconnect to broker
        TopicConnection connection = (TopicConnection) getConnection();
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        topic = createTopic(connection, "testResubscribeWithChangedSelectorAndRestart");
        producer = session.createProducer(topic);

        // Reconnect with new selector that matches B
        TopicSubscriber subC = session.createDurableSubscriber(topic, 
                "testResubscribeWithChangedSelectorAndRestart",
                "Match = False", false);

        //verify no messages now present on the queue after we restart the broker
        //check the dur sub's underlying queue now has msg count 0
        rMsg = subC.receive(getReceiveTimeout());
        assertNull(rMsg);

        // Check that new messages are still sent and recieved properly
        msg = session.createTextMessage("testResubscribeWithChangedSelectorAndRestart1");
        msg.setBooleanProperty("Match", true);
        producer.send(msg);
        msg = session.createTextMessage("testResubscribeWithChangedSelectorAndRestart2");
        msg.setBooleanProperty("Match", false);
        producer.send(msg);

        //check the dur sub's underlying queue now has msg count 1
        rMsg = subC.receive(getReceiveTimeout());
        assertNotNull(rMsg);
        assertEquals("Content was wrong", 
                     "testResubscribeWithChangedSelectorAndRestart2",
                     ((TextMessage) rMsg).getText());
        
        rMsg = subC.receive(getReceiveTimeout());
        assertNull(rMsg);

        subC.close();
        session.unsubscribe("testResubscribeWithChangedSelectorAndRestart");

        session.close();
        connection.close();
    }
}

