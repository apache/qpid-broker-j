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

package org.apache.qpid.test.client.failover;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.client.AMQConnection;
import org.apache.qpid.client.AMQSession;
import org.apache.qpid.test.utils.BrokerHolder;
import org.apache.qpid.test.utils.FailoverBaseCase;

public class FailoverTest extends FailoverBaseCase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FailoverTest.class);

    private static final int DEFAULT_NUM_MESSAGES = 10;
    private static final int DEFAULT_SEED = 20080921;
    protected int numMessages = 0;
    protected Connection connection;
    private Session producerSession;
    private Queue queue;
    private MessageProducer producer;
    private Session consumerSession;
    private MessageConsumer consumer;

    private boolean CLUSTERED = Boolean.getBoolean("profile.clustered");
    private int seed;
    private Random rand;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        numMessages = Integer.getInteger("profile.failoverMsgCount", DEFAULT_NUM_MESSAGES);
        seed = Integer.getInteger("profile.failoverRandomSeed", DEFAULT_SEED);
        rand = new Random(seed);
        
        connection = getConnection();
        ((AMQConnection) connection).setConnectionListener(this);
        connection.start();
    }

    private void init(boolean transacted, int mode) throws Exception
    {
        consumerSession = connection.createSession(transacted, mode);
        queue = consumerSession.createQueue(getName()+System.currentTimeMillis());
        consumer = consumerSession.createConsumer(queue);

        producerSession = connection.createSession(transacted, mode);
        producer = producerSession.createProducer(queue);
    }

    @Override
    public void tearDown() throws Exception
    {
        try
        {
            connection.close();
        }
        catch (Exception e)
        {

        }

        try
        {
            _alternativeBroker.shutdown();
        }
        finally
        {
            super.tearDown();
        }
    }

    private void consumeMessages(int startIndex,int endIndex, boolean transacted) throws JMSException
    {
        Message msg;
        LOGGER.debug("**************** Receive (Start: " + startIndex + ", End:" + endIndex + ")***********************");
        
        for (int i = startIndex; i < endIndex; i++)
        {
            msg = consumer.receive(1000);            
            assertNotNull("Message " + i + " was null!", msg);
            
            LOGGER.debug("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
            LOGGER.debug("Received : " + ((TextMessage) msg).getText());
            LOGGER.debug("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
            
            assertEquals("Invalid message order","message " + i, ((TextMessage) msg).getText());
            
        }
        LOGGER.debug("***********************************************************");
        
        if (transacted) 
        {
            consumerSession.commit();
        }
    }

    private void sendMessages(int startIndex,int endIndex, boolean transacted) throws Exception
    {
        LOGGER.debug("**************** Send (Start: " + startIndex + ", End:" + endIndex + ")***********************");
        
        for (int i = startIndex; i < endIndex; i++)
        {            
            producer.send(producerSession.createTextMessage("message " + i));
            
            LOGGER.debug("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
            LOGGER.debug("Sending message"+i);
            LOGGER.debug("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
        }
        
        LOGGER.debug("***********************************************************");
        
        if (transacted)
        {
            producerSession.commit();
        }
        else
        {
            ((AMQSession<?, ?>)producerSession).sync();
        }
    }

    public void testP2PFailover() throws Exception
    {
        testP2PFailover(numMessages, true, true, false);
    }

    public void testP2PFailoverWithMessagesLeftToConsumeAndProduce() throws Exception
    {
        if (CLUSTERED)
        {
            testP2PFailover(numMessages, false, false, false);
        }
    }
    
    public void testP2PFailoverWithMessagesLeftToConsume() throws Exception
    {
        if (CLUSTERED)
        {    
            testP2PFailover(numMessages, false, true, false);
        }
    }    
     
    public void testP2PFailoverTransacted() throws Exception
    {
        testP2PFailover(numMessages, true, true, true);
    }

    public void testP2PFailoverTransactedWithMessagesLeftToConsumeAndProduce() throws Exception
    {
        // Currently the cluster does not support transactions that span a failover
        if (CLUSTERED)
        {
            testP2PFailover(numMessages, false, false, false);
        }
    }
    private void testP2PFailover(int totalMessages, boolean consumeAll, boolean produceAll , boolean transacted) throws Exception
    {        
        init(transacted, Session.AUTO_ACKNOWLEDGE);
        runP2PFailover(getDefaultBroker(), totalMessages,consumeAll, produceAll , transacted);
    } 

    private void runP2PFailover(BrokerHolder broker, int totalMessages, boolean consumeAll, boolean produceAll , boolean transacted) throws Exception
    {
        int toProduce = totalMessages;
        
        LOGGER.debug("===================================================================");
        LOGGER.debug("Total messages used for the test " + totalMessages + " messages");
        LOGGER.debug("===================================================================");
        
        if (!produceAll)
        {
            toProduce = totalMessages - rand.nextInt(totalMessages);
        }
                
        LOGGER.debug("==================");
        LOGGER.debug("Sending " + toProduce + " messages");
        LOGGER.debug("==================");
        
        sendMessages(0, toProduce, transacted);

        // Consume some messages
        int toConsume = toProduce;
        if (!consumeAll)
        {
            toConsume = toProduce - rand.nextInt(toProduce);         
        }
        
        consumeMessages(0, toConsume, transacted);

        LOGGER.debug("==================");
        LOGGER.debug("Consuming " + toConsume + " messages");
        LOGGER.debug("==================");
        
        LOGGER.info("Failing over");

        causeFailure(broker, DEFAULT_FAILOVER_TIME);

        // Check that you produce and consume the rest of messages.
        LOGGER.debug("==================");
        LOGGER.debug("Sending " + (totalMessages-toProduce) + " messages");
        LOGGER.debug("==================");
        
        sendMessages(toProduce, totalMessages, transacted);
        consumeMessages(toConsume, totalMessages, transacted);
        
        LOGGER.debug("==================");
        LOGGER.debug("Consuming " + (totalMessages-toConsume) + " messages");
        LOGGER.debug("==================");
    }

    private void causeFailure(BrokerHolder broker, long delay)
    {

        failBroker(broker);

        LOGGER.info("Awaiting Failover completion");
        try
        {
            if (!_failoverComplete.await(delay, TimeUnit.MILLISECONDS))
            {
                fail("failover did not complete");
            }
        }
        catch (InterruptedException e)
        {
            //evil ignore IE.
        }
    }

    public void testClientAckFailover() throws Exception
    {
        init(false, Session.CLIENT_ACKNOWLEDGE);
        sendMessages(0,1, false);
        Message msg = consumer.receive();
        assertNotNull("Expected msgs not received", msg);

        causeFailure(getDefaultBroker(), DEFAULT_FAILOVER_TIME);

        Exception failure = null;
        try
        {
            msg.acknowledge();
        }
        catch (Exception e)
        {
            failure = e;
        }
        assertNotNull("Exception should be thrown", failure);
    } 

    /**
     * The idea is to run a failover test in a loop by failing over
     * to the other broker each time.
     */
    public void testFailoverInALoop() throws Exception
    {
        if (!CLUSTERED)
        {
            return;
        }

        BrokerHolder currentBroker = getDefaultBroker();
        int iterations = Integer.getInteger("profile.failoverIterations", 3);
        LOGGER.debug("LQ: iterations {}", iterations);
        boolean useDefaultBroker = true;
        init(false, Session.AUTO_ACKNOWLEDGE);
        for (int i=0; i < iterations; i++)
        {
            LOGGER.debug("===================================================================");
            LOGGER.debug("Failover In a loop : iteration number " + i);
            LOGGER.debug("===================================================================");

            runP2PFailover(currentBroker, numMessages, false, false, false);
            restartBroker(currentBroker);
            if (useDefaultBroker)
            {
                currentBroker = _alternativeBroker;
                useDefaultBroker = false;
            }
            else
            {
                currentBroker = getDefaultBroker();
                useDefaultBroker = true;
            }
        }
        //To prevent any failover logic being initiated when we shutdown the brokers.
        connection.close();
    }
}
