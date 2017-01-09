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

package org.apache.qpid.systests.jms_2_0.deliverydelay;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.InvalidDestinationRuntimeException;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSProducer;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;

import org.apache.qpid.test.utils.QpidBrokerTestCase;

public class DeliveryDelayTest extends QpidBrokerTestCase
{
    private static final int DELIVERY_DELAY = 3000;

    @Override
    public void setUp() throws Exception
    {
        setTestSystemProperty("virtualhost.housekeepingCheckPeriod", "100");
        super.setUp();
    }

    public void testDeliveryDelay() throws Exception
    {
        ConnectionFactory connectionFactory = getConnectionFactory();
        try (JMSContext context = connectionFactory.createContext(GUEST_USERNAME, GUEST_PASSWORD);
             Connection utilityConnection = connectionFactory.createConnection(GUEST_USERNAME, GUEST_PASSWORD))
        {
            Destination queue = createQueue(utilityConnection, getTestQueueName(), true);

            final AtomicLong messageReceiptTime = new AtomicLong();
            final CountDownLatch receivedLatch = new CountDownLatch(1);
            context.createConsumer(queue).setMessageListener(new MessageListener()
            {
                @Override
                public void onMessage(final Message message)
                {
                    messageReceiptTime.set(System.currentTimeMillis());
                    receivedLatch.countDown();
                }
            });

            JMSProducer producer = context.createProducer().setDeliveryDelay(DELIVERY_DELAY);

            final long messageSentTime = System.currentTimeMillis();
            producer.send(queue, "delayed message");

            final boolean messageArrived = receivedLatch.await(DELIVERY_DELAY * 3, TimeUnit.MILLISECONDS);
            assertTrue("Delayed message did not arrive within expected period", messageArrived);
            final long actualDelay = messageReceiptTime.get() - messageSentTime;
            assertTrue(String.format("Message was not delayed by sufficient time (%d). Actual delay (%d)",
                                     DELIVERY_DELAY, actualDelay),
                       actualDelay >= DELIVERY_DELAY);
        }
    }

    /**
     * The target queue, which is addressed directly by the client, does not have
     * holdsOnPublish turned on.  The Broker must reject the message.
     */
    public void testDeliveryDelayNotSupportedByQueue_MessageRejected() throws Exception
    {
        ConnectionFactory connectionFactory = getConnectionFactory();
        try (JMSContext context = connectionFactory.createContext(GUEST_USERNAME, GUEST_PASSWORD);
             Connection utilityConnection = connectionFactory.createConnection(GUEST_USERNAME, GUEST_PASSWORD))
        {
            Destination queue = createQueue(utilityConnection, getTestQueueName(), false);
            JMSProducer producer = context.createProducer().setDeliveryDelay(DELIVERY_DELAY);

            try
            {
                producer.send(queue, "message");
                fail("Exception not thrown");
            }
            catch (InvalidDestinationRuntimeException e)
            {
                // PASS
            }
        }
    }

    /**
     * The client sends a messagge to a fanout exchange instance which is bound to a queue with
     * holdsOnPublish turned off. The Broker must reject the message.
     */
    public void testDeliveryDelayNotSupportedByQueueViaExchange_MessageRejected() throws Exception
    {
        ConnectionFactory connectionFactory = getConnectionFactory();
        try (JMSContext context = connectionFactory.createContext(GUEST_USERNAME, GUEST_PASSWORD);
             Connection utilityConnection = connectionFactory.createConnection(GUEST_USERNAME, GUEST_PASSWORD))
        {
            String testQueueName = getTestQueueName();
            String testExchangeName = getTestName() + "_exch";

            Destination consumeDest = createQueue(utilityConnection, testQueueName, false);
            Destination publishDest = createExchange(utilityConnection, testExchangeName);
            bindQueueToExchange(utilityConnection, testExchangeName, testQueueName);


            JMSConsumer consumer = context.createConsumer(consumeDest);
            JMSProducer producer = context.createProducer();

            producer.send(publishDest, "message without delivery delay");

            Message message = consumer.receive(getReceiveTimeout());
            assertNotNull("Message published without delivery delay not received", message);

            producer.setDeliveryDelay(DELIVERY_DELAY);

            try
            {
                producer.send(publishDest, "message with delivery delay");
                fail("Exception not thrown");
            }
            catch (InvalidDestinationRuntimeException e)
            {
                // PASS
            }
        }
    }

    private Destination createQueue(Connection utilityConnection, String queueName, boolean holdsOnPublish) throws Exception
    {
        try (Session session = utilityConnection.createSession(Session.SESSION_TRANSACTED))
        {
            Map<String, Object> attributes = new HashMap<>();
            attributes.put(org.apache.qpid.server.model.Queue.HOLD_ON_PUBLISH_ENABLED, holdsOnPublish);
            createEntityUsingAmqpManagement(queueName, session, "org.apache.qpid.Queue", attributes);
            return session.createQueue(queueName);
        }
    }

    private Destination createExchange(Connection utilityConnection, String exchangeName) throws Exception
    {
        try (Session session = utilityConnection.createSession(Session.SESSION_TRANSACTED))
        {
            Map<String, Object> attributes = new HashMap<>();
            attributes.put(org.apache.qpid.server.model.Exchange.UNROUTABLE_MESSAGE_BEHAVIOUR, "REJECT");
            createEntityUsingAmqpManagement(exchangeName, session, "org.apache.qpid.FanoutExchange", attributes);
            return session.createQueue(exchangeName);
        }
    }

    private void bindQueueToExchange(Connection utilityConnection,
                                     String exchangeName,
                                     String queueName) throws Exception
    {
        try (Session session = utilityConnection.createSession(Session.SESSION_TRANSACTED))
        {
            final Map<String, Object> arguments = new HashMap<>();
            arguments.put("destination", queueName);
            arguments.put("bindingKey", queueName);
            performOperationUsingAmqpManagement(exchangeName, "bind", session, "org.apache.qpid.FanoutExchange", arguments);
        }
    }

}
