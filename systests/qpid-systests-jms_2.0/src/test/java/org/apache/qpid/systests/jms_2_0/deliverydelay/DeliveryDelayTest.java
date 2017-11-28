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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.Destination;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSProducer;
import javax.jms.JMSRuntimeException;
import javax.jms.Message;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.qpid.systests.JmsTestBase;
import org.apache.qpid.tests.utils.BrokerAdmin;

public class DeliveryDelayTest extends JmsTestBase
{
    private static final int DELIVERY_DELAY = 3000;

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        System.setProperty("virtualhost.housekeepingCheckPeriod", "100");
    }

    @AfterClass
    public static void tearDownClass()
    {
        System.clearProperty("virtualhost.housekeepingCheckPeriod");
    }

    @Test
    public void testDeliveryDelay() throws Exception
    {
        try (JMSContext context = getConnectionBuilder().buildConnectionFactory().createContext())
        {
            Destination queue = createQueue(context, BrokerAdmin.TEST_QUEUE_NAME, true);

            final AtomicLong messageReceiptTime = new AtomicLong();
            final CountDownLatch receivedLatch = new CountDownLatch(1);
            context.createConsumer(queue).setMessageListener(message -> {
                messageReceiptTime.set(System.currentTimeMillis());
                receivedLatch.countDown();
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
    @Test
    public void testDeliveryDelayNotSupportedByQueue_MessageRejected() throws Exception
    {
        try (JMSContext context = getConnectionBuilder().buildConnectionFactory().createContext())
        {
            Destination queue = createQueue(context, BrokerAdmin.TEST_QUEUE_NAME, false);
            JMSProducer producer = context.createProducer().setDeliveryDelay(DELIVERY_DELAY);

            try
            {
                producer.send(queue, "message");
                fail("Exception not thrown");
            }
            catch (JMSRuntimeException e)
            {
                assertTrue("Unexpected exception message: " + e.getMessage(),
                           e.getMessage().contains("amqp:precondition-failed"));
            }
        }
    }

    /**
     * The client sends a messagge to a fanout exchange instance which is bound to a queue with
     * holdsOnPublish turned off. The Broker must reject the message.
     */
    @Test
    public void testDeliveryDelayNotSupportedByQueueViaExchange_MessageRejected() throws Exception
    {
        try (JMSContext context = getConnectionBuilder().buildConnectionFactory().createContext())
        {
            String testQueueName = BrokerAdmin.TEST_QUEUE_NAME;
            String testExchangeName = "test_exch";

            Destination consumeDest = createQueue(context, testQueueName, false);
            Destination publishDest = createExchange(context, testExchangeName);
            bindQueueToExchange(testExchangeName, testQueueName);


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
            catch (JMSRuntimeException e)
            {
                assertTrue("Unexpected exception message: " + e.getMessage(),
                           e.getMessage().contains("amqp:precondition-failed"));
            }
        }
    }

    private Destination createQueue(final JMSContext context, String queueName,
                                    boolean holdsOnPublish) throws Exception
    {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(org.apache.qpid.server.model.Queue.HOLD_ON_PUBLISH_ENABLED, holdsOnPublish);
        createEntityUsingAmqpManagement(queueName,
                                        "org.apache.qpid.Queue",
                                        attributes);
        return context.createQueue(queueName);
    }

    private Destination createExchange(final JMSContext context, String exchangeName) throws Exception
    {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(org.apache.qpid.server.model.Exchange.UNROUTABLE_MESSAGE_BEHAVIOUR, "REJECT");
        createEntityUsingAmqpManagement(exchangeName,
                                        "org.apache.qpid.FanoutExchange",
                                        attributes);
        return context.createQueue(exchangeName);
    }

    private void bindQueueToExchange(String exchangeName,
                                     String queueName) throws Exception
    {
        final Map<String, Object> arguments = new HashMap<>();
        arguments.put("destination", queueName);
        arguments.put("bindingKey", queueName);
        performOperationUsingAmqpManagement(exchangeName,
                                            "bind",
                                            "org.apache.qpid.FanoutExchange",
                                            arguments);
    }

}
