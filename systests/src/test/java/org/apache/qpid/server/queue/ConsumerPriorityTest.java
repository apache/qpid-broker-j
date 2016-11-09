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
package org.apache.qpid.server.queue;

import java.util.HashMap;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.qpid.client.AMQSession;
import org.apache.qpid.jms.ConnectionURL;
import org.apache.qpid.test.utils.QpidBrokerTestCase;

public class ConsumerPriorityTest extends QpidBrokerTestCase
{

    private Connection _consumingConnection;
    private Session _consumingSession;
    private Connection _producingConnection;
    private Session _producingSession;

    protected void setUp() throws Exception
    {
        super.setUp();

        _consumingConnection = getConnection();
        _consumingSession = _consumingConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        _consumingConnection.start();

        _producingConnection = getConnection();
        _producingSession = _producingConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        _producingConnection.start();

    }


    public void testLowPriorityConsumerReceivesMessages() throws Exception
    {
        Queue queue = _consumingSession.createQueue("direct://amq.direct/" + getTestQueueName() + "/" + getTestQueueName() + "?x-priority='10'");
        final MessageConsumer consumer = _consumingSession.createConsumer(queue);
        assertNull("There should be no messages in the queue", consumer.receive(100L));
        Destination producerDestination = _producingSession.createQueue("direct://amq.direct/" + getTestQueueName() + "/" + getTestQueueName());
        final MessageProducer producer = _producingSession.createProducer(producerDestination);
        producer.send(_producingSession.createTextMessage(getTestName()));
        assertNotNull("There should be no messages in the queue", consumer.receive(2000L));
    }


    public void testLowPriorityConsumerDoesNotReceiveMessagesIfHigherPriorityConsumerAvailable() throws Exception
    {
        Queue queue = _consumingSession.createQueue("direct://amq.direct/" + getTestQueueName() + "/" + getTestQueueName() + "?x-priority='10'");
        doTestLowPriorityConsumerDoesNotReceiveMessagesIfHigherPriorityAvailable(queue);
    }

    public void testLowPriorityConsumerDoesNotReceiveMessagesIfHigherPriorityConsumerAvailableUsingADDR() throws Exception
    {
        Queue queue = _consumingSession.createQueue("ADDR:" + getTestQueueName() + "; { create: always, node: { type: queue }, link : { x-subscribe: { arguments : { x-priority : '10' } } } }");
        doTestLowPriorityConsumerDoesNotReceiveMessagesIfHigherPriorityAvailable(queue);
    }

    private void doTestLowPriorityConsumerDoesNotReceiveMessagesIfHigherPriorityAvailable(final Queue queue) throws Exception
    {
        final MessageConsumer consumer = _consumingSession.createConsumer(queue);
        assertNull("There should be no messages in the queue", consumer.receive(100L));

        // make sure that credit is restored on consumer after message.flush/flow being sent from receive
        ((AMQSession<?,?>)_consumingSession).sync();

        final Connection secondConsumingConnection = getConnection();
        final Session secondConsumingSession = secondConsumingConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        secondConsumingConnection.start();
        secondConsumingSession.createQueue("direct://amq.direct/" + getTestQueueName() + "/" + getTestQueueName());
        final MessageConsumer standardPriorityConsumer = secondConsumingSession.createConsumer(secondConsumingSession.createQueue("direct://amq.direct/" + getTestQueueName() + "/" + getTestQueueName()));
        assertNull("There should be no messages in the queue", standardPriorityConsumer.receive(100L));

        // make sure that credit is restored on consumer after message.flush/flow being sent from receive
        ((AMQSession<?,?>)secondConsumingSession).sync();

        Destination producerDestination = _producingSession.createQueue("direct://amq.direct/" + getTestQueueName() + "/" + getTestQueueName());
        final MessageProducer producer = _producingSession.createProducer(producerDestination);
        producer.send(_producingSession.createTextMessage(getTestName()));
        assertNull("Message should not go to the low priority consumer", consumer.receive(100L));

        // make sure that credit is restored on consumer after message.flush/flow being sent from receive
        ((AMQSession<?,?>)_consumingSession).sync();

        producer.send(_producingSession.createTextMessage(getTestName() + " 2"));
        assertNull("Message should not go to the low priority consumer", consumer.receive(100L));

        // make sure that credit is restored on consumer after message.flush/flow being sent from receive
        ((AMQSession<?,?>)_consumingSession).sync();

        assertNotNull(standardPriorityConsumer.receive(100L));

        assertNotNull(standardPriorityConsumer.receive(100L));

    }

    public void testLowPriorityConsumerReceiveMessagesIfHigherPriorityConsumerHasNoCredit() throws Exception
    {
        Queue queue = _consumingSession.createQueue("direct://amq.direct/" + getTestQueueName() + "/" + getTestQueueName() + "?x-priority='10'");
        final MessageConsumer consumer = _consumingSession.createConsumer(queue);
        assertNull("There should be no messages in the queue", consumer.receive(100L));

        Map<String, String> options = new HashMap<String, String>();
        options.put(ConnectionURL.OPTIONS_MAXPREFETCH, "2");

        final Connection secondConsumingConnection = getConnectionWithOptions(options);
        final Session secondConsumingSession = secondConsumingConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        secondConsumingConnection.start();
        secondConsumingSession.createQueue("direct://amq.direct/" + getTestQueueName() + "/" + getTestQueueName());
        final MessageConsumer standardPriorityConsumer = secondConsumingSession.createConsumer(secondConsumingSession.createQueue("direct://amq.direct/" + getTestQueueName() + "/" + getTestQueueName()));
        assertNull("There should be no messages in the queue", standardPriorityConsumer.receive(100L));


        Destination producerDestination = _producingSession.createQueue("direct://amq.direct/" + getTestQueueName() + "/" + getTestQueueName());
        final MessageProducer producer = _producingSession.createProducer(producerDestination);

        producer.send(createTextMessage(1));
        assertNull("Message should not go to the low priority consumer", consumer.receive(100L));
        producer.send(createTextMessage(2));
        assertNull("Message should not go to the low priority consumer", consumer.receive(100L));
        producer.send(createTextMessage(3));
        Message message = consumer.receive(100L);
        assertNotNull("Message should go to the low priority consumer as standard priority consumer has no credit", message);
        assertTrue("Message is not a text message", message instanceof TextMessage);
        assertEquals(getTestName() + " 3", ((TextMessage)message).getText());


    }


    public void testLowPriorityConsumerReceiveMessagesIfHigherPriorityConsumerDoesNotSelect() throws Exception
    {
        Queue queue = _consumingSession.createQueue("direct://amq.direct/" + getTestQueueName() + "/" + getTestQueueName() + "?x-priority='10'");
        final MessageConsumer consumer = _consumingSession.createConsumer(queue);
        assertNull("There should be no messages in the queue", consumer.receive(100L));


        final Connection secondConsumingConnection = getConnection();
        final Session secondConsumingSession = secondConsumingConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        secondConsumingConnection.start();
        secondConsumingSession.createQueue("direct://amq.direct/" + getTestQueueName() + "/" + getTestQueueName());
        final MessageConsumer standardPriorityConsumer = secondConsumingSession.createConsumer(secondConsumingSession.createQueue("direct://amq.direct/" + getTestQueueName() + "/" + getTestQueueName()), "msg <> 2");
        assertNull("There should be no messages in the queue", standardPriorityConsumer.receive(100L));


        Destination producerDestination = _producingSession.createQueue("direct://amq.direct/" + getTestQueueName() + "/" + getTestQueueName());
        final MessageProducer producer = _producingSession.createProducer(producerDestination);

        producer.send(createTextMessage(1));
        assertNull("Message should not go to the low priority consumer", consumer.receive(100L));
        producer.send(createTextMessage(2));
        Message message = consumer.receive(100L);
        assertNotNull("Message should go to the low priority consumer as standard priority consumer is not interested", message);
        assertTrue("Message is not a text message", message instanceof TextMessage);
        assertEquals(getTestName() + " 2", ((TextMessage)message).getText());


    }


    private TextMessage createTextMessage(final int msgId) throws JMSException
    {
        TextMessage textMessage = _producingSession.createTextMessage(getTestName() + " " + msgId);
        textMessage.setIntProperty("msg", msgId);
        return textMessage;
    }
}
