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
package org.apache.qpid.systest;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.test.utils.QpidBrokerTestCase;

public class MessageRoutingTest extends QpidBrokerTestCase
{
    private static final String EXCHANGE_NAME = "testExchange";
    private static final String QUEUE_NAME = "testQueue";
    private static final String ROUTING_KEY = "testRoute";
    private Connection _connection;
    private Session _session;

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
        _connection = getConnectionBuilder().build();
        _connection.start();
        _session = _connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        createEntityUsingAmqpManagement(EXCHANGE_NAME, _session, "org.apache.qpid.DirectExchange",
                                        Collections.singletonMap(Exchange.UNROUTABLE_MESSAGE_BEHAVIOUR, "REJECT"));
        createEntityUsingAmqpManagement(QUEUE_NAME, _session, "org.apache.qpid.Queue");

        final Map<String, Object> arguments = new HashMap<>();
        arguments.put("destination", QUEUE_NAME);
        arguments.put("bindingKey", ROUTING_KEY);
        performOperationUsingAmqpManagement(EXCHANGE_NAME, "bind", _connection.createSession(false, Session.AUTO_ACKNOWLEDGE), "org.apache.qpid.Exchange",
                                            arguments);
    }

    public void testRoutingWithSubjectSetAsJMSMessageType() throws Exception
    {
        Destination sendingDestination = _session.createTopic(EXCHANGE_NAME);
        Destination receivingDestination = _session.createQueue(QUEUE_NAME);

        Message message = _session.createTextMessage("test");
        message.setJMSType(ROUTING_KEY);

        MessageProducer messageProducer = _session.createProducer(sendingDestination);
        messageProducer.send(message);

        MessageConsumer messageConsumer = _session.createConsumer(receivingDestination);
        Message receivedMessage = messageConsumer.receive(getReceiveTimeout());

        assertNotNull("Message not received", receivedMessage);
        assertEquals("test", ((TextMessage)message).getText());
    }

    public void testAnonymousRelayRoutingWithSubjectSetAsJMSMessageType() throws Exception
    {
        Destination sendingDestination = _session.createTopic(EXCHANGE_NAME);
        Destination receivingDestination = _session.createQueue(QUEUE_NAME);

        Message message = _session.createTextMessage("test");
        message.setJMSType(ROUTING_KEY);

        MessageProducer messageProducer = _session.createProducer(null);
        messageProducer.send(sendingDestination, message);

        MessageConsumer messageConsumer = _session.createConsumer(receivingDestination);
        Message receivedMessage = messageConsumer.receive(getReceiveTimeout());

        assertNotNull("Message not received", receivedMessage);
        assertEquals("test", ((TextMessage)message).getText());
    }

    public void testRoutingWithRoutingKeySetAsJMSProperty() throws Exception
    {
        Destination sendingDestination = _session.createTopic(EXCHANGE_NAME);
        Destination receivingDestination = _session.createQueue(QUEUE_NAME);

        Message message = _session.createTextMessage("test");
        message.setStringProperty("routing_key", ROUTING_KEY);

        MessageProducer messageProducer = _session.createProducer(sendingDestination);
        messageProducer.send(message);

        MessageConsumer messageConsumer = _session.createConsumer(receivingDestination);
        Message receivedMessage = messageConsumer.receive(getReceiveTimeout());

        assertNotNull("Message not received", receivedMessage);
        assertEquals("test", ((TextMessage)message).getText());
    }

    public void testRoutingWithExchangeAndRoutingKeyDestination() throws Exception
    {
        Destination sendingDestination = _session.createTopic(EXCHANGE_NAME + "/" + ROUTING_KEY);
        Destination receivingDestination = _session.createQueue(QUEUE_NAME);

        Message message = _session.createTextMessage("test");

        MessageProducer messageProducer = _session.createProducer(sendingDestination);
        messageProducer.send(message);

        MessageConsumer messageConsumer = _session.createConsumer(receivingDestination);
        Message receivedMessage = messageConsumer.receive(getReceiveTimeout());

        assertNotNull("Message not received", receivedMessage);
        assertEquals("test", ((TextMessage)message).getText());
    }

    public void testAnonymousRelayRoutingWithExchangeAndRoutingKeyDestination() throws Exception
    {
        Destination sendingDestination = _session.createTopic(EXCHANGE_NAME + "/" + ROUTING_KEY);
        Destination receivingDestination = _session.createQueue(QUEUE_NAME);

        Message message = _session.createTextMessage("test");

        MessageProducer messageProducer = _session.createProducer(null);
        messageProducer.send(sendingDestination, message);

        MessageConsumer messageConsumer = _session.createConsumer(receivingDestination);
        Message receivedMessage = messageConsumer.receive(getReceiveTimeout());

        assertNotNull("Message not received", receivedMessage);
        assertEquals("test", ((TextMessage)message).getText());
    }

    public void testRoutingToQueue() throws Exception
    {
        Destination sendingDestination = _session.createQueue(QUEUE_NAME);
        Destination receivingDestination = _session.createQueue(QUEUE_NAME);

        Message message = _session.createTextMessage("test");

        MessageProducer messageProducer = _session.createProducer(sendingDestination);
        messageProducer.send(message);

        MessageConsumer messageConsumer = _session.createConsumer(receivingDestination);
        Message receivedMessage = messageConsumer.receive(getReceiveTimeout());

        assertNotNull("Message not received", receivedMessage);
        assertEquals("test", ((TextMessage)message).getText());
    }

    public void testAnonymousRelayRoutingToQueue() throws Exception
    {
        Destination sendingDestination = _session.createQueue(QUEUE_NAME);
        Destination receivingDestination = _session.createQueue(QUEUE_NAME);

        Message message = _session.createTextMessage("test");

        MessageProducer messageProducer = _session.createProducer(null);
        messageProducer.send(sendingDestination, message);

        MessageConsumer messageConsumer = _session.createConsumer(receivingDestination);
        Message receivedMessage = messageConsumer.receive(getReceiveTimeout());

        assertNotNull("Message not received", receivedMessage);
        assertEquals("test", ((TextMessage)message).getText());
    }
}
