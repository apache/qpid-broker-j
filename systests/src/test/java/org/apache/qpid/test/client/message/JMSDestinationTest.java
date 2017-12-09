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

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.qpid.client.AMQDestination;
import org.apache.qpid.client.CustomJMSXProperty;
import org.apache.qpid.configuration.ClientProperties;
import org.apache.qpid.test.utils.QpidBrokerTestCase;

/**
 * From the API Docs getJMSDestination:
 *
 * When a message is received, its JMSDestination value must be equivalent to
 * the value assigned when it was sent.
 */
public class JMSDestinationTest extends QpidBrokerTestCase
{

    private Connection _connection;
    private Session _session;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        _connection = getConnection();

        _session = _connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    /**
     * Test a message received without the JMS_QPID_DESTTYPE can be resent
     * and correctly have the property set.
     *
     * To do this we need to create a 0-10 connection and send a message
     * which is then received by a 0-8/9 client.
     *
     * @throws Exception
     */
    public void testReceiveResend() throws Exception
    {
        // Create a 0-10 Connection and send message
        setSystemProperty(ClientProperties.AMQP_VERSION, "0-10");

        Connection connection010 = getConnection();

        Session session010 = connection010.createSession(true, Session.SESSION_TRANSACTED);

        // Create queue for testing
        Queue queue = session010.createQueue(getTestQueueName());

        // Ensure queue exists
        session010.createConsumer(queue).close();

        sendMessage(session010, queue, 1);

        // Close the 010 connection
        connection010.close();

        // Create a 0-8 Connection and receive message
        setSystemProperty(ClientProperties.AMQP_VERSION, "0-8");

        Connection connection08 = getConnection();

        Session session08 = connection08.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageConsumer consumer = session08.createConsumer(queue);

        connection08.start();

        Message message = consumer.receive(1000);

        assertNotNull("Didn't receive 0-10 message.", message);

        // Validate that JMS_QPID_DESTTYPE is not set
        try
        {
            message.getIntProperty(CustomJMSXProperty.JMS_QPID_DESTTYPE.toString());
            fail("JMS_QPID_DESTTYPE should not be set, so should throw NumberFormatException");
        }
        catch (NumberFormatException nfe)
        {

        }

        // Resend message back to queue and validate that
        // a) getJMSDestination works without the JMS_QPID_DESTTYPE
        // b) we can actually send without a BufferOverFlow.

        MessageProducer producer = session08.createProducer(queue);
        producer.send(message);

        message = consumer.receive(1000);

        assertNotNull("Didn't receive recent 0-8 message.", message);

        // Validate that JMS_QPID_DESTTYPE is not set
        assertEquals("JMS_QPID_DESTTYPE should be set to a Queue", AMQDestination.QUEUE_TYPE,
                     message.getIntProperty(CustomJMSXProperty.JMS_QPID_DESTTYPE.toString()));

    }

    public void testQueueWithBindingUrlUsingCustomExchange() throws Exception
    {
        String exchangeName = "exch_" + getTestQueueName();
        String queueName = "queue_" + getTestQueueName();
        
        String address = String.format("direct://%s/%s/%s?routingkey='%s'", exchangeName, queueName, queueName, queueName);
        sendReceive(address);
    }

    public void testQueueWithBindingUrlUsingAmqDirectExchange() throws Exception
    {
        String queueName = getTestQueueName();
        String address = String.format("direct://amq.direct/%s/%s?routingkey='%s'", queueName, queueName, queueName);
        sendReceive(address);
    }

    public void testQueueWithBindingUrlUsingDefaultExchange() throws Exception
    {
        String queueName = getTestQueueName();
        String address = String.format("direct:///%s/%s?routingkey='%s'", queueName, queueName, queueName);
        sendReceive(address);
    }

    private void sendReceive(String address) throws JMSException, Exception
    {
        Destination dest = _session.createQueue(address);
        MessageConsumer consumer = _session.createConsumer(dest);

        _connection.start();

        sendMessage(_session, dest, 1);

        Message receivedMessage = consumer.receive(10000);

        assertNotNull("Message should not be null", receivedMessage);

        Destination receivedDestination = receivedMessage.getJMSDestination();

        assertNotNull("JMSDestination should not be null", receivedDestination);
        assertEquals("JMSDestination should match that sent", address, receivedDestination.toString());
    }
}
