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
 */
package org.apache.qpid.test.client.message;


import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.qpid.client.AMQConnectionURL;
import org.apache.qpid.test.utils.QpidBrokerTestCase;

public class JMSXUserIDTest extends QpidBrokerTestCase
{
    public void testJMSXUserIDIsSetByDefault() throws Exception
    {
        Connection connection = getConnection();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Destination destination = createTestQueue(session);
        MessageConsumer consumer = session.createConsumer(destination);
        MessageProducer producer = session.createProducer(destination);
        TextMessage message = session.createTextMessage("test");
        producer.send(message);
        assertEquals("Unexpected user ID", GUEST_USERNAME, message.getStringProperty("JMSXUserID"));
        session.commit();
        connection.start();
        Message receivedMessage = consumer.receive(RECEIVE_TIMEOUT);
        session.commit();
        assertNotNull("Expected receivedMessage not received", receivedMessage);
        assertEquals("Unexpected user ID", GUEST_USERNAME,receivedMessage.getStringProperty("JMSXUserID"));
    }

    public void testJMSXUserIDDisabled() throws Exception
    {

        String url = String.format("amqp://guest:guest@test/?brokerlist='tcp://localhost:%s'&populateJMSXUserID='false'",
                                   getDefaultBroker().getAmqpPort());
        Connection connection = getConnection(new AMQConnectionURL(url));
        Destination destination = getTestQueue();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        MessageConsumer consumer = session.createConsumer(destination);
        MessageProducer producer = session.createProducer(destination);
        TextMessage message = session.createTextMessage("test");
        producer.send(message);
        String userId = message.getStringProperty("JMSXUserID");
        assertEquals("Unexpected user ID =[" + userId + "]", null, userId);
        session.commit();
        connection.start();
        Message receivedMessage = consumer.receive(RECEIVE_TIMEOUT);
        session.commit();
        assertNotNull("Expected receivedMessage not received", receivedMessage);
        String receivedUserId = receivedMessage.getStringProperty("JMSXUserID");
        assertEquals("Unexpected user ID " + receivedUserId, null, receivedUserId);
    }
}
