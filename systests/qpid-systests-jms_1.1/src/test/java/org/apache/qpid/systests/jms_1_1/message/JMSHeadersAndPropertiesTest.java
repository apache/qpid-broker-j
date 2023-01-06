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
package org.apache.qpid.systests.jms_1_1.message;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.Enumeration;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageFormatException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.JmsTestBase;

public class JMSHeadersAndPropertiesTest extends JmsTestBase
{
    @Test
    public void resentJMSMessageGetsReplacementJMSMessageID() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            MessageProducer producer = session.createProducer(queue);

            final Message sentMessage = session.createMessage();
            producer.send(sentMessage);

            final String originalId = sentMessage.getJMSMessageID();
            assertNotNull(originalId, "JMSMessageID must be set after first publish");

            producer.send(sentMessage);
            final String firstResendID = sentMessage.getJMSMessageID();
            assertNotNull(firstResendID, "JMSMessageID must be set after first resend");
            assertNotSame(originalId, firstResendID, "JMSMessageID must be changed second publish");
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void redelivered() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnectionBuilder().setPrefetch(1).build();
        try
        {
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageProducer producer = session.createProducer(queue);
            producer.send(session.createTextMessage("A"));
            producer.send(session.createTextMessage("B"));
            session.commit();

            MessageConsumer consumer = session.createConsumer(queue);
            connection.start();

            Message message = consumer.receive(getReceiveTimeout());
            assertTrue(message instanceof TextMessage, "TextMessage should be received");
            assertFalse(message.getJMSRedelivered(), "Unexpected JMSRedelivered after first receive");
            assertEquals("A", ((TextMessage) message).getText(), "Unexpected message content");

            session.rollback();

            message = consumer.receive(getReceiveTimeout());
            assertTrue(message instanceof TextMessage, "TextMessage should be received");
            assertTrue(message.getJMSRedelivered(), "Unexpected JMSRedelivered after second receive");
            assertEquals("A", ((TextMessage) message).getText(), "Unexpected message content");

            message = consumer.receive(getReceiveTimeout());
            assertTrue(message instanceof TextMessage, "TextMessage should be received");
            assertFalse(message.getJMSRedelivered(), "Unexpected JMSRedelivered for second message");
            assertEquals("B", ((TextMessage) message).getText(), "Unexpected message content");

            session.commit();
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void headers() throws Exception
    {
        final Queue queue = createQueue(getTestName());
        final Destination replyTo = createQueue(getTestName() + "_replyTo");
        final Connection consumerConnection = getConnection();
        try
        {
            final Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final MessageConsumer consumer = consumerSession.createConsumer(queue);

            final String correlationId = "testCorrelationId";
            final String jmsType = "testJmsType";

            final int priority = 1;
            final long timeToLive = 30 * 60 * 1000;
            final Connection producerConnection = getConnection();
            try
            {
                final Session producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                final MessageProducer producer = producerSession.createProducer(queue);

                final Message message = producerSession.createMessage();
                message.setJMSCorrelationID(correlationId);
                message.setJMSType(jmsType);
                message.setJMSReplyTo(replyTo);

                long currentTime = System.currentTimeMillis();
                producer.send(message, DeliveryMode.NON_PERSISTENT, priority, timeToLive);

                consumerConnection.start();

                Message receivedMessage = consumer.receive(getReceiveTimeout());
                assertNotNull(receivedMessage);

                assertEquals(correlationId, receivedMessage.getJMSCorrelationID(), "JMSCorrelationID mismatch");
                assertEquals(message.getJMSType(), receivedMessage.getJMSType(), "JMSType mismatch");
                assertEquals(message.getJMSReplyTo(), receivedMessage.getJMSReplyTo(), "JMSReply To mismatch");
                assertTrue(receivedMessage.getJMSMessageID().startsWith("ID:"), "JMSMessageID does not start 'ID:'");
                assertEquals(priority, receivedMessage.getJMSPriority(), "JMSPriority mismatch");
                assertTrue(receivedMessage.getJMSExpiration() >= currentTime + timeToLive &&
                                receivedMessage.getJMSExpiration() <= System.currentTimeMillis() + timeToLive,
                        String.format("Unexpected JMSExpiration: got '%d', but expected value equals or greater than '%d'",
                                receivedMessage.getJMSExpiration(), currentTime + timeToLive));
            }
            finally
            {
                producerConnection.close();
            }
        }
        finally
        {
            consumerConnection.close();
        }
    }

    @Test
    public void groupIDAndGroupSeq() throws Exception
    {
        final Connection connection = getConnection();
        try
        {
            assumeTrue(isJMSXPropertySupported(connection, "JMSXGroupID"));
            assumeTrue(isJMSXPropertySupported(connection, "JMSXGroupSeq"));

            final String groupId = "testGroup";
            final int groupSequence = 3;
            final Queue queue = createQueue(getTestName());
            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final MessageProducer producer = session.createProducer(queue);
            final MessageConsumer consumer = session.createConsumer(queue);
            final Message message = session.createMessage();
            message.setStringProperty("JMSXGroupID", groupId);
            message.setIntProperty("JMSXGroupSeq", groupSequence);

            producer.send(message);
            connection.start();

            final Message receivedMessage = consumer.receive(getReceiveTimeout());
            assertNotNull(receivedMessage);

            assertEquals(groupId, receivedMessage.getStringProperty("JMSXGroupID"),
                    "Unexpected JMSXGroupID");
            assertEquals(groupSequence, receivedMessage.getIntProperty("JMSXGroupSeq"),
                    "Unexpected JMSXGroupSeq");
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void propertyValues() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(queue);
            Message message = session.createMessage();

            message.setBooleanProperty("boolean", true);
            message.setByteProperty("byte", Byte.MAX_VALUE);
            message.setShortProperty("short", Short.MAX_VALUE);
            message.setIntProperty("int", Integer.MAX_VALUE);
            message.setFloatProperty("float", Float.MAX_VALUE);
            message.setDoubleProperty("double", Double.MAX_VALUE);

            producer.send(message);

            final MessageConsumer consumer = session.createConsumer(queue);
            connection.start();

            final Message receivedMessage = consumer.receive(getReceiveTimeout());
            assertNotNull(receivedMessage);

            assertEquals(Boolean.TRUE, message.getBooleanProperty("boolean"),
                    "Unexpected boolean property value");
            assertEquals(Byte.MAX_VALUE, message.getByteProperty("byte"), "Unexpected byte property value");
            assertEquals(Short.MAX_VALUE, message.getShortProperty("short"),
                    "Unexpected short property value");
            assertEquals(Integer.MAX_VALUE, message.getIntProperty("int"), "Unexpected int property value");
            assertEquals(Float.MAX_VALUE, message.getFloatProperty("float"), 0f,
                    "Unexpected float property value");
            assertEquals(Double.MAX_VALUE, message.getDoubleProperty("double"), 0d,
                    "Unexpected double property value");
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void unsupportedObjectPropertyValue() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(queue);
            Message message = session.createMessage();
            try
            {
                message.setObjectProperty("invalidObject", new Exception());
            }
            catch (MessageFormatException e)
            {
                // pass
            }
            String validValue = "validValue";
            message.setObjectProperty("validObject", validValue);
            producer.send(message);

            final MessageConsumer consumer = session.createConsumer(queue);
            connection.start();

            final Message receivedMessage = consumer.receive(getReceiveTimeout());
            assertNotNull(receivedMessage);

            assertFalse(message.propertyExists("invalidObject"), "Unexpected property found");
            assertEquals(validValue, message.getObjectProperty("validObject"), "Unexpected property value");
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void disableJMSMessageId() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(queue);
            Message message = session.createMessage();
            producer.send(message);
            assertNotNull(message.getJMSMessageID(), "Produced message is expected to have a JMSMessageID");

            final MessageConsumer consumer = session.createConsumer(queue);
            connection.start();

            final Message receivedMessageWithId = consumer.receive(getReceiveTimeout());
            assertNotNull(receivedMessageWithId);

            assertNotNull(receivedMessageWithId.getJMSMessageID(),
                    "Received message is expected to have a JMSMessageID");
            assertEquals(message.getJMSMessageID(), receivedMessageWithId.getJMSMessageID(),
                    "Received message JMSMessageID should match the sent");

            producer.setDisableMessageID(true);
            producer.send(message);
            assertNull(message.getJMSMessageID(), "Produced message is expected to not have a JMSMessageID");

            final Message receivedMessageWithoutId = consumer.receive(getReceiveTimeout());
            assertNotNull(receivedMessageWithoutId);
            assertNull(receivedMessageWithoutId.getJMSMessageID(),
                    "Received message is not expected to have a JMSMessageID");
        }
        finally
        {
            connection.close();
        }
    }


    private boolean isJMSXPropertySupported(final Connection connection, final String propertyName) throws JMSException
    {
        boolean supported = false;
        Enumeration props = connection.getMetaData().getJMSXPropertyNames();
        while (props.hasMoreElements())
        {
            String name = (String) props.nextElement();
            if (name.equals(propertyName))
            {
                supported = true;
            }
        }
        return supported;
    }
}
