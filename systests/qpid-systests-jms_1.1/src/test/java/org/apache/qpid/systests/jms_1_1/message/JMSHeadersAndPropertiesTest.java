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

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeThat;

import java.util.Enumeration;

import javax.jms.Connection;
import javax.jms.ConnectionMetaData;
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

import org.junit.Ignore;
import org.junit.Test;

import org.apache.qpid.systests.JmsTestBase;

public class JMSHeadersAndPropertiesTest extends JmsTestBase
{

    @Ignore("QPID-8031")
    @Test
    public void testResentJMSMessageGetsReplacementJMSMessageID() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            ConnectionMetaData metaData = connection.getMetaData();

            MessageProducer producer = session.createProducer(queue);

            final Message sentMessage = session.createMessage();
            producer.send(sentMessage);

            final String originalId = sentMessage.getJMSMessageID();
            assertNotNull("JMSMessageID must be set after first publish", originalId);

            producer.send(sentMessage);
            final String firstResendID = sentMessage.getJMSMessageID();
            assertNotNull("JMSMessageID must be set after first resend", firstResendID);
            assertNotSame("JMSMessageID must be changed second publish", originalId, firstResendID);

            producer.setDisableMessageID(true);
            producer.send(sentMessage);
            final String secondResendID = sentMessage.getJMSMessageID();
            assertNull("JMSMessageID must be unset after second resend with IDs disabled", secondResendID);
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void JMSRedelivered() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageProducer producer = session.createProducer(queue);
            String messageText = "Test";
            producer.send(session.createTextMessage(messageText));
            session.commit();

            MessageConsumer consumer = session.createConsumer(queue);
            connection.start();

            Message message = consumer.receive(getReceiveTimeout());
            assertTrue("TextMessage should be received", message instanceof TextMessage);
            assertFalse("Unexpected JMSRedelivered after first receive", message.getJMSRedelivered());
            assertEquals("Unexpected message content", messageText, ((TextMessage) message).getText());

            session.rollback();

            message = consumer.receive(getReceiveTimeout());
            assertTrue("TextMessage should be received", message instanceof TextMessage);
            assertTrue("Unexpected JMSRedelivered after second receive", message.getJMSRedelivered());
            assertEquals("Unexpected message content", messageText, ((TextMessage) message).getText());

            session.commit();
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testJMSHeaders() throws Exception
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

                assertEquals("JMSCorrelationID mismatch", correlationId, receivedMessage.getJMSCorrelationID());
                assertEquals("JMSType mismatch", message.getJMSType(), receivedMessage.getJMSType());
                assertEquals("JMSReply To mismatch", message.getJMSReplyTo(), receivedMessage.getJMSReplyTo());
                assertTrue("JMSMessageID does not start 'ID:'", receivedMessage.getJMSMessageID().startsWith("ID:"));
                assertEquals("JMSPriority mismatch", priority, receivedMessage.getJMSPriority());
                assertTrue(String.format(
                        "Unexpected JMSExpiration: got '%d', but expected value equals or greater than '%d'",
                        receivedMessage.getJMSExpiration(),
                        currentTime + timeToLive),

                           receivedMessage.getJMSExpiration() >= currentTime + timeToLive
                           && receivedMessage.getJMSExpiration() <= System.currentTimeMillis() + timeToLive);
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
    public void testJMSXGroupIDAndJMSXGroupSeq() throws Exception
    {
        final Connection connection = getConnection();
        try
        {
            assumeThat(isJMSXPropertySupported(connection, "JMSXGroupID"), is(equalTo(true)));
            assumeThat(isJMSXPropertySupported(connection, "JMSXGroupSeq"), is(equalTo(true)));

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

            assertEquals("Unexpected JMSXGroupID", groupId, receivedMessage.getStringProperty("JMSXGroupID"));
            assertEquals("Unexpected JMSXGroupSeq", groupSequence, receivedMessage.getIntProperty("JMSXGroupSeq"));
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
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
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
            session.commit();

            final MessageConsumer consumer = session.createConsumer(queue);
            connection.start();

            final Message receivedMessage = consumer.receive(getReceiveTimeout());
            assertNotNull(receivedMessage);

            assertFalse("Unexpected property found", message.propertyExists("invalidObject"));
            assertEquals("Unexpected property value", validValue, message.getObjectProperty("validObject"));
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
