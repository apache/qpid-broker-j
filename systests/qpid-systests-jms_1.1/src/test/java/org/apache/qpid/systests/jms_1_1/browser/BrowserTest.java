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
package org.apache.qpid.systests.jms_1_1.browser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Enumeration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.jms.Connection;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;

import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.JmsTestBase;

public class BrowserTest extends JmsTestBase
{
    private static final String INDEX = "index";

    @Test
    public void emptyQueue() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            QueueBrowser browser = session.createBrowser(queue);
            Enumeration enumeration = browser.getEnumeration();
            assertFalse(enumeration.hasMoreElements());
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void browser() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final int lastIndex = 10;
            final List<Integer> indices = IntStream.rangeClosed(1, lastIndex).boxed().collect(Collectors.toList());
            populateQueue(queue, session, indices);

            QueueBrowser browser = session.createBrowser(queue);
            Enumeration enumeration = browser.getEnumeration();

            Message browsedMessage = null;
            long browsed = 0;
            while(enumeration.hasMoreElements())
            {
                browsed++;
                browsedMessage = (Message) enumeration.nextElement();
            }
            assertEquals(indices.size(), browsed, "Unexpected number of messages in enumeration");
            assertEquals(lastIndex, browsedMessage.getIntProperty(INDEX), "Last message has unexpected index");

            browser.close();
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void browserWithSelector() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final int lastIndex = 10;
            final List<Integer> indices = IntStream.rangeClosed(1, lastIndex).boxed().collect(Collectors.toList());
            populateQueue(queue, session, indices);

            QueueBrowser browser = session.createBrowser(queue, "index % 2 = 0");
            Enumeration enumeration = browser.getEnumeration();

            Message browsedMessage = null;
            long browsed = 0;
            while(enumeration.hasMoreElements())
            {
                browsed++;
                browsedMessage = (Message) enumeration.nextElement();
            }
            assertEquals(5, browsed, "Unexpected number of messages in enumeration");
            assertEquals(lastIndex, browsedMessage.getIntProperty(INDEX), "Last message has unexpected index");

            browser.close();
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void browserIsNonDestructive() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(queue);
            final Message message = session.createMessage();
            producer.send(message);
            producer.close();

            QueueBrowser browser = session.createBrowser(queue);
            Enumeration enumeration = browser.getEnumeration();
            assertTrue(enumeration.hasMoreElements(), "Unexpected browser state");

            Message browsedMessage = (Message) enumeration.nextElement();
            assertNotNull(browsedMessage, "No message returned by browser");
            assertEquals(message.getJMSMessageID(), browsedMessage.getJMSMessageID(),
                    "Unexpected JMSMessageID on browsed message");

            browser.close();

            MessageConsumer consumer = session.createConsumer(queue);
            Message consumedMessage = consumer.receive(getReceiveTimeout());
            assertNotNull(consumedMessage, "No message returned by consumer");
            assertEquals(message.getJMSMessageID(), consumedMessage.getJMSMessageID(),
                    "Unexpected JMSMessageID on consumed message");

            QueueBrowser browser2 = session.createBrowser(queue);
            Enumeration enumeration2 = browser2.getEnumeration();
            assertFalse(enumeration2.hasMoreElements(), "Unexpected browser state");
            browser2.close();
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void stoppedConnection() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            QueueBrowser browser = session.createBrowser(queue);
            try
            {
                browser.getEnumeration();
                // PASS
            }
            catch (IllegalStateException e)
            {
                //PASS
            }
        }
        finally
        {
            connection.close();
        }
    }

    private void populateQueue(final Queue queue, final Session session, final List<Integer> indices) throws JMSException
    {
        MessageProducer producer = session.createProducer(queue);
        indices.stream()
               .map(i -> createMessage(session, i))
               .forEach(x -> sendMessage(producer, x));
    }

    private void sendMessage(MessageProducer producer, final Message message)
    {
        try
        {
            producer.send(message);
        }
        catch (JMSException e)
        {
            throw new RuntimeException(e);
        }
    }

    private Message createMessage(final Session session, final int messageNumber)
    {
        try
        {
            Message message = session.createMessage();
            message.setIntProperty(INDEX, messageNumber);
            return message;
        }
        catch (JMSException e)
        {
            throw new RuntimeException(e);
        }
    }
}
