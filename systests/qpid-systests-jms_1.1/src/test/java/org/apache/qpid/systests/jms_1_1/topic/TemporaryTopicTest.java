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
package org.apache.qpid.systests.jms_1_1.topic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;

import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.JmsTestBase;

public class TemporaryTopicTest extends JmsTestBase
{
    @Test
    public void testMessageDeliveryUsingTemporaryTopic() throws Exception
    {
        final Connection connection = getConnection();
        try
        {
            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final TemporaryTopic topic = session.createTemporaryTopic();
            assertNotNull(topic, "Temporary topic is null");
            final MessageProducer producer = session.createProducer(topic);
            final MessageConsumer consumer1 = session.createConsumer(topic);
            final MessageConsumer consumer2 = session.createConsumer(topic);
            connection.start();
            producer.send(session.createTextMessage("hello"));

            final TextMessage tm1 = (TextMessage) consumer1.receive(getReceiveTimeout());
            final TextMessage tm2 = (TextMessage) consumer2.receive(getReceiveTimeout());

            assertNotNull(tm1, "Message not received by subscriber1");
            assertEquals("hello", tm1.getText());
            assertNotNull(tm2, "Message not received by subscriber2");
            assertEquals("hello", tm2.getText());
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testExplicitTemporaryTopicDeletion() throws Exception
    {
        final Connection connection = getConnection();
        try
        {
            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final TemporaryTopic topic = session.createTemporaryTopic();
            assertNotNull(topic, "Temporary topic is null");
            final MessageConsumer consumer = session.createConsumer(topic);
            connection.start();
            try
            {
                topic.delete();
                fail("Expected JMSException : should not be able to delete while there are active consumers");
            }
            catch (JMSException je)
            {
                //pass
            }

            consumer.close();

            // Now deletion should succeed.
            topic.delete();

            try
            {
                session.createConsumer(topic);
                fail("Exception not thrown");
            }
            catch (JMSException je)
            {
                //pass
            }
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testUseFromAnotherConnectionProhibited() throws Exception
    {
        final Connection connection = getConnection();
        try
        {
            final Connection connection2 = getConnection();
            try
            {
                final Session session1 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                final Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
                final TemporaryTopic topic = session1.createTemporaryTopic();

                try
                {
                    session2.createConsumer(topic);
                    fail("Expected a JMSException when subscribing to a temporary topic created on a different connection");
                }
                catch (JMSException je)
                {
                    // pass
                }
            }
            finally
            {
                connection2.close();
            }
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testTemporaryTopicReused() throws Exception
    {
        final Connection connection = getConnection();
        try
        {
            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final TemporaryTopic topic = session.createTemporaryTopic();
            assertNotNull(topic, "Temporary topic is null");

            final MessageProducer producer = session.createProducer(topic);
            final MessageConsumer consumer1 = session.createConsumer(topic);
            connection.start();
            producer.send(session.createTextMessage("message1"));
            TextMessage tm = (TextMessage) consumer1.receive(getReceiveTimeout());
            assertNotNull(tm, "Message not received by first consumer");
            assertEquals("message1", tm.getText());
            consumer1.close();

            final MessageConsumer consumer2 = session.createConsumer(topic);
            connection.start();
            producer.send(session.createTextMessage("message2"));
            tm = (TextMessage) consumer2.receive(getReceiveTimeout());
            assertNotNull(tm, "Message not received by second consumer");
            assertEquals("message2", tm.getText());
            consumer2.close();
        }
        finally
        {
            connection.close();
        }
    }
}
