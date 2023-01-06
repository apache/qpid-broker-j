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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.UUID;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;

import com.google.common.reflect.AbstractInvocationHandler;
import com.google.common.reflect.Reflection;

import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.JmsTestBase;

public class ForeignMessageTest extends JmsTestBase
{
    @Test
    public void testSendForeignMessage() throws Exception
    {
        final Destination replyTo = createQueue(getTestName() + "_replyTo");
        final Queue queue = createQueue(getTestName());
        final Connection connection = getConnection();
        try
        {
            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final String jmsType = "TestJmsType";
            final String correlationId = "testCorrelationId";
            final ObjectMessage message = session.createObjectMessage();
            final ObjectMessage foreignMessage =
                    Reflection.newProxy(ObjectMessage.class, new AbstractInvocationHandler()
                    {
                        @Override
                        protected Object handleInvocation(final Object proxy, final Method method, final Object[] args)
                                throws Throwable
                        {
                            return method.invoke(message, args);
                        }
                    });

            foreignMessage.setJMSCorrelationID(correlationId);
            foreignMessage.setJMSType(jmsType);
            foreignMessage.setJMSReplyTo(replyTo);
            Serializable payload = UUID.randomUUID();
            foreignMessage.setObject(payload);

            final MessageConsumer consumer = session.createConsumer(queue);
            final MessageProducer producer = session.createProducer(queue);
            producer.send(foreignMessage);

            connection.start();

            Message receivedMessage = consumer.receive(getReceiveTimeout());
            assertTrue(receivedMessage instanceof ObjectMessage, "ObjectMessage was not received ");
            assertEquals(foreignMessage.getJMSCorrelationID(), receivedMessage.getJMSCorrelationID(),
                    "JMSCorrelationID mismatch");
            assertEquals(foreignMessage.getJMSType(), receivedMessage.getJMSType(), "JMSType mismatch");
            assertEquals(foreignMessage.getJMSReplyTo(), receivedMessage.getJMSReplyTo(),
                    "JMSReply To mismatch");
            assertEquals(foreignMessage.getJMSMessageID(), receivedMessage.getJMSMessageID(),
                    "JMSMessageID mismatch");
            assertEquals(Message.DEFAULT_PRIORITY, receivedMessage.getJMSPriority(),
                    "JMS Default priority should be default");
            assertEquals(payload, ((ObjectMessage) receivedMessage).getObject(),
                    "Message payload not as expected");
        }
        finally
        {
            connection.close();
        }
    }
}
