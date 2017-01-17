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
package org.apache.qpid.test.unit.message;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
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

import org.apache.qpid.test.utils.QpidBrokerTestCase;


public class ForeignMessageTest extends QpidBrokerTestCase
{

    private static final String JMS_CORR_ID = "QPIDID_01";
    private static final String JMS_TYPE = "test.jms.type";
    private static final String GROUP = "group";
    private static final int JMSX_GROUP_SEQ_VALUE = 1;

    /**
     * Tests that a non-Qpid JMS message (in this case a proxy) can be sent and received.
     */
    public void testSendForeignMessage() throws Exception
    {
        final Connection con = getConnection();
        final Session session = con.createSession(true, Session.SESSION_TRANSACTED);
        Queue queue = createTestQueue(session);
        Destination replyTo = createTestQueue(session, "my.replyto");

        MessageConsumer consumer = session.createConsumer(queue);

        final MessageProducer producer = session.createProducer(queue);

        // create a 'foreign' JMS message using proxy
        ObjectMessage sentMsg = getForeignObjectMessage(session.createObjectMessage());

        sentMsg.setJMSCorrelationID(JMS_CORR_ID);
        sentMsg.setJMSType(JMS_TYPE);
        sentMsg.setStringProperty("JMSXGroupID", GROUP);
        sentMsg.setIntProperty("JMSXGroupSeq", JMSX_GROUP_SEQ_VALUE);
        sentMsg.setJMSReplyTo(replyTo);
        Serializable payload = UUID.randomUUID();
        sentMsg.setObject(payload);

        // send it
        producer.send(sentMsg);
        String sentMessageId = sentMsg.getJMSMessageID();
        session.commit();

        con.start();

        // get message and check JMS properties
        ObjectMessage rm = (ObjectMessage) consumer.receive(getReceiveTimeout());
        assertNotNull(rm);

        assertEquals("JMS Correlation ID mismatch", sentMsg.getJMSCorrelationID(), rm.getJMSCorrelationID());
        assertEquals("JMS Type mismatch", sentMsg.getJMSType(), rm.getJMSType());
        assertEquals("JMS Reply To mismatch", sentMsg.getJMSReplyTo(), rm.getJMSReplyTo());
        assertEquals("JMSMessageID mismatch:", sentMessageId, rm.getJMSMessageID());
        assertEquals("JMS Default priority should be 4",Message.DEFAULT_PRIORITY,rm.getJMSPriority());

        //Validate that the JMSX values are correct
        assertEquals("JMSXGroupID is not as expected:", GROUP, rm.getStringProperty("JMSXGroupID"));
        assertEquals("JMSXGroupSeq is not as expected:", JMSX_GROUP_SEQ_VALUE, rm.getIntProperty("JMSXGroupSeq"));

        assertEquals("Message payload not as expected", payload, rm.getObject());

        session.commit();
    }

    private ObjectMessage getForeignObjectMessage(final ObjectMessage message)
    {
        return Reflection.newProxy(ObjectMessage.class, new AbstractInvocationHandler()
        {
            @Override
            protected Object handleInvocation(final Object proxy, final Method method, final Object[] args)
                    throws Throwable
            {
                return method.invoke(message, args);
            }
        });
    }
}
