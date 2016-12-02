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

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.qpid.test.utils.QpidBrokerTestCase;

public class JMSHeaderTest extends QpidBrokerTestCase
{

    public void testResentJMSMessageGetsReplacementJMSMessageID() throws Exception
    {
        Connection con = getConnection();
        con.start();
        Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Queue queue = createTestQueue(session);

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
}
