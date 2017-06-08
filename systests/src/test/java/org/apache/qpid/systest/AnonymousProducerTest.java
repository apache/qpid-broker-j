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

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.qpid.test.utils.QpidBrokerTestCase;

public class AnonymousProducerTest extends QpidBrokerTestCase
{

    public void testPublishIntoDestinationBoundWithNotMatchingFilter() throws Exception
    {
        final Connection connection = getConnection();
        connection.start();

        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        MessageProducer messageProducer = session.createProducer(null);

        Topic topic = createTopicOnFanout(connection, getTestName());

        session.createConsumer(topic, "id>1");
        TextMessage test = session.createTextMessage("testMessage");
        test.setIntProperty("id", 1);
        try
        {
            messageProducer.send(topic, test);
            session.commit();
        }
        catch (JMSException e)
        {
            fail("Exception should not be thrown: " + e.getMessage());
        }
    }

    public void testPublishIntoNonExistingTopic() throws Exception
    {
        final Connection connection = getConnection();
        connection.start();

        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        MessageProducer messageProducer = session.createProducer(null);

        try
        {
            messageProducer.send(createTopicOnFanout(connection, "nonExistingTopic"),
                                 session.createTextMessage("testMessage"));
            session.commit();
        }
        catch (JMSException e)
        {
            fail("Message should be silently discarded. Exception should not be thrown: " + e.getMessage());
        }
    }

    public void testPublishIntoNonExistingQueue() throws Exception
    {
        final Connection connection = getConnection();
        connection.start();

        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        MessageProducer messageProducer = session.createProducer(null);

        try
        {
            messageProducer.send(session.createQueue("nonExistingQueue"), session.createTextMessage("testMessage"));
            session.commit();
            fail("Expected exception was not thrown");
        }
        catch (JMSException e)
        {
            // pass
        }
    }

    public void testSyncPublishIntoNonExistingQueue() throws Exception
    {
        Session session = getConnectionWithSyncPublishing().createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(null);
        try
        {
            final Queue queue = session.createQueue("nonExistingQueue");
            producer.send(queue, session.createTextMessage("hello"));
            fail("Send to unknown destination should result in error");
        }
        catch (JMSException e)
        {
            // pass
        }
    }

}
