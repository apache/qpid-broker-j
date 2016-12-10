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
package org.apache.qpid.test.unit.topic;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

import org.apache.qpid.test.utils.QpidBrokerTestCase;

public class TopicSubscriberTest extends QpidBrokerTestCase
{
    private Connection _connection;
    private Topic _topic;
    private Session _session;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        _connection = getConnection();
        _topic = createTopic(_connection, "mytopic");
        _session = _connection.createSession(true, Session.SESSION_TRANSACTED);
    }

    public void testCreateSubscriber() throws JMSException
    {
        TopicSession topicSession = (TopicSession)_session;
        TopicSubscriber subscriber = topicSession.createSubscriber(_topic);
        assertEquals("Topic names should match from TopicSubscriber", _topic.getTopicName(), subscriber.getTopic().getTopicName());

        subscriber = topicSession.createSubscriber(_topic, "abc", false);
        assertEquals("Topic names should match from TopicSubscriber with selector",
                     _topic.getTopicName(),
                     subscriber.getTopic().getTopicName());
    }

    public void testCreateDurableSubscriber() throws JMSException
    {
        TopicSubscriber subscriber = _session.createDurableSubscriber(_topic, "mysubname");
        assertEquals("Topic names should match from durable TopicSubscriber", _topic.getTopicName(), subscriber.getTopic().getTopicName());
        subscriber.close();

        subscriber = _session.createDurableSubscriber(_topic, "mysubname2", "abc", false);
        assertEquals("Topic names should match from durable TopicSubscriber with selector", _topic.getTopicName(), subscriber.getTopic().getTopicName());
        subscriber.close();
        _session.unsubscribe("mysubname");
        _session.unsubscribe("mysubname2");
    }
}
