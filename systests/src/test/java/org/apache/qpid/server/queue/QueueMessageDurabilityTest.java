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
package org.apache.qpid.server.queue;

import java.util.HashMap;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.NamingException;

import org.apache.qpid.configuration.ClientProperties;
import org.apache.qpid.server.store.MessageDurability;
import org.apache.qpid.test.utils.QpidBrokerTestCase;

public class QueueMessageDurabilityTest extends QpidBrokerTestCase
{
    private static final String DURABLE_ALWAYS_PERSIST_NAME = "DURABLE_QUEUE_ALWAYS_PERSIST";
    private static final String DURABLE_NEVER_PERSIST_NAME = "DURABLE_QUEUE_NEVER_PERSIST";
    private static final String DURABLE_DEFAULT_PERSIST_NAME = "DURABLE_QUEUE_DEFAULT_PERSIST";
    private static final String NONDURABLE_ALWAYS_PERSIST_NAME = "NONDURABLE_QUEUE_ALWAYS_PERSIST";
    private Queue _durableAlwaysPersist;
    private Queue _durableNeverPersist;
    private Queue _durableDefaultPersist;
    private Queue _nonDurableAlwaysPersist;
    private String _topicNameFormat;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        Connection conn = createStartedConnection();
        Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Map<String,Object> arguments = new HashMap<>();
        arguments.put(org.apache.qpid.server.model.Queue.MESSAGE_DURABILITY, MessageDurability.ALWAYS.name());
        arguments.put(org.apache.qpid.server.model.Queue.DURABLE, true);
        _durableAlwaysPersist = createQueueWithArguments(session, DURABLE_ALWAYS_PERSIST_NAME, arguments);

        arguments = new HashMap<>();
        arguments.put(org.apache.qpid.server.model.Queue.MESSAGE_DURABILITY, MessageDurability.NEVER.name());
        arguments.put(org.apache.qpid.server.model.Queue.DURABLE, true);
        _durableNeverPersist = createQueueWithArguments(session, DURABLE_NEVER_PERSIST_NAME, arguments);

        arguments = new HashMap<>();
        arguments.put(org.apache.qpid.server.model.Queue.MESSAGE_DURABILITY, MessageDurability.DEFAULT.name());
        arguments.put(org.apache.qpid.server.model.Queue.DURABLE, true);
        _durableDefaultPersist = createQueueWithArguments(session, DURABLE_DEFAULT_PERSIST_NAME, arguments);

        arguments = new HashMap<>();
        arguments.put(org.apache.qpid.server.model.Queue.MESSAGE_DURABILITY,MessageDurability.ALWAYS.name());
        arguments.put(org.apache.qpid.server.model.Queue.DURABLE, false);
        _nonDurableAlwaysPersist = createQueueWithArguments(session, NONDURABLE_ALWAYS_PERSIST_NAME, arguments);

        bindQueue(conn.createSession(false, Session.AUTO_ACKNOWLEDGE), "amq.topic", DURABLE_ALWAYS_PERSIST_NAME, "Y.*.*.*");
        bindQueue(conn.createSession(false, Session.AUTO_ACKNOWLEDGE), "amq.topic", DURABLE_NEVER_PERSIST_NAME, "*.Y.*.*");
        bindQueue(conn.createSession(false, Session.AUTO_ACKNOWLEDGE), "amq.topic", DURABLE_DEFAULT_PERSIST_NAME, "*.*.Y.*");
        bindQueue(conn.createSession(false, Session.AUTO_ACKNOWLEDGE), "amq.topic", NONDURABLE_ALWAYS_PERSIST_NAME, "*.*.*.Y");

        _topicNameFormat = isBroker10() ? "amq.topic/%s" : "%s";

        conn.close();
    }

    public void testSendPersistentMessageToAll() throws Exception
    {
        Connection conn = createStartedConnection();
        Session session = conn.createSession(true, Session.SESSION_TRANSACTED);
        MessageProducer producer = session.createProducer(null);
        conn.start();
        producer.send(session.createTopic(String.format(_topicNameFormat, "Y.Y.Y.Y")), session.createTextMessage("test"));
        session.commit();

        assertEquals(1, getQueueDepth(conn, _durableAlwaysPersist));
        assertEquals(1, getQueueDepth(conn, _durableNeverPersist));
        assertEquals(1, getQueueDepth(conn, _durableDefaultPersist));
        assertEquals(1, getQueueDepth(conn,_nonDurableAlwaysPersist));

        restartDefaultBroker();

        conn = createStartedConnection();
        assertEquals(1, getQueueDepth(conn, _durableAlwaysPersist));
        assertEquals(0, getQueueDepth(conn, _durableNeverPersist));
        assertEquals(1, getQueueDepth(conn, _durableDefaultPersist));

        assertFalse(isQueueExist(conn, _nonDurableAlwaysPersist));
    }

    public void testSendNonPersistentMessageToAll() throws Exception
    {
        Connection conn = createStartedConnection();
        Session session = conn.createSession(true, Session.SESSION_TRANSACTED);
        MessageProducer producer = session.createProducer(null);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        conn.start();
        producer.send(session.createTopic(String.format(_topicNameFormat, "Y.Y.Y.Y")), session.createTextMessage("test"));
        session.commit();

        assertEquals(1, getQueueDepth(conn, _durableAlwaysPersist));
        assertEquals(1, getQueueDepth(conn, _durableNeverPersist));
        assertEquals(1, getQueueDepth(conn, _durableDefaultPersist));
        assertEquals(1, getQueueDepth(conn,_nonDurableAlwaysPersist));

        restartDefaultBroker();

        conn = createStartedConnection();
        assertEquals(1, getQueueDepth(conn, _durableAlwaysPersist));
        assertEquals(0, getQueueDepth(conn, _durableNeverPersist));
        assertEquals(0, getQueueDepth(conn, _durableDefaultPersist));

        assertFalse(isQueueExist(conn, _nonDurableAlwaysPersist));

    }

    public void testNonPersistentContentRetained() throws Exception
    {
        Connection conn = createStartedConnection();
        Session session = conn.createSession(true, Session.SESSION_TRANSACTED);
        MessageProducer producer = session.createProducer(null);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        conn.start();
        producer.send(session.createTopic(String.format(_topicNameFormat, "N.N.Y.Y")), session.createTextMessage("test1"));
        producer.send(session.createTopic(String.format(_topicNameFormat, "Y.N.Y.Y")), session.createTextMessage("test2"));
        session.commit();
        MessageConsumer consumer = session.createConsumer(_durableAlwaysPersist);
        Message msg = consumer.receive(getReceiveTimeout());
        assertNotNull(msg);
        assertTrue(msg instanceof TextMessage);
        assertEquals("test2", ((TextMessage) msg).getText());
        session.rollback();
        restartDefaultBroker();
        conn = createStartedConnection();
        session = conn.createSession(true, Session.SESSION_TRANSACTED);
        assertEquals(1, getQueueDepth(conn, _durableAlwaysPersist));
        assertEquals(0, getQueueDepth(conn, _durableNeverPersist));
        assertEquals(0, getQueueDepth(conn, _durableDefaultPersist));

        consumer = session.createConsumer(_durableAlwaysPersist);
        msg = consumer.receive(getReceiveTimeout());
        assertNotNull(msg);
        assertTrue(msg instanceof TextMessage);
        assertEquals("test2", ((TextMessage)msg).getText());
        session.commit();
    }

    public void testPersistentContentRetainedOnTransientQueue() throws Exception
    {
        setTestClientSystemProperty(ClientProperties.QPID_DECLARE_QUEUES_PROP_NAME, "false");
        Connection conn = createStartedConnection();
        Session session = conn.createSession(true, Session.SESSION_TRANSACTED);
        MessageProducer producer = session.createProducer(null);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        conn.start();
        producer.send(session.createTopic(String.format(_topicNameFormat, "N.N.Y.Y")), session.createTextMessage("test1"));
        session.commit();
        MessageConsumer consumer = session.createConsumer(_durableDefaultPersist);
        Message msg = consumer.receive(getReceiveTimeout());
        assertNotNull(msg);
        assertTrue(msg instanceof TextMessage);
        assertEquals("test1", ((TextMessage)msg).getText());
        session.commit();
        System.gc();
        consumer = session.createConsumer(_nonDurableAlwaysPersist);
        msg = consumer.receive(getReceiveTimeout());
        assertNotNull(msg);
        assertTrue(msg instanceof TextMessage);
        assertEquals("test1", ((TextMessage)msg).getText());
        session.commit();
    }

    private Connection createStartedConnection() throws JMSException, NamingException
    {
        Connection conn = getConnection();
        conn.start();
        return conn;
    }

    private Queue createQueueWithArguments(final Session session,
                                           final String testQueueName,
                                           final Map<String, Object> arguments) throws Exception
    {
        createEntityUsingAmqpManagement(testQueueName, session, "org.apache.qpid.Queue", arguments);
        return getQueueFromName(session, testQueueName);
    }

    private void bindQueue(final Session session, final String exchange, final String queueName,
                           final String bindingKey) throws Exception
    {

        final Map<String, Object> arguments = new HashMap<>();
        arguments.put("destination", queueName);
        arguments.put("bindingKey", bindingKey);
        performOperationUsingAmqpManagement(exchange, "bind", session, "org.apache.qpid.TopicExchange", arguments);
    }

}
