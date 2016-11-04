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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.virtualhost.NodeAutoCreationPolicy;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.test.utils.QpidBrokerTestCase;
import org.apache.qpid.test.utils.TestBrokerConfiguration;

public class NodeAutoCreationPolicyTest extends QpidBrokerTestCase
{

    private Connection _connection;
    private Session _session;


    public String getTestProfileVirtualHostNodeBlueprint()
    {
        String blueprint = super.getTestProfileVirtualHostNodeBlueprint();
        ObjectMapper mapper = new ObjectMapper();
        try
        {
            Map blueprintMap = mapper.readValue(blueprint, Map.class);

            NodeAutoCreationPolicy[] policies = new NodeAutoCreationPolicy[] {
                    new NodeAutoCreationPolicy()
                    {
                        @Override
                        public String getPattern()
                        {
                            return "fooQ.*";
                        }

                        @Override
                        public boolean isCreatedOnPublish()
                        {
                            return true;
                        }

                        @Override
                        public boolean isCreatedOnConsume()
                        {
                            return true;
                        }

                        @Override
                        public String getNodeType()
                        {
                            return "Queue";
                        }

                        @Override
                        public Map<String, Object> getAttributes()
                        {
                            return Collections.emptyMap();
                        }
                    },
                    new NodeAutoCreationPolicy()
                    {
                        @Override
                        public String getPattern()
                        {
                            return "barE.*";
                        }

                        @Override
                        public boolean isCreatedOnPublish()
                        {
                            return true;
                        }

                        @Override
                        public boolean isCreatedOnConsume()
                        {
                            return false;
                        }

                        @Override
                        public String getNodeType()
                        {
                            return "Exchange";
                        }

                        @Override
                        public Map<String, Object> getAttributes()
                        {
                            return Collections.<String, Object>singletonMap(Exchange.TYPE, "fanout");
                        }
                    }
            };

            blueprintMap.put(QueueManagingVirtualHost.NODE_AUTO_CREATION_POLICIES, Arrays.asList(policies));
            String newprint = mapper.writeValueAsString(blueprintMap);
            return newprint;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }


    protected void setUp() throws Exception
    {
        getDefaultBrokerConfiguration().removeObjectConfiguration(VirtualHostNode.class, TestBrokerConfiguration.ENTRY_NAME_VIRTUAL_HOST);

        createTestVirtualHostNode(TestBrokerConfiguration.ENTRY_NAME_VIRTUAL_HOST);

        super.setUp();

        _connection = getConnection();
        _session = _connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        _connection.start();
    }

    public void testSendingToQueuePattern() throws Exception
    {
        final Queue queue = _session.createQueue("ADDR: fooQueue ; { assert: never, node: { type: queue } }");
        final MessageProducer producer = _session.createProducer(queue);
        producer.send(_session.createTextMessage("Hello world!"));

        final MessageConsumer consumer = _session.createConsumer(queue);
        Message received = consumer.receive(2000l);
        assertNotNull(received);
        assertTrue(received instanceof TextMessage);
        assertEquals("Hello world!", ((TextMessage)received).getText());
    }


    public void testSendingToNonMatchingQueuePattern() throws Exception
    {
        final Queue queue = _session.createQueue("ADDR: foQueue ; { assert: never, node: { type: queue } }");
        try
        {
            final MessageProducer producer = _session.createProducer(queue);
            fail("Creating producer should fail");
        }
        catch(JMSException e)
        {
            assertNotNull(e.getLinkedException());

            assertEquals("The name 'foQueue' supplied in the address doesn't resolve to an exchange or a queue",
                         e.getLinkedException().getMessage());
        }
    }


    public void testSendingToExchangePattern() throws Exception
    {
        final Topic topic = _session.createTopic("ADDR: barExchange/foo ; { assert: never, node: { type: topic } }");
        final MessageProducer producer = _session.createProducer(topic);
        producer.send(_session.createTextMessage("Hello world!"));

        final MessageConsumer consumer = _session.createConsumer(topic);
        Message received = consumer.receive(1000l);
        assertNull(received);

        producer.send(_session.createTextMessage("Hello world2!"));
        received = consumer.receive(1000l);

        assertNotNull(received);

        assertTrue(received instanceof TextMessage);
        assertEquals("Hello world2!", ((TextMessage)received).getText());
    }


    public void testSendingToNonMatchingTopicPattern() throws Exception
    {
        final Topic topic = _session.createTopic("ADDR: baa ; { assert: never, node: { type: topic } }");
        try
        {
            final MessageProducer producer = _session.createProducer(topic);
            fail("Creating producer should fail");
        }
        catch(JMSException e)
        {
            assertNotNull(e.getLinkedException());

            assertEquals("The name 'baa' supplied in the address doesn't resolve to an exchange or a queue",
                         e.getLinkedException().getMessage());
        }
    }


    public void testSendingToQueuePatternBURL() throws Exception
    {
        final Queue queue = _session.createQueue("BURL:direct:///fooQ/fooQ");
        final MessageProducer producer = _session.createProducer(queue);
        producer.send(_session.createTextMessage("Hello world!"));

        final MessageConsumer consumer = _session.createConsumer(queue);
        Message received = consumer.receive(2000l);
        assertNotNull(received);
        assertTrue(received instanceof TextMessage);
        assertEquals("Hello world!", ((TextMessage)received).getText());
    }


    public void testSendingToNonMatchingQueuePatternBURL() throws Exception
    {
        final Queue queue = _session.createQueue("BURL:direct:///fo/fo");
        try
        {
            _connection.close();
            _connection = getConnectionWithSyncPublishing();
            _session = _connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            _connection.start();
            final MessageProducer producer = _session.createProducer(queue);
            producer.send(_session.createTextMessage("Hello world!"));

            fail("Sending a message should fail");
        }
        catch(JMSException e)
        {
            // pass
        }
    }
}
