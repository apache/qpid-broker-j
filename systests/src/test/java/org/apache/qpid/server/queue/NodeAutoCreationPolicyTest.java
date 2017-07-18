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
import java.util.HashMap;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.qpid.client.AMQSession;
import org.apache.qpid.server.exchange.ExchangeDefaults;
import org.apache.qpid.server.model.AlternateBinding;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.virtualhost.NodeAutoCreationPolicy;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.test.utils.QpidBrokerTestCase;
import org.apache.qpid.test.utils.TestBrokerConfiguration;

public class NodeAutoCreationPolicyTest extends QpidBrokerTestCase
{
    private static final String DEAD_LETTER_QUEUE_SUFFIX = "_DLQ";
    private static final String DEAD_LETTER_EXCHANGE_SUFFIX = "_DLE";

    private Connection _connection;
    private Session _session;


    @Override
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
                            return Collections.singletonMap(Exchange.TYPE, "fanout");
                        }
                    },

                    new NodeAutoCreationPolicy()
                    {
                        @Override
                        public String getPattern()
                        {
                            return ".*" + DEAD_LETTER_QUEUE_SUFFIX;
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
                            return ".*" + DEAD_LETTER_EXCHANGE_SUFFIX;
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
                            return Collections.singletonMap(Exchange.TYPE, ExchangeDefaults.FANOUT_EXCHANGE_CLASS);
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


    @Override
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
        final Queue queue = _session.createQueue(isBroker10() ? "fooQueue" : "ADDR: fooQueue ; { assert: never, node: { type: queue } }");
        final MessageProducer producer = _session.createProducer(queue);
        producer.send(_session.createTextMessage("Hello world!"));

        final MessageConsumer consumer = _session.createConsumer(queue);
        Message received = consumer.receive(getReceiveTimeout());
        assertNotNull(received);
        assertTrue(received instanceof TextMessage);
        assertEquals("Hello world!", ((TextMessage)received).getText());
    }


    public void testSendingToNonMatchingQueuePattern() throws Exception
    {
        final Queue queue = _session.createQueue(isBroker10() ? "foQueue" : "ADDR: foQueue ; { assert: never, node: { type: queue } }");
        try
        {
            final MessageProducer producer = _session.createProducer(queue);
            fail("Creating producer should fail");
        }
        catch(JMSException e)
        {
            if(isBroker10())
            {
                assertTrue(e instanceof InvalidDestinationException);
            }
            else
            {
                assertNotNull(e.getLinkedException());
                assertEquals("The name 'foQueue' supplied in the address doesn't resolve to an exchange or a queue",
                             e.getLinkedException().getMessage());
            }
        }
    }


    public void testSendingToExchangePattern() throws Exception
    {
        final Topic topic = _session.createTopic(isBroker10() ? "barExchange/foo" : "ADDR: barExchange/foo ; { assert: never, node: { type: topic } }");
        final MessageProducer producer = _session.createProducer(topic);
        producer.send(_session.createTextMessage("Hello world!"));

        final MessageConsumer consumer = _session.createConsumer(topic);
        Message received = consumer.receive(getShortReceiveTimeout());
        assertNull(received);

        producer.send(_session.createTextMessage("Hello world2!"));
        received = consumer.receive(getReceiveTimeout());

        assertNotNull(received);

        assertTrue(received instanceof TextMessage);
        assertEquals("Hello world2!", ((TextMessage)received).getText());
    }


    public void testSendingToNonMatchingTopicPattern() throws Exception
    {
        final Topic topic = _session.createTopic(isBroker10() ? "baa" : "ADDR: baa ; { assert: never, node: { type: topic } }");
        try
        {
            final MessageProducer producer = _session.createProducer(topic);
            fail("Creating producer should fail");
        }
        catch(JMSException e)
        {
            if(isBroker10())
            {
                assertTrue(e instanceof InvalidDestinationException);
            }
            else
            {
                assertNotNull(e.getLinkedException());
                assertEquals("The name 'baa' supplied in the address doesn't resolve to an exchange or a queue",
                             e.getLinkedException().getMessage());
            }
        }
    }


    public void testSendingToQueuePatternBURL() throws Exception
    {
        final Queue queue = _session.createQueue("BURL:direct:///fooQ/fooQ");
        final MessageProducer producer = _session.createProducer(queue);
        producer.send(_session.createTextMessage("Hello world!"));

        final MessageConsumer consumer = _session.createConsumer(queue);
        Message received = consumer.receive(getReceiveTimeout());
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

    public void testQueueAlternateBindingCreation() throws Exception
    {
        Connection connection = getConnection();
        connection.start();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

        String queueName = getTestQueueName();
        String deadLetterQueueName = queueName + DEAD_LETTER_QUEUE_SUFFIX;

        final Map<String, Object> attributes = new HashMap<>();
        Map<String, Object> expectedAlternateBinding =
                Collections.singletonMap(AlternateBinding.DESTINATION, deadLetterQueueName);
        attributes.put(org.apache.qpid.server.model.Queue.ALTERNATE_BINDING,
                       new ObjectMapper().writeValueAsString(expectedAlternateBinding));
        createEntityUsingAmqpManagement(queueName,
                                        session,
                                        "org.apache.qpid.Queue", attributes);

        Map<String, Object> queueAttributes =
                managementReadObject(session, "org.apache.qpid.Queue", queueName, true);

        Object actualAlternateBinding = queueAttributes.get(org.apache.qpid.server.model.Queue.ALTERNATE_BINDING);
        Map<String, Object> actualAlternateBindingMap = convertIfNecessary(actualAlternateBinding);
        assertEquals("Unexpected alternate binding",
                     new HashMap<>(expectedAlternateBinding),
                     new HashMap<>(actualAlternateBindingMap));

        assertNotNull("Cannot get dead letter queue",
                      managementReadObject(session, "org.apache.qpid.Queue", deadLetterQueueName, true));
    }

    public void testExchangeAlternateBindingCreation() throws Exception
    {
        Connection connection = getConnection();
        connection.start();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

        String exchangeName = getTestQueueName();
        String deadLetterExchangeName = exchangeName + DEAD_LETTER_EXCHANGE_SUFFIX;

        final Map<String, Object> attributes = new HashMap<>();
        Map<String, Object> expectedAlternateBinding =
                Collections.singletonMap(AlternateBinding.DESTINATION, deadLetterExchangeName);
        attributes.put(Exchange.ALTERNATE_BINDING, new ObjectMapper().writeValueAsString(expectedAlternateBinding));
        attributes.put(Exchange.TYPE, ExchangeDefaults.DIRECT_EXCHANGE_CLASS);
        createEntityUsingAmqpManagement(exchangeName,
                                        session,
                                        "org.apache.qpid.DirectExchange", attributes);

        Map<String, Object> exchangeAttributes =
                managementReadObject(session, "org.apache.qpid.Exchange", exchangeName, true);

        Object actualAlternateBinding = exchangeAttributes.get(Exchange.ALTERNATE_BINDING);
        Map<String, Object> actualAlternateBindingMap = convertIfNecessary(actualAlternateBinding);
        assertEquals("Unexpected alternate binding",
                     new HashMap<>(expectedAlternateBinding),
                     new HashMap<>(actualAlternateBindingMap));

        assertNotNull("Cannot get dead letter exchange",
                      managementReadObject(session, "org.apache.qpid.FanoutExchange", deadLetterExchangeName, true));
    }

    public void testLegacyQueueDeclareArgumentAlternateBindingCreation() throws Exception
    {
        Connection connection = getConnection();
        connection.start();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

        final Map<String, Object> arguments = Collections.singletonMap(QueueArgumentsConverter.X_QPID_DLQ_ENABLED, true);
        String testQueueName = getTestQueueName();
        ((AMQSession<?,?>) session).createQueue(testQueueName, false, true, false, arguments);


        Map<String, Object> queueAttributes =
                managementReadObject(session, "org.apache.qpid.Queue", testQueueName, true);

        Object actualAlternateBinding = queueAttributes.get(Exchange.ALTERNATE_BINDING);
        assertTrue("Unexpected alternate binding", actualAlternateBinding instanceof Map);
        Object deadLetterQueueName = ((Map<String, Object>) actualAlternateBinding).get(AlternateBinding.DESTINATION);

        assertNotNull("Cannot get dead letter queue",
                      managementReadObject(session, "org.apache.qpid.Queue", String.valueOf(deadLetterQueueName), true));
    }

    private Map<String, Object> convertIfNecessary(final Object actualAlternateBinding) throws IOException
    {
        Map<String, Object> actualAlternateBindingMap;
        if (actualAlternateBinding instanceof String)
        {
            actualAlternateBindingMap = new ObjectMapper().readValue((String)actualAlternateBinding, Map.class);
        }
        else
        {
            actualAlternateBindingMap = (Map<String, Object>) actualAlternateBinding;
        }
        return actualAlternateBindingMap;
    }
}
