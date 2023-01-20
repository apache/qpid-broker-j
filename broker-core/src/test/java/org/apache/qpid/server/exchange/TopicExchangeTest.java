/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 *
 */
package org.apache.qpid.server.exchange;

import static org.apache.qpid.server.filter.AMQPFilterTypes.JMS_SELECTOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.binding.BindingImpl;
import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.message.InstanceProperties;
import org.apache.qpid.server.message.RoutingResult;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.Binding;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.test.utils.UnitTestBase;

public class TopicExchangeTest extends UnitTestBase
{
    private TopicExchange<?> _exchange;
    private VirtualHost<?> _vhost;
    private InstanceProperties _instanceProperties;
    private ServerMessage<?> _messageWithNoHeaders;

    @BeforeAll
    public void beforeAll() throws Exception
    {
        _vhost = BrokerTestHelper.createVirtualHost(getTestClassName(), this);
    }

    @BeforeEach
    public void setUp() throws Exception
    {
        final Map<String,Object> attributes = Map.of(Exchange.NAME, "test",
                Exchange.DURABLE, false,
                Exchange.TYPE, ExchangeDefaults.TOPIC_EXCHANGE_CLASS);
        _exchange = (TopicExchange<?>) _vhost.createChild(Exchange.class, attributes);
        _exchange.open();
        _instanceProperties = mock(InstanceProperties.class);
        _messageWithNoHeaders = createTestMessage(Map.of());
    }

    @AfterEach
    public void afterEach()
    {
        _exchange.close();
    }

    /* Thus the routing pattern *.stock.# matches the routing keys usd.stock and eur.stock.db but not stock.nasdaq. */
    @Test
    public void testNoRoute()
    {
        final Queue<?> queue = _vhost.createChild(Queue.class, Map.of(Queue.NAME, getTestName() + "_queue"));

        _exchange.bind(queue.getName(), "*.stock.#", null, false);

        final RoutingResult<ServerMessage<?>> result = _exchange.route(_messageWithNoHeaders, "stock.nasdaq",
                                                                                       _instanceProperties);
        assertFalse(result.hasRoutes(), "Message unexpected routed to queue after bind");
    }

    @Test
    public void testDirectMatch()
    {
        final Queue<?> queue = _vhost.createChild(Queue.class, Map.of(Queue.NAME, getTestName() + "_queue"));

        _exchange.bind(queue.getName(), "a.b", null, false);

        RoutingResult<ServerMessage<?>> result = _exchange.route(_messageWithNoHeaders, "a.b",
                _instanceProperties);

        assertEquals(1, (long) result.getNumberOfRoutes(), "Message unexpected routed to queue after bind");

        assertEquals(1, result.getNumberOfRoutes());

        result = _exchange.route(_messageWithNoHeaders, "a.c", _instanceProperties);
        assertEquals(0, result.getNumberOfRoutes());
    }

    /** * matches a single word */
    @Test
    public void testStarMatch()
    {
        final Queue<?> queue = _vhost.createChild(Queue.class, Map.of(Queue.NAME, getTestName() + "_queue"));

        _exchange.bind(queue.getName(), "a.*", null, false);

        RoutingResult<ServerMessage<?>> result = _exchange.route(_messageWithNoHeaders, "a.b",
                                                                                       _instanceProperties);
        assertEquals(1, result.getNumberOfRoutes());

        result = _exchange.route(_messageWithNoHeaders, "a.bb", _instanceProperties);
        assertEquals(1, result.getNumberOfRoutes());

        result = _exchange.route(_messageWithNoHeaders, "a.b.c", _instanceProperties);
        assertEquals(0, result.getNumberOfRoutes());

        result = _exchange.route(_messageWithNoHeaders, "a", _instanceProperties);
        assertEquals(0, result.getNumberOfRoutes());
    }

    /** # matches zero or more words */
    @Test
    public void testHashMatch()
    {
        final Queue<?> queue = _vhost.createChild(Queue.class, Map.of(Queue.NAME, getTestName() + "_queue"));
        _exchange.bind(queue.getName(), "a.#", null, false);

        RoutingResult<ServerMessage<?>> result = _exchange.route(_messageWithNoHeaders, "a.b",
                                                                                       _instanceProperties);
        assertEquals(1, result.getNumberOfRoutes());

        result = _exchange.route(_messageWithNoHeaders, "a.bb", _instanceProperties);
        assertEquals(1, result.getNumberOfRoutes());

        result = _exchange.route(_messageWithNoHeaders, "a.b.c", _instanceProperties);
        assertEquals(1, result.getNumberOfRoutes());

        result = _exchange.route(_messageWithNoHeaders, "a", _instanceProperties);
        assertEquals(1, result.getNumberOfRoutes());

        result = _exchange.route(_messageWithNoHeaders, "b", _instanceProperties);
        assertEquals(0, result.getNumberOfRoutes());
    }

    @Test
    public void testMidHash()
    {
        final Queue<?> queue = _vhost.createChild(Queue.class, Map.of(Queue.NAME, getTestName() + "_queue"));
        _exchange.bind(queue.getName(), "a.*.#.b", null, false);

        RoutingResult<ServerMessage<?>> result = _exchange.route(_messageWithNoHeaders, "a.c.d.b",
                                                                                       _instanceProperties);
        assertEquals(1, result.getNumberOfRoutes());

        result = _exchange.route(_messageWithNoHeaders, "a.c.d.d.b", _instanceProperties);
        assertEquals(1, result.getNumberOfRoutes());

        result = _exchange.route(_messageWithNoHeaders, "a.c.b", _instanceProperties);
        assertEquals(1, result.getNumberOfRoutes());
    }

    @Test
    public void testMatchAfterHash()
    {
        final Queue<?> queue = _vhost.createChild(Queue.class, Map.of(Queue.NAME, getTestName() + "_queue"));
        _exchange.bind(queue.getName(), "a.*.#.b.c", null, false);

        RoutingResult<ServerMessage<?>> result = _exchange.route(_messageWithNoHeaders, "a.c.b.b",
                                                                                       _instanceProperties);
        assertEquals(0, result.getNumberOfRoutes());

        result = _exchange.route(_messageWithNoHeaders, "a.a.b.c", _instanceProperties);
        assertEquals(1, result.getNumberOfRoutes());

        result = _exchange.route(_messageWithNoHeaders, "a.b.c.b", _instanceProperties);
        assertEquals(0, result.getNumberOfRoutes());

        result = _exchange.route(_messageWithNoHeaders, "a.b.c.b.c", _instanceProperties);
        assertEquals(1, result.getNumberOfRoutes());
    }
    
    @Test
    public void testHashAfterHash()
    {
        final Queue<?> queue = _vhost.createChild(Queue.class, Map.of(Queue.NAME, getTestName() + "_queue"));
        _exchange.bind(queue.getName(), "a.*.#.b.c.#.d", null, false);

        RoutingResult<ServerMessage<?>> result = _exchange.route(_messageWithNoHeaders, "a.c.b.b.c",
                                                                                       _instanceProperties);
        assertEquals(0, result.getNumberOfRoutes());

        result = _exchange.route(_messageWithNoHeaders, "a.a.b.c.d", _instanceProperties);
        assertEquals(1, result.getNumberOfRoutes());
    }

    @Test
    public void testHashHash()
    {
        final Queue<?> queue = _vhost.createChild(Queue.class, Map.of(Queue.NAME, getTestName() + "_queue"));
        _exchange.bind(queue.getName(), "a.#.*.#.d", null, false);

        RoutingResult<ServerMessage<?>> result = _exchange.route(_messageWithNoHeaders, "a.c.b.b.c",
                                                                                       _instanceProperties);
        assertEquals(0, result.getNumberOfRoutes());

        result = _exchange.route(_messageWithNoHeaders, "a.c.b.b.c", _instanceProperties);
        assertEquals(0, result.getNumberOfRoutes());

        result = _exchange.route(_messageWithNoHeaders, "a.a.b.c.d", _instanceProperties);
        assertEquals(1, result.getNumberOfRoutes());
    }

    @Test
    public void testSubMatchFails()
    {
        final Queue<?> queue = _vhost.createChild(Queue.class, Map.of(Queue.NAME, getTestName() + "_queue"));
        _exchange.bind(queue.getName(), "a.b.c.d", null, false);

        RoutingResult<ServerMessage<?>> result = _exchange.route(_messageWithNoHeaders, "a.b.c",
                                                                                       _instanceProperties);
        assertEquals(0, result.getNumberOfRoutes());
    }

    @Test
    public void testRouteToManyQueues()
    {
        final Queue<?> queue1 = _vhost.createChild(Queue.class, Map.of(Queue.NAME, getTestName() + "_queue1"));
        final Queue<?> queue2 = _vhost.createChild(Queue.class, Map.of(Queue.NAME, getTestName() + "_queue2"));
        _exchange.bind(queue1.getName(), "a.b", null, false);
        _exchange.bind(queue2.getName(), "a.*", null, false);

        RoutingResult<ServerMessage<?>> result = _exchange.route(_messageWithNoHeaders, "a.b",
                                                                                       _instanceProperties);
        assertEquals(2, result.getNumberOfRoutes());

        result = _exchange.route(_messageWithNoHeaders, "a.c", _instanceProperties);
        assertEquals(1, result.getNumberOfRoutes());

        _exchange.deleteBinding("a.b", queue1);

        result = _exchange.route(_messageWithNoHeaders, "a.b", _instanceProperties);
        assertEquals(1, result.getNumberOfRoutes());
    }

    @Test
    public void testRouteToQueueWithSelector()
    {
        final String bindingKey = "mybinding";
        final Queue<?> queue = _vhost.createChild(Queue.class, Map.of(Queue.NAME, getTestName() + "_queue"));
        final InstanceProperties instanceProperties = mock(InstanceProperties.class);
        final ServerMessage<?> matchingMessage = createTestMessage(Map.of("prop", true));
        final ServerMessage<?> unmatchingMessage = createTestMessage(Map.of("prop", false));
        final boolean bind = _exchange.bind(queue.getName(), bindingKey, Map.of(JMS_SELECTOR.toString(), "prop = True"),
                false);
        assertTrue(bind, "Bind operation should be successful");

        RoutingResult<ServerMessage<?>> result = _exchange.route(matchingMessage, "mybinding", instanceProperties);
        assertTrue(result.hasRoutes(), "Message with matching selector not routed to queue");

        result = _exchange.route(unmatchingMessage, "mybinding", instanceProperties);
        assertFalse(result.hasRoutes(), "Message without matching selector unexpectedly routed to queue");

        final boolean unbind = _exchange.unbind(queue.getName(), bindingKey);
        assertTrue(unbind, "Unbind operation should be successful");

        result = _exchange.route(matchingMessage, "mybinding", instanceProperties);
        assertFalse(result.hasRoutes(), "Message with matching selector unexpectedly routed to queue after unbind");
    }

    @Test
    public void testRouteToQueueViaTwoExchanges()
    {
        final String bindingKey = "key";
        final Map<String, Object> attributes = Map.of(Exchange.NAME, getTestName(),
                Exchange.TYPE, ExchangeDefaults.FANOUT_EXCHANGE_CLASS);
        final Exchange<?> via = _vhost.createChild(Exchange.class, attributes);
        final Queue<?> queue = _vhost.createChild(Queue.class, Map.of(Queue.NAME, getTestName() + "_queue"));
        final boolean exchToViaBind = _exchange.bind(via.getName(), bindingKey, Map.of(), false);
        assertTrue(exchToViaBind, "Exchange to exchange bind operation should be successful");

        final boolean viaToQueueBind = via.bind(queue.getName(), bindingKey, Map.of(), false);
        assertTrue(viaToQueueBind, "Exchange to queue bind operation should be successful");

        final RoutingResult<ServerMessage<?>> result = _exchange.route(_messageWithNoHeaders, bindingKey, _instanceProperties);
        assertTrue(result.hasRoutes(), "Message unexpectedly not routed to queue");
    }

    @Test
    public void testRouteToQueueViaTwoExchangesWithReplacementRoutingKey()
    {
        final Map<String, Object> attributes = Map.of(Exchange.NAME, getTestName(),
                Exchange.TYPE, ExchangeDefaults.DIRECT_EXCHANGE_CLASS);
        final Exchange<?> via = _vhost.createChild(Exchange.class, attributes);
        final Queue<?> queue = _vhost.createChild(Queue.class, Map.of(Queue.NAME, getTestName() + "_queue"));
        final String bindingKey = "key";
        final String replacementKey = "key1";
        final boolean exchToViaBind = _exchange.bind(via.getName(), bindingKey,
                Map.of(Binding.BINDING_ARGUMENT_REPLACEMENT_ROUTING_KEY, replacementKey), false);
        assertTrue(exchToViaBind, "Exchange to exchange bind operation should be successful");

        final boolean viaToQueueBind = via.bind(queue.getName(), replacementKey, Map.of(), false);
        assertTrue(viaToQueueBind, "Exchange to queue bind operation should be successful");

        RoutingResult<ServerMessage<?>> result = _exchange.route(_messageWithNoHeaders, bindingKey, _instanceProperties);
        assertTrue(result.hasRoutes(), "Message unexpectedly not routed to queue");

        result = _exchange.route(_messageWithNoHeaders, replacementKey, _instanceProperties);
        assertFalse(result.hasRoutes(), "Message unexpectedly was routed to queue");
    }

    @Test
    public void testRouteToQueueViaTwoExchangesWithReplacementRoutingKeyAndFiltering()
    {
        final String bindingKey = "key1";
        final String replacementKey = "key2";
        final Map<String, Object> viaExchangeArguments = Map.of(Exchange.NAME, getTestName() + "_via_exch",
                Exchange.TYPE, ExchangeDefaults.TOPIC_EXCHANGE_CLASS);
        final Exchange<?> via = _vhost.createChild(Exchange.class, viaExchangeArguments);
        final Queue<?> queue = _vhost.createChild(Queue.class, Map.of(Queue.NAME, getTestName() + "_queue"));
        final Map<String, Object> exchToViaBindArguments = Map.of(
                Binding.BINDING_ARGUMENT_REPLACEMENT_ROUTING_KEY, replacementKey,
                JMS_SELECTOR.toString(), "prop = True");
        final boolean exchToViaBind = _exchange.bind(via.getName(), bindingKey, exchToViaBindArguments, false);
        assertTrue(exchToViaBind, "Exchange to exchange bind operation should be successful");

        final boolean viaToQueueBind = via.bind(queue.getName(), replacementKey, Map.of(), false);
        assertTrue(viaToQueueBind, "Exchange to queue bind operation should be successful");

        RoutingResult<ServerMessage<?>> result = _exchange.route(createTestMessage(Map.of("prop", true)),
                bindingKey, _instanceProperties);
        assertTrue(result.hasRoutes(), "Message unexpectedly not routed to queue");

        result = _exchange.route(createTestMessage(Map.of("prop", false)), bindingKey, _instanceProperties);
        assertFalse(result.hasRoutes(), "Message unexpectedly routed to queue");
    }
    
    @Test
    public void testHierachicalRouteToQueueViaTwoExchangesWithReplacementRoutingKey()
    {
        final Map<String, Object> attributes = Map.of(Exchange.NAME, getTestName(),
                Exchange.TYPE, ExchangeDefaults.DIRECT_EXCHANGE_CLASS);
        final Exchange<?> via = _vhost.createChild(Exchange.class, attributes);
        final Queue<?> queue1 = _vhost.createChild(Queue.class, Map.of(Queue.NAME, getTestName() + "_queue1"));
        final Queue<?> queue2 = _vhost.createChild(Queue.class, Map.of(Queue.NAME, getTestName() + "_queue2"));
        final String bindingKey1 = "a.#";
        final String bindingKey2 = "a.*";
        final String replacementKey1 = "key1";
        final String replacementKey2 = "key2";

        assertTrue(_exchange.bind(via.getName(), bindingKey1,
                Map.of(Binding.BINDING_ARGUMENT_REPLACEMENT_ROUTING_KEY, replacementKey1), false),
                "Exchange to exchange bind operation should be successful");

        assertTrue(_exchange.bind(via.getName(), bindingKey2,
                Map.of(Binding.BINDING_ARGUMENT_REPLACEMENT_ROUTING_KEY, replacementKey2), false),
                "Exchange to exchange bind operation should be successful");

        assertTrue(via.bind(queue1.getName(), replacementKey1, Map.of(), false),
                   "Exchange to queue1 bind operation should be successful");

        assertTrue(via.bind(queue2.getName(), replacementKey2, Map.of(), false),
                   "Exchange to queue2 bind operation should be successful");

        RoutingResult<ServerMessage<?>> result = _exchange.route(_messageWithNoHeaders, "a.b",
                                                                                       _instanceProperties);
        assertEquals(2, (long) result.getNumberOfRoutes(), "Unexpected number of routes");

        result = _exchange.route(_messageWithNoHeaders, "a.b.c", _instanceProperties);
        assertEquals(1, (long) result.getNumberOfRoutes(), "Unexpected number of routes");

        assertTrue(result.getRoutes().contains(queue1), "Message is not routed into 'queue1'");
    }

    @Test
    public void testUpdateBindingReplacingSelector() throws Exception
    {
        final String bindingKey = "mybinding";
        final Queue<?> queue = _vhost.createChild(Queue.class, Map.of(Queue.NAME, getTestName() + "_queue"));
        final InstanceProperties instanceProperties = mock(InstanceProperties.class);
        ServerMessage<?> matchingMessage = createTestMessage(Map.of("prop", true));
        final boolean bind = _exchange.bind(queue.getName(), bindingKey, Map.of(JMS_SELECTOR.toString(), "prop = True"),
                false);
        assertTrue(bind, "Bind operation should be successful");

        RoutingResult<ServerMessage<?>> result = _exchange.route(matchingMessage, bindingKey, instanceProperties);
        assertTrue(result.hasRoutes(), "Message with matching selector not routed to queue");

        _exchange.replaceBinding(bindingKey, queue, Map.of(JMS_SELECTOR.toString(), "prop = False"));

        result = _exchange.route(matchingMessage, bindingKey, instanceProperties);
        assertFalse(result.hasRoutes(), "Message unexpectedly routed to queue after rebind");

        result = _exchange.route(matchingMessage, bindingKey, instanceProperties);
        assertFalse(result.hasRoutes());

        matchingMessage = createTestMessage(Map.of("prop", false));
        result = _exchange.route(matchingMessage, bindingKey, instanceProperties);
        assertTrue(result.hasRoutes(), "Message not routed to queue");
    }

    @Test
    public void testUpdateBindingRemovingSelector() throws Exception
    {
        final String bindingKey = "mybinding";
        final Queue<?> queue = _vhost.createChild(Queue.class, Map.of(Queue.NAME, getTestName() + "_queue"));
        final InstanceProperties instanceProperties = mock(InstanceProperties.class);
        final ServerMessage<?> message = createTestMessage(Map.of("prop", false));
        final boolean bind = _exchange.bind(queue.getName(), bindingKey, Map.of(JMS_SELECTOR.toString(), "prop = True"),
                false);
        assertTrue(bind, "Bind operation should be successful");

        RoutingResult<ServerMessage<?>> result = _exchange.route(message, bindingKey, instanceProperties);
        assertFalse(result.hasRoutes(), "Message that does not match selector routed to queue");

        _exchange.replaceBinding(bindingKey, queue, Map.of());

        result = _exchange.route(message, bindingKey, instanceProperties);
        assertTrue(result.hasRoutes(), "Message not routed to queue after rebind");
    }

    @Test
    public void testUpdateBindingAddingSelector() throws Exception
    {
        final String bindingKey = "mybinding";
        final Queue<?> queue = _vhost.createChild(Queue.class, Map.of(Queue.NAME, getTestName() + "_queue"));
        final InstanceProperties instanceProperties = mock(InstanceProperties.class);
        final ServerMessage<?> message = createTestMessage(Map.of("prop", false));
        final boolean bind = _exchange.bind(queue.getName(), bindingKey, Map.of(), false);
        assertTrue(bind, "Bind operation should be successful");

        RoutingResult<ServerMessage<?>> result = _exchange.route(message, bindingKey, instanceProperties);
        assertTrue(result.hasRoutes(), "Message not routed to queue");

        _exchange.replaceBinding(bindingKey, queue, Map.of(JMS_SELECTOR.toString(), "prop = false"));

        result = _exchange.route(message, bindingKey, instanceProperties);
        assertTrue(result.hasRoutes(), "Message that matches selector not routed to queue after rebind");

        result = _exchange.route(createTestMessage(Map.of("prop", true)), bindingKey, instanceProperties);
        assertFalse(result.hasRoutes(), "Message that does not match selector routed to queue after rebind");
    }

    @Test
    public void testUpdateBindingChangeReplacementKey()
    {
        final String bindingKey = "mybinding";
        final String replacementKey = "key1";
        final String replacementKey2 = "key2";
        final Map<String, Object> attributes = Map.of(Exchange.NAME, getTestName(),
                Exchange.TYPE, ExchangeDefaults.DIRECT_EXCHANGE_CLASS);
        final Exchange<?> via = _vhost.createChild(Exchange.class, attributes);
        final Queue<?> queue = _vhost.createChild(Queue.class, Map.of(Queue.NAME, getTestName() + "_queue"));
        final boolean exchToViaBind = _exchange.bind(via.getName(), bindingKey, Map.of(), false);
        assertTrue(exchToViaBind, "Exchange to exchange bind operation should be successful");

        final boolean viaToQueueBind = via.bind(queue.getName(), replacementKey, Map.of(), false);
        assertTrue(viaToQueueBind, "Exchange to queue bind operation should be successful");

        RoutingResult<ServerMessage<?>> result = _exchange.route(_messageWithNoHeaders, bindingKey, _instanceProperties);
        assertFalse(result.hasRoutes(), "Message unexpectedly routed to queue");

        _exchange.bind(via.getName(), bindingKey, Map.of(Binding.BINDING_ARGUMENT_REPLACEMENT_ROUTING_KEY, replacementKey),
                true);

        result = _exchange.route(_messageWithNoHeaders, bindingKey, _instanceProperties);
        assertTrue(result.hasRoutes(), "Message was not routed");
        assertTrue(result.getRoutes().contains(queue), "Message was not routed to queue");

        Queue<?> queue2 = _vhost.createChild(Queue.class, Map.of(Queue.NAME, getTestName() + "_queue2"));
        assertTrue(via.bind(queue2.getName(), replacementKey2, Map.of(), false),
                   "Binding of queue2 failed");

        _exchange.bind(via.getName(), bindingKey, Map.of(Binding.BINDING_ARGUMENT_REPLACEMENT_ROUTING_KEY, replacementKey2),
                true);

        result = _exchange.route(_messageWithNoHeaders, bindingKey, _instanceProperties);
        assertTrue(result.hasRoutes(), "Message was not routed");
        assertTrue(result.getRoutes().contains(queue2), "Message was not routed to queue2");
    }

    @Test
    public void testBindWithInvalidSelector()
    {
        final String queueName = getTestName() + "_queue";
        _vhost.createChild(Queue.class, Map.of(Queue.NAME, queueName));

        final Map<String, Object> bindArguments = Map.of(JMS_SELECTOR.toString(), "foo in (");
        assertThrows(IllegalArgumentException.class,
                () -> _exchange.bind(queueName, "#", bindArguments, false),
                "Queue can be bound when invalid selector expression is supplied as part of bind arguments");
        final ServerMessage<?> testMessage = createTestMessage(Map.of("foo", "bar"));
        final RoutingResult<ServerMessage<?>> result = _exchange.route(testMessage, queueName, _instanceProperties);

        assertFalse(result.hasRoutes(), "Message is unexpectedly routed to queue");
    }

    @Test
    public void testBindWithInvalidSelectorWhenBindingExists()
    {
        final String queueName = getTestName() + "_queue";
        _vhost.createChild(Queue.class, Map.of(Queue.NAME, queueName));

        final Map<String, Object> bindArguments = Map.of(JMS_SELECTOR.toString(), "foo in ('bar')");
        final boolean isBound = _exchange.bind(queueName, "#", bindArguments, false);
        assertTrue(isBound, "Could not bind queue");

        final ServerMessage<?> testMessage = createTestMessage(Map.of("foo", "bar"));
        final RoutingResult<ServerMessage<?>> result = _exchange.route(testMessage, queueName, _instanceProperties);
        assertTrue(result.hasRoutes(), "Message should be routed to queue");

        final Map<String, Object> bindArguments2 = Map.of(JMS_SELECTOR.toString(), "foo in (");
        assertThrows(IllegalArgumentException.class,
                () -> _exchange.bind(queueName, "#", bindArguments2, true),
                "Queue can be bound when invalid selector expression is supplied as part of bind arguments");

        final RoutingResult<ServerMessage<?>> result2 = _exchange.route(testMessage, queueName, _instanceProperties);
        assertTrue(result2.hasRoutes(), "Message should be be possible to route using old binding");
    }

    @Test
    public void testBindingWithSameDestinationName()
    {
        final String name = "test123";
        final Map<String, Object> queueAttributes = Map.of(Queue.NAME, name,
                Queue.DURABLE, false);
        final Queue<?> queue = (Queue<?>) _vhost.createChild(Queue.class, queueAttributes);
        final Map<String, Object> exchangeAttributes = Map.of(Exchange.NAME, name,
                Exchange.DURABLE, false,
                Exchange.TYPE, ExchangeDefaults.TOPIC_EXCHANGE_CLASS,
                Exchange.DURABLE_BINDINGS, List.of(new BindingImpl("#", name, Map.of())));
        final Exchange<?> exchange = (Exchange<?>) _vhost.createChild(Exchange.class, exchangeAttributes);

        assertEquals(1, queue.getBindingCount());
        assertEquals(1, exchange.getBindingCount());
    }

    private ServerMessage<?> createTestMessage(final Map<String, Object> headerValues)
    {
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        headerValues.forEach((key, value) -> when(header.getHeader(key)).thenReturn(value));

        final ServerMessage<?> message = mock(ServerMessage.class);
        when(message.isResourceAcceptable(any(TransactionLogResource.class))).thenReturn(true);
        when(message.getMessageHeader()).thenReturn(header);
        return message;
    }
}
