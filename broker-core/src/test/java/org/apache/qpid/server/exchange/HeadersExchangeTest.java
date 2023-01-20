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
package org.apache.qpid.server.exchange;

import static org.apache.qpid.server.filter.AMQPFilterTypes.JMS_SELECTOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.filter.AMQPFilterTypes;
import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.message.InstanceProperties;
import org.apache.qpid.server.message.RoutingResult;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.Binding;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.queue.BaseQueue;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.test.utils.UnitTestBase;

public class HeadersExchangeTest extends UnitTestBase
{
    private HeadersExchange<?> _exchange;
    private QueueManagingVirtualHost<?> _vhost;
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
                Exchange.TYPE, ExchangeDefaults.HEADERS_EXCHANGE_CLASS);

        _exchange = (HeadersExchange<?>) _vhost.createChild(Exchange.class, attributes);

        _instanceProperties = mock(InstanceProperties.class);
        _messageWithNoHeaders = createTestMessage(Map.of());
    }

    @AfterEach
    public void afterEach()
    {
        _exchange.close();
    }

    private void routeAndTest(ServerMessage<?> msg, Queue<?>... expected)
    {
        final RoutingResult<?> result = _exchange.route(msg, "", InstanceProperties.EMPTY);
        final Collection<BaseQueue> results = result.getRoutes();
        final List<BaseQueue> unexpected = new ArrayList<>(results);
        unexpected.removeAll(Arrays.asList(expected));
        assertTrue(unexpected.isEmpty(), "Message delivered to unexpected queues: " + unexpected);
        final List<BaseQueue> missing = new ArrayList<>(Arrays.asList(expected));
        missing.removeAll(results);
        assertTrue(missing.isEmpty(), "Message not delivered to expected queues: " + missing);
        assertEquals(results.size(), (new HashSet<>(results)).size(), "Duplicates " + results);
    }
    
    private Queue<?> createAndBind(final String name, final String... arguments)
            throws Exception
    {
        return createAndBind(name, getArgsMapFromStrings(arguments));
    }

    private Map<String, Object> getArgsMapFromStrings(final String... arguments)
    {
        final Map<String, Object> map = new HashMap<>();
        for (final String arg : arguments)
        {
            if (arg.contains("="))
            {
                final String[] keyValue = arg.split("=",2);
                map.put(keyValue[0],keyValue[1]);
            }
            else
            {
                map.put(arg,null);
            }
        }
        return map;
    }

    private Queue<?> createAndBind(final String name, final Map<String, Object> arguments)
            throws Exception
    {
        final Queue<?> q = _vhost.createChild(Queue.class, Map.of(Queue.NAME, name));
        _exchange.addBinding(name, q, arguments);
        return q;
    }

    @Test
    public void testSimple() throws Exception
    {
        final Queue<?> q1 = createAndBind("Q1", "F0000");
        final Queue<?> q2 = createAndBind("Q2", "F0000=Aardvark");
        final Queue<?> q3 = createAndBind("Q3", "F0001");
        final Queue<?> q4 = createAndBind("Q4", "F0001=Bear");
        final Queue<?> q5 = createAndBind("Q5", "F0000", "F0001");
        final Queue<?> q6 = createAndBind("Q6", "F0000=Aardvark", "F0001=Bear");
        final Queue<?> q7 = createAndBind("Q7", "F0000", "F0001=Bear");
        final Queue<?> q8 = createAndBind("Q8", "F0000=Aardvark", "F0001");

        routeAndTest(createTestMessage(getArgsMapFromStrings("F0000")), q1);
        routeAndTest(createTestMessage(getArgsMapFromStrings("F0000=Aardvark")), q1, q2);
        routeAndTest(createTestMessage(getArgsMapFromStrings("F0000=Aardvark", "F0001")), q1, q2, q3, q5, q8);
        routeAndTest(createTestMessage(getArgsMapFromStrings("F0000", "F0001=Bear")), q1, q3, q4, q5, q7);
        routeAndTest(createTestMessage(getArgsMapFromStrings("F0000=Aardvark", "F0001=Bear")),
                     q1, q2, q3, q4, q5, q6, q7, q8);
        routeAndTest(createTestMessage(getArgsMapFromStrings("F0002")));

        List.of(q1, q2, q3, q4, q5, q6, q7, q8).forEach(Queue::close);
    }

    @Test
    public void testAny() throws Exception
    {
        final Queue<?> q1 = createAndBind("Q1", "F0000", "F0001", "X-match=any");
        final Queue<?> q2 = createAndBind("Q2", "F0000=Aardvark", "F0001=Bear", "X-match=any");
        final Queue<?> q3 = createAndBind("Q3", "F0000", "F0001=Bear", "X-match=any");
        final Queue<?> q4 = createAndBind("Q4", "F0000=Aardvark", "F0001", "X-match=any");
        final Queue<?> q5 = createAndBind("Q5", "F0000=Apple", "F0001", "X-match=any");

        routeAndTest(createTestMessage(getArgsMapFromStrings("F0000")), q1, q3);
        routeAndTest(createTestMessage(getArgsMapFromStrings("F0000=Aardvark")), q1, q2, q3, q4);
        routeAndTest(createTestMessage(getArgsMapFromStrings("F0000=Aardvark", "F0001")), q1, q2, q3, q4, q5);
        routeAndTest(createTestMessage(getArgsMapFromStrings("F0000", "F0001=Bear")), q1, q2, q3, q4, q5);
        routeAndTest(createTestMessage(getArgsMapFromStrings("F0000=Aardvark", "F0001=Bear")), q1, q2, q3, q4, q5);
        routeAndTest(createTestMessage(getArgsMapFromStrings("F0002")));

        List.of(q1, q2, q3, q4, q5).forEach(Queue::close);
    }

    @Test
    public void testOnUnbind() throws Exception
    {
        final Queue<?> q1 = createAndBind("Q1", "F0000");
        final Queue<?> q2 = createAndBind("Q2", "F0000=Aardvark");
        final Queue<?> q3 = createAndBind("Q3", "F0001");

        routeAndTest(createTestMessage(getArgsMapFromStrings("F0000")), q1);
        routeAndTest(createTestMessage(getArgsMapFromStrings("F0000=Aardvark")), q1, q2);
        routeAndTest(createTestMessage(getArgsMapFromStrings("F0001")), q3);

        _exchange.deleteBinding("Q1",q1);

        routeAndTest(createTestMessage(getArgsMapFromStrings("F0000")));
        routeAndTest(createTestMessage(getArgsMapFromStrings("F0000=Aardvark")), q2);

        List.of(q1, q2, q3).forEach(Queue::close);
    }

    @Test
    public void testWithSelectors() throws Exception
    {
        final Queue<?> q1 = _vhost.createChild(Queue.class, Map.of(Queue.NAME, "Q1"));
        final Queue<?> q2 = _vhost.createChild(Queue.class, Map.of(Queue.NAME, "Q2"));
        _exchange.addBinding("q1", q1, getArgsMapFromStrings("F"));
        _exchange.addBinding("q1select", q1,
                             getArgsMapFromStrings("F", AMQPFilterTypes.JMS_SELECTOR + "=F='1'"));
        _exchange.addBinding("q2", q2, getArgsMapFromStrings("F=1"));

        routeAndTest(createTestMessage(getArgsMapFromStrings("F")),q1);

        routeAndTest(createTestMessage(getArgsMapFromStrings("F=1")), q1, q2);

        final Queue<?> q3 = _vhost.createChild(Queue.class, Map.of(Queue.NAME, "Q3"));
        _exchange.addBinding("q3select", q3,
                getArgsMapFromStrings("F", AMQPFilterTypes.JMS_SELECTOR + "=F='1'"));
        routeAndTest(createTestMessage(getArgsMapFromStrings("F=1")), q1, q2, q3);
        routeAndTest(createTestMessage(getArgsMapFromStrings("F=2")), q1);
        _exchange.addBinding("q3select2", q3,
                getArgsMapFromStrings("F", AMQPFilterTypes.JMS_SELECTOR + "=F='2'"));

        routeAndTest(createTestMessage(getArgsMapFromStrings("F=2")), q1, q3);

        List.of(q1, q2, q3).forEach(Queue::close);
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

        final RoutingResult<ServerMessage<?>> result = _exchange.route(_messageWithNoHeaders, bindingKey,
                                                                                       _instanceProperties);
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

        final Map<String, Object> arguments = getArgsMapFromStrings("prop=true", "prop2=true", "X-match=any");
        final boolean viaToQueueBind = via.bind(queue.getName(), replacementKey, arguments, false);
        assertTrue(viaToQueueBind, "Exchange to queue bind operation should be successful");

        final ServerMessage<?> testMessage = createTestMessage(Map.of("prop", true));
        final RoutingResult<ServerMessage<?>> result = _exchange.route(testMessage, bindingKey, _instanceProperties);
        assertTrue(result.hasRoutes(), "Message unexpectedly not routed to queue");
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

        final Map<String, Object> exchToViaBindArguments = Map.of(Binding.BINDING_ARGUMENT_REPLACEMENT_ROUTING_KEY, replacementKey,
                JMS_SELECTOR.toString(), "prop = True");

        final boolean exchToViaBind = _exchange.bind(via.getName(), bindingKey, exchToViaBindArguments, false);
        assertTrue(exchToViaBind, "Exchange to exchange bind operation should be successful");

        final boolean viaToQueueBind = via.bind(queue.getName(), replacementKey, Map.of(), false);
        assertTrue(viaToQueueBind, "Exchange to queue bind operation should be successful");

        RoutingResult<ServerMessage<?>> result =
                _exchange.route(createTestMessage(Map.of("prop", true)), bindingKey, _instanceProperties);
        assertTrue(result.hasRoutes(), "Message unexpectedly not routed to queue");

        result = _exchange.route(createTestMessage(Map.of("prop", false)), bindingKey, _instanceProperties);
        assertFalse(result.hasRoutes(), "Message unexpectedly routed to queue");
    }

    @Test
    public void testBindWithInvalidSelector()
    {
        final String queueName = getTestName() + "_queue";
        _vhost.createChild(Queue.class, Map.of(Queue.NAME, queueName));

        final Map<String, Object> bindArguments = new HashMap<>();
        bindArguments.put(JMS_SELECTOR.toString(), "foo in (");
        bindArguments.put("X-match", "any");
        bindArguments.put("foo", null);
        bindArguments.put("bar", null);

        assertThrows(IllegalArgumentException.class,
                () -> _exchange.bind(queueName, queueName, bindArguments, false),
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

        final Map<String, Object> bindArguments = new HashMap<>();
        bindArguments.put(JMS_SELECTOR.toString(), "foo in ('bar')");
        bindArguments.put("X-match", "any");
        bindArguments.put("foo", null);
        bindArguments.put("bar", null);

        final boolean isBound = _exchange.bind(queueName, queueName, bindArguments, false);
        assertTrue(isBound, "Could not bind queue");

        final ServerMessage<?> testMessage = createTestMessage(Map.of("foo", "bar"));
        final RoutingResult<ServerMessage<?>> result = _exchange.route(testMessage, queueName, _instanceProperties);
        assertTrue(result.hasRoutes(), "Message should be routed to queue");

        final Map<String, Object> bindArguments2 = new HashMap<>(bindArguments);
        bindArguments2.put(JMS_SELECTOR.toString(), "foo in (");
        assertThrows(IllegalArgumentException.class,
                () -> _exchange.bind(queueName, queueName, bindArguments2, true),
                "Queue can be bound when invalid selector expression is supplied as part of bind arguments");

        final RoutingResult<ServerMessage<?>> result2 = _exchange.route(testMessage, queueName, _instanceProperties);
        assertTrue(result2.hasRoutes(), "Message should be be possible to route using old binding");
    }

    @SuppressWarnings("unchecked")
    private ServerMessage<?> createTestMessage(final Map<String, Object> headerValues)
    {
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        headerValues.forEach((key, value) -> when(header.getHeader(key)).thenReturn(value));
        headerValues.forEach((key, value) -> when(header.containsHeader(key)).thenReturn(true));
        when(header.getHeaderNames()).thenReturn(headerValues.keySet());
        when(header.containsHeaders(any())).then(invocation ->
        {
            final Set<String> names = (Set<String>) invocation.getArguments()[0];
            return headerValues.keySet().containsAll(names);
        });

        final ServerMessage<?> message = mock(ServerMessage.class);
        when(message.isResourceAcceptable(any(TransactionLogResource.class))).thenReturn(true);
        when(message.getMessageHeader()).thenReturn(header);
        return message;
    }
}
