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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
    private QueueManagingVirtualHost<?> _virtualHost;
    private InstanceProperties _instanceProperties;
    private ServerMessage<?> _messageWithNoHeaders;

    @Before
    public void setUp() throws Exception
    {

        _virtualHost = BrokerTestHelper.createVirtualHost("test", this);

        Map<String,Object> attributes = new HashMap<>();
        attributes.put(Exchange.NAME, "test");
        attributes.put(Exchange.DURABLE, false);
        attributes.put(Exchange.TYPE, ExchangeDefaults.HEADERS_EXCHANGE_CLASS);


        _exchange = (HeadersExchange) _virtualHost.createChild(Exchange.class, attributes);

        _instanceProperties = mock(InstanceProperties.class);
        _messageWithNoHeaders = createTestMessage(Collections.emptyMap());

    }

    @After
    public void tearDown() throws Exception
    {
        if (_virtualHost  != null)
        {
            _virtualHost.close();
        }
    }

    private void routeAndTest(ServerMessage msg, Queue<?>... expected) throws Exception
    {
        RoutingResult<?> result = _exchange.route(msg, "", InstanceProperties.EMPTY);
        Collection<BaseQueue> results = result.getRoutes();
        List<BaseQueue> unexpected = new ArrayList<>(results);
        unexpected.removeAll(Arrays.asList(expected));
        assertTrue("Message delivered to unexpected queues: " + unexpected, unexpected.isEmpty());
        List<BaseQueue> missing = new ArrayList<>(Arrays.asList(expected));
        missing.removeAll(results);
        assertTrue("Message not delivered to expected queues: " + missing, missing.isEmpty());
        assertTrue("Duplicates " + results, results.size() == (new HashSet<>(results)).size());
    }


    private Queue<?> createAndBind(final String name, String... arguments)
            throws Exception
    {
        return createAndBind(name, getArgsMapFromStrings(arguments));
    }

    private Map<String, Object> getArgsMapFromStrings(String... arguments)
    {
        Map<String, Object> map = new HashMap<>();

        for(String arg : arguments)
        {
            if(arg.contains("="))
            {
                String[] keyValue = arg.split("=",2);
                map.put(keyValue[0],keyValue[1]);
            }
            else
            {
                map.put(arg,null);
            }
        }
        return map;
    }

    private Queue<?> createAndBind(final String name, Map<String, Object> arguments)
            throws Exception
    {
        Queue<?> q = _virtualHost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, name));
        _exchange.addBinding(name, q, arguments);
        return q;
    }


    @Test
    public void testSimple() throws Exception
    {
        Queue<?> q1 = createAndBind("Q1", "F0000");
        Queue<?> q2 = createAndBind("Q2", "F0000=Aardvark");
        Queue<?> q3 = createAndBind("Q3", "F0001");
        Queue<?> q4 = createAndBind("Q4", "F0001=Bear");
        Queue<?> q5 = createAndBind("Q5", "F0000", "F0001");
        Queue<?> q6 = createAndBind("Q6", "F0000=Aardvark", "F0001=Bear");
        Queue<?> q7 = createAndBind("Q7", "F0000", "F0001=Bear");
        Queue<?> q8 = createAndBind("Q8", "F0000=Aardvark", "F0001");

        routeAndTest(createTestMessage(getArgsMapFromStrings("F0000")), q1);
        routeAndTest(createTestMessage(getArgsMapFromStrings("F0000=Aardvark")), q1, q2);
        routeAndTest(createTestMessage(getArgsMapFromStrings("F0000=Aardvark", "F0001")), q1, q2, q3, q5, q8);
        routeAndTest(createTestMessage(getArgsMapFromStrings("F0000", "F0001=Bear")), q1, q3, q4, q5, q7);
        routeAndTest(createTestMessage(getArgsMapFromStrings("F0000=Aardvark", "F0001=Bear")),
                     q1, q2, q3, q4, q5, q6, q7, q8);
        routeAndTest(createTestMessage(getArgsMapFromStrings("F0002")));

    }

    @Test
    public void testAny() throws Exception
    {
        Queue<?> q1 = createAndBind("Q1", "F0000", "F0001", "X-match=any");
        Queue<?> q2 = createAndBind("Q2", "F0000=Aardvark", "F0001=Bear", "X-match=any");
        Queue<?> q3 = createAndBind("Q3", "F0000", "F0001=Bear", "X-match=any");
        Queue<?> q4 = createAndBind("Q4", "F0000=Aardvark", "F0001", "X-match=any");
        Queue<?> q5 = createAndBind("Q5", "F0000=Apple", "F0001", "X-match=any");

        routeAndTest(createTestMessage(getArgsMapFromStrings("F0000")), q1, q3);
        routeAndTest(createTestMessage(getArgsMapFromStrings("F0000=Aardvark")), q1, q2, q3, q4);
        routeAndTest(createTestMessage(getArgsMapFromStrings("F0000=Aardvark", "F0001")), q1, q2, q3, q4, q5);
        routeAndTest(createTestMessage(getArgsMapFromStrings("F0000", "F0001=Bear")), q1, q2, q3, q4, q5);
        routeAndTest(createTestMessage(getArgsMapFromStrings("F0000=Aardvark", "F0001=Bear")), q1, q2, q3, q4, q5);
        routeAndTest(createTestMessage(getArgsMapFromStrings("F0002")));
    }

    @Test
    public void testOnUnbind() throws Exception
    {
        Queue<?> q1 = createAndBind("Q1", "F0000");
        Queue<?> q2 = createAndBind("Q2", "F0000=Aardvark");
        Queue<?> q3 = createAndBind("Q3", "F0001");

        routeAndTest(createTestMessage(getArgsMapFromStrings("F0000")), q1);
        routeAndTest(createTestMessage(getArgsMapFromStrings("F0000=Aardvark")), q1, q2);
        routeAndTest(createTestMessage(getArgsMapFromStrings("F0001")), q3);

        _exchange.deleteBinding("Q1",q1);

        routeAndTest(createTestMessage(getArgsMapFromStrings("F0000")));
        routeAndTest(createTestMessage(getArgsMapFromStrings("F0000=Aardvark")), q2);
    }


    @Test
    public void testWithSelectors() throws Exception
    {
        Queue<?> q1 = _virtualHost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, "Q1"));
        Queue<?> q2 = _virtualHost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, "Q2"));
        _exchange.addBinding("q1", q1, getArgsMapFromStrings("F"));
        _exchange.addBinding("q1select",
                             q1,
                             getArgsMapFromStrings("F", AMQPFilterTypes.JMS_SELECTOR.toString() + "=F='1'"));
        _exchange.addBinding("q2", q2, getArgsMapFromStrings("F=1"));

        routeAndTest(createTestMessage(getArgsMapFromStrings("F")),q1);

        routeAndTest(createTestMessage(getArgsMapFromStrings("F=1")), q1, q2);

        Queue<?> q3 = _virtualHost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, "Q3"));
        _exchange.addBinding("q3select",
                             q3,
                             getArgsMapFromStrings("F", AMQPFilterTypes.JMS_SELECTOR.toString() + "=F='1'"));
        routeAndTest(createTestMessage(getArgsMapFromStrings("F=1")), q1, q2, q3);
        routeAndTest(createTestMessage(getArgsMapFromStrings("F=2")), q1);
        _exchange.addBinding("q3select2",
                             q3,
                             getArgsMapFromStrings("F", AMQPFilterTypes.JMS_SELECTOR.toString() + "=F='2'"));

        routeAndTest(createTestMessage(getArgsMapFromStrings("F=2")), q1, q3);

    }

    @Test
    public void testRouteToQueueViaTwoExchanges()
    {
        String bindingKey = "key";

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(Exchange.NAME, getTestName());
        attributes.put(Exchange.TYPE, ExchangeDefaults.FANOUT_EXCHANGE_CLASS);

        Exchange via = _virtualHost.createChild(Exchange.class, attributes);
        Queue<?> queue = _virtualHost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, getTestName() + "_queue"));

        boolean exchToViaBind = _exchange.bind(via.getName(), bindingKey, Collections.emptyMap(), false);
        assertTrue("Exchange to exchange bind operation should be successful", exchToViaBind);

        boolean viaToQueueBind = via.bind(queue.getName(), bindingKey, Collections.emptyMap(), false);
        assertTrue("Exchange to queue bind operation should be successful", viaToQueueBind);

        RoutingResult<ServerMessage<?>> result = _exchange.route(_messageWithNoHeaders,
                                                                                       bindingKey,
                                                                                       _instanceProperties);
        assertTrue("Message unexpectedly not routed to queue", result.hasRoutes());
    }

    @Test
    public void testRouteToQueueViaTwoExchangesWithReplacementRoutingKey()
    {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(Exchange.NAME, getTestName());
        attributes.put(Exchange.TYPE, ExchangeDefaults.DIRECT_EXCHANGE_CLASS);

        Exchange via = _virtualHost.createChild(Exchange.class, attributes);
        Queue<?> queue = _virtualHost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, getTestName() + "_queue"));

        String bindingKey = "key";
        String replacementKey = "key1";
        boolean exchToViaBind = _exchange.bind(via.getName(),
                                               bindingKey,
                                               Collections.singletonMap(Binding.BINDING_ARGUMENT_REPLACEMENT_ROUTING_KEY,
                                                                        replacementKey),
                                               false);
        assertTrue("Exchange to exchange bind operation should be successful", exchToViaBind);

        Map<String, Object> arguments = getArgsMapFromStrings("prop=true", "prop2=true", "X-match=any");
        boolean viaToQueueBind = via.bind(queue.getName(), replacementKey, arguments, false);
        assertTrue("Exchange to queue bind operation should be successful", viaToQueueBind);

        ServerMessage<?> testMessage = createTestMessage(Collections.singletonMap("prop", true));
        RoutingResult<ServerMessage<?>> result = _exchange.route(testMessage,
                                                                                       bindingKey,
                                                                                       _instanceProperties);
        assertTrue("Message unexpectedly not routed to queue", result.hasRoutes());
    }

    @Test
    public void testRouteToQueueViaTwoExchangesWithReplacementRoutingKeyAndFiltering()
    {
        String bindingKey = "key1";
        String replacementKey = "key2";

        Map<String, Object> viaExchangeArguments = new HashMap<>();
        viaExchangeArguments.put(Exchange.NAME, getTestName() + "_via_exch");
        viaExchangeArguments.put(Exchange.TYPE, ExchangeDefaults.TOPIC_EXCHANGE_CLASS);

        Exchange via = _virtualHost.createChild(Exchange.class, viaExchangeArguments);
        Queue<?> queue = _virtualHost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, getTestName() + "_queue"));


        Map<String, Object> exchToViaBindArguments = new HashMap<>();
        exchToViaBindArguments.put(Binding.BINDING_ARGUMENT_REPLACEMENT_ROUTING_KEY, replacementKey);
        exchToViaBindArguments.put(JMS_SELECTOR.toString(), "prop = True");

        boolean exchToViaBind = _exchange.bind(via.getName(),
                                               bindingKey,
                                               exchToViaBindArguments,
                                               false);
        assertTrue("Exchange to exchange bind operation should be successful", exchToViaBind);

        boolean viaToQueueBind = via.bind(queue.getName(), replacementKey, Collections.emptyMap(), false);
        assertTrue("Exchange to queue bind operation should be successful", viaToQueueBind);

        RoutingResult<ServerMessage<?>> result =
                _exchange.route(createTestMessage(Collections.singletonMap("prop", true)),
                                bindingKey,
                                _instanceProperties);
        assertTrue("Message unexpectedly not routed to queue", result.hasRoutes());

        result = _exchange.route(createTestMessage(Collections.singletonMap("prop", false)),
                                 bindingKey,
                                 _instanceProperties);
        assertFalse("Message unexpectedly routed to queue", result.hasRoutes());
    }

    @Test
    public void testBindWithInvalidSelector()
    {
        final String queueName = getTestName() + "_queue";
        _virtualHost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, queueName));

        final Map<String, Object> bindArguments = new HashMap<>();
        bindArguments.put(JMS_SELECTOR.toString(), "foo in (");
        bindArguments.put("X-match", "any");
        bindArguments.put("foo", null);
        bindArguments.put("bar", null);

        try
        {
            _exchange.bind(queueName, queueName, bindArguments, false);
            fail("Queue can be bound when invalid selector expression is supplied as part of bind arguments");
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }

        final ServerMessage<?> testMessage = createTestMessage(Collections.singletonMap("foo", "bar"));
        final RoutingResult<ServerMessage<?>> result = _exchange.route(testMessage, queueName, _instanceProperties);

        assertFalse("Message is unexpectedly routed to queue", result.hasRoutes());
    }

    @Test
    public void testBindWithInvalidSelectorWhenBindingExists()
    {
        final String queueName = getTestName() + "_queue";
        _virtualHost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, queueName));

        final Map<String, Object> bindArguments = new HashMap<>();
        bindArguments.put(JMS_SELECTOR.toString(), "foo in ('bar')");
        bindArguments.put("X-match", "any");
        bindArguments.put("foo", null);
        bindArguments.put("bar", null);

        final boolean isBound = _exchange.bind(queueName, queueName, bindArguments, false);
        assertTrue("Could not bind queue", isBound);

        final ServerMessage<?> testMessage = createTestMessage(Collections.singletonMap("foo", "bar"));
        final RoutingResult<ServerMessage<?>> result = _exchange.route(testMessage, queueName, _instanceProperties);
        assertTrue("Message should be routed to queue", result.hasRoutes());

        final Map<String, Object> bindArguments2 = new HashMap<>(bindArguments);
        bindArguments2.put(JMS_SELECTOR.toString(), "foo in (");
        try
        {
            _exchange.bind(queueName, queueName, bindArguments2, true);
            fail("Queue can be bound when invalid selector expression is supplied as part of bind arguments");
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }

        final RoutingResult<ServerMessage<?>> result2 = _exchange.route(testMessage, queueName, _instanceProperties);
        assertTrue("Message should be be possible to route using old binding", result2.hasRoutes());
    }

    private ServerMessage<?> createTestMessage(Map<String, Object> headerValues)
    {
        AMQMessageHeader header = mock(AMQMessageHeader.class);
        headerValues.forEach((key, value) -> when(header.getHeader(key)).thenReturn(value));
        headerValues.forEach((key, value) -> when(header.containsHeader(key)).thenReturn(true));
        when(header.getHeaderNames()).thenReturn(headerValues.keySet());
        when(header.containsHeaders(any())).then(invocation ->
                                                 {
                                                     final Set<String> names =
                                                             (Set<String>) invocation.getArguments()[0];
                                                     return headerValues.keySet().containsAll(names);
                                                 });

        @SuppressWarnings("unchecked")
        ServerMessage<?> message = mock(ServerMessage.class);
        when(message.isResourceAcceptable(any(TransactionLogResource.class))).thenReturn(true);
        when(message.getMessageHeader()).thenReturn(header);
        return message;
    }

}
