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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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

    @Before
    public void setUp() throws Exception
    {
        BrokerTestHelper.setUp();
        _vhost = BrokerTestHelper.createVirtualHost(getTestName(), this);

        Map<String,Object> attributes = new HashMap<>();
        attributes.put(Exchange.NAME, "test");
        attributes.put(Exchange.DURABLE, false);
        attributes.put(Exchange.TYPE, ExchangeDefaults.TOPIC_EXCHANGE_CLASS);

        _exchange = (TopicExchange) _vhost.createChild(Exchange.class, attributes);
        _exchange.open();

        _instanceProperties = mock(InstanceProperties.class);
        _messageWithNoHeaders = createTestMessage(Collections.emptyMap());

    }

    @After
    public void tearDown() throws Exception
    {
        try
        {
            if (_vhost != null)
            {
                _vhost.close();
            }
        }
        finally
        {
            BrokerTestHelper.tearDown();
        }
    }

    /* Thus the routing pattern *.stock.# matches the routing keys usd.stock and eur.stock.db but not stock.nasdaq. */
    @Test
    public void testNoRoute() throws Exception
    {
        Queue<?> queue = _vhost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, getTestName() + "_queue"));

        _exchange.bind(queue.getName(), "*.stock.#", null, false);

        RoutingResult<ServerMessage<?>> result = _exchange.route(_messageWithNoHeaders,
                                                                                       "stock.nasdaq",
                                                                                       _instanceProperties);
        assertFalse("Message unexpected routed to queue after bind", result.hasRoutes());
    }

    @Test
    public void testDirectMatch() throws Exception
    {
        Queue<?> queue = _vhost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, getTestName() + "_queue"));

        _exchange.bind(queue.getName(), "a.b", null, false);

        RoutingResult<ServerMessage<?>> result = _exchange.route(_messageWithNoHeaders,
                                                                                       "a.b",
                                                                                       _instanceProperties);

        assertEquals("Message unexpected routed to queue after bind",
                            (long) 1,
                            (long) result.getNumberOfRoutes());

        assertEquals(1, result.getNumberOfRoutes());

        result = _exchange.route(_messageWithNoHeaders, "a.c", _instanceProperties);
        assertEquals(0, result.getNumberOfRoutes());
    }

    /** * matches a single word */
    @Test
    public void testStarMatch() throws Exception
    {
        Queue<?> queue = _vhost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, getTestName() + "_queue"));

        _exchange.bind(queue.getName(), "a.*", null, false);

        RoutingResult<ServerMessage<?>> result = _exchange.route(_messageWithNoHeaders,
                                                                                       "a.b",
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
    public void testHashMatch() throws Exception
    {
        Queue<?> queue = _vhost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, getTestName() + "_queue"));
        _exchange.bind(queue.getName(), "a.#", null, false);

        RoutingResult<ServerMessage<?>> result = _exchange.route(_messageWithNoHeaders,
                                                                                       "a.b",
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
    public void testMidHash() throws Exception
    {
        Queue<?> queue = _vhost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, getTestName() + "_queue"));
        _exchange.bind(queue.getName(), "a.*.#.b", null, false);

        RoutingResult<ServerMessage<?>> result = _exchange.route(_messageWithNoHeaders,
                                                                                       "a.c.d.b",
                                                                                       _instanceProperties);
        assertEquals(1, result.getNumberOfRoutes());

        result = _exchange.route(_messageWithNoHeaders, "a.c.d.d.b", _instanceProperties);
        assertEquals(1, result.getNumberOfRoutes());

        result = _exchange.route(_messageWithNoHeaders, "a.c.b", _instanceProperties);
        assertEquals(1, result.getNumberOfRoutes());
    }

    @Test
    public void testMatchAfterHash() throws Exception
    {
        Queue<?> queue = _vhost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, getTestName() + "_queue"));
        _exchange.bind(queue.getName(), "a.*.#.b.c", null, false);

        RoutingResult<ServerMessage<?>> result = _exchange.route(_messageWithNoHeaders,
                                                                                       "a.c.b.b",
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
    public void testHashAfterHash() throws Exception
    {
        Queue<?> queue = _vhost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, getTestName() + "_queue"));
        _exchange.bind(queue.getName(), "a.*.#.b.c.#.d", null, false);


        RoutingResult<ServerMessage<?>> result = _exchange.route(_messageWithNoHeaders,
                                                                                       "a.c.b.b.c",
                                                                                       _instanceProperties);
        assertEquals(0, result.getNumberOfRoutes());

        result = _exchange.route(_messageWithNoHeaders,
                                 "a.a.b.c.d",
                                 _instanceProperties);
        assertEquals(1, result.getNumberOfRoutes());

    }

    @Test
    public void testHashHash() throws Exception
    {
        Queue<?> queue = _vhost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, getTestName() + "_queue"));
        _exchange.bind(queue.getName(), "a.#.*.#.d", null, false);

        RoutingResult<ServerMessage<?>> result = _exchange.route(_messageWithNoHeaders,
                                                                                       "a.c.b.b.c",
                                                                                       _instanceProperties);
        assertEquals(0, result.getNumberOfRoutes());

        result = _exchange.route(_messageWithNoHeaders, "a.c.b.b.c", _instanceProperties);
        assertEquals(0, result.getNumberOfRoutes());

        result = _exchange.route(_messageWithNoHeaders, "a.a.b.c.d", _instanceProperties);
        assertEquals(1, result.getNumberOfRoutes());
    }

    @Test
    public void testSubMatchFails() throws Exception
    {
        Queue<?> queue = _vhost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, getTestName() + "_queue"));
        _exchange.bind(queue.getName(), "a.b.c.d", null, false);

        RoutingResult<ServerMessage<?>> result = _exchange.route(_messageWithNoHeaders,
                                                                                       "a.b.c",
                                                                                       _instanceProperties);
        assertEquals(0, result.getNumberOfRoutes());
    }

    @Test
    public void testRouteToManyQueues() throws Exception
    {
        Queue<?> queue1 = _vhost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, getTestName() + "_queue1"));
        Queue<?> queue2 = _vhost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, getTestName() + "_queue2"));
        _exchange.bind(queue1.getName(), "a.b", null, false);
        _exchange.bind(queue2.getName(), "a.*", null, false);

        RoutingResult<ServerMessage<?>> result = _exchange.route(_messageWithNoHeaders,
                                                                                       "a.b",
                                                                                       _instanceProperties);
        assertEquals(2, result.getNumberOfRoutes());

        result = _exchange.route(_messageWithNoHeaders,
                                 "a.c",
                                 _instanceProperties);
        assertEquals(1, result.getNumberOfRoutes());

        _exchange.deleteBinding("a.b", queue1);

        result = _exchange.route(_messageWithNoHeaders,
                                 "a.b",
                                 _instanceProperties);
        assertEquals(1, result.getNumberOfRoutes());

    }

    @Test
    public void testRouteToQueueWithSelector()
    {
        String bindingKey = "mybinding";

        Queue<?> queue = _vhost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, getTestName() + "_queue"));

        InstanceProperties instanceProperties = mock(InstanceProperties.class);
        ServerMessage<?> matchingMessage = createTestMessage(Collections.singletonMap("prop", true));
        ServerMessage<?> unmatchingMessage = createTestMessage(Collections.singletonMap("prop", false));

        boolean bind = _exchange.bind(queue.getName(), bindingKey,
                                      Collections.singletonMap(JMS_SELECTOR.toString(), "prop = True"),
                                      false);
        assertTrue("Bind operation should be successful", bind);

        RoutingResult<ServerMessage<?>> result = _exchange.route(matchingMessage, "mybinding", instanceProperties);
        assertTrue("Message with matching selector not routed to queue", result.hasRoutes());

        result = _exchange.route(unmatchingMessage, "mybinding", instanceProperties);
        assertFalse("Message without matching selector unexpectedly routed to queue", result.hasRoutes());

        boolean unbind = _exchange.unbind(queue.getName(), bindingKey);
        assertTrue("Unbind operation should be successful", unbind);

        result = _exchange.route(matchingMessage, "mybinding", instanceProperties);
        assertFalse("Message with matching selector unexpectedly routed to queue after unbind",
                           result.hasRoutes());

    }

    @Test
    public void testRouteToQueueViaTwoExchanges()
    {
        String bindingKey = "key";

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(Exchange.NAME, getTestName());
        attributes.put(Exchange.TYPE, ExchangeDefaults.FANOUT_EXCHANGE_CLASS);

        Exchange via = _vhost.createChild(Exchange.class, attributes);
        Queue<?> queue = _vhost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, getTestName() + "_queue"));

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

        Exchange via = _vhost.createChild(Exchange.class, attributes);
        Queue<?> queue = _vhost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, getTestName() + "_queue"));

        String bindingKey = "key";
        String replacementKey = "key1";
        boolean exchToViaBind = _exchange.bind(via.getName(),
                                               bindingKey,
                                               Collections.singletonMap(Binding.BINDING_ARGUMENT_REPLACEMENT_ROUTING_KEY,
                                                                        replacementKey),
                                               false);
        assertTrue("Exchange to exchange bind operation should be successful", exchToViaBind);

        boolean viaToQueueBind = via.bind(queue.getName(), replacementKey, Collections.emptyMap(), false);
        assertTrue("Exchange to queue bind operation should be successful", viaToQueueBind);

        RoutingResult<ServerMessage<?>> result = _exchange.route(_messageWithNoHeaders,
                                                                                       bindingKey,
                                                                                       _instanceProperties);
        assertTrue("Message unexpectedly not routed to queue", result.hasRoutes());

        result = _exchange.route(_messageWithNoHeaders, replacementKey, _instanceProperties);
        assertFalse("Message unexpectedly was routed to queue", result.hasRoutes());
    }

    @Test
    public void testRouteToQueueViaTwoExchangesWithReplacementRoutingKeyAndFiltering()
    {
        String bindingKey = "key1";
        String replacementKey = "key2";

        Map<String, Object> viaExchangeArguments = new HashMap<>();
        viaExchangeArguments.put(Exchange.NAME, getTestName() + "_via_exch");
        viaExchangeArguments.put(Exchange.TYPE, ExchangeDefaults.TOPIC_EXCHANGE_CLASS);

        Exchange via = _vhost.createChild(Exchange.class, viaExchangeArguments);
        Queue<?> queue = _vhost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, getTestName() + "_queue"));


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

        RoutingResult<ServerMessage<?>> result = _exchange.route(createTestMessage(Collections.singletonMap("prop", true)),
                                                                                       bindingKey,
                                                                                       _instanceProperties);
        assertTrue("Message unexpectedly not routed to queue", result.hasRoutes());

        result = _exchange.route(createTestMessage(Collections.singletonMap("prop", false)),
                                 bindingKey,
                                 _instanceProperties);
        assertFalse("Message unexpectedly routed to queue", result.hasRoutes());
    }


    @Test
    public void testHierachicalRouteToQueueViaTwoExchangesWithReplacementRoutingKey()
    {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(Exchange.NAME, getTestName());
        attributes.put(Exchange.TYPE, ExchangeDefaults.DIRECT_EXCHANGE_CLASS);

        Exchange via = _vhost.createChild(Exchange.class, attributes);
        Queue<?> queue1 =
                _vhost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, getTestName() + "_queue1"));
        Queue<?> queue2 =
                _vhost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, getTestName() + "_queue2"));

        String bindingKey1 = "a.#";
        String bindingKey2 = "a.*";
        String replacementKey1 = "key1";
        String replacementKey2 = "key2";

        assertTrue("Exchange to exchange bind operation should be successful", _exchange.bind(via.getName(),
                                                                                                     bindingKey1,
                                                                                                     Collections.singletonMap(
                                                                                                      Binding.BINDING_ARGUMENT_REPLACEMENT_ROUTING_KEY,
                                                                                                      replacementKey1),
                                                                                                     false));

        assertTrue("Exchange to exchange bind operation should be successful", _exchange.bind(via.getName(),
                                                                                                     bindingKey2,
                                                                                                     Collections.singletonMap(
                                                                                                      Binding.BINDING_ARGUMENT_REPLACEMENT_ROUTING_KEY,
                                                                                                      replacementKey2),
                                                                                                     false));

        assertTrue("Exchange to queue1 bind operation should be successful",
                          via.bind(queue1.getName(), replacementKey1, Collections.emptyMap(), false));

        assertTrue("Exchange to queue2 bind operation should be successful",
                          via.bind(queue2.getName(), replacementKey2, Collections.emptyMap(), false));

        RoutingResult<ServerMessage<?>> result = _exchange.route(_messageWithNoHeaders,
                                                                                       "a.b",
                                                                                       _instanceProperties);
        assertEquals("Unexpected number of routes", (long) 2, (long) result.getNumberOfRoutes());

        result = _exchange.route(_messageWithNoHeaders, "a.b.c", _instanceProperties);
        assertEquals("Unexpected number of routes", (long) 1, (long) result.getNumberOfRoutes());

        assertTrue("Message is not routed into 'queue1'", result.getRoutes().contains(queue1));
    }


    @Test
    public void testUpdateBindingReplacingSelector() throws Exception
    {
        String bindingKey = "mybinding";

        Queue<?> queue = _vhost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, getTestName() + "_queue"));

        InstanceProperties instanceProperties = mock(InstanceProperties.class);
        ServerMessage<?> matchingMessage = createTestMessage(Collections.singletonMap("prop", true));

        boolean bind = _exchange.bind(queue.getName(), bindingKey,
                                      Collections.singletonMap(JMS_SELECTOR.toString(), "prop = True"),
                                      false);
        assertTrue("Bind operation should be successful", bind);

        RoutingResult<ServerMessage<?>> result = _exchange.route(matchingMessage, bindingKey, instanceProperties);
        assertTrue("Message with matching selector not routed to queue", result.hasRoutes());

        _exchange.replaceBinding(bindingKey, queue, Collections.singletonMap(JMS_SELECTOR.toString(), "prop = False"));

        result = _exchange.route(matchingMessage, bindingKey, instanceProperties);
        assertFalse("Message unexpectedly routed to queue after rebind", result.hasRoutes());

        result = _exchange.route(matchingMessage, bindingKey, instanceProperties);
        assertFalse(result.hasRoutes());

        matchingMessage = createTestMessage(Collections.singletonMap("prop", false));
        result = _exchange.route(matchingMessage, bindingKey, instanceProperties);
        assertTrue("Message not routed to queue", result.hasRoutes());
    }

    @Test
    public void testUpdateBindingRemovingSelector() throws Exception
    {
        String bindingKey = "mybinding";

        Queue<?> queue = _vhost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, getTestName() + "_queue"));

        InstanceProperties instanceProperties = mock(InstanceProperties.class);
        ServerMessage<?> message = createTestMessage(Collections.singletonMap("prop", false));

        boolean bind = _exchange.bind(queue.getName(), bindingKey,
                                      Collections.singletonMap(JMS_SELECTOR.toString(), "prop = True"),
                                      false);
        assertTrue("Bind operation should be successful", bind);

        RoutingResult<ServerMessage<?>> result = _exchange.route(message, bindingKey, instanceProperties);
        assertFalse("Message that does not match selector routed to queue", result.hasRoutes());

        _exchange.replaceBinding(bindingKey, queue, Collections.emptyMap());

        result = _exchange.route(message, bindingKey, instanceProperties);
        assertTrue("Message not routed to queue after rebind", result.hasRoutes());
    }

    @Test
    public void testUpdateBindingAddingSelector() throws Exception
    {
        String bindingKey = "mybinding";

        Queue<?> queue = _vhost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, getTestName() + "_queue"));

        InstanceProperties instanceProperties = mock(InstanceProperties.class);
        ServerMessage<?> message = createTestMessage(Collections.singletonMap("prop", false));

        boolean bind = _exchange.bind(queue.getName(), bindingKey,
                                      Collections.emptyMap(),
                                      false);
        assertTrue("Bind operation should be successful", bind);

        RoutingResult<ServerMessage<?>> result = _exchange.route(message, bindingKey, instanceProperties);
        assertTrue("Message not routed to queue", result.hasRoutes());

        _exchange.replaceBinding(bindingKey, queue, Collections.singletonMap(JMS_SELECTOR.toString(), "prop = false"));

        result = _exchange.route(message, bindingKey, instanceProperties);
        assertTrue("Message that matches selector not routed to queue after rebind", result.hasRoutes());

        result = _exchange.route(message = createTestMessage(Collections.singletonMap("prop", true)), bindingKey, instanceProperties);
        assertFalse("Message that does not match selector routed to queue after rebind", result.hasRoutes());
    }

    @Test
    public void testUpdateBindingChangeReplacementKey() throws Exception
    {
        String bindingKey = "mybinding";
        String replacementKey = "key1";
        String replacementKey2 = "key2";

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(Exchange.NAME, getTestName());
        attributes.put(Exchange.TYPE, ExchangeDefaults.DIRECT_EXCHANGE_CLASS);

        Exchange via = _vhost.createChild(Exchange.class, attributes);
        Queue<?> queue =
                _vhost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, getTestName() + "_queue"));

        boolean exchToViaBind = _exchange.bind(via.getName(),
                                               bindingKey,
                                               Collections.emptyMap(),
                                               false);
        assertTrue("Exchange to exchange bind operation should be successful", exchToViaBind);

        boolean viaToQueueBind = via.bind(queue.getName(), replacementKey, Collections.emptyMap(), false);
        assertTrue("Exchange to queue bind operation should be successful", viaToQueueBind);

        RoutingResult<ServerMessage<?>> result = _exchange.route(_messageWithNoHeaders,
                                                                                       bindingKey,
                                                                                       _instanceProperties);
        assertFalse("Message unexpectedly routed to queue", result.hasRoutes());

        _exchange.bind(via.getName(),
                       bindingKey,
                       Collections.singletonMap(Binding.BINDING_ARGUMENT_REPLACEMENT_ROUTING_KEY, replacementKey),
                       true);

        result = _exchange.route(_messageWithNoHeaders, bindingKey, _instanceProperties);
        assertTrue("Message was not routed", result.hasRoutes());
        assertTrue("Message was not routed to queue", result.getRoutes().contains(queue));

        Queue<?> queue2 =
                _vhost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, getTestName() + "_queue2"));
        assertTrue("Binding of queue2 failed",
                          via.bind(queue2.getName(), replacementKey2, Collections.emptyMap(), false));


        _exchange.bind(via.getName(),
                       bindingKey,
                       Collections.singletonMap(Binding.BINDING_ARGUMENT_REPLACEMENT_ROUTING_KEY, replacementKey2),
                       true);

        result = _exchange.route(_messageWithNoHeaders, bindingKey, _instanceProperties);
        assertTrue("Message was not routed", result.hasRoutes());
        assertTrue("Message was not routed to queue2", result.getRoutes().contains(queue2));
    }


    @Test
    public void testBindWithInvalidSelector()
    {
        final String queueName = getTestName() + "_queue";
        _vhost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, queueName));

        final Map<String, Object> bindArguments = Collections.singletonMap(JMS_SELECTOR.toString(), "foo in (");

        try
        {
            _exchange.bind(queueName, "#", bindArguments, false);
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
        _vhost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, queueName));

        final Map<String, Object> bindArguments = Collections.singletonMap(JMS_SELECTOR.toString(), "foo in ('bar')");
        final boolean isBound = _exchange.bind(queueName, "#", bindArguments, false);
        assertTrue("Could not bind queue", isBound);

        final ServerMessage<?> testMessage = createTestMessage(Collections.singletonMap("foo", "bar"));
        final RoutingResult<ServerMessage<?>> result = _exchange.route(testMessage, queueName, _instanceProperties);
        assertTrue("Message should be routed to queue", result.hasRoutes());

        final Map<String, Object> bindArguments2 = Collections.singletonMap(JMS_SELECTOR.toString(), "foo in (");
        try
        {
            _exchange.bind(queueName, "#", bindArguments2, true);
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

        @SuppressWarnings("unchecked")
        ServerMessage<?> message = mock(ServerMessage.class);
        when(message.isResourceAcceptable(any(TransactionLogResource.class))).thenReturn(true);
        when(message.getMessageHeader()).thenReturn(header);
        return message;
    }

}
