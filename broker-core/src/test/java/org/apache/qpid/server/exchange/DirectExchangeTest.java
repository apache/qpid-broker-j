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
package org.apache.qpid.server.exchange;

import static org.apache.qpid.server.filter.AMQPFilterTypes.JMS_SELECTOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
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

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.message.InstanceProperties;
import org.apache.qpid.server.message.RoutingResult;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.AlternateBinding;
import org.apache.qpid.server.model.Binding;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.server.virtualhost.MessageDestinationIsAlternateException;
import org.apache.qpid.server.virtualhost.ReservedExchangeNameException;
import org.apache.qpid.server.virtualhost.UnknownAlternateBindingException;
import org.apache.qpid.test.utils.UnitTestBase;

public class DirectExchangeTest extends UnitTestBase
{
    private DirectExchange<?> _exchange;
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
        attributes.put(Exchange.TYPE, ExchangeDefaults.DIRECT_EXCHANGE_CLASS);

        _exchange = (DirectExchange<?>) _vhost.createChild(Exchange.class, attributes);

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

    @Test
    public void testCreationOfExchangeWithReservedExchangePrefixRejected() throws Exception
    {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(Exchange.NAME, "amq.wibble");
        attributes.put(Exchange.DURABLE, false);
        attributes.put(Exchange.TYPE, ExchangeDefaults.DIRECT_EXCHANGE_CLASS);

        try
        {
            _exchange = (DirectExchangeImpl) _vhost.createChild(Exchange.class, attributes);
            _exchange.open();
            fail("Exception not thrown");
        }
        catch (ReservedExchangeNameException rene)
        {
            // PASS
        }
    }

    @Test
    public void testAmqpDirectRecreationRejected() throws Exception
    {
        DirectExchangeImpl amqpDirect = (DirectExchangeImpl) _vhost.getChildByName(Exchange.class, ExchangeDefaults.DIRECT_EXCHANGE_NAME);

        assertNotNull(amqpDirect);

        assertSame(amqpDirect, _vhost.getChildById(Exchange.class, amqpDirect.getId()));
        assertSame(amqpDirect, _vhost.getChildByName(Exchange.class, amqpDirect.getName()));

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(Exchange.NAME, ExchangeDefaults.DIRECT_EXCHANGE_NAME);
        attributes.put(Exchange.DURABLE, true);
        attributes.put(Exchange.TYPE, ExchangeDefaults.DIRECT_EXCHANGE_CLASS);

        try
        {
            _exchange = (DirectExchangeImpl) _vhost.createChild(Exchange.class, attributes);
            _exchange.open();
            fail("Exception not thrown");
        }
        catch (ReservedExchangeNameException rene)
        {
            // PASS
        }

        // QPID-6599, defect would mean that the existing exchange was removed from the in memory model.
        assertSame(amqpDirect, _vhost.getChildById(Exchange.class, amqpDirect.getId()));
        assertSame(amqpDirect, _vhost.getChildByName(Exchange.class, amqpDirect.getName()));
    }

    @Test
    public void testDeleteOfExchangeSetAsAlternate() throws Exception
    {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(Queue.NAME, getTestName());
        attributes.put(Queue.DURABLE, false);
        attributes.put(Queue.ALTERNATE_BINDING, Collections.singletonMap(AlternateBinding.DESTINATION, _exchange.getName()));

        Queue queue = _vhost.createChild(Queue.class, attributes);
        queue.open();

        assertEquals("Unexpected alternate exchange on queue", _exchange, queue.getAlternateBindingDestination());


        try
        {
            _exchange.delete();
            fail("Exchange deletion should fail with MessageDestinationIsAlternateException");
        }
        catch(MessageDestinationIsAlternateException e)
        {
            // pass
        }

        assertEquals("Unexpected effective exchange state", State.ACTIVE, _exchange.getState());
        assertEquals("Unexpected desired exchange state", State.ACTIVE, _exchange.getDesiredState());
    }

    @Test
    public void testAlternateBindingValidationRejectsNonExistingDestination()
    {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(Exchange.NAME, getTestName());
        attributes.put(Exchange.TYPE, ExchangeDefaults.DIRECT_EXCHANGE_CLASS);
        String alternateBinding = "nonExisting";
        attributes.put(Exchange.ALTERNATE_BINDING,
                       Collections.singletonMap(AlternateBinding.DESTINATION, alternateBinding));

        try
        {
            _vhost.createChild(Exchange.class, attributes);
            fail("Expected exception is not thrown");
        }
        catch (UnknownAlternateBindingException e)
        {
            assertEquals("Unexpected exception alternate binding", alternateBinding, e.getAlternateBindingName());
        }
    }

    @Test
    public void testAlternateBindingValidationRejectsSelf()
    {
        Map<String, String> alternateBinding = Collections.singletonMap(AlternateBinding.DESTINATION, _exchange.getName());
        Map<String, Object> newAttributes = Collections.singletonMap(Exchange.ALTERNATE_BINDING, alternateBinding);
        try
        {
            _exchange.setAttributes(newAttributes);
            fail("Expected exception is not thrown");
        }
        catch (IllegalConfigurationException e)
        {
            // pass
        }
    }

    @Test
    public void testDurableExchangeRejectsNonDurableAlternateBinding()
    {
        Map<String, Object> dlqAttributes = new HashMap<>();
        String dlqName = getTestName() + "_DLQ";
        dlqAttributes.put(Queue.NAME, dlqName);
        dlqAttributes.put(Queue.DURABLE, false);
        _vhost.createChild(Queue.class, dlqAttributes);

        Map<String, Object> exchangeAttributes = new HashMap<>();
        exchangeAttributes.put(Exchange.NAME, getTestName());
        exchangeAttributes.put(Exchange.ALTERNATE_BINDING, Collections.singletonMap(AlternateBinding.DESTINATION, dlqName));
        exchangeAttributes.put(Exchange.DURABLE, true);
        exchangeAttributes.put(Exchange.TYPE, ExchangeDefaults.DIRECT_EXCHANGE_CLASS);

        try
        {
            _vhost.createChild(Exchange.class, exchangeAttributes);
            fail("Expected exception is not thrown");
        }
        catch (IllegalConfigurationException e)
        {
            // pass
        }
    }

    @Test
    public void testAlternateBinding()
    {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(Exchange.NAME, getTestName());
        attributes.put(Exchange.TYPE, ExchangeDefaults.DIRECT_EXCHANGE_CLASS);
        attributes.put(Exchange.ALTERNATE_BINDING,
                       Collections.singletonMap(AlternateBinding.DESTINATION, _exchange.getName()));
        attributes.put(Exchange.DURABLE, false);

        Exchange newExchange = _vhost.createChild(Exchange.class, attributes);

        assertEquals("Unexpected alternate binding",
                            _exchange.getName(),
                            newExchange.getAlternateBinding().getDestination());
    }

    @Test
    public void testRouteToQueue()
    {
        String boundKey = "key";
        Queue<?> queue = _vhost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, getTestName() + "_queue"));

        RoutingResult<ServerMessage<?>> result = _exchange.route(_messageWithNoHeaders, boundKey,
                                                                                       _instanceProperties);
        assertFalse("Message unexpectedly routed to queue before bind", result.hasRoutes());

        boolean bind = _exchange.bind(queue.getName(), boundKey, Collections.emptyMap(), false);
        assertTrue("Bind operation should be successful", bind);

        result = _exchange.route(_messageWithNoHeaders, boundKey, _instanceProperties);
        assertTrue("Message unexpectedly not routed to queue after bind", result.hasRoutes());

        boolean unbind = _exchange.unbind(queue.getName(), boundKey);
        assertTrue("Unbind operation should be successful", unbind);

        result = _exchange.route(_messageWithNoHeaders, boundKey, _instanceProperties);
        assertFalse("Message unexpectedly routed to queue after unbind", result.hasRoutes());
    }

    @Test
    public void testRouteToQueueWithSelector()
    {
        String boundKey = "key";
        Queue<?> queue = _vhost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, getTestName() + "_queue"));

        InstanceProperties instanceProperties = mock(InstanceProperties.class);
        ServerMessage<?> matchingMessage = createTestMessage(Collections.singletonMap("prop", true));
        ServerMessage<?> unmatchingMessage = createTestMessage(Collections.singletonMap("prop", false));

        boolean bind = _exchange.bind(queue.getName(), boundKey,
                                      Collections.singletonMap(JMS_SELECTOR.toString(), "prop = True"),
                                      false);
        assertTrue("Bind operation should be successful", bind);

        RoutingResult<ServerMessage<?>> result = _exchange.route(matchingMessage, boundKey, instanceProperties);
        assertTrue("Message with matching selector not routed to queue", result.hasRoutes());

        result = _exchange.route(unmatchingMessage, boundKey, instanceProperties);
        assertFalse("Message with matching selector unexpectedly routed to queue", result.hasRoutes());

        boolean unbind = _exchange.unbind(queue.getName(), boundKey);
        assertTrue("Unbind operation should be successful", unbind);

        result = _exchange.route(matchingMessage, boundKey, instanceProperties);
        assertFalse("Message with matching selector unexpectedly routed to queue after unbind",
                           result.hasRoutes());

    }

    @Test
    public void testRouteToQueueViaTwoExchanges()
    {
        String boundKey = "key";

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(Exchange.NAME, getTestName());
        attributes.put(Exchange.TYPE, ExchangeDefaults.DIRECT_EXCHANGE_CLASS);

        Exchange via = _vhost.createChild(Exchange.class, attributes);
        Queue<?> queue = _vhost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, getTestName() + "_queue"));

        boolean exchToViaBind = _exchange.bind(via.getName(), boundKey, Collections.emptyMap(), false);
        assertTrue("Exchange to exchange bind operation should be successful", exchToViaBind);

        boolean viaToQueueBind = via.bind(queue.getName(), boundKey, Collections.emptyMap(), false);
        assertTrue("Exchange to queue bind operation should be successful", viaToQueueBind);

        RoutingResult<ServerMessage<?>> result = _exchange.route(_messageWithNoHeaders,
                                                                                       boundKey,
                                                                                       _instanceProperties);
        assertTrue("Message unexpectedly not routed to queue", result.hasRoutes());
    }


    @Test
    public void testDestinationDeleted()
    {
        String boundKey = "key";
        Queue<?> queue = _vhost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, getTestName() + "_queue"));


        assertFalse(_exchange.isBound(boundKey));
        assertFalse(_exchange.isBound(boundKey, queue));
        assertFalse(_exchange.isBound(queue));

        _exchange.bind(queue.getName(), boundKey, Collections.emptyMap(), false);

        assertTrue(_exchange.isBound(boundKey));
        assertTrue(_exchange.isBound(boundKey, queue));
        assertTrue(_exchange.isBound(queue));

        queue.delete();

        assertFalse(_exchange.isBound(boundKey));
        assertFalse(_exchange.isBound(boundKey, queue));
        assertFalse(_exchange.isBound(queue));
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

    @Test
    public void testRouteToMultipleQueues()
    {
        String boundKey = "key";
        Queue<?> queue1 = _vhost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, getTestName() + "_queue1"));
        Queue<?> queue2 = _vhost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, getTestName() + "_queue2"));

        boolean bind1 = _exchange.bind(queue1.getName(), boundKey, Collections.emptyMap(), false);
        assertTrue("Bind operation to queue1 should be successful", bind1);

        RoutingResult<ServerMessage<?>> result = _exchange.route(_messageWithNoHeaders, boundKey, _instanceProperties);
        assertEquals("Message routed to unexpected number of queues",
                            (long) 1,
                            (long) result.getNumberOfRoutes());

        _exchange.bind(queue2.getName(), boundKey, Collections.singletonMap(JMS_SELECTOR.toString(), "prop is null"), false);

        result = _exchange.route(_messageWithNoHeaders, boundKey, _instanceProperties);
        assertEquals("Message routed to unexpected number of queues",
                            (long) 2,
                            (long) result.getNumberOfRoutes());

        _exchange.unbind(queue1.getName(), boundKey);

        result = _exchange.route(_messageWithNoHeaders, boundKey, _instanceProperties);
        assertEquals("Message routed to unexpected number of queues",
                            (long) 1,
                            (long) result.getNumberOfRoutes());

        _exchange.unbind(queue2.getName(), boundKey);
        result = _exchange.route(_messageWithNoHeaders, boundKey, _instanceProperties);
        assertEquals("Message routed to unexpected number of queues",
                            (long) 0,
                            (long) result.getNumberOfRoutes());
    }

    @Test
    public void testRouteToQueueViaTwoExchangesWithReplacementRoutingKey()
    {
        Map<String, Object> viaExchangeArguments = new HashMap<>();
        viaExchangeArguments.put(Exchange.NAME, "via_exchange");
        viaExchangeArguments.put(Exchange.TYPE, ExchangeDefaults.DIRECT_EXCHANGE_CLASS);

        Exchange via = _vhost.createChild(Exchange.class, viaExchangeArguments);
        Queue<?> queue = _vhost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, getTestName() + "_queue"));

        boolean exchToViaBind = _exchange.bind(via.getName(),
                                               "key1",
                                               Collections.singletonMap(Binding.BINDING_ARGUMENT_REPLACEMENT_ROUTING_KEY, "key2"),
                                               false);
        assertTrue("Exchange to exchange bind operation should be successful", exchToViaBind);

        boolean viaToQueueBind = via.bind(queue.getName(), "key2", Collections.emptyMap(), false);
        assertTrue("Exchange to queue bind operation should be successful", viaToQueueBind);

        RoutingResult<ServerMessage<?>> result = _exchange.route(_messageWithNoHeaders,
                                                                                       "key1",
                                                                                       _instanceProperties);
        assertTrue("Message unexpectedly not routed to queue", result.hasRoutes());
    }

    @Test
    public void testRouteToQueueViaTwoExchangesWithReplacementRoutingKeyAndFiltering()
    {
        Map<String, Object> viaExchangeArguments = new HashMap<>();
        viaExchangeArguments.put(Exchange.NAME, getTestName() + "_via_exch");
        viaExchangeArguments.put(Exchange.TYPE, ExchangeDefaults.DIRECT_EXCHANGE_CLASS);

        Exchange via = _vhost.createChild(Exchange.class, viaExchangeArguments);
        Queue<?> queue = _vhost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, getTestName() + "_queue"));


        Map<String, Object> exchToViaBindArguments = new HashMap<>();
        exchToViaBindArguments.put(Binding.BINDING_ARGUMENT_REPLACEMENT_ROUTING_KEY, "key2");
        exchToViaBindArguments.put(JMS_SELECTOR.toString(), "prop = True");

        boolean exchToViaBind = _exchange.bind(via.getName(),
                                               "key1",
                                               exchToViaBindArguments,
                                               false);
        assertTrue("Exchange to exchange bind operation should be successful", exchToViaBind);

        boolean viaToQueueBind = via.bind(queue.getName(), "key2", Collections.emptyMap(), false);
        assertTrue("Exchange to queue bind operation should be successful", viaToQueueBind);

        RoutingResult<ServerMessage<?>> result = _exchange.route(createTestMessage(Collections.singletonMap("prop", true)),
                                                                                                            "key1",
                                                                                                            _instanceProperties);

        assertTrue("Message unexpectedly not routed to queue", result.hasRoutes());

        result = _exchange.route(createTestMessage(Collections.singletonMap("prop", false)),
                                 "key1",
                                 _instanceProperties);
        assertFalse("Message unexpectedly routed to queue", result.hasRoutes());
    }

    @Test
    public void testBindWithInvalidSelector()
    {
        final String queueName = getTestName() + "_queue";
        _vhost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, queueName));

        final Map<String, Object> bindArguments = Collections.singletonMap(JMS_SELECTOR.toString(), "foo in (");

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
        _vhost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, queueName));

        final Map<String, Object> bindArguments = Collections.singletonMap(JMS_SELECTOR.toString(), "foo in ('bar')");
        final boolean isBound = _exchange.bind(queueName, queueName, bindArguments, false);
        assertTrue("Could not bind queue", isBound);

        final ServerMessage<?> testMessage = createTestMessage(Collections.singletonMap("foo", "bar"));
        final RoutingResult<ServerMessage<?>> result = _exchange.route(testMessage, queueName, _instanceProperties);
        assertTrue("Message should be routed to queue", result.hasRoutes());

        final Map<String, Object> bindArguments2 = Collections.singletonMap(JMS_SELECTOR.toString(), "foo in (");
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
}
