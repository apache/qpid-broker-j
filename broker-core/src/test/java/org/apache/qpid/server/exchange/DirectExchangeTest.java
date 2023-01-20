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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

    @BeforeAll
    public void beforeAll() throws Exception
    {
        _vhost = BrokerTestHelper.createVirtualHost(getTestClassName(), this);

        final Map<String,Object> attributes = Map.of(Exchange.NAME, "test",
                Exchange.DURABLE, false,
                Exchange.TYPE, ExchangeDefaults.DIRECT_EXCHANGE_CLASS);

        _exchange = (DirectExchange<?>) _vhost.createChild(Exchange.class, attributes);
    }

    @BeforeEach
    public void setUp() throws Exception
    {
        _instanceProperties = mock(InstanceProperties.class);
        _messageWithNoHeaders = createTestMessage(Map.of());
    }

    @Test
    public void testCreationOfExchangeWithReservedExchangePrefixRejected()
    {
        final Map<String, Object> attributes = Map.of(Exchange.NAME, "amq.wibble",
                Exchange.DURABLE, false,
                Exchange.TYPE, ExchangeDefaults.DIRECT_EXCHANGE_CLASS);
        assertThrows(ReservedExchangeNameException.class, () ->
                {
                    _exchange = (DirectExchangeImpl) _vhost.createChild(Exchange.class, attributes);
                    _exchange.open();
                }, "Exception not thrown");
    }

    @Test
    public void testAmqpDirectRecreationRejected()
    {
        final DirectExchangeImpl amqpDirect = 
                (DirectExchangeImpl) _vhost.getChildByName(Exchange.class, ExchangeDefaults.DIRECT_EXCHANGE_NAME);

        assertNotNull(amqpDirect);

        assertSame(amqpDirect, _vhost.getChildById(Exchange.class, amqpDirect.getId()));
        assertSame(amqpDirect, _vhost.getChildByName(Exchange.class, amqpDirect.getName()));

        final Map<String, Object> attributes = Map.of(Exchange.NAME, ExchangeDefaults.DIRECT_EXCHANGE_NAME,
                Exchange.DURABLE, true,
                Exchange.TYPE, ExchangeDefaults.DIRECT_EXCHANGE_CLASS);
        assertThrows(ReservedExchangeNameException.class, () ->
                {
                    _exchange = (DirectExchangeImpl) _vhost.createChild(Exchange.class, attributes);
                    _exchange.open();
                }, "Exception not thrown");

        // QPID-6599, defect would mean that the existing exchange was removed from the in memory model.
        assertSame(amqpDirect, _vhost.getChildById(Exchange.class, amqpDirect.getId()));
        assertSame(amqpDirect, _vhost.getChildByName(Exchange.class, amqpDirect.getName()));
    }

    @Test
    public void testDeleteOfExchangeSetAsAlternate()
    {
        final Map<String, Object> attributes = Map.of(Queue.NAME, getTestName(),
                Queue.DURABLE, false,
                Queue.ALTERNATE_BINDING, Map.of(AlternateBinding.DESTINATION, _exchange.getName()));

        Queue<?> queue = _vhost.createChild(Queue.class, attributes);
        queue.open();

        assertEquals(_exchange, queue.getAlternateBindingDestination(), "Unexpected alternate exchange on queue");

        assertThrows(MessageDestinationIsAlternateException.class,() ->_exchange.delete(),
                "Exchange deletion should fail with MessageDestinationIsAlternateException");

        assertEquals(State.ACTIVE, _exchange.getState(), "Unexpected effective exchange state");
        assertEquals(State.ACTIVE, _exchange.getDesiredState(), "Unexpected desired exchange state");
    }

    @Test
    public void testAlternateBindingValidationRejectsNonExistingDestination()
    {
        final String alternateBinding = "nonExisting";
        final Map<String, Object> attributes = Map.of(Exchange.NAME, getTestName(),
                Exchange.TYPE, ExchangeDefaults.DIRECT_EXCHANGE_CLASS,
                Exchange.ALTERNATE_BINDING, Map.of(AlternateBinding.DESTINATION, alternateBinding));

        final UnknownAlternateBindingException thrown = assertThrows(UnknownAlternateBindingException.class,
                () -> _vhost.createChild(Exchange.class, attributes),
                "Expected exception is not thrown");
        assertEquals(alternateBinding, thrown.getAlternateBindingName(), "Unexpected exception alternate binding");
    }

    @Test
    public void testAlternateBindingValidationRejectsSelf()
    {
        final Map<String, String> alternateBinding = Map.of(AlternateBinding.DESTINATION, _exchange.getName());
        final Map<String, Object> newAttributes = Map.of(Exchange.ALTERNATE_BINDING, alternateBinding);
        assertThrows(IllegalConfigurationException.class, () -> _exchange.setAttributes(newAttributes),
                "Expected exception is not thrown");
    }

    @Test
    public void testDurableExchangeRejectsNonDurableAlternateBinding()
    {
        final String dlqName = getTestName() + "_DLQ";
        final Map<String, Object> dlqAttributes = Map.of(Queue.NAME, dlqName,
                Queue.DURABLE, false);
        _vhost.createChild(Queue.class, dlqAttributes);

        final Map<String, Object> exchangeAttributes = Map.of(Exchange.NAME, getTestName(),
                Exchange.ALTERNATE_BINDING, Map.of(AlternateBinding.DESTINATION, dlqName),
                Exchange.DURABLE, true,
                Exchange.TYPE, ExchangeDefaults.DIRECT_EXCHANGE_CLASS);
        assertThrows(IllegalConfigurationException.class, () -> _vhost.createChild(Exchange.class, exchangeAttributes),
                "Expected exception is not thrown");
    }

    @Test
    public void testAlternateBinding()
    {
        final Map<String, Object> attributes = Map.of(Exchange.NAME, getTestName(),
                Exchange.TYPE, ExchangeDefaults.DIRECT_EXCHANGE_CLASS,
                Exchange.ALTERNATE_BINDING, Map.of(AlternateBinding.DESTINATION, _exchange.getName()),
                Exchange.DURABLE, false);
        final Exchange<?> newExchange = _vhost.createChild(Exchange.class, attributes);

        assertEquals(_exchange.getName(), newExchange.getAlternateBinding().getDestination(),
                "Unexpected alternate binding");
    }

    @Test
    public void testRouteToQueue()
    {
        final String boundKey = "key";
        final Queue<?> queue = _vhost.createChild(Queue.class, Map.of(Queue.NAME, getTestName() + "_queue"));

        RoutingResult<ServerMessage<?>> result = _exchange.route(_messageWithNoHeaders, boundKey, _instanceProperties);
        assertFalse(result.hasRoutes(), "Message unexpectedly routed to queue before bind");

        final boolean bind = _exchange.bind(queue.getName(), boundKey, Map.of(), false);
        assertTrue(bind, "Bind operation should be successful");

        result = _exchange.route(_messageWithNoHeaders, boundKey, _instanceProperties);
        assertTrue(result.hasRoutes(), "Message unexpectedly not routed to queue after bind");

        final boolean unbind = _exchange.unbind(queue.getName(), boundKey);
        assertTrue(unbind, "Unbind operation should be successful");

        result = _exchange.route(_messageWithNoHeaders, boundKey, _instanceProperties);
        assertFalse(result.hasRoutes(), "Message unexpectedly routed to queue after unbind");
    }

    @Test
    public void testRouteToQueueWithSelector()
    {
        final String boundKey = "key";
        final Queue<?> queue = _vhost.createChild(Queue.class, Map.of(Queue.NAME, getTestName() + "_queue"));

        final InstanceProperties instanceProperties = mock(InstanceProperties.class);
        final ServerMessage<?> matchingMessage = createTestMessage(Map.of("prop", true));
        final ServerMessage<?> unmatchingMessage = createTestMessage(Map.of("prop", false));

        final boolean bind = _exchange.bind(queue.getName(), boundKey,
                Map.of(JMS_SELECTOR.toString(), "prop = True"), false);
        assertTrue(bind, "Bind operation should be successful");

        RoutingResult<ServerMessage<?>> result = _exchange.route(matchingMessage, boundKey, instanceProperties);
        assertTrue(result.hasRoutes(), "Message with matching selector not routed to queue");

        result = _exchange.route(unmatchingMessage, boundKey, instanceProperties);
        assertFalse(result.hasRoutes(), "Message with matching selector unexpectedly routed to queue");

        final boolean unbind = _exchange.unbind(queue.getName(), boundKey);
        assertTrue(unbind, "Unbind operation should be successful");

        result = _exchange.route(matchingMessage, boundKey, instanceProperties);
        assertFalse(result.hasRoutes(), "Message with matching selector unexpectedly routed to queue after unbind");
    }

    @Test
    public void testRouteToQueueViaTwoExchanges()
    {
        final String boundKey = "key";
        final Map<String, Object> attributes = Map.of(Exchange.NAME, getTestName(),
                Exchange.TYPE, ExchangeDefaults.DIRECT_EXCHANGE_CLASS);
        final Exchange<?> via = _vhost.createChild(Exchange.class, attributes);
        final Queue<?> queue = _vhost.createChild(Queue.class, Map.of(Queue.NAME, getTestName() + "_queue"));
        final boolean exchToViaBind = _exchange.bind(via.getName(), boundKey, Map.of(), false);
        assertTrue(exchToViaBind, "Exchange to exchange bind operation should be successful");

        final boolean viaToQueueBind = via.bind(queue.getName(), boundKey, Map.of(), false);
        assertTrue(viaToQueueBind, "Exchange to queue bind operation should be successful");

        final RoutingResult<ServerMessage<?>> result = _exchange.route(_messageWithNoHeaders, boundKey, _instanceProperties);
        assertTrue(result.hasRoutes(), "Message unexpectedly not routed to queue");
    }

    @Test
    public void testDestinationDeleted()
    {
        final String boundKey = "key";
        final Queue<?> queue = _vhost.createChild(Queue.class, Map.of(Queue.NAME, getTestName() + "_queue"));

        assertFalse(_exchange.isBound(boundKey));
        assertFalse(_exchange.isBound(boundKey, queue));
        assertFalse(_exchange.isBound(queue));

        _exchange.bind(queue.getName(), boundKey, Map.of(), false);

        assertTrue(_exchange.isBound(boundKey));
        assertTrue(_exchange.isBound(boundKey, queue));
        assertTrue(_exchange.isBound(queue));

        queue.delete();

        assertFalse(_exchange.isBound(boundKey));
        assertFalse(_exchange.isBound(boundKey, queue));
        assertFalse(_exchange.isBound(queue));
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

    @Test
    public void testRouteToMultipleQueues()
    {
        final String boundKey = "key";
        final Queue<?> queue1 = _vhost.createChild(Queue.class, Map.of(Queue.NAME, getTestName() + "_queue1"));
        final Queue<?> queue2 = _vhost.createChild(Queue.class, Map.of(Queue.NAME, getTestName() + "_queue2"));

        final boolean bind1 = _exchange.bind(queue1.getName(), boundKey, Map.of(), false);
        assertTrue(bind1, "Bind operation to queue1 should be successful");

        RoutingResult<ServerMessage<?>> result = _exchange.route(_messageWithNoHeaders, boundKey, _instanceProperties);
        assertEquals(1, (long) result.getNumberOfRoutes(), "Message routed to unexpected number of queues");

        _exchange.bind(queue2.getName(), boundKey, Map.of(JMS_SELECTOR.toString(), "prop is null"), false);

        result = _exchange.route(_messageWithNoHeaders, boundKey, _instanceProperties);
        assertEquals(2, (long) result.getNumberOfRoutes(), "Message routed to unexpected number of queues");

        _exchange.unbind(queue1.getName(), boundKey);

        result = _exchange.route(_messageWithNoHeaders, boundKey, _instanceProperties);
        assertEquals(1, (long) result.getNumberOfRoutes(), "Message routed to unexpected number of queues");

        _exchange.unbind(queue2.getName(), boundKey);
        result = _exchange.route(_messageWithNoHeaders, boundKey, _instanceProperties);
        assertEquals(0, (long) result.getNumberOfRoutes(), "Message routed to unexpected number of queues");
    }

    @Test
    public void testRouteToQueueViaTwoExchangesWithReplacementRoutingKey()
    {
        final Map<String, Object> viaExchangeArguments = Map.of(Exchange.NAME, "via_exchange",
                Exchange.TYPE, ExchangeDefaults.DIRECT_EXCHANGE_CLASS);
        final Exchange<?> via = _vhost.createChild(Exchange.class, viaExchangeArguments);
        final Queue<?> queue = _vhost.createChild(Queue.class, Map.of(Queue.NAME, getTestName() + "_queue"));
        final boolean exchToViaBind = _exchange.bind(via.getName(), "key1",
                Map.of(Binding.BINDING_ARGUMENT_REPLACEMENT_ROUTING_KEY, "key2"), false);
        assertTrue(exchToViaBind, "Exchange to exchange bind operation should be successful");

        final boolean viaToQueueBind = via.bind(queue.getName(), "key2", Map.of(), false);
        assertTrue(viaToQueueBind, "Exchange to queue bind operation should be successful");

        final RoutingResult<ServerMessage<?>> result = _exchange.route(_messageWithNoHeaders, "key1",
                _instanceProperties);
        assertTrue(result.hasRoutes(), "Message unexpectedly not routed to queue");
    }

    @Test
    public void testRouteToQueueViaTwoExchangesWithReplacementRoutingKeyAndFiltering()
    {
        final Map<String, Object> viaExchangeArguments = Map.of(Exchange.NAME, getTestName() + "_via_exch",
                Exchange.TYPE, ExchangeDefaults.DIRECT_EXCHANGE_CLASS);
        final Exchange<?> via = _vhost.createChild(Exchange.class, viaExchangeArguments);
        final Queue<?> queue = _vhost.createChild(Queue.class, Map.of(Queue.NAME, getTestName() + "_queue"));
        final Map<String, Object> exchToViaBindArguments = Map.of(Binding.BINDING_ARGUMENT_REPLACEMENT_ROUTING_KEY, "key2",
                JMS_SELECTOR.toString(), "prop = True");
        final boolean exchToViaBind = _exchange.bind(via.getName(), "key1", exchToViaBindArguments, false);
        assertTrue(exchToViaBind, "Exchange to exchange bind operation should be successful");

        final boolean viaToQueueBind = via.bind(queue.getName(), "key2", Map.of(), false);
        assertTrue(viaToQueueBind, "Exchange to queue bind operation should be successful");

        RoutingResult<ServerMessage<?>> result = _exchange.route(createTestMessage(Map.of("prop", true)), "key1",
                _instanceProperties);

        assertTrue(result.hasRoutes(), "Message unexpectedly not routed to queue");

        result = _exchange.route(createTestMessage(Map.of("prop", false)), "key1", _instanceProperties);
        assertFalse(result.hasRoutes(), "Message unexpectedly routed to queue");
    }

    @Test
    public void testBindWithInvalidSelector()
    {
        final String queueName = getTestName() + "_queue";
        _vhost.createChild(Queue.class, Map.of(Queue.NAME, queueName));

        final Map<String, Object> bindArguments = Map.of(JMS_SELECTOR.toString(), "foo in (");
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

        final Map<String, Object> bindArguments = Map.of(JMS_SELECTOR.toString(), "foo in ('bar')");
        final boolean isBound = _exchange.bind(queueName, queueName, bindArguments, false);
        assertTrue(isBound, "Could not bind queue");

        final ServerMessage<?> testMessage = createTestMessage(Map.of("foo", "bar"));
        final RoutingResult<ServerMessage<?>> result = _exchange.route(testMessage, queueName, _instanceProperties);
        assertTrue(result.hasRoutes(), "Message should be routed to queue");

        final Map<String, Object> bindArguments2 = Map.of(JMS_SELECTOR.toString(), "foo in (");
        assertThrows(IllegalArgumentException.class,
                () -> _exchange.bind(queueName, queueName, bindArguments2, true),
                "Queue can be bound when invalid selector expression is supplied as part of bind arguments");

        final RoutingResult<ServerMessage<?>> result2 = _exchange.route(testMessage, queueName, _instanceProperties);
        assertTrue(result2.hasRoutes(), "Message should be be possible to route using old binding");
    }
}
