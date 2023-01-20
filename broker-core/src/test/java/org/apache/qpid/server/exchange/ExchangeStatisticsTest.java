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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.message.InstanceProperties;
import org.apache.qpid.server.message.RoutingResult;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.test.utils.UnitTestBase;

public class ExchangeStatisticsTest extends UnitTestBase
{
    private VirtualHost<?> _vhost;
    private Exchange<?> _exchange;

    @BeforeEach
    public void setUp() throws Exception
    {
        _vhost = BrokerTestHelper.createVirtualHost(getTestName(), this);
        final Map<String, Object> attributes = Map.of(Exchange.NAME, "test",
                Exchange.DURABLE, false,
                Exchange.TYPE, ExchangeDefaults.TOPIC_EXCHANGE_CLASS);
        _exchange = _vhost.createChild(Exchange.class, attributes);
        _exchange.open();
    }

    @Test
    public void getStatistics()
    {
        final Map<String, Object> statistics = _exchange.getStatistics();

        assertEquals(6, statistics.size());

        assertTrue(statistics.containsKey("bindingCount"));
        assertTrue(statistics.containsKey("bytesDropped"));
        assertTrue(statistics.containsKey("bytesIn"));
        assertTrue(statistics.containsKey("messagesDropped"));
        assertTrue(statistics.containsKey("messagesIn"));
        assertTrue(statistics.containsKey("producerCount"));

        assertEquals(0L, _exchange.getStatistics().get("bindingCount"));
        assertEquals(0L, _exchange.getStatistics().get("bytesDropped"));
        assertEquals(0L, _exchange.getStatistics().get("bytesIn"));
        assertEquals(0L, _exchange.getStatistics().get("messagesDropped"));
        assertEquals(0L, _exchange.getStatistics().get("messagesIn"));
        assertEquals(0L, _exchange.getStatistics().get("producerCount"));
    }

    @Test
    public void bindingCount()
    {
        final Queue<?> queue = _vhost.createChild(Queue.class, Map.of(Queue.NAME, getTestName() + "_queue"));

        _exchange.bind(queue.getName(), "test", Map.of(JMS_SELECTOR.toString(), "prop = True"), false);
        assertEquals(1L, _exchange.getStatistics().get("bindingCount"));

        _exchange.unbind(queue.getName(), "test");
        assertEquals(0L, _exchange.getStatistics().get("bindingCount"));

        queue.close();
    }

    @Test
    public void messageRouted()
    {
        final InstanceProperties instanceProperties = mock(InstanceProperties.class);
        final ServerMessage<?> matchingMessage = createTestMessage(Map.of("prop", true));
        final Queue<?> queue = _vhost.createChild(Queue.class, Map.of(Queue.NAME, getTestName() + "_queue"));

        boolean bind = _exchange.bind(queue.getName(), "test", Map.of(JMS_SELECTOR.toString(), "prop = True"), false);
        assertTrue(bind, "Bind operation should be successful");

        final RoutingResult<ServerMessage<?>> result = _exchange.route(matchingMessage, "test", instanceProperties);
        assertTrue(result.hasRoutes(), "Message with matching selector not routed to queue");

        assertEquals(1L, _exchange.getStatistics().get("bindingCount"));
        assertEquals(0L, _exchange.getStatistics().get("bytesDropped"));
        assertEquals(100L, _exchange.getStatistics().get("bytesIn"));
        assertEquals(1L, _exchange.getStatistics().get("messagesIn"));
        assertEquals(0L, _exchange.getStatistics().get("messagesDropped"));

        _exchange.resetStatistics();

        assertEquals(1L, _exchange.getStatistics().get("bindingCount"));
        assertEquals(0L, _exchange.getStatistics().get("bytesIn"));
        assertEquals(0L, _exchange.getStatistics().get("bytesDropped"));
        assertEquals(0L, _exchange.getStatistics().get("messagesIn"));
        assertEquals(0L, _exchange.getStatistics().get("messagesDropped"));

        queue.close();
    }

    @Test
    public void messageNotRouted()
    {
        final InstanceProperties instanceProperties = mock(InstanceProperties.class);
        final ServerMessage<?> unmatchingMessage = createTestMessage(Map.of("prop", false));
        final Queue<?> queue = _vhost.createChild(Queue.class, Map.of(Queue.NAME, getTestName() + "_queue"));

        boolean bind = _exchange.bind(queue.getName(), "test", Map.of(JMS_SELECTOR.toString(), "prop = True"), false);
        assertTrue(bind, "Bind operation should be successful");

        final RoutingResult<ServerMessage<?>> result = _exchange.route(unmatchingMessage, "test", instanceProperties);
        assertFalse(result.hasRoutes(), "Message without matching selector unexpectedly routed to queue");

        assertEquals(1L, _exchange.getStatistics().get("bindingCount"));
        assertEquals(100L, _exchange.getStatistics().get("bytesDropped"));
        assertEquals(100L, _exchange.getStatistics().get("bytesIn"));
        assertEquals(1L, _exchange.getStatistics().get("messagesIn"));
        assertEquals(1L, _exchange.getStatistics().get("messagesDropped"));

        _exchange.resetStatistics();

        assertEquals(1L, _exchange.getStatistics().get("bindingCount"));
        assertEquals(0L, _exchange.getStatistics().get("bytesIn"));
        assertEquals(0L, _exchange.getStatistics().get("bytesDropped"));
        assertEquals(0L, _exchange.getStatistics().get("messagesIn"));
        assertEquals(0L, _exchange.getStatistics().get("messagesDropped"));

        queue.close();
    }

    private ServerMessage<?> createTestMessage(final Map<String, Object> headerValues)
    {
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        headerValues.forEach((key, value) -> when(header.getHeader(key)).thenReturn(value));
        final ServerMessage<?> message = mock(ServerMessage.class);
        when(message.isResourceAcceptable(any(TransactionLogResource.class))).thenReturn(true);
        when(message.getMessageHeader()).thenReturn(header);
        when(message.getSizeIncludingHeader()).thenReturn(100L);
        return message;
    }
}
