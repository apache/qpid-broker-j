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
package org.apache.qpid.server.session;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.exchange.ExchangeDefaults;
import org.apache.qpid.server.message.MessageSender;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.PublishingLink;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.session.AMQPSession;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.test.utils.UnitTestBase;

@SuppressWarnings({"rawtypes"})
public class ProducerTest extends UnitTestBase
{
    private QueueManagingVirtualHost<?> _virtualHost;
    private Exchange<?> _exchange;
    private Queue<?> _queue;
    private PublishingLink _link;
    private MessageSender _sender;

    @BeforeEach
    public void setUp() throws Exception
    {
        _virtualHost = BrokerTestHelper.createVirtualHost("test", this);

        final Map<String,Object> attributes = new HashMap<>();
        attributes.put(Exchange.NAME, "test");
        attributes.put(Exchange.DURABLE, false);
        attributes.put(Exchange.TYPE, ExchangeDefaults.HEADERS_EXCHANGE_CLASS);
        _exchange = _virtualHost.createChild(Exchange.class, attributes);

        final Map<String,Object> queueAttributes = new HashMap<>();
        queueAttributes.put(Queue.NAME, "queue");
        queueAttributes.put(Queue.DURABLE, false);
        queueAttributes.put(Queue.TYPE, "standard");
        _queue = _virtualHost.createChild(Queue.class, queueAttributes);

        final AMQPConnection connection = mock(AMQPConnection.class);
        when(connection.getPrincipal()).thenReturn("test-principal");
        when(connection.getRemoteAddress()).thenReturn("test-remote-address");

        final AMQPSession session = mock(AMQPSession.class);
        when(session.getId()).thenReturn(UUID.randomUUID());
        when(session.getName()).thenReturn("test-session");
        when(session.getAMQPConnection()).thenReturn(connection);

        _link = mock(PublishingLink.class);
        when(_link.getName()).thenReturn("test-link");
        when(_link.getType()).thenReturn(PublishingLink.TYPE_LINK);

        _sender = mock(MessageSender.class);
    }

    @Test
    public void addAndRemoveLinkToExchange()
    {
        assertEquals(0, _exchange.getProducerCount());
        _exchange.linkAdded(_sender, _link);
        assertEquals(1, _exchange.getProducerCount());
        _exchange.linkRemoved(_sender, _link);
        assertEquals(0, _exchange.getProducerCount());
    }

    @Test
    public void addAndRemoveLinkToQueue()
    {
        assertEquals(0, _queue.getProducerCount());
        _queue.linkAdded(_sender, _link);
        assertEquals(1, _queue.getProducerCount());
        _queue.linkRemoved(_sender, _link);
        assertEquals(0, _queue.getProducerCount());
    }

    @Test
    public void closeExchange()
    {
        final Map<String,Object> attributes = new HashMap<>();
        attributes.put(Exchange.NAME, "temporary");
        attributes.put(Exchange.DURABLE, false);
        attributes.put(Exchange.TYPE, ExchangeDefaults.HEADERS_EXCHANGE_CLASS);

        final Exchange exchange = _virtualHost.createChild(Exchange.class, attributes);
        exchange.linkAdded(_sender, _link);
        exchange.close();

        assertEquals(0, exchange.getProducerCount());
    }

    @Test
    public void closeQueue()
    {
        final Map<String,Object> attributes = new HashMap<>();
        attributes.put(Queue.NAME, "temporary");
        attributes.put(Queue.DURABLE, false);
        attributes.put(Queue.TYPE, "standard");

        final Queue queue = _virtualHost.createChild(Queue.class, attributes);
        queue.linkAdded(_sender, _link);
        queue.close();

        assertEquals(0, queue.getProducerCount());
    }
}
