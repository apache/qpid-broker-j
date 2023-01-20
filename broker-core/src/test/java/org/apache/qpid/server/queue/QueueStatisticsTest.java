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

import static org.apache.qpid.server.filter.AMQPFilterTypes.JMS_SELECTOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.exchange.ExchangeDefaults;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.test.utils.UnitTestBase;

public class QueueStatisticsTest extends UnitTestBase
{
    private QueueManagingVirtualHost<?> _vhost;
    private Queue<?> _queue;

    @BeforeEach
    public void setUp() throws Exception
    {
        _vhost = BrokerTestHelper.createVirtualHost(getClass().getName(), this);
        final Map<String,Object> attributes = Map.of(Queue.NAME, "queue",
                Queue.OWNER, "owner");
        _queue = _vhost.createChild(Queue.class, attributes);
    }

    @Test
    public void getStatistics()
    {
        final Map<String, Object> statistics = _queue.getStatistics();

        assertEquals(27, statistics.size());

        assertTrue(statistics.containsKey("availableBytes"));
        assertTrue(statistics.containsKey("availableBytesHighWatermark"));
        assertTrue(statistics.containsKey("availableMessages"));
        assertTrue(statistics.containsKey("availableMessagesHighWatermark"));
        assertTrue(statistics.containsKey("bindingCount"));
        assertTrue(statistics.containsKey("consumerCount"));
        assertTrue(statistics.containsKey("consumerCountWithCredit"));
        assertTrue(statistics.containsKey("producerCount"));
        assertTrue(statistics.containsKey("oldestMessageAge"));
        assertTrue(statistics.containsKey("persistentDequeuedBytes"));
        assertTrue(statistics.containsKey("persistentEnqueuedBytes"));
        assertTrue(statistics.containsKey("persistentDequeuedMessages"));
        assertTrue(statistics.containsKey("persistentEnqueuedMessages"));
        assertTrue(statistics.containsKey("queueDepthBytes"));
        assertTrue(statistics.containsKey("queueDepthBytesHighWatermark"));
        assertTrue(statistics.containsKey("queueDepthMessages"));
        assertTrue(statistics.containsKey("queueDepthMessagesHighWatermark"));
        assertTrue(statistics.containsKey("totalDequeuedBytes"));
        assertTrue(statistics.containsKey("totalDequeuedMessages"));
        assertTrue(statistics.containsKey("totalMalformedBytes"));
        assertTrue(statistics.containsKey("totalMalformedMessages"));
        assertTrue(statistics.containsKey("totalEnqueuedBytes"));
        assertTrue(statistics.containsKey("totalEnqueuedMessages"));
        assertTrue(statistics.containsKey("totalExpiredBytes"));
        assertTrue(statistics.containsKey("totalExpiredMessages"));
        assertTrue(statistics.containsKey("unacknowledgedBytes"));
        assertTrue(statistics.containsKey("unacknowledgedMessages"));

        assertEquals(0L, statistics.get("availableBytes"));
        assertEquals(0L, statistics.get("availableBytesHighWatermark"));
        assertEquals(0, statistics.get("availableMessages"));
        assertEquals(0, statistics.get("availableMessagesHighWatermark"));
        assertEquals(0, statistics.get("bindingCount"));
        assertEquals(0, statistics.get("consumerCount"));
        assertEquals(0, statistics.get("consumerCountWithCredit"));
        assertEquals(0L, statistics.get("producerCount"));
        assertEquals(0L, statistics.get("oldestMessageAge"));
        assertEquals(0L, statistics.get("persistentDequeuedBytes"));
        assertEquals(0L, statistics.get("persistentEnqueuedBytes"));
        assertEquals(0L, statistics.get("persistentDequeuedMessages"));
        assertEquals(0L, statistics.get("persistentEnqueuedMessages"));
        assertEquals(0L, statistics.get("queueDepthBytes"));
        assertEquals(0L, statistics.get("queueDepthBytesHighWatermark"));
        assertEquals(0, statistics.get("queueDepthMessages"));
        assertEquals(0, statistics.get("queueDepthMessagesHighWatermark"));
        assertEquals(0L, statistics.get("totalDequeuedBytes"));
        assertEquals(0L, statistics.get("totalDequeuedMessages"));
        assertEquals(0L, statistics.get("totalMalformedBytes"));
        assertEquals(0L, statistics.get("totalMalformedMessages"));
        assertEquals(0L, statistics.get("totalEnqueuedBytes"));
        assertEquals(0L, statistics.get("totalEnqueuedMessages"));
        assertEquals(0L, statistics.get("totalExpiredBytes"));
        assertEquals(0L, statistics.get("totalExpiredMessages"));
        assertEquals(0L, statistics.get("unacknowledgedBytes"));
        assertEquals(0L, statistics.get("unacknowledgedMessages"));
    }

    @Test
    public void bindingCount()
    {
        assertEquals(0, _queue.getStatistics().get("bindingCount"));

        final Exchange<?> exchange = _vhost.createChild(Exchange.class, Map.of(Exchange.NAME, "exchange",
                Exchange.TYPE, ExchangeDefaults.TOPIC_EXCHANGE_CLASS));
        exchange.bind(_queue.getName(), "test", Map.of(JMS_SELECTOR.toString(), "prop = True"), false);
        assertEquals(1, _queue.getStatistics().get("bindingCount"));

        exchange.unbind(_queue.getName(), "test");
        assertEquals(0, _queue.getStatistics().get("bindingCount"));

        exchange.close();
    }

    @Test
    public void resetStatistics()
    {
        final ServerMessage<?> message = BrokerTestHelper.createMessage(0L);
        _queue.enqueue(message, null, null);

        Map<String, Object> statistics = _queue.getStatistics();

        assertEquals(100L, statistics.get("availableBytes"));
        assertEquals(100L, statistics.get("availableBytesHighWatermark"));
        assertEquals(1, statistics.get("availableMessages"));
        assertEquals(1, statistics.get("availableMessagesHighWatermark"));
        assertEquals(0, statistics.get("bindingCount"));
        assertEquals(0, statistics.get("consumerCount"));
        assertEquals(0, statistics.get("consumerCountWithCredit"));
        assertEquals(0L, statistics.get("persistentDequeuedBytes"));
        assertEquals(0L, statistics.get("persistentEnqueuedBytes"));
        assertEquals(0L, statistics.get("persistentDequeuedMessages"));
        assertEquals(0L, statistics.get("persistentEnqueuedMessages"));
        assertEquals(100L, statistics.get("queueDepthBytes"));
        assertEquals(100L, statistics.get("queueDepthBytesHighWatermark"));
        assertEquals(1, statistics.get("queueDepthMessages"));
        assertEquals(1, statistics.get("queueDepthMessagesHighWatermark"));
        assertEquals(0L, statistics.get("totalDequeuedBytes"));
        assertEquals(0L, statistics.get("totalDequeuedMessages"));
        assertEquals(0L, statistics.get("totalMalformedBytes"));
        assertEquals(0L, statistics.get("totalMalformedMessages"));
        assertEquals(100L, statistics.get("totalEnqueuedBytes"));
        assertEquals(1L, statistics.get("totalEnqueuedMessages"));
        assertEquals(0L, statistics.get("totalExpiredBytes"));
        assertEquals(0L, statistics.get("totalExpiredMessages"));
        assertEquals(0L, statistics.get("unacknowledgedBytes"));
        assertEquals(0L, statistics.get("unacknowledgedMessages"));

        _queue.resetStatistics();
        statistics = _queue.getStatistics();

        assertEquals(100L, statistics.get("availableBytes"));
        assertEquals(100L, statistics.get("availableBytesHighWatermark"));
        assertEquals(1, statistics.get("availableMessages"));
        assertEquals(1, statistics.get("availableMessagesHighWatermark"));
        assertEquals(0, statistics.get("bindingCount"));
        assertEquals(0, statistics.get("consumerCount"));
        assertEquals(0, statistics.get("consumerCountWithCredit"));
        assertEquals(0L, statistics.get("persistentDequeuedBytes"));
        assertEquals(0L, statistics.get("persistentEnqueuedBytes"));
        assertEquals(0L, statistics.get("persistentDequeuedMessages"));
        assertEquals(0L, statistics.get("persistentEnqueuedMessages"));
        assertEquals(100L, statistics.get("queueDepthBytes"));
        assertEquals(100L, statistics.get("queueDepthBytesHighWatermark"));
        assertEquals(1, statistics.get("queueDepthMessages"));
        assertEquals(1, statistics.get("queueDepthMessagesHighWatermark"));
        assertEquals(0L, statistics.get("totalDequeuedBytes"));
        assertEquals(0L, statistics.get("totalDequeuedMessages"));
        assertEquals(0L, statistics.get("totalMalformedBytes"));
        assertEquals(0L, statistics.get("totalMalformedMessages"));
        assertEquals(0L, statistics.get("totalEnqueuedBytes"));
        assertEquals(0L, statistics.get("totalEnqueuedMessages"));
        assertEquals(0L, statistics.get("totalExpiredBytes"));
        assertEquals(0L, statistics.get("totalExpiredMessages"));
        assertEquals(0L, statistics.get("unacknowledgedBytes"));
        assertEquals(0L, statistics.get("unacknowledgedMessages"));
    }
}
