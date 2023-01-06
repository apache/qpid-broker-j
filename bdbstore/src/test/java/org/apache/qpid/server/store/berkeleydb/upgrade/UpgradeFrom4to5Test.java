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
package org.apache.qpid.server.store.berkeleydb.upgrade;

import static org.apache.qpid.server.store.berkeleydb.BDBStoreUpgradeTestPreparer.NONEXCLUSIVE_WITH_ERRONEOUS_OWNER;
import static org.apache.qpid.server.store.berkeleydb.BDBStoreUpgradeTestPreparer.NON_DURABLE_QUEUE_NAME;
import static org.apache.qpid.server.store.berkeleydb.BDBStoreUpgradeTestPreparer.QUEUE_NAME;
import static org.apache.qpid.server.store.berkeleydb.BDBStoreUpgradeTestPreparer.QUEUE_WITH_DLQ_NAME;
import static org.apache.qpid.server.store.berkeleydb.BDBStoreUpgradeTestPreparer.SELECTOR_TOPIC_NAME;
import static org.apache.qpid.server.store.berkeleydb.BDBStoreUpgradeTestPreparer.TOPIC_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Transaction;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.filter.AMQPFilterTypes;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.store.berkeleydb.upgrade.UpgradeFrom4To5.BindingRecord;
import org.apache.qpid.server.store.berkeleydb.upgrade.UpgradeFrom4To5.BindingTuple;
import org.apache.qpid.server.store.berkeleydb.upgrade.UpgradeFrom4To5.MessageContentKey;
import org.apache.qpid.server.store.berkeleydb.upgrade.UpgradeFrom4To5.MessageContentKeyBinding;
import org.apache.qpid.server.store.berkeleydb.upgrade.UpgradeFrom4To5.QueueEntryKey;
import org.apache.qpid.server.store.berkeleydb.upgrade.UpgradeFrom4To5.QueueEntryKeyBinding;
import org.apache.qpid.server.store.berkeleydb.upgrade.UpgradeFrom4To5.QueueRecord;

public class UpgradeFrom4to5Test extends AbstractUpgradeTestCase
{
    private static final String DURABLE_SUBSCRIPTION_QUEUE_WITH_SELECTOR = "clientid:mySelectorDurSubName";
    private static final String DURABLE_SUBSCRIPTION_QUEUE = "clientid:myDurSubName";
    private static final String EXCHANGE_DB_NAME = "exchangeDb_v5";
    private static final String MESSAGE_META_DATA_DB_NAME = "messageMetaDataDb_v5";
    private static final String MESSAGE_CONTENT_DB_NAME = "messageContentDb_v5";
    private static final String DELIVERY_DB_NAME = "deliveryDb_v5";
    private static final String BINDING_DB_NAME = "queueBindingsDb_v5";

    @Override
    protected String getStoreDirectoryName()
    {
        return "bdbstore-v4";
    }

    @Test
    public void testPerformUpgradeWithHandlerAnsweringYes()
    {
        UpgradeFrom4To5 upgrade = new UpgradeFrom4To5();
        upgrade.performUpgrade(_environment, new StaticAnswerHandler(UpgradeInteractionResponse.YES), getVirtualHost());

        assertQueues(new HashSet<>(Arrays.asList(QUEUE_NAMES)));

        assertDatabaseRecordCount(DELIVERY_DB_NAME, TOTAL_MESSAGE_NUMBER);
        assertDatabaseRecordCount(MESSAGE_META_DATA_DB_NAME, TOTAL_MESSAGE_NUMBER);
        assertDatabaseRecordCount(EXCHANGE_DB_NAME, TOTAL_EXCHANGES);

        for (int i = 0; i < QUEUE_SIZES.length; i++)
        {
            assertQueueMessages(QUEUE_NAMES[i], QUEUE_SIZES[i]);
        }

        final List<BindingRecord> queueBindings = loadBindings();

        assertEquals(TOTAL_BINDINGS, queueBindings.size(), "Unxpected bindings size");
        assertBindingRecord(queueBindings, DURABLE_SUBSCRIPTION_QUEUE, "amq.topic", TOPIC_NAME, "");
        assertBindingRecord(queueBindings, DURABLE_SUBSCRIPTION_QUEUE_WITH_SELECTOR, "amq.topic", SELECTOR_TOPIC_NAME, "testprop='true'");
        assertBindingRecord(queueBindings, QUEUE_NAME, "amq.direct", QUEUE_NAME, null);
        assertBindingRecord(queueBindings, NON_DURABLE_QUEUE_NAME, "amq.direct", NON_DURABLE_QUEUE_NAME, null);
        assertBindingRecord(queueBindings, NONEXCLUSIVE_WITH_ERRONEOUS_OWNER, "amq.direct", NONEXCLUSIVE_WITH_ERRONEOUS_OWNER, null);

        assertQueueHasOwner(NONEXCLUSIVE_WITH_ERRONEOUS_OWNER, "misused-owner-as-description");

        assertContent();
    }

    @Test
    public void testPerformUpgradeWithHandlerAnsweringNo()
    {
        UpgradeFrom4To5 upgrade = new UpgradeFrom4To5();
        upgrade.performUpgrade(_environment, new StaticAnswerHandler(UpgradeInteractionResponse.NO), getVirtualHost());
        HashSet<String> queues = new HashSet<>(Arrays.asList(QUEUE_NAMES));
        assertTrue(queues.remove(NON_DURABLE_QUEUE_NAME), NON_DURABLE_QUEUE_NAME + " should be in the list of queues");

        assertQueues(queues);

        assertDatabaseRecordCount(DELIVERY_DB_NAME, 13);
        assertDatabaseRecordCount(MESSAGE_META_DATA_DB_NAME, 13);
        assertDatabaseRecordCount(EXCHANGE_DB_NAME, TOTAL_EXCHANGES);

        assertQueueMessages(DURABLE_SUBSCRIPTION_QUEUE, 1);
        assertQueueMessages(DURABLE_SUBSCRIPTION_QUEUE_WITH_SELECTOR, 1);
        assertQueueMessages(QUEUE_NAME, 10);
        assertQueueMessages(QUEUE_WITH_DLQ_NAME + "_DLQ", 1);

        final List<BindingRecord> queueBindings = loadBindings();

        assertEquals(TOTAL_BINDINGS - 2, queueBindings.size(), "Unexpected list size");
        assertBindingRecord(queueBindings, DURABLE_SUBSCRIPTION_QUEUE, "amq.topic", TOPIC_NAME, "");
        assertBindingRecord(queueBindings, DURABLE_SUBSCRIPTION_QUEUE_WITH_SELECTOR, "amq.topic",
                SELECTOR_TOPIC_NAME, "testprop='true'");
        assertBindingRecord(queueBindings, QUEUE_NAME, "amq.direct", QUEUE_NAME, null);

        assertQueueHasOwner(NONEXCLUSIVE_WITH_ERRONEOUS_OWNER, "misused-owner-as-description");

        assertContent();
    }

    private List<BindingRecord> loadBindings()
    {
        final BindingTuple bindingTuple = new BindingTuple();
        final List<BindingRecord> queueBindings = new ArrayList<>();
        CursorOperation databaseOperation = new CursorOperation()
        {

            @Override
            public void processEntry(Database sourceDatabase, Database targetDatabase, Transaction transaction,
                    DatabaseEntry key, DatabaseEntry value)
            {
                BindingRecord bindingRecord = bindingTuple.entryToObject(key);

                AMQShortString queueName = bindingRecord.getQueueName();
                AMQShortString exchangeName = bindingRecord.getExchangeName();
                AMQShortString routingKey = bindingRecord.getRoutingKey();
                FieldTable arguments = bindingRecord.getArguments();
                queueBindings.add(new BindingRecord(exchangeName, queueName, routingKey, arguments));
            }
        };
        new DatabaseTemplate(_environment, BINDING_DB_NAME, null).run(databaseOperation);
        return queueBindings;
    }

    private void assertBindingRecord(List<BindingRecord> queueBindings, String queueName, String exchangeName,
            String routingKey, String selectorKey)
    {
        BindingRecord record = null;
        for (BindingRecord bindingRecord : queueBindings)
        {
            if (bindingRecord.getQueueName().toString().equals(queueName)
                    && bindingRecord.getExchangeName().toString().equals(exchangeName))
            {
                record = bindingRecord;
                break;
            }
        }
        assertNotNull(record, "Binding is not found for queue " + queueName + " and exchange " + exchangeName);
        assertEquals(routingKey, record.getRoutingKey().toString(), "Unexpected routing key");

        if (selectorKey != null)
        {
            assertEquals(selectorKey, record.getArguments().get(AMQPFilterTypes.JMS_SELECTOR.getValue()),
                         "Unexpected selector key for " + queueName);
        }
    }

    private void assertQueueMessages(final String queueName, final int expectedQueueSize)
    {
        final Set<Long> messageIdsForQueue = assertDeliveriesForQueue(queueName, expectedQueueSize);

        assertMetadataForQueue(queueName, expectedQueueSize, messageIdsForQueue);

        assertContentForQueue(queueName, expectedQueueSize, messageIdsForQueue);
    }

    private Set<Long> assertDeliveriesForQueue(final String queueName, final int expectedQueueSize)
    {
        final QueueEntryKeyBinding queueEntryKeyBinding = new QueueEntryKeyBinding();
        final AtomicInteger deliveryCounter = new AtomicInteger();
        final Set<Long> messagesForQueue = new HashSet<>();

        CursorOperation deliveryDatabaseOperation = new CursorOperation()
        {
            @Override
            public void processEntry(Database sourceDatabase, Database targetDatabase, Transaction transaction,
                    DatabaseEntry key, DatabaseEntry value)
            {
                QueueEntryKey entryKey = queueEntryKeyBinding.entryToObject(key);
                String thisQueueName = entryKey.getQueueName().toString();
                if (thisQueueName.equals(queueName))
                {
                    deliveryCounter.incrementAndGet();
                    messagesForQueue.add(entryKey.getMessageId());
                }
            }
        };
        new DatabaseTemplate(_environment, DELIVERY_DB_NAME, null).run(deliveryDatabaseOperation);

        assertEquals(expectedQueueSize, deliveryCounter.get(),
                     "Unxpected number of entries in delivery db for queue " + queueName);

        return messagesForQueue;
    }

    private void assertMetadataForQueue(final String queueName, final int expectedQueueSize,
            final Set<Long> messageIdsForQueue)
    {
        final AtomicInteger metadataCounter = new AtomicInteger();
        CursorOperation databaseOperation = new CursorOperation()
        {

            @Override
            public void processEntry(Database sourceDatabase, Database targetDatabase, Transaction transaction,
                    DatabaseEntry key, DatabaseEntry value)
            {
                Long messageId = LongBinding.entryToLong(key);

                boolean messageIsForTheRightQueue = messageIdsForQueue.contains(messageId);
                if (messageIsForTheRightQueue)
                {
                    metadataCounter.incrementAndGet();
                }
            }
        };
        new DatabaseTemplate(_environment, MESSAGE_META_DATA_DB_NAME, null).run(databaseOperation);

        assertEquals(expectedQueueSize, metadataCounter.get(),
                     "Unxpected number of entries in metadata db for queue " + queueName);
    }

    private void assertContentForQueue(String queueName, int expectedQueueSize, final Set<Long> messageIdsForQueue)
    {
        final AtomicInteger contentCounter = new AtomicInteger();
        final MessageContentKeyBinding keyBinding = new MessageContentKeyBinding();
        CursorOperation cursorOperation = new CursorOperation()
        {
            private long _prevMsgId = -1;

            @Override
            public void processEntry(Database sourceDatabase, Database targetDatabase, Transaction transaction,
                    DatabaseEntry key, DatabaseEntry value)
            {
                MessageContentKey contentKey = keyBinding.entryToObject(key);
                long msgId = contentKey.getMessageId();

                if (_prevMsgId != msgId && messageIdsForQueue.contains(msgId))
                {
                    contentCounter.incrementAndGet();
                }

                _prevMsgId = msgId;
            }
        };
        new DatabaseTemplate(_environment, MESSAGE_CONTENT_DB_NAME, null).run(cursorOperation);

        assertEquals(expectedQueueSize, contentCounter.get(),
                     "Unxpected number of entries in content db for queue " + queueName);
    }

    private void assertQueues(Set<String> expectedQueueNames)
    {
        List<AMQShortString> durableSubNames = Collections.emptyList();
        final UpgradeFrom4To5.QueueRecordBinding binding = new UpgradeFrom4To5.QueueRecordBinding(durableSubNames);
        final Set<String> actualQueueNames = new HashSet<>();

        CursorOperation queueNameCollector = new CursorOperation()
        {

            @Override
            public void processEntry(Database sourceDatabase, Database targetDatabase, Transaction transaction,
                    DatabaseEntry key, DatabaseEntry value)
            {
                QueueRecord record = binding.entryToObject(value);
                String queueName = record.getNameShortString().toString();
                actualQueueNames.add(queueName);
            }
        };
        new DatabaseTemplate(_environment, "queueDb_v5", null).run(queueNameCollector);

        assertEquals(expectedQueueNames, actualQueueNames, "Unexpected queue names");
    }

    private void assertQueueHasOwner(String queueName, final String expectedOwner)
    {
        List<AMQShortString> durableSubNames = Collections.emptyList();
        final UpgradeFrom4To5.QueueRecordBinding binding = new UpgradeFrom4To5.QueueRecordBinding(durableSubNames);
        final AtomicReference<String> actualOwner = new AtomicReference<>();
        final AtomicBoolean foundQueue = new AtomicBoolean(false);

        CursorOperation queueNameCollector = new CursorOperation()
        {

            @Override
            public void processEntry(Database sourceDatabase, Database targetDatabase, Transaction transaction,
                    DatabaseEntry key, DatabaseEntry value)
            {
                QueueRecord record = binding.entryToObject(value);
                String queueName = record.getNameShortString().toString();
                if (queueName.equals(queueName))
                {
                    foundQueue.set(true);
                    actualOwner.set(AMQShortString.toString(record.getOwner()));
                }
            }
        };
        new DatabaseTemplate(_environment, "queueDb_v5", null).run(queueNameCollector);

        assertTrue(foundQueue.get(), "Could not find queue in database");
        assertEquals(expectedOwner, actualOwner.get(), "Queue has unexpected owner");
    }

    private void assertContent()
    {
        final UpgradeFrom4To5.ContentBinding contentBinding = new UpgradeFrom4To5.ContentBinding();
        CursorOperation contentCursorOperation = new CursorOperation()
        {

            @Override
            public void processEntry(Database sourceDatabase, Database targetDatabase, Transaction transaction, DatabaseEntry key,
                    DatabaseEntry value)
            {
                long id = LongBinding.entryToLong(key);
                assertTrue(id > 0, "Unexpected id");
                ByteBuffer content = contentBinding.entryToObject(value);
                assertNotNull(content, "Unexpected content");
            }
        };
        new DatabaseTemplate(_environment, MESSAGE_CONTENT_DB_NAME, null).run(contentCursorOperation);
    }
}
