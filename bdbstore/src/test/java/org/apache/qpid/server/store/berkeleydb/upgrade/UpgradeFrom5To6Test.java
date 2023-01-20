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

import static org.apache.qpid.server.store.berkeleydb.BDBStoreUpgradeTestPreparer.PRIORITY_QUEUE_NAME;
import static org.apache.qpid.server.store.berkeleydb.BDBStoreUpgradeTestPreparer.QUEUE_WITH_DLQ_NAME;
import static org.apache.qpid.server.store.berkeleydb.upgrade.UpgradeFrom5To6.CONFIGURED_OBJECTS_DB_NAME;
import static org.apache.qpid.server.store.berkeleydb.upgrade.UpgradeFrom5To6.NEW_CONTENT_DB_NAME;
import static org.apache.qpid.server.store.berkeleydb.upgrade.UpgradeFrom5To6.NEW_DELIVERY_DB_NAME;
import static org.apache.qpid.server.store.berkeleydb.upgrade.UpgradeFrom5To6.NEW_METADATA_DB_NAME;
import static org.apache.qpid.server.store.berkeleydb.upgrade.UpgradeFrom5To6.NEW_XID_DB_NAME;
import static org.apache.qpid.server.store.berkeleydb.upgrade.UpgradeFrom5To6.OLD_CONTENT_DB_NAME;
import static org.apache.qpid.server.store.berkeleydb.upgrade.UpgradeFrom5To6.OLD_XID_DB_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.Transaction;

import org.junit.jupiter.api.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.Binding;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.LifetimePolicy;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.UUIDGenerator;
import org.apache.qpid.server.queue.QueueArgumentsConverter;
import org.apache.qpid.server.store.berkeleydb.tuple.XidBinding;
import org.apache.qpid.server.store.berkeleydb.upgrade.UpgradeFrom5To6.CompoundKey;
import org.apache.qpid.server.store.berkeleydb.upgrade.UpgradeFrom5To6.CompoundKeyBinding;
import org.apache.qpid.server.store.berkeleydb.upgrade.UpgradeFrom5To6.ConfiguredObjectBinding;
import org.apache.qpid.server.store.berkeleydb.upgrade.UpgradeFrom5To6.NewDataBinding;
import org.apache.qpid.server.store.berkeleydb.upgrade.UpgradeFrom5To6.NewPreparedTransaction;
import org.apache.qpid.server.store.berkeleydb.upgrade.UpgradeFrom5To6.NewPreparedTransactionBinding;
import org.apache.qpid.server.store.berkeleydb.upgrade.UpgradeFrom5To6.NewQueueEntryBinding;
import org.apache.qpid.server.store.berkeleydb.upgrade.UpgradeFrom5To6.NewQueueEntryKey;
import org.apache.qpid.server.store.berkeleydb.upgrade.UpgradeFrom5To6.NewRecordImpl;
import org.apache.qpid.server.store.berkeleydb.upgrade.UpgradeFrom5To6.OldPreparedTransaction;
import org.apache.qpid.server.store.berkeleydb.upgrade.UpgradeFrom5To6.OldPreparedTransactionBinding;
import org.apache.qpid.server.store.berkeleydb.upgrade.UpgradeFrom5To6.OldRecordImpl;
import org.apache.qpid.server.store.berkeleydb.upgrade.UpgradeFrom5To6.UpgradeConfiguredObjectRecord;
import org.apache.qpid.server.store.berkeleydb.upgrade.UpgradeFrom5To6.UpgradeUUIDBinding;
import org.apache.qpid.server.txn.Xid;

public class UpgradeFrom5To6Test extends AbstractUpgradeTestCase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(UpgradeFrom5To6Test.class);
    private static final String ARGUMENTS = "arguments";

    @Override
    protected String getStoreDirectoryName()
    {
        return "bdbstore-v5";
    }

    @Test
    public void testPerformUpgrade() throws Exception
    {
        UpgradeFrom5To6 upgrade = new UpgradeFrom5To6();
        upgrade.performUpgrade(_environment, UpgradeInteractionHandler.DEFAULT_HANDLER, getVirtualHost());

        assertDatabaseRecordCounts();
        assertContent();

        assertConfiguredObjects();
        assertQueueEntries();
    }

    @Test
    public void testPerformUpgradeWithMissingMessageChunkKeepsIncompleteMessage() throws Exception
    {
        corruptDatabase();

        UpgradeFrom5To6 upgrade = new UpgradeFrom5To6();
        upgrade.performUpgrade(_environment, new StaticAnswerHandler(UpgradeInteractionResponse.YES), getVirtualHost());

        assertDatabaseRecordCounts();

        assertConfiguredObjects();
        assertQueueEntries();
    }

    @Test
    public void testPerformUpgradeWithMissingMessageChunkDiscardsIncompleteMessage() throws Exception
    {
        corruptDatabase();

        UpgradeFrom5To6 upgrade = new UpgradeFrom5To6();

        UpgradeInteractionHandler discardMessageInteractionHandler = new StaticAnswerHandler(UpgradeInteractionResponse.NO);

        upgrade.performUpgrade(_environment, discardMessageInteractionHandler, getVirtualHost());

        assertDatabaseRecordCount(NEW_METADATA_DB_NAME, 12);
        assertDatabaseRecordCount(NEW_CONTENT_DB_NAME, 12);

        assertConfiguredObjects();
        assertQueueEntries();
    }

    @Test
    public void testPerformXidUpgrade()
    {
        File storeLocation = new File(TMP_FOLDER, getTestName());
        storeLocation.mkdirs();
        Environment environment = createEnvironment(storeLocation);
        try
        {
            populateOldXidEntries(environment);
            UpgradeFrom5To6 upgrade = new UpgradeFrom5To6();
            upgrade.performUpgrade(environment, UpgradeInteractionHandler.DEFAULT_HANDLER, getVirtualHost());
            assertXidEntries(environment);
        }
        finally
        {
            try
            {
                environment.close();
            }
            finally
            {
                deleteDirectoryIfExists(storeLocation);
            }

        }
    }

    private void assertXidEntries(Environment environment)
    {
        final DatabaseEntry value = new DatabaseEntry();
        final DatabaseEntry key = getXidKey();
        new DatabaseTemplate(environment, NEW_XID_DB_NAME, null).run((xidDatabase, nullDatabase, transaction) -> xidDatabase.get(null, key, value, LockMode.DEFAULT));
        NewPreparedTransactionBinding newBinding = new NewPreparedTransactionBinding();
        NewPreparedTransaction newTransaction = newBinding.entryToObject(value);
        NewRecordImpl[] newEnqueues = newTransaction.getEnqueues();
        NewRecordImpl[] newDequeues = newTransaction.getDequeues();
        assertEquals(1, newEnqueues.length, "Unexpected new enqueues number");
        NewRecordImpl enqueue = newEnqueues[0];
        assertEquals(UUIDGenerator.generateQueueUUID("TEST1", getVirtualHost().getName()), enqueue.getId(),
                "Unexpected queue id");
        assertEquals(1, enqueue.getMessageNumber(), "Unexpected message id");
        assertEquals(1, newDequeues.length, "Unexpected new dequeues number");
        NewRecordImpl dequeue = newDequeues[0];
        assertEquals(UUIDGenerator.generateQueueUUID("TEST2", getVirtualHost().getName()), dequeue.getId(),
                "Unexpected queue id");
        assertEquals(2, dequeue.getMessageNumber(), "Unexpected message id");
    }

    private void populateOldXidEntries(Environment environment)
    {

        final DatabaseEntry value = new DatabaseEntry();
        OldRecordImpl[] enqueues = { new OldRecordImpl("TEST1", 1) };
        OldRecordImpl[] dequeues = { new OldRecordImpl("TEST2", 2) };
        OldPreparedTransaction oldPreparedTransaction = new OldPreparedTransaction(enqueues, dequeues);
        OldPreparedTransactionBinding oldPreparedTransactionBinding = new OldPreparedTransactionBinding();
        oldPreparedTransactionBinding.objectToEntry(oldPreparedTransaction, value);

        final DatabaseEntry key = getXidKey();
        new DatabaseTemplate(environment, OLD_XID_DB_NAME, null).run((xidDatabase, nullDatabase, transaction) -> xidDatabase.put(null, key, value));
    }

    protected DatabaseEntry getXidKey()
    {
        final DatabaseEntry value = new DatabaseEntry();
        byte[] globalId = { 1 };
        byte[] branchId = { 2 };
        Xid xid = new Xid(1L, globalId, branchId);
        XidBinding xidBinding = XidBinding.getInstance();
        xidBinding.objectToEntry(xid, value);
        return value;
    }

    private void assertQueueEntries()
    {
        final Map<UUID, UpgradeConfiguredObjectRecord> configuredObjects = loadConfiguredObjects();
        final NewQueueEntryBinding newBinding = new NewQueueEntryBinding();
        CursorOperation cursorOperation = new CursorOperation()
        {

            @Override
            public void processEntry(Database sourceDatabase, Database targetDatabase, Transaction transaction,
                    DatabaseEntry key, DatabaseEntry value)
            {
                NewQueueEntryKey newEntryRecord = newBinding.entryToObject(key);
                assertTrue(configuredObjects.containsKey(newEntryRecord.getQueueId()), "Unexpected queue id");
            }
        };
        new DatabaseTemplate(_environment, NEW_DELIVERY_DB_NAME, null).run(cursorOperation);
    }

    /**
     * modify the chunk offset of a message to be wrong, so we can test logic
     * that preserves incomplete messages
     */
    private void corruptDatabase()
    {
        CursorOperation cursorOperation = new CursorOperation()
        {

            @Override
            public void processEntry(Database sourceDatabase, Database targetDatabase, Transaction transaction,
                    DatabaseEntry key, DatabaseEntry value)
            {
                CompoundKeyBinding binding = new CompoundKeyBinding();
                CompoundKey originalCompoundKey = binding.entryToObject(key);
                int corruptedOffset = originalCompoundKey.getOffset() + 2;
                CompoundKey corruptedCompoundKey = new CompoundKey(originalCompoundKey.getMessageId(), corruptedOffset);
                DatabaseEntry newKey = new DatabaseEntry();
                binding.objectToEntry(corruptedCompoundKey, newKey);

                LOGGER.info("Deliberately corrupted message id " + originalCompoundKey.getMessageId()
                        + ", changed offset from " + originalCompoundKey.getOffset() + " to "
                        + corruptedCompoundKey.getOffset());

                deleteCurrent();
                sourceDatabase.put(transaction, newKey, value);

                abort();
            }
        };

        Transaction transaction = _environment.beginTransaction(null, null);
        new DatabaseTemplate(_environment, OLD_CONTENT_DB_NAME, transaction).run(cursorOperation);
        transaction.commit();
    }

    private void assertDatabaseRecordCounts()
    {
        assertDatabaseRecordCount(CONFIGURED_OBJECTS_DB_NAME, 21);
        assertDatabaseRecordCount(NEW_DELIVERY_DB_NAME, 13);

        assertDatabaseRecordCount(NEW_METADATA_DB_NAME, 13);
        assertDatabaseRecordCount(NEW_CONTENT_DB_NAME, 13);
    }

    private void assertConfiguredObjects() throws Exception
    {
        Map<UUID, UpgradeConfiguredObjectRecord> configuredObjects = loadConfiguredObjects();
        assertEquals(21, configuredObjects.size(), "Unexpected number of configured objects");

        Set<Map<String, Object>> expected = new HashSet<>(12);
        List<UUID> expectedBindingIDs = new ArrayList<>();

        expected.add(createExpectedQueueMap("myUpgradeQueue", Boolean.FALSE, null, null));
        expected.add(createExpectedQueueMap("clientid:mySelectorDurSubName", Boolean.TRUE, "clientid", null));
        expected.add(createExpectedQueueMap("clientid:myDurSubName", Boolean.TRUE, "clientid", null));

        final Map<String, Object> queueWithOwnerArguments = new HashMap<>();
        queueWithOwnerArguments.put(QueueArgumentsConverter.X_QPID_PRIORITIES, 10);
        queueWithOwnerArguments.put(QueueArgumentsConverter.X_QPID_DESCRIPTION, "misused-owner-as-description");
        expected.add(createExpectedQueueMap("nonexclusive-with-erroneous-owner", Boolean.FALSE, null,queueWithOwnerArguments));

        final Map<String, Object> priorityQueueArguments = new HashMap<>();
        priorityQueueArguments.put(QueueArgumentsConverter.X_QPID_PRIORITIES, 10);
        expected.add(createExpectedQueueMap(PRIORITY_QUEUE_NAME, Boolean.FALSE, null, priorityQueueArguments));

        final Map<String, Object> queueWithDLQArguments = new HashMap<>();
        queueWithDLQArguments.put("x-qpid-dlq-enabled", true);
        queueWithDLQArguments.put("x-qpid-maximum-delivery-count", 2);
        expected.add(createExpectedQueueMap(QUEUE_WITH_DLQ_NAME, Boolean.FALSE, null, queueWithDLQArguments));

        final Map<String, Object> dlqArguments = new HashMap<>();
        dlqArguments.put("x-qpid-dlq-enabled", false);
        dlqArguments.put("x-qpid-maximum-delivery-count", 0);
        expected.add(createExpectedQueueMap(QUEUE_WITH_DLQ_NAME + "_DLQ", Boolean.FALSE, null, dlqArguments));
        expected.add(createExpectedExchangeMap(QUEUE_WITH_DLQ_NAME + "_DLE", "fanout"));

        expected.add(createExpectedQueueBindingMapAndID("myUpgradeQueue","myUpgradeQueue", "<<default>>", null, expectedBindingIDs));
        expected.add(createExpectedQueueBindingMapAndID("myUpgradeQueue", "myUpgradeQueue", "amq.direct", null, expectedBindingIDs));
        expected.add(createExpectedQueueBindingMapAndID("clientid:myDurSubName", "myUpgradeTopic", "amq.topic",
                Collections.singletonMap("x-filter-jms-selector", ""), expectedBindingIDs));
        expected.add(createExpectedQueueBindingMapAndID("clientid:mySelectorDurSubName", "mySelectorUpgradeTopic", "amq.topic",
                Collections.singletonMap("x-filter-jms-selector", "testprop='true'"), expectedBindingIDs));
        expected.add(createExpectedQueueBindingMapAndID("clientid:myDurSubName", "clientid:myDurSubName", "<<default>>", null, expectedBindingIDs));
        expected.add(createExpectedQueueBindingMapAndID("clientid:mySelectorDurSubName", "clientid:mySelectorDurSubName", "<<default>>", null, expectedBindingIDs));
        expected.add(createExpectedQueueBindingMapAndID("nonexclusive-with-erroneous-owner", "nonexclusive-with-erroneous-owner", "amq.direct", null, expectedBindingIDs));
        expected.add(createExpectedQueueBindingMapAndID("nonexclusive-with-erroneous-owner","nonexclusive-with-erroneous-owner", "<<default>>", null, expectedBindingIDs));

        expected.add(createExpectedQueueBindingMapAndID(PRIORITY_QUEUE_NAME, PRIORITY_QUEUE_NAME, "<<default>>", null, expectedBindingIDs));
        expected.add(createExpectedQueueBindingMapAndID(PRIORITY_QUEUE_NAME, PRIORITY_QUEUE_NAME, "amq.direct", null, expectedBindingIDs));

        expected.add(createExpectedQueueBindingMapAndID(QUEUE_WITH_DLQ_NAME, QUEUE_WITH_DLQ_NAME, "<<default>>", null, expectedBindingIDs));
        expected.add(createExpectedQueueBindingMapAndID(QUEUE_WITH_DLQ_NAME, QUEUE_WITH_DLQ_NAME, "amq.direct", null, expectedBindingIDs));
        expected.add(createExpectedQueueBindingMapAndID(QUEUE_WITH_DLQ_NAME + "_DLQ", "dlq", QUEUE_WITH_DLQ_NAME + "_DLE", null, expectedBindingIDs));

        Set<String> expectedTypes = new HashSet<>();
        expectedTypes.add(Queue.class.getName());
        expectedTypes.add(Exchange.class.getName());
        expectedTypes.add(Binding.class.getName());
        MapJsonSerializer jsonSerializer = new MapJsonSerializer();
        for (Entry<UUID, UpgradeConfiguredObjectRecord> entry : configuredObjects.entrySet())
        {
            UpgradeConfiguredObjectRecord object = entry.getValue();
            Map<String, Object> deserialized = jsonSerializer.deserialize(object.getAttributes());

            assertTrue(expected.remove(deserialized),
                    "Unexpected entry in a store - json [" + object.getAttributes() + "], map [" + deserialized + "]");
            String type = object.getType();
            assertTrue(expectedTypes.contains(type), "Unexpected type:" + type);
            UUID key = entry.getKey();

            assertNotNull(key, "Key cannot be null");

            if (type.equals(Exchange.class.getName()))
            {
                String exchangeName = (String) deserialized.get(Exchange.NAME);
                assertNotNull(exchangeName);
                assertEquals(key, UUIDGenerator.generateExchangeUUID(exchangeName, getVirtualHost().getName()),
                        "Unexpected key");
            }
            else if (type.equals(Queue.class.getName()))
            {
                String queueName = (String) deserialized.get(Queue.NAME);
                assertNotNull(queueName);
                assertEquals(key, UUIDGenerator.generateQueueUUID(queueName, getVirtualHost().getName()),
                        "Unexpected key");
            }
            else if (type.equals(Binding.class.getName()))
            {
                assertTrue(expectedBindingIDs.remove(key), "Unexpected binding id");
            }
        }

        assertTrue(expected.isEmpty(), "Not all expected configured objects found:" + expected);
        assertTrue(expectedBindingIDs.isEmpty(), "Not all expected bindings found:" + expectedBindingIDs);
    }

    private Map<String, Object> createExpectedQueueBindingMapAndID(String queue, String bindingName, String exchangeName, Map<String, String> argumentMap, List<UUID> expectedBindingIDs)
    {
        final Map<String, Object> expectedQueueBinding = new HashMap<>();
        expectedQueueBinding.put("queue", UUIDGenerator.generateQueueUUID(queue, getVirtualHost().getName()).toString());
        expectedQueueBinding.put("name", bindingName);
        expectedQueueBinding.put("exchange", UUIDGenerator.generateExchangeUUID(exchangeName, getVirtualHost().getName()).toString());
        if (argumentMap != null)
        {
            expectedQueueBinding.put("arguments", argumentMap);
        }

        expectedBindingIDs.add(UUIDGenerator.generateBindingUUID(exchangeName, queue, bindingName, getVirtualHost().getName()));

        return expectedQueueBinding;
    }

    private Map<String, Object> createExpectedQueueMap(String name, boolean exclusiveFlag, String owner, Map<String, Object> argumentMap)
    {
        final Map<String, Object> expectedQueueEntry = new HashMap<>();
        expectedQueueEntry.put(Queue.NAME, name);
        expectedQueueEntry.put(Queue.EXCLUSIVE, exclusiveFlag);
        expectedQueueEntry.put(Queue.OWNER, owner);
        if (argumentMap != null)
        {
            expectedQueueEntry.put(ARGUMENTS, argumentMap);
        }
        return expectedQueueEntry;
    }

    private Map<String, Object> createExpectedExchangeMap(String name, String type)
    {
        final Map<String, Object> expectedExchnageEntry = new HashMap<>();
        expectedExchnageEntry.put(Exchange.NAME, name);
        expectedExchnageEntry.put(Exchange.TYPE, type);
        expectedExchnageEntry.put(Exchange.LIFETIME_POLICY, LifetimePolicy.PERMANENT.name());
        return expectedExchnageEntry;
    }

    private Map<UUID, UpgradeConfiguredObjectRecord> loadConfiguredObjects()
    {
        final Map<UUID, UpgradeConfiguredObjectRecord> configuredObjectsRecords = new HashMap<>();
        final ConfiguredObjectBinding binding = new ConfiguredObjectBinding();
        final UpgradeUUIDBinding uuidBinding = new UpgradeUUIDBinding();
        CursorOperation configuredObjectsCursor = new CursorOperation()
        {
            @Override
            public void processEntry(Database sourceDatabase, Database targetDatabase, Transaction transaction,
                    DatabaseEntry key, DatabaseEntry value)
            {
                UUID id = uuidBinding.entryToObject(key);
                UpgradeConfiguredObjectRecord object = binding.entryToObject(value);
                configuredObjectsRecords.put(id, object);
            }
        };
        new DatabaseTemplate(_environment, CONFIGURED_OBJECTS_DB_NAME, null).run(configuredObjectsCursor);
        return configuredObjectsRecords;
    }

    private void assertContent()
    {
        final NewDataBinding contentBinding = new NewDataBinding();
        CursorOperation contentCursorOperation = new CursorOperation()
        {

            @Override
            public void processEntry(Database sourceDatabase, Database targetDatabase, Transaction transaction,
                    DatabaseEntry key, DatabaseEntry value)
            {
                long id = LongBinding.entryToLong(key);
                assertTrue(id > 0, "Unexpected id");
                byte[] content = contentBinding.entryToObject(value);
                assertNotNull(content, "Unexpected content");
            }
        };
        new DatabaseTemplate(_environment, NEW_CONTENT_DB_NAME, null).run(contentCursorOperation);
    }
}
