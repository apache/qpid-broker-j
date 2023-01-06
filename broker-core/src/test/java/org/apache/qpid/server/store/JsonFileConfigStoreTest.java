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
package org.apache.qpid.server.store;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.mockito.ArgumentMatcher;
import org.mockito.InOrder;

import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObjectFactory;
import org.apache.qpid.server.model.ConfiguredObjectFactoryImpl;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.store.handler.ConfiguredObjectRecordHandler;
import org.apache.qpid.server.util.FileUtils;
import org.apache.qpid.server.util.ServerScopedRuntimeException;
import org.apache.qpid.server.virtualhostnode.JsonVirtualHostNode;
import org.apache.qpid.test.utils.TestFileUtils;
import org.apache.qpid.test.utils.UnitTestBase;

public class JsonFileConfigStoreTest extends UnitTestBase
{
    private static final UUID ANY_UUID = randomUUID();
    private static final Map<String, Object> ANY_MAP = new HashMap<>();
    private static final String VIRTUAL_HOST_TYPE = "VirtualHost";

    private JsonFileConfigStore _store;
    private JsonVirtualHostNode<?> _parent;
    private File _storeLocation;
    private ConfiguredObjectRecordHandler _handler;
    private ConfiguredObjectRecord _rootRecord;

    @BeforeEach
    public void setUp() throws Exception
    {
        final ConfiguredObjectFactory factory = new ConfiguredObjectFactoryImpl(BrokerModel.getInstance());
        _parent = mock(JsonVirtualHostNode.class);
        _storeLocation = TestFileUtils.createTestDirectory("json", true);
        when(_parent.getName()).thenReturn(getTestName());
        when(_parent.getObjectFactory()).thenReturn(factory);
        when(_parent.getModel()).thenReturn(factory.getModel());
        when(_parent.getStorePath()).thenReturn(_storeLocation.getAbsolutePath());
        _store = new JsonFileConfigStore(VirtualHost.class);
        _handler = mock(ConfiguredObjectRecordHandler.class);
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        FileUtils.delete(_storeLocation, true);
    }

    @Test
    public void testNoStorePath()
    {
        when(_parent.getStorePath()).thenReturn(null);
        assertThrows(ServerScopedRuntimeException.class, () -> _store.init(_parent),
                "Store should not successfully configure if there is no path set");
    }


    @Test
    public void testInvalidStorePath()
    {
        final String unwritablePath = System.getProperty("file.separator");
        assumeFalse(new File(unwritablePath).canWrite());
        when(_parent.getStorePath()).thenReturn(unwritablePath);
        assertThrows(ServerScopedRuntimeException.class, () -> _store.init(_parent),
                "Store should not successfully configure if there is an invalid path set");
    }

    @Test
    public void testVisitEmptyStore()
    {
        _store.init(_parent);
        _store.openConfigurationStore(_handler);

        final InOrder inorder = inOrder(_handler);
        inorder.verify(_handler,times(0)).handle(any(ConfiguredObjectRecord.class));

        _store.closeConfigurationStore();
    }

    @Test
    public void testInsertAndUpdateTopLevelObject()
    {
        _store.init(_parent);
        _store.openConfigurationStore(mock(ConfiguredObjectRecordHandler.class));
        createRootRecord();
        _store.closeConfigurationStore();

        _store.init(_parent);
        _store.openConfigurationStore(record -> { });
        final Map<String, Object> newAttributes = new HashMap<>(_rootRecord.getAttributes());
        newAttributes.put("attributeName", "attributeValue");
        _store.update(false, new ConfiguredObjectRecordImpl(_rootRecord.getId(), _rootRecord.getType(), newAttributes));
        _store.closeConfigurationStore();

        _store.init(_parent);

        _store.openConfigurationStore(_handler);

        final Map<String, Object> expectedAttributes = Map.copyOf(newAttributes);
        verify(_handler, times(1)).handle(matchesRecord(_rootRecord.getId(), _rootRecord.getType(), expectedAttributes));
        _store.closeConfigurationStore();
    }

    @Test
    public void testCreateObject()
    {
        _store.init(_parent);
        _store.openConfigurationStore(mock(ConfiguredObjectRecordHandler.class));
        createRootRecord();

        final UUID queueId = new UUID(0, 1);
        final String queueType = Queue.class.getSimpleName();
        final Map<String,Object> queueAttr = Map.of("name", "q1");

        _store.create(new ConfiguredObjectRecordImpl(queueId, queueType, queueAttr, getRootAsParentMap()));
        _store.closeConfigurationStore();

        _store.init(_parent);

        _store.openConfigurationStore(_handler);
        verify(_handler).handle(matchesRecord(queueId, queueType, queueAttr));
        verify(_handler).handle(matchesRecord(ANY_UUID, VIRTUAL_HOST_TYPE, ANY_MAP));
        _store.closeConfigurationStore();
    }

    @Test
    public void testCreateAndUpdateObject()
    {
        _store.init(_parent);
        _store.openConfigurationStore(mock(ConfiguredObjectRecordHandler.class));
        createRootRecord();

        final UUID queueId = new UUID(0, 1);
        final String queueType = Queue.class.getSimpleName();
        Map<String,Object> queueAttr = Map.of("name", "q1");

        _store.create(new ConfiguredObjectRecordImpl(queueId, queueType, queueAttr, getRootAsParentMap()));

        queueAttr = new HashMap<>(queueAttr);
        queueAttr.put("owner", "theowner");
        _store.update(false, new ConfiguredObjectRecordImpl(queueId, queueType, queueAttr, getRootAsParentMap()));

        _store.closeConfigurationStore();

        _store.init(_parent);
        _store.openConfigurationStore(_handler);
        verify(_handler, times(1)).handle(matchesRecord(queueId, queueType, queueAttr));
        _store.closeConfigurationStore();
    }

    @Test
    public void testCreateAndRemoveObject()
    {
        _store.init(_parent);
        _store.openConfigurationStore(mock(ConfiguredObjectRecordHandler.class));
        createRootRecord();

        final UUID queueId = new UUID(0, 1);
        final String queueType = Queue.class.getSimpleName();
        final Map<String,Object> queueAttr = Map.of("name", "q1");

        final ConfiguredObjectRecordImpl record = new ConfiguredObjectRecordImpl(queueId, queueType, queueAttr, getRootAsParentMap());
        _store.create(record);

        _store.remove(record);

        _store.closeConfigurationStore();

        _store.init(_parent);
        _store.openConfigurationStore(_handler);
        verify(_handler, times(1)).handle(matchesRecord(ANY_UUID, VIRTUAL_HOST_TYPE, ANY_MAP));
        _store.closeConfigurationStore();
    }

    @Test
    public void testCreateUnknownObjectType()
    {
        _store.init(_parent);
        _store.openConfigurationStore(mock(ConfiguredObjectRecordHandler.class));
        createRootRecord();

        assertThrows(StoreException.class,
                () -> _store.create(new ConfiguredObjectRecordImpl(randomUUID(), "wibble", Map.of(), getRootAsParentMap())),
                "Should not be able to create instance of type wibble");
    }

    @Test
    public void testTwoObjectsWithSameId()
    {
        _store.init(_parent);
        _store.openConfigurationStore(mock(ConfiguredObjectRecordHandler.class));
        createRootRecord();

        final UUID id = randomUUID();
        _store.create(new ConfiguredObjectRecordImpl(id, "Queue", Map.of(ConfiguredObject.NAME, "queue"),
                getRootAsParentMap()));
        assertThrows(StoreException.class,
                () -> _store.create(new ConfiguredObjectRecordImpl(id, "Exchange",
                        Map.of(ConfiguredObject.NAME, "exchange"), getRootAsParentMap())),
                "Should not be able to create two objects with same id");
    }

    @Test
    public void testObjectWithoutName()
    {
        _store.init(_parent);
        _store.openConfigurationStore(mock(ConfiguredObjectRecordHandler.class));
        createRootRecord();

        final UUID id = randomUUID();

        assertThrows(StoreException.class,
                () -> _store.create(new ConfiguredObjectRecordImpl(id, "Exchange", Map.of(), getRootAsParentMap())),
                "Should not be able to create an object without a name");
    }

    @Test
    public void testObjectWithNonStringName()
    {
        _store.init(_parent);
        _store.openConfigurationStore(mock(ConfiguredObjectRecordHandler.class));
        createRootRecord();

        final UUID id = randomUUID();
        assertThrows(StoreException.class,
                () -> _store.update(true, new ConfiguredObjectRecordImpl(id, "Exchange",
                        Map.of(ConfiguredObject.NAME, 3), getRootAsParentMap())),
                "Should not be able to create an object without a name");
    }

    @Test
    public void testChangeTypeOfObject()
    {
        _store.init(_parent);
        _store.openConfigurationStore(mock(ConfiguredObjectRecordHandler.class));
        createRootRecord();

        final UUID id = randomUUID();
        _store.create(new ConfiguredObjectRecordImpl(id, "Queue", Map.of(ConfiguredObject.NAME, "queue"),
                getRootAsParentMap()));
        _store.closeConfigurationStore();
        _store.init(_parent);
        _store.openConfigurationStore(mock(ConfiguredObjectRecordHandler.class));

        assertThrows(StoreException.class,
                () -> _store.update(false, new ConfiguredObjectRecordImpl(id, "Exchange",
                        Map.of(ConfiguredObject.NAME, "exchange"), getRootAsParentMap())),
                "Should not be able to update object to different type");
    }

    @Test
    public void testLockFileGuaranteesExclusiveAccess()
    {
        _store.init(_parent);

        final JsonFileConfigStore secondStore = new JsonFileConfigStore(VirtualHost.class);

        assertThrows(ServerScopedRuntimeException.class,
                () -> secondStore.init(_parent),
                "Should not be able to open a second store with the same path");

        _store.closeConfigurationStore();
        secondStore.init(_parent);
    }

    @Test
    public void testStoreFileLifecycle()
    {
        final File expectedJsonFile = new File(_storeLocation, _parent.getName() + ".json");
        final File expectedJsonFileBak = new File(_storeLocation, _parent.getName() + ".bak");
        final File expectedJsonFileLck = new File(_storeLocation, _parent.getName() + ".lck");

        assertFalse(expectedJsonFile.exists(), "JSON store should not exist");
        assertFalse(expectedJsonFileBak.exists(), "JSON backup should not exist");
        assertFalse(expectedJsonFileLck.exists(), "JSON lock should not exist");

        _store.init(_parent);
        assertTrue(expectedJsonFile.exists(), "JSON store should exist after open");
        assertFalse(expectedJsonFileBak.exists(), "JSON backup should not exist after open");
        assertTrue(expectedJsonFileLck.exists(), "JSON lock should exist");

        _store.closeConfigurationStore();
        assertTrue(expectedJsonFile.exists(), "JSON store should exist after close");

        _store.onDelete(_parent);
        assertFalse(expectedJsonFile.exists(), "JSON store should not exist after delete");
        assertFalse(expectedJsonFileBak.exists(), "JSON backup should not exist after delete");
        assertFalse(expectedJsonFileLck.exists(), "JSON lock should not exist after delete");
    }

    @Test
    public void testCreatedNestedObjects()
    {
        _store.init(_parent);
        _store.openConfigurationStore(mock(ConfiguredObjectRecordHandler.class));
        createRootRecord();

        final UUID queueId = new UUID(0, 1);
        final UUID queue2Id = new UUID(1, 1);
        final UUID exchangeId = new UUID(0, 2);

        final Map<String, UUID> parents = getRootAsParentMap();
        final Map<String, Object> queueAttr = Map.of(ConfiguredObject.NAME, "queue");
        final ConfiguredObjectRecordImpl queueRecord =
                new ConfiguredObjectRecordImpl(queueId, "Queue", queueAttr, parents);
        _store.create(queueRecord);
        final Map<String, Object> queue2Attr = Map.of(ConfiguredObject.NAME, "queue2");
        final ConfiguredObjectRecordImpl queue2Record =
                new ConfiguredObjectRecordImpl(queue2Id, "Queue", queue2Attr, parents);
        _store.create(queue2Record);
        final Map<String, Object> exchangeAttr = Map.of(ConfiguredObject.NAME, "exchange");
        final ConfiguredObjectRecordImpl exchangeRecord =
                new ConfiguredObjectRecordImpl(exchangeId, "Exchange", exchangeAttr, parents);
        _store.create(exchangeRecord);
        _store.closeConfigurationStore();
        _store.init(_parent);
        _store.openConfigurationStore(_handler);
        verify(_handler).handle(matchesRecord(queueId, "Queue", queueAttr));
        verify(_handler).handle(matchesRecord(queue2Id, "Queue", queue2Attr));
        verify(_handler).handle(matchesRecord(exchangeId, "Exchange", exchangeAttr));
        _store.closeConfigurationStore();
    }

    private void createRootRecord()
    {
        final UUID rootRecordId = randomUUID();
        _rootRecord =
                new ConfiguredObjectRecordImpl(rootRecordId, VIRTUAL_HOST_TYPE, Map.of(ConfiguredObject.NAME, "root"));
        _store.create(_rootRecord);
    }

    private Map<String, UUID> getRootAsParentMap()
    {
        return Map.of(VIRTUAL_HOST_TYPE, _rootRecord.getId());
    }

    private ConfiguredObjectRecord matchesRecord(final UUID id, final String type, final Map<String, Object> attributes)
    {
        return argThat(new ConfiguredObjectMatcher(id, type, attributes));
    }

    private static class ConfiguredObjectMatcher implements ArgumentMatcher<ConfiguredObjectRecord>
    {
        private final Map<String,Object> _expectedAttributes;
        private final UUID _expectedId;
        private final String _expectedType;

        private ConfiguredObjectMatcher(final UUID id, final String type, final Map<String, Object> matchingMap)
        {
            _expectedId = id;
            _expectedType = type;
            _expectedAttributes = matchingMap;
        }

        @Override
        public boolean matches(final ConfiguredObjectRecord binding)
        {
            final Map<String,Object> arg = new HashMap<>(binding.getAttributes());
            arg.remove("createdBy");
            arg.remove("createdTime");
            return (_expectedId == ANY_UUID || _expectedId.equals(binding.getId())) &&
                   _expectedType.equals(binding.getType()) &&
                   (_expectedAttributes == ANY_MAP || arg.equals(_expectedAttributes));
        }
    }
}
