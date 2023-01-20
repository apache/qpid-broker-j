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
package org.apache.qpid.server.configuration.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.mockito.ArgumentCaptor;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.model.AbstractSystemConfig;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.JsonSystemConfigImpl;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.StateTransition;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.store.ConfiguredObjectRecord;
import org.apache.qpid.server.store.ConfiguredObjectRecordImpl;
import org.apache.qpid.server.store.DurableConfigurationStore;
import org.apache.qpid.server.store.handler.ConfiguredObjectRecordHandler;
import org.apache.qpid.test.utils.UnitTestBase;

public class ManagementModeStoreHandlerTest extends UnitTestBase
{
    private ManagementModeStoreHandler _handler;
    private Map<String,Object> _systemConfigAttributes;
    private DurableConfigurationStore _store;
    private ConfiguredObjectRecord _root;
    private ConfiguredObjectRecord _portEntry;
    private UUID _rootId, _portEntryId;
    private SystemConfig<?> _systemConfig;
    private TaskExecutor _taskExecutor;

    @BeforeEach
    public void setUp() throws Exception
    {
        _rootId = randomUUID();
        _portEntryId = randomUUID();
        _store = mock(DurableConfigurationStore.class);
        _taskExecutor = new CurrentThreadTaskExecutor();
        _taskExecutor.start();

        _systemConfig = new JsonSystemConfigImpl(_taskExecutor, mock(EventLogger.class),null, Map.of());

        final ConfiguredObjectRecord systemContextRecord = _systemConfig.asObjectRecord();

        _root = new ConfiguredObjectRecordImpl(_rootId, Broker.class.getSimpleName(), Map.of(Broker.NAME, "broker"), Map.of(SystemConfig.class.getSimpleName(), systemContextRecord.getId()));

        _portEntry = mock(ConfiguredObjectRecord.class);
        when(_portEntry.getId()).thenReturn(_portEntryId);
        when(_portEntry.getParents()).thenReturn(Map.of(Broker.class.getSimpleName(), _root.getId()));
        when(_portEntry.getType()).thenReturn(Port.class.getSimpleName());

        final ArgumentCaptor<ConfiguredObjectRecordHandler> recovererArgumentCaptor = 
                ArgumentCaptor.forClass(ConfiguredObjectRecordHandler.class);
        doAnswer(invocation -> 
        {
            final ConfiguredObjectRecordHandler recoverer = recovererArgumentCaptor.getValue();
            recoverer.handle(_root);
            recoverer.handle(_portEntry);
            return false;
        }).when(_store).openConfigurationStore(recovererArgumentCaptor.capture());
        _systemConfigAttributes = new HashMap<>();

        _handler = new ManagementModeStoreHandler(_store, _systemConfig);

        _handler.init(_systemConfig);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private ManagementModeStoreHandler createManagementModeStoreHandler()
    {
        _systemConfig.close();
        final Map<String, Object> attributes = new HashMap<>(_systemConfigAttributes);
        attributes.put(ConfiguredObject.DESIRED_STATE, State.QUIESCED);
        attributes.remove(ConfiguredObject.TYPE);

        _systemConfig = new AbstractSystemConfig(_taskExecutor, mock(EventLogger.class),mock(Principal.class), attributes)
        {
            @Override
            protected void onOpen()
            {
            }

            @Override
            protected DurableConfigurationStore createStoreObject()
            {
                return _store;
            }

            @Override
            protected ListenableFuture<Void> onClose()
            {
                return Futures.immediateFuture(null);
            }

            @Override
            @StateTransition(currentState = State.UNINITIALIZED, desiredState = State.QUIESCED)
            protected ListenableFuture<Void> startQuiesced()
            {
                return Futures.immediateFuture(null);
            }
        };
        _systemConfig.open();
        return new ManagementModeStoreHandler(_store, _systemConfig);
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        _taskExecutor.stop();
        _systemConfig.close();
    }

    private Collection<ConfiguredObjectRecord> openAndGetRecords()
    {
        final Collection<ConfiguredObjectRecord> records = new ArrayList<>();
        _handler.openConfigurationStore(records::add);
        return records;
    }

    private ConfiguredObjectRecord getRootEntry(Collection<ConfiguredObjectRecord> records)
    {
        final String brokerClassName = Broker.class.getSimpleName();
        return records.stream().filter(record -> record.getType().equals(brokerClassName)).findFirst().orElse(null);
    }

    private ConfiguredObjectRecord getEntry(Collection<ConfiguredObjectRecord> records, UUID id)
    {
        return records.stream().filter(record -> record.getId().equals(id)).findFirst().orElse(null);
    }

    private Collection<UUID> getChildrenIds(final Collection<ConfiguredObjectRecord> records,
                                            final ConfiguredObjectRecord parent)
    {
        return records.stream().filter(record -> record.getParents() != null)
                .filter(record -> record.getParents().values().stream().anyMatch(id -> id.equals(parent.getId())))
                .map(ConfiguredObjectRecord::getId)
                .collect(Collectors.toList());
    }

    @Test
    public void testGetRootEntryWithEmptyOptions()
    {
        final Collection<ConfiguredObjectRecord> records = openAndGetRecords();
        final ConfiguredObjectRecord root = getRootEntry(records);
        assertEquals(_rootId, root.getId(), "Unexpected root id");
        assertIterableEquals(Set.of(_portEntryId), getChildrenIds(records, root), "Unexpected children");
    }

    @Test
    public void testGetRootEntryWithHttpPortOverriden()
    {
        _systemConfigAttributes.put(SystemConfig.MANAGEMENT_MODE_HTTP_PORT_OVERRIDE,9090);
        _handler = createManagementModeStoreHandler();
        _handler.init(_systemConfig);
        final Collection<ConfiguredObjectRecord> records = openAndGetRecords();
        final ConfiguredObjectRecord root = getRootEntry(records);
        assertEquals(_rootId, root.getId(), "Unexpected root id");
        final Collection<UUID> childrenIds = getChildrenIds(records, root);
        assertEquals(2, (long) childrenIds.size(), "Unexpected children size");
        assertTrue(childrenIds.contains(_portEntryId), "Store port entry id is not found");
    }

    @Test
    public void testGetRootEntryWithManagementPortsOverriden()
    {
        _systemConfigAttributes.put(SystemConfig.MANAGEMENT_MODE_HTTP_PORT_OVERRIDE,1000);
        _handler = createManagementModeStoreHandler();
        _handler.init(_systemConfig);
        final Collection<ConfiguredObjectRecord> records = openAndGetRecords();
        final ConfiguredObjectRecord root = getRootEntry(records);
        assertEquals(_rootId, root.getId(), "Unexpected root id");
        final Collection<UUID> childrenIds = getChildrenIds(records, root);
        assertEquals(2, (long) childrenIds.size(), "Unexpected children size");
        assertTrue(childrenIds.contains(_portEntryId), "Store port entry id is not found");
    }

    @Test
    public void testGetEntryByRootId()
    {
        final Collection<ConfiguredObjectRecord> records = openAndGetRecords();
        final ConfiguredObjectRecord root = getEntry(records, _rootId);
        assertEquals(_rootId, root.getId(), "Unexpected root id");
        assertIterableEquals(Set.of(_portEntryId), getChildrenIds(records, root), "Unexpected children");
    }

    @Test
    public void testGetEntryByPortId()
    {
        final Collection<ConfiguredObjectRecord> records = openAndGetRecords();
        final ConfiguredObjectRecord portEntry = getEntry(records, _portEntryId);
        assertEquals(_portEntryId, portEntry.getId(), "Unexpected entry id");
        assertTrue(getChildrenIds(records, portEntry).isEmpty(), "Unexpected children");
        assertEquals(State.QUIESCED, portEntry.getAttributes().get(Port.DESIRED_STATE), "Unexpected state");
    }

    @Test
    public void testGetEntryByCLIHttpPortId()
    {
        _systemConfigAttributes.put(SystemConfig.MANAGEMENT_MODE_HTTP_PORT_OVERRIDE,9090);
        _handler = createManagementModeStoreHandler();
        _handler.init(_systemConfig);

        final Collection<ConfiguredObjectRecord> records = openAndGetRecords();
        final UUID optionsPort = getOptionsPortId(records);
        final ConfiguredObjectRecord portEntry = getEntry(records, optionsPort);
        assertCLIPortEntry(records, portEntry, optionsPort, Protocol.HTTP);
    }

    @Test
    public void testHttpPortEntryIsQuiesced()
    {
        final Map<String, Object> attributes = Map.of(Port.PROTOCOLS, Set.of(Protocol.HTTP));
        when(_portEntry.getAttributes()).thenReturn(attributes);
        _systemConfigAttributes.put(SystemConfig.MANAGEMENT_MODE_HTTP_PORT_OVERRIDE,9090);
        _handler = createManagementModeStoreHandler();
        _handler.init(_systemConfig);
        final Collection<ConfiguredObjectRecord> records = openAndGetRecords();
        final ConfiguredObjectRecord portEntry = getEntry(records, _portEntryId);
        assertEquals(State.QUIESCED, portEntry.getAttributes().get(Port.DESIRED_STATE), "Unexpected state");
    }

    @Test
    public void testVirtualHostEntryIsNotQuiescedByDefault()
    {
        virtualHostEntryQuiescedStatusTestImpl(false);
    }

    @Test
    public void testVirtualHostEntryIsQuiescedWhenRequested()
    {
        virtualHostEntryQuiescedStatusTestImpl(true);
    }

    private void virtualHostEntryQuiescedStatusTestImpl(boolean mmQuiesceVhosts)
    {
        final UUID virtualHostNodeId = randomUUID();
        final Map<String, Object> attributes = Map.of(VirtualHostNode.TYPE, "JSON");
        final ConfiguredObjectRecord virtualHostNodeRecord =
                new ConfiguredObjectRecordImpl(virtualHostNodeId, VirtualHostNode.class.getSimpleName(), attributes, Map.of(Broker.class.getSimpleName(), _root.getId()));
        final ArgumentCaptor<ConfiguredObjectRecordHandler> recovererArgumentCaptor =
                ArgumentCaptor.forClass(ConfiguredObjectRecordHandler.class);
        doAnswer(invocation ->
        {
            final ConfiguredObjectRecordHandler recoverer = recovererArgumentCaptor.getValue();
            recoverer.handle(_root);
            recoverer.handle(_portEntry);
            recoverer.handle(virtualHostNodeRecord);
            return false;
        }).when(_store).openConfigurationStore(recovererArgumentCaptor.capture());

        final State expectedState = mmQuiesceVhosts ? State.QUIESCED : null;
        if (mmQuiesceVhosts)
        {
            _systemConfigAttributes.put(SystemConfig.MANAGEMENT_MODE_QUIESCE_VIRTUAL_HOSTS, mmQuiesceVhosts);
        }

        _handler = createManagementModeStoreHandler();
        _handler.init(_systemConfig);
        final Collection<ConfiguredObjectRecord> records = openAndGetRecords();
        final ConfiguredObjectRecord nodeEntry = getEntry(records, virtualHostNodeId);
        final Map<String, Object> nodeAttributes = new HashMap<>(nodeEntry.getAttributes());
        assertEquals(expectedState, nodeAttributes.get(VirtualHostNode.DESIRED_STATE), "Unexpected state");
        nodeAttributes.remove(VirtualHostNode.DESIRED_STATE);
        assertEquals(attributes, nodeAttributes, "Unexpected attributes");
    }

    @SuppressWarnings("unchecked")
    private void assertCLIPortEntry(final Collection<ConfiguredObjectRecord> records,
                                    final ConfiguredObjectRecord portEntry,
                                    final UUID optionsPort,
                                    final Protocol protocol)
    {
        assertEquals(optionsPort, portEntry.getId(), "Unexpected entry id");
        assertTrue(getChildrenIds(records, portEntry).isEmpty(), "Unexpected children");
        final Map<String, Object> attributes = portEntry.getAttributes();
        assertEquals("MANAGEMENT-MODE-PORT-" + protocol.name(), attributes.get(Port.NAME),
                "Unexpected name");
        assertIterableEquals(Set.of(protocol), (Collection<Protocol>) attributes.get(Port.PROTOCOLS),
                "Unexpected protocol");
    }

    @Test
    public void testSavePort()
    {
        _systemConfigAttributes.put(SystemConfig.MANAGEMENT_MODE_HTTP_PORT_OVERRIDE,1000);
        _handler = createManagementModeStoreHandler();
        _handler.init(_systemConfig);
        final Collection<ConfiguredObjectRecord> records = openAndGetRecords();
        final Map<String, Object> attributes = Map.of(Port.NAME, "TEST");
        final ConfiguredObjectRecord configurationEntry =
                new ConfiguredObjectRecordImpl(_portEntryId, Port.class.getSimpleName(), attributes,
                                               Map.of(Broker.class.getSimpleName(), getRootEntry(records).getId()));
        _handler.create(configurationEntry);
        verify(_store).create(any(ConfiguredObjectRecord.class));
    }

    @Test
    public void testSaveRoot()
    {
        _systemConfigAttributes.put(SystemConfig.MANAGEMENT_MODE_HTTP_PORT_OVERRIDE,1000);
        _handler = createManagementModeStoreHandler();
        _handler.init(_systemConfig);
        final Collection<ConfiguredObjectRecord> records = openAndGetRecords();
        final ConfiguredObjectRecord root = getRootEntry(records);
        final Map<String, Object> attributes = Map.of(Broker.NAME, "TEST");
        final ConfiguredObjectRecord configurationEntry =
                new ConfiguredObjectRecordImpl(_rootId, Broker.class.getSimpleName(), attributes,root.getParents());
        _handler.update(false, configurationEntry);
        verify(_store).update(anyBoolean(), any(ConfiguredObjectRecord.class));
    }

    @Test
    public void testSaveCLIHttpPort()
    {
        _systemConfigAttributes.put(SystemConfig.MANAGEMENT_MODE_HTTP_PORT_OVERRIDE,1000);
        _handler = createManagementModeStoreHandler();
        _handler.init(_systemConfig);
        final Collection<ConfiguredObjectRecord> records = openAndGetRecords();
        final UUID portId = getOptionsPortId(records);
        final Map<String, Object> attributes = Map.of(Port.NAME, "TEST");
        final ConfiguredObjectRecord configurationEntry =
                new ConfiguredObjectRecordImpl(portId, Port.class.getSimpleName(), attributes,
                                               Map.of(Broker.class.getSimpleName(), getRootEntry(records).getId()));
        assertThrows(IllegalConfigurationException.class, () -> _handler.update(false, configurationEntry),
                     "Exception should be thrown on trying to save CLI port");
    }

    @Test
    public void testRemove()
    {
        _systemConfigAttributes.put(SystemConfig.MANAGEMENT_MODE_HTTP_PORT_OVERRIDE,1000);
        _handler = createManagementModeStoreHandler();
        _handler.init(_systemConfig);
        openAndGetRecords();

        final ConfiguredObjectRecord record = new ConfiguredObjectRecord()
        {
            @Override
            public UUID getId()
            {
                return _portEntryId;
            }

            @Override
            public String getType()
            {
                return Port.class.getSimpleName();
            }

            @Override
            public Map<String, Object> getAttributes()
            {
                return Map.of();
            }

            @Override
            public Map<String, UUID> getParents()
            {
                return null;
            }
        };
        _handler.remove(record);
        verify(_store).remove(record);
    }

    @Test
    public void testRemoveCLIPort()
    {
        _systemConfigAttributes.put(SystemConfig.MANAGEMENT_MODE_HTTP_PORT_OVERRIDE,1000);
        _handler = createManagementModeStoreHandler();
        _handler.init(_systemConfig);
        final Collection<ConfiguredObjectRecord> records = openAndGetRecords();
        final UUID portId = getOptionsPortId(records);
        final ConfiguredObjectRecord record = mock(ConfiguredObjectRecord.class);
        when(record.getId()).thenReturn(portId);

        assertThrows(IllegalConfigurationException.class, () -> _handler.remove(record),
                     "Exception should be thrown on trying to remove CLI port");
    }

    private UUID getOptionsPortId(Collection<ConfiguredObjectRecord> records)
    {
        final ConfiguredObjectRecord root = getRootEntry(records);
        assertEquals(_rootId, root.getId(), "Unexpected root id");
        final Collection<UUID> childrenIds = getChildrenIds(records, root);
        childrenIds.remove(_portEntryId);
        return childrenIds.iterator().next();
    }
}
