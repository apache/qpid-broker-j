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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

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
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.store.ConfiguredObjectRecord;
import org.apache.qpid.server.store.ConfiguredObjectRecordImpl;
import org.apache.qpid.server.store.DurableConfigurationStore;
import org.apache.qpid.server.store.handler.ConfiguredObjectRecordHandler;
import org.apache.qpid.test.utils.QpidTestCase;

public class ManagementModeStoreHandlerTest extends QpidTestCase
{
    private ManagementModeStoreHandler _handler;
    private Map<String,Object> _systemConfigAttributes;
    private DurableConfigurationStore _store;
    private ConfiguredObjectRecord _root;
    private ConfiguredObjectRecord _portEntry;
    private UUID _rootId, _portEntryId;
    private SystemConfig _systemConfig;
    private TaskExecutor _taskExecutor;

    protected void setUp() throws Exception
    {
        super.setUp();
        _rootId = UUID.randomUUID();
        _portEntryId = UUID.randomUUID();
        _store = mock(DurableConfigurationStore.class);
        _taskExecutor = new CurrentThreadTaskExecutor();
        _taskExecutor.start();

        _systemConfig = new JsonSystemConfigImpl(_taskExecutor, mock(EventLogger.class),
                                                 null, new HashMap<String, Object>());


        ConfiguredObjectRecord systemContextRecord = _systemConfig.asObjectRecord();



        _root = new ConfiguredObjectRecordImpl(_rootId, Broker.class.getSimpleName(), Collections.singletonMap(Broker.NAME,
                                                                                                               (Object) "broker"), Collections.singletonMap(SystemConfig.class.getSimpleName(), systemContextRecord.getId()));

        _portEntry = mock(ConfiguredObjectRecord.class);
        when(_portEntry.getId()).thenReturn(_portEntryId);
        when(_portEntry.getParents()).thenReturn(Collections.singletonMap(Broker.class.getSimpleName(), _root.getId()));
        when(_portEntry.getType()).thenReturn(Port.class.getSimpleName());

        final ArgumentCaptor<ConfiguredObjectRecordHandler> recovererArgumentCaptor = ArgumentCaptor.forClass(ConfiguredObjectRecordHandler.class);
        doAnswer(
                new Answer()
                {
                    @Override
                    public Object answer(final InvocationOnMock invocation) throws Throwable
                    {
                        ConfiguredObjectRecordHandler recoverer = recovererArgumentCaptor.getValue();
                        recoverer.handle(_root);
                        recoverer.handle(_portEntry);
                        return false;
                    }
                }
                ).when(_store).openConfigurationStore(recovererArgumentCaptor.capture());
        _systemConfigAttributes = new HashMap<>();

        _handler = new ManagementModeStoreHandler(_store, _systemConfig);;

        _handler.init(_systemConfig);
    }

    private ManagementModeStoreHandler createManagementModeStoreHandler()
    {
        _systemConfig.close();
        Map<String, Object> attributes = new HashMap<>(_systemConfigAttributes);
        attributes.put(ConfiguredObject.DESIRED_STATE, State.QUIESCED);
        attributes.remove(ConfiguredObject.TYPE);

        _systemConfig = new AbstractSystemConfig(_taskExecutor,
                                                 mock(EventLogger.class),
                                                 mock(Principal.class), attributes)
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

            @StateTransition(currentState = State.UNINITIALIZED, desiredState = State.QUIESCED)
            protected ListenableFuture<Void> startQuiesced()
            {
                return Futures.immediateFuture(null);
            }
        };
        _systemConfig.open();
        return new ManagementModeStoreHandler(_store, _systemConfig);
    }

    @Override
    public void tearDown() throws Exception
    {
        _taskExecutor.stop();
        _systemConfig.close();
        super.tearDown();
    }

    private Collection<ConfiguredObjectRecord> openAndGetRecords()
    {
        final Collection<ConfiguredObjectRecord> records = new ArrayList<>();
        _handler.openConfigurationStore(new ConfiguredObjectRecordHandler()
        {

            @Override
            public void handle(final ConfiguredObjectRecord record)
            {
                records.add(record);
            }

        });
        return records;
    }

    private ConfiguredObjectRecord getRootEntry(Collection<ConfiguredObjectRecord> records)
    {
        for(ConfiguredObjectRecord record : records)
        {
            if (record.getType().equals(Broker.class.getSimpleName()))
            {
                return record;
            }
        }
        return null;
    }

    private ConfiguredObjectRecord getEntry(Collection<ConfiguredObjectRecord> records, UUID id)
    {
        for(ConfiguredObjectRecord record : records)
        {
            if (record.getId().equals(id))
            {
                return record;
            }
        }
        return null;
    }

    private Collection<UUID> getChildrenIds(Collection<ConfiguredObjectRecord> records, ConfiguredObjectRecord parent)
    {
        Collection<UUID> childIds = new HashSet<>();

        for(ConfiguredObjectRecord record : records)
        {

            if (record.getParents() != null)
            {
                for (UUID parentId : record.getParents().values())
                {
                    if (parentId.equals(parent.getId()))
                    {
                        childIds.add(record.getId());
                    }
                }

            }
        }

        return childIds;
    }

    public void testGetRootEntryWithEmptyOptions()
    {
        Collection<ConfiguredObjectRecord> records = openAndGetRecords();
        ConfiguredObjectRecord root = getRootEntry(records);
        assertEquals("Unexpected root id", _rootId, root.getId());
        assertEquals("Unexpected children", Collections.singleton(_portEntryId), getChildrenIds(records, root));
    }

    public void testGetRootEntryWithHttpPortOverriden()
    {
        _systemConfigAttributes.put(SystemConfig.MANAGEMENT_MODE_HTTP_PORT_OVERRIDE,9090);
        _handler = createManagementModeStoreHandler();
        _handler.init(_systemConfig);
        Collection<ConfiguredObjectRecord> records = openAndGetRecords();

        ConfiguredObjectRecord root = getRootEntry(records);
        assertEquals("Unexpected root id", _rootId, root.getId());
        Collection<UUID> childrenIds = getChildrenIds(records, root);
        assertEquals("Unexpected children size", 2, childrenIds.size());
        assertTrue("Store port entry id is not found", childrenIds.contains(_portEntryId));
    }

    public void testGetRootEntryWithManagementPortsOverriden()
    {
        _systemConfigAttributes.put(SystemConfig.MANAGEMENT_MODE_HTTP_PORT_OVERRIDE,1000);
        _handler = createManagementModeStoreHandler();
        _handler.init(_systemConfig);
        Collection<ConfiguredObjectRecord> records = openAndGetRecords();

        ConfiguredObjectRecord root = getRootEntry(records);
        assertEquals("Unexpected root id", _rootId, root.getId());
        Collection<UUID> childrenIds = getChildrenIds(records, root);
        assertEquals("Unexpected children size", 2, childrenIds.size());
        assertTrue("Store port entry id is not found", childrenIds.contains(_portEntryId));
    }

    public void testGetEntryByRootId()
    {
        Collection<ConfiguredObjectRecord> records = openAndGetRecords();

        ConfiguredObjectRecord root = getEntry(records, _rootId);
        assertEquals("Unexpected root id", _rootId, root.getId());
        assertEquals("Unexpected children", Collections.singleton(_portEntryId), getChildrenIds(records, root));
    }

    public void testGetEntryByPortId()
    {
        Collection<ConfiguredObjectRecord> records = openAndGetRecords();

        ConfiguredObjectRecord portEntry = getEntry(records, _portEntryId);
        assertEquals("Unexpected entry id", _portEntryId, portEntry.getId());
        assertTrue("Unexpected children", getChildrenIds(records, portEntry).isEmpty());
        assertEquals("Unexpected state", State.QUIESCED, portEntry.getAttributes().get(Port.STATE));
    }

    public void testGetEntryByCLIHttpPortId()
    {
        _systemConfigAttributes.put(SystemConfig.MANAGEMENT_MODE_HTTP_PORT_OVERRIDE,9090);
        _handler = createManagementModeStoreHandler();
        _handler.init(_systemConfig);

        Collection<ConfiguredObjectRecord> records = openAndGetRecords();

        UUID optionsPort = getOptionsPortId(records);
        ConfiguredObjectRecord portEntry = getEntry(records, optionsPort);
        assertCLIPortEntry(records, portEntry, optionsPort, Protocol.HTTP);
    }

    public void testHttpPortEntryIsQuiesced()
    {
        Map<String, Object> attributes = new HashMap<String, Object>();
        attributes.put(Port.PROTOCOLS, Collections.singleton(Protocol.HTTP));
        when(_portEntry.getAttributes()).thenReturn(attributes);
        _systemConfigAttributes.put(SystemConfig.MANAGEMENT_MODE_HTTP_PORT_OVERRIDE,9090);
        _handler = createManagementModeStoreHandler();
        _handler.init(_systemConfig);

        Collection<ConfiguredObjectRecord> records = openAndGetRecords();

        ConfiguredObjectRecord portEntry = getEntry(records, _portEntryId);
        assertEquals("Unexpected state", State.QUIESCED, portEntry.getAttributes().get(Port.STATE));
    }

    public void testVirtualHostEntryIsNotQuiescedByDefault()
    {
        virtualHostEntryQuiescedStatusTestImpl(false);
    }

    public void testVirtualHostEntryIsQuiescedWhenRequested()
    {
        virtualHostEntryQuiescedStatusTestImpl(true);
    }

    private void virtualHostEntryQuiescedStatusTestImpl(boolean mmQuiesceVhosts)
    {
        UUID virtualHostId = UUID.randomUUID();
        Map<String, Object> attributes = new HashMap<String, Object>();
        attributes.put(VirtualHost.TYPE, "STANDARD");

        final ConfiguredObjectRecord virtualHost = new ConfiguredObjectRecordImpl(virtualHostId, VirtualHost.class.getSimpleName(), attributes, Collections.singletonMap(Broker.class.getSimpleName(), _root.getId()));
        final ArgumentCaptor<ConfiguredObjectRecordHandler> recovererArgumentCaptor = ArgumentCaptor.forClass(ConfiguredObjectRecordHandler.class);
        doAnswer(
                new Answer()
                {
                    @Override
                    public Object answer(final InvocationOnMock invocation) throws Throwable
                    {
                        ConfiguredObjectRecordHandler recoverer = recovererArgumentCaptor.getValue();
                        recoverer.handle(_root);
                        recoverer.handle(_portEntry);
                        recoverer.handle(virtualHost);
                        return false;
                    }
                }
                ).when(_store).openConfigurationStore(recovererArgumentCaptor.capture());

        State expectedState = mmQuiesceVhosts ? State.QUIESCED : null;
        if(mmQuiesceVhosts)
        {
            _systemConfigAttributes.put(SystemConfig.MANAGEMENT_MODE_QUIESCE_VIRTUAL_HOSTS, mmQuiesceVhosts);
        }

        _handler = createManagementModeStoreHandler();
        _handler.init(_systemConfig);
        Collection<ConfiguredObjectRecord> records = openAndGetRecords();

        ConfiguredObjectRecord hostEntry = getEntry(records, virtualHostId);
        Map<String, Object> hostAttributes = new HashMap<String, Object>(hostEntry.getAttributes());
        assertEquals("Unexpected state", expectedState, hostAttributes.get(VirtualHost.STATE));
        hostAttributes.remove(VirtualHost.STATE);
        assertEquals("Unexpected attributes", attributes, hostAttributes);
    }

    @SuppressWarnings("unchecked")
    private void assertCLIPortEntry(final Collection<ConfiguredObjectRecord> records,
                                    ConfiguredObjectRecord portEntry,
                                    UUID optionsPort,
                                    Protocol protocol)
    {
        assertEquals("Unexpected entry id", optionsPort, portEntry.getId());
        assertTrue("Unexpected children", getChildrenIds(records, portEntry).isEmpty());
        Map<String, Object> attributes = portEntry.getAttributes();
        assertEquals("Unexpected name", "MANAGEMENT-MODE-PORT-" + protocol.name(), attributes.get(Port.NAME));
        assertEquals("Unexpected protocol", Collections.singleton(protocol), new HashSet<Protocol>(
                (Collection<Protocol>) attributes.get(Port.PROTOCOLS)));
    }

    public void testSavePort()
    {
        _systemConfigAttributes.put(SystemConfig.MANAGEMENT_MODE_HTTP_PORT_OVERRIDE,1000);
        _handler = createManagementModeStoreHandler();
        _handler.init(_systemConfig);
        Collection<ConfiguredObjectRecord> records = openAndGetRecords();

        Map<String, Object> attributes = new HashMap<String, Object>();
        attributes.put(Port.NAME, "TEST");
        ConfiguredObjectRecord
                configurationEntry = new ConfiguredObjectRecordImpl(_portEntryId, Port.class.getSimpleName(), attributes,
                Collections.singletonMap(Broker.class.getSimpleName(), getRootEntry(records).getId()));
        _handler.create(configurationEntry);
        verify(_store).create(any(ConfiguredObjectRecord.class));
    }

    public void testSaveRoot()
    {
        _systemConfigAttributes.put(SystemConfig.MANAGEMENT_MODE_HTTP_PORT_OVERRIDE,1000);
        _handler = createManagementModeStoreHandler();
        _handler.init(_systemConfig);
        Collection<ConfiguredObjectRecord> records = openAndGetRecords();

        ConfiguredObjectRecord root = getRootEntry(records);
        Map<String, Object> attributes = new HashMap<String, Object>();
        attributes.put(Broker.NAME, "TEST");
        ConfiguredObjectRecord
                configurationEntry = new ConfiguredObjectRecordImpl(_rootId, Broker.class.getSimpleName(), attributes,root.getParents());
        _handler.update(false, configurationEntry);
        verify(_store).update(anyBoolean(), any(ConfiguredObjectRecord.class));
    }

    public void testSaveCLIHttpPort()
    {
        _systemConfigAttributes.put(SystemConfig.MANAGEMENT_MODE_HTTP_PORT_OVERRIDE,1000);
        _handler = createManagementModeStoreHandler();
        _handler.init(_systemConfig);
        Collection<ConfiguredObjectRecord> records = openAndGetRecords();

        UUID portId = getOptionsPortId(records);
        Map<String, Object> attributes = new HashMap<String, Object>();
        attributes.put(Port.NAME, "TEST");
        ConfiguredObjectRecord
                configurationEntry = new ConfiguredObjectRecordImpl(portId, Port.class.getSimpleName(), attributes,
                                                                    Collections.singletonMap(Broker.class.getSimpleName(),
                                                                                             getRootEntry(records).getId()));
        try
        {
            _handler.update(false, configurationEntry);
            fail("Exception should be thrown on trying to save CLI port");
        }
        catch (IllegalConfigurationException e)
        {
            // pass
        }
    }

    public void testRemove()
    {
        _systemConfigAttributes.put(SystemConfig.MANAGEMENT_MODE_HTTP_PORT_OVERRIDE,1000);
        _handler = createManagementModeStoreHandler();
        _handler.init(_systemConfig);
        Collection<ConfiguredObjectRecord> records = openAndGetRecords();

        ConfiguredObjectRecord record = new ConfiguredObjectRecord()
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
                return Collections.emptyMap();
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

    public void testRemoveCLIPort()
    {
        _systemConfigAttributes.put(SystemConfig.MANAGEMENT_MODE_HTTP_PORT_OVERRIDE,1000);
        _handler = createManagementModeStoreHandler();
        _handler.init(_systemConfig);
        Collection<ConfiguredObjectRecord> records = openAndGetRecords();

        UUID portId = getOptionsPortId(records);
        ConfiguredObjectRecord record = mock(ConfiguredObjectRecord.class);
        when(record.getId()).thenReturn(portId);
        try
        {
            _handler.remove(record);
            fail("Exception should be thrown on trying to remove CLI port");
        }
        catch (IllegalConfigurationException e)
        {
            // pass
        }
    }

    private UUID getOptionsPortId(Collection<ConfiguredObjectRecord> records)
    {

        ConfiguredObjectRecord root = getRootEntry(records);
        assertEquals("Unexpected root id", _rootId, root.getId());
        Collection<UUID> childrenIds = getChildrenIds(records, root);

        childrenIds.remove(_portEntryId);
        UUID optionsPort = childrenIds.iterator().next();
        return optionsPort;
    }


}
