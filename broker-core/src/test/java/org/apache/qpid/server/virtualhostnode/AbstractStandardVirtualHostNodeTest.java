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
package org.apache.qpid.server.virtualhostnode;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.security.AccessControlException;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObjectFactoryImpl;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.RemoteReplicationNode;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.security.AccessControl;
import org.apache.qpid.server.security.Result;
import org.apache.qpid.server.security.access.Operation;
import org.apache.qpid.server.store.ConfiguredObjectRecord;
import org.apache.qpid.server.store.DurableConfigurationStore;
import org.apache.qpid.server.store.NullMessageStore;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.server.store.handler.ConfiguredObjectRecordHandler;
import org.apache.qpid.server.virtualhost.TestMemoryVirtualHost;
import org.apache.qpid.test.utils.UnitTestBase;

public class AbstractStandardVirtualHostNodeTest extends UnitTestBase
{
    private static final String TEST_VIRTUAL_HOST_NODE_NAME = "testNode";
    private static final String TEST_VIRTUAL_HOST_NAME = "testVirtualHost";

    private final UUID _nodeId = randomUUID();
    private Broker<?> _broker;
    private TaskExecutor _taskExecutor;

    @BeforeEach
    public void setUp() throws Exception
    {
        _taskExecutor = new CurrentThreadTaskExecutor();
        _broker = BrokerTestHelper.createBrokerMock();
        final SystemConfig<?> systemConfig = (SystemConfig<?>) _broker.getParent();
        when(systemConfig.getObjectFactory()).thenReturn(new ConfiguredObjectFactoryImpl(mock(Model.class)));

        _taskExecutor.start();
        when(_broker.getTaskExecutor()).thenReturn(_taskExecutor);
        when(_broker.getChildExecutor()).thenReturn(_taskExecutor);
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        _taskExecutor.stopImmediately();
    }

    /**
     *  Tests activating a virtualhostnode with a config store that specifies a
     *  virtualhost.  Ensures that the virtualhost created.
     */
    @Test
    public void testActivateVHN_StoreHasVH()
    {
        final UUID virtualHostId = randomUUID();
        final ConfiguredObjectRecord vhostRecord = createVirtualHostConfiguredObjectRecord(virtualHostId);
        final DurableConfigurationStore configStore = configStoreThatProduces(vhostRecord);

        final Map<String, Object> nodeAttributes = Map.of(VirtualHostNode.NAME, TEST_VIRTUAL_HOST_NODE_NAME,
                VirtualHostNode.ID, _nodeId);

        final VirtualHostNode<?> node = new TestVirtualHostNode(_broker, nodeAttributes, configStore);
        node.open();
        node.start();

        final VirtualHost<?> virtualHost = node.getVirtualHost();
        assertNotNull(virtualHost, "Virtual host was not recovered");
        assertEquals(TEST_VIRTUAL_HOST_NAME, virtualHost.getName(), "Unexpected virtual host name");
        assertEquals(State.ACTIVE, virtualHost.getState(), "Unexpected virtual host state");
        assertEquals(virtualHostId, virtualHost.getId(), "Unexpected virtual host id");
        node.close();
    }

    /**
     *  Tests activating a virtualhostnode with a config store which does not specify
     *  a virtualhost.  Checks no virtualhost is created.
     */
    @Test
    public void testActivateVHN_StoreHasNoVH()
    {
        final DurableConfigurationStore configStore = configStoreThatProducesNoRecords();

        final Map<String, Object> nodeAttributes = Map.of(VirtualHostNode.NAME, TEST_VIRTUAL_HOST_NODE_NAME,
                VirtualHostNode.ID, _nodeId);

        final VirtualHostNode<?> node = new TestVirtualHostNode(_broker, nodeAttributes, configStore);
        node.open();
        node.start();

        final VirtualHost<?> virtualHost = node.getVirtualHost();
        assertNull(virtualHost, "Virtual host should not be automatically created");
        node.close();
    }

    /**
     *  Tests activating a virtualhostnode with a blueprint context variable.  Config store
     *  does not specify a virtualhost.  Checks virtualhost is created from the blueprint.
     */
    @Test
    public void testActivateVHNWithVHBlueprint_StoreHasNoVH()
    {
        final DurableConfigurationStore configStore = new NullMessageStore() {};

        final String vhBlueprint = String.format("{ \"type\" : \"%s\", \"name\" : \"%s\"}",
                TestMemoryVirtualHost.VIRTUAL_HOST_TYPE, TEST_VIRTUAL_HOST_NAME);
        final Map<String, String> context = Map.of(AbstractVirtualHostNode.VIRTUALHOST_BLUEPRINT_CONTEXT_VAR, vhBlueprint);

        final Map<String, Object> nodeAttributes = Map.of(VirtualHostNode.NAME, TEST_VIRTUAL_HOST_NODE_NAME,
                VirtualHostNode.ID, _nodeId,
                VirtualHostNode.CONTEXT, context);

        final VirtualHostNode<?> node = new TestVirtualHostNode(_broker, nodeAttributes, configStore);
        node.open();
        node.start();

        final VirtualHost<?> virtualHost = node.getVirtualHost();

        assertNotNull(virtualHost, "Virtual host should be created by blueprint");
        assertEquals(TEST_VIRTUAL_HOST_NAME, virtualHost.getName(), "Unexpected virtual host name");
        assertEquals(State.ACTIVE, virtualHost.getState(), "Unexpected virtual host state");
        assertNotNull(virtualHost.getId(), "Unexpected virtual host id");

        assertEquals("{}", node.getVirtualHostInitialConfiguration(),
                "Initial configuration should be empty");
        node.close();
    }

    /**
     *  Tests activating a virtualhostnode with blueprint context variable and the
     *  but the virtualhostInitialConfiguration set to empty.  Config store does not specify a virtualhost.
     *  Checks virtualhost is not recreated from the blueprint.
     */
    @Test
    public void testActivateVHNWithVHBlueprintUsed_StoreHasNoVH()
    {
        final DurableConfigurationStore configStore = configStoreThatProducesNoRecords();

        final String vhBlueprint = String.format("{ \"type\" : \"%s\", \"name\" : \"%s\"}",
                TestMemoryVirtualHost.VIRTUAL_HOST_TYPE, TEST_VIRTUAL_HOST_NAME);
        final Map<String, String> context = Map.of(AbstractVirtualHostNode.VIRTUALHOST_BLUEPRINT_CONTEXT_VAR, vhBlueprint);

        final Map<String, Object> nodeAttributes = Map.of(VirtualHostNode.NAME, TEST_VIRTUAL_HOST_NODE_NAME,
                VirtualHostNode.ID, _nodeId,
                VirtualHostNode.CONTEXT, context,
                VirtualHostNode.VIRTUALHOST_INITIAL_CONFIGURATION, "{}");

        final VirtualHostNode<?> node = new TestVirtualHostNode(_broker, nodeAttributes, configStore);
        node.open();
        node.start();

        final VirtualHost<?> virtualHost = node.getVirtualHost();

        assertNull(virtualHost, "Virtual host should not be created by blueprint");
        node.close();
    }

    /**
     *  Tests activating a virtualhostnode with a blueprint context variable.  Config store
     *  does specify a virtualhost.  Checks that virtualhost is recovered from store and
     *  blueprint is ignored..
     */
    @Test
    public void testActivateVHNWithVHBlueprint_StoreHasExistingVH()
    {
        final UUID virtualHostId = randomUUID();
        final ConfiguredObjectRecord record = createVirtualHostConfiguredObjectRecord(virtualHostId);
        final DurableConfigurationStore configStore = configStoreThatProduces(record);
        final String vhBlueprint = String.format("{ \"type\" : \"%s\", \"name\" : \"%s\"}",
                TestMemoryVirtualHost.VIRTUAL_HOST_TYPE, "vhFromBlueprint");
        final Map<String, String> context = Map.of(AbstractVirtualHostNode.VIRTUALHOST_BLUEPRINT_CONTEXT_VAR, vhBlueprint);
        final Map<String, Object> nodeAttributes = Map.of(VirtualHostNode.NAME, TEST_VIRTUAL_HOST_NODE_NAME,
                VirtualHostNode.ID, _nodeId,
                VirtualHostNode.CONTEXT, context);
        final VirtualHostNode<?> node = new TestVirtualHostNode(_broker, nodeAttributes, configStore);
        node.open();
        node.start();

        final VirtualHost<?> virtualHost = node.getVirtualHost();

        assertNotNull(virtualHost, "Virtual host should be recovered");
        assertEquals(TEST_VIRTUAL_HOST_NAME, virtualHost.getName(), "Unexpected virtual host name");
        assertEquals(State.ACTIVE, virtualHost.getState(), "Unexpected virtual host state");
        assertEquals(virtualHostId, virtualHost.getId(), "Unexpected virtual host id");
        node.close();
    }

    @Test
    public void testStopStartVHN()
    {
        final DurableConfigurationStore configStore = configStoreThatProducesNoRecords();
        final Map<String, Object> nodeAttributes = Map.of(VirtualHostNode.NAME, TEST_VIRTUAL_HOST_NODE_NAME,
                VirtualHostNode.ID, _nodeId);
        final VirtualHostNode<?> node = new TestVirtualHostNode(_broker, nodeAttributes, configStore);
        node.open();
        node.start();

        assertEquals(State.ACTIVE, node.getState(), "Unexpected virtual host node state");

        node.stop();
        assertEquals(State.STOPPED, node.getState(), "Unexpected virtual host node state after stop");

        node.start();
        assertEquals(State.ACTIVE, node.getState(), "Unexpected virtual host node state after start");
        node.close();
    }

    // ***************  VHN Access Control Tests  ***************

    @Test
    public void testUpdateVHNDeniedByACL()
    {
        final AccessControl<?> mockAccessControl = mock(AccessControl.class);
        final DurableConfigurationStore configStore = configStoreThatProducesNoRecords();
        final Map<String, Object> nodeAttributes = Map.of(VirtualHostNode.NAME, TEST_VIRTUAL_HOST_NODE_NAME,
                VirtualHostNode.ID, _nodeId);
        final TestVirtualHostNode node = new TestVirtualHostNode(_broker, nodeAttributes, configStore);
        node.setAccessControl(mockAccessControl);
        node.open();
        node.start();

        when(mockAccessControl.authorise(eq(null), eq(Operation.UPDATE), same(node), any())).thenReturn(Result.DENIED);

        assertNull(node.getDescription());

        assertThrows(AccessControlException.class,
                () -> node.setAttributes(Map.of(VirtualHostNode.DESCRIPTION, "My virtualhost node")),
                "Exception not thrown");

        assertNull(node.getDescription(), "Description unexpected updated");
        node.close();
    }

    @Test
    public void testDeleteVHNDeniedByACL()
    {
        final AccessControl<?> mockAccessControl = mock(AccessControl.class);
        final DurableConfigurationStore configStore = configStoreThatProducesNoRecords();
        final Map<String, Object> nodeAttributes = Map.of(
                VirtualHostNode.NAME, TEST_VIRTUAL_HOST_NODE_NAME,
                VirtualHostNode.ID, _nodeId);
        final TestVirtualHostNode node = new TestVirtualHostNode(_broker, nodeAttributes, configStore);
        node.setAccessControl(mockAccessControl);

        node.open();
        node.start();
        when(mockAccessControl.authorise(null, Operation.DELETE, node, Map.of())).thenReturn(Result.DENIED);

        assertThrows(AccessControlException.class, node::delete, "Exception not thrown");

        assertEquals(State.ACTIVE, node.getState(), "Virtual host node state changed unexpectedly");
        node.close();
    }

    @Test
    public void testStopVHNDeniedByACL()
    {
        final AccessControl<?> mockAccessControl = mock(AccessControl.class);
        final DurableConfigurationStore configStore = configStoreThatProducesNoRecords();
        final Map<String, Object> nodeAttributes = Map.of(VirtualHostNode.NAME, TEST_VIRTUAL_HOST_NODE_NAME,
                VirtualHostNode.ID, _nodeId);
        final TestVirtualHostNode node = new TestVirtualHostNode(_broker, nodeAttributes, configStore);
        node.setAccessControl(mockAccessControl);

        node.open();
        node.start();

        when(mockAccessControl.authorise(eq(null), eq(Operation.UPDATE), same(node), any())).thenReturn(Result.DENIED);

        assertThrows(AccessControlException.class, node::stop, "Exception not thrown");

        assertEquals(State.ACTIVE, node.getState(), "Virtual host node state changed unexpectedly");
        node.close();
    }

    @Test
    public void testValidateOnCreateFails_StoreFails()
    {
        final String nodeName = getTestName();
        final Map<String, Object> attributes = Map.of(TestVirtualHostNode.NAME, nodeName);
        final DurableConfigurationStore store = mock(DurableConfigurationStore.class);
        doThrow(new RuntimeException("Cannot open store")).when(store).init(any(ConfiguredObject.class));
        final AbstractStandardVirtualHostNode<?> node = createTestStandardVirtualHostNode(attributes, store);

        final IllegalConfigurationException thrown = assertThrows(IllegalConfigurationException.class,
                node::create,
                "Exception not thrown");
        assertTrue(thrown.getMessage().startsWith("Cannot open node configuration store"),
                   "Unexpected exception " + thrown.getMessage());
    }

    @Test
    public void testValidateOnCreateFails_ExistingDefaultVHN()
    {
        final String nodeName = getTestName();
        final Map<String, Object> attributes = Map.of(TestVirtualHostNode.NAME, nodeName,
                TestVirtualHostNode.DEFAULT_VIRTUAL_HOST_NODE, Boolean.TRUE);
        final VirtualHostNode<?> existingDefault = mock(VirtualHostNode.class);
        when(existingDefault.getName()).thenReturn("existingDefault");
        when(_broker.findDefautVirtualHostNode()).thenReturn(existingDefault);

        final DurableConfigurationStore store = mock(DurableConfigurationStore.class);
        final AbstractStandardVirtualHostNode<?> node = createTestStandardVirtualHostNode(attributes, store);

        final IllegalConfigurationException thrown = assertThrows(IllegalConfigurationException.class,
                node::create,
                "Exception not thrown");
        assertTrue(thrown.getMessage().startsWith("The existing virtual host node 'existingDefault' is already "
                + "the default for the Broker"), "Unexpected exception " + thrown.getMessage());
    }

    @Test
    public void testValidateOnCreateSucceeds()
    {
        final String nodeName = getTestName();
        final Map<String, Object> attributes = Map.of(TestVirtualHostNode.NAME, nodeName);
        final DurableConfigurationStore store = mock(DurableConfigurationStore.class);
        final AbstractStandardVirtualHostNode<?> node = createTestStandardVirtualHostNode(attributes, store);

        node.create();
        verify(store, times(2)).init(node); // once of validation, once for real
        verify(store, times(1)).closeConfigurationStore();
        node.close();
    }

    @Test
    public void testOpenFails()
    {
        final String nodeName = getTestName();
        final Map<String, Object> attributes = Map.of(TestVirtualHostNode.NAME, nodeName);
        final DurableConfigurationStore store = mock(DurableConfigurationStore.class);
        final AbstractVirtualHostNode<?> node = new TestAbstractVirtualHostNode(_broker, attributes, store);
        node.open();
        assertEquals(State.ERRORED, node.getState(), "Unexpected node state");
        node.close();
    }

    @Test
    public void testOpenSucceeds()
    {
        final String nodeName = getTestName();
        final Map<String, Object> attributes = Map.of(TestVirtualHostNode.NAME, nodeName);
        final AtomicBoolean onFailureFlag = new AtomicBoolean();
        final DurableConfigurationStore store = mock(DurableConfigurationStore.class);
        final AbstractVirtualHostNode<?> node = new TestAbstractVirtualHostNode( _broker, attributes, store)
        {
            @Override
            public void onValidate()
            {
                // no op
            }

            @Override
            protected void onExceptionInOpen(RuntimeException e)
            {
                try
                {
                    super.onExceptionInOpen(e);
                }
                finally
                {
                    onFailureFlag.set(true);
                }
            }
        };

        node.open();
        assertEquals(State.ACTIVE, node.getState(), "Unexpected node state");
        assertFalse(onFailureFlag.get(), "onExceptionInOpen was called");
        node.close();
    }

    @Test
    public void testDeleteInErrorStateAfterOpen()
    {
        final String nodeName = getTestName();
        final Map<String, Object> attributes = Map.of(TestVirtualHostNode.NAME, nodeName);
        final DurableConfigurationStore store = mock(DurableConfigurationStore.class);
        doThrow(new RuntimeException("Cannot open store")).when(store).init(any(ConfiguredObject.class));
        final AbstractStandardVirtualHostNode<?> node = createTestStandardVirtualHostNode(attributes, store);
        node.open();
        assertEquals(State.ERRORED, node.getState(), "Unexpected node state");

        node.delete();
        assertEquals(State.DELETED, node.getState(), "Unexpected state");
    }

    @Test
    public void testActivateInErrorStateAfterOpen()
    {
        final String nodeName = getTestName();
        final Map<String, Object> attributes = Map.of(TestVirtualHostNode.NAME, nodeName);
        final DurableConfigurationStore store = mock(DurableConfigurationStore.class);
        doThrow(new RuntimeException("Cannot open store")).when(store).init(any(ConfiguredObject.class));
        final AbstractVirtualHostNode<?> node = createTestStandardVirtualHostNode(attributes, store);
        node.open();
        assertEquals(State.ERRORED, node.getState(), "Unexpected node state");
        doNothing().when(store).init(any(ConfiguredObject.class));

        node.setAttributes(Map.of(VirtualHostNode.DESIRED_STATE, State.ACTIVE));
        assertEquals(State.ACTIVE, node.getState(), "Unexpected state");
        node.close();
    }

    @Test
    public void testStartInErrorStateAfterOpen()
    {
        final String nodeName = getTestName();
        final Map<String, Object> attributes = Map.of(TestVirtualHostNode.NAME, nodeName);
        final DurableConfigurationStore store = mock(DurableConfigurationStore.class);
        doThrow(new RuntimeException("Cannot open store")).when(store).init(any(ConfiguredObject.class));
        final AbstractVirtualHostNode<?> node = createTestStandardVirtualHostNode(attributes, store);
        node.open();
        assertEquals(State.ERRORED, node.getState(), "Unexpected node state");
        doNothing().when(store).init(any(ConfiguredObject.class));

        node.start();
        assertEquals(State.ACTIVE, node.getState(), "Unexpected state");
        node.close();
    }

    private ConfiguredObjectRecord createVirtualHostConfiguredObjectRecord(UUID virtualHostId)
    {
        final Map<String, Object> virtualHostAttributes = Map.of(VirtualHost.NAME, TEST_VIRTUAL_HOST_NAME,
                VirtualHost.TYPE, TestMemoryVirtualHost.VIRTUAL_HOST_TYPE,
                VirtualHost.MODEL_VERSION, BrokerModel.MODEL_VERSION);
        final ConfiguredObjectRecord record = mock(ConfiguredObjectRecord.class);
        when(record.getId()).thenReturn(virtualHostId);
        when(record.getAttributes()).thenReturn(virtualHostAttributes);
        when(record.getType()).thenReturn(VirtualHost.class.getSimpleName());
        return record;
    }

    private NullMessageStore configStoreThatProduces(final ConfiguredObjectRecord record)
    {
        return new NullMessageStore(){

            @Override
            public boolean openConfigurationStore(final ConfiguredObjectRecordHandler handler,
                                                  final ConfiguredObjectRecord... initialRecords) throws StoreException
            {
                if (record != null)
                {
                    handler.handle(record);
                }
                return false;
            }
        };
    }

    private NullMessageStore configStoreThatProducesNoRecords()
    {
        return configStoreThatProduces(null);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private AbstractStandardVirtualHostNode<?> createTestStandardVirtualHostNode(final Map<String, Object> attributes,
                                                                                 final DurableConfigurationStore store)
    {
        return new AbstractStandardVirtualHostNode(attributes,  _broker)
        {

            @Override
            protected void writeLocationEventLog()
            {

            }

            @Override
            protected DurableConfigurationStore createConfigurationStore()
            {
                return store;
            }
        };
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static class TestAbstractVirtualHostNode extends AbstractVirtualHostNode
    {
        private final DurableConfigurationStore _store;

        public TestAbstractVirtualHostNode(final Broker<?> parent,
                                           final Map<String, Object> attributes,
                                           final DurableConfigurationStore store)
        {
            super(parent, attributes);
            _store = store;
        }

        @Override
        public void onValidate()
        {
            throw new RuntimeException("Cannot validate");
        }

        @Override
        protected DurableConfigurationStore createConfigurationStore()
        {
            return _store;
        }

        @Override
        protected CompletableFuture<Void> activate()
        {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        protected ConfiguredObjectRecord enrichInitialVirtualHostRootRecord(ConfiguredObjectRecord vhostRecord)
        {
            return null;
        }

        @Override
        public Collection<? extends RemoteReplicationNode<?>> getRemoteReplicationNodes()
        {
            return null;
        }
    }
}
