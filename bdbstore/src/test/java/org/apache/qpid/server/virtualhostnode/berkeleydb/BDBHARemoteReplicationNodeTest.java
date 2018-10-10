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
 */

package org.apache.qpid.server.virtualhostnode.berkeleydb;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.security.AccessControlException;
import java.security.Principal;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.ConfiguredObjectFactory;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.security.AccessControl;
import org.apache.qpid.server.security.Result;
import org.apache.qpid.server.security.SecurityToken;
import org.apache.qpid.server.security.access.Operation;
import org.apache.qpid.server.store.DurableConfigurationStore;
import org.apache.qpid.server.store.berkeleydb.replication.ReplicatedEnvironmentFacade;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.server.util.ServerScopedRuntimeException;
import org.apache.qpid.test.utils.UnitTestBase;
import org.apache.qpid.test.utils.VirtualHostNodeStoreType;

public class BDBHARemoteReplicationNodeTest extends UnitTestBase
{
    private final AccessControl _mockAccessControl = mock(AccessControl.class);

    private Broker _broker;
    private TaskExecutor _taskExecutor;
    private BDBHAVirtualHostNode<?> _virtualHostNode;
    private DurableConfigurationStore _configStore;
    private ReplicatedEnvironmentFacade _facade;


    @Before
    public void setUp() throws Exception
    {
        assumeThat(getVirtualHostNodeStoreType(), is(equalTo(VirtualHostNodeStoreType.BDB)));

        _facade = mock(ReplicatedEnvironmentFacade.class);

        _broker = BrokerTestHelper.createBrokerMock();

        _taskExecutor = new CurrentThreadTaskExecutor();
        _taskExecutor.start();
        when(_broker.getTaskExecutor()).thenReturn(_taskExecutor);
        when(_broker.getChildExecutor()).thenReturn(_taskExecutor);

        _virtualHostNode = BrokerTestHelper.mockWithSystemPrincipalAndAccessControl(BDBHAVirtualHostNode.class,
                                                                                    mock(Principal.class),
                                                                                    _mockAccessControl);

        _configStore = mock(DurableConfigurationStore.class);
        when(_virtualHostNode.getConfigurationStore()).thenReturn(_configStore);

        // Virtualhost needs the EventLogger from the SystemContext.
        when(_virtualHostNode.getParent()).thenReturn(_broker);
        doReturn(VirtualHostNode.class).when(_virtualHostNode).getCategoryClass();
        ConfiguredObjectFactory objectFactory = _broker.getObjectFactory();
        when(_virtualHostNode.getModel()).thenReturn(objectFactory.getModel());
        when(_virtualHostNode.getTaskExecutor()).thenReturn(_taskExecutor);
        when(_virtualHostNode.getChildExecutor()).thenReturn(_taskExecutor);

    }

    @Test
    public void testUpdateRole() throws Exception
    {
        String remoteReplicationName = getTestName();
        BDBHARemoteReplicationNode remoteReplicationNode = createRemoteReplicationNode(remoteReplicationName);
        // Simulate an election that put the node in REPLICA state
        ((BDBHARemoteReplicationNodeImpl)remoteReplicationNode).setRole(NodeRole.REPLICA);

        Future<Void> future = mock(Future.class);
        when(_facade.transferMasterAsynchronously(remoteReplicationName)).thenReturn(future);
        remoteReplicationNode.setAttributes(Collections.singletonMap(BDBHARemoteReplicationNode.ROLE, NodeRole.MASTER));

        verify(_facade).transferMasterAsynchronously(remoteReplicationName);

        remoteReplicationNode = createRemoteReplicationNode(remoteReplicationName);
        ((BDBHARemoteReplicationNodeImpl)remoteReplicationNode).setRole(NodeRole.REPLICA);

        doThrow(new ExecutionException(new RuntimeException("Test"))).when(future).get(anyLong(), any(TimeUnit.class));
        try
        {
            remoteReplicationNode.setAttributes(Collections.singletonMap(BDBHARemoteReplicationNode.ROLE,
                                                                         NodeRole.MASTER));
            fail("ConnectionScopedRuntimeException is expected");
        }
        catch(ConnectionScopedRuntimeException e)
        {
            // pass
        }

        remoteReplicationNode = createRemoteReplicationNode(remoteReplicationName);
        ((BDBHARemoteReplicationNodeImpl)remoteReplicationNode).setRole(NodeRole.REPLICA);

        doThrow(new ExecutionException(new ServerScopedRuntimeException("Test"))).when(future).get(anyLong(), any(TimeUnit.class));
        try
        {
            remoteReplicationNode.setAttributes(Collections.singletonMap(BDBHARemoteReplicationNode.ROLE,
                                                                         NodeRole.MASTER));
            fail("ServerScopedRuntimeException is expected");
        }
        catch(ServerScopedRuntimeException e)
        {
            // pass
        }
    }

    @Test
    public void testDelete()
    {
        String remoteReplicationName = getTestName();
        BDBHARemoteReplicationNode remoteReplicationNode = createRemoteReplicationNode(remoteReplicationName);

        remoteReplicationNode.delete();

        verify(_facade).removeNodeFromGroup(remoteReplicationName);
    }

    // ***************  ReplicationNode Access Control Tests  ***************

    @Test
    public void testUpdateDeniedByACL()
    {
        String remoteReplicationName = getTestName();
        BDBHARemoteReplicationNode remoteReplicationNode = createRemoteReplicationNode(remoteReplicationName);
        when(_mockAccessControl.authorise(any(SecurityToken.class),
                                          eq(Operation.UPDATE),
                                          eq(remoteReplicationNode),
                                          anyMap())).thenReturn(Result.DENIED);
        when(_mockAccessControl.authorise(isNull(),
                                          eq(Operation.UPDATE),
                                          eq(remoteReplicationNode),
                                          anyMap())).thenReturn(Result.DENIED);

        assertNull(remoteReplicationNode.getDescription());

        try
        {
            remoteReplicationNode.setAttributes(Collections.singletonMap(VirtualHost.DESCRIPTION, "My description"));
            fail("Exception not thrown");
        }
        catch (AccessControlException ace)
        {
            // PASS
        }
    }

    @Test
    public void testDeleteDeniedByACL()
    {
        String remoteReplicationName = getTestName();
        BDBHARemoteReplicationNode remoteReplicationNode = createRemoteReplicationNode(remoteReplicationName);

        when(_mockAccessControl.authorise(any(SecurityToken.class),
                                          eq(Operation.DELETE),
                                          eq(remoteReplicationNode),
                                          anyMap())).thenReturn(Result.DENIED);
        when(_mockAccessControl.authorise(isNull(),
                                          eq(Operation.DELETE),
                                          eq(remoteReplicationNode),
                                          anyMap())).thenReturn(Result.DENIED);
        assertNull(remoteReplicationNode.getDescription());

        try
        {
            remoteReplicationNode.delete();
            fail("Exception not thrown");
        }
        catch (AccessControlException ace)
        {
            // PASS
        }
    }

    private BDBHARemoteReplicationNode createRemoteReplicationNode(final String replicationNodeName)
    {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(BDBHARemoteReplicationNode.NAME, replicationNodeName);
        attributes.put(BDBHARemoteReplicationNode.MONITOR, Boolean.FALSE);

        BDBHARemoteReplicationNodeImpl node = new BDBHARemoteReplicationNodeImpl(_virtualHostNode, attributes, _facade);
        node.create();
        return node;
    }


}
