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
package org.apache.qpid.server.store.berkeleydb;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;
import static org.mockito.Mockito.when;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.sleepycat.je.Durability;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.rep.NoConsistencyRequiredPolicy;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.AbstractConfigurationChangeListener;
import org.apache.qpid.server.model.ConfigurationChangeListener;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.RemoteReplicationNode;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.store.DurableConfigurationStore;
import org.apache.qpid.server.store.berkeleydb.replication.DatabasePinger;
import org.apache.qpid.server.store.berkeleydb.replication.ReplicatedEnvironmentFacade;
import org.apache.qpid.server.util.ExternalServiceException;
import org.apache.qpid.server.util.FileUtils;
import org.apache.qpid.server.virtualhost.berkeleydb.BDBHAVirtualHost;
import org.apache.qpid.server.virtualhost.berkeleydb.BDBHAVirtualHostImpl;
import org.apache.qpid.server.virtualhostnode.berkeleydb.BDBHARemoteReplicationNode;
import org.apache.qpid.server.virtualhostnode.berkeleydb.BDBHARemoteReplicationNodeImpl;
import org.apache.qpid.server.virtualhostnode.berkeleydb.BDBHAVirtualHostNode;
import org.apache.qpid.server.virtualhostnode.berkeleydb.BDBHAVirtualHostNodeTestHelper;
import org.apache.qpid.server.virtualhostnode.berkeleydb.NodeRole;
import org.apache.qpid.test.utils.PortHelper;
import org.apache.qpid.test.utils.TestFileUtils;
import org.apache.qpid.test.utils.UnitTestBase;
import org.apache.qpid.test.utils.VirtualHostNodeStoreType;

public class BDBHAVirtualHostNodeTest extends UnitTestBase
{
    private final static Logger LOGGER = LoggerFactory.getLogger(BDBHAVirtualHostNodeTest.class);

    private BDBHAVirtualHostNodeTestHelper _helper;
    private PortHelper _portHelper = new PortHelper();

    @Before
    public void setUp() throws Exception
    {
        assumeThat(getVirtualHostNodeStoreType(), is(equalTo(VirtualHostNodeStoreType.BDB)));
        _helper = new BDBHAVirtualHostNodeTestHelper(getTestName());
    }

    @After
    public void tearDown() throws Exception
    {
        try
        {
            if (_helper != null)
            {
                _helper.tearDown();
            }
        }
        finally
        {
            _portHelper.waitUntilAllocatedPortsAreFree();
        }
    }

    @Test
    public void testCreateAndActivateVirtualHostNode() throws Exception
    {
        int node1PortNumber = _portHelper.getNextAvailable();
        String helperAddress = "localhost:" + node1PortNumber;
        String groupName = "group";
        String nodeName = "node1";

        Map<String, Object> attributes = _helper.createNodeAttributes(nodeName, groupName, helperAddress, helperAddress, nodeName, node1PortNumber);
        String messageStorePath = (String)attributes.get(BDBHAVirtualHostNode.STORE_PATH);
        String repStreamTimeout = "2 h";
        Map<String,String> context = (Map<String,String>)attributes.get(BDBHAVirtualHostNode.CONTEXT);
        context.put(ReplicationConfig.REP_STREAM_TIMEOUT, repStreamTimeout);
        context.put(EnvironmentConfig.ENV_IS_TRANSACTIONAL, "false");
        try
        {
            _helper.createHaVHN(attributes);
            fail("Exception was not thrown.");
        }
        catch (RuntimeException e)
        {
            final boolean condition = e.getCause() instanceof IllegalArgumentException;
            assertTrue("Unexpected Exception being thrown.", condition);
        }
        context.put(EnvironmentConfig.ENV_IS_TRANSACTIONAL, "true");
        BDBHAVirtualHostNode<?> node = _helper.createHaVHN(attributes);

        node.start();
        _helper.assertNodeRole(node, NodeRole.MASTER, NodeRole.REPLICA);

        assertEquals("Unexpected node state", State.ACTIVE, node.getState());

        DurableConfigurationStore store = node.getConfigurationStore();
        assertNotNull(store);

        BDBConfigurationStore bdbConfigurationStore = (BDBConfigurationStore) store;
        ReplicatedEnvironmentFacade environmentFacade = (ReplicatedEnvironmentFacade) bdbConfigurationStore.getEnvironmentFacade();


        assertEquals(nodeName, environmentFacade.getNodeName());
        assertEquals(groupName, environmentFacade.getGroupName());
        assertEquals(helperAddress, environmentFacade.getHostPort());
        assertEquals(helperAddress, environmentFacade.getHelperHostPort());

        assertEquals("SYNC,NO_SYNC,SIMPLE_MAJORITY", environmentFacade.getMessageStoreDurability().toString());

        _helper.awaitForVirtualhost(node);
        VirtualHost<?> virtualHost = node.getVirtualHost();
        assertNotNull("Virtual host child was not added", virtualHost);
        assertEquals("Unexpected virtual host name", groupName, virtualHost.getName());
        assertEquals("Unexpected virtual host store",
                            bdbConfigurationStore.getMessageStore(),
                            virtualHost.getMessageStore());
        assertEquals("Unexpected virtual host state", State.ACTIVE, virtualHost.getState());

        node.stop();
        assertEquals("Unexpected state returned after stop", State.STOPPED, node.getState());
        assertEquals("Unexpected state", State.STOPPED, node.getState());

        assertNull("Virtual host is not destroyed", node.getVirtualHost());

        node.delete();
        assertEquals("Unexpected state returned after delete", State.DELETED, node.getState());
        assertEquals("Unexpected state", State.DELETED, node.getState());
        assertFalse("Store still exists " + messageStorePath, new File(messageStorePath).exists());
    }

    @Test
    public void testMutableAttributes() throws Exception
    {
        int node1PortNumber = _portHelper.getNextAvailable();
        String helperAddress = "localhost:" + node1PortNumber;
        String groupName = "group";
        String nodeName = "node1";

        Map<String, Object> attributes = _helper.createNodeAttributes(nodeName, groupName, helperAddress, helperAddress, nodeName, node1PortNumber);
        BDBHAVirtualHostNode<?> node = _helper.createAndStartHaVHN(attributes);

        assertEquals("Unexpected node priority value before mutation", (long) 1, (long) node.getPriority());
        assertFalse("Unexpected designated primary value before mutation", node.isDesignatedPrimary());
        assertEquals("Unexpected electable group override value before mutation",
                            (long) 0,
                            (long) node.getQuorumOverride());

        Map<String, Object> update = new HashMap<>();
        update.put(BDBHAVirtualHostNode.PRIORITY,  2);
        update.put(BDBHAVirtualHostNode.DESIGNATED_PRIMARY, true);
        update.put(BDBHAVirtualHostNode.QUORUM_OVERRIDE, 1);
        node.setAttributes(update);

        assertEquals("Unexpected node priority value after mutation", (long) 2, (long) node.getPriority());
        assertTrue("Unexpected designated primary value after mutation", node.isDesignatedPrimary());
        assertEquals("Unexpected electable group override value after mutation",
                            (long) 1,
                            (long) node.getQuorumOverride());

        assertNotNull("Join time should be set", node.getJoinTime());
        assertNotNull("Last known replication transaction id should be set",
                             node.getLastKnownReplicationTransactionId());
    }

    @Test
    public void testMutableAttributesAfterMajorityLost() throws Exception
    {
        int node1PortNumber = _portHelper.getNextAvailable();
        int node2PortNumber = _portHelper.getNextAvailable();
        int node3PortNumber = _portHelper.getNextAvailable();

        String helperAddress = "localhost:" + node1PortNumber;
        String groupName = "group";
        String nodeName = "node1";

        Map<String, Object> node1Attributes = _helper.createNodeAttributes(nodeName, groupName, helperAddress, helperAddress, nodeName, node1PortNumber, node2PortNumber, node3PortNumber);
        BDBHAVirtualHostNode<?> node1 = _helper.createAndStartHaVHN(node1Attributes);

        Map<String, Object> node2Attributes = _helper.createNodeAttributes("node2", groupName, "localhost:" + node2PortNumber, helperAddress, nodeName);
        BDBHAVirtualHostNode<?> node2 = _helper.createAndStartHaVHN(node2Attributes);

        Map<String, Object> node3Attributes = _helper.createNodeAttributes("node3", groupName, "localhost:" + node3PortNumber, helperAddress, nodeName);
        BDBHAVirtualHostNode<?> node3 = _helper.createAndStartHaVHN(node3Attributes);

        assertEquals("Unexpected node priority value before mutation", (long) 1, (long) node1.getPriority());
        assertFalse("Unexpected designated primary value before mutation", node1.isDesignatedPrimary());
        assertEquals("Unexpected electable group override value before mutation",
                            (long) 0,
                            (long) node1.getQuorumOverride());

        node2.close();
        node3.close();

        _helper.assertNodeRole(node1, NodeRole.DETACHED);

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(BDBHAVirtualHostNode.PRIORITY, 200);
        attributes.put(BDBHAVirtualHostNode.DESIGNATED_PRIMARY, true);
        attributes.put(BDBHAVirtualHostNode.QUORUM_OVERRIDE, 1);
        node1.setAttributes(attributes);

        _helper.awaitForVirtualhost(node1);

        assertEquals("Unexpected node priority value after mutation", (long) 200, (long) node1.getPriority());
        assertTrue("Unexpected designated primary value after mutation", node1.isDesignatedPrimary());
        assertEquals("Unexpected electable group override value after mutation",
                            (long) 1,
                            (long) node1.getQuorumOverride());
    }

    @Test
    public void testTransferMasterToSelf() throws Exception
    {
        int node1PortNumber = _portHelper.getNextAvailable();
        int node2PortNumber = _portHelper.getNextAvailable();
        int node3PortNumber = _portHelper.getNextAvailable();

        String helperAddress = "localhost:" + node1PortNumber;
        String groupName = "group";
        String nodeName = "node1";

        Map<String, Object> node1Attributes = _helper.createNodeAttributes(nodeName, groupName, helperAddress, helperAddress, nodeName, node1PortNumber, node2PortNumber, node3PortNumber);
        _helper.createAndStartHaVHN(node1Attributes);

        Map<String, Object> node2Attributes = _helper.createNodeAttributes("node2", groupName, "localhost:" + node2PortNumber, helperAddress, nodeName);
        _helper.createAndStartHaVHN(node2Attributes);

        Map<String, Object> node3Attributes = _helper.createNodeAttributes("node3", groupName, "localhost:" + node3PortNumber, helperAddress, nodeName);
        _helper.createAndStartHaVHN(node3Attributes);

        BDBHAVirtualHostNode<?> replica = _helper.awaitAndFindNodeInRole(NodeRole.REPLICA);

        replica.setAttributes(Collections.<String, Object>singletonMap(BDBHAVirtualHostNode.ROLE,  NodeRole.MASTER));

        _helper.assertNodeRole(replica, NodeRole.MASTER);
    }

    @Test
    public void testTransferMasterToRemoteReplica() throws Exception
    {
        int node1PortNumber = _portHelper.getNextAvailable();
        int node2PortNumber = _portHelper.getNextAvailable();
        int node3PortNumber = _portHelper.getNextAvailable();

        String helperAddress = "localhost:" + node1PortNumber;
        String groupName = "group";
        String nodeName = "node1";

        Map<String, Object> node1Attributes = _helper.createNodeAttributes(nodeName, groupName, helperAddress,
                helperAddress, nodeName, node1PortNumber, node2PortNumber, node3PortNumber);
        BDBHAVirtualHostNode<?> node1 = _helper.createAndStartHaVHN(node1Attributes);

        final AtomicReference<RemoteReplicationNode<?>> lastSeenReplica = new AtomicReference<>();
        final CountDownLatch remoteNodeLatch = new CountDownLatch(2);
        node1.addChangeListener(new AbstractConfigurationChangeListener()
        {
            @Override
            public void childAdded(ConfiguredObject<?> object, ConfiguredObject<?> child)
            {
                if (child instanceof RemoteReplicationNode)
                {
                    remoteNodeLatch.countDown();
                    lastSeenReplica.set((RemoteReplicationNode<?>)child);
                }
            }
        });

        Map<String, Object> node2Attributes = _helper.createNodeAttributes("node2", groupName, "localhost:" + node2PortNumber, helperAddress, nodeName);
        BDBHAVirtualHostNode<?> node2 = _helper.createAndStartHaVHN(node2Attributes);

        Map<String, Object> node3Attributes = _helper.createNodeAttributes("node3", groupName, "localhost:" + node3PortNumber, helperAddress, nodeName);
        BDBHAVirtualHostNode<?> node3 = _helper.createAndStartHaVHN(node3Attributes);

        assertTrue("Replication nodes have not been seen during 5s", remoteNodeLatch.await(5, TimeUnit.SECONDS));

        BDBHARemoteReplicationNodeImpl replicaRemoteNode = (BDBHARemoteReplicationNodeImpl)lastSeenReplica.get();
        _helper.awaitForAttributeChange(replicaRemoteNode, BDBHARemoteReplicationNodeImpl.ROLE, NodeRole.REPLICA);

        replicaRemoteNode.setAttributes(Collections.<String,Object>singletonMap(BDBHARemoteReplicationNode.ROLE, NodeRole.MASTER));

        BDBHAVirtualHostNode<?> replica = replicaRemoteNode.getName().equals(node2.getName())? node2 : node3;
        _helper.assertNodeRole(replica, NodeRole.MASTER);
    }

    @Test
    public void testMutatingRoleWhenNotReplica_IsDisallowed() throws Exception
    {
        int nodePortNumber = _portHelper.getNextAvailable();
        String helperAddress = "localhost:" + nodePortNumber;
        String groupName = "group";
        String nodeName = "node1";

        Map<String, Object> node1Attributes = _helper.createNodeAttributes(nodeName, groupName, helperAddress, helperAddress, nodeName, nodePortNumber);
        BDBHAVirtualHostNode<?> node = _helper.createAndStartHaVHN(node1Attributes);
        _helper.assertNodeRole(node, NodeRole.MASTER);

        try
        {
            node.setAttributes(Collections.<String,Object>singletonMap(BDBHAVirtualHostNode.ROLE, NodeRole.REPLICA));
            fail("Role mutation should fail");
        }
        catch(IllegalStateException e)
        {
            // PASS
        }
    }


    @Test
    public void testRemoveReplicaNode() throws Exception
    {
        int node1PortNumber = _portHelper.getNextAvailable();
        int node2PortNumber = _portHelper.getNextAvailable();
        int node3PortNumber = _portHelper.getNextAvailable();

        String helperAddress = "localhost:" + node1PortNumber;
        String groupName = "group";
        String nodeName = "node1";

        assertTrue(_portHelper.isPortAvailable(node1PortNumber));

        Map<String, Object> node1Attributes = _helper.createNodeAttributes(nodeName, groupName, helperAddress, helperAddress, nodeName, node1PortNumber, node2PortNumber, node3PortNumber);
        _helper.createAndStartHaVHN(node1Attributes);

        assertTrue(_portHelper.isPortAvailable(node2PortNumber));

        Map<String, Object> node2Attributes = _helper.createNodeAttributes("node2", groupName, "localhost:" + node2PortNumber, helperAddress, nodeName);
        _helper.createAndStartHaVHN(node2Attributes);

        assertTrue(_portHelper.isPortAvailable(node3PortNumber));

        Map<String, Object> node3Attributes = _helper.createNodeAttributes("node3", groupName, "localhost:" + node3PortNumber, helperAddress, nodeName);
        _helper.createAndStartHaVHN(node3Attributes);


        BDBHAVirtualHostNode<?> master = _helper.awaitAndFindNodeInRole(NodeRole.MASTER);
        _helper.awaitRemoteNodes(master, 2);

        BDBHAVirtualHostNode<?> replica = _helper.awaitAndFindNodeInRole(NodeRole.REPLICA);
        _helper.awaitRemoteNodes(replica, 2);

        assertNotNull("Remote node " + replica.getName() + " is not found",
                             _helper.findRemoteNode(master, replica.getName()));
        replica.delete();

        _helper.awaitRemoteNodes(master, 1);

        assertNull("Remote node " + replica.getName() + " is not found",
                          _helper.findRemoteNode(master, replica.getName()));

    }

    @Test
    public void testSetSynchronizationPolicyAttributesOnVirtualHost() throws Exception
    {
        int node1PortNumber = _portHelper.getNextAvailable();
        String helperAddress = "localhost:" + node1PortNumber;
        String groupName = "group";
        String nodeName = "node1";

        Map<String, Object> nodeAttributes = _helper.createNodeAttributes(nodeName, groupName, helperAddress, helperAddress, nodeName, node1PortNumber);
        BDBHAVirtualHostNode<?> node = _helper.createHaVHN(nodeAttributes);

        node.start();
        _helper.assertNodeRole(node, NodeRole.MASTER, NodeRole.REPLICA);
        assertEquals("Unexpected node state", State.ACTIVE, node.getState());

        _helper.awaitForVirtualhost(node);
        BDBHAVirtualHostImpl virtualHost = (BDBHAVirtualHostImpl)node.getVirtualHost();
        assertNotNull("Virtual host is not created", virtualHost);

        _helper.awaitForAttributeChange(virtualHost, BDBHAVirtualHostImpl.COALESCING_SYNC, true);

        assertEquals("Unexpected local transaction synchronization policy",
                            "SYNC",
                            virtualHost.getLocalTransactionSynchronizationPolicy());
        assertEquals("Unexpected remote transaction synchronization policy",
                            "NO_SYNC",
                            virtualHost.getRemoteTransactionSynchronizationPolicy());
        assertTrue("CoalescingSync is not ON", virtualHost.isCoalescingSync());

        Map<String, Object> virtualHostAttributes = new HashMap<String,Object>();
        virtualHostAttributes.put(BDBHAVirtualHost.LOCAL_TRANSACTION_SYNCHRONIZATION_POLICY, "WRITE_NO_SYNC");
        virtualHostAttributes.put(BDBHAVirtualHost.REMOTE_TRANSACTION_SYNCHRONIZATION_POLICY, "SYNC");
        virtualHost.setAttributes(virtualHostAttributes);

        virtualHost.stop();
        virtualHost.start();

        assertEquals("Unexpected local transaction synchronization policy",
                            "WRITE_NO_SYNC",
                            virtualHost.getLocalTransactionSynchronizationPolicy());
        assertEquals("Unexpected remote transaction synchronization policy",
                            "SYNC",
                            virtualHost.getRemoteTransactionSynchronizationPolicy());
        assertFalse("CoalescingSync is not OFF", virtualHost.isCoalescingSync());
        try
        {
            virtualHost.setAttributes(Collections.<String, Object>singletonMap(BDBHAVirtualHost.LOCAL_TRANSACTION_SYNCHRONIZATION_POLICY, "INVALID"));
            fail("Invalid synchronization policy is set");
        }
        catch(IllegalArgumentException e)
        {
            //pass
        }

        try
        {
            virtualHost.setAttributes(Collections.<String, Object>singletonMap(BDBHAVirtualHost.REMOTE_TRANSACTION_SYNCHRONIZATION_POLICY, "INVALID"));
            fail("Invalid synchronization policy is set");
        }
        catch(IllegalArgumentException e)
        {
            //pass
        }

    }

    @Test
    public void testNotPermittedNodeIsNotAllowedToConnect() throws Exception
    {
        int node1PortNumber = _portHelper.getNextAvailable();
        int node2PortNumber = _portHelper.getNextAvailable();
        int node3PortNumber = _portHelper.getNextAvailable();

        String helperAddress = "localhost:" + node1PortNumber;
        String groupName = "group";
        String nodeName = "node1";

        Map<String, Object> node1Attributes = _helper.createNodeAttributes(nodeName, groupName, helperAddress, helperAddress, nodeName, node1PortNumber, node2PortNumber);
        BDBHAVirtualHostNode<?> node1 = _helper.createAndStartHaVHN(node1Attributes);

        Map<String, Object> node2Attributes = _helper.createNodeAttributes("node2", groupName, "localhost:" + node2PortNumber, helperAddress, nodeName);
        BDBHAVirtualHostNode<?> node2 = _helper.createAndStartHaVHN(node2Attributes);

        Map<String, Object> node3Attributes = _helper.createNodeAttributes("node3", groupName, "localhost:" + node3PortNumber, helperAddress, nodeName);

        try
        {
            _helper.createHaVHN(node3Attributes);
            fail("The VHN should not be permitted to join the group");
        }
        catch(IllegalConfigurationException e)
        {
            assertEquals("Unexpected exception message",
                                String.format("Node using address '%s' is not permitted to join the group 'group'", "localhost:" + node3PortNumber, groupName),
                                e.getMessage());
        }
    }

    @Test
    public void testCurrentNodeCannotBeRemovedFromPermittedNodeList() throws Exception
    {
        int node1PortNumber = _portHelper.getNextAvailable();
        int node2PortNumber = _portHelper.getNextAvailable();
        int node3PortNumber = _portHelper.getNextAvailable();

        String node1Address = "localhost:" + node1PortNumber;
        String node2Address = "localhost:" + node2PortNumber;
        String node3Address = "localhost:" + node3PortNumber;

        String groupName = "group";
        String node1Name = "node1";

        Map<String, Object> node1Attributes = _helper.createNodeAttributes(node1Name, groupName, node1Address, node1Address, node1Name, node1PortNumber, node2PortNumber, node3PortNumber);
        BDBHAVirtualHostNode<?> node1 = _helper.createAndStartHaVHN(node1Attributes);

        Map<String, Object> node2Attributes = _helper.createNodeAttributes("node2", groupName, node2Address, node1Address, node1Name);
        BDBHAVirtualHostNode<?> node2 = _helper.createAndStartHaVHN(node2Attributes);

        Map<String, Object> node3Attributes = _helper.createNodeAttributes("node3", groupName, node3Address, node1Address, node1Name);
        BDBHAVirtualHostNode<?> node3 = _helper.createAndStartHaVHN(node3Attributes);

        _helper.awaitRemoteNodes(node1, 2);

        // Create new "proposed" permitted nodes list with a current node missing
        List<String> amendedPermittedNodes = new ArrayList<String>();
        amendedPermittedNodes.add(node1Address);
        amendedPermittedNodes.add(node2Address);

        // Try to update the permitted nodes attributes using the new list
        try
        {
            node1.setAttributes(Collections.<String, Object>singletonMap(BDBHAVirtualHostNode.PERMITTED_NODES, amendedPermittedNodes));
            fail("Operation to remove current group node from permitted nodes should have failed");
        }
        catch(IllegalArgumentException e)
        {
            assertEquals("Unexpected exception message",
                                String.format("The current group node '%s' cannot be removed from '%s' as its already a group member", node3Address, BDBHAVirtualHostNode.PERMITTED_NODES),
                                e.getMessage());
        }
    }

    @Test
    public void testPermittedNodesAttributeModificationConditions() throws Exception
    {
        int node1PortNumber = _portHelper.getNextAvailable();
        int node2PortNumber = _portHelper.getNextAvailable();
        int node3PortNumber = _portHelper.getNextAvailable();
        int node4PortNumber = _portHelper.getNextAvailable();
        int node5PortNumber = _portHelper.getNextAvailable();

        String node1Address = "localhost:" + node1PortNumber;
        String node2Address = "localhost:" + node2PortNumber;
        String node3Address = "localhost:" + node3PortNumber;
        String node4Address = "localhost:" + node4PortNumber;
        String node5Address = "localhost:" + node5PortNumber;

        String groupName = "group";
        String node1Name = "node1";

        Map<String, Object> node1Attributes = _helper.createNodeAttributes(node1Name, groupName, node1Address, node1Address, node1Name, node1PortNumber, node2PortNumber, node3PortNumber);
        BDBHAVirtualHostNode<?> node1 = _helper.createAndStartHaVHN(node1Attributes);

        Map<String, Object> node2Attributes = _helper.createNodeAttributes("node2", groupName, node2Address, node1Address, node1Name);
        BDBHAVirtualHostNode<?> node2 = _helper.createAndStartHaVHN(node2Attributes);

        Map<String, Object> node3Attributes = _helper.createNodeAttributes("node3", groupName, node3Address, node1Address, node1Name);
        BDBHAVirtualHostNode<?> node3 = _helper.createAndStartHaVHN(node3Attributes);

        _helper.awaitRemoteNodes(node1, 2);

        // Create new "proposed" permitted nodes list for update
        List<String> amendedPermittedNodes = new ArrayList<String>();
        amendedPermittedNodes.add(node1Address);
        amendedPermittedNodes.add(node2Address);
        amendedPermittedNodes.add(node3Address);
        amendedPermittedNodes.add(node4Address);

        // Try to update the permitted nodes attributes using the new list on REPLICA - should fail
        BDBHAVirtualHostNode<?> nonMasterNode = _helper.findNodeInRole(NodeRole.REPLICA);
        try
        {
            nonMasterNode.setAttributes(Collections.<String, Object>singletonMap(BDBHAVirtualHostNode.PERMITTED_NODES, amendedPermittedNodes));
            fail("Operation to update permitted nodes should have failed from non MASTER node");
        }
        catch(IllegalArgumentException e)
        {
            assertEquals("Unexpected exception message",
                                String.format("Attribute '%s' can only be set on '%s' node or node in '%s' or '%s' state", BDBHAVirtualHostNode.PERMITTED_NODES, NodeRole.MASTER, State.STOPPED, State.ERRORED),
                                e.getMessage());
        }

        // Try to update the permitted nodes attributes using the new list on MASTER - should succeed
        BDBHAVirtualHostNode<?> masterNode = _helper.findNodeInRole(NodeRole.MASTER);
        masterNode.setAttributes(Collections.<String, Object>singletonMap(BDBHAVirtualHostNode.PERMITTED_NODES, amendedPermittedNodes));

        // Try to update the permitted nodes attributes using the new list on a STOPPED node - should succeed
        nonMasterNode.stop();
        amendedPermittedNodes.add(node5Address);
        nonMasterNode.setAttributes(Collections.<String, Object>singletonMap(BDBHAVirtualHostNode.PERMITTED_NODES, amendedPermittedNodes));
    }

    @Test
    public void testNodeCannotStartWithIntruder() throws Exception
    {
        int nodePortNumber = _portHelper.getNextAvailable();
        int intruderPortNumber = _portHelper.getNextAvailable();

        String helperAddress = "localhost:" + nodePortNumber;
        String groupName = "group";
        String nodeName = "node";

        Map<String, Object> node1Attributes = _helper.createNodeAttributes(nodeName, groupName, helperAddress, helperAddress, nodeName, nodePortNumber);
        BDBHAVirtualHostNode<?> node = _helper.createAndStartHaVHN(node1Attributes);
        final CountDownLatch stopLatch = new CountDownLatch(1);
        ConfigurationChangeListener listener = new AbstractConfigurationChangeListener()
        {
            @Override
            public void stateChanged(ConfiguredObject<?> object, State oldState, State newState)
            {
                if (newState == State.ERRORED)
                {
                    stopLatch.countDown();
                }
            }
        };
        node.addChangeListener(listener);

        File environmentPathFile = new File(_helper.getMessageStorePath() + File.separator + "intruder");
        Durability durability = Durability.parse((String) node1Attributes.get(BDBHAVirtualHostNode.DURABILITY));
        joinIntruder(intruderPortNumber, "intruder", groupName, helperAddress, durability, environmentPathFile);

        assertTrue("Intruder protection was not triggered during expected timeout",
                          stopLatch.await(10, TimeUnit.SECONDS));

        final CountDownLatch stateChangeLatch = new CountDownLatch(1);
        final CountDownLatch roleChangeLatch = new CountDownLatch(1);
        node.addChangeListener(new AbstractConfigurationChangeListener()
        {
            @Override
            public void stateChanged(final ConfiguredObject<?> object, final State oldState, final State newState)
            {
                if (newState == State.ERRORED)
                {
                    stateChangeLatch.countDown();
                }
            }

            @Override
            public void attributeSet(final ConfiguredObject<?> object,
                                     final String attributeName,
                                     final Object oldAttributeValue,
                                     final Object newAttributeValue)
            {
                if (BDBHAVirtualHostNode.ROLE.equals(attributeName) && NodeRole.DETACHED.equals(NodeRole.DETACHED))
                {
                    roleChangeLatch.countDown();
                }
            }
        });

        // Try top re start the ERRORED node and ensure exception is thrown
        try
        {
            node.start();
            fail("Restart of node should have thrown exception");
        }
        catch (IllegalStateException ise)
        {
            assertEquals("Unexpected exception when restarting node post intruder detection",
                                "Intruder node detected: " + "localhost:" + intruderPortNumber,
                                ise.getMessage());
        }

        // verify that intruder detection is triggered after restart and environment is closed
        assertTrue("Node state was not set to ERRORED", stateChangeLatch.await(10, TimeUnit.SECONDS));
        assertTrue("Node role was not set to DETACHED", roleChangeLatch.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void testIntruderProtectionInManagementMode() throws Exception
    {
        int nodePortNumber = _portHelper.getNextAvailable();
        int intruderPortNumber = _portHelper.getNextAvailable();

        String helperAddress = "localhost:" + nodePortNumber;
        String groupName = "group";
        String nodeName = "node";

        Map<String, Object> nodeAttributes = _helper.createNodeAttributes(nodeName,
                                                                          groupName,
                                                                          helperAddress,
                                                                          helperAddress,
                                                                          nodeName,
                                                                          nodePortNumber);
        BDBHAVirtualHostNode<?> node = _helper.createAndStartHaVHN(nodeAttributes);

        final CountDownLatch stopLatch = new CountDownLatch(1);
        ConfigurationChangeListener listener = new AbstractConfigurationChangeListener()
        {
            @Override
            public void stateChanged(ConfiguredObject<?> object, State oldState, State newState)
            {
                if (newState == State.ERRORED)
                {
                    stopLatch.countDown();
                }
            }
        };
        node.addChangeListener(listener);

        File environmentPathFile = new File(_helper.getMessageStorePath() + File.separator + "intruder");
        Durability durability = Durability.parse((String) nodeAttributes.get(BDBHAVirtualHostNode.DURABILITY));
        joinIntruder(intruderPortNumber, "intruder", groupName, helperAddress, durability, environmentPathFile);

        LOGGER.debug("Permitted and intruder nodes are created");

        assertTrue("Intruder protection was not triggered during expected timeout",
                          stopLatch.await(10, TimeUnit.SECONDS));

        LOGGER.debug("Master node transited into ERRORED state due to intruder protection");
        when(_helper.getBroker().isManagementMode()).thenReturn(true);

        LOGGER.debug("Starting node in management mode");

        final CountDownLatch stateChangeLatch = new CountDownLatch(1);
        final CountDownLatch roleChangeLatch = new CountDownLatch(1);
        node.addChangeListener(new AbstractConfigurationChangeListener()
        {
            @Override
            public void stateChanged(final ConfiguredObject<?> object, final State oldState, final State newState)
            {
                if (newState == State.ERRORED)
                {
                    stateChangeLatch.countDown();
                }
            }

            @Override
            public void attributeSet(final ConfiguredObject<?> object,
                                     final String attributeName,
                                     final Object oldAttributeValue,
                                     final Object newAttributeValue)
            {
                if (BDBHAVirtualHostNode.ROLE.equals(attributeName) && NodeRole.DETACHED.equals(NodeRole.DETACHED))
                {
                    roleChangeLatch.countDown();
                }
            }
        });
        node.start();
        LOGGER.debug("Node is started");

        // verify that intruder detection is triggered after restart and environment is closed
        assertTrue("Node state was not set to ERRORED", stateChangeLatch.await(10, TimeUnit.SECONDS));
        assertTrue("Node role was not set to DETACHED", roleChangeLatch.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void testPermittedNodesChangedOnReplicaNodeOnlyOnceAfterBeingChangedOnMaster() throws Exception
    {
        int node1PortNumber = _portHelper.getNextAvailable();
        int node2PortNumber = _portHelper.getNextAvailable();

        String helperAddress = "localhost:" + node1PortNumber;
        String groupName = "group";
        String nodeName = "node1";

        Map<String, Object> node1Attributes = _helper.createNodeAttributes(nodeName, groupName, helperAddress, helperAddress, nodeName, node1PortNumber, node2PortNumber);
        BDBHAVirtualHostNode<?> node1 = _helper.createAndStartHaVHN(node1Attributes);

        Map<String, Object> node2Attributes = _helper.createNodeAttributes("node2", groupName, "localhost:" + node2PortNumber, helperAddress, nodeName);
        node2Attributes.put(BDBHAVirtualHostNode.PRIORITY, 0);
        BDBHAVirtualHostNode<?> node2 = _helper.createAndStartHaVHN(node2Attributes);
        assertEquals("Unexpected role", NodeRole.REPLICA, node2.getRole());
        _helper.awaitRemoteNodes(node2, 1);

        BDBHARemoteReplicationNode<?> remote = _helper.findRemoteNode(node2, node1.getName());

        final AtomicInteger permittedNodesChangeCounter = new AtomicInteger();
        final CountDownLatch _permittedNodesLatch = new CountDownLatch(1);
        node2.addChangeListener(new AbstractConfigurationChangeListener()
        {
            @Override
            public void attributeSet(ConfiguredObject<?> object, String attributeName, Object oldAttributeValue, Object newAttributeValue)
            {
                if (attributeName.equals(BDBHAVirtualHostNode.PERMITTED_NODES))
                {
                    permittedNodesChangeCounter.incrementAndGet();
                    _permittedNodesLatch.countDown();
                }
            }
        });
        List<String> permittedNodes = new ArrayList<>(node1.getPermittedNodes());
        permittedNodes.add("localhost:5000");
        node1.setAttributes(Collections.<String, Object>singletonMap(BDBHAVirtualHostNode.PERMITTED_NODES, permittedNodes));

        assertTrue("Permitted nodes were not changed on Replica",
                          _permittedNodesLatch.await(10, TimeUnit.SECONDS));
        assertEquals("Not the same permitted nodes",
                            new HashSet<>(node1.getPermittedNodes()),
                            new HashSet<>(node2.getPermittedNodes()));
        assertEquals("Unexpected counter of changes permitted nodes",
                            (long) 1,
                            (long) permittedNodesChangeCounter.get());

        // change the order of permitted nodes
        Collections.swap(permittedNodes, 0, 2);
        node1.setAttributes(Collections.<String, Object>singletonMap(BDBHAVirtualHostNode.PERMITTED_NODES, permittedNodes));

        // make sure that node2 onNodeState was invoked by performing transaction on master and making sure that it was replicated
        performTransactionAndAwaitForRemoteNodeToGetAware(node1, remote);

        // perform transaction second time because permitted nodes are changed after last transaction id
        performTransactionAndAwaitForRemoteNodeToGetAware(node1, remote);
        assertEquals("Unexpected counter of changes permitted nodes",
                            (long) 1,
                            (long) permittedNodesChangeCounter.get());
    }

    private void performTransactionAndAwaitForRemoteNodeToGetAware(BDBHAVirtualHostNode<?> node1, BDBHARemoteReplicationNode<?> remote) throws InterruptedException
    {
        new DatabasePinger().pingDb(((BDBConfigurationStore)node1.getConfigurationStore()).getEnvironmentFacade());

        int waitCounter = 100;
        while ( remote.getLastKnownReplicationTransactionId() != node1.getLastKnownReplicationTransactionId() && (waitCounter--) != 0)
        {
            Thread.sleep(100l);
        }
        assertEquals("Last transaction was not replicated",
                            new Long(remote.getLastKnownReplicationTransactionId()),
                            node1.getLastKnownReplicationTransactionId());
    }

    @Test
    public void testIntruderConnected() throws Exception
    {
        int node1PortNumber = _portHelper.getNextAvailable();
        int node2PortNumber = _portHelper.getNextAvailable();

        String helperAddress = "localhost:" + node1PortNumber;
        String groupName = "group";
        String nodeName = "node1";

        Map<String, Object> node1Attributes = _helper.createNodeAttributes(nodeName, groupName, helperAddress, helperAddress, nodeName, node1PortNumber);
        BDBHAVirtualHostNode<?> node1 = _helper.createAndStartHaVHN(node1Attributes);

        final CountDownLatch stopLatch = new CountDownLatch(1);
        ConfigurationChangeListener listener = new AbstractConfigurationChangeListener()
        {
            @Override
            public void stateChanged(ConfiguredObject<?> object, State oldState, State newState)
            {
                if (newState == State.ERRORED)
                {
                    stopLatch.countDown();
                }
            }
        };
        node1.addChangeListener(listener);

        String node2Name = "node2";
        File environmentPathFile = new File(_helper.getMessageStorePath() + File.separator + node2Name);
        Durability durability = Durability.parse((String) node1Attributes.get(BDBHAVirtualHostNode.DURABILITY));
        joinIntruder(node2PortNumber, node2Name, groupName, helperAddress, durability, environmentPathFile);

        assertTrue("Intruder protection was not triggered during expected timeout",
                          stopLatch.await(20, TimeUnit.SECONDS));
    }

    private void joinIntruder(final int nodePortNumber,
                              final String nodeName,
                              final String groupName,
                              final String helperAddress,
                              final Durability durability,
                              final File environmentPathFile)
    {
        environmentPathFile.mkdirs();

        ReplicationConfig replicationConfig = new ReplicationConfig(groupName, nodeName, "localhost:" + nodePortNumber );
        replicationConfig.setNodePriority(0);
        replicationConfig.setHelperHosts(helperAddress);
        replicationConfig.setConsistencyPolicy(NoConsistencyRequiredPolicy.NO_CONSISTENCY);
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        envConfig.setDurability(durability);

        ReplicatedEnvironment intruder = null;
        String originalThreadName = Thread.currentThread().getName();
        try
        {
            intruder = new ReplicatedEnvironment(environmentPathFile, replicationConfig, envConfig);
        }
        finally
        {
            try
            {
                if (intruder != null)
                {
                    intruder.close();
                }
            }
            finally
            {
                Thread.currentThread().setName(originalThreadName);
            }
        }
    }

    @Test
    public void testValidateOnCreateForNonExistingHelperNode() throws Exception
    {
        int node1PortNumber = findFreePort();
        int node2PortNumber = getNextAvailable(node1PortNumber + 1);


        Map<String, Object> attributes = _helper.createNodeAttributes("node1", "group", "localhost:" + node1PortNumber,
                "localhost:" + node2PortNumber, "node2", node1PortNumber, node1PortNumber, node2PortNumber);
        try
        {
            _helper.createAndStartHaVHN(attributes);
            fail("Node creation should fail because of invalid helper address");
        }
        catch(ExternalServiceException e)
        {
            assertEquals("Unexpected exception on connection to non-existing helper address",
                                String.format("Cannot connect to existing node '%s' at '%s'", "node2", "localhost:" + node2PortNumber),
                                e.getMessage());
        }
    }

    @Test
    public void testValidateOnCreateForAlreadyBoundAddress() throws Exception
    {
        try(ServerSocket serverSocket = new ServerSocket())
        {
            serverSocket.setReuseAddress(true);
            serverSocket.bind(new InetSocketAddress("localhost", 0));
            int node1PortNumber = serverSocket.getLocalPort();

            Map<String, Object> attributes = _helper.createNodeAttributes("node1", "group", "localhost:" + node1PortNumber,
                    "localhost:" + node1PortNumber, "node2", node1PortNumber, node1PortNumber);
            try
            {
                _helper.createAndStartHaVHN(attributes);
                fail("Node creation should fail because of invalid address");
            }
            catch(IllegalConfigurationException e)
            {
                assertEquals("Unexpected exception on attempt to create node with already bound address",
                                    String.format("Cannot bind to address '%s'. Address is already in use.", "localhost:" + node1PortNumber),
                                    e.getMessage());
            }
        }
    }

    @Test
    public void testValidateOnCreateForInvalidStorePath() throws Exception
    {
        int node1PortNumber = 0;

        File storeBaseFolder = TestFileUtils.createTestDirectory();
        File file = new File(storeBaseFolder, getTestName());
        file.createNewFile();
        File storePath = new File(file, "test");
        try
        {
            Map<String, Object> attributes = _helper.createNodeAttributes("node1", "group", "localhost:" + node1PortNumber,
                    "localhost:" + node1PortNumber, "node2", node1PortNumber, node1PortNumber);
            attributes.put(BDBHAVirtualHostNode.STORE_PATH, storePath.getAbsoluteFile());
            try
            {
                _helper.createAndStartHaVHN(attributes);
                fail("Node creation should fail because of invalid store path");
            }
            catch (IllegalConfigurationException e)
            {
                assertEquals("Unexpected exception on attempt to create environment in invalid location",
                                    String.format("Store path '%s' is not a folder", storePath.getAbsoluteFile()),
                                    e.getMessage());
            }
        }
        finally
        {
            FileUtils.delete(storeBaseFolder, true);
        }
    }
}
