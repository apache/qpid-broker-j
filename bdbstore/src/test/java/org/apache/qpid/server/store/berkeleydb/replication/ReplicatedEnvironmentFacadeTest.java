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
package org.apache.qpid.server.store.berkeleydb.replication;

import static org.apache.qpid.server.store.berkeleydb.EnvironmentFacade.JUL_LOGGER_LEVEL_OVERRIDE;
import static org.apache.qpid.server.store.berkeleydb.EnvironmentFacade.LOG_HANDLER_CLEANER_PROTECTED_FILES_LIMIT_PROPERTY_NAME;
import static org.apache.qpid.server.store.berkeleydb.replication.ReplicatedEnvironmentFacade.*;
import static org.apache.qpid.server.virtualhost.berkeleydb.BDBVirtualHost.DEFAULT_QPID_BROKER_BDB_COMMITER_NOTIFY_THRESHOLD;
import static org.apache.qpid.server.virtualhost.berkeleydb.BDBVirtualHost.DEFAULT_QPID_BROKER_BDB_COMMITER_WAIT_TIMEOUT;
import static org.apache.qpid.server.virtualhost.berkeleydb.BDBVirtualHost.QPID_BROKER_BDB_COMMITER_NOTIFY_THRESHOLD;
import static org.apache.qpid.server.virtualhost.berkeleydb.BDBVirtualHost.QPID_BROKER_BDB_COMMITER_WAIT_TIMEOUT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.google.common.util.concurrent.SettableFuture;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Durability;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.rep.NoConsistencyRequiredPolicy;
import com.sleepycat.je.rep.NodeState;
import com.sleepycat.je.rep.ReplicaWriteException;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.ReplicationNode;
import com.sleepycat.je.rep.StateChangeListener;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.store.berkeleydb.EnvironmentFacade;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.server.util.FileUtils;
import org.apache.qpid.test.utils.PortHelper;
import org.apache.qpid.test.utils.TestFileUtils;
import org.apache.qpid.test.utils.UnitTestBase;
import org.apache.qpid.test.utils.VirtualHostNodeStoreType;

public class ReplicatedEnvironmentFacadeTest extends UnitTestBase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicatedEnvironmentFacadeTest.class);

    private final PortHelper _portHelper = new PortHelper();
    private final String TEST_GROUP_NAME = "testGroupName";
    private final String TEST_NODE_NAME = "testNodeName";
    private final int TEST_NODE_PORT = _portHelper.getNextAvailable();
    private final String TEST_NODE_HOST_PORT = "localhost:" + TEST_NODE_PORT;
    private final String TEST_NODE_HELPER_HOST_PORT = TEST_NODE_HOST_PORT;
    private final Durability TEST_DURABILITY = Durability.parse("SYNC,NO_SYNC,SIMPLE_MAJORITY");
    private final boolean TEST_DESIGNATED_PRIMARY = false;
    private final int TEST_PRIORITY = 1;
    private final int TEST_ELECTABLE_GROUP_OVERRIDE = 0;

    private int _timeout = 30;
    private File _storePath;
    private Map<String, ReplicatedEnvironmentFacade> _nodes;
    private Thread.UncaughtExceptionHandler _defaultUncaughtExceptionHandler;
    private CopyOnWriteArrayList<Throwable> _unhandledExceptions;

    @BeforeEach
    public void setUp() throws Exception
    {
        assumeTrue(Objects.equals(getVirtualHostNodeStoreType(), VirtualHostNodeStoreType.BDB),
                "VirtualHostNodeStoreType should be BDB");
        _timeout = Integer.getInteger("ReplicatedEnvironmentFacadeTest.timeout", 30);

        _nodes = new HashMap<>();
        _unhandledExceptions = new CopyOnWriteArrayList<>();

        _defaultUncaughtExceptionHandler = Thread.getDefaultUncaughtExceptionHandler();
        Thread.setDefaultUncaughtExceptionHandler((t, e) ->
                                                  {
                                                      LOGGER.error("Unhandled exception in thread " + t, e);
                                                      _unhandledExceptions.add(e);
                                                  });
        _storePath = TestFileUtils.createTestDirectory("bdb", true);

        setTestSystemProperty(DB_PING_SOCKET_TIMEOUT_PROPERTY_NAME, "100");
    }

    @AfterEach
    public void tearDown()
    {
        try
        {
            if (_nodes != null)
            {
                for (final EnvironmentFacade ef : _nodes.values())
                {
                    ef.close();
                }
            }
        }
        finally
        {
            if (_defaultUncaughtExceptionHandler != null)
            {
                Thread.setDefaultUncaughtExceptionHandler(_defaultUncaughtExceptionHandler);
            }

            try
            {
                if (_storePath != null)
                {
                    FileUtils.delete(_storePath, true);
                }
            }
            finally
            {
                if (_unhandledExceptions != null && !_unhandledExceptions.isEmpty())
                {
                    fail("Unhandled exception(s) detected:" + _unhandledExceptions);
                }
            }
        }

        _portHelper.waitUntilAllocatedPortsAreFree();
    }

    @Test
    public void testClose() throws Exception
    {
        ReplicatedEnvironmentFacade ef = createMaster();
        ef.close();
        assertEquals(ReplicatedEnvironmentFacade.State.CLOSED, ef.getFacadeState(), "Unexpected state after close");

    }

    @Test
    public void testOpenDatabaseReusesCachedHandle() throws Exception
    {
        DatabaseConfig createIfAbsentDbConfig = DatabaseConfig.DEFAULT.setAllowCreate(true);

        EnvironmentFacade ef = createMaster();
        Database handle1 = ef.openDatabase("myDatabase", createIfAbsentDbConfig);
        assertNotNull(handle1);

        Database handle2 = ef.openDatabase("myDatabase", createIfAbsentDbConfig);
        assertSame(handle1, handle2, "Database handle should be cached");

        ef.closeDatabase("myDatabase");

        Database handle3 = ef.openDatabase("myDatabase", createIfAbsentDbConfig);
        assertNotSame(handle1, handle3, "Expecting a new handle after database closure");
    }

    @Test
    public void testOpenDatabaseWhenFacadeIsNotOpened() throws Exception
    {
        DatabaseConfig createIfAbsentDbConfig = DatabaseConfig.DEFAULT.setAllowCreate(true);

        EnvironmentFacade ef = createMaster();
        ef.close();

        try
        {
            ef.openDatabase("myDatabase", createIfAbsentDbConfig );
            fail("Database open should fail");
        }
        catch(ConnectionScopedRuntimeException e)
        {
            assertEquals("Environment facade is not in opened state", e.getMessage(), "Unexpected exception");
        }
    }

    @Test
    public void testGetGroupName() throws Exception
    {
        assertEquals(TEST_GROUP_NAME, createMaster().getGroupName(), "Unexpected group name");
    }

    @Test
    public void testGetNodeName() throws Exception
    {
        assertEquals(TEST_NODE_NAME, createMaster().getNodeName(), "Unexpected group name");
    }

    @Test
    public void testLastKnownReplicationTransactionId() throws Exception
    {
        ReplicatedEnvironmentFacade master = createMaster();
        long lastKnownReplicationTransactionId = master.getLastKnownReplicationTransactionId();
        assertTrue(lastKnownReplicationTransactionId > 0,
                   "Unexpected LastKnownReplicationTransactionId " + lastKnownReplicationTransactionId);

    }

    @Test
    public void testGetNodeHostPort() throws Exception
    {
        assertEquals(TEST_NODE_HOST_PORT, createMaster().getHostPort(), "Unexpected node host port");
    }

    @Test
    public void testGetHelperHostPort() throws Exception
    {
        assertEquals(TEST_NODE_HELPER_HOST_PORT, createMaster().getHelperHostPort(),
                "Unexpected node helper host port");
    }

    @Test
    public void testSetMessageStoreDurability() throws Exception
    {
        ReplicatedEnvironmentFacade master = createMaster();
        assertEquals(new Durability(Durability.SyncPolicy.NO_SYNC, Durability.SyncPolicy.NO_SYNC,
                        Durability.ReplicaAckPolicy.SIMPLE_MAJORITY),
                master.getRealMessageStoreDurability(), "Unexpected message store durability");
        assertEquals(TEST_DURABILITY, master.getMessageStoreDurability(), "Unexpected durability");
        assertTrue(master.isCoalescingSync(), "Unexpected coalescing sync");

        master.setMessageStoreDurability(Durability.SyncPolicy.WRITE_NO_SYNC, Durability.SyncPolicy.SYNC, Durability.ReplicaAckPolicy.ALL);
        assertEquals(new Durability(Durability.SyncPolicy.WRITE_NO_SYNC, Durability.SyncPolicy.SYNC,
                        Durability.ReplicaAckPolicy.ALL),
                master.getRealMessageStoreDurability(), "Unexpected message store durability");
        assertFalse(master.isCoalescingSync(), "Coalescing sync committer is still running");
    }

    @Test
    public void testSetMessageStoreDurabilityWithDisabledCoalescingSync() throws Exception
    {
        ReplicatedEnvironmentFacade master = createMaster(true);
        assertEquals(new Durability(Durability.SyncPolicy.NO_SYNC, Durability.SyncPolicy.NO_SYNC,
                        Durability.ReplicaAckPolicy.SIMPLE_MAJORITY),
                master.getRealMessageStoreDurability(), "Unexpected message store durability");
        assertTrue(master.isCoalescingSync(), "Unexpected coalescing sync");

        master.setMessageStoreDurability(Durability.SyncPolicy.NO_SYNC,
                                         Durability.SyncPolicy.WRITE_NO_SYNC,
                                         Durability.ReplicaAckPolicy.ALL);
        assertEquals(new Durability(Durability.SyncPolicy.NO_SYNC, Durability.SyncPolicy.WRITE_NO_SYNC,
                        Durability.ReplicaAckPolicy.ALL),
                master.getRealMessageStoreDurability(), "Unexpected message store durability");
        assertFalse(master.isCoalescingSync(), "Coalescing sync committer is still running");
    }

    @Test
    public void testGetNodeState() throws Exception
    {
        assertEquals(State.MASTER.name(), createMaster().getNodeState(), "Unexpected state");
    }

    @Test
    public void testPriority() throws Exception
    {
        final TestStateChangeListener masterListener = new TestStateChangeListener();
        final ReplicationGroupListener masterGroupListener = new NoopReplicationGroupListener();

        ReplicatedEnvironmentConfiguration masterConfig = createReplicatedEnvironmentConfiguration(TEST_NODE_NAME, TEST_NODE_HOST_PORT, TEST_DESIGNATED_PRIMARY);
        ReplicatedEnvironmentFacade facade = createReplicatedEnvironmentFacade(TEST_NODE_NAME, masterListener, masterGroupListener, masterConfig);
        assertTrue(masterListener.awaitForStateChange(State.MASTER, _timeout, TimeUnit.SECONDS),
                "Master was not created");

        assertEquals(TEST_PRIORITY, (long) facade.getPriority(), "Unexpected priority");

        int newPriority = TEST_PRIORITY + 1;
        when(masterConfig.getPriority()).thenReturn(newPriority);
        Future<Void> future = facade.reapplyPriority();

        future.get(_timeout, TimeUnit.SECONDS);
        assertEquals(newPriority, (long) facade.getPriority(), "Unexpected priority after change");
    }

    @Test
    public void testDesignatedPrimary()  throws Exception
    {
        final TestStateChangeListener masterListener = new TestStateChangeListener();
        final ReplicationGroupListener masterGroupListener = new NoopReplicationGroupListener();

        ReplicatedEnvironmentConfiguration masterConfig = createReplicatedEnvironmentConfiguration(TEST_NODE_NAME, TEST_NODE_HOST_PORT, TEST_DESIGNATED_PRIMARY);
        ReplicatedEnvironmentFacade master = createReplicatedEnvironmentFacade(TEST_NODE_NAME, masterListener, masterGroupListener, masterConfig);
        assertTrue(masterListener.awaitForStateChange(State.MASTER, _timeout, TimeUnit.SECONDS),
                "Master was not created");

        assertEquals(TEST_DESIGNATED_PRIMARY, master.isDesignatedPrimary(), "Unexpected designated primary");
        boolean newDesignatedPrimary = !TEST_DESIGNATED_PRIMARY;
        when(masterConfig.isDesignatedPrimary()).thenReturn(newDesignatedPrimary);
        Future<Void> future = master.reapplyDesignatedPrimary();
        future.get(_timeout, TimeUnit.SECONDS);
        assertEquals(newDesignatedPrimary, master.isDesignatedPrimary(),
                "Unexpected designated primary after change");
    }

    @Test
    public void testElectableGroupSizeOverride() throws Exception
    {
        final TestStateChangeListener masterListener = new TestStateChangeListener();
        final ReplicationGroupListener masterGroupListener = new NoopReplicationGroupListener();

        ReplicatedEnvironmentConfiguration masterConfig = createReplicatedEnvironmentConfiguration(TEST_NODE_NAME, TEST_NODE_HOST_PORT, false);
        ReplicatedEnvironmentFacade facade = createReplicatedEnvironmentFacade(TEST_NODE_NAME, masterListener, masterGroupListener, masterConfig);
        assertTrue(masterListener.awaitForStateChange(State.MASTER, _timeout, TimeUnit.SECONDS),
                "Master was not created");

        assertEquals(TEST_ELECTABLE_GROUP_OVERRIDE, (long) facade.getElectableGroupSizeOverride(),
                "Unexpected Electable Group Size Override");


        int newElectableGroupOverride = TEST_ELECTABLE_GROUP_OVERRIDE + 1;
        when(masterConfig.getQuorumOverride()).thenReturn(newElectableGroupOverride);
        Future<Void> future = facade.reapplyElectableGroupSizeOverride();

        future.get(_timeout, TimeUnit.SECONDS);
        assertEquals(newElectableGroupOverride, (long) facade.getElectableGroupSizeOverride(),
                     "Unexpected Electable Group Size Override after change");
    }

    @Test
    public void testReplicationGroupListenerHearsAboutExistingRemoteReplicationNodes() throws Exception
    {
        ReplicatedEnvironmentFacade master = createMaster();
        String nodeName2 = TEST_NODE_NAME + "_2";
        String host = "localhost";
        int port = _portHelper.getNextAvailable();
        String node2NodeHostPort = host + ":" + port;

        final AtomicInteger invocationCount = new AtomicInteger();
        final CountDownLatch nodeRecoveryLatch = new CountDownLatch(1);
        ReplicationGroupListener listener = new NoopReplicationGroupListener()
        {
            @Override
            public void onReplicationNodeRecovered(ReplicationNode node)
            {
                nodeRecoveryLatch.countDown();
                invocationCount.incrementAndGet();
            }
        };

        createReplica(nodeName2, node2NodeHostPort, listener);

        assertEquals(2, (long) master.getNumberOfElectableGroupMembers(), "Unexpected number of nodes");

        assertTrue(nodeRecoveryLatch.await(_timeout, TimeUnit.SECONDS), "Listener not fired within timeout");
        assertEquals(1, (long) invocationCount.get(), "Unexpected number of listener invocations");
    }

    @Test
    public void testReplicationGroupListenerHearsNodeAdded() throws Exception
    {
        final CountDownLatch nodeAddedLatch = new CountDownLatch(1);
        final AtomicInteger invocationCount = new AtomicInteger();
        ReplicationGroupListener listener = new NoopReplicationGroupListener()
        {
            @Override
            public void onReplicationNodeAddedToGroup(ReplicationNode node)
            {
                invocationCount.getAndIncrement();
                nodeAddedLatch.countDown();
            }
        };

        TestStateChangeListener stateChangeListener = new TestStateChangeListener();
        ReplicatedEnvironmentFacade replicatedEnvironmentFacade = addNode(stateChangeListener, listener);
        assertTrue(stateChangeListener.awaitForStateChange(State.MASTER, _timeout, TimeUnit.SECONDS),
                "Master was not started");

        assertEquals(1, replicatedEnvironmentFacade.getNumberOfElectableGroupMembers(),
                "Unexpected number of nodes at start of test");

        String node2Name = TEST_NODE_NAME + "_2";
        String node2NodeHostPort = "localhost" + ":" + _portHelper.getNextAvailable();
        replicatedEnvironmentFacade.setPermittedNodes(Arrays.asList(replicatedEnvironmentFacade.getHostPort(), node2NodeHostPort));
        createReplica(node2Name, node2NodeHostPort, new NoopReplicationGroupListener());

        assertTrue(nodeAddedLatch.await(_timeout, TimeUnit.SECONDS), "Listener not fired within timeout");

        assertEquals(2, replicatedEnvironmentFacade.getNumberOfElectableGroupMembers(), "Unexpected number of nodes");

        assertEquals(1, invocationCount.get(), "Unexpected number of listener invocations");
    }

    @Test
    public void testReplicationGroupListenerHearsNodeRemoved() throws Exception
    {
        final CountDownLatch nodeDeletedLatch = new CountDownLatch(1);
        final CountDownLatch nodeAddedLatch = new CountDownLatch(1);
        final AtomicInteger invocationCount = new AtomicInteger();
        ReplicationGroupListener listener = new NoopReplicationGroupListener()
        {
            @Override
            public void onReplicationNodeRecovered(ReplicationNode node)
            {
                nodeAddedLatch.countDown();
            }

            @Override
            public void onReplicationNodeAddedToGroup(ReplicationNode node)
            {
                nodeAddedLatch.countDown();
            }

            @Override
            public void onReplicationNodeRemovedFromGroup(ReplicationNode node)
            {
                invocationCount.getAndIncrement();
                nodeDeletedLatch.countDown();
            }
        };

        TestStateChangeListener stateChangeListener = new TestStateChangeListener();
        ReplicatedEnvironmentFacade replicatedEnvironmentFacade = addNode(stateChangeListener, listener);
        // Set the node to be primary so that the node will remain master even when the 2nd node is shutdown
        replicatedEnvironmentFacade.reapplyDesignatedPrimary();
        assertTrue(stateChangeListener.awaitForStateChange(State.MASTER, _timeout, TimeUnit.SECONDS),
                "Master was not started");

        String node2Name = TEST_NODE_NAME + "_2";
        String node2NodeHostPort = "localhost" + ":" + _portHelper.getNextAvailable();
        replicatedEnvironmentFacade.setPermittedNodes(Arrays.asList(replicatedEnvironmentFacade.getHostPort(), node2NodeHostPort));
        createReplica(node2Name, node2NodeHostPort, new NoopReplicationGroupListener());

        assertEquals(2, replicatedEnvironmentFacade.getNumberOfElectableGroupMembers(),
                "Unexpected number of nodes at start of test");

        // Need to await the listener hearing the addition of the node to the model.
        assertTrue(nodeAddedLatch.await(_timeout, TimeUnit.SECONDS), "Node add not fired within timeout");

        // Now remove the node and ensure we hear the event
        replicatedEnvironmentFacade.removeNodeFromGroup(node2Name);

        assertTrue(nodeDeletedLatch.await(_timeout, TimeUnit.SECONDS), "Node delete not fired within timeout");

        assertEquals(1, replicatedEnvironmentFacade.getNumberOfElectableGroupMembers(),
                "Unexpected number of nodes after node removal");

        assertEquals(1, invocationCount.get(), "Unexpected number of listener invocations");
    }

    @Test
    public void testMasterHearsRemoteNodeRoles() throws Exception
    {
        final String node2Name = TEST_NODE_NAME + "_2";
        final CountDownLatch nodeAddedLatch = new CountDownLatch(1);
        final AtomicReference<ReplicationNode> nodeRef = new AtomicReference<>();
        final CountDownLatch stateLatch = new CountDownLatch(1);
        final AtomicReference<NodeState> stateRef = new AtomicReference<>();
        ReplicationGroupListener listener = new NoopReplicationGroupListener()
        {
            @Override
            public void onReplicationNodeAddedToGroup(ReplicationNode node)
            {
                nodeRef.set(node);
                nodeAddedLatch.countDown();
            }

            @Override
            public void onNodeState(ReplicationNode node, NodeState nodeState)
            {
                if (node2Name.equals(node.getName()))
                {
                    stateRef.set(nodeState);
                    stateLatch.countDown();
                }
            }
        };

        TestStateChangeListener stateChangeListener = new TestStateChangeListener();
        ReplicatedEnvironmentFacade replicatedEnvironmentFacade = addNode(stateChangeListener, listener);
        assertTrue(stateChangeListener.awaitForStateChange(State.MASTER, _timeout, TimeUnit.SECONDS),
                "Master was not started");

        String node2NodeHostPort = "localhost" + ":" + _portHelper.getNextAvailable();
        replicatedEnvironmentFacade.setPermittedNodes(Arrays.asList(replicatedEnvironmentFacade.getHostPort(), node2NodeHostPort));
        createReplica(node2Name, node2NodeHostPort, new NoopReplicationGroupListener());

        assertEquals(2, replicatedEnvironmentFacade.getNumberOfElectableGroupMembers(),
                "Unexpected number of nodes at start of test");

        assertTrue(nodeAddedLatch.await(_timeout, TimeUnit.SECONDS), "Node add not fired within timeout");

        ReplicationNode remoteNode = nodeRef.get();
        assertEquals(node2Name, remoteNode.getName(), "Unexpected node name");

        assertTrue(stateLatch.await(_timeout, TimeUnit.SECONDS), "Node state not fired within timeout");
        assertEquals(State.REPLICA, stateRef.get().getNodeState(), "Unexpected node state");
    }

    @Test
    public void testRemoveNodeFromGroup() throws Exception
    {
        TestStateChangeListener stateChangeListener = new TestStateChangeListener();
        ReplicatedEnvironmentFacade environmentFacade = addNode(TEST_NODE_NAME, TEST_NODE_HOST_PORT, true, stateChangeListener,
                                                                new NoopReplicationGroupListener());
        assertTrue(stateChangeListener.awaitForStateChange(State.MASTER, _timeout, TimeUnit.SECONDS),
                "Environment was not created");

        String node2Name = TEST_NODE_NAME + "_2";
        String node2NodeHostPort = "localhost:" + _portHelper.getNextAvailable();
        ReplicatedEnvironmentFacade ref2 = createReplica(node2Name, node2NodeHostPort, new NoopReplicationGroupListener());

        assertEquals(2, environmentFacade.getNumberOfElectableGroupMembers(), "Unexpected group members count");
        ref2.close();

        environmentFacade.removeNodeFromGroup(node2Name);
        assertEquals(1, environmentFacade.getNumberOfElectableGroupMembers(), "Unexpected group members count");
    }

    @Test
    public void testRemoveNodeFromGroupTwice() throws Exception
    {
        TestStateChangeListener stateChangeListener = new TestStateChangeListener();
        ReplicatedEnvironmentFacade environmentFacade = addNode(TEST_NODE_NAME,
                                                                TEST_NODE_HOST_PORT,
                                                                true,
                                                                stateChangeListener,
                                                                new NoopReplicationGroupListener());
        assertTrue(stateChangeListener.awaitForStateChange(State.MASTER, _timeout, TimeUnit.SECONDS),
                "Environment was not created");

        String node2Name = TEST_NODE_NAME + "_2";
        String node2NodeHostPort = "localhost:" + _portHelper.getNextAvailable();
        ReplicatedEnvironmentFacade ref2 =
                createReplica(node2Name, node2NodeHostPort, new NoopReplicationGroupListener());
        ref2.close();

        environmentFacade.removeNodeFromGroup(node2Name);
        try
        {
            environmentFacade.removeNodeFromGroup(node2Name);
            fail("Exception is expected");
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }
    }

    @Test
    public void testEnvironmentFacadeDetectsRemovalOfRemoteNode() throws Exception
    {
        final String replicaName = TEST_NODE_NAME + "_1";
        final CountDownLatch nodeRemovedLatch = new CountDownLatch(1);
        final CountDownLatch nodeAddedLatch = new CountDownLatch(1);
        final AtomicReference<ReplicationNode> addedNodeRef = new AtomicReference<>();
        final AtomicReference<ReplicationNode> removedNodeRef = new AtomicReference<>();
        final CountDownLatch stateLatch = new CountDownLatch(1);
        final AtomicReference<NodeState> stateRef = new AtomicReference<>();

        ReplicationGroupListener listener = new NoopReplicationGroupListener()
        {
            @Override
            public void onReplicationNodeAddedToGroup(ReplicationNode node)
            {
                if (addedNodeRef.compareAndSet(null, node))
                {
                    nodeAddedLatch.countDown();
                }
            }

            @Override
            public void onReplicationNodeRemovedFromGroup(ReplicationNode node)
            {
                removedNodeRef.set(node);
                nodeRemovedLatch.countDown();
            }

            @Override
            public void onNodeState(ReplicationNode node, NodeState nodeState)
            {
                if (replicaName.equals(node.getName()))
                {
                    stateRef.set(nodeState);
                    stateLatch.countDown();
                }
            }
        };

        TestStateChangeListener stateChangeListener = new TestStateChangeListener();
        final ReplicatedEnvironmentFacade masterEnvironment = addNode(stateChangeListener, listener);
        assertTrue(stateChangeListener.awaitForStateChange(State.MASTER, _timeout, TimeUnit.SECONDS),
                "Master was not started");

        masterEnvironment.reapplyDesignatedPrimary();

        int replica1Port = _portHelper.getNextAvailable();
        String node1NodeHostPort = "localhost:" + replica1Port;
        masterEnvironment.setPermittedNodes(Arrays.asList(masterEnvironment.getHostPort(), node1NodeHostPort));
        ReplicatedEnvironmentFacade replica = createReplica(replicaName, node1NodeHostPort,
                                                            new NoopReplicationGroupListener());

        assertTrue(nodeAddedLatch.await(_timeout, TimeUnit.SECONDS), "Node should be added");

        ReplicationNode node = addedNodeRef.get();
        assertEquals(replicaName, node.getName(), "Unexpected node name");

        assertTrue(stateLatch.await(_timeout, TimeUnit.SECONDS), "Node state was not heard");
        assertEquals(State.REPLICA, stateRef.get().getNodeState(), "Unexpected node role");
        assertEquals(replicaName, stateRef.get().getNodeName(), "Unexpected node name");

        replica.close();
        masterEnvironment.removeNodeFromGroup(node.getName());

        assertTrue(nodeRemovedLatch.await(_timeout, TimeUnit.SECONDS),
                "Node deleting is undetected by the environment facade");
        assertEquals(node, removedNodeRef.get(), "Unexpected node is deleted");
    }

    @Test
    public void testCloseStateTransitions() throws Exception
    {
        ReplicatedEnvironmentFacade replicatedEnvironmentFacade = createMaster();

        assertEquals(ReplicatedEnvironmentFacade.State.OPEN, replicatedEnvironmentFacade.getFacadeState(),
                "Unexpected state " + replicatedEnvironmentFacade.getFacadeState());
        replicatedEnvironmentFacade.close();
        assertEquals(ReplicatedEnvironmentFacade.State.CLOSED, replicatedEnvironmentFacade.getFacadeState(),
                "Unexpected state " + replicatedEnvironmentFacade.getFacadeState());
    }

    @Test
    public void testEnvironmentAutomaticallyRestartsAndBecomesUnknownOnInsufficientReplicas() throws Exception
    {
        final CountDownLatch masterLatch = new CountDownLatch(1);
        final CountDownLatch secondMasterLatch = new CountDownLatch(1);
        final AtomicInteger masterStateChangeCount = new AtomicInteger();
        final CountDownLatch unknownLatch = new CountDownLatch(1);
        final AtomicInteger unknownStateChangeCount = new AtomicInteger();
        StateChangeListener stateChangeListener = stateChangeEvent ->
        {
            if (stateChangeEvent.getState() == State.MASTER)
            {
                masterStateChangeCount.incrementAndGet();
                if (masterLatch.getCount() == 1)
                {
                    masterLatch.countDown();
                }
                else
                {
                    secondMasterLatch.countDown();
                }
            }
            else if (stateChangeEvent.getState() == State.UNKNOWN)
            {
                unknownStateChangeCount.incrementAndGet();
                unknownLatch.countDown();
            }
        };

        // make sure that node is re-elected as MASTER on second start-up
        ReplicatedEnvironmentConfiguration config = createReplicatedEnvironmentConfiguration(TEST_NODE_NAME, TEST_NODE_HOST_PORT, TEST_DESIGNATED_PRIMARY);
        when(config.getPriority()).thenReturn(2);
        createReplicatedEnvironmentFacade(TEST_NODE_NAME, stateChangeListener,
                                          new NoopReplicationGroupListener(), config);

        assertTrue(masterLatch.await(_timeout, TimeUnit.SECONDS), "Master was not started");

        int replica1Port = _portHelper.getNextAvailable();
        String node1NodeHostPort = "localhost:" + replica1Port;
        int replica2Port = _portHelper.getNextAvailable();
        String node2NodeHostPort = "localhost:" + replica2Port;

        ReplicatedEnvironmentFacade replica1 = createReplica(TEST_NODE_NAME + "_1", node1NodeHostPort,
                                                             new NoopReplicationGroupListener());
        ReplicatedEnvironmentFacade replica2 = createReplica(TEST_NODE_NAME + "_2", node2NodeHostPort,
                                                             new NoopReplicationGroupListener());

        // close replicas
        replica1.close();
        replica2.close();

        assertTrue(unknownLatch.await(_timeout, TimeUnit.SECONDS),
                "Environment should be recreated and go into unknown state");

        // bring back the cluster in order to make sure that no extra state transition happens between UNKNOWN and MASTER
        createReplica(TEST_NODE_NAME + "_1", node1NodeHostPort, new NoopReplicationGroupListener());

        assertTrue(secondMasterLatch.await(_timeout, TimeUnit.SECONDS), "Master node did not resume");

        assertEquals(2, masterStateChangeCount.get(), "Node transited into Master state unexpected number of times");
        assertEquals(1, unknownStateChangeCount.get(), "Node transited into Unknown state unexpected number of times");
    }

    @Test
    public void testTransferMasterToSelf() throws Exception
    {
        final CountDownLatch firstNodeReplicaStateLatch = new CountDownLatch(1);
        final CountDownLatch firstNodeMasterStateLatch = new CountDownLatch(1);
        StateChangeListener stateChangeListener = event ->
        {
            State state = event.getState();
            if (state == State.REPLICA)
            {
                firstNodeReplicaStateLatch.countDown();
            }
            if (state == State.MASTER)
            {
                firstNodeMasterStateLatch.countDown();
            }
        };
        ReplicatedEnvironmentFacade firstNode = addNode(stateChangeListener, new NoopReplicationGroupListener());
        assertTrue(firstNodeMasterStateLatch.await(_timeout, TimeUnit.SECONDS), "Environment did not become a master");

        int replica1Port = _portHelper.getNextAvailable();
        String node1NodeHostPort = "localhost:" + replica1Port;
        ReplicatedEnvironmentFacade secondNode = createReplica(TEST_NODE_NAME + "_1", node1NodeHostPort,
                                                               new NoopReplicationGroupListener());
        assertEquals(State.REPLICA.name(), secondNode.getNodeState(), "Unexpected state");

        int replica2Port = _portHelper.getNextAvailable();
        String node2NodeHostPort = "localhost:" + replica2Port;
        final CountDownLatch replicaStateLatch = new CountDownLatch(1);
        final CountDownLatch masterStateLatch = new CountDownLatch(1);
        StateChangeListener testStateChangeListener = event ->
        {
            State state = event.getState();
            if (state == State.REPLICA)
            {
                replicaStateLatch.countDown();
            }
            if (state == State.MASTER)
            {
                masterStateLatch.countDown();
            }
        };
        ReplicatedEnvironmentFacade thirdNode = addNode(TEST_NODE_NAME + "_2", node2NodeHostPort, TEST_DESIGNATED_PRIMARY,
                                                        testStateChangeListener,
                                                        new NoopReplicationGroupListener());
        assertTrue(replicaStateLatch.await(_timeout, TimeUnit.SECONDS), "Environment did not become a replica");
        assertEquals(3, (long) thirdNode.getNumberOfElectableGroupMembers());

        thirdNode.transferMasterToSelfAsynchronously();
        assertTrue(masterStateLatch.await(_timeout, TimeUnit.SECONDS), "Environment did not become a master");
        assertTrue(firstNodeReplicaStateLatch.await(_timeout, TimeUnit.SECONDS),
                "First node environment did not become a replica");
        assertEquals(State.REPLICA.name(), firstNode.getNodeState(), "Unexpected state");
    }

    @Test
    public void testTransferMasterAnotherNode() throws Exception
    {
        final CountDownLatch firstNodeReplicaStateLatch = new CountDownLatch(1);
        final CountDownLatch firstNodeMasterStateLatch = new CountDownLatch(1);
        StateChangeListener stateChangeListener = event ->
        {
            State state = event.getState();
            if (state == State.REPLICA)
            {
                firstNodeReplicaStateLatch.countDown();
            }
            if (state == State.MASTER)
            {
                firstNodeMasterStateLatch.countDown();
            }
        };
        ReplicatedEnvironmentFacade firstNode = addNode(stateChangeListener, new NoopReplicationGroupListener());
        assertTrue(firstNodeMasterStateLatch.await(_timeout, TimeUnit.SECONDS),
                "Environment did not become a master");

        int replica1Port = _portHelper.getNextAvailable();
        String node1NodeHostPort = "localhost:" + replica1Port;
        ReplicatedEnvironmentFacade secondNode = createReplica(TEST_NODE_NAME + "_1", node1NodeHostPort,
                                                               new NoopReplicationGroupListener());
        assertEquals(State.REPLICA.name(), secondNode.getNodeState(), "Unexpected state");

        int replica2Port = _portHelper.getNextAvailable();
        String node2NodeHostPort = "localhost:" + replica2Port;
        final CountDownLatch replicaStateLatch = new CountDownLatch(1);
        final CountDownLatch masterStateLatch = new CountDownLatch(1);
        StateChangeListener testStateChangeListener = event ->
        {
            State state = event.getState();
            if (state == State.REPLICA)
            {
                replicaStateLatch.countDown();
            }
            if (state == State.MASTER)
            {
                masterStateLatch.countDown();
            }
        };
        String thirdNodeName = TEST_NODE_NAME + "_2";
        ReplicatedEnvironmentFacade thirdNode = addNode(thirdNodeName, node2NodeHostPort, TEST_DESIGNATED_PRIMARY,
                                                        testStateChangeListener, new NoopReplicationGroupListener());
        assertTrue(replicaStateLatch.await(_timeout, TimeUnit.SECONDS), "Environment did not become a replica");
        assertEquals(3, thirdNode.getNumberOfElectableGroupMembers());

        firstNode.transferMasterAsynchronously(thirdNodeName);
        assertTrue(masterStateLatch.await(_timeout, TimeUnit.SECONDS), "Environment did not become a master");
        assertTrue(firstNodeReplicaStateLatch.await(_timeout, TimeUnit.SECONDS),
                "First node environment did not become a replica");
        assertEquals(State.REPLICA.name(), firstNode.getNodeState(), "Unexpected state");
    }

    @Test
    public void testBeginTransaction() throws Exception
    {
        ReplicatedEnvironmentFacade facade = createMaster();
        Transaction txn = null;
        try
        {
            TransactionConfig transactionConfig = new TransactionConfig();
            transactionConfig.setDurability(facade.getRealMessageStoreDurability());
            txn = facade.beginTransaction(transactionConfig);
            assertNotNull(txn, "Transaction is not created");
            txn.commit();
            txn = null;
        }
        finally
        {
            if (txn != null)
            {
                txn.abort();
            }
        }
    }

    @Test
    public void testSetPermittedNodes() throws Exception
    {
        ReplicatedEnvironmentFacade firstNode = createMaster();

        Set<String> permittedNodes = new HashSet<>();
        permittedNodes.add("localhost:" + TEST_NODE_PORT);
        permittedNodes.add("localhost:" + _portHelper.getNextAvailable());
        firstNode.setPermittedNodes(permittedNodes);

        ReplicationNodeImpl replicationNode = new ReplicationNodeImpl(TEST_NODE_NAME, TEST_NODE_HOST_PORT);
        NodeState nodeState = getRemoteNodeState(TEST_GROUP_NAME, replicationNode, 5000);

        ObjectMapper objectMapper = new ObjectMapper();

        Map<String, Object> settings = objectMapper.readValue(nodeState.getAppState(), Map.class);
        Collection<String> appStatePermittedNodes =  (Collection<String>)settings.get(PERMITTED_NODE_LIST);
        assertEquals(permittedNodes, new HashSet<>(appStatePermittedNodes), "Unexpected permitted nodes");
    }

    @Test
    public void testPermittedNodeIsAllowedToConnect() throws Exception
    {
        ReplicatedEnvironmentFacade firstNode = createMaster();

        int replica1Port = _portHelper.getNextAvailable();
        String node1NodeHostPort = "localhost:" + replica1Port;

        Set<String> permittedNodes = new HashSet<>();
        permittedNodes.add("localhost:" + TEST_NODE_PORT);
        permittedNodes.add(node1NodeHostPort);
        firstNode.setPermittedNodes(permittedNodes);

        ReplicatedEnvironmentConfiguration configuration =  createReplicatedEnvironmentConfiguration(TEST_NODE_NAME + "_1", node1NodeHostPort, false);
        when(configuration.getHelperNodeName()).thenReturn(TEST_NODE_NAME);

        ReplicatedEnvironmentFacade secondNode =
                createNode(configuration, TEST_NODE_NAME + "_1", State.REPLICA);
        assertEquals(State.REPLICA.name(), secondNode.getNodeState(), "Unexpected state");
    }

    @Test
    public void testIntruderNodeIsDetected() throws Exception
    {
        final CountDownLatch intruderLatch = new CountDownLatch(1);
        ReplicationGroupListener listener = new NoopReplicationGroupListener()
        {
            @Override
            public boolean onIntruderNode(ReplicationNode node)
            {
                intruderLatch.countDown();
                return true;
            }
        };
        ReplicatedEnvironmentFacade firstNode = createMaster(listener);
        int replica1Port = _portHelper.getNextAvailable();
        String node1NodeHostPort = "localhost:" + replica1Port;

        Set<String> permittedNodes = new HashSet<>();
        permittedNodes.add("localhost:" + TEST_NODE_PORT);

        firstNode.setPermittedNodes(permittedNodes);

        String nodeName = TEST_NODE_NAME + "_1";
        createIntruder(nodeName, node1NodeHostPort);
        assertTrue(intruderLatch.await(_timeout, TimeUnit.SECONDS), "Intruder node was not detected");
    }

    @Test
    public void testNodeRolledback()  throws Exception
    {
        DatabaseConfig createConfig = createDatabaseConfig();

        TestStateChangeListener masterListener = new TestStateChangeListener();
        ReplicatedEnvironmentFacade node1 = addNode(TEST_NODE_NAME, TEST_NODE_HOST_PORT, true, masterListener,
                                                    new NoopReplicationGroupListener());
        assertTrue(masterListener.awaitForStateChange(State.MASTER, _timeout, TimeUnit.SECONDS),
                "Environment was not created");

        String replicaNodeHostPort = "localhost:" + _portHelper.getNextAvailable();
        String replicaName = TEST_NODE_NAME + 1;
        ReplicatedEnvironmentFacade node2 = createReplica(replicaName, replicaNodeHostPort,
                                                          new NoopReplicationGroupListener());

        Database db = node1.openDatabase("mydb", createConfig);

        // Put a record (that will be replicated)
        putRecord(node1, db, 1, "value1", false);

        node2.close();

        // Put a record (that will be only on node1 as node2 is now offline)
        putRecord(node1, db, 2, "value2", false);

        db.close();

        // Stop node1
        node1.close();

        LOGGER.debug("RESTARTING " + replicaName);
        // Restart the node2, making it primary so it becomes master
        TestStateChangeListener node2StateChangeListener = new TestStateChangeListener();
        node2 = addNode(replicaName, replicaNodeHostPort, true, node2StateChangeListener,
                        new NoopReplicationGroupListener());
        boolean awaitForStateChange = node2StateChangeListener.awaitForStateChange(State.MASTER, _timeout, TimeUnit.SECONDS);
        assertTrue(awaitForStateChange, replicaName + " did not go into desired state; current actual state is " +
                                        node2StateChangeListener.getCurrentActualState());

        db = node2.openDatabase("mydb", DatabaseConfig.DEFAULT);

        // Do a transaction on node2. The two environments will have diverged
        putRecord(node2, db, 3, "diverged", false);

        LOGGER.debug("RESTARTING " + TEST_NODE_NAME);
        // Now restart node1 and ensure that it realises it needs to rollback before it can rejoin.
        TestStateChangeListener node1StateChangeListener = new TestStateChangeListener();
        final CountDownLatch _replicaRolledback = new CountDownLatch(1);
        node1 = addNode(node1StateChangeListener, new NoopReplicationGroupListener()
        {
            @Override
            public void onNodeRolledback()
            {
                LOGGER.debug("onNodeRolledback in " + TEST_NODE_NAME);
                _replicaRolledback.countDown();
            }
        });
        assertTrue(node1StateChangeListener.awaitForStateChange(State.REPLICA, _timeout, TimeUnit.SECONDS),
                "Node 1 did not go into desired state");
        assertTrue(_replicaRolledback.await(_timeout, TimeUnit.SECONDS),
                "Node 1 did not experience rollback within timeout");

        // Finally do one more transaction through the master
        putRecord(node2, db, 4, "value4", false);
        db.close();

        LOGGER.debug("CLOSING");
        node1.close();
        node2.close();
    }

    @Test
    public void testReplicaTransactionBeginsImmediately()  throws Exception
    {
        ReplicatedEnvironmentFacade master = createMaster();
        String nodeName2 = TEST_NODE_NAME + "_2";
        String host = "localhost";
        int port = _portHelper.getNextAvailable();
        String node2NodeHostPort = host + ":" + port;

        final ReplicatedEnvironmentFacade replica = createReplica(nodeName2, node2NodeHostPort,
                                                                  new NoopReplicationGroupListener());

        // close the master
        master.close();

        // try to create a transaction in a separate thread
        // and make sure that transaction is created immediately.
        ExecutorService service =  Executors.newSingleThreadExecutor();
        try
        {

            Future<Transaction> future = service.submit(() -> replica.beginTransaction(null));
            Transaction transaction = future.get(_timeout, TimeUnit.SECONDS);
            assertNotNull(transaction, "Transaction was not created during expected time");
            transaction.abort();
        }
        finally
        {
            service.shutdown();
        }
    }

    @Test
    public void testReplicaWriteExceptionIsConvertedIntoConnectionScopedRuntimeException()  throws Exception
    {
        ReplicatedEnvironmentFacade master = createMaster();
        String nodeName2 = TEST_NODE_NAME + "_2";
        String host = "localhost";
        int port = _portHelper.getNextAvailable();
        String node2NodeHostPort = host + ":" + port;

        final ReplicatedEnvironmentFacade replica = createReplica(nodeName2, node2NodeHostPort,
                                                                  new NoopReplicationGroupListener());

        // close the master
        master.close();

        try
        {
            replica.openDatabase("test", DatabaseConfig.DEFAULT.setAllowCreate(true) );
            fail("Replica write operation should fail");
        }
        catch(ReplicaWriteException e)
        {
            RuntimeException handledException = master.handleDatabaseException("test", e);
            final boolean condition = handledException instanceof ConnectionScopedRuntimeException;
            assertTrue(condition, "Unexpected exception");
        }
    }

    @Test
    public void testSetElectableGroupSizeOverrideAfterMajorityLost()  throws Exception
    {
        final SettableFuture<Boolean> majorityLost = SettableFuture.create();
        final TestStateChangeListener masterListener = new TestStateChangeListener();

        ReplicationGroupListener masterGroupListener = new NoopReplicationGroupListener()
        {
            @Override
            public void onNoMajority()
            {
                majorityLost.set(true);
            }
        };

        ReplicatedEnvironmentConfiguration masterConfig = createReplicatedEnvironmentConfiguration(TEST_NODE_NAME, TEST_NODE_HOST_PORT, false);
        ReplicatedEnvironmentFacade master = createReplicatedEnvironmentFacade(TEST_NODE_NAME, masterListener, masterGroupListener, masterConfig);
        assertTrue(masterListener.awaitForStateChange(State.MASTER, _timeout, TimeUnit.SECONDS),
                   "Master was not created");

        int replica1Port = _portHelper.getNextAvailable();
        String node1NodeHostPort = "localhost:" + replica1Port;
        int replica2Port = _portHelper.getNextAvailable();
        String node2NodeHostPort = "localhost:" + replica2Port;

        master.setPermittedNodes(Arrays.asList(master.getHostPort(), node1NodeHostPort, node2NodeHostPort));

        ReplicatedEnvironmentFacade replica1 = createReplica(TEST_NODE_NAME + "_1", node1NodeHostPort,
                                                             new NoopReplicationGroupListener());
        ReplicatedEnvironmentFacade replica2 = createReplica(TEST_NODE_NAME + "_2", node2NodeHostPort,
                                                             new NoopReplicationGroupListener());

        replica1.close();
        replica2.close();

        assertTrue(masterListener.awaitForStateChange(State.DETACHED, _timeout, TimeUnit.SECONDS),
                "Node that was master did not become detached after the replica closed");
        assertTrue(majorityLost.get(_timeout, TimeUnit.SECONDS), "Majority lost is undetected");

        assertEquals(ReplicatedEnvironmentFacade.State.RESTARTING, master.getFacadeState(), "Unexpected facade state");

        when(masterConfig.getQuorumOverride()).thenReturn(1);
        master.reapplyElectableGroupSizeOverride();

        assertTrue(masterListener.awaitForStateChange(State.MASTER, _timeout, TimeUnit.SECONDS),
                   "Master did not become available again following the application of the electable group override");
    }

    @Test
    public void testSetDesignatedPrimaryAfterMajorityLost()  throws Exception
    {
        final SettableFuture<Boolean> majorityLost = SettableFuture.create();
        final TestStateChangeListener masterListener = new TestStateChangeListener();
        final NoopReplicationGroupListener masterGroupListener = new NoopReplicationGroupListener()
        {
            @Override
            public void onNoMajority()
            {
                super.onNoMajority();
                majorityLost.set(true);
            }
        };

        ReplicatedEnvironmentConfiguration masterConfig = createReplicatedEnvironmentConfiguration(TEST_NODE_NAME, TEST_NODE_HOST_PORT, false);
        ReplicatedEnvironmentFacade master = createReplicatedEnvironmentFacade(TEST_NODE_NAME, masterListener, masterGroupListener, masterConfig);
        assertTrue(masterListener.awaitForStateChange(State.MASTER, _timeout, TimeUnit.SECONDS),
                "Master was not created");

        int replicaPort = _portHelper.getNextAvailable();
        String replicaNodeHostPort = "localhost:" + replicaPort;

        master.setPermittedNodes(Arrays.asList(master.getHostPort(), replicaNodeHostPort));

        ReplicatedEnvironmentFacade replica1 = createReplica(TEST_NODE_NAME + "_1", replicaNodeHostPort,
                                                             new NoopReplicationGroupListener());
        replica1.close();

        assertTrue(masterListener.awaitForStateChange(State.DETACHED, _timeout, TimeUnit.SECONDS),
                "Node that was master did not become detached after the replica closed");
        assertTrue(majorityLost.get(_timeout, TimeUnit.SECONDS), "Majority lost is undetected");

        assertEquals(ReplicatedEnvironmentFacade.State.RESTARTING, master.getFacadeState(), "Unexpected facade state");

        when(masterConfig.isDesignatedPrimary()).thenReturn(true);
        master.reapplyDesignatedPrimary();

        assertTrue(masterListener.awaitForStateChange(State.MASTER, _timeout, TimeUnit.SECONDS),
                "Master did not become available again following designated primary");
    }

    @Test
    public void testCommitNoSyncWithCoalescing() throws Exception
    {
        final ReplicatedEnvironmentConfiguration node1Config =
                createReplicatedEnvironmentConfiguration(TEST_NODE_NAME, TEST_NODE_HOST_PORT, true, false);
        when(node1Config.getFacadeParameter(eq(String.class),
                                            eq(NO_SYNC_TX_DURABILITY_PROPERTY_NAME),
                                            anyString())).thenReturn("NO_SYNC,NO_SYNC,NONE");

        final ReplicatedEnvironmentFacade node1 = createNode(node1Config, TEST_NODE_NAME, State.MASTER);

        final String replicaNodeHostPort = "localhost:" + _portHelper.getNextAvailable();
        final String replicaName = TEST_NODE_NAME + 1;
        final ReplicatedEnvironmentConfiguration node2Config =
                createReplicatedEnvironmentConfiguration(replicaName, replicaNodeHostPort, false, false);
        when(node2Config.getFacadeParameter(eq(String.class),
                                            eq(NO_SYNC_TX_DURABILITY_PROPERTY_NAME),
                                            anyString())).thenReturn("NO_SYNC,NO_SYNC,NONE");
        final ReplicatedEnvironmentFacade node2 = createNode(node2Config, replicaName, State.REPLICA);

        try (final Database db = node1.openDatabase("mydb", createDatabaseConfig()))
        {
            putRecord(node1, db, 1, "value", true);
        }

        node2.close();
        node1.close();

        LOGGER.debug("RESTARTING " + TEST_NODE_NAME);
        final ReplicatedEnvironmentFacade node1Restarted = createNode(node1Config, TEST_NODE_NAME, State.MASTER);
        LOGGER.debug("RESTARTING " + replicaName);
        final ReplicatedEnvironmentFacade node2Restarted = createNode(node2Config, replicaName, State.REPLICA);

        try (final Database db = node1Restarted.openDatabase("mydb", null))
        {
            assertEquals("value", getTestKeyValue(db, 1));
        }
        node1Restarted.close();
        node2Restarted.close();
    }

    @Test
    public void testCommitNoSyncWithoutCoalescing() throws Exception
    {
        final ReplicatedEnvironmentConfiguration node1Config =
                createReplicatedEnvironmentConfiguration(TEST_NODE_NAME, TEST_NODE_HOST_PORT, true, true);
        when(node1Config.getFacadeParameter(eq(String.class),
                                            eq(NO_SYNC_TX_DURABILITY_PROPERTY_NAME),
                                            anyString())).thenReturn("SYNC,NO_SYNC,NONE");

        final ReplicatedEnvironmentFacade node1 = createNode(node1Config, TEST_NODE_NAME, State.MASTER);

        final String replicaNodeHostPort = "localhost:" + _portHelper.getNextAvailable();
        final String replicaName = TEST_NODE_NAME + 1;

        final ReplicatedEnvironmentConfiguration node2Config =
                createReplicatedEnvironmentConfiguration(replicaName, replicaNodeHostPort, false, true);
        when(node2Config.getFacadeParameter(eq(String.class),
                                            eq(NO_SYNC_TX_DURABILITY_PROPERTY_NAME),
                                            anyString())).thenReturn("NO_SYNC,NO_SYNC,NONE");
        final ReplicatedEnvironmentFacade node2 = createNode(node2Config, replicaName, State.REPLICA);

        try (final Database db = node1.openDatabase("mydb", createDatabaseConfig()))
        {
            putRecord(node1, db, 1, "value", true);
        }

        node1.close();
        node2.close();

        LOGGER.debug("RESTARTING " + TEST_NODE_NAME);
        final ReplicatedEnvironmentFacade node1Restarted = createNode(node1Config, TEST_NODE_NAME, State.MASTER);
        LOGGER.debug("RESTARTING " + replicaName);
        final ReplicatedEnvironmentFacade node2Restarted = createNode(node2Config, replicaName, State.REPLICA);

        try (final Database db = node1Restarted.openDatabase("mydb", null))
        {
            assertEquals("value", getTestKeyValue(db, 1));
        }
        node1Restarted.close();
        node2Restarted.close();
    }

    private ReplicatedEnvironmentFacade createNode(final ReplicatedEnvironmentConfiguration node1Config,
                                                   final String nodeName,
                                                   final State nodeRole)
            throws Exception
    {
        final TestStateChangeListener roleListener = new TestStateChangeListener();
        final ReplicatedEnvironmentFacade node = createReplicatedEnvironmentFacade(nodeName,
                                                                                   roleListener,
                                                                                   new NoopReplicationGroupListener(),
                                                                                   node1Config);
        assertTrue(roleListener.awaitForStateChange(nodeRole, _timeout, TimeUnit.SECONDS),
                "Environment was not created");
        return node;
    }

    private DatabaseConfig createDatabaseConfig()
    {
        final DatabaseConfig createConfig = new DatabaseConfig();
        createConfig.setAllowCreate(true);
        createConfig.setTransactional(true);
        return createConfig;
    }

    private String getTestKeyValue(final Database db, final int keyValue)
    {
        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry result = new DatabaseEntry();
        IntegerBinding.intToEntry(keyValue, key);
        final OperationStatus status = db.get(null, key, result, LockMode.DEFAULT);
        if (status == OperationStatus.SUCCESS)
        {
            return StringBinding.entryToString(result);
        }
        return null;
    }

    private void putRecord(final ReplicatedEnvironmentFacade master,
                           final Database db,
                           final int key,
                           final String value,
                           final boolean commitNosync)
    {
        final DatabaseEntry keyEntry = new DatabaseEntry();
        IntegerBinding.intToEntry(key, keyEntry);
        final DatabaseEntry dataEntry = new DatabaseEntry();
        StringBinding.stringToEntry(value, dataEntry);

        final TransactionConfig transactionConfig = new TransactionConfig();
        transactionConfig.setDurability(master.getRealMessageStoreDurability());
        final Transaction txn = master.beginTransaction(transactionConfig);
        db.put(txn, keyEntry, dataEntry);
        if (commitNosync)
        {
            master.commitNoSync(txn);
        }
        else
        {
            master.commit(txn);
        }
    }

    private void createIntruder(String nodeName, String node1NodeHostPort)
    {
        File environmentPathFile = new File(_storePath, nodeName);
        environmentPathFile.mkdirs();

        ReplicationConfig replicationConfig = new ReplicationConfig(TEST_GROUP_NAME, nodeName, node1NodeHostPort);
        replicationConfig.setHelperHosts(TEST_NODE_HOST_PORT);
        replicationConfig.setConsistencyPolicy(NoConsistencyRequiredPolicy.NO_CONSISTENCY);

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        envConfig.setDurability(TEST_DURABILITY);
        ReplicatedEnvironment intruder = null;
        try
        {
            intruder = new ReplicatedEnvironment(environmentPathFile, replicationConfig, envConfig);
        }
        finally
        {
            if (intruder != null)
            {
                intruder.close();
            }
        }
    }

    private ReplicatedEnvironmentFacade createMaster() throws Exception
    {
        return createMaster(false);
    }
    private ReplicatedEnvironmentFacade createMaster(final boolean disableCoalescing) throws Exception
    {
        return createMaster(new NoopReplicationGroupListener(), disableCoalescing);
    }
    private ReplicatedEnvironmentFacade createMaster(ReplicationGroupListener replicationGroupListener) throws Exception
    {
        return createMaster(replicationGroupListener,false);
    }

    private ReplicatedEnvironmentFacade createMaster(ReplicationGroupListener replicationGroupListener,final boolean disableCoalescing) throws Exception
    {
        TestStateChangeListener stateChangeListener = new TestStateChangeListener();
        ReplicatedEnvironmentFacade env = addNode(stateChangeListener, replicationGroupListener);
        assertTrue(stateChangeListener.awaitForStateChange(State.MASTER, _timeout, TimeUnit.SECONDS),
                "Environment was not created");
        return env;
    }

    private ReplicatedEnvironmentFacade createReplica(String nodeName, String nodeHostPort, ReplicationGroupListener replicationGroupListener) throws Exception
    {
        TestStateChangeListener testStateChangeListener = new TestStateChangeListener();
        return createReplica(nodeName, nodeHostPort, testStateChangeListener, replicationGroupListener);
    }

    private ReplicatedEnvironmentFacade createReplica(String nodeName, String nodeHostPort,
                                                      TestStateChangeListener testStateChangeListener, ReplicationGroupListener replicationGroupListener)
            throws Exception
    {
        ReplicatedEnvironmentFacade replicaEnvironmentFacade = addNode(nodeName, nodeHostPort, TEST_DESIGNATED_PRIMARY,
                                                                       testStateChangeListener, replicationGroupListener);
        boolean awaitForStateChange = testStateChangeListener.awaitForStateChange(State.REPLICA,
                                                                                  _timeout, TimeUnit.SECONDS);
        assertTrue(awaitForStateChange, "Replica " + nodeName + " did not go into desired state; current actual state is " +
                testStateChangeListener.getCurrentActualState());
        return replicaEnvironmentFacade;
    }
    private ReplicatedEnvironmentFacade addNode(String nodeName, String nodeHostPort, boolean designatedPrimary,
                                                StateChangeListener stateChangeListener, ReplicationGroupListener replicationGroupListener,final boolean disableCoalescing)
    {
        ReplicatedEnvironmentConfiguration config = createReplicatedEnvironmentConfiguration(nodeName, nodeHostPort, designatedPrimary,disableCoalescing);
        return createReplicatedEnvironmentFacade(nodeName, stateChangeListener, replicationGroupListener, config);
    }

    private ReplicatedEnvironmentFacade addNode(String nodeName, String nodeHostPort, boolean designatedPrimary,
                                                StateChangeListener stateChangeListener, ReplicationGroupListener replicationGroupListener)
    {
       return addNode(nodeName,nodeHostPort,designatedPrimary,stateChangeListener,replicationGroupListener,false);
    }

    private ReplicatedEnvironmentFacade createReplicatedEnvironmentFacade(String nodeName, StateChangeListener stateChangeListener, ReplicationGroupListener replicationGroupListener, ReplicatedEnvironmentConfiguration config) {
        ReplicatedEnvironmentFacade ref = new ReplicatedEnvironmentFacade(config);
        ref.setStateChangeListener(stateChangeListener);
        ref.setReplicationGroupListener(replicationGroupListener);
        ref.setMessageStoreDurability(TEST_DURABILITY.getLocalSync(), TEST_DURABILITY.getReplicaSync(), TEST_DURABILITY.getReplicaAck());
        _nodes.put(nodeName, ref);
        return ref;
    }

    private ReplicatedEnvironmentFacade addNode(StateChangeListener stateChangeListener,
                                                ReplicationGroupListener replicationGroupListener,final boolean diableCoalescing)
    {
        return addNode(TEST_NODE_NAME, TEST_NODE_HOST_PORT, TEST_DESIGNATED_PRIMARY,
                       stateChangeListener, replicationGroupListener);
    }
    private ReplicatedEnvironmentFacade addNode(StateChangeListener stateChangeListener,
                                                ReplicationGroupListener replicationGroupListener)
    {
        return addNode(stateChangeListener,replicationGroupListener,false);
    }
    private ReplicatedEnvironmentConfiguration createReplicatedEnvironmentConfiguration(String nodeName, String nodeHostPort, boolean designatedPrimary)
    {
        return createReplicatedEnvironmentConfiguration(nodeName,nodeHostPort,designatedPrimary,false);
    }

    private ReplicatedEnvironmentConfiguration createReplicatedEnvironmentConfiguration(String nodeName, String nodeHostPort, boolean designatedPrimary,final boolean disableCoalescing)
    {
        ReplicatedEnvironmentConfiguration node = mock(ReplicatedEnvironmentConfiguration.class);
        when(node.getName()).thenReturn(nodeName);
        when(node.getHostPort()).thenReturn(nodeHostPort);
        when(node.isDesignatedPrimary()).thenReturn(designatedPrimary);
        when(node.getQuorumOverride()).thenReturn(TEST_ELECTABLE_GROUP_OVERRIDE);
        when(node.getPriority()).thenReturn(TEST_PRIORITY);
        when(node.getGroupName()).thenReturn(TEST_GROUP_NAME);
        when(node.getHelperHostPort()).thenReturn(TEST_NODE_HELPER_HOST_PORT);
        when(node.getHelperNodeName()).thenReturn(TEST_NODE_NAME);

        when(node.getFacadeParameter(eq(Integer.class),
                                     eq(MASTER_TRANSFER_TIMEOUT_PROPERTY_NAME),
                                     anyInt())).thenReturn(60000);
        when(node.getFacadeParameter(eq(Integer.class), eq(DB_PING_SOCKET_TIMEOUT_PROPERTY_NAME), anyInt())).thenReturn(
                10000);
        when(node.getFacadeParameter(eq(Integer.class),
                                     eq(REMOTE_NODE_MONITOR_INTERVAL_PROPERTY_NAME),
                                     anyInt())).thenReturn(1000);
        when(node.getFacadeParameter(eq(Integer.class),
                                     eq(REMOTE_NODE_MONITOR_TIMEOUT_PROPERTY_NAME),
                                     anyInt())).thenReturn(1000);
        when(node.getFacadeParameter(eq(Integer.class),
                                     eq(ENVIRONMENT_RESTART_RETRY_LIMIT_PROPERTY_NAME),
                                     anyInt())).thenReturn(3);
        when(node.getFacadeParameter(eq(Integer.class),
                                     eq(EXECUTOR_SHUTDOWN_TIMEOUT_PROPERTY_NAME),
                                     anyInt())).thenReturn(10000);
        when(node.getFacadeParameter(eq(Integer.class),
                                     eq(LOG_HANDLER_CLEANER_PROTECTED_FILES_LIMIT_PROPERTY_NAME),
                                     anyInt())).thenReturn(0);
        when(node.getFacadeParameter(eq(Map.class), any(), eq(JUL_LOGGER_LEVEL_OVERRIDE), any())).thenReturn(Collections.emptyMap());
        when(node.getFacadeParameter(eq(Boolean.class),
                                     eq(DISABLE_COALESCING_COMMITTER_PROPERTY_NAME),
                                     anyBoolean())).thenReturn(disableCoalescing);
        when(node.getFacadeParameter(eq(Integer.class),
                                     eq(QPID_BROKER_BDB_COMMITER_NOTIFY_THRESHOLD),
                                     anyInt())).thenReturn(DEFAULT_QPID_BROKER_BDB_COMMITER_NOTIFY_THRESHOLD);
        when(node.getFacadeParameter(eq(Long.class),
                                     eq(QPID_BROKER_BDB_COMMITER_WAIT_TIMEOUT),
                                     anyLong())).thenReturn(DEFAULT_QPID_BROKER_BDB_COMMITER_WAIT_TIMEOUT);

        Map<String, String> repConfig = new HashMap<>();
        repConfig.put(ReplicationConfig.REPLICA_ACK_TIMEOUT, "2 s");
        repConfig.put(ReplicationConfig.INSUFFICIENT_REPLICAS_TIMEOUT, "2 s");
        when(node.getReplicationParameters()).thenReturn(repConfig);
        when(node.getStorePath()).thenReturn(new File(_storePath, nodeName).getAbsolutePath());
        return node;
    }

    static class NoopReplicationGroupListener implements ReplicationGroupListener
    {

        @Override
        public void onReplicationNodeAddedToGroup(ReplicationNode node)
        {
        }

        @Override
        public void onReplicationNodeRecovered(ReplicationNode node)
        {
        }

        @Override
        public void onReplicationNodeRemovedFromGroup(ReplicationNode node)
        {
        }

        @Override
        public void onNodeState(ReplicationNode node, NodeState nodeState)
        {
        }

        @Override
        public boolean onIntruderNode(ReplicationNode node)
        {
            LOGGER.warn("Intruder node " + node);
            return true;
        }

        @Override
        public void onNoMajority()
        {
        }

        @Override
        public void onNodeRolledback()
        {
        }

        @Override
        public void onException(Exception e)
        {
        }
    }
}
