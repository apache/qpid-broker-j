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
 *
 */
package org.apache.qpid.server.store.berkeleydb.replication;

import static junit.framework.TestCase.assertEquals;
import static org.apache.qpid.systests.Utils.INDEX;
import static org.apache.qpid.systests.Utils.getReceiveTimeout;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertNotNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.TransactionRolledBackException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sleepycat.je.Durability;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationConfig;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.util.FileUtils;
import org.apache.qpid.server.virtualhostnode.berkeleydb.BDBHARemoteReplicationNode;
import org.apache.qpid.server.virtualhostnode.berkeleydb.BDBHAVirtualHostNode;
import org.apache.qpid.server.virtualhostnode.berkeleydb.NodeRole;
import org.apache.qpid.systests.ConnectionBuilder;
import org.apache.qpid.systests.GenericConnectionListener;
import org.apache.qpid.systests.Utils;
import org.apache.qpid.test.utils.PortHelper;
import org.apache.qpid.test.utils.TestUtils;
import org.apache.qpid.tests.utils.ConfigItem;

@GroupConfig(numberOfNodes = 3, groupName = "test")
@ConfigItem(name = Broker.BROKER_FAIL_STARTUP_WITH_ERRORED_CHILD, value = "false")
public class MultiNodeTest extends GroupJmsTestBase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiNodeTest.class);


    private FailoverAwaitingListener _failoverListener = new FailoverAwaitingListener();

    private static final int FAILOVER_COMPLETION_TIMEOUT = 60000;

    @Test
    public void testLossOfMasterNodeCausesClientToFailover() throws Exception
    {
        final Connection connection = getConnectionBuilder().build();
        try
        {
            getJmsProvider().addGenericConnectionListener(connection, _failoverListener);

            final int masterPort = getJmsProvider().getConnectedURI(connection).getPort();
            LOGGER.info("Active connection port {}", masterPort);

            getBrokerAdmin().stopNode(masterPort);
            LOGGER.info("Node is stopped");
            _failoverListener.awaitFailoverCompletion(FAILOVER_COMPLETION_TIMEOUT);
            LOGGER.info("Listener has finished");
            // any op to ensure connection remains
            connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testLossOfReplicaNodeDoesNotCauseClientToFailover() throws Exception
    {
        final Connection connection = getConnectionBuilder().build();
        try
        {
            getJmsProvider().addGenericConnectionListener(connection, _failoverListener);

            final int activeBrokerPort = getJmsProvider().getConnectedURI(connection).getPort();
            LOGGER.info("Active connection port {}", activeBrokerPort);

            final int inactiveBrokerPort = getBrokerAdmin().getAmqpPort(activeBrokerPort);
            LOGGER.info("Stopping inactive broker on port {} ", inactiveBrokerPort);

            getBrokerAdmin().stopNode(inactiveBrokerPort);

            _failoverListener.assertNoFailoverCompletionWithin(2000);

            // any op to ensure connection remains
            connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testLossOfQuorumCausesClientDisconnection() throws Exception
    {
        final Connection connection = getConnectionBuilder().build();
        try
        {
            getJmsProvider().addGenericConnectionListener(connection, _failoverListener);
            Set<Integer> ports =
                    Arrays.stream(getBrokerAdmin().getGroupAmqpPorts()).boxed().collect(Collectors.toSet());
            final int activeBrokerPort = getJmsProvider().getConnectedURI(connection).getPort();
            ports.remove(activeBrokerPort);

            // Stop all other nodes
            for (Integer p : ports)
            {
                getBrokerAdmin().stopNode(p);
            }

            _failoverListener.awaitPreFailover(2000);
        }
        finally
        {
            LOGGER.debug("Closing original connection");
            connection.close();
        }

        // New connections should now fail as vhost will be unavailable
        try
        {
            Connection unexpectedConnection = getConnectionBuilder()
                    .setFailoverReconnectAttempts(SHORT_FAILOVER_CYCLECOUNT)
                    .setFailoverReconnectDelay(SHORT_FAILOVER_CONNECTDELAY)
                    .build();
            fail("Got unexpected connection to node in group without quorum " + unexpectedConnection);
        }
        catch (JMSException je)
        {
            // PASS
        }
    }

    /**
     * JE requires that transactions are ended before the ReplicatedEnvironment is closed.  This
     * test ensures that open messaging transactions are correctly rolled-back as quorum is lost,
     * and later the node rejoins the group in either master or replica role.
     */
    @Test
    public void testQuorumLostAndRestored_OriginalMasterRejoinsTheGroup() throws Exception
    {
        final Connection connection = getConnectionBuilder().build();
        try
        {
            getJmsProvider().addGenericConnectionListener(connection, _failoverListener);
            Destination dest = createTestQueue(connection);

            Set<Integer> ports =
                    Arrays.stream(getBrokerAdmin().getGroupAmqpPorts()).boxed().collect(Collectors.toSet());

            final int activeBrokerPort = getJmsProvider().getConnectedURI(connection).getPort();
            ports.remove(activeBrokerPort);

            Session session1 = connection.createSession(true, Session.SESSION_TRANSACTED);
            Session session2 = connection.createSession(true, Session.SESSION_TRANSACTED);

            session1.createConsumer(dest).close();

            MessageProducer producer1 = session1.createProducer(dest);
            producer1.send(session1.createMessage());
            MessageProducer producer2 = session2.createProducer(dest);
            producer2.send(session2.createMessage());

            // Leave transactions open, this will leave two store transactions open on the store

            // Stop all other nodes
            for (Integer p : ports)
            {
                getBrokerAdmin().stopNode(p);
            }

            // Await the old master discovering that it is all alone
            getBrokerAdmin().awaitNodeRole(activeBrokerPort, "WAITING");

            // Restart all other nodes
            for (Integer p : ports)
            {
                getBrokerAdmin().startNode(p);
            }

            _failoverListener.awaitFailoverCompletion(FAILOVER_COMPLETION_TIMEOUT);

            getBrokerAdmin().awaitNodeRole(activeBrokerPort, "MASTER", "REPLICA");
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testPersistentMessagesAvailableAfterFailover() throws Exception
    {
        final Connection connection = getConnectionBuilder().build();
        try
        {
            getJmsProvider().addGenericConnectionListener(connection, _failoverListener);
            Destination queue = createTestQueue(connection);

            final int activeBrokerPort = getJmsProvider().getConnectedURI(connection).getPort();

            Session producingSession = connection.createSession(true, Session.SESSION_TRANSACTED);
            Utils.sendMessages(producingSession, queue, 10);

            getBrokerAdmin().stopNode(activeBrokerPort);
            LOGGER.info("Old master (broker port {}) is stopped", activeBrokerPort);

            _failoverListener.awaitFailoverCompletion(FAILOVER_COMPLETION_TIMEOUT);
            LOGGER.info("Failover has finished");

            final int activeBrokerPortAfterFailover = getJmsProvider().getConnectedURI(connection).getPort();
            LOGGER.info("New master (broker port {}) after failover", activeBrokerPortAfterFailover);

            Session consumingSession = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageConsumer consumer = consumingSession.createConsumer(queue);

            connection.start();
            for (int i = 0; i < 10; i++)
            {
                Message m = consumer.receive(getReceiveTimeout());
                assertNotNull("Message " + i + "  is not received", m);
                assertEquals("Unexpected message received", i, m.getIntProperty(INDEX));
            }
            consumingSession.commit();
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testTransferMasterFromLocalNode() throws Exception
    {
        final Connection connection = getConnectionBuilder().build();
        try
        {
            Destination queue = createTestQueue(connection);

            final int activeBrokerPort = getJmsProvider().getConnectedURI(connection).getPort();
            LOGGER.info("Active connection port {}", activeBrokerPort);

            final int inactiveBrokerPort = getBrokerAdmin().getAmqpPort(activeBrokerPort);
            LOGGER.info("Update role attribute on inactive broker on port {}", inactiveBrokerPort);

            // transfer mastership 3 times in order to verify
            // that repeated mastership transfer to the same node works, See QPID-6996
            transferMasterFromLocalNode(connection, queue, inactiveBrokerPort, activeBrokerPort);
            transferMasterFromLocalNode(connection, queue, activeBrokerPort, inactiveBrokerPort);
            transferMasterFromLocalNode(connection, queue, inactiveBrokerPort, activeBrokerPort);
        }
        finally
        {
            connection.close();
        }
    }

    private void transferMasterFromLocalNode(final Connection connection,
                                             final Destination queue,
                                             final int inactiveBrokerPort,
                                             final int activeBrokerPort) throws Exception
    {
        transferMasterToNodeWithAmqpPort(connection, inactiveBrokerPort);

        assertThat(Utils.produceConsume(connection, queue), is(equalTo(true)));

        getBrokerAdmin().awaitNodeRole(activeBrokerPort, "REPLICA");
    }

    private void transferMasterToNodeWithAmqpPort(final Connection connection, final int nodeAmqpPort)
            throws InterruptedException
    {
        _failoverListener = new FailoverAwaitingListener();
        getJmsProvider().addGenericConnectionListener(connection, _failoverListener);

        Map<String, Object> attributes = getBrokerAdmin().getNodeAttributes(nodeAmqpPort);
        assertEquals("Inactive broker has unexpected role", "REPLICA", attributes.get(BDBHAVirtualHostNode.ROLE));
        getBrokerAdmin().setNodeAttributes(nodeAmqpPort,
                                           Collections.singletonMap(BDBHAVirtualHostNode.ROLE, "MASTER"));

        _failoverListener.awaitFailoverCompletion(FAILOVER_COMPLETION_TIMEOUT);
        LOGGER.info("Listener has finished");

        attributes = getBrokerAdmin().getNodeAttributes(nodeAmqpPort);
        assertEquals("Inactive broker has unexpected role", "MASTER", attributes.get(BDBHAVirtualHostNode.ROLE));
    }

    @Test
    public void testTransferMasterFromRemoteNode() throws Exception
    {
        final Connection connection = getConnectionBuilder().build();
        try
        {
            Destination queue = createTestQueue(connection);
            final int activeBrokerPort = getJmsProvider().getConnectedURI(connection).getPort();
            LOGGER.info("Active connection port {}", activeBrokerPort);

            final int inactiveBrokerPort = getBrokerAdmin().getAmqpPort(activeBrokerPort);
            LOGGER.info("Update role attribute on inactive broker on port {}", inactiveBrokerPort);

            // transfer mastership 3 times in order to verify
            // that repeated mastership transfer to the same node works, See QPID-6996
            transferMasterFromRemoteNode(connection, queue, activeBrokerPort, inactiveBrokerPort);
            transferMasterFromRemoteNode(connection, queue, inactiveBrokerPort, activeBrokerPort);
            transferMasterFromRemoteNode(connection, queue, activeBrokerPort, inactiveBrokerPort);
        }
        finally
        {
            connection.close();
        }
    }

    private void transferMasterFromRemoteNode(final Connection connection,
                                              final Destination queue,
                                              final int activeBrokerPort,
                                              final int inactiveBrokerPort) throws Exception
    {
        _failoverListener = new FailoverAwaitingListener();
        getJmsProvider().addGenericConnectionListener(connection, _failoverListener);

        getBrokerAdmin().awaitRemoteNodeRole(activeBrokerPort, inactiveBrokerPort, "REPLICA");
        Map<String, Object> attributes = getBrokerAdmin().getRemoteNodeAttributes(activeBrokerPort, inactiveBrokerPort);
        assertEquals("Inactive broker has unexpected role", "REPLICA", attributes.get(BDBHAVirtualHostNode.ROLE));

        getBrokerAdmin().setRemoteNodeAttributes(activeBrokerPort,
                                                 inactiveBrokerPort,
                                                 Collections.singletonMap(BDBHAVirtualHostNode.ROLE, "MASTER"));

        _failoverListener.awaitFailoverCompletion(FAILOVER_COMPLETION_TIMEOUT);
        LOGGER.info("Listener has finished");

        attributes = getBrokerAdmin().getNodeAttributes(inactiveBrokerPort);
        assertEquals("Inactive broker has unexpected role", "MASTER", attributes.get(BDBHAVirtualHostNode.ROLE));

        assertThat(Utils.produceConsume(connection, queue), is(equalTo(true)));

        getBrokerAdmin().awaitNodeRole(activeBrokerPort, "REPLICA");
    }


    @Test
    public void testTransferMasterWhilstMessagesInFlight() throws Exception
    {
        final Connection connection = getConnectionBuilder().build();
        try
        {
            getJmsProvider().addGenericConnectionListener(connection, _failoverListener);

            final Destination destination = createTestQueue(connection);

            final AtomicBoolean masterTransferred = new AtomicBoolean(false);
            final AtomicBoolean keepRunning = new AtomicBoolean(true);
            final AtomicReference<Exception> workerException = new AtomicReference<>();
            final CountDownLatch producedOneBefore = new CountDownLatch(1);
            final CountDownLatch producedOneAfter = new CountDownLatch(1);
            final CountDownLatch workerShutdown = new CountDownLatch(1);

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Runnable producer = () -> {
                try
                {
                    int count = 0;
                    MessageProducer producer1 = session.createProducer(destination);

                    while (keepRunning.get())
                    {
                        String messageText = "message" + count;
                        try
                        {
                            Message message = session.createTextMessage(messageText);
                            producer1.send(message);
                            session.commit();
                            LOGGER.debug("Sent message " + count);

                            producedOneBefore.countDown();

                            if (masterTransferred.get())
                            {
                                producedOneAfter.countDown();
                            }
                            count++;
                        }
                        catch (javax.jms.IllegalStateException ise)
                        {
                            throw ise;
                        }
                        catch (TransactionRolledBackException trbe)
                        {
                            // Pass - failover in prgoress
                        }
                        catch (JMSException je)
                        {
                            // Pass - failover in progress
                        }
                    }
                }
                catch (Exception e)
                {
                    workerException.set(e);
                }
                finally
                {
                    workerShutdown.countDown();
                }
            };

            Thread backgroundWorker = new Thread(producer);
            backgroundWorker.start();

            boolean workerRunning = producedOneBefore.await(5000, TimeUnit.MILLISECONDS);
            assertTrue(workerRunning);

            final int activeBrokerPort = getJmsProvider().getConnectedURI(connection).getPort();
            LOGGER.info("Active connection port {}", activeBrokerPort);

            final int inactiveBrokerPort = getBrokerAdmin().getAmqpPort(activeBrokerPort);
            LOGGER.info("Update role attribute on inactive broker on port {}", inactiveBrokerPort);

            getBrokerAdmin().awaitNodeRole(inactiveBrokerPort, "REPLICA");
            Map<String, Object> attributes = getBrokerAdmin().getNodeAttributes(inactiveBrokerPort);
            assertEquals("Inactive broker has unexpected role", "REPLICA", attributes.get(BDBHAVirtualHostNode.ROLE));

            getBrokerAdmin().setNodeAttributes(inactiveBrokerPort,
                                               Collections.singletonMap(BDBHAVirtualHostNode.ROLE, "MASTER"));

            _failoverListener.awaitFailoverCompletion(FAILOVER_COMPLETION_TIMEOUT);
            LOGGER.info("Failover has finished");

            attributes = getBrokerAdmin().getNodeAttributes(inactiveBrokerPort);
            assertEquals("New master has unexpected role", "MASTER", attributes.get(BDBHAVirtualHostNode.ROLE));

            getBrokerAdmin().awaitNodeRole(activeBrokerPort, "REPLICA");

            LOGGER.info("Master transfer known to have completed successfully.");
            masterTransferred.set(true);

            boolean producedMore = producedOneAfter.await(5000, TimeUnit.MILLISECONDS);
            assertTrue("Should have successfully produced at least one message after transfer complete", producedMore);

            keepRunning.set(false);
            boolean shutdown = workerShutdown.await(5000, TimeUnit.MILLISECONDS);
            assertTrue("Worker thread should have shutdown", shutdown);

            backgroundWorker.join(5000);
            assertThat(workerException.get(), is(nullValue()));

            assertNotNull(session.createTemporaryQueue());
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testInFlightTransactionsWhilstMajorityIsLost() throws Exception
    {

        int connectionNumber = Integer.getInteger(
                "MultiNodeTest.testInFlightTransactionsWhilstMajorityIsLost.numberOfConnections",
                20);
        ExecutorService executorService = Executors.newFixedThreadPool(connectionNumber + 2);
        try
        {
            final ConnectionBuilder connectionBuilder =
                    getConnectionBuilder().setFailoverReconnectDelay(100).setFailoverReconnectAttempts(100);
            final Connection consumerConnection = connectionBuilder.build();
            try
            {
                Destination destination = createTestQueue(consumerConnection);
                consumerConnection.start();

                final Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                consumerSession.createConsumer(destination).setMessageListener(message -> {
                    try
                    {
                        LOGGER.info("Message received: " + ((TextMessage) message).getText());
                    }
                    catch (JMSException e)
                    {
                        LOGGER.error("Failure to get message text", e);
                    }
                });

                final Connection[] connections = new Connection[connectionNumber];
                final Session[] sessions = new Session[connectionNumber];
                for (int i = 0; i < sessions.length; i++)
                {
                    connections[i] = connectionBuilder.setClientId("test-" + UUID.randomUUID()).build();
                    sessions[i] = connections[i].createSession(true, Session.SESSION_TRANSACTED);
                    LOGGER.info("Session {} is created", i);
                }
                try
                {
                    Set<Integer> ports =
                            Arrays.stream(getBrokerAdmin().getGroupAmqpPorts()).boxed().collect(Collectors.toSet());

                    int maxMessageSize = 10;
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < maxMessageSize - 2; i++)
                    {
                        sb.append("X");
                    }
                    String messageText = sb.toString();
                    for (int n = 0; n < 3; n++)
                    {
                        LOGGER.info("Starting iteration {}", n);

                        FailoverAwaitingListener failoverListener = new FailoverAwaitingListener(connectionNumber);

                        for (int i = 0; i < sessions.length; i++)
                        {
                            Connection connection = connections[i];
                            getJmsProvider().addGenericConnectionListener(connection, failoverListener);

                            MessageProducer producer = sessions[i].createProducer(destination);
                            Message message = sessions[i].createTextMessage(messageText + "-" + i);
                            producer.send(message);
                        }

                        LOGGER.info("All publishing sessions have uncommitted transactions");

                        final int activeBrokerPort = getJmsProvider().getConnectedURI(connections[0]).getPort();
                        LOGGER.info("Active connection port {}", activeBrokerPort);

                        List<Integer> inactivePorts = new ArrayList<>(ports);
                        inactivePorts.remove(new Integer(activeBrokerPort));

                        final CountDownLatch latch = new CountDownLatch(inactivePorts.size());
                        for (int port : inactivePorts)
                        {
                            final int inactiveBrokerPort = port;
                            LOGGER.info("Stop node for inactive broker on port " + inactiveBrokerPort);

                            executorService.submit(() -> {
                                try
                                {
                                    getBrokerAdmin().setNodeAttributes(inactiveBrokerPort,
                                                                       Collections.singletonMap(
                                                                               BDBHAVirtualHostNode.DESIRED_STATE,
                                                                               State.STOPPED.name()));
                                }
                                catch (Exception e)
                                {
                                    LOGGER.error("Failed to stop node on broker with port {}", inactiveBrokerPort, e);
                                }
                                finally
                                {
                                    latch.countDown();
                                }
                            });
                        }

                        latch.await(500, TimeUnit.MILLISECONDS);

                        LOGGER.info("Committing transactions in parallel to provoke a lot of syncing to disk");
                        for (final Session session : sessions)
                        {
                            executorService.submit(() -> {
                                try
                                {
                                    session.commit();
                                }
                                catch (JMSException e)
                                {
                                    // majority of commits might fail due to insufficient replicas
                                }
                            });
                        }

                        LOGGER.info("Verify that stopped nodes are in detached role");
                        for (int port : inactivePorts)
                        {
                            getBrokerAdmin().awaitNodeRole(port, NodeRole.DETACHED.name());
                        }

                        LOGGER.info("Start stopped nodes");
                        for (int port : inactivePorts)
                        {
                            LOGGER.info("Starting node for inactive broker on port " + port);
                            try
                            {
                                getBrokerAdmin().setNodeAttributes(port,
                                                                   Collections.singletonMap(
                                                                           BDBHAVirtualHostNode.DESIRED_STATE,
                                                                           State.ACTIVE.name()));
                            }
                            catch (Exception e)
                            {
                                LOGGER.error("Failed to start node on broker with port " + port, e);
                            }
                        }

                        for (int port : ports)
                        {
                            getBrokerAdmin().awaitNodeRole(port, "REPLICA", "MASTER");
                        }

                        if (failoverListener.isFailoverStarted())
                        {
                            LOGGER.info("Waiting for failover completion");
                            failoverListener.awaitFailoverCompletion(20000 * connectionNumber);
                            LOGGER.info("Failover has finished");
                        }
                        else
                        {
                            LOGGER.info("Failover never started");
                        }
                    }
                }
                finally
                {
                    for (Connection c: connections)
                    {
                        try
                        {
                            c.close();
                        }
                        finally
                        {
                            LOGGER.error("Unexpected exception on connection close");
                        }
                    }
                }
            }
            finally
            {
                consumerConnection.close();
            }
        }
        finally
        {
            executorService.shutdown();
        }
    }

    /**
     * Tests aims to demonstrate that in a disaster situation (where all nodes except the master is lost), that operation
     * can be continued from a single node using the QUORUM_OVERRIDE feature.
     */
    @Test
    public void testQuorumOverride() throws Exception
    {
        final Connection connection = getConnectionBuilder().build();
        try
        {
            getJmsProvider().addGenericConnectionListener(connection, _failoverListener);
            Destination queue = createTestQueue(connection);

            Set<Integer> ports =
                    Arrays.stream(getBrokerAdmin().getGroupAmqpPorts()).boxed().collect(Collectors.toSet());

            final int activeBrokerPort = getJmsProvider().getConnectedURI(connection).getPort();
            ports.remove(activeBrokerPort);

            // Stop all other nodes
            for (Integer p : ports)
            {
                getBrokerAdmin().stopNode(p);
            }

            LOGGER.info("Awaiting failover to start");
            _failoverListener.awaitPreFailover(FAILOVER_COMPLETION_TIMEOUT);
            LOGGER.info("Failover has begun");

            Map<String, Object> attributes = getBrokerAdmin().getNodeAttributes(activeBrokerPort);
            assertEquals("Broker has unexpected quorum override",
                         new Integer(0),
                         attributes.get(BDBHAVirtualHostNode.QUORUM_OVERRIDE));
            getBrokerAdmin().setNodeAttributes(activeBrokerPort,
                                               Collections.singletonMap(BDBHAVirtualHostNode.QUORUM_OVERRIDE, 1));

            attributes = getBrokerAdmin().getNodeAttributes(activeBrokerPort);
            assertEquals("Broker has unexpected quorum override",
                         new Integer(1),
                         attributes.get(BDBHAVirtualHostNode.QUORUM_OVERRIDE));

            _failoverListener.awaitFailoverCompletion(FAILOVER_COMPLETION_TIMEOUT);
            LOGGER.info("Failover has finished");

            assertThat(Utils.produceConsume(connection, queue), is(equalTo(true)));
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testPriority() throws Exception
    {
        final Connection connection = getConnectionBuilder().build();
        try
        {
            getJmsProvider().addGenericConnectionListener(connection, _failoverListener);
            Destination queue = createTestQueue(connection);

            final int activeBrokerPort = getJmsProvider().getConnectedURI(connection).getPort();
            LOGGER.info("Active connection port {}", activeBrokerPort);

            int priority = 1;
            Integer highestPriorityBrokerPort = null;
            Set<Integer> ports =
                    Arrays.stream(getBrokerAdmin().getGroupAmqpPorts()).boxed().collect(Collectors.toSet());
            for (Integer port : ports)
            {
                if (activeBrokerPort != port)
                {
                    priority = priority + 1;
                    highestPriorityBrokerPort = port;
                    getBrokerAdmin().setNodeAttributes(port,
                                                       Collections.singletonMap(BDBHAVirtualHostNode.PRIORITY,
                                                                                priority));
                    Map<String, Object> attributes = getBrokerAdmin().getNodeAttributes(port);
                    assertEquals("Broker has unexpected priority",
                                 priority,
                                 attributes.get(BDBHAVirtualHostNode.PRIORITY));
                }
            }

            LOGGER.info("Broker on port {} has the highest priority of {}", highestPriorityBrokerPort, priority);

            for (Integer port : ports)
            {
                if (activeBrokerPort != port)
                {
                    getBrokerAdmin().awaitNodeRole(port, BDBHARemoteReplicationNode.ROLE, "REPLICA");
                }
            }

            // do work on master
            assertThat(Utils.produceConsume(connection, queue), is(equalTo(true)));

            Map<String, Object> masterNodeAttributes = getBrokerAdmin().getNodeAttributes(activeBrokerPort);

            Object lastTransactionId =
                    masterNodeAttributes.get(BDBHAVirtualHostNode.LAST_KNOWN_REPLICATION_TRANSACTION_ID);
            assertTrue("Unexpected last transaction id: " + lastTransactionId, lastTransactionId instanceof Number);

            // make sure all remote nodes have the same transaction id as master
            for (Integer port : ports)
            {
                if (activeBrokerPort != port)
                {
                    getBrokerAdmin().awaitNodeToAttainAttributeValue(activeBrokerPort,
                                                                     BDBHAVirtualHostNode.LAST_KNOWN_REPLICATION_TRANSACTION_ID,
                                                                     lastTransactionId);
                }
            }

            LOGGER.info("Shutting down the MASTER");
            getBrokerAdmin().stopNode(activeBrokerPort);

            _failoverListener.awaitFailoverCompletion(FAILOVER_COMPLETION_TIMEOUT);
            LOGGER.info("Listener has finished");

            Map<String, Object> attributes =
                    getBrokerAdmin().getNodeAttributes(highestPriorityBrokerPort);
            assertEquals("Inactive broker has unexpected role", "MASTER", attributes.get(BDBHAVirtualHostNode.ROLE));

            assertThat(Utils.produceConsume(connection, queue), is(equalTo(true)));
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testClusterCannotStartWithIntruder() throws Exception
    {
        int intruderPort =
                new PortHelper().getNextAvailable(Arrays.stream(getBrokerAdmin().getBdbPorts()).max().getAsInt() + 1);
        String nodeName = "intruder";
        String nodeHostPort = getBrokerAdmin().getHost() + ":" + intruderPort;
        File environmentPathFile = Files.createTempDirectory("qpid-work-intruder").toFile();
        try
        {
            environmentPathFile.mkdirs();
            ReplicationConfig replicationConfig =
                    new ReplicationConfig("test", nodeName, nodeHostPort);
            replicationConfig.setHelperHosts(getBrokerAdmin().getHelperHostPort());
            EnvironmentConfig envConfig = new EnvironmentConfig();
            envConfig.setAllowCreate(true);
            envConfig.setTransactional(true);
            envConfig.setDurability(new Durability(Durability.SyncPolicy.SYNC,
                                                   Durability.SyncPolicy.WRITE_NO_SYNC,
                                                   Durability.ReplicaAckPolicy.SIMPLE_MAJORITY));

            final String currentThreadName = Thread.currentThread().getName();
            try (ReplicatedEnvironment intruder = new ReplicatedEnvironment(environmentPathFile,
                                                                            replicationConfig,
                                                                            envConfig))
            {
                LOGGER.debug("Intruder started");
            }
            finally
            {
                Thread.currentThread().setName(currentThreadName);
            }

            Set<Integer> ports =
                    Arrays.stream(getBrokerAdmin().getGroupAmqpPorts()).boxed().collect(Collectors.toSet());
            for (int port : ports)
            {
                getBrokerAdmin().awaitNodeToAttainAttributeValue(port,
                                                                 BDBHAVirtualHostNode.STATE,
                                                                 State.ERRORED.name());
            }

            getBrokerAdmin().stop();
            try
            {
                getBrokerAdmin().start();
                fail("Cluster cannot start with an intruder node");
            }
            catch (Exception e)
            {
                // pass
            }
        }
        finally
        {
            FileUtils.delete(environmentPathFile, true);
        }
    }

    @Test
    public void testAsynchronousRecoverer() throws Exception
    {
        configureAsynchronousRecoveryOnAllNodes();

        final Connection connection = getConnectionBuilder().build();
        try
        {
            getJmsProvider().addGenericConnectionListener(connection, _failoverListener);
            final Destination queue = createTestQueue(connection);
            int brokerPort = getJmsProvider().getConnectedURI(connection).getPort();
            LOGGER.info("Sending message 'A' to the node with port {}", brokerPort);
            Utils.sendTextMessage(connection, queue, "A");

            final int anotherNodePort = getBrokerAdmin().getAmqpPort(brokerPort);
            LOGGER.info("Changing mastership to the node with port {}", anotherNodePort);
            transferMasterToNodeWithAmqpPort(connection, anotherNodePort);
            getBrokerAdmin().awaitNodeRole(brokerPort, "REPLICA", "MASTER");

            LOGGER.info("Sending message 'B' to the node with port {}", anotherNodePort);
            Utils.sendTextMessage(connection, queue, "B");

            LOGGER.info("Transfer mastership back to broker with port {}", brokerPort);
            transferMasterToNodeWithAmqpPort(connection, brokerPort);
            getBrokerAdmin().awaitNodeRole(anotherNodePort, "REPLICA", "MASTER");

            LOGGER.info("Sending message 'C' to the node with port {}", anotherNodePort);
            Utils.sendTextMessage(connection, queue, "C");

            consumeTextMessages(connection, queue, "A", "B", "C");
        }
        finally
        {
            connection.close();
        }
    }

    private void configureAsynchronousRecoveryOnAllNodes() throws Exception
    {
        final GroupBrokerAdmin brokerAdmin = getBrokerAdmin();
        final String contextValue = new ObjectMapper().writeValueAsString(Collections.singletonMap(
                "use_async_message_store_recovery",
                "true"));
        for (int port : brokerAdmin.getGroupAmqpPorts())
        {
            brokerAdmin.setNodeAttributes(port, Collections.singletonMap(BDBHAVirtualHostNode.CONTEXT,
                                                                         contextValue));
            brokerAdmin.stopNode(port);
            brokerAdmin.startNode(port);
            brokerAdmin.awaitNodeRole(port, BDBHARemoteReplicationNode.ROLE, "REPLICA", "MASTER");
        }

        LOGGER.info("Asynchronous recoverer is configured on all group nodes");
    }

    private void consumeTextMessages(final Connection connection, final Destination queue, final String... expected)
            throws JMSException
    {
        LOGGER.info("Trying to consume messages: {}", String.join(",", expected));
        connection.start();
        final Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        try
        {
            final MessageConsumer consumer = session.createConsumer(queue);

            for (String m : expected)
            {
                final Message message = consumer.receive(getReceiveTimeout());
                assertThat(message, is(instanceOf(TextMessage.class)));
                assertThat(((TextMessage) message).getText(), is(equalTo(m)));
            }
        }
        finally
        {
            session.close();
        }
    }

    private final class FailoverAwaitingListener implements GenericConnectionListener
    {
        private final CountDownLatch _failoverCompletionLatch;
        private final CountDownLatch _preFailoverLatch;
        private volatile boolean _failoverStarted;

        private FailoverAwaitingListener()
        {
            this(1);
        }

        private FailoverAwaitingListener(int connectionNumber)
        {
            _failoverCompletionLatch = new CountDownLatch(connectionNumber);
            _preFailoverLatch = new CountDownLatch(1);
        }

        @Override
        public void onConnectionInterrupted(URI uri)
        {
            _failoverStarted = true;
            _preFailoverLatch.countDown();
        }

        @Override
        public void onConnectionRestored(URI uri)
        {
            _failoverCompletionLatch.countDown();
        }

        void awaitFailoverCompletion(long delay) throws InterruptedException
        {
            if (!_failoverCompletionLatch.await(delay, TimeUnit.MILLISECONDS))
            {
                LOGGER.warn("Failover did not occur, dumping threads:\n\n" + TestUtils.dumpThreads() + "\n");
                Map<Integer, String> threadDumps = getBrokerAdmin().groupThreadDumps();
                for (Map.Entry<Integer, String> entry : threadDumps.entrySet())
                {
                    LOGGER.warn("Broker {} thread dump:\n\n {}", entry.getKey(), entry.getValue());
                }
            }
            assertEquals("Failover did not occur", 0, _failoverCompletionLatch.getCount());
        }

        void assertNoFailoverCompletionWithin(long delay) throws InterruptedException
        {
            _failoverCompletionLatch.await(delay, TimeUnit.MILLISECONDS);
            assertEquals("Failover occurred unexpectedly", 1L, _failoverCompletionLatch.getCount());
        }

        void awaitPreFailover(long delay) throws InterruptedException
        {
            boolean complete = _preFailoverLatch.await(delay, TimeUnit.MILLISECONDS);
            assertTrue("Failover was expected to begin within " + delay + " ms.", complete);
        }

        synchronized boolean isFailoverStarted()
        {
            return _failoverStarted;
        }
    }
}
