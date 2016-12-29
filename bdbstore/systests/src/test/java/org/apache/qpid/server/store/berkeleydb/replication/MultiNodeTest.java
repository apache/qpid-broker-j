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

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.TransactionRolledBackException;

import com.sleepycat.je.Durability;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.client.AMQConnection;
import org.apache.qpid.jms.ConnectionListener;
import org.apache.qpid.jms.ConnectionURL;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.virtualhostnode.berkeleydb.BDBHAVirtualHostNode;
import org.apache.qpid.server.virtualhostnode.berkeleydb.NodeRole;
import org.apache.qpid.test.utils.QpidBrokerTestCase;
import org.apache.qpid.test.utils.TestUtils;
import org.apache.qpid.util.FileUtils;

public class MultiNodeTest extends QpidBrokerTestCase
{
    protected static final Logger LOGGER = LoggerFactory.getLogger(MultiNodeTest.class);

    private static final String VIRTUAL_HOST = "test";
    private static final int NUMBER_OF_NODES = 3;

    private GroupCreator _groupCreator;

    private FailoverAwaitingListener _failoverListener;

    /** Used when expectation is client will (re)-connect */
    private ConnectionURL _positiveFailoverUrl;

    /** Used when expectation is client will not (re)-connect */
    private ConnectionURL _negativeFailoverUrl;

    @Override
    protected void setUp() throws Exception
    {
        assertTrue(isJavaBroker());
        assertTrue(isBrokerStorePersistent());

        _groupCreator = new GroupCreator(this, VIRTUAL_HOST, NUMBER_OF_NODES);
        _groupCreator.configureClusterNodes();

        _positiveFailoverUrl = _groupCreator.getConnectionUrlForAllClusterNodes();
        _negativeFailoverUrl = _groupCreator.getConnectionUrlForAllClusterNodes(200, 0, 2);

        _groupCreator.startCluster();
        _failoverListener = new FailoverAwaitingListener();

        super.setUp();
    }

    @Override
    public void startDefaultBroker() throws Exception
    {
        // Don't start default broker provided by QBTC.
    }

    public void testLossOfMasterNodeCausesClientToFailover() throws Exception
    {
        final Connection connection = getConnection(_positiveFailoverUrl);

        ((AMQConnection)connection).setConnectionListener(_failoverListener);

        final int activeBrokerPort = _groupCreator.getBrokerPortNumberFromConnection(connection);
        LOGGER.info("Active connection port " + activeBrokerPort);

        _groupCreator.stopNode(activeBrokerPort);
        LOGGER.info("Node is stopped");
        _failoverListener.awaitFailoverCompletion(20000);
        LOGGER.info("Listener has finished");
        // any op to ensure connection remains
        connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    public void testLossOfReplicaNodeDoesNotCauseClientToFailover() throws Exception
    {
        final Connection connection = getConnection(_positiveFailoverUrl);

        ((AMQConnection)connection).setConnectionListener(_failoverListener);
        final int activeBrokerPort = _groupCreator.getBrokerPortNumberFromConnection(connection);
        LOGGER.info("Active connection port " + activeBrokerPort);
        final int inactiveBrokerPort = _groupCreator.getPortNumberOfAnInactiveBroker(connection);

        LOGGER.info("Stopping inactive broker on port " + inactiveBrokerPort);

        _groupCreator.stopNode(inactiveBrokerPort);

        _failoverListener.assertNoFailoverCompletionWithin(2000);

        assertProducingConsuming(connection);
    }

    public void testLossOfQuorumCausesClientDisconnection() throws Exception
    {
        final Connection connection = getConnection(_negativeFailoverUrl);

        Set<Integer> ports = _groupCreator.getBrokerPortNumbersForNodes();

        final int activeBrokerPort = _groupCreator.getBrokerPortNumberFromConnection(connection);
        ports.remove(activeBrokerPort);

        // Stop all other nodes
        for (Integer p : ports)
        {
            _groupCreator.stopNode(p);
        }

        try
        {
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Destination destination = session.createQueue(getTestQueueName());
            session.createConsumer(destination).close();
            fail("Exception not thrown - creating durable queue should fail without quorum");
        }
        catch(JMSException jms)
        {
            // PASS
        }

        // New connections should now fail as vhost will be unavailable
        try
        {
            Connection unexpectedConnection = getConnection(_negativeFailoverUrl);
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
    public void testQuorumLostAndRestored_OriginalMasterRejoinsTheGroup() throws Exception
    {
        final Connection connection = getConnection(_positiveFailoverUrl);

        ((AMQConnection)connection).setConnectionListener(_failoverListener);

        Set<Integer> ports = _groupCreator.getBrokerPortNumbersForNodes();

        final int activeBrokerPort = _groupCreator.getBrokerPortNumberFromConnection(connection);
        ports.remove(activeBrokerPort);

        Session session1 = connection.createSession(true, Session.SESSION_TRANSACTED);
        Session session2 = connection.createSession(true, Session.SESSION_TRANSACTED);

        Destination dest = session1.createQueue(getTestQueueName());
        session1.createConsumer(dest).close();

        MessageProducer producer1 = session1.createProducer(dest);
        producer1.send(session1.createMessage());
        MessageProducer producer2 = session2.createProducer(dest);
        producer2.send(session2.createMessage());

        // Leave transactions open, this will leave two store transactions open on the store

        // Stop all other nodes
        for (Integer p : ports)
        {
            _groupCreator.stopNode(p);
        }

        // Await the old master discovering that it is all alone
        _groupCreator.awaitNodeToAttainRole(activeBrokerPort, "WAITING");

        // Restart all other nodes
        for (Integer p : ports)
        {
            _groupCreator.startNode(p);
        }

        _failoverListener.awaitFailoverCompletion(20000);

        _groupCreator.awaitNodeToAttainRole(activeBrokerPort, "MASTER", "REPLICA");
    }

    public void testPersistentMessagesAvailableAfterFailover() throws Exception
    {
        final Connection connection = getConnection(_positiveFailoverUrl);

        ((AMQConnection)connection).setConnectionListener(_failoverListener);

        final int activeBrokerPort = _groupCreator.getBrokerPortNumberFromConnection(connection);

        Session producingSession = connection.createSession(true, Session.SESSION_TRANSACTED);
        Destination queue = producingSession.createQueue(getTestQueueName());
        producingSession.createConsumer(queue).close();
        sendMessage(producingSession, queue, 10);

        _groupCreator.stopNode(activeBrokerPort);
        LOGGER.info("Old master (broker port " + activeBrokerPort + ") is stopped");

        _failoverListener.awaitFailoverCompletion(20000);
        LOGGER.info("Failover has finished");

        final int activeBrokerPortAfterFailover = _groupCreator.getBrokerPortNumberFromConnection(connection);
        LOGGER.info("New master (broker port " + activeBrokerPort + ") after failover");

        Session consumingSession = connection.createSession(true, Session.SESSION_TRANSACTED);
        MessageConsumer consumer = consumingSession.createConsumer(queue);

        connection.start();
        for(int i = 0; i < 10; i++)
        {
            Message m = consumer.receive(RECEIVE_TIMEOUT);
            assertNotNull("Message " + i + "  is not received", m);
            assertEquals("Unexpected message received", i, m.getIntProperty(INDEX));
        }
        consumingSession.commit();
    }

    public void testTransferMasterFromLocalNode() throws Exception
    {
        final Connection connection = getConnection(_positiveFailoverUrl);

        final int activeBrokerPort = _groupCreator.getBrokerPortNumberFromConnection(connection);
        LOGGER.info("Active connection port " + activeBrokerPort);

        final int inactiveBrokerPort = _groupCreator.getPortNumberOfAnInactiveBroker(connection);
        LOGGER.info("Update role attribute on inactive broker on port " + inactiveBrokerPort);

        // transfer mastership 3 times in order to verify
        // that repeated mastership transfer to the same node works, See QPID-6996
        transferMasterFromLocalNode(connection, inactiveBrokerPort, activeBrokerPort);
        transferMasterFromLocalNode(connection, activeBrokerPort, inactiveBrokerPort);
        transferMasterFromLocalNode(connection, inactiveBrokerPort, activeBrokerPort);
    }

    private void transferMasterFromLocalNode(final Connection connection,
                                             final int inactiveBrokerPort,
                                             final int activeBrokerPort) throws Exception
    {
        _failoverListener = new FailoverAwaitingListener();
        ((AMQConnection)connection).setConnectionListener(_failoverListener);

        Map<String, Object> attributes = _groupCreator.getNodeAttributes(inactiveBrokerPort);
        assertEquals("Inactive broker has unexpected role", "REPLICA", attributes.get(BDBHAVirtualHostNode.ROLE));
        _groupCreator.setNodeAttributes(inactiveBrokerPort,
                                          Collections.<String, Object>singletonMap(BDBHAVirtualHostNode.ROLE, "MASTER"));

        _failoverListener.awaitFailoverCompletion(20000);
        LOGGER.info("Listener has finished");

        attributes = _groupCreator.getNodeAttributes(inactiveBrokerPort);
        assertEquals("Inactive broker has unexpected role", "MASTER", attributes.get(BDBHAVirtualHostNode.ROLE));

        assertProducingConsuming(connection);

        _groupCreator.awaitNodeToAttainRole(activeBrokerPort, "REPLICA");
    }

    public void testTransferMasterFromRemoteNode() throws Exception
    {
        final Connection connection = getConnection(_positiveFailoverUrl);

        ((AMQConnection)connection).setConnectionListener(_failoverListener);

        final int activeBrokerPort = _groupCreator.getBrokerPortNumberFromConnection(connection);
        LOGGER.info("Active connection port " + activeBrokerPort);

        final int inactiveBrokerPort = _groupCreator.getPortNumberOfAnInactiveBroker(connection);
        LOGGER.info("Update role attribute on inactive broker on port " + inactiveBrokerPort);

        // transfer mastership 3 times in order to verify
        // that repeated mastership transfer to the same node works, See QPID-6996
        transferMasterFromRemoteNode(connection, activeBrokerPort, inactiveBrokerPort);
        transferMasterFromRemoteNode(connection, inactiveBrokerPort, activeBrokerPort);
        transferMasterFromRemoteNode(connection, activeBrokerPort, inactiveBrokerPort);
    }

    private void transferMasterFromRemoteNode(final Connection connection,
                                              final int activeBrokerPort,
                                              final int inactiveBrokerPort) throws Exception
    {
        _failoverListener = new FailoverAwaitingListener();
        ((AMQConnection)connection).setConnectionListener(_failoverListener);

        _groupCreator.awaitNodeToAttainRole(activeBrokerPort, inactiveBrokerPort, "REPLICA");
        Map<String, Object> attributes = _groupCreator.getNodeAttributes(activeBrokerPort, inactiveBrokerPort);
        assertEquals("Inactive broker has unexpected role", "REPLICA", attributes.get(BDBHAVirtualHostNode.ROLE));

        _groupCreator.setNodeAttributes(activeBrokerPort, inactiveBrokerPort, Collections.<String, Object>singletonMap(BDBHAVirtualHostNode.ROLE, "MASTER"));

        _failoverListener.awaitFailoverCompletion(20000);
        LOGGER.info("Listener has finished");

        attributes = _groupCreator.getNodeAttributes(inactiveBrokerPort);
        assertEquals("Inactive broker has unexpected role", "MASTER", attributes.get(BDBHAVirtualHostNode.ROLE));

        assertProducingConsuming(connection);

        _groupCreator.awaitNodeToAttainRole(activeBrokerPort, "REPLICA");
    }

    public void testTransferMasterWhilstMessagesInFlight() throws Exception
    {
        final Connection connection = getConnection(_positiveFailoverUrl);
        ((AMQConnection) connection).setConnectionListener(_failoverListener);

        final Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        final Destination destination = session.createQueue(getTestQueueName());
        session.createConsumer(destination).close();

        final AtomicBoolean masterTransferred = new AtomicBoolean(false);
        final AtomicBoolean keepRunning = new AtomicBoolean(true);
        final AtomicReference<Exception> workerException = new AtomicReference<>();
        final CountDownLatch producedOneBefore = new CountDownLatch(1);
        final CountDownLatch producedOneAfter = new CountDownLatch(1);
        final CountDownLatch workerShutdown = new CountDownLatch(1);

        Runnable producer = new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    int count = 0;
                    MessageProducer producer = session.createProducer(destination);

                    while (keepRunning.get())
                    {
                        String messageText = "message" + count;
                        try
                        {
                            Message message = session.createTextMessage(messageText);
                            producer.send(message);
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
                        catch(JMSException je)
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
            }

        };

        Thread backgroundWorker = new Thread(producer);
        backgroundWorker.start();

        boolean workerRunning = producedOneBefore.await(5000, TimeUnit.MILLISECONDS);
        assertTrue(workerRunning);

        final int activeBrokerPort = _groupCreator.getBrokerPortNumberFromConnection(connection);
        LOGGER.info("Active connection port " + activeBrokerPort);

        final int inactiveBrokerPort = _groupCreator.getPortNumberOfAnInactiveBroker(connection);
        LOGGER.info("Update role attribute on inactive broker on port " + inactiveBrokerPort);

        _groupCreator.awaitNodeToAttainRole(activeBrokerPort, inactiveBrokerPort, "REPLICA");
        Map<String, Object> attributes = _groupCreator.getNodeAttributes(activeBrokerPort, inactiveBrokerPort);
        assertEquals("Inactive broker has unexpected role", "REPLICA", attributes.get(BDBHAVirtualHostNode.ROLE));

        _groupCreator.setNodeAttributes(activeBrokerPort, inactiveBrokerPort, Collections.<String, Object>singletonMap(BDBHAVirtualHostNode.ROLE, "MASTER"));

        _failoverListener.awaitFailoverCompletion(20000);
        LOGGER.info("Failover has finished");

        attributes = _groupCreator.getNodeAttributes(inactiveBrokerPort);
        assertEquals("New master has unexpected role", "MASTER", attributes.get(BDBHAVirtualHostNode.ROLE));

        _groupCreator.awaitNodeToAttainRole(activeBrokerPort, "REPLICA");

        LOGGER.info("Master transfer known to have completed successfully.");
        masterTransferred.set(true);

        boolean producedMore = producedOneAfter.await(5000, TimeUnit.MILLISECONDS);
        assertTrue("Should have successfully produced at least one message after transfer complete", producedMore);

        keepRunning.set(false);
        boolean shutdown = workerShutdown.await(5000, TimeUnit.MILLISECONDS);
        assertTrue("Worker thread should have shutdown", shutdown);

        backgroundWorker.join(5000);
        assertNull(workerException.get());

        assertNotNull(session.createTemporaryQueue());

    }

    public void testInFlightTransactionsWhilstMajorityIsLost() throws Exception
    {
        int connectionNumber = Integer.getInteger("MultiNodeTest.testInFlightTransactionsWhilstMajorityIsLost.numberOfConnections", 20);
        ExecutorService executorService = Executors.newFixedThreadPool(connectionNumber + NUMBER_OF_NODES -1);
        try
        {
            ConnectionURL connectionUrl = _groupCreator.getConnectionUrlForAllClusterNodes(100, 0, 100);

            final Connection consumerConnection = getConnection(connectionUrl);
            consumerConnection.start();

            final Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final Destination destination = consumerSession.createQueue(getTestQueueName());
            consumerSession.createConsumer(destination).setMessageListener(new MessageListener()
            {
                @Override
                public void onMessage(final Message message)
                {
                    try
                    {
                        LOGGER.info("Message received: " + ((TextMessage) message).getText());
                    }
                    catch (JMSException e)
                    {
                        LOGGER.error("Failure to get message text", e);
                    }
                }
            });

            final Connection[] connections = new Connection[connectionNumber];
            final Session[] sessions = new Session[connectionNumber];
            for (int i = 0; i < sessions.length; i++)
            {
                connections[i] = getConnection(connectionUrl);
                sessions[i] = connections[i].createSession(true, Session.SESSION_TRANSACTED);
                LOGGER.info("Session {} is created", i);
            }

            List<Integer> ports = new ArrayList<>(_groupCreator.getBrokerPortNumbersForNodes());

            int maxMessageSize = 10;
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < maxMessageSize - 2; i++)
            {
                sb.append("X");
            }
            String messageText = sb.toString();
            for (int n = 0; n < NUMBER_OF_NODES; n++)
            {
                LOGGER.info("Starting iteration {}", n);

                FailoverAwaitingListener failoverListener = new FailoverAwaitingListener(connectionNumber);

                for (int i = 0; i < sessions.length; i++)
                {
                    AMQConnection connection = (AMQConnection)connections[i];
                    connection.setConnectionListener(failoverListener);

                    MessageProducer producer = sessions[i].createProducer(destination);
                    Message message = sessions[i].createTextMessage(messageText + "-" + i);
                    producer.send(message);
                }

                LOGGER.info("All publishing sessions have uncommitted transactions");

                final int activeBrokerPort = _groupCreator.getBrokerPortNumberFromConnection(connections[0]);
                LOGGER.info("Active connection port " + activeBrokerPort);

                List<Integer> inactivePorts = new ArrayList<>(ports);
                inactivePorts.remove(new Integer(activeBrokerPort));

                final CountDownLatch latch = new CountDownLatch(inactivePorts.size());
                for (int port : inactivePorts)
                {
                    final int inactiveBrokerPort = port;
                    LOGGER.info("Stop node for inactive broker on port " + inactiveBrokerPort);

                    executorService.submit(new Runnable()
                    {
                        @Override
                        public void run()
                        {
                            try
                            {
                                _groupCreator.setNodeAttributes(inactiveBrokerPort,
                                                                inactiveBrokerPort,
                                                                Collections.<String, Object>singletonMap(
                                                                        BDBHAVirtualHostNode.DESIRED_STATE,
                                                                        State.STOPPED.name()));
                            }
                            catch (Exception e)
                            {
                                LOGGER.error("Failed to stop node on broker with port " + inactiveBrokerPort, e);
                            }
                            finally
                            {
                                latch.countDown();
                            }
                        }
                    });
                }

                latch.await(500, TimeUnit.MILLISECONDS);

                LOGGER.info("Committing transactions in parallel to provoke a lot of syncing to disk");
                for (int i = 0; i < sessions.length; i++)
                {
                    final Session session = sessions[i];
                    executorService.submit(new Runnable()
                    {
                        @Override
                        public void run()
                        {
                            try
                            {
                                session.commit();
                            }
                            catch (JMSException e)
                            {
                                // majority of commits might fail due to insufficient replicas
                            }
                        }
                    });
                }

                LOGGER.info("Verify that stopped nodes are in detached role");
                for (int port : inactivePorts)
                {
                    _groupCreator.awaitNodeToAttainRole(port, NodeRole.DETACHED.name());
                }

                LOGGER.info("Start stopped nodes");
                for (int port : inactivePorts)
                {
                    LOGGER.info("Starting node for inactive broker on port " + port);
                    try
                    {
                        _groupCreator.setNodeAttributes(port,
                                                        port,
                                                        Collections.<String, Object>singletonMap(
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
                    _groupCreator.awaitNodeToAttainRole(port, "REPLICA", "MASTER");
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
            executorService.shutdown();
        }
    }

    /**
     * Tests aims to demonstrate that in a disaster situation (where all nodes except the master is lost), that operation
     * can be continued from a single node using the QUORUM_OVERRIDE feature.
     */
    public void testQuorumOverride() throws Exception
    {
        final Connection connection = getConnection(_positiveFailoverUrl);
        ((AMQConnection)connection).setConnectionListener(_failoverListener);

        Set<Integer> ports = _groupCreator.getBrokerPortNumbersForNodes();

        final int activeBrokerPort = _groupCreator.getBrokerPortNumberFromConnection(connection);
        ports.remove(activeBrokerPort);

        // Stop all other nodes
        for (Integer p : ports)
        {
            _groupCreator.stopNode(p);
        }

        LOGGER.info("Awaiting failover to start");
        _failoverListener.awaitPreFailover(20000);
        LOGGER.info("Failover has begun");

        Map<String, Object> attributes = _groupCreator.getNodeAttributes(activeBrokerPort);
        assertEquals("Broker has unexpected quorum override", new Integer(0), attributes.get(BDBHAVirtualHostNode.QUORUM_OVERRIDE));
        _groupCreator.setNodeAttributes(activeBrokerPort, Collections.<String, Object>singletonMap(BDBHAVirtualHostNode.QUORUM_OVERRIDE, 1));

        attributes = _groupCreator.getNodeAttributes(activeBrokerPort);
        assertEquals("Broker has unexpected quorum override", new Integer(1), attributes.get(BDBHAVirtualHostNode.QUORUM_OVERRIDE));

        _failoverListener.awaitFailoverCompletion(20000);
        LOGGER.info("Failover has finished");

        assertProducingConsuming(connection);
    }

    public void testPriority() throws Exception
    {
        final Connection connection = getConnection(_positiveFailoverUrl);

        ((AMQConnection)connection).setConnectionListener(_failoverListener);

        final int activeBrokerPort = _groupCreator.getBrokerPortNumberFromConnection(connection);
        LOGGER.info("Active connection port " + activeBrokerPort);

        int priority = 1;
        Integer highestPriorityBrokerPort = null;
        Set<Integer> ports = _groupCreator.getBrokerPortNumbersForNodes();
        for (Integer port : ports)
        {
            if (activeBrokerPort != port.intValue())
            {
                priority = priority + 1;
                highestPriorityBrokerPort = port;
                _groupCreator.setNodeAttributes(port, port, Collections.<String, Object>singletonMap(BDBHAVirtualHostNode.PRIORITY, priority));
                Map<String, Object> attributes = _groupCreator.getNodeAttributes(port, port);
                assertEquals("Broker has unexpected priority", priority, attributes.get(BDBHAVirtualHostNode.PRIORITY));
            }
        }

        LOGGER.info("Broker on port " + highestPriorityBrokerPort + " has the highest priority of " + priority);

        LOGGER.info("Shutting down the MASTER");
        _groupCreator.stopNode(activeBrokerPort);

        _failoverListener.awaitFailoverCompletion(20000);
        LOGGER.info("Listener has finished");

        Map<String, Object> attributes = _groupCreator.getNodeAttributes(highestPriorityBrokerPort, highestPriorityBrokerPort);
        assertEquals("Inactive broker has unexpected role", "MASTER", attributes.get(BDBHAVirtualHostNode.ROLE));

        assertProducingConsuming(connection);
    }

    public void testClusterCannotStartWithIntruder() throws Exception
    {
        //set property explicitly as test requires broker to start to enable check for ERRORED nodes
        setSystemProperty(Broker.BROKER_FAIL_STARTUP_WITH_ERRORED_CHILD, String.valueOf(Boolean.FALSE));

        int intruderPort = getNextAvailable(Collections.max(_groupCreator.getBdbPortNumbers()) + 1);
        String nodeName = "intruder";
        String nodeHostPort = _groupCreator.getIpAddressOfBrokerHost() + ":" + intruderPort;
        File environmentPathFile = Files.createTempDirectory("qpid-work-intruder").toFile();
        try
        {
            environmentPathFile.mkdirs();
            ReplicationConfig replicationConfig =
                    new ReplicationConfig(_groupCreator.getGroupName(), nodeName, nodeHostPort);
            replicationConfig.setHelperHosts(_groupCreator.getHelperHostPort());
            EnvironmentConfig envConfig = new EnvironmentConfig();
            envConfig.setAllowCreate(true);
            envConfig.setTransactional(true);
            envConfig.setDurability(new Durability(Durability.SyncPolicy.SYNC,
                                                   Durability.SyncPolicy.WRITE_NO_SYNC,
                                                   Durability.ReplicaAckPolicy.SIMPLE_MAJORITY));

            ReplicatedEnvironment intruder = null;
            final String currentThreadName = Thread.currentThread().getName();
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
                    Thread.currentThread().setName(currentThreadName);
                }
            }

            for (int port : _groupCreator.getBrokerPortNumbersForNodes())
            {
                _groupCreator.awaitNodeToAttainAttributeValue(port,
                                                              port,
                                                              BDBHAVirtualHostNode.STATE,
                                                              State.ERRORED.name());
            }

            _groupCreator.stopCluster();
            _groupCreator.startCluster();

            for (int port : _groupCreator.getBrokerPortNumbersForNodes())
            {
                _groupCreator.awaitNodeToAttainAttributeValue(port,
                                                              port,
                                                              BDBHAVirtualHostNode.STATE,
                                                              State.ERRORED.name());
            }
        }
        finally
        {
            FileUtils.delete(environmentPathFile, true);
        }
    }

    private void awaitNextTransaction(final int brokerPort) throws Exception
    {
        Map<String, Object> attributes = _groupCreator.getNodeAttributes(brokerPort);
        final int originalTransactionId = (int) attributes.get(BDBHAVirtualHostNode.LAST_KNOWN_REPLICATION_TRANSACTION_ID);
        int currentTransactionId = 0;
        long timeout = System.currentTimeMillis() + 60000;
        LOGGER.debug("Awaiting next transaction. Original transaction id {}", originalTransactionId);
        do
        {
            Thread.sleep(250);
            attributes = _groupCreator.getNodeAttributes(brokerPort);
            currentTransactionId = (int) attributes.get(BDBHAVirtualHostNode.LAST_KNOWN_REPLICATION_TRANSACTION_ID);
            LOGGER.debug("Current transaction id {}", currentTransactionId);
        }
        while (originalTransactionId >= currentTransactionId && timeout > System.currentTimeMillis());

        assertTrue("Group transaction has not occurred within timeout."
                   + "Current transaction id " + currentTransactionId
                   + "Original transaction id " + originalTransactionId,
                   currentTransactionId > originalTransactionId);
    }

    private final class FailoverAwaitingListener implements ConnectionListener
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
        public boolean preResubscribe()
        {
            return true;
        }

        @Override
        public synchronized boolean preFailover(boolean redirect)
        {
            _failoverStarted = true;
            _preFailoverLatch.countDown();
            return true;
        }

        public void awaitFailoverCompletion(long delay) throws InterruptedException
        {
            if (!_failoverCompletionLatch.await(delay, TimeUnit.MILLISECONDS))
            {
                LOGGER.warn("Failover did not occur, dumping threads:\n\n" + TestUtils.dumpThreads() + "\n");
                Map<Integer,String> threadDumps = _groupCreator.groupThreadumps();
                for (Map.Entry<Integer,String> entry : threadDumps.entrySet())
                {
                    LOGGER.warn("Broker {} thread dump:\n\n {}" , entry.getKey(), entry.getValue());
                }
            }
            assertEquals("Failover did not occur", 0, _failoverCompletionLatch.getCount());
        }

        public void assertNoFailoverCompletionWithin(long delay) throws InterruptedException
        {
            _failoverCompletionLatch.await(delay, TimeUnit.MILLISECONDS);
            assertEquals("Failover occurred unexpectedly", 1L, _failoverCompletionLatch.getCount());
        }

        public void awaitPreFailover(long delay) throws InterruptedException
        {
            boolean complete = _preFailoverLatch.await(delay, TimeUnit.MILLISECONDS);
            assertTrue("Failover was expected to begin within " + delay + " ms.", complete);
        }

        @Override
        public void failoverComplete()
        {
            _failoverCompletionLatch.countDown();
        }

        @Override
        public void bytesSent(long count)
        {
        }

        @Override
        public void bytesReceived(long count)
        {
        }

        public synchronized boolean isFailoverStarted()
        {
            return _failoverStarted;
        }
    }

}
