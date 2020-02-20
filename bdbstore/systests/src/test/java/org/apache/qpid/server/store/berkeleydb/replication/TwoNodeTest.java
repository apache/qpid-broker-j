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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

import java.util.Map;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Queue;

import org.junit.Test;

import org.apache.qpid.server.virtualhostnode.berkeleydb.BDBHAVirtualHostNode;
import org.apache.qpid.systests.Utils;

@GroupConfig(numberOfNodes = 2, groupName = "test")
public class TwoNodeTest extends GroupJmsTestBase
{

    @Test
    public void testMasterDesignatedPrimaryCanBeRestartedWithoutReplica() throws Exception
    {
        final Connection initialConnection = getConnectionBuilder().build();
        int masterPort;
        Queue queue;
        try
        {
            queue = createTestQueue(initialConnection);
            masterPort = getJmsProvider().getConnectedURI(initialConnection).getPort();
            getBrokerAdmin().setDesignatedPrimary(masterPort, true);
        }
        finally
        {
            initialConnection.close();
        }
        getBrokerAdmin().stop();
        getBrokerAdmin().startNode(masterPort);

        assertProduceConsume(queue);
    }

    @Test
    public void testClusterRestartWithoutDesignatedPrimary() throws Exception
    {
        Queue queue;
        final Connection initialConnection = getConnectionBuilder().build();
        try
        {
            queue = createTestQueue(initialConnection);
            assertThat(Utils.produceConsume(initialConnection, queue), is(equalTo(true)));
        }
        finally
        {
            initialConnection.close();
        }

        getBrokerAdmin().stop();
        getBrokerAdmin().start();

        assertProduceConsume(queue);
    }

    @Test
    public void testDesignatedPrimaryContinuesAfterSecondaryStopped() throws Exception
    {
        int masterPort;
        Queue queue;
        final Connection initialConnection = getConnectionBuilder().build();
        try
        {
            masterPort = getJmsProvider().getConnectedURI(initialConnection).getPort();
            queue = createTestQueue(initialConnection);
            getBrokerAdmin().setDesignatedPrimary(masterPort, true);
        }
        finally
        {
            initialConnection.close();
        }

        int replicaPort = getBrokerAdmin().getAmqpPort(masterPort);
        getBrokerAdmin().stopNode(replicaPort);

        assertProduceConsume(queue);
    }

    @Test
    public void testPersistentOperationsFailOnNonDesignatedPrimaryAfterSecondaryStopped() throws Exception
    {
        int masterPort;
        Queue queue ;
        final Connection initialConnection = getConnectionBuilder().build();
        try
        {
            masterPort = getJmsProvider().getConnectedURI(initialConnection).getPort();
            queue = createTestQueue(initialConnection);
        }
        finally
        {
            initialConnection.close();
        }

        int replicaPort = getBrokerAdmin().getAmqpPort(masterPort);

        getBrokerAdmin().stopNode(replicaPort);

        try
        {
            Connection connection = getConnectionBuilder().setFailoverReconnectDelay(SHORT_FAILOVER_CONNECTDELAY)
                                                          .setFailoverReconnectAttempts(SHORT_FAILOVER_CYCLECOUNT)
                                                          .build();
            Utils.produceConsume(connection, queue);
            fail("Exception not thrown");
        }
        catch(JMSException e)
        {
            // JMSException should be thrown either on connection open, or produce/consume
            // depending on whether the relative timing of the node discovering that the
            // secondary has gone.
        }
    }

    @Test
    public void testSecondaryDoesNotBecomePrimaryWhenDesignatedPrimaryStopped() throws Exception
    {
        int masterPort;
        final Connection initialConnection = getConnectionBuilder().build();
        try
        {
            masterPort = getJmsProvider().getConnectedURI(initialConnection).getPort();
            getBrokerAdmin().setDesignatedPrimary(masterPort, true);
        }
        finally
        {
            initialConnection.close();
        }

        getBrokerAdmin().stopNode(masterPort);
        try
        {
            getConnectionBuilder().setFailoverReconnectDelay(SHORT_FAILOVER_CONNECTDELAY)
                                  .setFailoverReconnectAttempts(SHORT_FAILOVER_CYCLECOUNT)
                                  .build();
            fail("Connection not expected");
        }
        catch (JMSException e)
        {
            // PASS
        }
    }

    @Test
    public void testInitialDesignatedPrimaryStateOfNodes() throws Exception
    {
        int masterPort;
        final Connection initialConnection = getConnectionBuilder().build();
        try
        {
            masterPort = getJmsProvider().getConnectedURI(initialConnection).getPort();
            getBrokerAdmin().setDesignatedPrimary(masterPort, true);
        }
        finally
        {
            initialConnection.close();
        }

        Map<String, Object>
                primaryNodeAttributes = getBrokerAdmin().getNodeAttributes(masterPort);
        assertThat("Expected primary node to be set as designated primary",
                   primaryNodeAttributes.get(BDBHAVirtualHostNode.DESIGNATED_PRIMARY), is(equalTo(true)));

        int replicaPort = getBrokerAdmin().getAmqpPort(masterPort);

        Map<String, Object> secondaryNodeAttributes = getBrokerAdmin().getNodeAttributes(replicaPort);
        assertThat("Expected secondary node to NOT be set as designated primary",
                   secondaryNodeAttributes.get(BDBHAVirtualHostNode.DESIGNATED_PRIMARY), is(equalTo(false)));
    }

    @Test
    public void testSecondaryDesignatedAsPrimaryAfterOriginalPrimaryStopped() throws Exception
    {
        int masterPort;
        Queue queue;
        final Connection initialConnection = getConnectionBuilder().build();
        try
        {
            masterPort = getJmsProvider().getConnectedURI(initialConnection).getPort();
            getBrokerAdmin().setDesignatedPrimary(masterPort, true);
            queue = createTestQueue(initialConnection);
        }
        finally
        {
            initialConnection.close();
        }


        getBrokerAdmin().stopNode(masterPort);
        int replicaPort = getBrokerAdmin().getAmqpPort(masterPort);

        Map<String, Object> secondaryNodeAttributes = getBrokerAdmin().getNodeAttributes(replicaPort);
        assertThat("Expected secondary node to NOT be set as designated primary",
                   secondaryNodeAttributes.get(BDBHAVirtualHostNode.DESIGNATED_PRIMARY), is(equalTo(false)));

        getBrokerAdmin().setDesignatedPrimary(replicaPort, true);
        getBrokerAdmin().awaitNodeRole(replicaPort, "MASTER");

        assertProduceConsume(queue);
    }

    @Test
    public void testSetDesignatedAfterReplicaBeingStopped() throws Exception
    {
        final Connection initialConnection = getConnectionBuilder().build();
        int masterPort;
        Queue queue;
        try
        {
            masterPort = getJmsProvider().getConnectedURI(initialConnection).getPort();
            queue = createTestQueue(initialConnection);
        }
        finally
        {
            initialConnection.close();
        }

        int replicaPort = getBrokerAdmin().getAmqpPort(masterPort);
        getBrokerAdmin().stopNode(replicaPort);

        Map<String, Object>
                primaryNodeAttributes = getBrokerAdmin().getNodeAttributes(masterPort);
        assertThat("Expected node to NOT be set as designated primary",
                   primaryNodeAttributes.get(BDBHAVirtualHostNode.DESIGNATED_PRIMARY), is(equalTo(false)));

        getBrokerAdmin().setDesignatedPrimary(masterPort, true);
        getBrokerAdmin().awaitNodeRole(masterPort, "MASTER");

        assertProduceConsume(queue);
    }

}
