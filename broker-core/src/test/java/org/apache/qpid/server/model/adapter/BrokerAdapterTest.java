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
package org.apache.qpid.server.model.adapter;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.security.Principal;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.server.configuration.updater.TaskExecutorImpl;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.store.DurableConfigurationStore;
import org.apache.qpid.server.store.preferences.PreferenceStore;
import org.apache.qpid.server.virtualhost.TestMemoryVirtualHost;
import org.apache.qpid.server.virtualhostnode.TestVirtualHostNode;
import org.apache.qpid.test.utils.QpidTestCase;


public class BrokerAdapterTest extends QpidTestCase
{
    private TaskExecutorImpl _taskExecutor;
    private SystemConfig _systemConfig;
    private BrokerAdapter _brokerAdapter;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        _taskExecutor = new TaskExecutorImpl();
        _taskExecutor.start();

        _systemConfig = BrokerTestHelper.mockWithSystemPrincipal(SystemConfig.class, mock(Principal.class));
        when(_systemConfig.getTaskExecutor()).thenReturn(_taskExecutor);
        when(_systemConfig.getChildExecutor()).thenReturn(_taskExecutor);
        when(_systemConfig.getModel()).thenReturn(BrokerModel.getInstance());
        when(_systemConfig.getEventLogger()).thenReturn(new EventLogger());
        when(_systemConfig.getCategoryClass()).thenReturn(SystemConfig.class);
        when(_systemConfig.createPreferenceStore()).thenReturn(mock(PreferenceStore.class));
    }

    @Override
    public void tearDown() throws Exception
    {
        try
        {
            if (_brokerAdapter != null)
            {
                _brokerAdapter.close();
            }

            if (_taskExecutor != null)
            {
                _taskExecutor.stopImmediately();
            }

        }
        finally
        {
            super.tearDown();
        }
    }

    public void testAssignTargetSizes_NoQueueDepth() throws Exception
    {
        doAssignTargetSizeTest(new long[] {0, 0}, 1024 * 1024 * 1024);
    }

    public void testAssignTargetSizes_OneQueue() throws Exception
    {
        doAssignTargetSizeTest(new long[] {37}, 1024 * 1024 * 1024);
    }

    public void testAssignTargetSizes_ThreeQueues() throws Exception
    {
        doAssignTargetSizeTest(new long[] {37, 47, 0}, 1024 * 1024 * 1024);
    }

    public void testAssignTargetSizes_QueuesOversize() throws Exception
    {
        int flowToDiskThreshold = 1024 * 1024 * 1024;
        doAssignTargetSizeTest(new long[] {flowToDiskThreshold / 2, flowToDiskThreshold / 2 , 1024},
                               flowToDiskThreshold);
    }

    public void testNetworkBufferSize()
    {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(Broker.NAME, "Broker");
        attributes.put(Broker.MODEL_VERSION, BrokerModel.MODEL_VERSION);
        attributes.put(Broker.DURABLE, true);

        // testing successful case is not possible because of the static nature of QpidByteBuffer which *should* be unrelated

        // testing unsuccessful case
        attributes.put(Broker.CONTEXT, Collections.singletonMap(Broker.NETWORK_BUFFER_SIZE, Broker.MINIMUM_NETWORK_BUFFER_SIZE - 1));
        _brokerAdapter = new BrokerAdapter(attributes, _systemConfig);
        _brokerAdapter.open();
        assertEquals("Broker open should fail with network buffer size less then minimum",
                     State.ERRORED,
                     _brokerAdapter.getState());
        assertEquals("Unexpected buffer size",
                     Broker.DEFAULT_NETWORK_BUFFER_SIZE,
                     _brokerAdapter.getNetworkBufferSize());
    }

    private void doAssignTargetSizeTest(final long[] virtualHostQueueSizes, final long flowToDiskThreshold)
    {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("name", "Broker");
        attributes.put(Broker.MODEL_VERSION, BrokerModel.MODEL_VERSION);
        attributes.put(Broker.DURABLE, true);
        attributes.put("context", Collections.singletonMap(Broker.BROKER_FLOW_TO_DISK_THRESHOLD, flowToDiskThreshold));
        _brokerAdapter = new BrokerAdapter(attributes, _systemConfig);
        _brokerAdapter.open();
        assertEquals("Unexpected broker state", State.ACTIVE, _brokerAdapter.getState());

        for(int i=0; i < virtualHostQueueSizes.length; i++)
        {
            createVhnWithVh(_brokerAdapter, i, virtualHostQueueSizes[i]);
        }

        long totalAssignedTargetSize = 0;
        for(VirtualHostNode<?> vhn : _brokerAdapter.getVirtualHostNodes())
        {
            long targetSize = vhn.getVirtualHost().getTargetSize();
            assertTrue("A virtualhost's target size cannot be zero", targetSize > 0);
            totalAssignedTargetSize += targetSize;
        }

        long diff = Math.abs(flowToDiskThreshold - totalAssignedTargetSize);
        long tolerance = _brokerAdapter.getVirtualHostNodes().size() * 2;
        assertTrue(String.format("Assigned target size not within expected tolerance. Diff %d Tolerance %d", diff, tolerance), diff < tolerance);
    }

    private void createVhnWithVh(final BrokerAdapter brokerAdapter, int nameSuffix, final long totalQueueSize)
    {
        final Map<String, Object> vhnAttributes = new HashMap<>();
        vhnAttributes.put(VirtualHostNode.TYPE, TestVirtualHostNode.VIRTUAL_HOST_NODE_TYPE);
        vhnAttributes.put(VirtualHostNode.NAME, "testVhn" + nameSuffix);

        final DurableConfigurationStore store = mock(DurableConfigurationStore.class);
        TestVirtualHostNode vhn = new TestVirtualHostNode(brokerAdapter, vhnAttributes, store);
        vhn.create();


        final Map<String, Object> vhAttributes = new HashMap<>();
        vhAttributes.put(VirtualHost.TYPE, TestMemoryVirtualHost.VIRTUAL_HOST_TYPE);
        vhAttributes.put(VirtualHost.NAME, "testVh" + nameSuffix);
        TestMemoryVirtualHost vh = new TestMemoryVirtualHost(vhAttributes, vhn)
        {
            @Override
            public long getTotalQueueDepthBytes()
            {
                return totalQueueSize;
            }
        };
        vh.create();
    }
}
