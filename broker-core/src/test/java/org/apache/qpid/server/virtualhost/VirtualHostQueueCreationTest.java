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
package org.apache.qpid.server.virtualhost;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.security.Principal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.ConfiguredObjectFactory;
import org.apache.qpid.server.model.ConfiguredObjectFactoryImpl;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.queue.MessageGroupType;
import org.apache.qpid.server.queue.PriorityQueue;
import org.apache.qpid.server.queue.PriorityQueueImpl;
import org.apache.qpid.server.queue.StandardQueueImpl;
import org.apache.qpid.server.security.AccessControl;
import org.apache.qpid.server.store.DurableConfigurationStore;
import org.apache.qpid.server.store.preferences.PreferenceStore;
import org.apache.qpid.test.utils.UnitTestBase;

public class VirtualHostQueueCreationTest extends UnitTestBase
{
    private VirtualHost<?> _virtualHost;
    private VirtualHostNode _virtualHostNode;
    private TaskExecutor _taskExecutor;

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Before
    public void setUp() throws Exception
    {

        EventLogger eventLogger = mock(EventLogger.class);
        ConfiguredObjectFactory objectFactory = new ConfiguredObjectFactoryImpl(BrokerModel.getInstance());

        _taskExecutor = new CurrentThreadTaskExecutor();
        _taskExecutor.start();

        SystemConfig<?> context = mock(SystemConfig.class);
        when(context.getEventLogger()).thenReturn(eventLogger);
        when(context.createPreferenceStore()).thenReturn(mock(PreferenceStore.class));

        Principal systemPrincipal = mock(Principal.class);
        AccessControl accessControl = BrokerTestHelper.createAccessControlMock();

        Broker broker = BrokerTestHelper.mockWithSystemPrincipalAndAccessControl(Broker.class, systemPrincipal, accessControl);
        when(broker.getObjectFactory()).thenReturn(objectFactory);
        when(broker.getCategoryClass()).thenReturn(Broker.class);
        when(broker.getParent()).thenReturn(context);
        when(broker.getModel()).thenReturn(objectFactory.getModel());
        when(broker.getTaskExecutor()).thenReturn(_taskExecutor);
        when(broker.getChildExecutor()).thenReturn(_taskExecutor);

        _virtualHostNode = BrokerTestHelper.mockWithSystemPrincipalAndAccessControl(VirtualHostNode.class, systemPrincipal, accessControl);
        when(_virtualHostNode.getParent()).thenReturn(broker);
        when(_virtualHostNode.getConfigurationStore()).thenReturn(mock(DurableConfigurationStore.class));
        when(_virtualHostNode.getObjectFactory()).thenReturn(objectFactory);
        when(_virtualHostNode.getModel()).thenReturn(objectFactory.getModel());
        when(_virtualHostNode.getTaskExecutor()).thenReturn(_taskExecutor);
        when(_virtualHostNode.getChildExecutor()).thenReturn(_taskExecutor);
        when(_virtualHostNode.getCategoryClass()).thenReturn(VirtualHostNode.class);

        when(_virtualHostNode.createPreferenceStore()).thenReturn(mock(PreferenceStore.class));
        _virtualHost = createHost();
    }

    @After
    public void tearDown() throws Exception
    {
        try
        {
            _taskExecutor.stopImmediately();
            _virtualHost.close();
        }
        finally
        {
        }
    }

    private VirtualHost<?> createHost()
    {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(VirtualHost.NAME, getTestName());
        attributes.put(VirtualHost.TYPE, TestMemoryVirtualHost.VIRTUAL_HOST_TYPE);

        attributes = new HashMap<>(attributes);
        attributes.put(VirtualHost.ID, UUID.randomUUID());
        TestMemoryVirtualHost host = new TestMemoryVirtualHost(attributes, _virtualHostNode);
        host.create();
        host.start();
        return host;
    }

    @Test
    public void testPriorityQueueRegistration() throws Exception
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(Queue.ID, UUID.randomUUID());
        attributes.put(Queue.NAME, "testPriorityQueue");

        attributes.put(PriorityQueue.PRIORITIES, 5);


        Queue<?> queue = _virtualHost.createChild(Queue.class, attributes);

        final boolean condition = queue instanceof PriorityQueueImpl;
        assertTrue("Queue not a priority queue", condition);
        assertNotNull("Queue " + "testPriorityQueue" + " was not created", _virtualHost.getChildByName(Queue.class,
                                                                                                              "testPriorityQueue"));


        assertEquals("Queue was not registered in virtualhost",
                            (long) 1,
                            (long) _virtualHost.getChildren(Queue.class).size());
    }

    @Test
    public void testSimpleQueueCreation() throws Exception
    {
        String queueName = getTestName();

        Map<String,Object> attributes = new HashMap<>();
        attributes.put(Queue.ID, UUID.randomUUID());
        attributes.put(Queue.NAME, queueName);


        Queue<?> queue = _virtualHost.createChild(Queue.class, attributes);
        final boolean condition = queue instanceof StandardQueueImpl;
        assertTrue("Queue not a simple queue", condition);
        assertNotNull("Queue " + queueName + " was not created",
                             _virtualHost.getChildByName(Queue.class, queueName));

        assertEquals("Queue was not registered in virtualhost",
                            (long) 1,
                            (long) _virtualHost.getChildren(Queue.class).size());
    }

    @Test
    public void testMaximumDeliveryCount() throws Exception
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(Queue.ID, UUID.randomUUID());
        attributes.put(Queue.NAME, "testMaximumDeliveryCount");

        attributes.put(Queue.MAXIMUM_DELIVERY_ATTEMPTS, 5);

        final Queue<?> queue = _virtualHost.createChild(Queue.class, attributes);

        assertNotNull("The queue was not registered as expected ", queue);
        assertEquals("Maximum delivery count not as expected", (long) 5, (long) queue.getMaximumDeliveryAttempts
                ());

        assertEquals("Queue was not registered in virtualhost",
                            (long) 1,
                            (long) _virtualHost.getChildren(Queue.class).size());
    }

    @Test
    public void testMessageGroupQueue() throws Exception
    {

        Map<String,Object> attributes = new HashMap<>();
        attributes.put(Queue.ID, UUID.randomUUID());
        attributes.put(Queue.NAME, getTestName());
        attributes.put(Queue.MESSAGE_GROUP_KEY_OVERRIDE, "mykey");
        attributes.put(Queue.MESSAGE_GROUP_TYPE, MessageGroupType.SHARED_GROUPS);

        Queue<?> queue = _virtualHost.createChild(Queue.class, attributes);
        assertEquals("mykey", queue.getAttribute(Queue.MESSAGE_GROUP_KEY_OVERRIDE));
        assertEquals(MessageGroupType.SHARED_GROUPS, queue.getAttribute(Queue.MESSAGE_GROUP_TYPE));
    }


}
