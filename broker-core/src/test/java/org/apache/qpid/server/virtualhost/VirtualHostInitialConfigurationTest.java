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
import org.apache.qpid.server.message.MessageSource;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.ConfiguredObjectFactory;
import org.apache.qpid.server.model.ConfiguredObjectFactoryImpl;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.security.AccessControl;
import org.apache.qpid.server.store.DurableConfigurationStore;
import org.apache.qpid.server.store.preferences.PreferenceStore;
import org.apache.qpid.test.utils.UnitTestBase;

public class VirtualHostInitialConfigurationTest extends UnitTestBase
{
    private VirtualHostNode<?> _virtualHostNode;
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

        when(_virtualHostNode.createPreferenceStore()).thenReturn(mock(PreferenceStore.class));
    }

    @After
    public void tearDown() throws Exception
    {
        _taskExecutor.stopImmediately();
    }

    @Test
    public void nodeAutoCreationPolicyWithEmptyAttributes()
    {
        String nodeAutoCreationPolicies = "{"
                                          + "\"pattern\":\".*\","
                                          + "\"createdOnPublish\":\"true\","
                                          + "\"createdOnConsume\":\"true\","
                                          + "\"nodeType\":\"queue\", "
                                          + "\"attributes\": {}"
                                          + "}";
        VirtualHost<?> vh = createVirtualHost(nodeAutoCreationPolicies);
        MessageSource ms =  vh.getAttainedMessageSource("queue1");
        assertTrue(Queue.class.isAssignableFrom(ms.getClass()));
        assertEquals("queue1", ms.getName());
    }

    @Test
    public void nodeAutoCreationPolicyWithoutAttributes()
    {
        String nodeAutoCreationPolicies = "{"
                                          + "\"pattern\":\".*\","
                                          + "\"createdOnPublish\":\"true\","
                                          + "\"createdOnConsume\":\"true\","
                                          + "\"nodeType\":\"queue\""
                                          + "}";
        VirtualHost<?> vh = createVirtualHost(nodeAutoCreationPolicies);
        MessageSource ms =  vh.getAttainedMessageSource("queue1");
        assertTrue(Queue.class.isAssignableFrom(ms.getClass()));
        assertEquals("queue1", ms.getName());
    }

    private VirtualHost<?> createVirtualHost(String nodeAutoCreationPolicies)
    {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(VirtualHost.NAME, getTestName());
        attributes.put(VirtualHost.TYPE, TestMemoryVirtualHost.VIRTUAL_HOST_TYPE);
        attributes.put("nodeAutoCreationPolicies", nodeAutoCreationPolicies);

        attributes = new HashMap<>(attributes);
        attributes.put(VirtualHost.ID, UUID.randomUUID());
        TestMemoryVirtualHost host = new TestMemoryVirtualHost(attributes, _virtualHostNode);
        host.create();
        host.start();
        return host;
    }
}
