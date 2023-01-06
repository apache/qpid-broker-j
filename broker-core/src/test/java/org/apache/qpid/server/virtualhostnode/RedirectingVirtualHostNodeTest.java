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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.ConfiguredObjectFactoryImpl;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.test.utils.UnitTestBase;

public class RedirectingVirtualHostNodeTest extends UnitTestBase
{
    private static final String TEST_VIRTUAL_HOST_NODE_NAME = "testNode";

    private Broker<?> _broker;
    private TaskExecutor _taskExecutor;
    private AmqpPort<?> _port;

    @BeforeEach
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception
    {
        _broker = BrokerTestHelper.createBrokerMock();
        final SystemConfig<?> systemConfig = (SystemConfig<?>) _broker.getParent();
        when(systemConfig.getObjectFactory()).thenReturn(new ConfiguredObjectFactoryImpl(mock(Model.class)));

        _taskExecutor = new CurrentThreadTaskExecutor();
        _taskExecutor.start();
        when(_broker.getTaskExecutor()).thenReturn(_taskExecutor);
        when(_broker.getChildExecutor()).thenReturn(_taskExecutor);

        final AuthenticationProvider<?> dummyAuthProvider = mock(AuthenticationProvider.class);
        when(dummyAuthProvider.getName()).thenReturn("dummy");
        when(dummyAuthProvider.getId()).thenReturn(randomUUID());
        when(dummyAuthProvider.getMechanisms()).thenReturn(List.of("PLAIN"));
        when(_broker.getChildren(eq(AuthenticationProvider.class))).thenReturn(Set.of(dummyAuthProvider));

        final Map<String, Object> attributes = new HashMap<>();
        attributes.put(Port.NAME, getTestName());
        attributes.put(Port.PORT, 0);
        attributes.put(Port.AUTHENTICATION_PROVIDER, "dummy");
        attributes.put(Port.TYPE, "AMQP");
        _port = (AmqpPort<?>) _broker.getObjectFactory().create(Port.class, attributes, _broker);
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        _taskExecutor.stopImmediately();
    }

    @AfterAll
    public void afterAll()
    {
        when(_broker.getChildren(eq(AuthenticationProvider.class))).thenReturn(Set.of());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testRedirectingVHNHasRedirectingVHToo()
    {
        final Map<String, Object> attributes = createVirtualHostNodeAttributes();

        final RedirectingVirtualHostNode<?> node =
                (RedirectingVirtualHostNode<?>) _broker.getObjectFactory().create(VirtualHostNode.class, attributes,
                _broker);
        assertEquals(1, (long) node.getChildren(VirtualHost.class).size(),
                "Unexpected number of virtualhost children");

        final boolean condition =
                node.getChildren(VirtualHost.class).iterator().next() instanceof RedirectingVirtualHost;
        assertTrue(condition, "Virtualhost child is of unexpected type");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testStopAndRestartVHN()
    {
        final Map<String, Object> attributes = createVirtualHostNodeAttributes();

        final RedirectingVirtualHostNode<?> node =
                (RedirectingVirtualHostNode<?>) _broker.getObjectFactory().create(VirtualHostNode.class, attributes,
                _broker);
        assertEquals(1, (long) node.getChildren(VirtualHost.class).size(),
                "Unexpected number of virtualhost children before stop");
        node.stop();
        assertEquals(0, (long) node.getChildren(VirtualHost.class).size(),
                "Unexpected number of virtualhost children after stop");
        node.start();
        assertEquals(1, (long) node.getChildren(VirtualHost.class).size(),
                "Unexpected number of virtualhost children after restart");
    }

    private Map<String, Object> createVirtualHostNodeAttributes()
    {
        return Map.of(VirtualHostNode.TYPE, RedirectingVirtualHostNodeImpl.VIRTUAL_HOST_NODE_TYPE,
                VirtualHostNode.NAME, TEST_VIRTUAL_HOST_NODE_NAME,
                RedirectingVirtualHostNode.REDIRECTS, Map.of(_port, "myalternativehostname"));
    }
}
