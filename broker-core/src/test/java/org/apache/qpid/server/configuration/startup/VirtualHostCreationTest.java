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
package org.apache.qpid.server.configuration.startup;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.security.Principal;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObjectFactory;
import org.apache.qpid.server.model.ConfiguredObjectFactoryImpl;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.store.DurableConfigurationStore;
import org.apache.qpid.server.virtualhost.TestMemoryVirtualHost;
import org.apache.qpid.test.utils.UnitTestBase;

public class VirtualHostCreationTest extends UnitTestBase
{
    @SuppressWarnings("rawtypes")
    private VirtualHostNode _virtualHostNode;

    @BeforeEach
    @SuppressWarnings({"rawtypes"})
    public void setUp() throws Exception
    {
        final EventLogger eventLogger = mock(EventLogger.class);
        final TaskExecutor executor = CurrentThreadTaskExecutor.newStartedInstance();
        final SystemConfig systemConfig = mock(SystemConfig.class);
        final ConfiguredObjectFactory objectFactory = new ConfiguredObjectFactoryImpl(BrokerModel.getInstance());
        when(systemConfig.getObjectFactory()).thenReturn(objectFactory);
        when(systemConfig.getModel()).thenReturn(objectFactory.getModel());
        when(systemConfig.getEventLogger()).thenReturn(eventLogger);
        when(systemConfig.getTaskExecutor()).thenReturn(executor);
        when(systemConfig.getChildExecutor()).thenReturn(executor);

        final Broker broker = mock(Broker.class);
        when(broker.getObjectFactory()).thenReturn(objectFactory);
        when(broker.getModel()).thenReturn(objectFactory.getModel());
        when(broker.getCategoryClass()).thenReturn(Broker.class);
        when(broker.getParent()).thenReturn(systemConfig);
        when(broker.getTaskExecutor()).thenReturn(executor);
        when(broker.getChildExecutor()).thenReturn(executor);

        _virtualHostNode = BrokerTestHelper.mockWithSystemPrincipal(VirtualHostNode.class, mock(Principal.class));
        when(_virtualHostNode.getParent()).thenReturn(broker);
        when(_virtualHostNode.getObjectFactory()).thenReturn(objectFactory);
        when(_virtualHostNode.getConfigurationStore()).thenReturn(mock(DurableConfigurationStore.class));
        when(_virtualHostNode.getModel()).thenReturn(objectFactory.getModel());
        when(_virtualHostNode.getCategoryClass()).thenReturn(VirtualHostNode.class);
        when(_virtualHostNode.getTaskExecutor()).thenReturn(executor);
        when(_virtualHostNode.getChildExecutor()).thenReturn(executor);
    }

    @Test
    public void testCreateVirtualHostFromStoreConfigAttributes()
    {
        final Map<String, Object> attributes = Map.of(VirtualHost.NAME, getTestName(),
                VirtualHost.TYPE, TestMemoryVirtualHost.VIRTUAL_HOST_TYPE,
                VirtualHost.ID, randomUUID());
        final VirtualHost<?> host = new TestMemoryVirtualHost(attributes, _virtualHostNode);
        host.open();

        assertNotNull(host, "Null is returned");
        assertEquals(getTestName(), host.getName(), "Unexpected name");
        host.close();
    }

    @Test
    public void testCreateWithoutMandatoryAttributesResultsInException()
    {
        final Map<String, Object> attributes = Map.of(VirtualHost.NAME, getTestName(),
                VirtualHost.TYPE, TestMemoryVirtualHost.VIRTUAL_HOST_TYPE);
        final String[] mandatoryAttributes = { VirtualHost.NAME };
        checkMandatoryAttributesAreValidated(mandatoryAttributes, attributes);
    }

    public void checkMandatoryAttributesAreValidated(final String[] mandatoryAttributes,
                                                     final Map<String, Object> attributes)
    {
        for (final String name : mandatoryAttributes)
        {
            final Map<String, Object> copy = new HashMap<>(attributes);
            copy.remove(name);
            copy.put(ConfiguredObject.ID, randomUUID());
            try
            {
                final VirtualHost<?> host = new TestMemoryVirtualHost(copy, _virtualHostNode);
                host.open();
                fail("Cannot create a virtual host without a mandatory attribute " + name);
            }
            catch(IllegalConfigurationException | IllegalArgumentException e)
            {
                // pass
            }
        }
    }
}
