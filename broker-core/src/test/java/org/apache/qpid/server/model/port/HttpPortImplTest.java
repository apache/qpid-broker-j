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

package org.apache.qpid.server.model.port;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.test.utils.UnitTestBase;

@SuppressWarnings({"rawtypes", "unchecked"})
public class HttpPortImplTest extends UnitTestBase
{
    private static final String AUTHENTICATION_PROVIDER_NAME = "test";

    private Broker _broker;

    @BeforeEach
    public void setUp() throws Exception
    {
        final TaskExecutor taskExecutor = CurrentThreadTaskExecutor.newStartedInstance();
        final Model model = BrokerModel.getInstance();
        final SystemConfig systemConfig = mock(SystemConfig.class);
        _broker = mock(Broker.class);
        when(_broker.getParent()).thenReturn(systemConfig);
        when(_broker.getTaskExecutor()).thenReturn(taskExecutor);
        when(_broker.getChildExecutor()).thenReturn(taskExecutor);
        when(_broker.getModel()).thenReturn(model);
        when(_broker.getEventLogger()).thenReturn(new EventLogger());
        when(_broker.getCategoryClass()).thenReturn(Broker.class);

        final AuthenticationProvider<?> provider = mock(AuthenticationProvider.class);
        when(provider.getName()).thenReturn(AUTHENTICATION_PROVIDER_NAME);
        when(provider.getParent()).thenReturn(_broker);
        when(provider.getMechanisms()).thenReturn(List.of("PLAIN"));
        when(_broker.getChildren(AuthenticationProvider.class)).thenReturn(Set.of(provider));
        when(_broker.getChildByName(AuthenticationProvider.class, AUTHENTICATION_PROVIDER_NAME)).thenReturn(provider);

    }

    @Test
    public void testCreateWithIllegalThreadPoolValues()
    {
        final int threadPoolMinimumSize = 37;
        final int invalidThreadPoolMaximumSize = threadPoolMinimumSize - 1;
        final Map<String, Object> attributes = Map.of(HttpPort.PORT, 0,
                HttpPort.NAME, getTestName(),
                HttpPort.THREAD_POOL_MINIMUM, threadPoolMinimumSize,
                HttpPort.THREAD_POOL_MAXIMUM, invalidThreadPoolMaximumSize,
                HttpPort.AUTHENTICATION_PROVIDER, AUTHENTICATION_PROVIDER_NAME);
        final HttpPortImpl port = new HttpPortImpl(attributes, _broker);
        assertThrows(IllegalConfigurationException.class, port::create, "Creation should fail due to validation check");
    }

    @Test
    public void testIllegalChangeWithMaxThreadPoolSizeSmallerThanMinThreadPoolSize()
    {
        final int threadPoolMinimumSize = 37;
        final int invalidThreadPoolMaximumSize = threadPoolMinimumSize - 1;
        final Map<String, Object> attributes = Map.of(HttpPort.PORT, 0,
                HttpPort.NAME, getTestName(),
                HttpPort.AUTHENTICATION_PROVIDER, AUTHENTICATION_PROVIDER_NAME);
        final HttpPortImpl port = new HttpPortImpl(attributes, _broker);
        port.create();

        final Map<String, Object> updates = Map.of(HttpPort.THREAD_POOL_MINIMUM, threadPoolMinimumSize,
                HttpPort.THREAD_POOL_MAXIMUM, invalidThreadPoolMaximumSize);
        assertThrows(IllegalConfigurationException.class, () -> port.setAttributes(updates),
                "Change should fail due to validation check");
    }

    @Test
    public void testIllegalChangeWithNegativeThreadPoolSize()
    {
        final int illegalThreadPoolMinimumSize = -1;
        final int threadPoolMaximumSize = 1;
        final Map<String, Object> attributes = Map.of(HttpPort.PORT, 0,
                HttpPort.NAME, getTestName(),
                HttpPort.AUTHENTICATION_PROVIDER, AUTHENTICATION_PROVIDER_NAME);
        final HttpPortImpl port = new HttpPortImpl(attributes, _broker);
        port.create();

        final Map<String, Object> updates = Map.of(HttpPort.THREAD_POOL_MINIMUM, illegalThreadPoolMinimumSize,
                HttpPort.THREAD_POOL_MAXIMUM, threadPoolMaximumSize);
        assertThrows(IllegalConfigurationException.class, () -> port.setAttributes(updates),
                "Change should fail due to validation check");
    }

    @Test
    public void testChangeWithLegalThreadPoolValues()
    {
        final int threadPoolMinimumSize = 37;
        final int threadPoolMaximumSize = threadPoolMinimumSize + 1;
        final Map<String, Object> attributes = Map.of(HttpPort.PORT, 0,
                HttpPort.NAME, getTestName(),
                HttpPort.AUTHENTICATION_PROVIDER, AUTHENTICATION_PROVIDER_NAME);
        final HttpPortImpl port = new HttpPortImpl(attributes, _broker);
        port.create();

        final Map<String, Object> updates = Map.of(HttpPort.THREAD_POOL_MINIMUM, threadPoolMinimumSize,
                HttpPort.THREAD_POOL_MAXIMUM, threadPoolMaximumSize);
        port.setAttributes(updates);
        assertEquals(port.getThreadPoolMinimum(), (long) threadPoolMinimumSize,
                "Port did not pickup changes to minimum thread pool size");

        assertEquals(port.getThreadPoolMaximum(), (long) threadPoolMaximumSize,
                "Port did not pickup changes to maximum thread pool size");
    }
}
