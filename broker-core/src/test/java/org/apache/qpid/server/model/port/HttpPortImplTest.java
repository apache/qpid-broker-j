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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
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
import org.apache.qpid.server.model.BrokerImpl;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.JsonSystemConfigImpl;
import org.apache.qpid.server.model.KeyStore;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.test.utils.UnitTestBase;

@SuppressWarnings({"rawtypes", "unchecked"})
class HttpPortImplTest extends UnitTestBase
{
    private static final String AUTHENTICATION_PROVIDER_NAME = "test";

    private Broker _broker;

    @BeforeEach
    public void setUp()
    {
        final TaskExecutor taskExecutor = CurrentThreadTaskExecutor.newStartedInstance();
        final Model model = BrokerModel.getInstance();
        final SystemConfig systemConfig = mock(SystemConfig.class);
        when(systemConfig.getCategoryClass()).thenReturn(SystemConfig.class);
        when(systemConfig.getTypeClass()).thenReturn(JsonSystemConfigImpl.class);
        _broker = mock(Broker.class);
        when(_broker.getParent()).thenReturn(systemConfig);
        when(_broker.getTaskExecutor()).thenReturn(taskExecutor);
        when(_broker.getChildExecutor()).thenReturn(taskExecutor);
        when(_broker.getModel()).thenReturn(model);
        when(_broker.getEventLogger()).thenReturn(new EventLogger());
        when(_broker.getCategoryClass()).thenReturn(Broker.class);
        when(_broker.getTypeClass()).thenReturn(BrokerImpl.class);

        final AuthenticationProvider<?> provider = mock(AuthenticationProvider.class);
        when(provider.getName()).thenReturn(AUTHENTICATION_PROVIDER_NAME);
        when(provider.getParent()).thenReturn(_broker);
        when(provider.getMechanisms()).thenReturn(List.of("PLAIN"));
        when(_broker.getChildren(AuthenticationProvider.class)).thenReturn(Set.of(provider));
        when(_broker.getChildByName(AuthenticationProvider.class, AUTHENTICATION_PROVIDER_NAME)).thenReturn(provider);

    }

    @Test
    void testCreateWithIllegalThreadPoolValues()
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
    void testIllegalChangeWithMaxThreadPoolSizeSmallerThanMinThreadPoolSize()
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
    void testIllegalChangeWithNegativeThreadPoolSize()
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
    void testChangeWithLegalThreadPoolValues()
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

    @Test
    void deletePort()
    {
        final Map<String, Object> attributes = Map.of(HttpPort.PORT, 0,
                HttpPort.NAME, getTestName(),
                HttpPort.AUTHENTICATION_PROVIDER, AUTHENTICATION_PROVIDER_NAME,
                HttpPort.WANT_CLIENT_AUTH, true,
                HttpPort.NEED_CLIENT_AUTH, true,
                HttpPort.PROTOCOLS, List.of("HTTP"),
                HttpPort.TRANSPORTS, List.of("SSL"),
                HttpPort.KEY_STORE, mock(KeyStore.class),
                HttpPort.TRUST_STORES, List.of(mock(TrustStore.class)));

        final HttpPortImpl port = new HttpPortImpl(attributes, _broker);

        assertEquals(State.UNINITIALIZED, port.getState());

        port.create();

        assertEquals(State.QUIESCED, port.getState());

        port.delete();

        assertEquals(State.DELETED, port.getState());
    }

    /** Checks the deletion of an invalid port */
    @Test
    void deleteUninitializedInvalidPort()
    {
        final Map<String, Object> attributes = Map.of(HttpPort.PORT, 0,
                HttpPort.NAME, getTestName(),
                HttpPort.AUTHENTICATION_PROVIDER, AUTHENTICATION_PROVIDER_NAME,
                HttpPort.WANT_CLIENT_AUTH, true,
                HttpPort.NEED_CLIENT_AUTH, true,
                HttpPort.PROTOCOLS, List.of("HTTP"),
                HttpPort.TRANSPORTS, List.of("SSL"),
                HttpPort.KEY_STORE, mock(KeyStore.class),
                HttpPort.TRUST_STORES, List.of());

        final HttpPortImpl port = new HttpPortImpl(attributes, _broker);

        assertEquals(State.UNINITIALIZED, port.getState());

        port.delete();

        assertEquals(State.DELETED, port.getState());
    }

    /** Checks the update of an invalid port */
    @Test
    void updateUninitializedInvalidPort()
    {
        final Map<String, Object> attributes = Map.of(HttpPort.PORT, 0,
                HttpPort.NAME, getTestName(),
                HttpPort.AUTHENTICATION_PROVIDER, AUTHENTICATION_PROVIDER_NAME,
                HttpPort.WANT_CLIENT_AUTH, true,
                HttpPort.NEED_CLIENT_AUTH, true,
                HttpPort.PROTOCOLS, List.of("HTTP"),
                HttpPort.TRANSPORTS, List.of("SSL"),
                HttpPort.KEY_STORE, mock(KeyStore.class),
                HttpPort.TRUST_STORES, List.of());

        final HttpPortImpl port = new HttpPortImpl(attributes, _broker);

        assertEquals(State.UNINITIALIZED, port.getState());

        final Map<String, Object> args1 = Map.of(HttpPort.NAME, "changed");
        IllegalConfigurationException exception = assertThrows(IllegalConfigurationException.class, () -> port.setAttributes(args1));
        assertEquals("Changing the port name is not allowed", exception.getMessage());

        final Map<String, Object> args2 = Map.of(HttpPort.WANT_CLIENT_AUTH, false);
        exception = assertThrows(IllegalConfigurationException.class, () -> port.setAttributes(args2));
        assertEquals("Can't create port which requests SSL client certificates but has no trust store configured.",
                exception.getMessage());

        final Map<String, Object> args3 = Map.of(HttpPort.NEED_CLIENT_AUTH, false);
        exception = assertThrows(IllegalConfigurationException.class, () -> port.setAttributes(args3));
        assertEquals("Can't create port which requests SSL client certificates but has no trust store configured.",
                exception.getMessage());

        final Map<String, Object> args4 = Map.of(HttpPort.TRANSPORTS, List.of("TCP"));
        exception = assertThrows(IllegalConfigurationException.class, () -> port.setAttributes(args4));
        assertEquals("Can't create port which requests SSL client certificates but doesn't use SSL transport.",
                exception.getMessage());

        final Map<String, Object> args5 = new HashMap<>();
        args5.put(HttpPort.KEY_STORE, null);
        exception = assertThrows(IllegalConfigurationException.class, () -> port.setAttributes(args5));
        assertEquals("Can't create port which requires SSL but has no key store configured.",
                exception.getMessage());

        final Map<String, Object> args6 = Map.of(HttpPort.TRUST_STORES, List.of(mock(TrustStore.class)));
        assertDoesNotThrow(() -> port.setAttributes(args6));
    }
}
