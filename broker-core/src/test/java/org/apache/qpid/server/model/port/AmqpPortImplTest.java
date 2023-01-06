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
 */

package org.apache.qpid.server.model.port;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.security.Principal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.LogMessage;
import org.apache.qpid.server.logging.LogSubject;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.KeyStore;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.server.security.SiteSpecificTrustStore;
import org.apache.qpid.test.utils.UnitTestBase;

@SuppressWarnings({"rawtypes", "unchecked"})
public class AmqpPortImplTest extends UnitTestBase
{
    private static final String AUTHENTICATION_PROVIDER_NAME = "test";
    private static final String KEYSTORE_NAME = "keystore";
    private static final String TRUSTSTORE_NAME = "truststore";
    
    private TaskExecutor _taskExecutor;
    private Broker _broker;
    private AmqpPortImpl _port;

    @BeforeEach
    public void setUp() throws Exception
    {
        _taskExecutor = CurrentThreadTaskExecutor.newStartedInstance();
        final Model model = BrokerModel.getInstance();
        final SystemConfig systemConfig = mock(SystemConfig.class);
        _broker = BrokerTestHelper.mockWithSystemPrincipal(Broker.class, mock(Principal.class));
        when(_broker.getParent()).thenReturn(systemConfig);
        when(_broker.getTaskExecutor()).thenReturn(_taskExecutor);
        when(_broker.getChildExecutor()).thenReturn(_taskExecutor);
        when(_broker.getModel()).thenReturn(model);
        when(_broker.getId()).thenReturn(randomUUID());
        when(_broker.getCategoryClass()).thenReturn(Broker.class);
        when(_broker.getEventLogger()).thenReturn(new EventLogger());

        final KeyStore<?> keyStore = mock(KeyStore.class);
        when(keyStore.getName()).thenReturn(KEYSTORE_NAME);
        when(keyStore.getParent()).thenReturn(_broker);

        final TrustStore<?> trustStore = mock(TrustStore.class);
        when(trustStore.getName()).thenReturn(TRUSTSTORE_NAME);
        when(trustStore.getParent()).thenReturn(_broker);

        final AuthenticationProvider<?> authProvider = mock(AuthenticationProvider.class);
        when(authProvider.getName()).thenReturn(AUTHENTICATION_PROVIDER_NAME);
        when(authProvider.getParent()).thenReturn(_broker);
        when(authProvider.getMechanisms()).thenReturn(List.of("PLAIN"));

        when(_broker.getChildren(AuthenticationProvider.class)).thenReturn(Set.of(authProvider));
        when(_broker.getChildren(KeyStore.class)).thenReturn(Set.of(keyStore));
        when(_broker.getChildren(TrustStore.class)).thenReturn(Set.of(trustStore));
        when(_broker.getChildByName(AuthenticationProvider.class, AUTHENTICATION_PROVIDER_NAME)).thenReturn(authProvider);
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        try
        {
            _taskExecutor.stop();
        }
        finally
        {
            if (_port != null)
            {
                while (_port.getConnectionCount() >0)
                {
                    _port.decrementConnectionCount();
                }
                _port.close();
            }
        }
    }

    @Test
    public void testPortAlreadyBound() throws Exception
    {
        try (final ServerSocket socket = openSocket())
        {
            final IllegalConfigurationException thrown = assertThrows(IllegalConfigurationException.class,
                    () -> createPort(getTestName(), Map.of(AmqpPort.PORT, socket.getLocalPort())),
                    "Creation should fail due to validation check");
            assertEquals(String.format("Cannot bind to port %d and binding address '%s'. Port is already is use.",
                    socket.getLocalPort(), "*"), thrown.getMessage(), "Unexpected exception message");
        }
    }

    @Test
    public void testCreateTls()
    {
        final Map<String, Object> attributes = Map.of(AmqpPort.TRANSPORTS, List.of(Transport.SSL),
                AmqpPort.KEY_STORE, KEYSTORE_NAME);
        _port = createPort(getTestName(), attributes);
    }

    @Test
    public void testCreateTlsClientAuth()
    {
        final Map<String, Object> attributes = Map.of(AmqpPort.TRANSPORTS, List.of(Transport.SSL),
                AmqpPort.KEY_STORE, KEYSTORE_NAME,
                AmqpPort.TRUST_STORES, List.of(TRUSTSTORE_NAME));
        _port = createPort(getTestName(), attributes);
    }

    @Test
    public void testCreateTlsClientAuthUsingSiteTrustStore()
    {
        final String trustStoreName = "siteSpecificTrustStore";
        final SiteSpecificTrustStore<?> trustStore = mock(SiteSpecificTrustStore.class);
        when(trustStore.getName()).thenReturn(trustStoreName);
        when(trustStore.getParent()).thenReturn(_broker);
        when(_broker.getChildren(TrustStore.class)).thenReturn(Set.of(trustStore));

        final Map<String, Object> attributes = Map.of(AmqpPort.TRANSPORTS, List.of(Transport.SSL),
                AmqpPort.KEY_STORE, KEYSTORE_NAME,
                AmqpPort.TRUST_STORES, List.of(trustStoreName),
                AmqpPort.NEED_CLIENT_AUTH, "true");
        assertThrows(IllegalConfigurationException.class,
                () -> createPort(getTestName(), attributes),
                "Exception not thrown");
    }

    @Test
    public void testTlsWithoutKeyStore()
    {
        assertThrows(IllegalConfigurationException.class,
                () -> createPort(getTestName(), Map.of(Port.TRANSPORTS, List.of(Transport.SSL))),
                "Exception not thrown");
        assertThrows(IllegalConfigurationException.class,
                () -> createPort(getTestName(), Map.of(Port.TRANSPORTS, List.of(Transport.SSL, Transport.TCP))),
                "Exception not thrown");
    }

    @Test
    public void testTlsWantNeedWithoutTrustStores()
    {
        final Map<String, Object> base = Map.of(AmqpPort.TRANSPORTS, List.of(Transport.SSL),
                AmqpPort.KEY_STORE, KEYSTORE_NAME);

        final Map<String, Object> needClientAuthAttributes = new HashMap<>(base);
        needClientAuthAttributes.put(Port.NEED_CLIENT_AUTH, true);
        assertThrows(IllegalConfigurationException.class,
                () -> createPort(getTestName(), needClientAuthAttributes),
                "Exception not thrown");

        final Map<String, Object> wantClientAuthAttributes = new HashMap<>(base);
        wantClientAuthAttributes.put(Port.WANT_CLIENT_AUTH, true);
        assertThrows(IllegalConfigurationException.class,
                () -> createPort(getTestName(), wantClientAuthAttributes),
                "Exception not thrown");
    }

    @Test
    public void testOnCreateValidation()
    {
        assertThrows(IllegalConfigurationException.class,
                () -> createPort(getTestName(), Map.of(AmqpPort.NUMBER_OF_SELECTORS, "-1")),
                "Exception not thrown for negative number of selectors");
        assertThrows(IllegalConfigurationException.class,
                () -> createPort(getTestName(), Map.of(AmqpPort.THREAD_POOL_SIZE, "-1")),
                "Exception not thrown for negative thread pool size");
        assertThrows(IllegalConfigurationException.class,
                () -> createPort(getTestName(), Map.of(AmqpPort.NUMBER_OF_SELECTORS, AmqpPort.DEFAULT_PORT_AMQP_THREAD_POOL_SIZE)),
                "Exception not thrown for number of selectors equal to thread pool size");
    }

    @Test
    public void testOnChangeThreadPoolValidation()
    {
        _port = createPort(getTestName());
        assertThrows(IllegalConfigurationException.class,
                () -> _port.setAttributes(Map.of(AmqpPort.NUMBER_OF_SELECTORS, "-1")),
                "Exception not thrown for negative number of selectors");
        assertThrows(IllegalConfigurationException.class,
                () -> _port.setAttributes(Map.of(AmqpPort.THREAD_POOL_SIZE, "-1")),
                "Exception not thrown for negative thread pool size");
        assertThrows(IllegalConfigurationException.class,
                () -> _port.setAttributes(Map.of(AmqpPort.NUMBER_OF_SELECTORS, AmqpPort.DEFAULT_PORT_AMQP_THREAD_POOL_SIZE)),
                "Exception not thrown for number of selectors equal to thread pool size");
    }

    @Test
    public void testConnectionCounting()
    {
        final Map<String, Object> attributes = Map.of(AmqpPort.PORT, 0,
                AmqpPort.NAME, getTestName(),
                AmqpPort.AUTHENTICATION_PROVIDER, AUTHENTICATION_PROVIDER_NAME,
                AmqpPort.MAX_OPEN_CONNECTIONS, 10,
                AmqpPort.CONTEXT, Map.of(AmqpPort.OPEN_CONNECTIONS_WARN_PERCENT, "80"));
        _port = new AmqpPortImpl(attributes, _broker);
        _port.create();
        EventLogger mockLogger = mock(EventLogger.class);

        when(_broker.getEventLogger()).thenReturn(mockLogger);

        for (int i = 0; i < 8; i++)
        {
            assertTrue(_port.acceptNewConnectionAndIncrementCount(new InetSocketAddress("example.org", 0)));
            assertEquals(i + 1L, _port.getConnectionCount());
            verify(mockLogger, never()).message(any(LogSubject.class), any(LogMessage.class));
        }

        assertTrue(_port.acceptNewConnectionAndIncrementCount(new InetSocketAddress("example.org", 0)));
        assertEquals(9, _port.getConnectionCount());
        verify(mockLogger, times(1)).message(any(LogSubject.class), any(LogMessage.class));

        assertTrue(_port.acceptNewConnectionAndIncrementCount(new InetSocketAddress("example.org", 0)));
        assertEquals(10, _port.getConnectionCount());
        verify(mockLogger, times(1)).message(any(LogSubject.class), any(LogMessage.class));

        assertFalse(_port.acceptNewConnectionAndIncrementCount(new InetSocketAddress("example.org", 0)));
    }

    @Test
    public void resetStatistics()
    {
        final Map<String, Object> attributes = Map.of(AmqpPort.PORT, 0,
                AmqpPort.NAME, getTestName(),
                AmqpPort.AUTHENTICATION_PROVIDER, AUTHENTICATION_PROVIDER_NAME,
                AmqpPort.MAX_OPEN_CONNECTIONS, 10,
                AmqpPort.CONTEXT, Map.of(AmqpPort.OPEN_CONNECTIONS_WARN_PERCENT, "80"));
        _port = new AmqpPortImpl(attributes, _broker);
        _port.create();

        _port.acceptNewConnectionAndIncrementCount(new InetSocketAddress("example.org", 0));

        final Map<String, Object> statisticsBeforeReset = _port.getStatistics();
        assertEquals(1L, statisticsBeforeReset.get("totalConnectionCount"));

        _port.resetStatistics();

        final Map<String, Object> statisticsAfterReset = _port.getStatistics();
        assertEquals(0L, statisticsAfterReset.get("totalConnectionCount"));
    }

    private AmqpPortImpl createPort(final String portName)
    {
        return createPort(portName, Map.of());
    }

    private AmqpPortImpl createPort(final String portName, final Map<String, Object> attributes)
    {
        final Map<String, Object> portAttributes = new HashMap<>();
        portAttributes.put(AmqpPort.PORT, 0);
        portAttributes.put(AmqpPort.NAME, portName);
        portAttributes.put(AmqpPort.AUTHENTICATION_PROVIDER, AUTHENTICATION_PROVIDER_NAME);
        portAttributes.putAll(attributes);
        AmqpPortImpl port = new AmqpPortImpl(portAttributes, _broker);
        port.create();
        return port;
    }

    private ServerSocket openSocket() throws IOException
    {
        final ServerSocket serverSocket = new ServerSocket();
        serverSocket.setReuseAddress(true);
        serverSocket.bind(new InetSocketAddress(0));
        return serverSocket;
    }
}
