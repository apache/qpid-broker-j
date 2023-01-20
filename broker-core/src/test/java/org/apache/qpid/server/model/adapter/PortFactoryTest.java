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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.security.Principal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObjectFactory;
import org.apache.qpid.server.model.ConfiguredObjectFactoryImpl;
import org.apache.qpid.server.model.KeyStore;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.model.port.PortFactory;
import org.apache.qpid.test.utils.UnitTestBase;

@SuppressWarnings({"rawtypes", "unchecked"})
public class PortFactoryTest extends UnitTestBase
{
    private final UUID _portId = randomUUID();
    private final Set<String> _tcpStringSet = Set.of(Transport.TCP.name());
    private final Set<Transport> _tcpTransports = Set.of(Transport.TCP);
    private final Set<String> _sslStringSet = Set.of(Transport.SSL.name());
    private final Set<Transport> _sslTransports = Set.of(Transport.SSL);
    private final Broker _broker = BrokerTestHelper.mockWithSystemPrincipal(Broker.class, mock(Principal.class));
    private final KeyStore<?> _keyStore = mock(KeyStore.class);
    private final TrustStore<?> _trustStore = mock(TrustStore.class);
    private final String _authProviderName = "authProvider";
    private final AuthenticationProvider _authProvider = mock(AuthenticationProvider.class);

    private Map<String, Object> _attributes = new HashMap<>();
    private int _portNumber;
    private ConfiguredObjectFactoryImpl _factory;
    private Port<?> _port;

    @BeforeEach
    public void setUp() throws Exception
    {
        final SystemConfig systemConfig = mock(SystemConfig.class);
        _portNumber = findFreePort();
        final TaskExecutor executor = CurrentThreadTaskExecutor.newStartedInstance();
        when(_authProvider.getName()).thenReturn(_authProviderName);
        when(_broker.getChildren(eq(AuthenticationProvider.class))).thenReturn(Set.of(_authProvider));
        when(_broker.getCategoryClass()).thenReturn(Broker.class);
        when(_broker.getEventLogger()).thenReturn(new EventLogger());
        when(_broker.getParent()).thenReturn(systemConfig);
        when(_broker.getTypeClass()).thenReturn(Broker.class);

        final ConfiguredObjectFactory objectFactory = new ConfiguredObjectFactoryImpl(BrokerModel.getInstance());
        when(_broker.getObjectFactory()).thenReturn(objectFactory);
        when(_broker.getModel()).thenReturn(objectFactory.getModel());
        when(_authProvider.getModel()).thenReturn(objectFactory.getModel());
        when(_authProvider.getObjectFactory()).thenReturn(objectFactory);
        when(_authProvider.getCategoryClass()).thenReturn(AuthenticationProvider.class);
        when(_authProvider.getMechanisms()).thenReturn(List.of("PLAIN"));

        when(_keyStore.getModel()).thenReturn(objectFactory.getModel());
        when(_keyStore.getObjectFactory()).thenReturn(objectFactory);
        when(_trustStore.getModel()).thenReturn(objectFactory.getModel());
        when(_trustStore.getObjectFactory()).thenReturn(objectFactory);

        for (final ConfiguredObject obj : List.of(_authProvider, _broker, _keyStore, _trustStore))
        {
            when(obj.getTaskExecutor()).thenReturn(executor);
            when(obj.getChildExecutor()).thenReturn(executor);
        }

        _factory = new ConfiguredObjectFactoryImpl(BrokerModel.getInstance());

        _attributes.clear();
        _attributes.put(Port.ID, _portId);
        _attributes.put(Port.NAME, getTestName());
        _attributes.put(Port.PORT, _portNumber);
        _attributes.put(Port.TRANSPORTS, _tcpStringSet);
        _attributes.put(Port.AUTHENTICATION_PROVIDER, _authProviderName);
        _attributes.put(Port.TCP_NO_DELAY, "true");
        _attributes.put(Port.BINDING_ADDRESS, "127.0.0.1");
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        if (_port != null)
        {
            _port.close();
        }
    }

    @Test
    public void testCreatePortWithMinimumAttributes()
    {
        final Map<String, Object> attributes = Map.of(Port.PORT, _portNumber,
                Port.NAME, getTestName(),
                Port.AUTHENTICATION_PROVIDER, _authProviderName,
                Port.DESIRED_STATE, State.QUIESCED);

        _port = _factory.create(Port.class, attributes, _broker);

        assertNotNull(_port);
        final boolean condition = _port instanceof AmqpPort;
        assertTrue(condition);
        assertEquals(_portNumber, (long) _port.getPort(), "Unexpected _port");
        assertEquals(Set.of(PortFactory.DEFAULT_TRANSPORT),_port.getTransports(), "Unexpected transports");

        assertEquals(PortFactory.DEFAULT_AMQP_NEED_CLIENT_AUTH, _port.getAttribute(Port.NEED_CLIENT_AUTH),
                "Unexpected need client auth");
        assertEquals(PortFactory.DEFAULT_AMQP_WANT_CLIENT_AUTH, _port.getAttribute(Port.WANT_CLIENT_AUTH),
                "Unexpected want client auth");
        assertEquals(PortFactory.DEFAULT_AMQP_TCP_NO_DELAY, _port.getAttribute(Port.TCP_NO_DELAY),
                "Unexpected tcp no delay");
        assertEquals(PortFactory.DEFAULT_AMQP_BINDING, _port.getAttribute(Port.BINDING_ADDRESS),
                "Unexpected binding");
    }

    @Test
    public void testCreateAmqpPort()
    {
        createAmqpPortTestImpl(false, false, false, null, null);
    }

    @Test
    public void testCreateAmqpPortUsingSslFailsWithoutKeyStore()
    {
        assertThrows(IllegalConfigurationException.class,
                () -> createAmqpPortTestImpl(true, false, false, null, null),
                "Expected exception due to lack of SSL keystore");
    }

    @Test
    public void testCreateAmqpPortUsingSslSucceedsWithKeyStore()
    {
        final String keyStoreName = "myKeyStore";
        when(_keyStore.getName()).thenReturn(keyStoreName);
        when(_broker.getChildren(eq(KeyStore.class))).thenReturn(List.of(_keyStore));
        createAmqpPortTestImpl(true, false, false, keyStoreName, null);
    }

    @Test
    public void testCreateAmqpPortNeedingClientAuthFailsWithoutTrustStore()
    {
        final String keyStoreName = "myKeyStore";
        when(_keyStore.getName()).thenReturn(keyStoreName);
        when(_broker.getChildren(eq(KeyStore.class))).thenReturn(List.of(_keyStore));
        when(_broker.getChildren(eq(TrustStore.class))).thenReturn(List.of());
        assertThrows(IllegalConfigurationException.class,
                () -> createAmqpPortTestImpl(true, true, false, keyStoreName, null),
                "Expected exception due to lack of SSL truststore");
    }

    @Test
    public void testCreateAmqpPortNeedingClientAuthSucceedsWithTrustStore()
    {
        final String keyStoreName = "myKeyStore";
        when(_keyStore.getName()).thenReturn(keyStoreName);
        when(_broker.getChildren(eq(KeyStore.class))).thenReturn(List.of(_keyStore));

        final String trustStoreName = "myTrustStore";
        when(_trustStore.getName()).thenReturn(trustStoreName);
        when(_broker.getChildren(eq(TrustStore.class))).thenReturn(List.of(_trustStore));

        createAmqpPortTestImpl(true, true, false, keyStoreName, new String[]{trustStoreName});
    }

    @Test
    public void testCreateAmqpPortWantingClientAuthFailsWithoutTrustStore()
    {
        final String keyStoreName = "myKeyStore";
        when(_keyStore.getName()).thenReturn(keyStoreName);
        when(_broker.getChildren(eq(KeyStore.class))).thenReturn(List.of(_keyStore));
        assertThrows(IllegalConfigurationException.class,
                () -> createAmqpPortTestImpl(true, false, true, keyStoreName, null),
                "Expected exception due to lack of SSL truststore");
    }

    @Test
    public void testCreateAmqpPortWantingClientAuthSucceedsWithTrustStore()
    {
        final String keyStoreName = "myKeyStore";
        when(_keyStore.getName()).thenReturn(keyStoreName);
        when(_broker.getChildren(eq(KeyStore.class))).thenReturn(List.of(_keyStore));

        final String trustStoreName = "myTrustStore";
        when(_trustStore.getName()).thenReturn(trustStoreName);
        when(_broker.getChildren(eq(TrustStore.class))).thenReturn(List.of(_trustStore));

        createAmqpPortTestImpl(true, false, true, keyStoreName, new String[]{trustStoreName});
    }

    public void createAmqpPortTestImpl(final boolean useSslTransport,
                                       final boolean needClientAuth,
                                       final boolean wantClientAuth,
                                       final String keystoreName,
                                       final String[] trustStoreNames)
    {
        final Set<Protocol> amqp010ProtocolSet = Set.of(Protocol.AMQP_0_10);
        final Set<String> amqp010StringSet = Set.of(Protocol.AMQP_0_10.name());
        _attributes.put(Port.PROTOCOLS, amqp010StringSet);

        if (useSslTransport)
        {
            _attributes.put(Port.TRANSPORTS, _sslStringSet);
        }

        if (needClientAuth)
        {
            _attributes.put(Port.NEED_CLIENT_AUTH, "true");
        }

        if (wantClientAuth)
        {
            _attributes.put(Port.WANT_CLIENT_AUTH, "true");
        }

        if (keystoreName != null)
        {
            _attributes.put(Port.KEY_STORE, keystoreName);
        }

        if (trustStoreNames != null)
        {
            _attributes.put(Port.TRUST_STORES, Arrays.asList(trustStoreNames));
        }

        _attributes.put(Port.DESIRED_STATE, State.QUIESCED);

        _port = _factory.create(Port.class, _attributes, _broker);

        assertNotNull(_port);
        final boolean condition = _port instanceof AmqpPort;
        assertTrue(condition);
        assertEquals(_portId, _port.getId());
        assertEquals(_portNumber, (long) _port.getPort());
        if (useSslTransport)
        {
            assertEquals(_sslTransports, _port.getTransports());
        }
        else
        {
            assertEquals(_tcpTransports, _port.getTransports());
        }
        assertEquals(amqp010ProtocolSet, _port.getProtocols());
        assertEquals(needClientAuth, _port.getAttribute(Port.NEED_CLIENT_AUTH), "Unexpected need client auth");
        assertEquals(wantClientAuth, _port.getAttribute(Port.WANT_CLIENT_AUTH), "Unexpected want client auth");
        assertEquals(true, _port.getAttribute(Port.TCP_NO_DELAY), "Unexpected tcp no delay");
        assertEquals("127.0.0.1", _port.getAttribute(Port.BINDING_ADDRESS), "Unexpected binding");
    }

    @Test
    public void testCreateHttpPort()
    {
        final Set<Protocol> httpProtocolSet = Set.of(Protocol.HTTP);
        final Set<String> httpStringSet = Set.of(Protocol.HTTP.name());
        _attributes = new HashMap<>();
        _attributes.put(Port.PROTOCOLS, httpStringSet);
        _attributes.put(Port.AUTHENTICATION_PROVIDER, _authProviderName);
        _attributes.put(Port.PORT, _portNumber);
        _attributes.put(Port.TRANSPORTS, _tcpStringSet);
        _attributes.put(Port.NAME, getTestName());
        _attributes.put(Port.ID, _portId);

        _port = _factory.create(Port.class, _attributes, _broker);

        assertNotNull(_port);
        final boolean condition = _port instanceof AmqpPort;
        assertFalse(condition, "Port should not be an AMQP-specific subclass");
        assertEquals(_portId, _port.getId());
        assertEquals(_portNumber, (long) _port.getPort());
        assertEquals(_tcpTransports, _port.getTransports());
        assertEquals(httpProtocolSet, _port.getProtocols());
    }

    @Test
    public void testCreateHttpPortWithPartiallySetAttributes()
    {
        final Set<Protocol> httpProtocolSet = Set.of(Protocol.HTTP);
        final Set<String> httpStringSet = Set.of(Protocol.HTTP.name());
        _attributes = new HashMap<>();
        _attributes.put(Port.PROTOCOLS, httpStringSet);
        _attributes.put(Port.AUTHENTICATION_PROVIDER, _authProviderName);
        _attributes.put(Port.PORT, _portNumber);
        _attributes.put(Port.NAME, getTestName());
        _attributes.put(Port.ID, _portId);

        _port = _factory.create(Port.class, _attributes, _broker);

        assertNotNull(_port);
        final boolean condition = _port instanceof AmqpPort;
        assertFalse(condition, "Port not be an AMQP-specific _port subclass");
        assertEquals(_portId, _port.getId());
        assertEquals(_portNumber, (long) _port.getPort());
        assertEquals(Set.of(PortFactory.DEFAULT_TRANSPORT), _port.getTransports());
        assertEquals(httpProtocolSet, _port.getProtocols());
    }

    @Test
    public void testCreateMixedAmqpAndNonAmqpThrowsException()
    {
        final Set<String> mixedProtocolSet = Set.of(Protocol.AMQP_0_10.name(), Protocol.HTTP.name());
        _attributes.put(Port.PROTOCOLS, mixedProtocolSet);
        assertThrows(IllegalConfigurationException.class,
                () -> _port = _factory.create(Port.class, _attributes, _broker),
                "Exception not thrown");
    }

    @Test
    public void testCreatePortWithoutAuthenticationMechanism()
    {
        when(_authProvider.getDisabledMechanisms()).thenReturn(List.of("PLAIN"));
        assertThrows(IllegalConfigurationException.class,
                () -> createAmqpPortTestImpl(false, false, false, null, null),
                "Port creation should fail due to no authentication mechanism being available");
        when(_authProvider.getDisabledMechanisms()).thenReturn(List.of());
    }
}
