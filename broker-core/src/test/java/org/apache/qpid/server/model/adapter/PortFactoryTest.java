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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.security.Principal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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

public class PortFactoryTest extends UnitTestBase
{
    private UUID _portId = UUID.randomUUID();
    private int _portNumber;
    private Set<String> _tcpStringSet = Collections.singleton(Transport.TCP.name());
    private Set<Transport> _tcpTransports = Collections.singleton(Transport.TCP);
    private Set<String> _sslStringSet = Collections.singleton(Transport.SSL.name());
    private Set<Transport> _sslTransports = Collections.singleton(Transport.SSL);

    private Map<String, Object> _attributes = new HashMap<String, Object>();

    private Broker _broker = BrokerTestHelper.mockWithSystemPrincipal(Broker.class, mock(Principal.class));
    private KeyStore _keyStore = mock(KeyStore.class);
    private TrustStore _trustStore = mock(TrustStore.class);
    private String _authProviderName = "authProvider";
    private AuthenticationProvider _authProvider = mock(AuthenticationProvider.class);
    private ConfiguredObjectFactoryImpl _factory;
    private Port<?> _port;


    @Before
    public void setUp() throws Exception
    {
        SystemConfig systemConfig = mock(SystemConfig.class);
        _portNumber = findFreePort();
        TaskExecutor executor = CurrentThreadTaskExecutor.newStartedInstance();
        when(_authProvider.getName()).thenReturn(_authProviderName);
        when(_broker.getChildren(eq(AuthenticationProvider.class))).thenReturn(Collections.singleton(_authProvider));
        when(_broker.getCategoryClass()).thenReturn(Broker.class);
        when(_broker.getEventLogger()).thenReturn(new EventLogger());
        when(_broker.getParent()).thenReturn(systemConfig);
        when(_broker.getTypeClass()).thenReturn(Broker.class);

        ConfiguredObjectFactory objectFactory = new ConfiguredObjectFactoryImpl(BrokerModel.getInstance());
        when(_broker.getObjectFactory()).thenReturn(objectFactory);
        when(_broker.getModel()).thenReturn(objectFactory.getModel());
        when(_authProvider.getModel()).thenReturn(objectFactory.getModel());
        when(_authProvider.getObjectFactory()).thenReturn(objectFactory);
        when(_authProvider.getCategoryClass()).thenReturn(AuthenticationProvider.class);
        when(_authProvider.getMechanisms()).thenReturn(Arrays.asList("PLAIN"));


        when(_keyStore.getModel()).thenReturn(objectFactory.getModel());
        when(_keyStore.getObjectFactory()).thenReturn(objectFactory);
        when(_trustStore.getModel()).thenReturn(objectFactory.getModel());
        when(_trustStore.getObjectFactory()).thenReturn(objectFactory);

        for(ConfiguredObject obj : new ConfiguredObject[]{_authProvider, _broker, _keyStore, _trustStore})
        {
            when(obj.getTaskExecutor()).thenReturn(executor);
            when(obj.getChildExecutor()).thenReturn(executor);
        }


        _factory = new ConfiguredObjectFactoryImpl(BrokerModel.getInstance());
        _attributes.put(Port.ID, _portId);
        _attributes.put(Port.NAME, getTestName());
        _attributes.put(Port.PORT, _portNumber);
        _attributes.put(Port.TRANSPORTS, _tcpStringSet);
        _attributes.put(Port.AUTHENTICATION_PROVIDER, _authProviderName);
        _attributes.put(Port.TCP_NO_DELAY, "true");
        _attributes.put(Port.BINDING_ADDRESS, "127.0.0.1");
    }

    @After
    public void tearDown() throws Exception
    {
        try
        {
            if (_port != null)
            {
                _port.close();
            }
        }
        finally
        {
        }
    }

    @Test
    public void testCreatePortWithMinimumAttributes()
    {
        Map<String, Object> attributes = new HashMap<String, Object>();
        attributes.put(Port.PORT, _portNumber);
        attributes.put(Port.NAME, getTestName());
        attributes.put(Port.AUTHENTICATION_PROVIDER, _authProviderName);
        attributes.put(Port.DESIRED_STATE, State.QUIESCED);

        _port = _factory.create(Port.class, attributes, _broker);

        assertNotNull(_port);
        final boolean condition = _port instanceof AmqpPort;
        assertTrue(condition);
        assertEquals("Unexpected _port", (long) _portNumber, (long) _port.getPort());
        assertEquals("Unexpected transports",
                            Collections.singleton(PortFactory.DEFAULT_TRANSPORT),
                            _port.getTransports());

        assertEquals("Unexpected need client auth",
                            PortFactory.DEFAULT_AMQP_NEED_CLIENT_AUTH,
                            _port.getAttribute(Port.NEED_CLIENT_AUTH));
        assertEquals("Unexpected want client auth",
                            PortFactory.DEFAULT_AMQP_WANT_CLIENT_AUTH,
                            _port.getAttribute(Port.WANT_CLIENT_AUTH));
        assertEquals("Unexpected tcp no delay",
                            PortFactory.DEFAULT_AMQP_TCP_NO_DELAY,
                            _port.getAttribute(Port.TCP_NO_DELAY));
        assertEquals("Unexpected binding",
                            PortFactory.DEFAULT_AMQP_BINDING,
                            _port.getAttribute(Port.BINDING_ADDRESS));
    }

    @Test
    public void testCreateAmqpPort()
    {
        createAmqpPortTestImpl(false, false, false, null, null);
    }

    @Test
    public void testCreateAmqpPortUsingSslFailsWithoutKeyStore()
    {
        try
        {
            createAmqpPortTestImpl(true, false, false, null, null);
            fail("expected exception due to lack of SSL keystore");
        }
        catch(IllegalConfigurationException e)
        {
            //expected
        }
    }

    @Test
    public void testCreateAmqpPortUsingSslSucceedsWithKeyStore()
    {
        String keyStoreName = "myKeyStore";
        when(_keyStore.getName()).thenReturn(keyStoreName);
        when(_broker.getChildren(eq(KeyStore.class))).thenReturn(Collections.singletonList(_keyStore));

        createAmqpPortTestImpl(true, false, false, keyStoreName, null);
    }

    @Test
    public void testCreateAmqpPortNeedingClientAuthFailsWithoutTrustStore()
    {
        String keyStoreName = "myKeyStore";
        when(_keyStore.getName()).thenReturn(keyStoreName);
        when(_broker.getChildren(eq(KeyStore.class))).thenReturn(Collections.singletonList(_keyStore));
        when(_broker.getChildren(eq(TrustStore.class))).thenReturn(Collections.emptyList());
        try
        {
            createAmqpPortTestImpl(true, true, false, keyStoreName, null);
            fail("expected exception due to lack of SSL truststore");
        }
        catch(IllegalConfigurationException e)
        {
            //expected
        }
    }

    @Test
    public void testCreateAmqpPortNeedingClientAuthSucceedsWithTrustStore()
    {
        String keyStoreName = "myKeyStore";
        when(_keyStore.getName()).thenReturn(keyStoreName);
        when(_broker.getChildren(eq(KeyStore.class))).thenReturn(Collections.singletonList(_keyStore));

        String trustStoreName = "myTrustStore";
        when(_trustStore.getName()).thenReturn(trustStoreName);
        when(_broker.getChildren(eq(TrustStore.class))).thenReturn(Collections.singletonList(_trustStore));

        createAmqpPortTestImpl(true, true, false, keyStoreName, new String[]{trustStoreName});
    }

    @Test
    public void testCreateAmqpPortWantingClientAuthFailsWithoutTrustStore()
    {
        String keyStoreName = "myKeyStore";
        when(_keyStore.getName()).thenReturn(keyStoreName);
        when(_broker.getChildren(eq(KeyStore.class))).thenReturn(Collections.singletonList(_keyStore));

        try
        {
            createAmqpPortTestImpl(true, false, true, keyStoreName, null);
            fail("expected exception due to lack of SSL truststore");
        }
        catch(IllegalConfigurationException e)
        {
            //expected
        }
    }

    @Test
    public void testCreateAmqpPortWantingClientAuthSucceedsWithTrustStore()
    {
        String keyStoreName = "myKeyStore";
        when(_keyStore.getName()).thenReturn(keyStoreName);
        when(_broker.getChildren(eq(KeyStore.class))).thenReturn(Collections.singletonList(_keyStore));

        String trustStoreName = "myTrustStore";
        when(_trustStore.getName()).thenReturn(trustStoreName);
        when(_broker.getChildren(eq(TrustStore.class))).thenReturn(Collections.singletonList(_trustStore));

        createAmqpPortTestImpl(true, false, true, keyStoreName, new String[]{trustStoreName});
    }

    public void createAmqpPortTestImpl(boolean useSslTransport, boolean needClientAuth, boolean wantClientAuth,
                                       String keystoreName, String[] trustStoreNames)
    {
        Set<Protocol> amqp010ProtocolSet = Collections.singleton(Protocol.AMQP_0_10);
        Set<String> amqp010StringSet = Collections.singleton(Protocol.AMQP_0_10.name());
        _attributes.put(Port.PROTOCOLS, amqp010StringSet);

        if(useSslTransport)
        {
            _attributes.put(Port.TRANSPORTS, _sslStringSet);
        }

        if(needClientAuth)
        {
            _attributes.put(Port.NEED_CLIENT_AUTH, "true");
        }

        if(wantClientAuth)
        {
            _attributes.put(Port.WANT_CLIENT_AUTH, "true");
        }

        if(keystoreName != null)
        {
            _attributes.put(Port.KEY_STORE, keystoreName);
        }

        if(trustStoreNames != null)
        {
            _attributes.put(Port.TRUST_STORES, Arrays.asList(trustStoreNames));
        }

        _attributes.put(Port.DESIRED_STATE, State.QUIESCED);

        _port = _factory.create(Port.class, _attributes, _broker);

        assertNotNull(_port);
        final boolean condition = _port instanceof AmqpPort;
        assertTrue(condition);
        assertEquals(_portId, _port.getId());
        assertEquals((long) _portNumber, (long) _port.getPort());
        if(useSslTransport)
        {
            assertEquals(_sslTransports, _port.getTransports());
        }
        else
        {
            assertEquals(_tcpTransports, _port.getTransports());
        }
        assertEquals(amqp010ProtocolSet, _port.getProtocols());
        assertEquals("Unexpected need client auth", needClientAuth, _port.getAttribute(Port.NEED_CLIENT_AUTH));
        assertEquals("Unexpected want client auth", wantClientAuth, _port.getAttribute(Port.WANT_CLIENT_AUTH));
        assertEquals("Unexpected tcp no delay", true, _port.getAttribute(Port.TCP_NO_DELAY));
        assertEquals("Unexpected binding", "127.0.0.1", _port.getAttribute(Port.BINDING_ADDRESS));
    }

    @Test
    public void testCreateHttpPort()
    {
        Set<Protocol> httpProtocolSet = Collections.singleton(Protocol.HTTP);
        Set<String> httpStringSet = Collections.singleton(Protocol.HTTP.name());
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
        assertFalse("Port should not be an AMQP-specific subclass", condition);
        assertEquals(_portId, _port.getId());
        assertEquals((long) _portNumber, (long) _port.getPort());
        assertEquals(_tcpTransports, _port.getTransports());
        assertEquals(httpProtocolSet, _port.getProtocols());
    }

    @Test
    public void testCreateHttpPortWithPartiallySetAttributes()
    {
        Set<Protocol> httpProtocolSet = Collections.singleton(Protocol.HTTP);
        Set<String> httpStringSet = Collections.singleton(Protocol.HTTP.name());
        _attributes = new HashMap<>();
        _attributes.put(Port.PROTOCOLS, httpStringSet);
        _attributes.put(Port.AUTHENTICATION_PROVIDER, _authProviderName);
        _attributes.put(Port.PORT, _portNumber);
        _attributes.put(Port.NAME, getTestName());
        _attributes.put(Port.ID, _portId);

        _port = _factory.create(Port.class, _attributes, _broker);

        assertNotNull(_port);
        final boolean condition = _port instanceof AmqpPort;
        assertFalse("Port not be an AMQP-specific _port subclass", condition);
        assertEquals(_portId, _port.getId());
        assertEquals((long) _portNumber, (long) _port.getPort());
        assertEquals(Collections.singleton(PortFactory.DEFAULT_TRANSPORT), _port.getTransports());
        assertEquals(httpProtocolSet, _port.getProtocols());
    }

    @Test
    public void testCreateMixedAmqpAndNonAmqpThrowsException()
    {
        Set<String> mixedProtocolSet = new HashSet<>(Arrays.asList(Protocol.AMQP_0_10.name(), Protocol.HTTP.name()));
        _attributes.put(Port.PROTOCOLS, mixedProtocolSet);

        try
        {
            _port = _factory.create(Port.class, _attributes, _broker);
            fail("Exception not thrown");
        }
        catch (IllegalConfigurationException e)
        {
            // pass
        }
    }

    @Test
    public void testCreatePortWithoutAuthenticationMechanism()
    {
        when(_authProvider.getDisabledMechanisms()).thenReturn(Arrays.asList("PLAIN"));
        try
        {
            createAmqpPortTestImpl(false, false, false, null, null);
            fail("Port creation should fail due to no authentication mechanism being available.");
        }
        catch(IllegalConfigurationException e)
        {
            // pass
        }
        when(_authProvider.getDisabledMechanisms()).thenReturn(Collections.emptyList());
    }
}
