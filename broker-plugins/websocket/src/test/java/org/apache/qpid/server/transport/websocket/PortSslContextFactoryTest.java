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
package org.apache.qpid.server.transport.websocket;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.test.utils.UnitTestBase;

class PortSslContextFactoryTest extends UnitTestBase
{
    private AmqpPort<?> _port;
    private PortSslContextFactory _factory;

    @BeforeEach
    void beforeEach() throws Exception
    {
        _port = mock(AmqpPort.class);
        when(_port.getSSLContext()).thenReturn(createSslContext());
    }

    @AfterEach
    void afterEach() throws Exception
    {
        if (_factory != null)
        {
            _factory.stop();
        }
    }

    @ParameterizedTest
    @CsvSource({"false, false", "true, false", "false, true", "true, true"})
    void clientAuthentication(final boolean needClientAuth, final boolean wantClientAuth) throws Exception
    {
        when(_port.getNeedClientAuth()).thenReturn(needClientAuth);
        when(_port.getWantClientAuth()).thenReturn(wantClientAuth);
        startFactory();

        final SSLEngine engine = _factory.newSSLEngine();

        assertFalse(engine.getUseClientMode());
        assertEquals(needClientAuth, engine.getNeedClientAuth());
        assertEquals(wantClientAuth && !needClientAuth, engine.getWantClientAuth());
    }

    @Test
    void reloadUpdatesContextAndClientAuthentication() throws Exception
    {
        when(_port.getNeedClientAuth()).thenReturn(true);
        startFactory();
        final SSLEngine existingEngine = _factory.newSSLEngine();
        assertTrue(existingEngine.getNeedClientAuth());

        final SSLContext replacement = createSslContext();
        when(_port.getSSLContext()).thenReturn(replacement);
        when(_port.getNeedClientAuth()).thenReturn(false);
        when(_port.getWantClientAuth()).thenReturn(true);

        _factory.reloadFromPort();

        final SSLEngine newEngine = _factory.newSSLEngine();
        assertSame(replacement, _factory.getSslContext());
        assertFalse(newEngine.getNeedClientAuth());
        assertTrue(newEngine.getWantClientAuth());
        assertTrue(existingEngine.getNeedClientAuth());
        assertFalse(existingEngine.getWantClientAuth());
    }

    @Test
    void cipherPolicyUsesCurrentPortSettingsAndPreservesOrder() throws Exception
    {
        final String firstCipher = "TLS_AES_256_GCM_SHA384";
        final String secondCipher = "TLS_AES_128_GCM_SHA256";
        when(_port.getTlsCipherSuiteAllowList()).thenReturn(List.of(firstCipher, secondCipher));
        startFactory();

        final SSLEngine existingEngine = _factory.newSSLEngine();
        assertArrayEquals(new String[] {firstCipher, secondCipher}, existingEngine.getEnabledCipherSuites());
        assertTrue(existingEngine.getSSLParameters().getUseCipherSuitesOrder());

        when(_port.getTlsCipherSuiteDenyList()).thenReturn(List.of(firstCipher));

        final SSLEngine newEngine = _factory.newSSLEngine();
        assertArrayEquals(new String[] {secondCipher}, newEngine.getEnabledCipherSuites());
        assertArrayEquals(new String[] {firstCipher, secondCipher}, existingEngine.getEnabledCipherSuites());
    }

    @Test
    void protocolPolicyUsesCurrentPortSettings() throws Exception
    {
        when(_port.getTlsProtocolAllowList()).thenReturn(List.of("TLSv1.3", "TLSv1.2"));
        when(_port.getTlsProtocolDenyList()).thenReturn(List.of("TLSv1.3"));
        startFactory();

        final SSLEngine existingEngine = _factory.newSSLEngine();
        assertArrayEquals(new String[] {"TLSv1.2"}, existingEngine.getEnabledProtocols());

        when(_port.getTlsProtocolDenyList()).thenReturn(List.of("TLSv1.2"));

        assertArrayEquals(new String[] {"TLSv1.3"}, _factory.newSSLEngine().getEnabledProtocols());
        assertArrayEquals(new String[] {"TLSv1.2"}, existingEngine.getEnabledProtocols());
    }

    private void startFactory() throws Exception
    {
        _factory = new PortSslContextFactory(_port);
        _factory.start();
    }

    private SSLContext createSslContext() throws Exception
    {
        final SSLContext context = SSLContext.getInstance("TLS");
        context.init(null, null, null);
        return context;
    }
}
