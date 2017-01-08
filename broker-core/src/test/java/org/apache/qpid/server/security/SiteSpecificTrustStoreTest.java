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
package org.apache.qpid.server.security;


import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocketFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.ConfiguredObjectFactory;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.test.utils.QpidTestCase;
import org.apache.qpid.test.utils.TestSSLConstants;

public class SiteSpecificTrustStoreTest extends QpidTestCase
{
    private static final String EXPECTED_SUBJECT = "CN=localhost, OU=Unknown, O=Unknown, L=Unknown, ST=Unknown, C=Unknown";
    private static final String EXPECTED_ISSUER = "CN=MyRootCA, O=ACME, ST=Ontario, C=CA";
    private final Broker<?> _broker = mock(Broker.class);
    private final TaskExecutor _taskExecutor = CurrentThreadTaskExecutor.newStartedInstance();
    private final Model _model = BrokerModel.getInstance();
    private final ConfiguredObjectFactory _factory = _model.getObjectFactory();
    private TestPeer _testPeer;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        setTestSystemProperty(SiteSpecificTrustStore.TRUST_STORE_SITE_SPECIFIC_CONNECT_TIMEOUT, "100");
        setTestSystemProperty(SiteSpecificTrustStore.TRUST_STORE_SITE_SPECIFIC_READ_TIMEOUT, "100");

        when(_broker.getTaskExecutor()).thenReturn(_taskExecutor);
        when(_broker.getChildExecutor()).thenReturn(_taskExecutor);
        when(_broker.getModel()).thenReturn(_model);
        when(_broker.getEventLogger()).thenReturn(new EventLogger());
        when(((Broker) _broker).getCategoryClass()).thenReturn(Broker.class);
    }

    @Override
    public void tearDown() throws Exception
    {
        try
        {
            super.tearDown();
        }
        finally
        {
            if (_testPeer != null)
            {
                _testPeer.close();
            }
        }
    }

    public void testMalformedSiteUrl() throws Exception
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(SiteSpecificTrustStore.NAME, "mySiteSpecificTrustStore");
        attributes.put(SiteSpecificTrustStore.TYPE, "SiteSpecificTrustStore");
        attributes.put("siteUrl", "notaurl:541");

        try
        {
            _factory.create(TrustStore.class, attributes, _broker);
            fail("Exception not thrown");
        }
        catch (IllegalConfigurationException e)
        {
            // PASS
        }
    }

    public void testSiteUrlDoesNotSupplyHostPort() throws Exception
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(SiteSpecificTrustStore.NAME, "mySiteSpecificTrustStore");
        attributes.put(SiteSpecificTrustStore.TYPE, "SiteSpecificTrustStore");
        attributes.put("siteUrl", "file:/not/a/host");

        try
        {
            _factory.create(TrustStore.class, attributes, _broker);
            fail("Exception not thrown");
        }
        catch (IllegalConfigurationException e)
        {
            // PASS
        }
    }

    public void testUnresponsiveSite() throws Exception
    {
        _testPeer = new TestPeer();
        _testPeer.setAccept(false);
        int listeningPort = _testPeer.start();

        Map<String, Object> attributes = getTrustStoreAttributes(listeningPort);

        try
        {
            _factory.create(TrustStore.class, attributes, _broker);
            fail("Exception not thrown");
        }
        catch (IllegalConfigurationException e)
        {
            // PASS
        }
    }

    public void testValidSiteUrl() throws Exception
    {
        _testPeer = new TestPeer();
        int listeningPort = _testPeer.start();

        Map<String, Object> attributes = getTrustStoreAttributes(listeningPort);

        final SiteSpecificTrustStore trustStore =
                (SiteSpecificTrustStore) _factory.create(TrustStore.class, attributes, _broker);

        assertEquals("Unexpected certificate subject", EXPECTED_SUBJECT, trustStore.getCertificateSubject());
        assertEquals("Unexpected certificate issuer", EXPECTED_ISSUER, trustStore.getCertificateIssuer());
    }

    public void testRefreshCertificate() throws Exception
    {
        _testPeer = new TestPeer();
        int listeningPort = _testPeer.start();

        Map<String, Object> attributes = getTrustStoreAttributes(listeningPort);

        final SiteSpecificTrustStore trustStore =
                (SiteSpecificTrustStore) _factory.create(TrustStore.class, attributes, _broker);

        assertEquals("Unexpected certificate subject", EXPECTED_SUBJECT, trustStore.getCertificateSubject());
        assertEquals("Unexpected certificate issuer", EXPECTED_ISSUER, trustStore.getCertificateIssuer());

        trustStore.refreshCertificate();

        assertEquals("Unexpected certificate subject", EXPECTED_SUBJECT, trustStore.getCertificateSubject());
        assertEquals("Unexpected certificate issuer", EXPECTED_ISSUER, trustStore.getCertificateIssuer());
    }

    private Map<String, Object> getTrustStoreAttributes(final int listeningPort)
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(SiteSpecificTrustStore.NAME, "mySiteSpecificTrustStore");
        attributes.put(SiteSpecificTrustStore.TYPE, "SiteSpecificTrustStore");
        attributes.put("siteUrl", String.format("https://localhost:%d", listeningPort));
        return attributes;
    }

    private class TestPeer implements Closeable
    {
        private final ExecutorService _socketAcceptExecutor = Executors.newSingleThreadExecutor();
        private ServerSocket _serverSocket;
        private final AtomicBoolean _shutdown = new AtomicBoolean();
        private boolean _accept = true;

        public void setAccept(final boolean accept)
        {
            _accept = accept;
        }

        public int start() throws Exception
        {
            _serverSocket = createTestSSLServerSocket();
            if (_accept)
            {
                _socketAcceptExecutor.execute(new AcceptingRunnable());
            }

            return _serverSocket.getLocalPort();
        }

        @Override
        public void close() throws IOException
        {
            _shutdown.set(true);
            try
            {
                if (_serverSocket != null)
                {
                    _serverSocket.close();
                }
            }
            finally
            {
                _socketAcceptExecutor.shutdown();
            }
        }

        private ServerSocket createTestSSLServerSocket() throws Exception
        {
            char[] keyPassword = TestSSLConstants.KEYSTORE_PASSWORD.toCharArray();
            try(InputStream inputStream = getClass().getResourceAsStream("/java_broker_keystore.jks"))
            {
                KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
                KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                keyStore.load(inputStream, keyPassword);
                keyManagerFactory.init(keyStore, keyPassword);
                KeyManager keyManagers[] = keyManagerFactory.getKeyManagers();
                SSLContext sslContext = SSLContext.getInstance("SSL");
                sslContext.init(keyManagers, null, new SecureRandom());
                SSLServerSocketFactory socketFactory = sslContext.getServerSocketFactory();
                ServerSocket serverSocket = socketFactory.createServerSocket(0);
                serverSocket.setSoTimeout(100);

                return serverSocket;
            }
        }

        private class AcceptingRunnable implements Runnable
        {
            @Override
            public void run()
            {
                do
                {
                    try (Socket sock = _serverSocket.accept())
                    {
                        final InputStream inputStream = sock.getInputStream();
                        while (inputStream.read() != -1)
                        {
                        }
                    }
                    catch (IOException e)
                    {
                        // Ignore
                    }
                }
                while (!_shutdown.get());
            }
        }
    }
}