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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocketFactory;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.ConfiguredObjectFactory;
import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.server.transport.network.security.ssl.SSLUtil;
import org.apache.qpid.test.utils.UnitTestBase;
import org.apache.qpid.test.utils.tls.KeyCertificatePair;
import org.apache.qpid.test.utils.tls.PrivateKeyEntry;
import org.apache.qpid.test.utils.tls.TlsResource;
import org.apache.qpid.test.utils.tls.TlsResourceBuilder;
import org.apache.qpid.test.utils.tls.TlsResourceExtension;
import org.apache.qpid.test.utils.tls.TlsResourceHelper;

@ExtendWith({ TlsResourceExtension.class })
public class SiteSpecificTrustStoreTest extends UnitTestBase
{
    private static final Broker<?> BROKER = BrokerTestHelper.createBrokerMock();
    private static final ConfiguredObjectFactory FACTORY = BrokerModel.getInstance().getObjectFactory();
    private static final String EXPECTED_SUBJECT = "CN=localhost";
    private static final String EXPECTED_ISSUER = "CN=MyRootCA";
    private static final String DN_BAR = "CN=bar";
    private static final String NAME = "mySiteSpecificTrustStore";
    private static final String SITE_SPECIFIC_TRUST_STORE = "SiteSpecificTrustStore";
    private static final String NOT_SUPPORTED_URL = "file:/not/a/host";
    private static final String INVALID_URL = "notaurl:541";
    private static final String NOT_A_CRL = "/not/a/crl";

    private TestPeer _testPeer;
    private String _clrUrl;
    private KeyCertificatePair _caKeyCertPair;
    private KeyCertificatePair _keyCertPair;

    @BeforeEach
    public void setUpSiteSpecificTrustStore(final TlsResource tls) throws Exception
    {
        final int connectTimeout = Integer.getInteger("SiteSpecificTrustStoreTest.connectTimeout", 1000);
        final int readTimeout = Integer.getInteger("SiteSpecificTrustStoreTest.readTimeout", 1000);
        setTestSystemProperty(SiteSpecificTrustStore.TRUST_STORE_SITE_SPECIFIC_CONNECT_TIMEOUT,
                              String.valueOf(connectTimeout));
        setTestSystemProperty(SiteSpecificTrustStore.TRUST_STORE_SITE_SPECIFIC_READ_TIMEOUT,
                              String.valueOf(readTimeout));

        _caKeyCertPair = TlsResourceBuilder.createKeyPairAndRootCA(EXPECTED_ISSUER);
        _keyCertPair = TlsResourceBuilder.createKeyPairAndCertificate(EXPECTED_SUBJECT, _caKeyCertPair);
        final KeyCertificatePair keyCertPair2 = TlsResourceBuilder.createKeyPairAndCertificate(DN_BAR, _caKeyCertPair);
        _clrUrl = tls.createCrlAsDataUrl(_caKeyCertPair, keyCertPair2.certificate());
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        if (_testPeer != null)
        {
            _testPeer.close();
        }
    }

    @Test
    public void testMalformedSiteUrl()
    {
        final Map<String, Object> attributes = Map.of(SiteSpecificTrustStore.NAME, NAME,
                SiteSpecificTrustStore.TYPE, SITE_SPECIFIC_TRUST_STORE,
                "siteUrl", INVALID_URL);

        KeyStoreTestHelper.checkExceptionThrownDuringKeyStoreCreation(FACTORY, BROKER, TrustStore.class, attributes,
                String.format("'%s' is not a valid URL", INVALID_URL));
    }

    @Test
    public void testSiteUrlDoesNotSupplyHostPort()
    {
        final Map<String, Object> attributes = Map.of(SiteSpecificTrustStore.NAME, NAME,
                SiteSpecificTrustStore.TYPE, SITE_SPECIFIC_TRUST_STORE,
                "siteUrl", NOT_SUPPORTED_URL);

        KeyStoreTestHelper.checkExceptionThrownDuringKeyStoreCreation(FACTORY, BROKER, TrustStore.class, attributes,
                String.format("URL '%s' does not provide a hostname and port number", NOT_SUPPORTED_URL));
    }

    @Test
    public void testUnresponsiveSite() throws Exception
    {
        _testPeer = new TestPeer();
        _testPeer.setAccept(false);
        final int listeningPort = _testPeer.start();
        final Map<String, Object> attributes = getTrustStoreAttributes(listeningPort);

        KeyStoreTestHelper.checkExceptionThrownDuringKeyStoreCreation(FACTORY, BROKER, TrustStore.class, attributes,
                String.format("Unable to get certificate for '%s' from", NAME));
    }

    @Test
    public void testValidSiteUrl() throws Exception
    {
        _testPeer = new TestPeer();
        final int listeningPort = _testPeer.start();
        final Map<String, Object> attributes = new HashMap<>(getTrustStoreAttributes(listeningPort));
        attributes.put(SiteSpecificTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        attributes.put(SiteSpecificTrustStore.CERTIFICATE_REVOCATION_LIST_URL, _clrUrl);
        final SiteSpecificTrustStore<?> trustStore = createTestTrustStore(attributes);
        final List<CertificateDetails> certDetails = trustStore.getCertificateDetails();

        assertEquals(1, certDetails.size(), "Unexpected number of certificates");
        final CertificateDetails certificateDetails = certDetails.get(0);

        assertEquals(EXPECTED_SUBJECT, certificateDetails.getSubjectName(),
                "Unexpected certificate subject");
        assertEquals(EXPECTED_ISSUER, certificateDetails.getIssuerName(), "Unexpected certificate issuer");
    }

    @Test
    public void testChangeOfCrlInValidSiteUrl(final TlsResource tls) throws Exception
    {
        _testPeer = new TestPeer();
        final int listeningPort = _testPeer.start();
        final Map<String, Object> attributes = new HashMap<>(getTrustStoreAttributes(listeningPort));
        attributes.put(SiteSpecificTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        attributes.put(SiteSpecificTrustStore.CERTIFICATE_REVOCATION_LIST_URL, _clrUrl);
        final SiteSpecificTrustStore<?> trustStore = createTestTrustStore(attributes);

        final IllegalConfigurationException thrown = assertThrows(IllegalConfigurationException.class,
                () -> trustStore.setAttributes(Map.of(FileTrustStore.CERTIFICATE_REVOCATION_LIST_URL, NOT_A_CRL)),
                "Exception not thrown");

        assertTrue(thrown.getMessage().contains(
                String.format("Unable to load certificate revocation list '%s' for truststore '%s'", NOT_A_CRL, NAME)),
                "Exception text not as unexpected:" + thrown.getMessage());

        assertEquals(_clrUrl, trustStore.getCertificateRevocationListUrl(),
                "Unexpected CRL path value after failed change");

        final Path emptyCrl = tls.createCrl(_caKeyCertPair);
        trustStore.setAttributes(Map.of(FileTrustStore.CERTIFICATE_REVOCATION_LIST_URL, emptyCrl.toFile().getAbsolutePath()));

        assertEquals(emptyCrl.toFile().getAbsolutePath(), trustStore.getCertificateRevocationListUrl(),
                "Unexpected CRL path value after change that is expected to be successful");
    }

    @Test
    public void testValidSiteUrl_MissingCrlFile() throws Exception
    {
        _testPeer = new TestPeer();
        final int listeningPort = _testPeer.start();
        final Map<String, Object> attributes = new HashMap<>(getTrustStoreAttributes(listeningPort));
        attributes.put(SiteSpecificTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        attributes.put(SiteSpecificTrustStore.CERTIFICATE_REVOCATION_LIST_URL, NOT_A_CRL);

        KeyStoreTestHelper.checkExceptionThrownDuringKeyStoreCreation(FACTORY, BROKER, TrustStore.class, attributes,
                String.format("Unable to load certificate revocation list '%s' for truststore '%s'", NOT_A_CRL, NAME));
    }

    @Test
    public void testRefreshCertificate() throws Exception
    {
        _testPeer = new TestPeer();
        final int listeningPort = _testPeer.start();
        final Map<String, Object> attributes = getTrustStoreAttributes(listeningPort);
        final SiteSpecificTrustStore<?> trustStore = createTestTrustStore(attributes);

        List<CertificateDetails> certDetails = trustStore.getCertificateDetails();
        assertEquals(1, certDetails.size(), "Unexpected number of certificates");

        CertificateDetails certificateDetails = certDetails.get(0);

        assertEquals(EXPECTED_SUBJECT, certificateDetails.getSubjectName(), "Unexpected certificate subject");
        assertEquals(EXPECTED_ISSUER, certificateDetails.getIssuerName(), "Unexpected certificate issuer");

        trustStore.refreshCertificate();

        certDetails = trustStore.getCertificateDetails();
        certificateDetails = certDetails.get(0);

        assertEquals(EXPECTED_SUBJECT, certificateDetails.getSubjectName(), "Unexpected certificate subject");
        assertEquals(EXPECTED_ISSUER, certificateDetails.getIssuerName(), "Unexpected certificate issuer");
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private SiteSpecificTrustStore createTestTrustStore(final Map<String, Object> attributes)
    {
        return (SiteSpecificTrustStore) FACTORY.create(TrustStore.class, attributes, BROKER);
    }

    private Map<String, Object> getTrustStoreAttributes(final int listeningPort)
    {
        return Map.of(SiteSpecificTrustStore.NAME, NAME,
                SiteSpecificTrustStore.TYPE, SITE_SPECIFIC_TRUST_STORE,
                "siteUrl", String.format("https://localhost:%d", listeningPort));
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
            final char[] secret = "".toCharArray();

            final java.security.KeyStore inMemoryKeyStore =
                    TlsResourceHelper.createKeyStore(java.security.KeyStore.getDefaultType(),
                                                     secret,
                                                     new PrivateKeyEntry("1",
                                                                         _keyCertPair.privateKey(),
                                                                         _keyCertPair.certificate()));

            final KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(inMemoryKeyStore, secret);
            final KeyManager[] keyManagers = kmf.getKeyManagers();
            final SSLContext sslContext = SSLUtil.tryGetSSLContext();
            sslContext.init(keyManagers, null, new SecureRandom());
            final SSLServerSocketFactory socketFactory = sslContext.getServerSocketFactory();
            final ServerSocket serverSocket = socketFactory.createServerSocket(0);
            serverSocket.setSoTimeout(100);
            return serverSocket;
        }

        private class AcceptingRunnable implements Runnable
        {
            @Override
            public void run()
            {
                do
                {
                    try (final Socket sock = _serverSocket.accept();
                         final InputStream inputStream = sock.getInputStream())
                    {
                        while (inputStream.read() != -1)
                        {
                            // ignore
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