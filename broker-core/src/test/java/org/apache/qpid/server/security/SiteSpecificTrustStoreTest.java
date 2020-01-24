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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.KeyStore;
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

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.ConfiguredObjectFactory;
import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.test.utils.UnitTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.test.utils.TestSSLConstants;

public class SiteSpecificTrustStoreTest extends UnitTestBase
{
    private static final Broker BROKER = BrokerTestHelper.createBrokerMock();
    private static final ConfiguredObjectFactory FACTORY = BrokerModel.getInstance().getObjectFactory();
    private static final String EXPECTED_SUBJECT = "CN=localhost,OU=Unknown,O=Unknown,L=Unknown,ST=Unknown,C=CA";
    private static final String EXPECTED_ISSUER = "CN=MyRootCA,O=ACME,ST=Ontario,C=CA";
    private TestPeer _testPeer;

    @Before
    public void setUpSiteSpecificTrustStore()
    {
        int connectTimeout = Integer.getInteger("SiteSpecificTrustStoreTest.connectTimeout", 1000);
        int readTimeout = Integer.getInteger("SiteSpecificTrustStoreTest.readTimeout", 1000);
        setTestSystemProperty(SiteSpecificTrustStore.TRUST_STORE_SITE_SPECIFIC_CONNECT_TIMEOUT, String.valueOf(connectTimeout));
        setTestSystemProperty(SiteSpecificTrustStore.TRUST_STORE_SITE_SPECIFIC_READ_TIMEOUT, String.valueOf(readTimeout));
    }

    @After
    public void tearDown() throws Exception
    {
        try
        {
        }
        finally
        {
            if (_testPeer != null)
            {
                _testPeer.close();
            }
        }
    }

    @Test
    public void testMalformedSiteUrl()
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(SiteSpecificTrustStore.NAME, "mySiteSpecificTrustStore");
        attributes.put(SiteSpecificTrustStore.TYPE, "SiteSpecificTrustStore");
        attributes.put("siteUrl", "notaurl:541");

        KeyStoreTestHelper.checkExceptionThrownDuringKeyStoreCreation(FACTORY, BROKER, TrustStore.class, attributes,
                "'notaurl:541' is not a valid URL");
    }

    @Test
    public void testSiteUrlDoesNotSupplyHostPort()
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(SiteSpecificTrustStore.NAME, "mySiteSpecificTrustStore");
        attributes.put(SiteSpecificTrustStore.TYPE, "SiteSpecificTrustStore");
        attributes.put("siteUrl", "file:/not/a/host");

        KeyStoreTestHelper.checkExceptionThrownDuringKeyStoreCreation(FACTORY, BROKER, TrustStore.class, attributes,
                "URL 'file:/not/a/host' does not provide a hostname and port number");
    }

    @Test
    public void testUnresponsiveSite() throws Exception
    {
        _testPeer = new TestPeer();
        _testPeer.setAccept(false);
        int listeningPort = _testPeer.start();
        Map<String, Object> attributes = getTrustStoreAttributes(listeningPort);

        KeyStoreTestHelper.checkExceptionThrownDuringKeyStoreCreation(FACTORY, BROKER, TrustStore.class, attributes,
                "Unable to get certificate for 'mySiteSpecificTrustStore' from");
    }

    @Test
    public void testValidSiteUrl() throws Exception
    {
        _testPeer = new TestPeer();
        int listeningPort = _testPeer.start();

        Map<String, Object> attributes = getTrustStoreAttributes(listeningPort);
        attributes.put(SiteSpecificTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        attributes.put(SiteSpecificTrustStore.CERTIFICATE_REVOCATION_LIST_URL, TestSSLConstants.CA_CRL);

        final SiteSpecificTrustStore trustStore =
                (SiteSpecificTrustStore) FACTORY.create(TrustStore.class, attributes, BROKER);

        List<CertificateDetails> certDetails = trustStore.getCertificateDetails();
        assertEquals("Unexpected number of certificates", 1, certDetails.size());
        CertificateDetails certificateDetails = certDetails.get(0);

        assertEquals("Unexpected certificate subject", EXPECTED_SUBJECT, certificateDetails.getSubjectName());
        assertEquals("Unexpected certificate issuer", EXPECTED_ISSUER, certificateDetails.getIssuerName());
    }

    @Test
    public void testChangeOfCrlInValidSiteUrl() throws Exception
    {
        _testPeer = new TestPeer();
        int listeningPort = _testPeer.start();

        Map<String, Object> attributes = getTrustStoreAttributes(listeningPort);
        attributes.put(SiteSpecificTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        attributes.put(SiteSpecificTrustStore.CERTIFICATE_REVOCATION_LIST_URL, TestSSLConstants.CA_CRL);

        final SiteSpecificTrustStore trustStore =
                (SiteSpecificTrustStore) FACTORY.create(TrustStore.class, attributes, BROKER);

        try
        {
            Map<String,Object> unacceptableAttributes = new HashMap<>();
            unacceptableAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_LIST_URL, "/not/a/crl");

            trustStore.setAttributes(unacceptableAttributes);
            fail("Exception not thrown");
        }
        catch (IllegalConfigurationException e)
        {
            String message = e.getMessage();
            assertTrue("Exception text not as unexpected:" + message,
                    message.contains("Unable to load certificate revocation list '/not/a/crl' for truststore 'mySiteSpecificTrustStore'"));
        }

        assertEquals("Unexpected CRL path value after failed change",
                TestSSLConstants.CA_CRL, trustStore.getCertificateRevocationListUrl());

        Map<String,Object> changedAttributes = new HashMap<>();
        changedAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_LIST_URL, TestSSLConstants.CA_CRL_EMPTY);

        trustStore.setAttributes(changedAttributes);

        assertEquals("Unexpected CRL path value after change that is expected to be successful",
                TestSSLConstants.CA_CRL_EMPTY, trustStore.getCertificateRevocationListUrl());
    }

    @Test
    public void testValidSiteUrl_MissingCrlFile() throws Exception
    {
        _testPeer = new TestPeer();
        int listeningPort = _testPeer.start();
        Map<String, Object> attributes = getTrustStoreAttributes(listeningPort);
        attributes.put(SiteSpecificTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        attributes.put(SiteSpecificTrustStore.CERTIFICATE_REVOCATION_LIST_URL, "/not/a/crl");

        KeyStoreTestHelper.checkExceptionThrownDuringKeyStoreCreation(FACTORY, BROKER, TrustStore.class, attributes,
                "Unable to load certificate revocation list '/not/a/crl' for truststore 'mySiteSpecificTrustStore'");
    }

    @Test
    public void testRefreshCertificate() throws Exception
    {
        _testPeer = new TestPeer();
        int listeningPort = _testPeer.start();

        Map<String, Object> attributes = getTrustStoreAttributes(listeningPort);

        final SiteSpecificTrustStore trustStore =
                (SiteSpecificTrustStore) FACTORY.create(TrustStore.class, attributes, BROKER);

        List<CertificateDetails> certDetails = trustStore.getCertificateDetails();
        assertEquals("Unexpected number of certificates", 1, certDetails.size());

        CertificateDetails certificateDetails = certDetails.get(0);

        assertEquals("Unexpected certificate subject", EXPECTED_SUBJECT, certificateDetails.getSubjectName());
        assertEquals("Unexpected certificate issuer", EXPECTED_ISSUER, certificateDetails.getIssuerName());

        trustStore.refreshCertificate();

        certDetails = trustStore.getCertificateDetails();
        certificateDetails = certDetails.get(0);

        assertEquals("Unexpected certificate subject", EXPECTED_SUBJECT, certificateDetails.getSubjectName());
        assertEquals("Unexpected certificate issuer", EXPECTED_ISSUER, certificateDetails.getIssuerName());
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
            char[] keyPassword = TestSSLConstants.PASSWORD.toCharArray();
            try(InputStream inputStream = new FileInputStream(TestSSLConstants.BROKER_KEYSTORE))
            {
                KeyStore keyStore = KeyStore.getInstance(TestSSLConstants.JAVA_KEYSTORE_TYPE);
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