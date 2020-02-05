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

package org.apache.qpid.server.security;


import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import java.nio.file.Path;
import java.security.PrivateKey;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.KeyManager;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.LogMessage;
import org.apache.qpid.server.logging.MessageLogger;
import org.apache.qpid.server.logging.messages.KeyStoreMessages;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.ConfiguredObjectFactory;
import org.apache.qpid.server.model.KeyStore;
import org.apache.qpid.test.utils.tls.KeyCertificatePair;
import org.apache.qpid.test.utils.tls.TlsResource;
import org.apache.qpid.test.utils.tls.TlsResourceBuilder;
import org.apache.qpid.server.util.DataUrlUtils;
import org.apache.qpid.test.utils.UnitTestBase;
import org.apache.qpid.test.utils.tls.TlsResourceHelper;

public class NonJavaKeyStoreTest extends UnitTestBase
{
    @ClassRule
    public static final TlsResource TLS_RESOURCE = new TlsResource();

    private static final String DN_FOO = "CN=foo";
    private static final String NAME = "myTestTrustStore";
    private static final String NON_JAVA_KEY_STORE = "NonJavaKeyStore";
    private static final Broker BROKER = BrokerTestHelper.createBrokerMock();
    private static final ConfiguredObjectFactory FACTORY = BrokerModel.getInstance().getObjectFactory();
    private MessageLogger _messageLogger;
    private KeyCertificatePair _keyCertPair;

    @Before
    public void setUp() throws Exception
    {
        _messageLogger = mock(MessageLogger.class);
        when(BROKER.getEventLogger()).thenReturn(new EventLogger(_messageLogger));
        _keyCertPair = generateSelfSignedCertificate();
    }

    @Test
    public void testCreationOfTrustStoreFromValidPrivateKeyAndCertificateInDERFormat() throws Exception
    {
        final Path privateKeyFile = TLS_RESOURCE.savePrivateKeyAsDer(_keyCertPair.getPrivateKey());
        final Path certificateFile = TLS_RESOURCE.saveCertificateAsDer(_keyCertPair.getCertificate());
        assertCreationOfTrustStoreFromValidPrivateKeyAndCertificate(privateKeyFile, certificateFile);
    }

    @Test
    public void testCreationOfTrustStoreFromValidPrivateKeyAndCertificateInPEMFormat() throws Exception
    {
        final Path privateKeyFile = TLS_RESOURCE.savePrivateKeyAsPem(_keyCertPair.getPrivateKey());
        final Path certificateFile = TLS_RESOURCE.saveCertificateAsPem(_keyCertPair.getCertificate());
        assertCreationOfTrustStoreFromValidPrivateKeyAndCertificate(privateKeyFile, certificateFile);
    }

    private void assertCreationOfTrustStoreFromValidPrivateKeyAndCertificate(Path privateKeyFile, Path certificateFile) throws Exception
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(NonJavaKeyStore.NAME, NAME);
        attributes.put("privateKeyUrl", privateKeyFile.toFile().getAbsolutePath());
        attributes.put("certificateUrl", certificateFile.toFile().getAbsolutePath());
        attributes.put(NonJavaKeyStore.TYPE, NON_JAVA_KEY_STORE);

        final NonJavaKeyStore<?> fileTrustStore = (NonJavaKeyStore<?>)  createTestKeyStore(attributes);

        KeyManager[] keyManagers = fileTrustStore.getKeyManagers();
        assertNotNull(keyManagers);
        assertEquals("Unexpected number of key managers", 1, keyManagers.length);
        assertNotNull("Key manager is null", keyManagers[0]);
    }

    @Test
    public void testCreationOfTrustStoreFromValidPrivateKeyAndInvalidCertificate()throws Exception
    {
        final Path privateKeyFile = TLS_RESOURCE.savePrivateKeyAsPem(_keyCertPair.getPrivateKey());
        final Path certificateFile = TLS_RESOURCE.createFile(".cer");

        Map<String,Object> attributes = new HashMap<>();
        attributes.put(NonJavaKeyStore.NAME, NAME);
        attributes.put("privateKeyUrl", privateKeyFile.toFile().getAbsolutePath());
        attributes.put("certificateUrl", certificateFile.toFile().getAbsolutePath());
        attributes.put(NonJavaKeyStore.TYPE, NON_JAVA_KEY_STORE);

        KeyStoreTestHelper.checkExceptionThrownDuringKeyStoreCreation(FACTORY, BROKER, KeyStore.class, attributes,
                "Cannot load private key or certificate(s)");
    }

    @Test
    public void testCreationOfTrustStoreFromInvalidPrivateKeyAndValidCertificate()throws Exception
    {
        final Path privateKeyFile =  TLS_RESOURCE.createFile(".pk");
        final Path certificateFile = TLS_RESOURCE.saveCertificateAsPem(_keyCertPair.getCertificate());

        Map<String,Object> attributes = new HashMap<>();
        attributes.put(NonJavaKeyStore.NAME, NAME);
        attributes.put("privateKeyUrl", privateKeyFile.toFile().getAbsolutePath());
        attributes.put("certificateUrl", certificateFile.toFile().getAbsolutePath());
        attributes.put(NonJavaKeyStore.TYPE, NON_JAVA_KEY_STORE);

        KeyStoreTestHelper.checkExceptionThrownDuringKeyStoreCreation(FACTORY, BROKER, KeyStore.class, attributes,
                "Cannot load private key or certificate(s): java.security.spec.InvalidKeySpecException: " +
                        "Unable to parse key as PKCS#1 format");
    }

    @Test
    public void testExpiryCheckingFindsExpired() throws Exception
    {
        doCertExpiryChecking(1);

        verify(_messageLogger, times(1)).message(argThat(new LogMessageArgumentMatcher()));

    }

    @Test
    public void testExpiryCheckingIgnoresValid() throws Exception
    {
        doCertExpiryChecking(-1);

        verify(_messageLogger, never()).message(argThat(new LogMessageArgumentMatcher()));

    }

    private void doCertExpiryChecking(final int expiryOffset) throws Exception
    {
        when(BROKER.scheduleHouseKeepingTask(anyLong(), any(TimeUnit.class), any(Runnable.class))).thenReturn(mock(ScheduledFuture.class));

        final Path privateKeyFile =  TLS_RESOURCE.savePrivateKeyAsDer(_keyCertPair.getPrivateKey());
        final Path certificateFile = TLS_RESOURCE.saveCertificateAsDer(_keyCertPair.getCertificate());
        final long expiryDays = ChronoUnit.DAYS.between(Instant.now(), _keyCertPair.getCertificate().getNotAfter().toInstant());

        Map<String,Object> attributes = new HashMap<>();
        attributes.put(NonJavaKeyStore.NAME, NAME);
        attributes.put("privateKeyUrl", privateKeyFile.toFile().getAbsolutePath());
        attributes.put("certificateUrl", certificateFile.toFile().getAbsolutePath());
        attributes.put("context", Collections.singletonMap(KeyStore.CERTIFICATE_EXPIRY_WARN_PERIOD, expiryDays + expiryOffset));
        attributes.put(NonJavaKeyStore.TYPE, NON_JAVA_KEY_STORE);
        createTestKeyStore(attributes);
    }

    @Test
    public void testCreationOfKeyStoreWithNonMatchingPrivateKeyAndCertificate()throws Exception
    {
        final KeyCertificatePair keyCertPair2 = generateSelfSignedCertificate();

        final Map<String,Object> attributes = new HashMap<>();
        attributes.put(NonJavaKeyStore.NAME, NAME);
        attributes.put(NonJavaKeyStore.PRIVATE_KEY_URL, getPrivateKeyAsDataUrl(_keyCertPair.getPrivateKey()));
        attributes.put(NonJavaKeyStore.CERTIFICATE_URL, getCertificateAsDataUrl(keyCertPair2.getCertificate()));
        attributes.put(NonJavaKeyStore.TYPE, NON_JAVA_KEY_STORE);

        KeyStoreTestHelper.checkExceptionThrownDuringKeyStoreCreation(FACTORY, BROKER, KeyStore.class, attributes,
                "Private key does not match certificate");
    }

    @Test
    public void testUpdateKeyStoreToNonMatchingCertificate()throws Exception
    {
        final Map<String,Object> attributes = new HashMap<>();
        attributes.put(NonJavaKeyStore.NAME, getTestName());
        attributes.put(NonJavaKeyStore.PRIVATE_KEY_URL, getPrivateKeyAsDataUrl(_keyCertPair.getPrivateKey()));
        attributes.put(NonJavaKeyStore.CERTIFICATE_URL, getCertificateAsDataUrl(_keyCertPair.getCertificate()));
        attributes.put(NonJavaKeyStore.TYPE, NON_JAVA_KEY_STORE);

        final KeyStore<?> trustStore = createTestKeyStore(attributes);

        final KeyCertificatePair keyCertPair2 = generateSelfSignedCertificate();
        try
        {
            final String certUrl = getCertificateAsDataUrl(keyCertPair2.getCertificate());
            trustStore.setAttributes(Collections.singletonMap("certificateUrl", certUrl));
            fail("Created key store from invalid certificate");
        }
        catch(IllegalConfigurationException e)
        {
            // pass
        }
    }

    @SuppressWarnings("unchecked")
    private KeyStore<?> createTestKeyStore(final Map<String, Object> attributes)
    {
        return (KeyStore<?>) FACTORY.create(KeyStore.class, attributes, BROKER);
    }

    private String getCertificateAsDataUrl(final X509Certificate certificate) throws CertificateEncodingException
    {
        return DataUrlUtils.getDataUrlForBytes(TlsResourceHelper.toPEM(certificate).getBytes(UTF_8));
    }

    private String getPrivateKeyAsDataUrl(final PrivateKey privateKey)
    {
        return DataUrlUtils.getDataUrlForBytes(TlsResourceHelper.toPEM(privateKey).getBytes(UTF_8));
    }

    private KeyCertificatePair generateSelfSignedCertificate() throws Exception
    {
        return TlsResourceBuilder.createSelfSigned(DN_FOO);
    }

    private static class LogMessageArgumentMatcher implements ArgumentMatcher<LogMessage>
    {
        @Override
        public boolean matches(final LogMessage arg)
        {
            return arg.getLogHierarchy().equals(KeyStoreMessages.EXPIRING_LOG_HIERARCHY);
        }
    }
}
