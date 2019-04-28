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
import static org.apache.qpid.test.utils.TestSSLConstants.JAVA_KEYSTORE_TYPE;
import static org.apache.qpid.test.utils.TestSSLConstants.KEYSTORE_PASSWORD;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.security.Key;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.KeyManager;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.LogMessage;
import org.apache.qpid.server.logging.MessageLogger;
import org.apache.qpid.server.logging.messages.KeyStoreMessages;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.ConfiguredObjectFactory;
import org.apache.qpid.server.model.KeyStore;
import org.apache.qpid.server.transport.network.security.ssl.SSLUtil;
import org.apache.qpid.server.util.DataUrlUtils;
import org.apache.qpid.test.utils.TestFileUtils;
import org.apache.qpid.test.utils.TestSSLUtils;
import org.apache.qpid.test.utils.UnitTestBase;

public class NonJavaKeyStoreTest extends UnitTestBase
{
    private static final String KEYSTORE = "/ssl/java_broker_keystore.pkcs12";
    private Broker<?> _broker;
    private ConfiguredObjectFactory _factory;
    private List<File> _testResources;
    private MessageLogger _messageLogger;

    @Before
    public void setUp() throws Exception
    {
        _messageLogger = mock(MessageLogger.class);
        _broker = BrokerTestHelper.createBrokerMock();
        when(_broker.getEventLogger()).thenReturn(new EventLogger(_messageLogger));
        _factory = _broker.getObjectFactory();
        _testResources = new ArrayList<>();
    }

    @After
    public void tearDown() throws Exception
    {
        for (File resource: _testResources)
        {
            try
            {
                resource.delete();
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
    }

    private File[] extractResourcesFromTestKeyStore(boolean pem, final String storeResource) throws Exception
    {
        java.security.KeyStore ks = java.security.KeyStore.getInstance(JAVA_KEYSTORE_TYPE);
        try(InputStream is = getClass().getResourceAsStream(storeResource))
        {
            ks.load(is, KEYSTORE_PASSWORD.toCharArray() );
        }


        File privateKeyFile = TestFileUtils.createTempFile(this, ".private-key.der");
        try(FileOutputStream kos = new FileOutputStream(privateKeyFile))
        {
            Key pvt = ks.getKey("java-broker", KEYSTORE_PASSWORD.toCharArray());
            if (pem)
            {
                kos.write(TestSSLUtils.privateKeyToPEM(pvt).getBytes(UTF_8));
            }
            else
            {
                kos.write(pvt.getEncoded());
            }
            kos.flush();
        }

        File certificateFile = TestFileUtils.createTempFile(this, ".certificate.der");

        try(FileOutputStream cos = new FileOutputStream(certificateFile))
        {
            Certificate pub = ks.getCertificate("java-broker");
            if (pem)
            {
                cos.write(TestSSLUtils.certificateToPEM(pub).getBytes(UTF_8));
            }
            else
            {
                cos.write(pub.getEncoded());
            }
            cos.flush();
        }

        return new File[]{privateKeyFile,certificateFile};
    }

    @Test
    public void testCreationOfTrustStoreFromValidPrivateKeyAndCertificateInDERFormat() throws Exception
    {
        runTestCreationOfTrustStoreFromValidPrivateKeyAndCertificateInDerFormat(false);
    }

    @Test
    public void testCreationOfTrustStoreFromValidPrivateKeyAndCertificateInPEMFormat() throws Exception
    {
        runTestCreationOfTrustStoreFromValidPrivateKeyAndCertificateInDerFormat(true);
    }

    private void runTestCreationOfTrustStoreFromValidPrivateKeyAndCertificateInDerFormat(boolean isPEM)throws Exception
    {
        File[] resources = extractResourcesFromTestKeyStore(isPEM, KEYSTORE);
        _testResources.addAll(Arrays.asList(resources));

        Map<String,Object> attributes = new HashMap<>();
        attributes.put(NonJavaKeyStore.NAME, "myTestTrustStore");
        attributes.put("privateKeyUrl", resources[0].toURI().toURL().toExternalForm());
        attributes.put("certificateUrl", resources[1].toURI().toURL().toExternalForm());
        attributes.put(NonJavaKeyStore.TYPE, "NonJavaKeyStore");

        NonJavaKeyStoreImpl fileTrustStore =
                (NonJavaKeyStoreImpl) _factory.create(KeyStore.class, attributes,  _broker);

        KeyManager[] keyManagers = fileTrustStore.getKeyManagers();
        assertNotNull(keyManagers);
        assertEquals("Unexpected number of key managers", (long) 1, (long) keyManagers.length);
        assertNotNull("Key manager is null", keyManagers[0]);
    }

    @Test
    public void testCreationOfTrustStoreFromValidPrivateKeyAndInvalidCertificate()throws Exception
    {
        File[] resources = extractResourcesFromTestKeyStore(true, KEYSTORE);
        _testResources.addAll(Arrays.asList(resources));

        File invalidCertificate = TestFileUtils.createTempFile(this, ".invalid.cert", "content");
        _testResources.add(invalidCertificate);

        Map<String,Object> attributes = new HashMap<>();
        attributes.put(NonJavaKeyStore.NAME, "myTestTrustStore");
        attributes.put("privateKeyUrl", resources[0].toURI().toURL().toExternalForm());
        attributes.put("certificateUrl", invalidCertificate.toURI().toURL().toExternalForm());
        attributes.put(NonJavaKeyStore.TYPE, "NonJavaKeyStore");

        try
        {
            _factory.create(KeyStore.class, attributes, _broker);
            fail("Created key store from invalid certificate");
        }
        catch(IllegalConfigurationException e)
        {
            // pass
        }
    }

    @Test
    public void testCreationOfTrustStoreFromInvalidPrivateKeyAndValidCertificate()throws Exception
    {
        File[] resources = extractResourcesFromTestKeyStore(true, KEYSTORE);
        _testResources.addAll(Arrays.asList(resources));

        File invalidPrivateKey = TestFileUtils.createTempFile(this, ".invalid.pk", "content");
        _testResources.add(invalidPrivateKey);

        Map<String,Object> attributes = new HashMap<>();
        attributes.put(NonJavaKeyStore.NAME, "myTestTrustStore");
        attributes.put("privateKeyUrl", invalidPrivateKey.toURI().toURL().toExternalForm());
        attributes.put("certificateUrl", resources[1].toURI().toURL().toExternalForm());
        attributes.put(NonJavaKeyStore.TYPE, "NonJavaKeyStore");

        try
        {
            _factory.create(KeyStore.class, attributes, _broker);
            fail("Created key store from invalid certificate");
        }
        catch(IllegalConfigurationException e)
        {
            // pass
        }
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
        when(_broker.scheduleHouseKeepingTask(anyLong(), any(TimeUnit.class), any(Runnable.class))).thenReturn(mock(ScheduledFuture.class));

        java.security.KeyStore ks = java.security.KeyStore.getInstance(JAVA_KEYSTORE_TYPE);
        final String storeLocation = KEYSTORE;
        try(InputStream is = getClass().getResourceAsStream(storeLocation))
        {
            ks.load(is, KEYSTORE_PASSWORD.toCharArray() );
        }
        X509Certificate cert = (X509Certificate) ks.getCertificate("rootca");
        int expiryDays = (int)((cert.getNotAfter().getTime() - System.currentTimeMillis()) / (24l * 60l * 60l * 1000l));

        File[] resources = extractResourcesFromTestKeyStore(false, storeLocation);
        _testResources.addAll(Arrays.asList(resources));

        Map<String,Object> attributes = new HashMap<>();
        attributes.put(NonJavaKeyStore.NAME, "myTestTrustStore");
        attributes.put("privateKeyUrl", resources[0].toURI().toURL().toExternalForm());
        attributes.put("certificateUrl", resources[1].toURI().toURL().toExternalForm());
        attributes.put("context", Collections.singletonMap(KeyStore.CERTIFICATE_EXPIRY_WARN_PERIOD, expiryDays + expiryOffset));
        attributes.put(NonJavaKeyStore.TYPE, "NonJavaKeyStore");
        _factory.create(KeyStore.class, attributes, _broker);
    }

    @Test
    public void testCreationOfKeyStoreWithNonMatchingPrivateKeyAndCertificate()throws Exception
    {
        assumeThat(SSLUtil.canGenerateCerts(), is(true));

        final SSLUtil.KeyCertPair keyCertPair = generateSelfSignedCertificate();
        final SSLUtil.KeyCertPair keyCertPair2 = generateSelfSignedCertificate();

        final Map<String,Object> attributes = new HashMap<>();
        attributes.put(NonJavaKeyStore.NAME, "myTestTrustStore");
        attributes.put(NonJavaKeyStore.PRIVATE_KEY_URL,
                       DataUrlUtils.getDataUrlForBytes(TestSSLUtils.privateKeyToPEM(keyCertPair.getPrivateKey()).getBytes(UTF_8)));
        attributes.put(NonJavaKeyStore.CERTIFICATE_URL,
                       DataUrlUtils.getDataUrlForBytes(TestSSLUtils.certificateToPEM(keyCertPair2.getCertificate()).getBytes(UTF_8)));
        attributes.put(NonJavaKeyStore.TYPE, "NonJavaKeyStore");

        try
        {
            _factory.create(KeyStore.class, attributes, _broker);
            fail("Created key store from invalid certificate");
        }
        catch(IllegalConfigurationException e)
        {
            // pass
        }
    }

    @Test
    public void testUpdateKeyStoreToNonMatchingCertificate()throws Exception
    {
        assumeThat(SSLUtil.canGenerateCerts(), is(true));

        final SSLUtil.KeyCertPair keyCertPair = generateSelfSignedCertificate();
        final SSLUtil.KeyCertPair keyCertPair2 = generateSelfSignedCertificate();

        final Map<String,Object> attributes = new HashMap<>();
        attributes.put(NonJavaKeyStore.NAME, getTestName());
        attributes.put(NonJavaKeyStore.PRIVATE_KEY_URL,
                       DataUrlUtils.getDataUrlForBytes(TestSSLUtils.privateKeyToPEM(keyCertPair.getPrivateKey()).getBytes(UTF_8)));
        attributes.put(NonJavaKeyStore.CERTIFICATE_URL,
                       DataUrlUtils.getDataUrlForBytes(TestSSLUtils.certificateToPEM(keyCertPair.getCertificate()).getBytes(UTF_8)));
        attributes.put(NonJavaKeyStore.TYPE, "NonJavaKeyStore");

        final KeyStore trustStore = _factory.create(KeyStore.class, attributes, _broker);
        try
        {
            final String certUrl = DataUrlUtils.getDataUrlForBytes(TestSSLUtils.certificateToPEM(keyCertPair2.getCertificate()).getBytes(UTF_8));
            trustStore.setAttributes(Collections.singletonMap("certificateUrl", certUrl));
            fail("Created key store from invalid certificate");
        }
        catch(IllegalConfigurationException e)
        {
            // pass
        }
    }

    private SSLUtil.KeyCertPair generateSelfSignedCertificate() throws Exception
    {
        return SSLUtil.generateSelfSignedCertificate("RSA",
                                                     "SHA256WithRSA",
                                                     2048,
                                                     Instant.now()
                                                            .minus(1, ChronoUnit.DAYS)
                                                            .toEpochMilli(),
                                                     Duration.of(365, ChronoUnit.DAYS)
                                                             .getSeconds(),
                                                     "CN=foo",
                                                     Collections.emptySet(),
                                                     Collections.emptySet());
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
