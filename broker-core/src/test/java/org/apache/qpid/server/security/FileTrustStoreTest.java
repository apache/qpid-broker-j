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

import static org.apache.qpid.server.transport.network.security.ssl.SSLUtil.getInitializedKeyStore;
import static org.apache.qpid.test.utils.JvmVendor.IBM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateExpiredException;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Enumeration;
import java.util.Map;
import java.util.Objects;

import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.junit.jupiter.api.Test;

import org.junit.jupiter.api.extension.RegisterExtension;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.ConfiguredObjectFactory;
import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.test.utils.tls.CertificateEntry;
import org.apache.qpid.test.utils.tls.KeyCertificatePair;
import org.apache.qpid.test.utils.tls.PrivateKeyEntry;
import org.apache.qpid.test.utils.tls.SecretKeyEntry;
import org.apache.qpid.test.utils.tls.TlsResource;
import org.apache.qpid.server.transport.network.security.ssl.QpidPeersOnlyTrustManager;
import org.apache.qpid.test.utils.tls.TlsResourceBuilder;
import org.apache.qpid.server.util.DataUrlUtils;
import org.apache.qpid.test.utils.UnitTestBase;
import org.apache.qpid.test.utils.tls.TlsResourceHelper;

public class FileTrustStoreTest extends UnitTestBase
{
    @RegisterExtension
    public static final TlsResource TLS_RESOURCE = new TlsResource();

    private static final Broker<?> BROKER = BrokerTestHelper.createBrokerMock();
    private static final ConfiguredObjectFactory FACTORY = BrokerModel.getInstance().getObjectFactory();
    private static final String DN_FOO = "CN=foo";
    private static final String DN_BAR = "CN=bar";
    private static final String DN_CA = "CN=CA";
    private static final String CERTIFICATE_ALIAS_A = "a";
    private static final String CERTIFICATE_ALIAS_B = "b";
    private static final String NOT_A_CRL = "/not/a/crl";
    private static final String NAME = "myFileTrustStore";
    private static final String NOT_A_TRUSTSTORE = "/not/a/truststore";
    private static final String SECRET_KEY_ALIAS = "secret-key-alias";

    @Test
    public void testCreateFileTrustStoreWithoutCRL() throws Exception
    {
        final Path keyStoreFile = TLS_RESOURCE.createSelfSignedTrustStore(DN_FOO);
        final Map<String, Object> attributes = Map.of(
                FileTrustStore.NAME, NAME,
                FileTrustStore.STORE_URL, keyStoreFile.toFile().getAbsolutePath(),
                FileTrustStore.PASSWORD, TLS_RESOURCE.getSecret(),
                FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, false);
        final FileTrustStore<?> fileTrustStore = createFileTrustStore(attributes);
        final TrustManager[] trustManagers = fileTrustStore.getTrustManagers();

        assertNotNull(trustManagers);
        assertEquals(1, trustManagers.length, "Unexpected number of trust managers");
        assertNotNull(trustManagers[0], "Trust manager unexpected null");
    }

    @Test
    public void testCreateFileTrustStoreFromWithExplicitlySetCRL() throws Exception
    {
        final StoreAndCrl<Path> data = generateTrustStoreAndCrl();
        final Map<String, Object> attributes = Map.of(FileTrustStore.NAME, NAME,
                FileTrustStore.STORE_URL, data.getStore().toFile().getAbsolutePath(),
                FileTrustStore.PASSWORD, TLS_RESOURCE.getSecret(),
                FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true,
                FileTrustStore.CERTIFICATE_REVOCATION_LIST_URL, data.getCrl().toFile().getPath());
        final FileTrustStore<?> fileTrustStore = createFileTrustStore(attributes);
        final TrustManager[] trustManagers = fileTrustStore.getTrustManagers();

        assertNotNull(trustManagers);
        assertEquals(1, trustManagers.length, "Unexpected number of trust managers");
        assertNotNull(trustManagers[0], "Trust manager unexpected null");
    }

    @Test
    public void testCreateTrustStoreFromFile_WrongPassword() throws Exception
    {
        final Path keyStoreFile = TLS_RESOURCE.createSelfSignedTrustStore(DN_FOO);
        final Map<String, Object> attributes = Map.of(FileTrustStore.NAME, NAME,
                FileTrustStore.STORE_URL, keyStoreFile.toFile().getAbsolutePath(),
                FileTrustStore.PASSWORD, TLS_RESOURCE.getSecret() + "_",
                FileTrustStore.TRUST_STORE_TYPE, TLS_RESOURCE.getKeyStoreType());

        KeyStoreTestHelper.checkExceptionThrownDuringKeyStoreCreation(FACTORY, BROKER, TrustStore.class, attributes,
                "Check trust store password");
    }

    @Test
    public void testCreateTrustStoreFromFile_MissingCrlFile() throws Exception
    {
        final Path keyStoreFile = TLS_RESOURCE.createSelfSignedTrustStore(DN_FOO);
        final Map<String, Object> attributes = Map.of(FileTrustStore.NAME, NAME,
                FileTrustStore.STORE_URL, keyStoreFile.toFile().getAbsolutePath(),
                FileTrustStore.PASSWORD, TLS_RESOURCE.getSecret(),
                FileTrustStore.TRUST_STORE_TYPE, TLS_RESOURCE.getKeyStoreType(),
                FileTrustStore.CERTIFICATE_REVOCATION_LIST_URL, NOT_A_CRL);

        KeyStoreTestHelper.checkExceptionThrownDuringKeyStoreCreation(FACTORY, BROKER, TrustStore.class, attributes,
                String.format("Unable to load certificate revocation list '%s' for truststore 'myFileTrustStore'", NOT_A_CRL));
    }

    @Test
    public void testCreatePeersOnlyTrustStoreFromFile_Success() throws Exception
    {
        final KeyCertificatePair keyPairAndRootCA = TlsResourceBuilder.createKeyPairAndRootCA(DN_CA);
        final Path keyStoreFile = TLS_RESOURCE.createTrustStore(DN_FOO, keyPairAndRootCA);
        final Map<String, Object> attributes = Map.of(FileTrustStore.NAME, NAME,
                FileTrustStore.STORE_URL, keyStoreFile.toFile().getAbsolutePath(),
                FileTrustStore.PASSWORD, TLS_RESOURCE.getSecret(),
                FileTrustStore.PEERS_ONLY, true,
                FileTrustStore.TRUST_STORE_TYPE, TLS_RESOURCE.getKeyStoreType());
        final FileTrustStore<?> fileTrustStore = createFileTrustStore(attributes);
        final TrustManager[] trustManagers = fileTrustStore.getTrustManagers();

        assertNotNull(trustManagers);
        assertEquals(1, trustManagers.length, "Unexpected number of trust managers");
        assertNotNull(trustManagers[0], "Trust manager unexpected null");
        final boolean condition = trustManagers[0] instanceof QpidPeersOnlyTrustManager;
        assertTrue(condition, "Trust manager unexpected null");
    }

    @Test
    public void testUseOfExpiredTrustAnchorAllowed() throws Exception
    {
        // https://www.ibm.com/support/knowledgecenter/en/SSYKE2_8.0.0/com.ibm.java.security.component.80.doc/security-
        assumeFalse(Objects.equals(getJvmVendor(), IBM), "IBMJSSE2 trust factory (IbmX509) validates the entire chain, including trusted certificates");

        final Path keyStoreFile = createTrustStoreWithExpiredCertificate();
        final Map<String, Object> attributes = Map.of(FileTrustStore.NAME, NAME,
                FileTrustStore.STORE_URL, keyStoreFile.toFile().getAbsolutePath(),
                FileTrustStore.PASSWORD, TLS_RESOURCE.getSecret(),
                FileTrustStore.TRUST_STORE_TYPE, TLS_RESOURCE.getKeyStoreType());
        final FileTrustStore<?> trustStore = createFileTrustStore(attributes);
        final TrustManager[] trustManagers = trustStore.getTrustManagers();

        assertNotNull(trustManagers);
        assertEquals(1, trustManagers.length, "Unexpected number of trust managers");
        final boolean condition = trustManagers[0] instanceof X509TrustManager;
        assertTrue(condition, "Unexpected trust manager type");

        final X509TrustManager trustManager = (X509TrustManager) trustManagers[0];
        final KeyStore clientStore = getInitializedKeyStore(keyStoreFile.toFile().getAbsolutePath(),
                                                      TLS_RESOURCE.getSecret(),
                                                      TLS_RESOURCE.getKeyStoreType());
        final String alias = clientStore.aliases().nextElement();
        final X509Certificate certificate = (X509Certificate) clientStore.getCertificate(alias);

        trustManager.checkClientTrusted(new X509Certificate[]{certificate}, "NULL");
    }

    @Test
    public void testUseOfExpiredTrustAnchorDenied() throws Exception
    {
        final Path keyStoreFile = createTrustStoreWithExpiredCertificate();
        final Map<String, Object> attributes = Map.of(FileTrustStore.NAME, NAME,
                FileTrustStore.TRUST_ANCHOR_VALIDITY_ENFORCED, true,
                FileTrustStore.STORE_URL, keyStoreFile.toFile().getAbsolutePath(),
                FileTrustStore.PASSWORD, TLS_RESOURCE.getSecret(),
                FileTrustStore.TRUST_STORE_TYPE, TLS_RESOURCE.getKeyStoreType());
        final TrustStore<?> trustStore = createFileTrustStore(attributes);
        final TrustManager[] trustManagers = trustStore.getTrustManagers();

        assertNotNull(trustManagers);
        assertEquals(1, trustManagers.length, "Unexpected number of trust managers");
        final boolean condition = trustManagers[0] instanceof X509TrustManager;
        assertTrue(condition, "Unexpected trust manager type");

        final X509TrustManager trustManager = (X509TrustManager) trustManagers[0];
        final KeyStore clientStore = getInitializedKeyStore(keyStoreFile.toFile().getAbsolutePath(),
                                                      TLS_RESOURCE.getSecret(),
                                                      TLS_RESOURCE.getKeyStoreType());
        final String alias = clientStore.aliases().nextElement();
        final X509Certificate certificate = (X509Certificate) clientStore.getCertificate(alias);
        final CertificateException thrown = assertThrows(
                CertificateException.class,
                () -> trustManager.checkClientTrusted(new X509Certificate[]{certificate}, "NULL"),
                "Exception not thrown");
        assertTrue(thrown instanceof CertificateExpiredException ||
                "Certificate expired".equals(thrown.getMessage()));
    }

    @Test
    public void testCreateTrustStoreFromDataUrl_Success() throws Exception
    {
        final StoreAndCrl<String> data = generateTrustStoreAndCrlAsDataUrl();
        final Map<String, Object> attributes = Map.of(FileTrustStore.NAME, NAME,
                FileTrustStore.STORE_URL, data.getStore(),
                FileTrustStore.PASSWORD, TLS_RESOURCE.getSecret(),
                FileTrustStore.TRUST_STORE_TYPE, TLS_RESOURCE.getKeyStoreType(),
                FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true,
                FileTrustStore.CERTIFICATE_REVOCATION_LIST_URL, data.getCrl());
        final FileTrustStore<?> fileTrustStore = createFileTrustStore(attributes);
        final TrustManager[] trustManagers = fileTrustStore.getTrustManagers();

        assertNotNull(trustManagers);
        assertEquals(1, trustManagers.length, "Unexpected number of trust managers");
        assertNotNull(trustManagers[0], "Trust manager unexpected null");
    }

    @Test
    public void testCreateTrustStoreFromDataUrl_WrongPassword() throws Exception
    {
        final String trustStoreAsDataUrl = TLS_RESOURCE.createSelfSignedTrustStoreAsDataUrl(DN_FOO);
        final Map<String, Object> attributes = Map.of(FileTrustStore.NAME, NAME,
                FileTrustStore.PASSWORD, TLS_RESOURCE.getSecret() + "_",
                FileTrustStore.STORE_URL, trustStoreAsDataUrl,
                FileTrustStore.TRUST_STORE_TYPE, TLS_RESOURCE.getKeyStoreType());

        KeyStoreTestHelper.checkExceptionThrownDuringKeyStoreCreation(FACTORY, BROKER, TrustStore.class, attributes,
            "Check trust store password");
    }

    @Test
    public void testCreateTrustStoreFromDataUrl_BadTruststoreBytes()
    {
        final String trustStoreAsDataUrl = DataUrlUtils.getDataUrlForBytes("notatruststore".getBytes());
        final Map<String, Object> attributes = Map.of(FileTrustStore.NAME, NAME,
                FileTrustStore.PASSWORD, TLS_RESOURCE.getSecret(),
                FileTrustStore.STORE_URL, trustStoreAsDataUrl,
                FileTrustStore.TRUST_STORE_TYPE, TLS_RESOURCE.getKeyStoreType());

        KeyStoreTestHelper.checkExceptionThrownDuringKeyStoreCreation(FACTORY, BROKER, TrustStore.class, attributes,
                "Cannot instantiate trust store");
    }

    @Test
    public void testUpdateTrustStore_Success() throws Exception
    {
        final StoreAndCrl<Path> data = generateTrustStoreAndCrl();
        final Map<String, Object> attributes = Map.of(FileTrustStore.NAME, NAME,
                FileTrustStore.STORE_URL, data.getStore().toFile().getAbsolutePath(),
                FileTrustStore.PASSWORD, TLS_RESOURCE.getSecret(),
                FileTrustStore.TRUST_STORE_TYPE, TLS_RESOURCE.getKeyStoreType(),
                FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true,
                FileTrustStore.CERTIFICATE_REVOCATION_LIST_URL, data.getCrl().toFile().getAbsolutePath());
        final FileTrustStore<?> fileTrustStore = createFileTrustStore(attributes);

        assertEquals(data.getStore().toFile().getAbsolutePath(), fileTrustStore.getStoreUrl(),
                "Unexpected path value before change");

        IllegalConfigurationException thrown = assertThrows(IllegalConfigurationException.class,
                () -> fileTrustStore.setAttributes(Map.of(FileTrustStore.STORE_URL, NOT_A_TRUSTSTORE)),
                "Exception not thrown");
        assertTrue(thrown.getMessage().contains("Cannot instantiate trust store"),
                "Exception text not as unexpected:" + thrown.getMessage());
        assertEquals(data.getStore().toFile().getAbsolutePath(), fileTrustStore.getStoreUrl(),
                "Unexpected keystore path value after failed change");

        thrown = assertThrows(IllegalConfigurationException.class,
                () -> fileTrustStore.setAttributes(Map.of(FileTrustStore.CERTIFICATE_REVOCATION_LIST_URL, NOT_A_CRL)),
                "Exception not thrown");
        assertTrue(thrown.getMessage().contains(
                String.format("Unable to load certificate revocation list '%s' for truststore '%s'", NOT_A_CRL, NAME)),
                "Exception text not as unexpected:" + thrown.getMessage());

        assertEquals(data.getCrl().toFile().getAbsolutePath(), fileTrustStore.getCertificateRevocationListUrl(),
                "Unexpected CRL path value after failed change");

        assertEquals(data.getStore().toFile().getAbsolutePath(), fileTrustStore.getStoreUrl(),
                "Unexpected path value after failed change");

        final Path keyStoreFile2 = TLS_RESOURCE.createTrustStore(DN_FOO, data.getCa());
        final Path emptyCrl = TLS_RESOURCE.createCrl(data.getCa());

        final Map<String, Object> changedAttributes = Map.of(FileTrustStore.STORE_URL, keyStoreFile2.toFile().getAbsolutePath(),
                FileTrustStore.PASSWORD, TLS_RESOURCE.getSecret(),
                FileTrustStore.CERTIFICATE_REVOCATION_LIST_URL, emptyCrl.toFile().getAbsolutePath());

        fileTrustStore.setAttributes(changedAttributes);

        assertEquals(keyStoreFile2.toFile().getAbsolutePath(), fileTrustStore.getStoreUrl(),
                "Unexpected keystore path value after change that is expected to be successful");
        assertEquals(emptyCrl.toFile().getAbsolutePath(), fileTrustStore.getCertificateRevocationListUrl(),
                "Unexpected CRL path value after change that is expected to be successful");
    }

    @Test
    public void testEmptyTrustStoreRejected() throws Exception
    {
        final Path path = TLS_RESOURCE.createKeyStore();
        final Map<String, Object> attributes = Map.of(FileKeyStore.NAME, NAME,
                FileKeyStore.PASSWORD, TLS_RESOURCE.getSecret(),
                FileKeyStore.STORE_URL, path.toFile().getAbsolutePath(),
                FileTrustStore.TRUST_STORE_TYPE, TLS_RESOURCE.getKeyStoreType());

        KeyStoreTestHelper.checkExceptionThrownDuringKeyStoreCreation(FACTORY, BROKER, TrustStore.class, attributes,
                "must contain at least one certificate");
    }

    @Test
    public void testTrustStoreWithNoCertificateRejected() throws Exception
    {
        final Path path = TLS_RESOURCE.createSelfSignedKeyStore(DN_FOO);
        final Map<String, Object> attributes = Map.of(FileTrustStore.NAME, getTestName(),
                FileTrustStore.PASSWORD, TLS_RESOURCE.getSecret(),
                FileTrustStore.STORE_URL, path.toFile().getAbsolutePath(),
                FileTrustStore.TRUST_STORE_TYPE, TLS_RESOURCE.getKeyStoreType());

        KeyStoreTestHelper.checkExceptionThrownDuringKeyStoreCreation(FACTORY, BROKER, TrustStore.class, attributes,
                "must contain at least one certificate");
    }

    @Test
    public void testSymmetricKeyEntryIgnored() throws Exception
    {
        final String keyStoreType = "jceks";
        final Path keyStoreFile = createSelfSignedKeyStoreWithSecretKeyAndCertificate(keyStoreType, DN_FOO);
        final Map<String, Object> attributes = Map.of(FileTrustStore.NAME, getTestName(),
                FileTrustStore.PASSWORD, TLS_RESOURCE.getSecret(),
                FileTrustStore.STORE_URL, keyStoreFile.toFile().getAbsolutePath(),
                FileTrustStore.TRUST_STORE_TYPE, keyStoreType);
        final FileTrustStore<?> trustStore = createFileTrustStore(attributes);
        final Certificate[] certificates = trustStore.getCertificates();

        assertEquals(getNumberOfCertificates(keyStoreFile, keyStoreType), (long) certificates.length,
                     "Unexpected number of certificates");
    }

    @Test
    public void testPrivateKeyEntryIgnored() throws Exception
    {
        final Path keyStoreFile = TLS_RESOURCE.createSelfSignedKeyStoreWithCertificate(DN_FOO);
        final Map<String, Object> attributes = Map.of(FileTrustStore.NAME, getTestName(),
                FileTrustStore.PASSWORD, TLS_RESOURCE.getSecret(),
                FileTrustStore.STORE_URL, keyStoreFile.toFile().getAbsolutePath(),
                FileTrustStore.TRUST_STORE_TYPE, TLS_RESOURCE.getKeyStoreType());
        final FileTrustStore<?> trustStore = createFileTrustStore(attributes);
        final Certificate[] certificates = trustStore.getCertificates();

        assertEquals(getNumberOfCertificates(keyStoreFile, TLS_RESOURCE.getKeyStoreType()), (long) certificates.length,
                     "Unexpected number of certificates");
    }

    @Test
    public void testReloadKeystore() throws Exception
    {
        final Path keyStorePath = TLS_RESOURCE.createSelfSignedKeyStoreWithCertificate(DN_FOO);
        final Path keyStorePath2 = TLS_RESOURCE.createSelfSignedKeyStoreWithCertificate(DN_BAR);
        final Map<String, Object> attributes = Map.of(FileTrustStore.NAME, getTestName(),
                FileTrustStore.STORE_URL, keyStorePath.toFile().getAbsolutePath(),
                FileTrustStore.PASSWORD, TLS_RESOURCE.getSecret());
        final FileTrustStore<?> trustStoreObject = createFileTrustStore(attributes);
        final X509Certificate certificate = getCertificate(trustStoreObject);
        assertEquals(DN_FOO, certificate.getIssuerX500Principal().getName());

        Files.copy(keyStorePath2, keyStorePath, StandardCopyOption.REPLACE_EXISTING);
        trustStoreObject.reload();

        final X509Certificate certificate2 = getCertificate(trustStoreObject);
        assertEquals(DN_BAR, certificate2.getIssuerX500Principal().getName());
    }

    @SuppressWarnings("unchecked")
    private FileTrustStore<?> createFileTrustStore(final Map<String, Object> attributes)
    {
        return (FileTrustStore<?>) FACTORY.create(TrustStore.class, attributes, BROKER);
    }

    private X509Certificate getCertificate(final FileTrustStore<?> trustStore)
            throws java.security.GeneralSecurityException
    {
        final Certificate[] certificates = trustStore.getCertificates();

        assertNotNull(certificates);
        assertEquals(1, certificates.length);

        final Certificate certificate = certificates[0];
        assertTrue(certificate instanceof X509Certificate);
        return (X509Certificate) certificate;
    }

    private int getNumberOfCertificates(Path keystore, String type) throws Exception
    {
        final KeyStore ks = KeyStore.getInstance(type);
        try (final InputStream is = new FileInputStream(keystore.toFile()))
        {
            ks.load(is, TLS_RESOURCE.getSecret().toCharArray());
        }

        int result = 0;
        final Enumeration<String> aliases = ks.aliases();
        while (aliases.hasMoreElements())
        {
            final String alias = aliases.nextElement();
            if (ks.isCertificateEntry(alias))
            {
                result++;
            }
        }
        return result;
    }

    private Path createTrustStoreWithExpiredCertificate() throws Exception
    {
        final Instant from = Instant.now().minus(10, ChronoUnit.DAYS);
        final Instant to = Instant.now().minus(5, ChronoUnit.DAYS);
        return TLS_RESOURCE.createSelfSignedTrustStore(DN_FOO, from, to);
    }

    public Path createSelfSignedKeyStoreWithSecretKeyAndCertificate(final String keyStoreType, final String dn)
            throws Exception
    {
        final KeyCertificatePair keyCertPair = TlsResourceBuilder.createSelfSigned(dn);
        return TLS_RESOURCE.createKeyStore(keyStoreType, new PrivateKeyEntry(TLS_RESOURCE.getPrivateKeyAlias(),
                                                                keyCertPair.getPrivateKey(),
                                                                keyCertPair.getCertificate()),
                              new CertificateEntry(TLS_RESOURCE.getCertificateAlias(), keyCertPair.getCertificate()),
                              new SecretKeyEntry(SECRET_KEY_ALIAS, TlsResourceHelper.createAESSecretKey()));
    }


    private StoreAndCrl<Path> generateTrustStoreAndCrl() throws Exception
    {
        final KeyCertificatePair caPair = TlsResourceBuilder.createKeyPairAndRootCA(DN_CA);
        final KeyCertificatePair keyCertPair1 = TlsResourceBuilder.createKeyPairAndCertificate(DN_FOO, caPair);
        final KeyCertificatePair keyCertPair2 = TlsResourceBuilder.createKeyPairAndCertificate(DN_BAR, caPair);
        final Path keyStoreFile = TLS_RESOURCE.createKeyStore(new CertificateEntry(CERTIFICATE_ALIAS_A,
                                                                           keyCertPair1.getCertificate()),
                                                                   new CertificateEntry(CERTIFICATE_ALIAS_B,
                                                                           keyCertPair2.getCertificate()));

        final Path clrFile = TLS_RESOURCE.createCrl(caPair, keyCertPair2.getCertificate());
        return new StoreAndCrl<>(keyStoreFile, clrFile, caPair);
    }

    private StoreAndCrl<String> generateTrustStoreAndCrlAsDataUrl() throws Exception
    {
        final KeyCertificatePair caPair = TlsResourceBuilder.createKeyPairAndRootCA(DN_CA);
        final KeyCertificatePair keyCertPair1 = TlsResourceBuilder.createKeyPairAndCertificate(DN_FOO, caPair);
        final KeyCertificatePair keyCertPair2 = TlsResourceBuilder.createKeyPairAndCertificate(DN_BAR, caPair);
        final String trustStoreAsDataUrl =
                TLS_RESOURCE.createKeyStoreAsDataUrl(new CertificateEntry(CERTIFICATE_ALIAS_A, keyCertPair1.getCertificate()),
                        new CertificateEntry(CERTIFICATE_ALIAS_B, keyCertPair2.getCertificate()));

        final String crlAsDataUrl = TLS_RESOURCE.createCrlAsDataUrl(caPair, keyCertPair2.getCertificate());
        return new StoreAndCrl<>(trustStoreAsDataUrl, crlAsDataUrl, caPair);
    }

    private static class StoreAndCrl<T>
    {
        private final T _store;
        private final T _crl;
        private final KeyCertificatePair _ca;

        private StoreAndCrl(final T store, final T crl, final KeyCertificatePair ca)
        {
            _store = store;
            _crl = crl;
            _ca = ca;
        }

        T getStore()
        {
            return _store;
        }

        T getCrl()
        {
            return _crl;
        }

        KeyCertificatePair getCa()
        {
            return _ca;
        }
    }
}
