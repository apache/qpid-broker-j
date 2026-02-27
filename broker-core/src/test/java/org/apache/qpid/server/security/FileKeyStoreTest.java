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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;

import javax.net.ssl.KeyManager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.ConfiguredObjectFactory;
import org.apache.qpid.server.model.KeyStore;
import org.apache.qpid.test.utils.tls.CertificateEntry;
import org.apache.qpid.test.utils.tls.KeyCertificatePair;
import org.apache.qpid.test.utils.tls.PrivateKeyEntry;
import org.apache.qpid.test.utils.tls.SecretKeyEntry;
import org.apache.qpid.test.utils.tls.TlsResource;
import org.apache.qpid.server.util.DataUrlUtils;
import org.apache.qpid.test.utils.UnitTestBase;
import org.apache.qpid.test.utils.tls.TlsResourceBuilder;
import org.apache.qpid.test.utils.tls.TlsResourceExtension;
import org.apache.qpid.test.utils.tls.TlsResourceHelper;

@ExtendWith({ TlsResourceExtension.class })
public class FileKeyStoreTest extends UnitTestBase
{
    private static final Broker<?> BROKER = BrokerTestHelper.createBrokerMock();
    private static final ConfiguredObjectFactory FACTORY = BrokerModel.getInstance().getObjectFactory();
    private static final String DN_FOO = "CN=foo";
    private static final String DN_BAR = "CN=bar";
    private static final String NAME = "myFileKeyStore";
    private static final String SECRET_KEY_ALIAS = "secret-key-alias";

    @Test
    void testCreateKeyStoreFromFile_Success(final TlsResource tls) throws Exception
    {
        final Path keyStoreFile = tls.createSelfSignedKeyStore(DN_FOO);
        final Map<String, Object> attributes = Map.of(FileKeyStore.NAME, NAME,
                FileKeyStore.STORE_URL, keyStoreFile.toFile().getAbsolutePath(),
                FileKeyStore.PASSWORD, tls.getSecret(),
                FileKeyStore.KEY_STORE_TYPE, tls.getKeyStoreType());
        final KeyStore<?> fileKeyStore = createFileKeyStore(attributes);
        final KeyManager[] keyManager = fileKeyStore.getKeyManagers();

        assertNotNull(keyManager);
        assertEquals(1, keyManager.length, "Unexpected number of key managers");
        assertNotNull(keyManager[0], "Key manager unexpected null");
    }

    @Test
    void testCreateKeyStoreWithAliasFromFile_Success(final TlsResource tls) throws Exception
    {
        final Path keyStoreFile = tls.createSelfSignedKeyStore(DN_FOO);
        final Map<String, Object> attributes = Map.of(FileKeyStore.NAME, NAME,
                FileKeyStore.STORE_URL, keyStoreFile.toFile().getAbsolutePath(),
                FileKeyStore.PASSWORD, tls.getSecret(),
                FileKeyStore.CERTIFICATE_ALIAS, tls.getPrivateKeyAlias(),
                FileKeyStore.KEY_STORE_TYPE, tls.getKeyStoreType());
        final KeyStore<?> fileKeyStore = createFileKeyStore(attributes);
        final KeyManager[] keyManager = fileKeyStore.getKeyManagers();
        assertNotNull(keyManager);
        assertEquals(1, keyManager.length, "Unexpected number of key managers");
        assertNotNull(keyManager[0], "Key manager unexpected null");
    }

    @Test
    void testCreateKeyStoreFromFile_WrongPassword(final TlsResource tls) throws Exception
    {
        final Path keyStoreFile = tls.createSelfSignedKeyStore(DN_FOO);
        final Map<String, Object> attributes = Map.of(FileKeyStore.NAME, NAME,
                FileKeyStore.STORE_URL, keyStoreFile.toFile().getAbsolutePath(),
                FileKeyStore.PASSWORD, tls.getSecret() + "_",
                FileKeyStore.KEY_STORE_TYPE, tls.getKeyStoreType());
        KeyStoreTestHelper.checkExceptionThrownDuringKeyStoreCreation(FACTORY, BROKER, KeyStore.class, attributes,
                "Check key store password");
    }

    @Test
    void testCreateKeyStoreFromFile_UnknownAlias(final TlsResource tls) throws Exception
    {
        final Path keyStoreFile = tls.createSelfSignedKeyStore(DN_FOO);
        final String unknownAlias = tls.getPrivateKeyAlias() + "_";
        final Map<String, Object> attributes = Map.of(FileKeyStore.NAME, NAME,
                FileKeyStore.STORE_URL, keyStoreFile.toFile().getAbsolutePath(),
                FileKeyStore.PASSWORD, tls.getSecret(),
                FileKeyStore.CERTIFICATE_ALIAS, unknownAlias,
                FileKeyStore.KEY_STORE_TYPE, tls.getKeyStoreType());

        KeyStoreTestHelper.checkExceptionThrownDuringKeyStoreCreation(FACTORY, BROKER, KeyStore.class, attributes,
                String.format("Cannot find a certificate with alias '%s' in key store", unknownAlias));
    }

    @Test
    void testCreateKeyStoreFromFile_NonKeyAlias(final TlsResource tls) throws Exception
    {
        final Path keyStoreFile = tls.createSelfSignedTrustStore(DN_FOO);
        final Map<String, Object> attributes = Map.of(FileKeyStore.NAME, NAME,
                FileKeyStore.STORE_URL, keyStoreFile.toFile().getAbsolutePath(),
                FileKeyStore.PASSWORD, tls.getSecret(),
                FileKeyStore.CERTIFICATE_ALIAS, tls.getCertificateAlias(),
                FileKeyStore.KEY_STORE_TYPE, tls.getKeyStoreType());

        KeyStoreTestHelper.checkExceptionThrownDuringKeyStoreCreation(FACTORY, BROKER, KeyStore.class, attributes,
                "does not identify a private key");
    }

    @Test
    void testCreateKeyStoreFromDataUrl_Success(final TlsResource tls) throws Exception
    {
        final String keyStoreAsDataUrl = tls.createSelfSignedKeyStoreAsDataUrl(DN_FOO);
        final Map<String, Object> attributes = Map.of(FileKeyStore.NAME, NAME,
                FileKeyStore.STORE_URL, keyStoreAsDataUrl,
                FileKeyStore.PASSWORD, tls.getSecret(),
                FileKeyStore.KEY_STORE_TYPE, tls.getKeyStoreType());
        final KeyStore<?> fileKeyStore = createFileKeyStore(attributes);
        final KeyManager[] keyManagers = fileKeyStore.getKeyManagers();
        assertNotNull(keyManagers);
        assertEquals(1, keyManagers.length, "Unexpected number of key managers");
        assertNotNull(keyManagers[0], "Key manager unexpected null");
    }

    @Test
    void testCreateKeyStoreWithAliasFromDataUrl_Success(final TlsResource tls) throws Exception
    {
        final String keyStoreAsDataUrl = tls.createSelfSignedKeyStoreAsDataUrl(DN_FOO);
        final Map<String, Object> attributes = Map.of(FileKeyStore.NAME, NAME,
                FileKeyStore.STORE_URL, keyStoreAsDataUrl,
                FileKeyStore.PASSWORD, tls.getSecret(),
                FileKeyStore.CERTIFICATE_ALIAS, tls.getPrivateKeyAlias(),
                FileKeyStore.KEY_STORE_TYPE, tls.getKeyStoreType());
        final KeyStore<?> fileKeyStore = createFileKeyStore(attributes);
        final KeyManager[] keyManagers = fileKeyStore.getKeyManagers();
        assertNotNull(keyManagers);
        assertEquals(1, keyManagers.length, "Unexpected number of key managers");
        assertNotNull(keyManagers[0], "Key manager unexpected null");
    }

    @Test
    void testCreateKeyStoreFromDataUrl_WrongPassword(final TlsResource tls) throws Exception
    {
        final String keyStoreAsDataUrl = tls.createSelfSignedKeyStoreAsDataUrl(DN_FOO);
        final Map<String, Object> attributes = Map.of(FileKeyStore.NAME, NAME,
                FileKeyStore.PASSWORD, tls.getSecret() + "_",
                FileKeyStore.STORE_URL, keyStoreAsDataUrl);

        KeyStoreTestHelper.checkExceptionThrownDuringKeyStoreCreation(FACTORY, BROKER, KeyStore.class, attributes,
                "Check key store password");
    }

    @Test
    void testCreateKeyStoreFromDataUrl_BadKeystoreBytes(final TlsResource tls)
    {
        final String keyStoreAsDataUrl = DataUrlUtils.getDataUrlForBytes("notatruststore".getBytes());
        final Map<String, Object> attributes = Map.of(FileKeyStore.NAME, NAME,
                FileKeyStore.PASSWORD, tls.getSecret(),
                FileKeyStore.STORE_URL, keyStoreAsDataUrl);

        KeyStoreTestHelper.checkExceptionThrownDuringKeyStoreCreation(FACTORY, BROKER, KeyStore.class, attributes,
                "Cannot instantiate key store");
    }

    @Test
    void testCreateKeyStoreFromDataUrl_UnknownAlias(final TlsResource tls) throws Exception
    {
        final String keyStoreAsDataUrl = tls.createSelfSignedKeyStoreAsDataUrl(DN_FOO);
        final String unknownAlias = tls.getPrivateKeyAlias() + "_";
        final Map<String, Object> attributes = Map.of(FileKeyStore.NAME, NAME,
                FileKeyStore.PASSWORD, tls.getSecret(),
                FileKeyStore.STORE_URL, keyStoreAsDataUrl,
                FileKeyStore.CERTIFICATE_ALIAS, unknownAlias,
                FileKeyStore.KEY_STORE_TYPE, tls.getKeyStoreType());

        KeyStoreTestHelper.checkExceptionThrownDuringKeyStoreCreation(FACTORY, BROKER, KeyStore.class, attributes,
                String.format("Cannot find a certificate with alias '%s' in key store", unknownAlias));
    }

    @Test
    void testEmptyKeystoreRejected(final TlsResource tls) throws Exception
    {
        final Path keyStoreFile = tls.createKeyStore();
        final Map<String, Object> attributes = Map.of(FileKeyStore.NAME, NAME,
                FileKeyStore.PASSWORD, tls.getSecret(),
                FileKeyStore.STORE_URL, keyStoreFile.toFile().getAbsolutePath());

        KeyStoreTestHelper.checkExceptionThrownDuringKeyStoreCreation(FACTORY, BROKER, KeyStore.class, attributes,
                "must contain at least one private key");
    }

    @Test
    void testKeystoreWithNoPrivateKeyRejected(final TlsResource tls) throws Exception
    {
        final Path keyStoreFile = tls.createSelfSignedTrustStore(DN_FOO);
        final Map<String, Object> attributes = Map.of(FileKeyStore.NAME, getTestName(),
                FileKeyStore.PASSWORD, tls.getSecret(),
                FileKeyStore.STORE_URL, keyStoreFile.toFile().getAbsolutePath(),
                FileKeyStore.KEY_STORE_TYPE, tls.getKeyStoreType());

        KeyStoreTestHelper.checkExceptionThrownDuringKeyStoreCreation(FACTORY, BROKER, KeyStore.class, attributes,
                "must contain at least one private key");
    }

    @Test
    void testSymmetricKeysIgnored(final TlsResource tls) throws Exception
    {
        final String keyStoreType = "jceks"; // or jks
        final Path keyStoreFile = createSelfSignedKeyStoreWithSecretKeyAndCertificate(keyStoreType, DN_FOO, tls);
        final Map<String, Object> attributes = Map.of(FileKeyStore.NAME, NAME,
                FileKeyStore.PASSWORD, tls.getSecret(),
                FileKeyStore.STORE_URL, keyStoreFile,
                FileKeyStore.KEY_STORE_TYPE, keyStoreType);
        final KeyStore<?> keyStore = createFileKeyStore(attributes);
        assertNotNull(keyStore);
    }

    @Test
    void testUpdateKeyStore_Success(final TlsResource tls) throws Exception
    {
        final Path keyStoreFile = tls.createSelfSignedKeyStore(DN_FOO);
        final Map<String, Object> attributes = Map.of(FileKeyStore.NAME, NAME,
                FileKeyStore.STORE_URL, keyStoreFile.toFile().getAbsolutePath(),
                FileKeyStore.PASSWORD, tls.getSecret(),
                FileKeyStore.KEY_STORE_TYPE, tls.getKeyStoreType());
        final FileKeyStore<?> fileKeyStore = createFileKeyStore(attributes);

        assertNull(fileKeyStore.getCertificateAlias(), "Unexpected alias value before change");

        final String unknownAlias = tls.getSecret() + "_";
        final Map<String, Object> unacceptableAttributes = Map.of(FileKeyStore.CERTIFICATE_ALIAS, unknownAlias);
        final IllegalConfigurationException thrown = assertThrows(IllegalConfigurationException.class,
                () -> fileKeyStore.setAttributes(unacceptableAttributes),
                "Exception not thrown");

        assertTrue(thrown.getMessage().contains(
                String.format("Cannot find a certificate with alias '%s' in key store", unknownAlias)),
                "Exception text not as unexpected:" + thrown.getMessage());
        assertNull(fileKeyStore.getCertificateAlias(), "Unexpected alias value after failed change");

        final Map<String, Object> changedAttributes = Map.of(FileKeyStore.CERTIFICATE_ALIAS, tls.getPrivateKeyAlias());
        fileKeyStore.setAttributes(changedAttributes);

        assertEquals(tls.getPrivateKeyAlias(), fileKeyStore.getCertificateAlias(),
                     "Unexpected alias value after change that is expected to be successful");
    }

    @Test
    void testReloadKeystore(final TlsResource tls) throws Exception
    {
        final Path keyStorePath = tls.createSelfSignedKeyStoreWithCertificate(DN_FOO);
        final Path keyStorePath2 = tls.createSelfSignedKeyStoreWithCertificate(DN_BAR);
        final Map<String, Object> attributes = Map.of(FileKeyStore.NAME, getTestName(),
                FileKeyStore.STORE_URL, keyStorePath.toFile().getAbsolutePath(),
                FileKeyStore.PASSWORD, tls.getSecret());
        final FileKeyStore<?> keyStoreObject = createFileKeyStore(attributes);
        final CertificateDetails certificate = getCertificate(keyStoreObject);
        assertEquals(DN_FOO, certificate.getIssuerName());

        Files.copy(keyStorePath2, keyStorePath, StandardCopyOption.REPLACE_EXISTING);

        keyStoreObject.reload();

        final CertificateDetails certificate2 = getCertificate(keyStoreObject);
        assertEquals(DN_BAR, certificate2.getIssuerName());
    }

    @Test
    void privateKeyEntryCertificate(final TlsResource tls) throws Exception
    {
        final Path keyStoreFile = tls.createSelfSignedKeyStoreWithCertificate(DN_FOO);
        final Map<String, Object> attributes = Map.of(FileKeyStore.NAME, getTestName(),
                FileKeyStore.PASSWORD, tls.getSecret(),
                FileKeyStore.STORE_URL, keyStoreFile.toFile().getAbsolutePath(),
                FileKeyStore.KEY_STORE_TYPE, tls.getKeyStoreType());
        final FileKeyStore<?> keyStore = createFileKeyStore(attributes);
        final List<CertificateDetails> certificateDetails = keyStore.getCertificateDetails();

        final int keyCertificates = KeyStoreTestHelper
                .getNumberOfCertificates(keyStoreFile, "PKCS12", tls.getSecret().toCharArray(), true);
        assertEquals(keyCertificates, certificateDetails.size(), "Unexpected number of certificates");
        assertEquals("private-key-alias", certificateDetails.get(0).getAlias(), "Unexpected alias name");
    }

    @SuppressWarnings("unchecked")
    private FileKeyStore<?> createFileKeyStore(final Map<String, Object> attributes)
    {
        return (FileKeyStore<?>) FACTORY.create(KeyStore.class, attributes, BROKER);
    }

    private CertificateDetails getCertificate(final FileKeyStore<?> keyStore)
    {
        final List<CertificateDetails> certificates = keyStore.getCertificateDetails();

        assertNotNull(certificates);
        assertEquals(1, certificates.size());

        return certificates.get(0);
    }


    public Path createSelfSignedKeyStoreWithSecretKeyAndCertificate(final String keyStoreType, final String dn, final TlsResource tls)
            throws Exception
    {
        final KeyCertificatePair keyCertPair = TlsResourceBuilder.createSelfSigned(dn);
        final PrivateKeyEntry privateKeyEntry = new PrivateKeyEntry(tls.getPrivateKeyAlias(), keyCertPair);
        final CertificateEntry certificateEntry = new CertificateEntry(tls.getCertificateAlias(), keyCertPair.certificate());
        final SecretKeyEntry secretKeyEntry = new SecretKeyEntry(SECRET_KEY_ALIAS, TlsResourceHelper.createAESSecretKey());
        return tls.createKeyStore(keyStoreType, privateKeyEntry, certificateEntry, secretKeyEntry);
    }
}
