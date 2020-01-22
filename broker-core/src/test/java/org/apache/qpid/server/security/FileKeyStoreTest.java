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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.net.ssl.KeyManager;

import org.junit.ClassRule;
import org.junit.Test;

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
import org.apache.qpid.test.utils.tls.TlsResourceHelper;

public class FileKeyStoreTest extends UnitTestBase
{
    @ClassRule
    public static final TlsResource TLS_RESOURCE = new TlsResource();

    private static final Broker BROKER = BrokerTestHelper.createBrokerMock();
    private static final ConfiguredObjectFactory FACTORY = BrokerModel.getInstance().getObjectFactory();
    private static final String DN_FOO = "CN=foo";
    private static final String DN_BAR = "CN=bar";
    private static final String NAME = "myFileKeyStore";
    private static final String SECRET_KEY_ALIAS = "secret-key-alias";

    @Test
    public void testCreateKeyStoreFromFile_Success() throws Exception
    {
        final Path keyStoreFile = TLS_RESOURCE.createSelfSignedKeyStore(DN_FOO);
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(FileKeyStore.NAME, NAME);
        attributes.put(FileKeyStore.STORE_URL, keyStoreFile.toFile().getAbsolutePath());
        attributes.put(FileKeyStore.PASSWORD, TLS_RESOURCE.getSecret());
        attributes.put(FileKeyStore.KEY_STORE_TYPE, TLS_RESOURCE.getKeyStoreType());

        final KeyStore<?> fileKeyStore = createFileKeyStore(attributes);

        KeyManager[] keyManager = fileKeyStore.getKeyManagers();
        assertNotNull(keyManager);
        assertEquals("Unexpected number of key managers", 1, keyManager.length);
        assertNotNull("Key manager unexpected null", keyManager[0]);
    }

    @Test
    public void testCreateKeyStoreWithAliasFromFile_Success() throws Exception
    {
        final Path keyStoreFile = TLS_RESOURCE.createSelfSignedKeyStore(DN_FOO);
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(FileKeyStore.NAME, NAME);
        attributes.put(FileKeyStore.STORE_URL, keyStoreFile.toFile().getAbsolutePath());
        attributes.put(FileKeyStore.PASSWORD, TLS_RESOURCE.getSecret());
        attributes.put(FileKeyStore.CERTIFICATE_ALIAS, TLS_RESOURCE.getPrivateKeyAlias());
        attributes.put(FileKeyStore.KEY_STORE_TYPE, TLS_RESOURCE.getKeyStoreType());

        final KeyStore<?> fileKeyStore = createFileKeyStore(attributes);

        KeyManager[] keyManager = fileKeyStore.getKeyManagers();
        assertNotNull(keyManager);
        assertEquals("Unexpected number of key managers", 1, keyManager.length);
        assertNotNull("Key manager unexpected null", keyManager[0]);
    }

    @Test
    public void testCreateKeyStoreFromFile_WrongPassword() throws Exception
    {
        final Path keyStoreFile = TLS_RESOURCE.createSelfSignedKeyStore(DN_FOO);
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(FileKeyStore.NAME, NAME);
        attributes.put(FileKeyStore.STORE_URL, keyStoreFile.toFile().getAbsolutePath());
        attributes.put(FileKeyStore.PASSWORD, TLS_RESOURCE.getSecret() + "_");
        attributes.put(FileKeyStore.KEY_STORE_TYPE, TLS_RESOURCE.getKeyStoreType());

        KeyStoreTestHelper.checkExceptionThrownDuringKeyStoreCreation(FACTORY,
                                                                      BROKER,
                                                                      KeyStore.class, attributes,
                                                                      "Check key store password");
    }

    @Test
    public void testCreateKeyStoreFromFile_UnknownAlias() throws Exception
    {
        final Path keyStoreFile = TLS_RESOURCE.createSelfSignedKeyStore(DN_FOO);
        final String unknownAlias = TLS_RESOURCE.getPrivateKeyAlias() + "_";
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(FileKeyStore.NAME, NAME);
        attributes.put(FileKeyStore.STORE_URL, keyStoreFile.toFile().getAbsolutePath());
        attributes.put(FileKeyStore.PASSWORD, TLS_RESOURCE.getSecret());
        attributes.put(FileKeyStore.CERTIFICATE_ALIAS, unknownAlias);
        attributes.put(FileKeyStore.KEY_STORE_TYPE, TLS_RESOURCE.getKeyStoreType());

        KeyStoreTestHelper.checkExceptionThrownDuringKeyStoreCreation(FACTORY,
                                                                      BROKER,
                                                                      KeyStore.class,
                                                                      attributes,
                                                                      String.format(
                                                                              "Cannot find a certificate with alias '%s' in key store",
                                                                              unknownAlias));
    }

    @Test
    public void testCreateKeyStoreFromFile_NonKeyAlias() throws Exception
    {
        final Path keyStoreFile = TLS_RESOURCE.createSelfSignedTrustStore(DN_FOO);
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(FileKeyStore.NAME, NAME);
        attributes.put(FileKeyStore.STORE_URL, keyStoreFile.toFile().getAbsolutePath());
        attributes.put(FileKeyStore.PASSWORD, TLS_RESOURCE.getSecret());
        attributes.put(FileKeyStore.CERTIFICATE_ALIAS, TLS_RESOURCE.getCertificateAlias());
        attributes.put(FileKeyStore.KEY_STORE_TYPE, TLS_RESOURCE.getKeyStoreType());

        KeyStoreTestHelper.checkExceptionThrownDuringKeyStoreCreation(FACTORY, BROKER, KeyStore.class, attributes,
                                                                      "does not identify a private key");
    }

    @Test
    public void testCreateKeyStoreFromDataUrl_Success() throws Exception
    {
        final String keyStoreAsDataUrl = TLS_RESOURCE.createSelfSignedKeyStoreAsDataUrl(DN_FOO);

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(FileKeyStore.NAME, NAME);
        attributes.put(FileKeyStore.STORE_URL, keyStoreAsDataUrl);
        attributes.put(FileKeyStore.PASSWORD, TLS_RESOURCE.getSecret());
        attributes.put(FileKeyStore.KEY_STORE_TYPE, TLS_RESOURCE.getKeyStoreType());

        final KeyStore<?> fileKeyStore = createFileKeyStore(attributes);

        KeyManager[] keyManagers = fileKeyStore.getKeyManagers();
        assertNotNull(keyManagers);
        assertEquals("Unexpected number of key managers", 1, keyManagers.length);
        assertNotNull("Key manager unexpected null", keyManagers[0]);
    }

    @Test
    public void testCreateKeyStoreWithAliasFromDataUrl_Success() throws Exception
    {
        final String keyStoreAsDataUrl = TLS_RESOURCE.createSelfSignedKeyStoreAsDataUrl(DN_FOO);

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(FileKeyStore.NAME, NAME);
        attributes.put(FileKeyStore.STORE_URL, keyStoreAsDataUrl);
        attributes.put(FileKeyStore.PASSWORD, TLS_RESOURCE.getSecret());
        attributes.put(FileKeyStore.CERTIFICATE_ALIAS, TLS_RESOURCE.getPrivateKeyAlias());
        attributes.put(FileKeyStore.KEY_STORE_TYPE, TLS_RESOURCE.getKeyStoreType());

        final KeyStore<?> fileKeyStore = createFileKeyStore(attributes);

        KeyManager[] keyManagers = fileKeyStore.getKeyManagers();
        assertNotNull(keyManagers);
        assertEquals("Unexpected number of key managers", 1, keyManagers.length);
        assertNotNull("Key manager unexpected null", keyManagers[0]);
    }

    @Test
    public void testCreateKeyStoreFromDataUrl_WrongPassword() throws Exception
    {
        final String keyStoreAsDataUrl = TLS_RESOURCE.createSelfSignedKeyStoreAsDataUrl(DN_FOO);

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(FileKeyStore.NAME, NAME);
        attributes.put(FileKeyStore.PASSWORD, TLS_RESOURCE.getSecret() + "_");
        attributes.put(FileKeyStore.STORE_URL, keyStoreAsDataUrl);

        KeyStoreTestHelper.checkExceptionThrownDuringKeyStoreCreation(FACTORY, BROKER, KeyStore.class, attributes,
                                                                      "Check key store password");
    }

    @Test
    public void testCreateKeyStoreFromDataUrl_BadKeystoreBytes()
    {
        String keyStoreAsDataUrl = DataUrlUtils.getDataUrlForBytes("notatruststore".getBytes());

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(FileKeyStore.NAME, NAME);
        attributes.put(FileKeyStore.PASSWORD, TLS_RESOURCE.getSecret());
        attributes.put(FileKeyStore.STORE_URL, keyStoreAsDataUrl);

        KeyStoreTestHelper.checkExceptionThrownDuringKeyStoreCreation(FACTORY, BROKER, KeyStore.class, attributes,
                                                                      "Cannot instantiate key store");
    }

    @Test
    public void testCreateKeyStoreFromDataUrl_UnknownAlias() throws Exception
    {
        final String keyStoreAsDataUrl = TLS_RESOURCE.createSelfSignedKeyStoreAsDataUrl(DN_FOO);
        final String unknownAlias = TLS_RESOURCE.getPrivateKeyAlias() + "_";

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(FileKeyStore.NAME, NAME);
        attributes.put(FileKeyStore.PASSWORD, TLS_RESOURCE.getSecret());
        attributes.put(FileKeyStore.STORE_URL, keyStoreAsDataUrl);
        attributes.put(FileKeyStore.CERTIFICATE_ALIAS, unknownAlias);
        attributes.put(FileKeyStore.KEY_STORE_TYPE, TLS_RESOURCE.getKeyStoreType());

        KeyStoreTestHelper.checkExceptionThrownDuringKeyStoreCreation(FACTORY,
                                                                      BROKER,
                                                                      KeyStore.class,
                                                                      attributes,
                                                                      String.format(
                                                                              "Cannot find a certificate with alias '%s' in key store",
                                                                              unknownAlias));
    }

    @Test
    public void testEmptyKeystoreRejected() throws Exception
    {
        final Path keyStoreFile = TLS_RESOURCE.createKeyStore();

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(FileKeyStore.NAME, NAME);
        attributes.put(FileKeyStore.PASSWORD, TLS_RESOURCE.getSecret());
        attributes.put(FileKeyStore.STORE_URL, keyStoreFile.toFile().getAbsolutePath());

        KeyStoreTestHelper.checkExceptionThrownDuringKeyStoreCreation(FACTORY,
                                                                      BROKER,
                                                                      KeyStore.class,
                                                                      attributes,
                                                                      "must contain at least one private key");
    }

    @Test
    public void testKeystoreWithNoPrivateKeyRejected() throws Exception
    {
        final Path keyStoreFile = TLS_RESOURCE.createSelfSignedTrustStore(DN_FOO);

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(FileKeyStore.NAME, getTestName());
        attributes.put(FileKeyStore.PASSWORD, TLS_RESOURCE.getSecret());
        attributes.put(FileKeyStore.STORE_URL, keyStoreFile.toFile().getAbsolutePath());
        attributes.put(FileKeyStore.KEY_STORE_TYPE, TLS_RESOURCE.getKeyStoreType());

        KeyStoreTestHelper.checkExceptionThrownDuringKeyStoreCreation(FACTORY, BROKER, KeyStore.class, attributes,
                                                                      "must contain at least one private key");
    }

    @Test
    public void testSymmetricKeysIgnored() throws Exception
    {
        final String keyStoreType = "jceks"; // or jks
        final Path keyStoreFile = createSelfSignedKeyStoreWithSecretKeyAndCertificate(keyStoreType, DN_FOO);

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(FileKeyStore.NAME, NAME);
        attributes.put(FileKeyStore.PASSWORD, TLS_RESOURCE.getSecret());
        attributes.put(FileKeyStore.STORE_URL, keyStoreFile);
        attributes.put(FileKeyStore.KEY_STORE_TYPE, keyStoreType);

        KeyStore<?> keyStore = createFileKeyStore(attributes);
        assertNotNull(keyStore);
    }

    @Test
    public void testUpdateKeyStore_Success() throws Exception
    {
        final Path keyStoreFile = TLS_RESOURCE.createSelfSignedKeyStore(DN_FOO);
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(FileKeyStore.NAME, NAME);

        attributes.put(FileKeyStore.STORE_URL, keyStoreFile.toFile().getAbsolutePath());
        attributes.put(FileKeyStore.PASSWORD, TLS_RESOURCE.getSecret());
        attributes.put(FileKeyStore.KEY_STORE_TYPE, TLS_RESOURCE.getKeyStoreType());

        final FileKeyStore<?> fileKeyStore = createFileKeyStore(attributes);

        assertNull("Unexpected alias value before change", fileKeyStore.getCertificateAlias());

        String unknownAlias = TLS_RESOURCE.getSecret() + "_";
        Map<String, Object> unacceptableAttributes = new HashMap<>();
        unacceptableAttributes.put(FileKeyStore.CERTIFICATE_ALIAS, unknownAlias);
        try
        {
            fileKeyStore.setAttributes(unacceptableAttributes);
            fail("Exception not thrown");
        }
        catch (IllegalConfigurationException e)
        {
            String message = e.getMessage();
            assertTrue("Exception text not as unexpected:" + message,
                       message.contains(String.format("Cannot find a certificate with alias '%s' in key store",
                                                      unknownAlias)));
        }

        assertNull("Unexpected alias value after failed change", fileKeyStore.getCertificateAlias());

        Map<String, Object> changedAttributes = new HashMap<>();
        changedAttributes.put(FileKeyStore.CERTIFICATE_ALIAS, TLS_RESOURCE.getPrivateKeyAlias());

        fileKeyStore.setAttributes(changedAttributes);

        assertEquals("Unexpected alias value after change that is expected to be successful",
                     TLS_RESOURCE.getPrivateKeyAlias(),
                     fileKeyStore.getCertificateAlias());
    }

    @Test
    public void testReloadKeystore() throws Exception
    {
        final Path keyStorePath = TLS_RESOURCE.createSelfSignedKeyStoreWithCertificate(DN_FOO);
        final Path keyStorePath2 = TLS_RESOURCE.createSelfSignedKeyStoreWithCertificate(DN_BAR);
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put(FileKeyStore.NAME, getTestName());
        attributes.put(FileKeyStore.STORE_URL, keyStorePath.toFile().getAbsolutePath());
        attributes.put(FileKeyStore.PASSWORD, TLS_RESOURCE.getSecret());

        final FileKeyStore<?> keyStoreObject = createFileKeyStore(attributes);

        final CertificateDetails certificate = getCertificate(keyStoreObject);
        assertEquals(DN_FOO, certificate.getIssuerName());

        Files.copy(keyStorePath2, keyStorePath, StandardCopyOption.REPLACE_EXISTING);

        keyStoreObject.reload();

        final CertificateDetails certificate2 = getCertificate(keyStoreObject);
        assertEquals(DN_BAR, certificate2.getIssuerName());
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
}
