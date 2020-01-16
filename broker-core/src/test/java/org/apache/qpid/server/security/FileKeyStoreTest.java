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


import static org.apache.qpid.server.security.FileTrustStoreTest.createDataUrlForFile;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.net.ssl.KeyManager;

import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.ConfiguredObjectFactory;
import org.apache.qpid.test.utils.UnitTestBase;
import org.junit.Test;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.KeyStore;
import org.apache.qpid.server.transport.network.security.ssl.SSLUtil;
import org.apache.qpid.server.util.DataUrlUtils;
import org.apache.qpid.test.utils.TestFileUtils;
import org.apache.qpid.test.utils.TestSSLConstants;

public class FileKeyStoreTest extends UnitTestBase
{
    private static final Broker BROKER = BrokerTestHelper.createBrokerMock();
    private static final ConfiguredObjectFactory FACTORY = BrokerModel.getInstance().getObjectFactory();

    @Test
    public void testCreateKeyStoreFromFile_Success() throws Exception
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileKeyStore.NAME, "myFileKeyStore");
        attributes.put(FileKeyStore.STORE_URL, TestSSLConstants.BROKER_KEYSTORE);
        attributes.put(FileKeyStore.PASSWORD, TestSSLConstants.PASSWORD);
        attributes.put(FileKeyStore.KEY_STORE_TYPE, TestSSLConstants.JAVA_KEYSTORE_TYPE);

        FileKeyStoreImpl fileKeyStore = (FileKeyStoreImpl) FACTORY.create(KeyStore.class, attributes, BROKER);

        KeyManager[] keyManager = fileKeyStore.getKeyManagers();
        assertNotNull(keyManager);
        assertEquals("Unexpected number of key managers", 1, keyManager.length);
        assertNotNull("Key manager unexpected null", keyManager[0]);
    }

    @Test
    public void testCreateKeyStoreWithAliasFromFile_Success() throws Exception
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileKeyStore.NAME, "myFileKeyStore");
        attributes.put(FileKeyStore.STORE_URL, TestSSLConstants.BROKER_KEYSTORE);
        attributes.put(FileKeyStore.PASSWORD, TestSSLConstants.PASSWORD);
        attributes.put(FileKeyStore.CERTIFICATE_ALIAS, TestSSLConstants.BROKER_KEYSTORE_ALIAS);
        attributes.put(FileKeyStore.KEY_STORE_TYPE, TestSSLConstants.JAVA_KEYSTORE_TYPE);

        FileKeyStoreImpl fileKeyStore = (FileKeyStoreImpl) FACTORY.create(KeyStore.class, attributes, BROKER);

        KeyManager[] keyManager = fileKeyStore.getKeyManagers();
        assertNotNull(keyManager);
        assertEquals("Unexpected number of key managers", 1, keyManager.length);
        assertNotNull("Key manager unexpected null", keyManager[0]);
    }

    @Test
    public void testCreateKeyStoreFromFile_WrongPassword()
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileKeyStore.NAME, "myFileKeyStore");
        attributes.put(FileKeyStore.STORE_URL, TestSSLConstants.BROKER_KEYSTORE);
        attributes.put(FileKeyStore.PASSWORD, "wrong");
        attributes.put(FileKeyStore.KEY_STORE_TYPE, TestSSLConstants.JAVA_KEYSTORE_TYPE);

        KeyStoreTestHelper.checkExceptionThrownDuringKeyStoreCreation(FACTORY, BROKER, KeyStore.class, attributes,
                "Check key store password");
    }

    @Test
    public void testCreateKeyStoreFromFile_UnknownAlias()
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileKeyStore.NAME, "myFileKeyStore");
        attributes.put(FileKeyStore.STORE_URL, TestSSLConstants.CLIENT_KEYSTORE);
        attributes.put(FileKeyStore.PASSWORD, TestSSLConstants.PASSWORD);
        attributes.put(FileKeyStore.CERTIFICATE_ALIAS, "notknown");
        attributes.put(FileKeyStore.KEY_STORE_TYPE, TestSSLConstants.JAVA_KEYSTORE_TYPE);

        KeyStoreTestHelper.checkExceptionThrownDuringKeyStoreCreation(FACTORY, BROKER, KeyStore.class, attributes,
                "Cannot find a certificate with alias 'notknown' in key store");
    }

    @Test
    public void testCreateKeyStoreFromFile_NonKeyAlias()
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileKeyStore.NAME, "myFileKeyStore");
        attributes.put(FileKeyStore.STORE_URL, TestSSLConstants.CLIENT_KEYSTORE);
        attributes.put(FileKeyStore.PASSWORD, TestSSLConstants.PASSWORD);
        attributes.put(FileKeyStore.CERTIFICATE_ALIAS, TestSSLConstants.CERT_ALIAS_ROOT_CA);
        attributes.put(FileKeyStore.KEY_STORE_TYPE, TestSSLConstants.JAVA_KEYSTORE_TYPE);

        KeyStoreTestHelper.checkExceptionThrownDuringKeyStoreCreation(FACTORY, BROKER, KeyStore.class, attributes,
                "does not identify a private key");
    }

    @Test
    public void testCreateKeyStoreFromDataUrl_Success() throws Exception
    {
        String trustStoreAsDataUrl = createDataUrlForFile(TestSSLConstants.BROKER_KEYSTORE);

        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileKeyStore.NAME, "myFileKeyStore");
        attributes.put(FileKeyStore.STORE_URL, trustStoreAsDataUrl);
        attributes.put(FileKeyStore.PASSWORD, TestSSLConstants.PASSWORD);
        attributes.put(FileKeyStore.KEY_STORE_TYPE, TestSSLConstants.JAVA_KEYSTORE_TYPE);

        FileKeyStoreImpl fileKeyStore = (FileKeyStoreImpl) FACTORY.create(KeyStore.class, attributes, BROKER);

        KeyManager[] keyManagers = fileKeyStore.getKeyManagers();
        assertNotNull(keyManagers);
        assertEquals("Unexpected number of key managers", 1, keyManagers.length);
        assertNotNull("Key manager unexpected null", keyManagers[0]);
    }

    @Test
    public void testCreateKeyStoreWithAliasFromDataUrl_Success() throws Exception
    {
        String trustStoreAsDataUrl = createDataUrlForFile(TestSSLConstants.BROKER_KEYSTORE);

        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileKeyStore.NAME, "myFileKeyStore");
        attributes.put(FileKeyStore.STORE_URL, trustStoreAsDataUrl);
        attributes.put(FileKeyStore.PASSWORD, TestSSLConstants.PASSWORD);
        attributes.put(FileKeyStore.CERTIFICATE_ALIAS, TestSSLConstants.BROKER_KEYSTORE_ALIAS);
        attributes.put(FileKeyStore.KEY_STORE_TYPE, TestSSLConstants.JAVA_KEYSTORE_TYPE);

        FileKeyStoreImpl fileKeyStore = (FileKeyStoreImpl) FACTORY.create(KeyStore.class, attributes, BROKER);

        KeyManager[] keyManagers = fileKeyStore.getKeyManagers();
        assertNotNull(keyManagers);
        assertEquals("Unexpected number of key managers", 1, keyManagers.length);
        assertNotNull("Key manager unexpected null", keyManagers[0]);
    }

    @Test
    public void testCreateKeyStoreFromDataUrl_WrongPassword() throws Exception
    {
        String keyStoreAsDataUrl = createDataUrlForFile(TestSSLConstants.BROKER_KEYSTORE);

        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileKeyStore.NAME, "myFileKeyStore");
        attributes.put(FileKeyStore.PASSWORD, "wrong");
        attributes.put(FileKeyStore.STORE_URL, keyStoreAsDataUrl);
        attributes.put(FileKeyStore.KEY_STORE_TYPE, TestSSLConstants.JAVA_KEYSTORE_TYPE);

        KeyStoreTestHelper.checkExceptionThrownDuringKeyStoreCreation(FACTORY, BROKER, KeyStore.class, attributes,
                "Check key store password");
    }

    @Test
    public void testCreateKeyStoreFromDataUrl_BadKeystoreBytes()
    {
        String keyStoreAsDataUrl = DataUrlUtils.getDataUrlForBytes("notatruststore".getBytes());

        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileKeyStore.NAME, "myFileKeyStore");
        attributes.put(FileKeyStore.PASSWORD, TestSSLConstants.PASSWORD);
        attributes.put(FileKeyStore.STORE_URL, keyStoreAsDataUrl);

        KeyStoreTestHelper.checkExceptionThrownDuringKeyStoreCreation(FACTORY, BROKER, KeyStore.class, attributes,
                "Cannot instantiate key store");
    }

    @Test
    public void testCreateKeyStoreFromDataUrl_UnknownAlias() throws Exception
    {
        String keyStoreAsDataUrl = createDataUrlForFile(TestSSLConstants.BROKER_KEYSTORE);

        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileKeyStore.NAME, "myFileKeyStore");
        attributes.put(FileKeyStore.PASSWORD, TestSSLConstants.PASSWORD);
        attributes.put(FileKeyStore.STORE_URL, keyStoreAsDataUrl);
        attributes.put(FileKeyStore.CERTIFICATE_ALIAS, "notknown");
        attributes.put(FileKeyStore.KEY_STORE_TYPE, TestSSLConstants.JAVA_KEYSTORE_TYPE);

        KeyStoreTestHelper.checkExceptionThrownDuringKeyStoreCreation(FACTORY, BROKER, KeyStore.class, attributes,
                "Cannot find a certificate with alias 'notknown' in key store");
    }

    @Test
    public void testEmptyKeystoreRejected()
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileKeyStore.NAME, "myFileKeyStore");
        attributes.put(FileKeyStore.PASSWORD, TestSSLConstants.PASSWORD);
        attributes.put(FileKeyStore.STORE_URL, TestSSLConstants.TEST_EMPTY_KEYSTORE);

        KeyStoreTestHelper.checkExceptionThrownDuringKeyStoreCreation(FACTORY, BROKER, KeyStore.class, attributes,
                "must contain at least one private key");
    }

    @Test
    public void testKeystoreWithNoPrivateKeyRejected()
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileKeyStore.NAME, getTestName());
        attributes.put(FileKeyStore.PASSWORD, TestSSLConstants.PASSWORD);
        attributes.put(FileKeyStore.STORE_URL, TestSSLConstants.TEST_CERT_ONLY_KEYSTORE);
        attributes.put(FileKeyStore.KEY_STORE_TYPE, TestSSLConstants.JAVA_KEYSTORE_TYPE);

        KeyStoreTestHelper.checkExceptionThrownDuringKeyStoreCreation(FACTORY, BROKER, KeyStore.class, attributes,
                "must contain at least one private key");
    }

    @Test
    public void testSymmetricKeysIgnored()
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileKeyStore.NAME, "myFileKeyStore");
        attributes.put(FileKeyStore.PASSWORD, TestSSLConstants.PASSWORD);
        attributes.put(FileKeyStore.STORE_URL, TestSSLConstants.TEST_SYMMETRIC_KEY_KEYSTORE);
        attributes.put(FileKeyStore.KEY_STORE_TYPE, TestSSLConstants.JAVA_KEYSTORE_TYPE);

        KeyStore keyStore = (KeyStore) FACTORY.create(KeyStore.class, attributes, BROKER);
        assertNotNull(keyStore);
    }

    @Test
    public void testUpdateKeyStore_Success()
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileKeyStore.NAME, "myFileKeyStore");
        attributes.put(FileKeyStore.STORE_URL, TestSSLConstants.BROKER_KEYSTORE);
        attributes.put(FileKeyStore.PASSWORD, TestSSLConstants.PASSWORD);
        attributes.put(FileKeyStore.KEY_STORE_TYPE, TestSSLConstants.JAVA_KEYSTORE_TYPE);

        FileKeyStoreImpl fileKeyStore = (FileKeyStoreImpl) FACTORY.create(KeyStore.class, attributes, BROKER);

        assertNull("Unexpected alias value before change", fileKeyStore.getCertificateAlias());

        try
        {
            Map<String,Object> unacceptableAttributes = new HashMap<>();
            unacceptableAttributes.put(FileKeyStore.CERTIFICATE_ALIAS, "notknown");

            fileKeyStore.setAttributes(unacceptableAttributes);
            fail("Exception not thrown");
        }
        catch (IllegalConfigurationException e)
        {
            String message = e.getMessage();
            assertTrue("Exception text not as unexpected:" + message,
                              message.contains("Cannot find a certificate with alias 'notknown' in key store"));
        }

        assertNull("Unexpected alias value after failed change", fileKeyStore.getCertificateAlias());

        Map<String,Object> changedAttributes = new HashMap<>();
        changedAttributes.put(FileKeyStore.CERTIFICATE_ALIAS, TestSSLConstants.BROKER_KEYSTORE_ALIAS);

        fileKeyStore.setAttributes(changedAttributes);

        assertEquals("Unexpected alias value after change that is expected to be successful",
                TestSSLConstants.BROKER_KEYSTORE_ALIAS, fileKeyStore.getCertificateAlias());

    }

    @Test
    public void testReloadKeystore() throws Exception
    {
        assumeThat(SSLUtil.canGenerateCerts(), is(equalTo(true)));

        final SSLUtil.KeyCertPair selfSigned1 = KeyStoreTestHelper.generateSelfSigned("CN=foo");
        final SSLUtil.KeyCertPair selfSigned2 = KeyStoreTestHelper.generateSelfSigned("CN=bar");

        final File keyStoreFile = TestFileUtils.createTempFile(this, ".ks");
        final String dummy = "changit";
        final char[] pass = dummy.toCharArray();
        final String certificateAlias = "test1";
        final String keyAlias = "test2";
        try
        {
            final java.security.KeyStore keyStore =
                    KeyStoreTestHelper.saveKeyStore(selfSigned1, certificateAlias, keyAlias, pass, keyStoreFile);

            final Map<String, Object> attributes = new HashMap<>();
            attributes.put(FileKeyStore.NAME, getTestName());
            attributes.put(FileKeyStore.STORE_URL, keyStoreFile.getAbsolutePath());
            attributes.put(FileKeyStore.PASSWORD, dummy);
            attributes.put(FileKeyStore.KEY_STORE_TYPE, keyStore.getType());

            final FileKeyStore keyStoreObject = (FileKeyStore) FACTORY.create(KeyStore.class, attributes, BROKER);

            final CertificateDetails certificate = getCertificate(keyStoreObject);
            assertEquals("CN=foo", certificate.getIssuerName());

            assertTrue(keyStoreFile.delete());
            assertTrue(keyStoreFile.createNewFile());keyStoreFile.deleteOnExit();
            KeyStoreTestHelper.saveKeyStore(selfSigned2, certificateAlias, keyAlias, pass, keyStoreFile);

            keyStoreObject.reload();

            final CertificateDetails certificate2 = getCertificate(keyStoreObject);
            assertEquals("CN=bar", certificate2.getIssuerName());
        }
        finally
        {
            assertTrue(keyStoreFile.delete());
        }
    }

    public CertificateDetails getCertificate(final FileKeyStore keyStore)
    {
        final List<CertificateDetails> certificates = keyStore.getCertificateDetails();

        assertNotNull(certificates);
        assertEquals(1, certificates.size());

        return certificates.get(0);
    }
}
