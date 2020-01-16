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
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateExpiredException;
import java.security.cert.X509Certificate;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import com.google.common.io.ByteStreams;
import org.junit.Test;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.server.transport.network.security.ssl.QpidPeersOnlyTrustManager;
import org.apache.qpid.server.transport.network.security.ssl.SSLUtil;
import org.apache.qpid.server.util.DataUrlUtils;
import org.apache.qpid.test.utils.TestFileUtils;
import org.apache.qpid.test.utils.TestSSLConstants;

public class FileTrustStoreTest extends KeyStoreTestBase
{
    public FileTrustStoreTest()
    {
        super(TrustStore.class);
    }

    @Test
    public void testCreateTrustStoreFromFile_Success() throws Exception
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileTrustStore.NAME, "myFileTrustStore");
        attributes.put(FileTrustStore.STORE_URL, TestSSLConstants.CLIENT_TRUSTSTORE);
        attributes.put(FileTrustStore.PASSWORD, TestSSLConstants.PASSWORD);
        attributes.put(FileTrustStore.REVOCATION, true);
        attributes.put(FileTrustStore.CRL_URL, TestSSLConstants.CA_CRL);

        TrustStore<?> fileTrustStore = _factory.create(TrustStore.class, attributes, _broker);

        TrustManager[] trustManagers = fileTrustStore.getTrustManagers();
        assertNotNull(trustManagers);
        assertEquals("Unexpected number of trust managers", 1, trustManagers.length);
        assertNotNull("Trust manager unexpected null", trustManagers[0]);
    }

    @Test
    public void testCreateTrustStoreFromFile_WrongPassword()
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileTrustStore.NAME, "myFileTrustStore");
        attributes.put(FileTrustStore.STORE_URL, TestSSLConstants.CLIENT_TRUSTSTORE);
        attributes.put(FileTrustStore.PASSWORD, "wrong");
        attributes.put(FileTrustStore.TRUST_STORE_TYPE, TestSSLConstants.JAVA_KEYSTORE_TYPE);

        checkExceptionThrownDuringKeyStoreCreation(attributes, "Check trust store password");
    }

    @Test
    public void testCreateTrustStoreFromFile_MissingCrlFile()
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileTrustStore.NAME, "myFileTrustStore");
        attributes.put(FileTrustStore.STORE_URL, TestSSLConstants.CLIENT_TRUSTSTORE);
        attributes.put(FileTrustStore.PASSWORD, TestSSLConstants.PASSWORD);
        attributes.put(FileTrustStore.TRUST_STORE_TYPE, TestSSLConstants.JAVA_KEYSTORE_TYPE);
        attributes.put(FileTrustStore.CRL_URL, "/not/a/crl");

        checkExceptionThrownDuringKeyStoreCreation(attributes, "Unable to load certificate revocation list");
    }

    @Test
    public void testCreatePeersOnlyTrustStoreFromFile_Success() throws Exception
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileTrustStore.NAME, "myFileTrustStore");
        attributes.put(FileTrustStore.STORE_URL, TestSSLConstants.BROKER_PEERSTORE);
        attributes.put(FileTrustStore.PASSWORD, TestSSLConstants.PASSWORD);
        attributes.put(FileTrustStore.PEERS_ONLY, true);
        attributes.put(FileTrustStore.TRUST_STORE_TYPE, TestSSLConstants.JAVA_KEYSTORE_TYPE);
        attributes.put(FileTrustStore.REVOCATION, true);
        attributes.put(FileTrustStore.CRL_URL, TestSSLConstants.CA_CRL);

        TrustStore<?> fileTrustStore = (TrustStore) _factory.create(_keystoreClass, attributes,  _broker);

        TrustManager[] trustManagers = fileTrustStore.getTrustManagers();
        assertNotNull(trustManagers);
        assertEquals("Unexpected number of trust managers", 1, trustManagers.length);
        assertNotNull("Trust manager unexpected null", trustManagers[0]);
        final boolean condition = trustManagers[0] instanceof QpidPeersOnlyTrustManager;
        assertTrue("Trust manager unexpected null", condition);
    }

    @Test
    public void testUseOfExpiredTrustAnchorAllowed() throws Exception
    {
        // https://www.ibm.com/support/knowledgecenter/en/SSYKE2_8.0.0/com.ibm.java.security.component.80.doc/security-
        assumeThat("IBMJSSE2 trust factory (IbmX509) validates the entire chain, including trusted certificates.",
                getJvmVendor(),
                is(not(equalTo(IBM))));

        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileTrustStore.NAME, "myFileTrustStore");
        attributes.put(FileTrustStore.STORE_URL, TestSSLConstants.BROKER_EXPIRED_TRUSTSTORE);
        attributes.put(FileTrustStore.PASSWORD, TestSSLConstants.PASSWORD);
        attributes.put(FileTrustStore.TRUST_STORE_TYPE, TestSSLConstants.JAVA_KEYSTORE_TYPE);

        TrustStore trustStore = (TrustStore) _factory.create(_keystoreClass, attributes, _broker);

        TrustManager[] trustManagers = trustStore.getTrustManagers();
        assertNotNull(trustManagers);
        assertEquals("Unexpected number of trust managers", 1, trustManagers.length);
        final boolean condition = trustManagers[0] instanceof X509TrustManager;
        assertTrue("Unexpected trust manager type", condition);
        X509TrustManager trustManager = (X509TrustManager) trustManagers[0];

        KeyStore clientStore = getInitializedKeyStore(TestSSLConstants.CLIENT_EXPIRED_KEYSTORE,
                                                      TestSSLConstants.PASSWORD,
                                                      TestSSLConstants.JAVA_KEYSTORE_TYPE);
        String alias = clientStore.aliases().nextElement();
        X509Certificate certificate = (X509Certificate) clientStore.getCertificate(alias);

        trustManager.checkClientTrusted(new X509Certificate[] {certificate}, "NULL");
    }

    @Test
    public void testUseOfExpiredTrustAnchorDenied() throws Exception
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileTrustStore.NAME, "myFileTrustStore");
        attributes.put(FileTrustStore.STORE_URL, TestSSLConstants.BROKER_EXPIRED_TRUSTSTORE);
        attributes.put(FileTrustStore.PASSWORD, TestSSLConstants.PASSWORD);
        attributes.put(FileTrustStore.TRUST_ANCHOR_VALIDITY_ENFORCED, true);
        attributes.put(FileTrustStore.TRUST_STORE_TYPE, TestSSLConstants.JAVA_KEYSTORE_TYPE);

        TrustStore trustStore = (TrustStore) _factory.create(_keystoreClass, attributes, _broker);

        TrustManager[] trustManagers = trustStore.getTrustManagers();
        assertNotNull(trustManagers);
        assertEquals("Unexpected number of trust managers", 1, trustManagers.length);
        final boolean condition = trustManagers[0] instanceof X509TrustManager;
        assertTrue("Unexpected trust manager type", condition);
        X509TrustManager trustManager = (X509TrustManager) trustManagers[0];

        KeyStore clientStore = getInitializedKeyStore(TestSSLConstants.CLIENT_EXPIRED_KEYSTORE,
                                                      TestSSLConstants.PASSWORD,
                                                      TestSSLConstants.JAVA_KEYSTORE_TYPE);
        String alias = clientStore.aliases().nextElement();
        X509Certificate certificate = (X509Certificate) clientStore.getCertificate(alias);

        try
        {
            trustManager.checkClientTrusted(new X509Certificate[] {certificate}, "NULL");
            fail("Exception not thrown");
        }
        catch (CertificateException e)
        {
            if (e instanceof CertificateExpiredException || "Certificate expired".equals(e.getMessage()))
            {
                // IBMJSSE2 does not throw CertificateExpiredException, it throws a CertificateException
                // PASS
            }
            else
            {
                throw e;
            }

        }
    }

    @Test
    public void testCreateTrustStoreFromDataUrl_Success() throws Exception
    {
        String trustStoreAsDataUrl = createDataUrlForFile(TestSSLConstants.CLIENT_TRUSTSTORE);
        String crlAsDataUrl = createDataUrlForFile(TestSSLConstants.CA_CRL);

        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileTrustStore.NAME, "myFileTrustStore");
        attributes.put(FileTrustStore.STORE_URL, trustStoreAsDataUrl);
        attributes.put(FileTrustStore.PASSWORD, TestSSLConstants.PASSWORD);
        attributes.put(FileTrustStore.TRUST_STORE_TYPE, TestSSLConstants.JAVA_KEYSTORE_TYPE);
        attributes.put(FileTrustStore.REVOCATION, true);
        attributes.put(FileTrustStore.CRL_URL, crlAsDataUrl);

        TrustStore<?> fileTrustStore = (TrustStore) _factory.create(_keystoreClass, attributes,  _broker);

        TrustManager[] trustManagers = fileTrustStore.getTrustManagers();
        assertNotNull(trustManagers);
        assertEquals("Unexpected number of trust managers", 1, trustManagers.length);
        assertNotNull("Trust manager unexpected null", trustManagers[0]);
    }

    @Test
    public void testCreateTrustStoreFromDataUrl_WrongPassword() throws Exception
    {
        String trustStoreAsDataUrl = createDataUrlForFile(TestSSLConstants.CLIENT_TRUSTSTORE);

        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileTrustStore.NAME, "myFileTrustStore");
        attributes.put(FileTrustStore.PASSWORD, "wrong");
        attributes.put(FileTrustStore.STORE_URL, trustStoreAsDataUrl);
        attributes.put(FileTrustStore.TRUST_STORE_TYPE, TestSSLConstants.JAVA_KEYSTORE_TYPE);

        checkExceptionThrownDuringKeyStoreCreation(attributes, "Check trust store password");
    }

    @Test
    public void testCreateTrustStoreFromDataUrl_BadTruststoreBytes()
    {
        String trustStoreAsDataUrl = DataUrlUtils.getDataUrlForBytes("notatruststore".getBytes());

        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileTrustStore.NAME, "myFileTrustStore");
        attributes.put(FileTrustStore.PASSWORD, TestSSLConstants.PASSWORD);
        attributes.put(FileTrustStore.STORE_URL, trustStoreAsDataUrl);
        attributes.put(FileTrustStore.TRUST_STORE_TYPE, TestSSLConstants.JAVA_KEYSTORE_TYPE);

        checkExceptionThrownDuringKeyStoreCreation(attributes, "Cannot instantiate trust store");
    }

    @Test
    public void testUpdateTrustStore_Success()
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileTrustStore.NAME, "myFileTrustStore");
        attributes.put(FileTrustStore.STORE_URL, TestSSLConstants.CLIENT_TRUSTSTORE);
        attributes.put(FileTrustStore.PASSWORD, TestSSLConstants.PASSWORD);
        attributes.put(FileTrustStore.TRUST_STORE_TYPE, TestSSLConstants.JAVA_KEYSTORE_TYPE);
        attributes.put(FileTrustStore.REVOCATION, true);
        attributes.put(FileTrustStore.CRL_URL, TestSSLConstants.CA_CRL);

        FileTrustStore<?> fileTrustStore = (FileTrustStore<?>) _factory.create(_keystoreClass, attributes,  _broker);

        assertEquals("Unexpected path value before change",
                            TestSSLConstants.CLIENT_TRUSTSTORE,
                            fileTrustStore.getStoreUrl());


        try
        {
            Map<String,Object> unacceptableAttributes = new HashMap<>();
            unacceptableAttributes.put(FileTrustStore.STORE_URL, "/not/a/truststore");

            fileTrustStore.setAttributes(unacceptableAttributes);
            fail("Exception not thrown");
        }
        catch (IllegalConfigurationException e)
        {
            String message = e.getMessage();
            assertTrue("Exception text not as unexpected:" + message,
                              message.contains("Cannot instantiate trust store"));
        }

        assertEquals("Unexpected keystore path value after failed change",
                TestSSLConstants.CLIENT_TRUSTSTORE,
                fileTrustStore.getStoreUrl());

        try
        {
            Map<String,Object> unacceptableAttributes = new HashMap<>();
            unacceptableAttributes.put(FileTrustStore.CRL_URL, "/not/a/crl");

            fileTrustStore.setAttributes(unacceptableAttributes);
            fail("Exception not thrown");
        }
        catch (IllegalConfigurationException e)
        {
            String message = e.getMessage();
            assertTrue("Exception text not as unexpected:" + message,
                    message.contains("Unable to load certificate revocation list '/not/a/crl' for truststore " +
                            "'myFileTrustStore' :java.io.FileNotFoundException: /not/a/crl (No such file or directory)"));
        }

        assertEquals("Unexpected CRL path value after failed change",
                            TestSSLConstants.CA_CRL,
                            fileTrustStore.getCrlUrl());

        Map<String,Object> changedAttributes = new HashMap<>();
        changedAttributes.put(FileTrustStore.STORE_URL, TestSSLConstants.BROKER_TRUSTSTORE);
        changedAttributes.put(FileTrustStore.PASSWORD, TestSSLConstants.PASSWORD);
        changedAttributes.put(FileTrustStore.CRL_URL, TestSSLConstants.CA_CRL_EMPTY);

        fileTrustStore.setAttributes(changedAttributes);

        assertEquals("Unexpected keystore path value after change that is expected to be successful",
                            TestSSLConstants.BROKER_TRUSTSTORE,
                            fileTrustStore.getStoreUrl());
        assertEquals("Unexpected CRL path value after change that is expected to be successful",
                TestSSLConstants.CA_CRL_EMPTY,
                fileTrustStore.getCrlUrl());
    }

    @Test
    public void testEmptyTrustStoreRejected()
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileKeyStore.NAME, "myFileTrustStore");
        attributes.put(FileKeyStore.PASSWORD, TestSSLConstants.PASSWORD);
        attributes.put(FileKeyStore.STORE_URL, TestSSLConstants.TEST_EMPTY_KEYSTORE);
        attributes.put(FileTrustStore.TRUST_STORE_TYPE, "jks");

        checkExceptionThrownDuringKeyStoreCreation(attributes, "must contain at least one certificate");
    }

    @Test
    public void testTrustStoreWithNoCertificateRejected()
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileTrustStore.NAME, getTestName());
        attributes.put(FileTrustStore.PASSWORD, TestSSLConstants.PASSWORD);
        attributes.put(FileTrustStore.STORE_URL, TestSSLConstants.TEST_PK_ONLY_KEYSTORE);
        attributes.put(FileTrustStore.TRUST_STORE_TYPE, TestSSLConstants.JAVA_KEYSTORE_TYPE);

        checkExceptionThrownDuringKeyStoreCreation(attributes, "must contain at least one certificate");
    }

    @Test
    public void testSymmetricKeyEntryIgnored() throws Exception
    {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(FileTrustStore.NAME, getTestName());
        attributes.put(FileTrustStore.PASSWORD, TestSSLConstants.PASSWORD);
        attributes.put(FileTrustStore.STORE_URL, TestSSLConstants.TEST_SYMMETRIC_KEY_KEYSTORE);
        attributes.put(FileTrustStore.TRUST_STORE_TYPE, TestSSLConstants.JAVA_KEYSTORE_TYPE);

        TrustStore trustStore = (TrustStore) _factory.create(_keystoreClass, attributes, _broker);

        Certificate[] certificates = trustStore.getCertificates();
        assertEquals("Unexpected number of certificates",
                            getNumberOfCertificates(TestSSLConstants.TEST_SYMMETRIC_KEY_KEYSTORE,
                                    TestSSLConstants.JAVA_KEYSTORE_TYPE),
                            certificates.length);
    }

    @Test
    public void testPrivateKeyEntryIgnored() throws Exception
    {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(FileTrustStore.NAME, getTestName());
        attributes.put(FileTrustStore.PASSWORD, TestSSLConstants.PASSWORD);
        attributes.put(FileTrustStore.STORE_URL, TestSSLConstants.TEST_KEYSTORE);
        attributes.put(FileTrustStore.TRUST_STORE_TYPE, TestSSLConstants.JAVA_KEYSTORE_TYPE);

        TrustStore trustStore = (TrustStore) _factory.create(_keystoreClass, attributes, _broker);

        Certificate[] certificates = trustStore.getCertificates();
        assertEquals("Unexpected number of certificates",
                            getNumberOfCertificates(TestSSLConstants.TEST_KEYSTORE,
                                    TestSSLConstants.JAVA_KEYSTORE_TYPE),
                            certificates.length);
    }

    @Test
    public void testReloadKeystore() throws Exception
    {
        assumeThat(SSLUtil.canGenerateCerts(), is(equalTo(true)));

        final SSLUtil.KeyCertPair selfSigned1 = KeystoreTestHelper.generateSelfSigned("CN=foo");
        final SSLUtil.KeyCertPair selfSigned2 = KeystoreTestHelper.generateSelfSigned("CN=bar");

        final File keyStoreFile = TestFileUtils.createTempFile(this, ".ks");
        final String dummy = "changit";
        final char[] pass = dummy.toCharArray();
        final String alias = "test";
        try
        {
            final java.security.KeyStore keyStore =
                    KeystoreTestHelper.saveKeyStore(alias, selfSigned1.getCertificate(), pass, keyStoreFile);

            final Map<String, Object> attributes = new HashMap<>();
            attributes.put(FileTrustStore.NAME, getTestName());
            attributes.put(FileTrustStore.PASSWORD, dummy);
            attributes.put(FileTrustStore.STORE_URL, keyStoreFile.getAbsolutePath());
            attributes.put(FileTrustStore.TRUST_STORE_TYPE, keyStore.getType());

            final FileTrustStore trustStore = (FileTrustStore) _factory.create(_keystoreClass, attributes, _broker);

            final X509Certificate certificate = getCertificate(trustStore);
            assertEquals("CN=foo", certificate.getIssuerX500Principal().getName());

            KeystoreTestHelper.saveKeyStore(alias, selfSigned2.getCertificate(), pass, keyStoreFile);

            trustStore.reload();

            final X509Certificate certificate2 = getCertificate(trustStore);
            assertEquals("CN=bar", certificate2.getIssuerX500Principal().getName());
        }
        finally
        {
            assertTrue(keyStoreFile.delete());
        }
    }

    public X509Certificate getCertificate(final FileTrustStore trustStore) throws java.security.GeneralSecurityException
    {
        Certificate[] certificates = trustStore.getCertificates();

        assertNotNull(certificates);
        assertEquals(1, certificates.length);

        Certificate certificate = certificates[0];
        assertTrue(certificate instanceof X509Certificate);
        return (X509Certificate)certificate;
    }

    private int getNumberOfCertificates(String keystore, String type) throws Exception
    {
        KeyStore ks = KeyStore.getInstance(type);
        try(InputStream is = new FileInputStream(keystore))
        {
            ks.load(is, TestSSLConstants.PASSWORD.toCharArray());
        }

        int result = 0;
        Enumeration<String> aliases = ks.aliases();
        while (aliases.hasMoreElements())
        {
            String alias = aliases.nextElement();
            if (ks.isCertificateEntry(alias))
            {
                result++;
            }
        }
        return result;
    }

    public  static String createDataUrlForFile(String filename) throws IOException
    {
        InputStream in = null;
        try
        {
            File f = new File(filename);
            if (f.exists())
            {
                in = new FileInputStream(f);
            }
            else
            {
                in = Thread.currentThread().getContextClassLoader().getResourceAsStream(filename);
            }
            byte[] fileAsBytes = ByteStreams.toByteArray(in);
            return DataUrlUtils.getDataUrlForBytes(fileAsBytes);
        }
        finally
        {
            if (in != null)
            {
                in.close();
            }
        }
    }
}
