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


import static org.apache.qpid.server.security.FileKeyStoreTest.EMPTY_KEYSTORE_RESOURCE;
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
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
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.ConfiguredObjectFactory;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.server.transport.network.security.ssl.QpidPeersOnlyTrustManager;
import org.apache.qpid.server.util.DataUrlUtils;
import org.apache.qpid.test.utils.TestSSLConstants;
import org.apache.qpid.test.utils.UnitTestBase;

public class FileTrustStoreTest extends UnitTestBase
{
    static final String SYMMETRIC_KEY_KEYSTORE_RESOURCE = "/ssl/test_symmetric_key_keystore.pkcs12";
    private static final String KEYSTORE_PK_ONLY_RESOURCE = "/ssl/test_pk_only_keystore.pkcs12";
    private static final String TRUSTSTORE_PASSWORD = TestSSLConstants.TRUSTSTORE_PASSWORD;
    private static final String PEER_STORE_PASSWORD = TestSSLConstants.BROKER_PEERSTORE_PASSWORD;
    private static final String KEYSTORE_PASSWORD = TestSSLConstants.KEYSTORE_PASSWORD;
    private static final String KEYSTORE_RESOURCE = "/ssl/test_keystore.jks";
    private static final String TRUST_STORE_PATH = "classpath:ssl/java_client_truststore.pkcs12";
    private static final String PEER_STORE_PATH = "classpath:ssl/java_broker_peerstore.pkcs12";
    private static final String EXPIRED_TRUST_STORE_PATH = "classpath:ssl/java_broker_expired_truststore.pkcs12";
    private static final String EXPIRED_KEYSTORE_PATH = "ssl/java_client_expired_keystore.pkcs12";
    private static final String TRUST_STORE = "ssl/java_client_truststore.pkcs12";
    private static final String BROKER_TRUST_STORE_PATH = "classpath:ssl/java_broker_truststore.pkcs12";
    private static final String BROKER_TRUST_STORE_PASSWORD = TestSSLConstants.BROKER_TRUSTSTORE_PASSWORD;
    private static final String BROKER_KEYSTORE_PASSWORD = TestSSLConstants.BROKER_KEYSTORE_PASSWORD;


    private final Broker _broker = mock(Broker.class);
    private final TaskExecutor _taskExecutor = CurrentThreadTaskExecutor.newStartedInstance();
    private final Model _model = BrokerModel.getInstance();
    private final ConfiguredObjectFactory _factory = _model.getObjectFactory();

    @Before
    public void setUp() throws Exception
    {

        when(_broker.getTaskExecutor()).thenReturn(_taskExecutor);
        when(_broker.getChildExecutor()).thenReturn(_taskExecutor);

        when(_broker.getModel()).thenReturn(_model);
        when(_broker.getCategoryClass()).thenReturn(Broker.class);
        when(_broker.getEventLogger()).thenReturn(new EventLogger());
        when(_broker.getTypeClass()).thenReturn(Broker.class);
    }

    @Test
    public void testCreateTrustStoreFromFile_Success() throws Exception
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileTrustStore.NAME, "myFileTrustStore");
        attributes.put(FileTrustStore.STORE_URL, TRUST_STORE_PATH);
        attributes.put(FileTrustStore.PASSWORD, TRUSTSTORE_PASSWORD);

        TrustStore<?> fileTrustStore = _factory.create(TrustStore.class, attributes,  _broker);

        TrustManager[] trustManagers = fileTrustStore.getTrustManagers();
        assertNotNull(trustManagers);
        assertEquals("Unexpected number of trust managers", (long) 1, (long) trustManagers.length);
        assertNotNull("Trust manager unexpected null", trustManagers[0]);
    }

    @Test
    public void testCreateTrustStoreFromFile_WrongPassword() throws Exception
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileTrustStore.NAME, "myFileTrustStore");
        attributes.put(FileTrustStore.STORE_URL, TRUST_STORE_PATH);
        attributes.put(FileTrustStore.PASSWORD, "wrong");

        try
        {
            _factory.create(TrustStore.class, attributes,  _broker);
            fail("Exception not thrown");
        }
        catch (IllegalConfigurationException ice)
        {
            String message = ice.getMessage();
            assertTrue("Exception text not as unexpected:" + message,
                              message.contains("Check trust store password"));

        }
    }

    @Test
    public void testCreatePeersOnlyTrustStoreFromFile_Success() throws Exception
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileTrustStore.NAME, "myFileTrustStore");
        attributes.put(FileTrustStore.STORE_URL, PEER_STORE_PATH);
        attributes.put(FileTrustStore.PASSWORD, PEER_STORE_PASSWORD);
        attributes.put(FileTrustStore.PEERS_ONLY, true);

        TrustStore<?> fileTrustStore = _factory.create(TrustStore.class, attributes,  _broker);

        TrustManager[] trustManagers = fileTrustStore.getTrustManagers();
        assertNotNull(trustManagers);
        assertEquals("Unexpected number of trust managers", (long) 1, (long) trustManagers.length);
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
        attributes.put(FileTrustStore.STORE_URL, EXPIRED_TRUST_STORE_PATH);
        attributes.put(FileTrustStore.PASSWORD, BROKER_TRUST_STORE_PASSWORD);

        TrustStore trustStore = _factory.create(TrustStore.class, attributes, _broker);

        TrustManager[] trustManagers = trustStore.getTrustManagers();
        assertNotNull(trustManagers);
        assertEquals("Unexpected number of trust managers", (long) 1, (long) trustManagers.length);
        final boolean condition = trustManagers[0] instanceof X509TrustManager;
        assertTrue("Unexpected trust manager type", condition);
        X509TrustManager trustManager = (X509TrustManager) trustManagers[0];

        KeyStore clientStore = getInitializedKeyStore(EXPIRED_KEYSTORE_PATH,
                                                      KEYSTORE_PASSWORD,
                                                      "pkcs12");
        String alias = clientStore.aliases().nextElement();
        X509Certificate certificate = (X509Certificate) clientStore.getCertificate(alias);

        trustManager.checkClientTrusted(new X509Certificate[] {certificate}, "NULL");
    }

    @Test
    public void testUseOfExpiredTrustAnchorDenied() throws Exception
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileTrustStore.NAME, "myFileTrustStore");
        attributes.put(FileTrustStore.STORE_URL, EXPIRED_TRUST_STORE_PATH);
        attributes.put(FileTrustStore.PASSWORD, BROKER_TRUST_STORE_PASSWORD);
        attributes.put(FileTrustStore.TRUST_ANCHOR_VALIDITY_ENFORCED, true);

        TrustStore trustStore = _factory.create(TrustStore.class, attributes, _broker);

        TrustManager[] trustManagers = trustStore.getTrustManagers();
        assertNotNull(trustManagers);
        assertEquals("Unexpected number of trust managers", (long) 1, (long) trustManagers.length);
        final boolean condition = trustManagers[0] instanceof X509TrustManager;
        assertTrue("Unexpected trust manager type", condition);
        X509TrustManager trustManager = (X509TrustManager) trustManagers[0];

        KeyStore clientStore = getInitializedKeyStore(EXPIRED_KEYSTORE_PATH,
                                                      KEYSTORE_PASSWORD,
                                                      KeyStore.getDefaultType());
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
        String trustStoreAsDataUrl = createDataUrlForFile(TRUST_STORE);

        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileTrustStore.NAME, "myFileTrustStore");
        attributes.put(FileTrustStore.STORE_URL, trustStoreAsDataUrl);
        attributes.put(FileTrustStore.PASSWORD, TRUSTSTORE_PASSWORD);

        TrustStore<?> fileTrustStore = _factory.create(TrustStore.class, attributes,  _broker);

        TrustManager[] trustManagers = fileTrustStore.getTrustManagers();
        assertNotNull(trustManagers);
        assertEquals("Unexpected number of trust managers", (long) 1, (long) trustManagers.length);
        assertNotNull("Trust manager unexpected null", trustManagers[0]);
    }

    @Test
    public void testCreateTrustStoreFromDataUrl_WrongPassword() throws Exception
    {
        String trustStoreAsDataUrl = createDataUrlForFile(TRUST_STORE);

        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileTrustStore.NAME, "myFileTrustStore");
        attributes.put(FileTrustStore.PASSWORD, "wrong");
        attributes.put(FileTrustStore.STORE_URL, trustStoreAsDataUrl);

        try
        {
            _factory.create(TrustStore.class, attributes,  _broker);
            fail("Exception not thrown");
        }
        catch (IllegalConfigurationException ice)
        {
            String message = ice.getMessage();
            assertTrue("Exception text not as unexpected:" + message,
                              message.contains("Check trust store password"));
        }
    }

    @Test
    public void testCreateTrustStoreFromDataUrl_BadTruststoreBytes() throws Exception
    {
        String trustStoreAsDataUrl = DataUrlUtils.getDataUrlForBytes("notatruststore".getBytes());

        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileTrustStore.NAME, "myFileTrustStore");
        attributes.put(FileTrustStore.PASSWORD, TRUSTSTORE_PASSWORD);
        attributes.put(FileTrustStore.STORE_URL, trustStoreAsDataUrl);

        try
        {
            _factory.create(TrustStore.class, attributes,  _broker);
            fail("Exception not thrown");
        }
        catch (IllegalConfigurationException ice)
        {
            String message = ice.getMessage();
            assertTrue("Exception text not as unexpected:" + message,
                              message.contains("Cannot instantiate trust store"));
        }
    }

    @Test
    public void testUpdateTrustStore_Success() throws Exception
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileTrustStore.NAME, "myFileTrustStore");
        attributes.put(FileTrustStore.STORE_URL, TRUST_STORE_PATH);
        attributes.put(FileTrustStore.PASSWORD, TRUSTSTORE_PASSWORD);

        FileTrustStore<?> fileTrustStore = (FileTrustStore<?>) _factory.create(TrustStore.class, attributes,  _broker);

        assertEquals("Unexpected path value before change",
                            TRUST_STORE_PATH,
                            fileTrustStore.getStoreUrl());


        try
        {
            Map<String,Object> unacceptableAttributes = new HashMap<>();
            unacceptableAttributes.put(FileTrustStore.STORE_URL, "/not/a/truststore");

            fileTrustStore.setAttributes(unacceptableAttributes);
            fail("Exception not thrown");
        }
        catch (IllegalConfigurationException ice)
        {
            String message = ice.getMessage();
            assertTrue("Exception text not as unexpected:" + message,
                              message.contains("Cannot instantiate trust store"));
        }

        assertEquals("Unexpected path value after failed change",
                            TRUST_STORE_PATH,
                            fileTrustStore.getStoreUrl());

        Map<String,Object> changedAttributes = new HashMap<>();
        changedAttributes.put(FileTrustStore.STORE_URL, BROKER_TRUST_STORE_PATH);
        changedAttributes.put(FileTrustStore.PASSWORD, BROKER_TRUST_STORE_PASSWORD);

        fileTrustStore.setAttributes(changedAttributes);

        assertEquals("Unexpected path value after change that is expected to be successful",
                     BROKER_TRUST_STORE_PATH,
                            fileTrustStore.getStoreUrl());
    }

    @Test
    public void testEmptyTrustStoreRejected()
    {
        final URL emptyKeystore = getClass().getResource(EMPTY_KEYSTORE_RESOURCE);
        assertNotNull("Empty keystore not found", emptyKeystore);

        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileKeyStore.NAME, "myFileTrustStore");
        attributes.put(FileKeyStore.PASSWORD, KEYSTORE_PASSWORD);
        attributes.put(FileKeyStore.STORE_URL, emptyKeystore);

        try
        {
            _factory.create(TrustStore.class, attributes, _broker);
            fail("Exception not thrown");
        }
        catch (IllegalConfigurationException ice)
        {
            // pass
        }
    }

    @Test
    public void testTrustStoreWithNoCertificateRejected()
    {
        final URL keystoreUrl = getClass().getResource(KEYSTORE_PK_ONLY_RESOURCE);
        assertNotNull("Keystore not found", keystoreUrl);

        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileTrustStore.NAME, getTestName());
        attributes.put(FileTrustStore.PASSWORD, TRUSTSTORE_PASSWORD);
        attributes.put(FileTrustStore.STORE_URL, keystoreUrl);
        attributes.put(FileTrustStore.TRUST_STORE_TYPE, "PKCS12");

        try
        {
            _factory.create(TrustStore.class, attributes, _broker);
            fail("Exception not thrown");
        }
        catch (IllegalConfigurationException ice)
        {
            String message = ice.getMessage();
            assertTrue("Exception text not as unexpected:" + message,
                              message.contains("must contain at least one certificate"));
        }
    }

    @Test
    public void testSymmetricKeyEntryIgnored() throws Exception
    {
        final URL keystoreUrl = getClass().getResource(SYMMETRIC_KEY_KEYSTORE_RESOURCE);
        assertNotNull("Symmetric key keystore not found", keystoreUrl);

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(FileTrustStore.NAME, getTestName());
        attributes.put(FileTrustStore.PASSWORD, TRUSTSTORE_PASSWORD);
        attributes.put(FileTrustStore.STORE_URL, keystoreUrl);
        attributes.put(FileTrustStore.TRUST_STORE_TYPE, "PKCS12");

        TrustStore trustStore = _factory.create(TrustStore.class, attributes, _broker);

        Certificate[] certificates = trustStore.getCertificates();
        assertEquals("Unexpected number of certificates",
                            (long) getNumberOfCertificates(keystoreUrl, "PKCS12"),
                            (long) certificates.length);
    }

    @Test
    public void testPrivateKeyEntryIgnored() throws Exception
    {
        final URL keystoreUrl = getClass().getResource(KEYSTORE_RESOURCE);
        assertNotNull("Keystore not found", keystoreUrl);

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(FileTrustStore.NAME, getTestName());
        attributes.put(FileTrustStore.PASSWORD, BROKER_KEYSTORE_PASSWORD);
        attributes.put(FileTrustStore.STORE_URL, keystoreUrl);

        TrustStore trustStore = _factory.create(TrustStore.class, attributes, _broker);

        Certificate[] certificates = trustStore.getCertificates();
        assertEquals("Unexpected number of certificates",
                            (long) getNumberOfCertificates(keystoreUrl, "jks"),
                            (long) certificates.length);
    }

    private int getNumberOfCertificates(URL url, String type) throws Exception
    {
        KeyStore ks = KeyStore.getInstance(type);
        try(InputStream is = url.openStream())
        {
            ks.load(is, BROKER_KEYSTORE_PASSWORD.toCharArray());
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
