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


import static org.apache.qpid.test.utils.TestSSLConstants.JAVA_KEYSTORE_TYPE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.security.cert.CertificateExpiredException;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import com.google.common.io.ByteStreams;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.ConfiguredObjectFactory;
import org.apache.qpid.server.model.IntegrityViolationException;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.server.security.auth.manager.SimpleLDAPAuthenticationManager;
import org.apache.qpid.server.transport.network.security.ssl.QpidPeersOnlyTrustManager;
import org.apache.qpid.server.transport.network.security.ssl.SSLUtil;
import org.apache.qpid.server.util.DataUrlUtils;
import org.apache.qpid.test.utils.QpidTestCase;
import org.apache.qpid.test.utils.TestSSLConstants;

public class FileTrustStoreTest extends QpidTestCase
{
    private static final String TRUSTSTORE_PASSWORD = TestSSLConstants.TRUSTSTORE_PASSWORD;
    private static final String PEER_STORE_PASSWORD = TestSSLConstants.BROKER_PEERSTORE_PASSWORD;
    private static final String KEYSTORE_PASSWORD = TestSSLConstants.KEYSTORE_PASSWORD;
    private static final String TRUST_STORE_PATH = "classpath:ssl/java_client_truststore.pkcs12";
    private static final String PEER_STORE_PATH = "classpath:ssl/java_broker_peerstore.pkcs12";
    private static final String EXPIRED_TRUST_STORE_PATH = "classpath:ssl/java_broker_expired_truststore.pkcs12";
    private static final String EXPIRED_KEYSTORE_PATH = "ssl/java_client_expired_keystore.pkcs12";
    private static final String TRUST_STORE = "ssl/java_client_truststore.pkcs12";
    private static final String BROKER_TRUST_STORE_PATH = "classpath:ssl/java_broker_truststore.pkcs12";
    private static final String BROKER_TRUST_STORE_PASSWORD = TestSSLConstants.BROKER_TRUSTSTORE_PASSWORD;
    private final Broker _broker = mock(Broker.class);
    private final TaskExecutor _taskExecutor = CurrentThreadTaskExecutor.newStartedInstance();
    private final Model _model = BrokerModel.getInstance();
    private final ConfiguredObjectFactory _factory = _model.getObjectFactory();

    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        when(_broker.getTaskExecutor()).thenReturn(_taskExecutor);
        when(_broker.getChildExecutor()).thenReturn(_taskExecutor);

        when(_broker.getModel()).thenReturn(_model);
        when(_broker.getCategoryClass()).thenReturn(Broker.class);
        when(_broker.getEventLogger()).thenReturn(new EventLogger());
        when(_broker.getTypeClass()).thenReturn(Broker.class);
    }

    public void testCreateTrustStoreFromFile_Success() throws Exception
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileTrustStore.NAME, "myFileTrustStore");
        attributes.put(FileTrustStore.STORE_URL, TRUST_STORE_PATH);
        attributes.put(FileTrustStore.PASSWORD, TRUSTSTORE_PASSWORD);
        attributes.put(FileTrustStore.TRUST_STORE_TYPE, JAVA_KEYSTORE_TYPE);

        TrustStore<?> fileTrustStore = _factory.create(TrustStore.class, attributes,  _broker);

        TrustManager[] trustManagers = fileTrustStore.getTrustManagers();
        assertNotNull(trustManagers);
        assertEquals("Unexpected number of trust managers", 1, trustManagers.length);
        assertNotNull("Trust manager unexpected null", trustManagers[0]);
    }

    public void testCreateTrustStoreFromFile_WrongPassword() throws Exception
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileTrustStore.NAME, "myFileTrustStore");
        attributes.put(FileTrustStore.STORE_URL, TRUST_STORE_PATH);
        attributes.put(FileTrustStore.PASSWORD, "wrong");
        attributes.put(FileTrustStore.TRUST_STORE_TYPE, JAVA_KEYSTORE_TYPE);

        try
        {
            _factory.create(TrustStore.class, attributes,  _broker);
            fail("Exception not thrown");
        }
        catch (IllegalConfigurationException ice)
        {
            String message = ice.getMessage();
            assertTrue("Exception text not as unexpected:" + message, message.contains("Check trust store password"));
        }
    }

    public void testCreatePeersOnlyTrustStoreFromFile_Success() throws Exception
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileTrustStore.NAME, "myFileTrustStore");
        attributes.put(FileTrustStore.STORE_URL, PEER_STORE_PATH);
        attributes.put(FileTrustStore.PASSWORD, PEER_STORE_PASSWORD);
        attributes.put(FileTrustStore.PEERS_ONLY, true);
        attributes.put(FileTrustStore.TRUST_STORE_TYPE, JAVA_KEYSTORE_TYPE);

        TrustStore<?> fileTrustStore = _factory.create(TrustStore.class, attributes,  _broker);

        TrustManager[] trustManagers = fileTrustStore.getTrustManagers();
        assertNotNull(trustManagers);
        assertEquals("Unexpected number of trust managers", 1, trustManagers.length);
        assertNotNull("Trust manager unexpected null", trustManagers[0]);
        assertTrue("Trust manager unexpected null", trustManagers[0] instanceof QpidPeersOnlyTrustManager);
    }

    public void testUseOfExpiredTrustAnchorAllowed() throws Exception
    {
        if (getJvmVendor() == JvmVendor.IBM)
        {
            //IBMJSSE2 trust factory (IbmX509) validates the entire chain, including trusted certificates.
            return;
        }
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileTrustStore.NAME, "myFileTrustStore");
        attributes.put(FileTrustStore.STORE_URL, EXPIRED_TRUST_STORE_PATH);
        attributes.put(FileTrustStore.PASSWORD, BROKER_TRUST_STORE_PASSWORD);
        attributes.put(FileTrustStore.TRUST_STORE_TYPE, JAVA_KEYSTORE_TYPE);

        TrustStore trustStore = _factory.create(TrustStore.class, attributes, _broker);

        TrustManager[] trustManagers = trustStore.getTrustManagers();
        assertNotNull(trustManagers);
        assertEquals("Unexpected number of trust managers", 1, trustManagers.length);
        assertTrue("Unexpected trust manager type",trustManagers[0] instanceof X509TrustManager);
        X509TrustManager trustManager = (X509TrustManager) trustManagers[0];

        KeyStore clientStore = SSLUtil.getInitializedKeyStore(EXPIRED_KEYSTORE_PATH,
                                                              KEYSTORE_PASSWORD,
                                                              JAVA_KEYSTORE_TYPE);
        String alias = clientStore.aliases().nextElement();
        X509Certificate certificate = (X509Certificate) clientStore.getCertificate(alias);

        trustManager.checkClientTrusted(new X509Certificate[] {certificate}, "NULL");
    }

    public void testUseOfExpiredTrustAnchorDenied() throws Exception
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileTrustStore.NAME, "myFileTrustStore");
        attributes.put(FileTrustStore.STORE_URL, EXPIRED_TRUST_STORE_PATH);
        attributes.put(FileTrustStore.PASSWORD, BROKER_TRUST_STORE_PASSWORD);
        attributes.put(FileTrustStore.TRUST_ANCHOR_VALIDITY_ENFORCED, true);
        attributes.put(FileTrustStore.TRUST_STORE_TYPE, JAVA_KEYSTORE_TYPE);

        TrustStore trustStore = _factory.create(TrustStore.class, attributes, _broker);

        TrustManager[] trustManagers = trustStore.getTrustManagers();
        assertNotNull(trustManagers);
        assertEquals("Unexpected number of trust managers", 1, trustManagers.length);
        assertTrue("Unexpected trust manager type",trustManagers[0] instanceof X509TrustManager);
        X509TrustManager trustManager = (X509TrustManager) trustManagers[0];

        KeyStore clientStore = SSLUtil.getInitializedKeyStore(EXPIRED_KEYSTORE_PATH,
                                                              KEYSTORE_PASSWORD,
                                                              JAVA_KEYSTORE_TYPE);
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

    public void testCreateTrustStoreFromDataUrl_Success() throws Exception
    {
        String trustStoreAsDataUrl = createDataUrlForFile(TRUST_STORE);

        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileTrustStore.NAME, "myFileTrustStore");
        attributes.put(FileTrustStore.STORE_URL, trustStoreAsDataUrl);
        attributes.put(FileTrustStore.PASSWORD, TRUSTSTORE_PASSWORD);
        attributes.put(FileTrustStore.TRUST_STORE_TYPE, JAVA_KEYSTORE_TYPE);

        TrustStore<?> fileTrustStore = _factory.create(TrustStore.class, attributes,  _broker);

        TrustManager[] trustManagers = fileTrustStore.getTrustManagers();
        assertNotNull(trustManagers);
        assertEquals("Unexpected number of trust managers", 1, trustManagers.length);
        assertNotNull("Trust manager unexpected null", trustManagers[0]);
    }

    public void testCreateTrustStoreFromDataUrl_WrongPassword() throws Exception
    {
        String trustStoreAsDataUrl = createDataUrlForFile(TRUST_STORE);

        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileTrustStore.NAME, "myFileTrustStore");
        attributes.put(FileTrustStore.PASSWORD, "wrong");
        attributes.put(FileTrustStore.STORE_URL, trustStoreAsDataUrl);
        attributes.put(FileTrustStore.TRUST_STORE_TYPE, JAVA_KEYSTORE_TYPE);

        try
        {
            _factory.create(TrustStore.class, attributes,  _broker);
            fail("Exception not thrown");
        }
        catch (IllegalConfigurationException ice)
        {
            String message = ice.getMessage();
            assertTrue("Exception text not as unexpected:" + message, message.contains("Check trust store password"));
        }
    }

    public void testCreateTrustStoreFromDataUrl_BadTruststoreBytes() throws Exception
    {
        String trustStoreAsDataUrl = DataUrlUtils.getDataUrlForBytes("notatruststore".getBytes());

        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileTrustStore.NAME, "myFileTrustStore");
        attributes.put(FileTrustStore.PASSWORD, TRUSTSTORE_PASSWORD);
        attributes.put(FileTrustStore.STORE_URL, trustStoreAsDataUrl);
        attributes.put(FileTrustStore.TRUST_STORE_TYPE, JAVA_KEYSTORE_TYPE);

        try
        {
            _factory.create(TrustStore.class, attributes,  _broker);
            fail("Exception not thrown");
        }
        catch (IllegalConfigurationException ice)
        {
            String message = ice.getMessage();
            assertTrue("Exception text not as unexpected:" + message, message.contains("Cannot instantiate trust store"));

        }
    }

    public void testUpdateTrustStore_Success() throws Exception
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileTrustStore.NAME, "myFileTrustStore");
        attributes.put(FileTrustStore.STORE_URL, TRUST_STORE_PATH);
        attributes.put(FileTrustStore.PASSWORD, TRUSTSTORE_PASSWORD);
        attributes.put(FileTrustStore.TRUST_STORE_TYPE, JAVA_KEYSTORE_TYPE);

        FileTrustStore<?> fileTrustStore = (FileTrustStore<?>) _factory.create(TrustStore.class, attributes,  _broker);

        assertEquals("Unexpected path value before change", TRUST_STORE_PATH, fileTrustStore.getStoreUrl());

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
            assertTrue("Exception text not as unexpected:" + message, message.contains("Cannot instantiate trust store"));
        }

        assertEquals("Unexpected path value after failed change", TRUST_STORE_PATH, fileTrustStore.getStoreUrl());

        Map<String,Object> changedAttributes = new HashMap<>();
        changedAttributes.put(FileTrustStore.STORE_URL, BROKER_TRUST_STORE_PATH);
        changedAttributes.put(FileTrustStore.PASSWORD, BROKER_TRUST_STORE_PASSWORD);

        fileTrustStore.setAttributes(changedAttributes);

        assertEquals("Unexpected path value after change that is expected to be successful",
                     BROKER_TRUST_STORE_PATH,
                     fileTrustStore.getStoreUrl());
    }

    public void testDeleteTrustStore_Success() throws Exception
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileTrustStore.NAME, "myFileTrustStore");
        attributes.put(FileTrustStore.STORE_URL, BROKER_TRUST_STORE_PATH);
        attributes.put(FileTrustStore.PASSWORD, KEYSTORE_PASSWORD);
        attributes.put(FileTrustStore.TRUST_STORE_TYPE, JAVA_KEYSTORE_TYPE);

        TrustStore<?> fileTrustStore = _factory.create(TrustStore.class, attributes,  _broker);

        fileTrustStore.delete();
    }

    public void testDeleteTrustStore_TrustManagerInUseByAuthProvider() throws Exception
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileTrustStore.NAME, "myFileTrustStore");
        attributes.put(FileTrustStore.PASSWORD, TRUSTSTORE_PASSWORD);
        attributes.put(FileTrustStore.STORE_URL, TRUST_STORE_PATH);
        attributes.put(FileTrustStore.TRUST_STORE_TYPE, JAVA_KEYSTORE_TYPE);

        TrustStore<?> fileTrustStore = _factory.create(TrustStore.class, attributes,  _broker);

        SimpleLDAPAuthenticationManager ldap = mock(SimpleLDAPAuthenticationManager.class);
        when(ldap.getTrustStore()).thenReturn(fileTrustStore);

        Collection<AuthenticationProvider<?>> authenticationProviders = Collections.<AuthenticationProvider<?>>singletonList(ldap);
        when(_broker.getAuthenticationProviders()).thenReturn(authenticationProviders);

        try
        {
            fileTrustStore.delete();
            fail("Exception not thrown");
        }
        catch (IntegrityViolationException ive)
        {
            // PASS
        }
    }

    public void testDeleteTrustStore_TrustManagerInUseByPort() throws Exception
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileTrustStore.NAME, "myFileTrustStore");
        attributes.put(FileTrustStore.STORE_URL, TRUST_STORE_PATH);
        attributes.put(FileTrustStore.PASSWORD, TRUSTSTORE_PASSWORD);
        attributes.put(FileTrustStore.TRUST_STORE_TYPE, JAVA_KEYSTORE_TYPE);

        TrustStore<?> fileTrustStore = _factory.create(TrustStore.class, attributes, _broker);

        Port<?> port = mock(Port.class);
        when(port.getTrustStores()).thenReturn(Collections.<TrustStore>singletonList(fileTrustStore));

        when(_broker.getPorts()).thenReturn(Collections.<Port<?>>singletonList(port));

        try
        {
            fileTrustStore.delete();
            fail("Exception not thrown");
        }
        catch (IntegrityViolationException ive)
        {
            // PASS
        }
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
