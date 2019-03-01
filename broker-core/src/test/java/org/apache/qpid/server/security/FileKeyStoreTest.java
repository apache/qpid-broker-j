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


import static org.apache.qpid.server.security.FileTrustStoreTest.SYMMETRIC_KEY_KEYSTORE_RESOURCE;
import static org.apache.qpid.server.security.FileTrustStoreTest.createDataUrlForFile;
import static org.apache.qpid.test.utils.TestSSLConstants.JAVA_KEYSTORE_TYPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import javax.net.ssl.KeyManager;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.ConfiguredObjectFactory;
import org.apache.qpid.server.model.KeyStore;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.util.DataUrlUtils;
import org.apache.qpid.test.utils.TestSSLConstants;
import org.apache.qpid.test.utils.UnitTestBase;

public class FileKeyStoreTest extends UnitTestBase
{
    static final String EMPTY_KEYSTORE_RESOURCE = "/ssl/test_empty_keystore.jks";
    private static final String KEYSTORE_CERTIFICATE_ONLY_RESOURCE = "/ssl/test_cert_only_keystore.pkcs12";
    private static final String BROKER_KEYSTORE = "ssl/java_broker_keystore.pkcs12";
    private static final String BROKER_KEYSTORE_PATH = "classpath:" + BROKER_KEYSTORE;
    private static final String BROKER_KEYSTORE_PASSWORD = TestSSLConstants.BROKER_KEYSTORE_PASSWORD;
    private static final String CLIENT_KEYSTORE_PATH = "classpath:ssl/java_client_keystore.pkcs12";
    private static final String CLIENT_KEYSTORE_PASSWORD = TestSSLConstants.KEYSTORE_PASSWORD;
    private static final String BROKER_KEYSTORE_ALIAS = TestSSLConstants.BROKER_KEYSTORE_ALIAS;

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
    public void testCreateKeyStoreFromFile_Success() throws Exception
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileKeyStore.NAME, "myFileKeyStore");
        attributes.put(FileKeyStore.STORE_URL, BROKER_KEYSTORE_PATH);
        attributes.put(FileKeyStore.PASSWORD, BROKER_KEYSTORE_PASSWORD);
        attributes.put(FileKeyStore.KEY_STORE_TYPE, JAVA_KEYSTORE_TYPE);

        FileKeyStoreImpl fileKeyStore = (FileKeyStoreImpl) _factory.create(KeyStore.class, attributes,  _broker);

        KeyManager[] keyManager = fileKeyStore.getKeyManagers();
        assertNotNull(keyManager);
        assertEquals("Unexpected number of key managers", (long) 1, (long) keyManager.length);
        assertNotNull("Key manager unexpected null", keyManager[0]);
    }

    @Test
    public void testCreateKeyStoreWithAliasFromFile_Success() throws Exception
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileKeyStore.NAME, "myFileKeyStore");
        attributes.put(FileKeyStore.STORE_URL, BROKER_KEYSTORE_PATH);
        attributes.put(FileKeyStore.PASSWORD, BROKER_KEYSTORE_PASSWORD);
        attributes.put(FileKeyStore.CERTIFICATE_ALIAS, BROKER_KEYSTORE_ALIAS);
        attributes.put(FileKeyStore.KEY_STORE_TYPE, JAVA_KEYSTORE_TYPE);

        FileKeyStoreImpl fileKeyStore = (FileKeyStoreImpl) _factory.create(KeyStore.class, attributes,  _broker);

        KeyManager[] keyManager = fileKeyStore.getKeyManagers();
        assertNotNull(keyManager);
        assertEquals("Unexpected number of key managers", (long) 1, (long) keyManager.length);
        assertNotNull("Key manager unexpected null", keyManager[0]);
    }

    @Test
    public void testCreateKeyStoreFromFile_WrongPassword() throws Exception
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileKeyStore.NAME, "myFileKeyStore");
        attributes.put(FileKeyStore.STORE_URL, BROKER_KEYSTORE_PATH);
        attributes.put(FileKeyStore.PASSWORD, "wrong");
        attributes.put(FileKeyStore.KEY_STORE_TYPE, JAVA_KEYSTORE_TYPE);

        try
        {
            _factory.create(KeyStore.class, attributes,  _broker);
            fail("Exception not thrown");
        }
        catch (IllegalConfigurationException ice)
        {
            String message = ice.getMessage();
            assertTrue("Exception text not as unexpected:" + message,
                              message.contains("Check key store password"));

        }
    }

    @Test
    public void testCreateKeyStoreFromFile_UnknownAlias() throws Exception
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileKeyStore.NAME, "myFileKeyStore");
        attributes.put(FileKeyStore.STORE_URL, CLIENT_KEYSTORE_PATH);
        attributes.put(FileKeyStore.PASSWORD, CLIENT_KEYSTORE_PASSWORD);
        attributes.put(FileKeyStore.CERTIFICATE_ALIAS, "notknown");
        attributes.put(FileKeyStore.KEY_STORE_TYPE, JAVA_KEYSTORE_TYPE);

        try
        {
            _factory.create(KeyStore.class, attributes,  _broker);
            fail("Exception not thrown");
        }
        catch (IllegalConfigurationException ice)
        {
            String message = ice.getMessage();
            assertTrue("Exception text not as unexpected:" + message,
                              message.contains("Cannot find a certificate with alias 'notknown' in key store"));
        }
    }

    @Test
    public void testCreateKeyStoreFromFile_NonKeyAlias() throws Exception
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileKeyStore.NAME, "myFileKeyStore");
        attributes.put(FileKeyStore.STORE_URL, CLIENT_KEYSTORE_PATH);
        attributes.put(FileKeyStore.PASSWORD, CLIENT_KEYSTORE_PASSWORD);
        attributes.put(FileKeyStore.CERTIFICATE_ALIAS, "rootca");
        attributes.put(FileKeyStore.KEY_STORE_TYPE, JAVA_KEYSTORE_TYPE);

        try
        {
            _factory.create(KeyStore.class, attributes,  _broker);
            fail("Exception not thrown");
        }
        catch (IllegalConfigurationException ice)
        {
            String message = ice.getMessage();
            assertTrue("Exception text not as unexpected:" + message,
                              message.contains("does not identify a private key"));
        }
    }

    @Test
    public void testCreateKeyStoreFromDataUrl_Success() throws Exception
    {
        String trustStoreAsDataUrl = createDataUrlForFile(BROKER_KEYSTORE);

        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileKeyStore.NAME, "myFileKeyStore");
        attributes.put(FileKeyStore.STORE_URL, trustStoreAsDataUrl);
        attributes.put(FileKeyStore.PASSWORD, BROKER_KEYSTORE_PASSWORD);
        attributes.put(FileKeyStore.KEY_STORE_TYPE, JAVA_KEYSTORE_TYPE);

        FileKeyStoreImpl fileKeyStore = (FileKeyStoreImpl) _factory.create(KeyStore.class, attributes,  _broker);

        KeyManager[] keyManagers = fileKeyStore.getKeyManagers();
        assertNotNull(keyManagers);
        assertEquals("Unexpected number of key managers", (long) 1, (long) keyManagers.length);
        assertNotNull("Key manager unexpected null", keyManagers[0]);
    }

    @Test
    public void testCreateKeyStoreWithAliasFromDataUrl_Success() throws Exception
    {
        String trustStoreAsDataUrl = createDataUrlForFile(BROKER_KEYSTORE);

        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileKeyStore.NAME, "myFileKeyStore");
        attributes.put(FileKeyStore.STORE_URL, trustStoreAsDataUrl);
        attributes.put(FileKeyStore.PASSWORD, BROKER_KEYSTORE_PASSWORD);
        attributes.put(FileKeyStore.CERTIFICATE_ALIAS, BROKER_KEYSTORE_ALIAS);
        attributes.put(FileKeyStore.KEY_STORE_TYPE, JAVA_KEYSTORE_TYPE);

        FileKeyStoreImpl fileKeyStore = (FileKeyStoreImpl) _factory.create(KeyStore.class, attributes,  _broker);

        KeyManager[] keyManagers = fileKeyStore.getKeyManagers();
        assertNotNull(keyManagers);
        assertEquals("Unexpected number of key managers", (long) 1, (long) keyManagers.length);
        assertNotNull("Key manager unexpected null", keyManagers[0]);
    }

    @Test
    public void testCreateKeyStoreFromDataUrl_WrongPassword() throws Exception
    {
        String keyStoreAsDataUrl = createDataUrlForFile(BROKER_KEYSTORE);

        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileKeyStore.NAME, "myFileKeyStore");
        attributes.put(FileKeyStore.PASSWORD, "wrong");
        attributes.put(FileKeyStore.STORE_URL, keyStoreAsDataUrl);
        attributes.put(FileKeyStore.KEY_STORE_TYPE, JAVA_KEYSTORE_TYPE);

        try
        {
            _factory.create(KeyStore.class, attributes,  _broker);
            fail("Exception not thrown");
        }
        catch (IllegalConfigurationException ice)
        {
            String message = ice.getMessage();
            assertTrue("Exception text not as unexpected:" + message,
                              message.contains("Check key store password"));
        }
    }

    @Test
    public void testCreateKeyStoreFromDataUrl_BadKeystoreBytes() throws Exception
    {
        String keyStoreAsDataUrl = DataUrlUtils.getDataUrlForBytes("notatruststore".getBytes());

        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileKeyStore.NAME, "myFileKeyStore");
        attributes.put(FileKeyStore.PASSWORD, BROKER_KEYSTORE_PASSWORD);
        attributes.put(FileKeyStore.STORE_URL, keyStoreAsDataUrl);

        try
        {
            _factory.create(KeyStore.class, attributes,  _broker);
            fail("Exception not thrown");
        }
        catch (IllegalConfigurationException ice)
        {
            String message = ice.getMessage();
            assertTrue("Exception text not as unexpected:" + message,
                              message.contains("Cannot instantiate key store"));
        }
    }

    @Test
    public void testCreateKeyStoreFromDataUrl_UnknownAlias() throws Exception
    {
        String keyStoreAsDataUrl = createDataUrlForFile(BROKER_KEYSTORE);

        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileKeyStore.NAME, "myFileKeyStore");
        attributes.put(FileKeyStore.PASSWORD, BROKER_KEYSTORE_PASSWORD);
        attributes.put(FileKeyStore.STORE_URL, keyStoreAsDataUrl);
        attributes.put(FileKeyStore.CERTIFICATE_ALIAS, "notknown");
        attributes.put(FileKeyStore.KEY_STORE_TYPE, JAVA_KEYSTORE_TYPE);

        try
        {
            _factory.create(KeyStore.class, attributes,  _broker);
            fail("Exception not thrown");
        }
        catch (IllegalConfigurationException ice)
        {
            String message = ice.getMessage();
            assertTrue("Exception text not as unexpected:" + message,
                              message.contains("Cannot find a certificate with alias 'notknown' in key store"));
        }
    }

    @Test
    public void testEmptyKeystoreRejected() throws Exception
    {
        final URL emptyKeystore = getClass().getResource(EMPTY_KEYSTORE_RESOURCE);
        assertNotNull("Empty keystore not found", emptyKeystore);

        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileKeyStore.NAME, "myFileKeyStore");
        attributes.put(FileKeyStore.PASSWORD, BROKER_KEYSTORE_PASSWORD);
        attributes.put(FileKeyStore.STORE_URL, emptyKeystore);

        try
        {
            _factory.create(KeyStore.class, attributes,  _broker);
            fail("Exception not thrown");
        }
        catch (IllegalConfigurationException ice)
        {
            // pass
        }
    }

    @Test
    public void testKeystoreWithNoPrivateKeyRejected()
    {
        final URL keystoreUrl = getClass().getResource(KEYSTORE_CERTIFICATE_ONLY_RESOURCE);
        assertNotNull("Keystore not found", keystoreUrl);

        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileKeyStore.NAME, getTestName());
        attributes.put(FileKeyStore.PASSWORD, BROKER_KEYSTORE_PASSWORD);
        attributes.put(FileKeyStore.STORE_URL, keystoreUrl);
        attributes.put(FileKeyStore.KEY_STORE_TYPE, JAVA_KEYSTORE_TYPE);

        try
        {
            _factory.create(KeyStore.class, attributes,  _broker);
            fail("Exception not thrown");
        }
        catch (IllegalConfigurationException ice)
        {
            String message = ice.getMessage();
            assertTrue("Exception text not as unexpected:" + message,
                              message.contains("must contain at least one private key"));
        }
    }

    @Test
    public void testSymmetricKeysIgnored()
    {
        final URL keystoreUrl = getClass().getResource(SYMMETRIC_KEY_KEYSTORE_RESOURCE);
        assertNotNull("Keystore not found", keystoreUrl);

        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileKeyStore.NAME, "myFileKeyStore");
        attributes.put(FileKeyStore.PASSWORD, BROKER_KEYSTORE_PASSWORD);
        attributes.put(FileKeyStore.STORE_URL, keystoreUrl);
        attributes.put(FileKeyStore.KEY_STORE_TYPE, JAVA_KEYSTORE_TYPE);

        KeyStore keyStore = _factory.create(KeyStore.class, attributes,  _broker);
        assertNotNull(keyStore);
    }

    @Test
    public void testUpdateKeyStore_Success() throws Exception
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(FileKeyStore.NAME, "myFileKeyStore");
        attributes.put(FileKeyStore.STORE_URL, BROKER_KEYSTORE_PATH);
        attributes.put(FileKeyStore.PASSWORD, BROKER_KEYSTORE_PASSWORD);
        attributes.put(FileKeyStore.KEY_STORE_TYPE, JAVA_KEYSTORE_TYPE);

        FileKeyStoreImpl fileKeyStore = (FileKeyStoreImpl) _factory.create(KeyStore.class, attributes,  _broker);

        assertNull("Unexpected alias value before change", fileKeyStore.getCertificateAlias());

        try
        {
            Map<String,Object> unacceptableAttributes = new HashMap<>();
            unacceptableAttributes.put(FileKeyStore.CERTIFICATE_ALIAS, "notknown");

            fileKeyStore.setAttributes(unacceptableAttributes);
            fail("Exception not thrown");
        }
        catch (IllegalConfigurationException ice)
        {
            String message = ice.getMessage();
            assertTrue("Exception text not as unexpected:" + message,
                              message.contains("Cannot find a certificate with alias 'notknown' in key store"));
        }

        assertNull("Unexpected alias value after failed change", fileKeyStore.getCertificateAlias());

        Map<String,Object> changedAttributes = new HashMap<>();
        changedAttributes.put(FileKeyStore.CERTIFICATE_ALIAS, BROKER_KEYSTORE_ALIAS);

        fileKeyStore.setAttributes(changedAttributes);

        assertEquals("Unexpected alias value after change that is expected to be successful",
                     BROKER_KEYSTORE_ALIAS,
                            fileKeyStore.getCertificateAlias());

    }

}
