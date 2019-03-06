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
import static org.apache.qpid.test.utils.TestSSLConstants.KEYSTORE_PASSWORD;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.argThat;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.KeyManager;
import javax.xml.bind.DatatypeConverter;

import org.mockito.ArgumentMatcher;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.LogMessage;
import org.apache.qpid.server.logging.MessageLogger;
import org.apache.qpid.server.logging.messages.KeyStoreMessages;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.ConfiguredObjectFactory;
import org.apache.qpid.server.model.KeyStore;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.test.utils.QpidTestCase;
import org.apache.qpid.test.utils.TestFileUtils;

public class NonJavaKeyStoreTest extends QpidTestCase
{
    private static final String KEYSTORE = "/ssl/java_broker_keystore.pkcs12";

    private final Broker<?> _broker = mock(Broker.class);
    private final TaskExecutor _taskExecutor = CurrentThreadTaskExecutor.newStartedInstance();
    private final Model _model = BrokerModel.getInstance();
    private final ConfiguredObjectFactory _factory = _model.getObjectFactory();
    private List<File> _testResources;
    private MessageLogger _messageLogger;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        _messageLogger = mock(MessageLogger.class);
        when(_broker.getTaskExecutor()).thenReturn(_taskExecutor);
        when(_broker.getChildExecutor()).thenReturn(_taskExecutor);
        when(_broker.getModel()).thenReturn(_model);
        when(_broker.getEventLogger()).thenReturn(new EventLogger(_messageLogger));
        when(((Broker) _broker).getCategoryClass()).thenReturn(Broker.class);
        _testResources = new ArrayList<>();
    }

    @Override
    public void tearDown() throws Exception
    {
        try
        {
            super.tearDown();
        }
        finally
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
                kos.write("-----BEGIN PRIVATE KEY-----\n".getBytes());
                String base64encoded = DatatypeConverter.printBase64Binary(pvt.getEncoded());
                while(base64encoded.length() > 76)
                {
                    kos.write(base64encoded.substring(0,76).getBytes());
                    kos.write("\n".getBytes());
                    base64encoded = base64encoded.substring(76);
                }

                kos.write(base64encoded.getBytes());
                kos.write("\n-----END PRIVATE KEY-----".getBytes());
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
            Certificate pub = ks.getCertificate("rootca");
            if (pem)
            {
                cos.write("-----BEGIN CERTIFICATE-----\n".getBytes());
                String base64encoded = DatatypeConverter.printBase64Binary(pub.getEncoded());
                while(base64encoded.length() > 76)
                {
                    cos.write(base64encoded.substring(0,76).getBytes());
                    cos.write("\n".getBytes());
                    base64encoded = base64encoded.substring(76);
                }
                cos.write(base64encoded.getBytes());

                cos.write("\n-----END CERTIFICATE-----".getBytes());
            }
            else
            {
                cos.write(pub.getEncoded());
            }
            cos.flush();
        }

        return new File[]{privateKeyFile,certificateFile};
    }

    public void testCreationOfTrustStoreFromValidPrivateKeyAndCertificateInDERFormat() throws Exception
    {
        runTestCreationOfTrustStoreFromValidPrivateKeyAndCertificateInDerFormat(false);
    }

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
        assertEquals("Unexpected number of key managers", 1, keyManagers.length);
        assertNotNull("Key manager is null", keyManagers[0]);
    }

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

    public void testExpiryCheckingFindsExpired() throws Exception
    {
        doCertExpiryChecking(1);

        verify(_messageLogger, times(1)).message(argThat(new LogMessageArgumentMatcher()));

    }

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


    private static class LogMessageArgumentMatcher extends ArgumentMatcher<LogMessage>
    {
        @Override
        public boolean matches(final Object argument)
        {
            LogMessage arg = (LogMessage)argument;
            return arg.getLogHierarchy().equals(KeyStoreMessages.EXPIRING_LOG_HIERARCHY);
        }
    }
}
