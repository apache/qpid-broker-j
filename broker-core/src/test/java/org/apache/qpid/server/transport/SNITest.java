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
 *
 */

package org.apache.qpid.server.transport;

import java.io.File;
import java.io.FileOutputStream;
import java.net.InetSocketAddress;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.qpid.server.SystemLauncher;
import org.apache.qpid.server.SystemLauncherListener.DefaultSystemLauncherListener;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.JsonSystemConfigImpl;
import org.apache.qpid.server.model.KeyStore;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.security.FileKeyStore;
import org.apache.qpid.server.security.auth.manager.AnonymousAuthenticationManager;
import org.apache.qpid.server.transport.network.security.ssl.SSLUtil;
import org.apache.qpid.server.transport.network.security.ssl.SSLUtil.KeyCertPair;
import org.apache.qpid.test.utils.QpidTestCase;
import org.apache.qpid.test.utils.TestFileUtils;

public class SNITest extends QpidTestCase
{
    private static final int SOCKET_TIMEOUT = 10000;
    private static final String KEYSTORE_PASSWORD = "password";

    private File _keyStoreFile;
    private KeyCertPair _fooValid;
    private KeyCertPair _fooInvalid;
    private KeyCertPair _barInvalid;
    private SystemLauncher _systemLauncher;
    private Broker<?> _broker;
    private int _boundPort;
    private File _brokerWork;

    @Override
    public void setUp() throws Exception
    {
        if(SSLUtil.canGenerateCerts())
        {

            _fooValid = SSLUtil.generateSelfSignedCertificate("RSA",
                                                              "SHA256WithRSA",
                                                              2048,
                                                              Instant.now().minus(1, ChronoUnit.DAYS).toEpochMilli(),
                                                              Duration.of(365, ChronoUnit.DAYS).getSeconds(),
                                                              "CN=foo",
                                                              Collections.emptySet(),
                                                              Collections.emptySet());
            _fooInvalid = SSLUtil.generateSelfSignedCertificate("RSA",
                                                                "SHA256WithRSA",
                                                                2048,
                                                                Instant.now().plus(1, ChronoUnit.HOURS).toEpochMilli(),
                                                                Duration.of(365, ChronoUnit.DAYS).getSeconds(),
                                                                "CN=foo",
                                                                Collections.emptySet(),
                                                                Collections.emptySet());

            _barInvalid = SSLUtil.generateSelfSignedCertificate("RSA",
                                                                "SHA256WithRSA",
                                                                2048,
                                                                Instant.now().plus(1, ChronoUnit.HOURS).toEpochMilli(),
                                                                Duration.of(365, ChronoUnit.DAYS).getSeconds(),
                                                                "CN=Qpid",
                                                                Collections.singleton("bar"),
                                                                Collections.emptySet());

            java.security.KeyStore inMemoryKeyStore =
                    java.security.KeyStore.getInstance(java.security.KeyStore.getDefaultType());

            inMemoryKeyStore.load(null, KEYSTORE_PASSWORD.toCharArray());
            inMemoryKeyStore.setKeyEntry("foovalid",
                                         _fooValid.getPrivateKey(),
                                         KEYSTORE_PASSWORD.toCharArray(),
                                         new X509Certificate[]{_fooValid.getCertificate()});

            inMemoryKeyStore.setKeyEntry("fooinvalid",
                                         _fooInvalid.getPrivateKey(),
                                         KEYSTORE_PASSWORD.toCharArray(),
                                         new X509Certificate[]{_fooInvalid.getCertificate()});

            inMemoryKeyStore.setKeyEntry("barinvalid",
                                         _barInvalid.getPrivateKey(),
                                         KEYSTORE_PASSWORD.toCharArray(),
                                         new X509Certificate[]{_barInvalid.getCertificate()});

            _keyStoreFile = File.createTempFile("keyStore", "jks");
            try (FileOutputStream os = new FileOutputStream(_keyStoreFile))
            {
                inMemoryKeyStore.store(os, KEYSTORE_PASSWORD.toCharArray());
            }
        }
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception
    {
        try
        {
            if (_systemLauncher != null)
            {
                _systemLauncher.shutdown();
            }

            if (_brokerWork != null)
            {
                _brokerWork.delete();
            }
            if (_keyStoreFile != null)
            {
                _keyStoreFile.delete();
            }
        }
        finally
        {
            super.tearDown();
        }

    }

    public void testValidCertChosen() throws Exception
    {
        performTest(true, "fooinvalid", "foo", _fooValid);
    }

    public void testMatchCertChosenEvenIfInvalid() throws Exception
    {
        performTest(true, "fooinvalid", "bar", _barInvalid);
    }

    public void testDefaultCertChose() throws Exception
    {
        performTest(true, "fooinvalid", null, _fooInvalid);
    }

    public void testMatchingCanBeDisabled() throws Exception
    {
        performTest(false, "fooinvalid", "foo", _fooInvalid);
    }


    private void performTest(final boolean useMatching,
                             final String defaultAlias,
                             final String sniHostName,
                             final KeyCertPair expectedCert) throws Exception
    {
        if (SSLUtil.canGenerateCerts())
        {
            doBrokerStartup(useMatching, defaultAlias);
            SSLContext context = SSLUtil.tryGetSSLContext();
            context.init(null,
                         new TrustManager[]
                                 {
                                         new X509TrustManager()
                                         {
                                             @Override
                                             public X509Certificate[] getAcceptedIssuers()
                                             {
                                                 return null;
                                             }

                                             @Override
                                             public void checkClientTrusted(X509Certificate[] certs, String authType)
                                             {
                                             }

                                             @Override
                                             public void checkServerTrusted(X509Certificate[] certs, String authType)
                                             {
                                             }
                                         }
                                 },
                         null);

            SSLSocketFactory socketFactory = context.getSocketFactory();
            try (SSLSocket socket = (SSLSocket) socketFactory.createSocket())
            {
                SSLParameters parameters = socket.getSSLParameters();
                if (sniHostName != null)
                {
                    parameters.setServerNames(Collections.singletonList(new SNIHostName(sniHostName)));
                }
                socket.setSSLParameters(parameters);
                InetSocketAddress address = new InetSocketAddress("localhost", _boundPort);
                socket.connect(address, SOCKET_TIMEOUT);

                final Certificate[] certs = socket.getSession().getPeerCertificates();
                assertEquals(1, certs.length);
                assertEquals(expectedCert.getCertificate(), certs[0]);
            }
        }
    }

    private void doBrokerStartup(boolean useMatching, String defaultAlias) throws Exception
    {
        final File initialConfiguration = createInitialContext();
        _brokerWork = TestFileUtils.createTestDirectory("qpid-work", true);

        Map<String, String> context = new HashMap<>();
        context.put("qpid.work_dir", _brokerWork.toString());

        Map<String,Object> attributes = new HashMap<>();
        attributes.put(SystemConfig.INITIAL_CONFIGURATION_LOCATION, initialConfiguration.getAbsolutePath());
        attributes.put(SystemConfig.TYPE, JsonSystemConfigImpl.SYSTEM_CONFIG_TYPE);
        attributes.put(SystemConfig.CONTEXT, context);
        _systemLauncher = new SystemLauncher(new DefaultSystemLauncherListener()
        {
            @Override
            public void onContainerResolve(final SystemConfig<?> systemConfig)
            {
                _broker = systemConfig.getContainer(Broker.class);
            }
        });
        _systemLauncher.startup(attributes);

        final Map<String, Object> authProviderAttr = new HashMap<>();
        authProviderAttr.put(AuthenticationProvider.NAME, "myAuthProvider");
        authProviderAttr.put(AuthenticationProvider.TYPE, AnonymousAuthenticationManager.PROVIDER_TYPE);

        final AuthenticationProvider authProvider = _broker.createChild(AuthenticationProvider.class, authProviderAttr);

        Map<String, Object> keyStoreAttr = new HashMap<>();
        keyStoreAttr.put(FileKeyStore.NAME, "myKeyStore");
        keyStoreAttr.put(FileKeyStore.STORE_URL, _keyStoreFile.toURI().toURL().toString());
        keyStoreAttr.put(FileKeyStore.PASSWORD, KEYSTORE_PASSWORD);
        keyStoreAttr.put(FileKeyStore.USE_HOST_NAME_MATCHING, useMatching);
        keyStoreAttr.put(FileKeyStore.CERTIFICATE_ALIAS, defaultAlias);

        final KeyStore keyStore = _broker.createChild(KeyStore.class, keyStoreAttr);

        Map<String, Object> portAttr = new HashMap<>();
        portAttr.put(Port.NAME, "myPort");
        portAttr.put(Port.TYPE, "AMQP");
        portAttr.put(Port.TRANSPORTS, Collections.singleton(Transport.SSL));
        portAttr.put(Port.PORT, 0);
        portAttr.put(Port.AUTHENTICATION_PROVIDER, authProvider);
        portAttr.put(Port.KEY_STORE, keyStore);

        final Port<?> port = _broker.createChild(Port.class, portAttr);

        _boundPort = port.getBoundPort();
    }

    private File createInitialContext() throws JsonProcessingException
    {
        // create empty initial configuration
        Map<String,Object> initialConfig = new HashMap<>();
        initialConfig.put(ConfiguredObject.NAME, "test");
        initialConfig.put(Broker.MODEL_VERSION, BrokerModel.MODEL_VERSION);

        ObjectMapper mapper = new ObjectMapper();
        String config = mapper.writeValueAsString(initialConfig);
        return TestFileUtils.createTempFile(this, ".initial-config.json", config);
    }
}
