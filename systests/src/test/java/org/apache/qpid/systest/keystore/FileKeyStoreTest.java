/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.qpid.systest.keystore;

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

import org.apache.qpid.server.model.DefaultVirtualHostAlias;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.VirtualHostAlias;
import org.apache.qpid.server.model.VirtualHostNameAlias;
import org.apache.qpid.server.security.FileKeyStore;
import org.apache.qpid.server.transport.network.security.ssl.SSLUtil;
import org.apache.qpid.test.utils.QpidBrokerTestCase;
import org.apache.qpid.test.utils.TestBrokerConfiguration;

public class FileKeyStoreTest extends QpidBrokerTestCase
{
    private File _keyStoreFile;
    private SSLUtil.KeyCertPair _fooValid;
    private SSLUtil.KeyCertPair _fooInvalid;
    private SSLUtil.KeyCertPair _barInvalid;
    private TrustManager[] _clientTrustManagers;

    @Override
    public void setUp() throws Exception
    {
        setSystemProperty("javax.net.debug", "ssl");
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

            inMemoryKeyStore.load(null, "password".toCharArray());
            inMemoryKeyStore.setKeyEntry("foovalid",
                                         _fooValid.getPrivateKey(),
                                         "password".toCharArray(),
                                         new X509Certificate[]{_fooValid.getCertificate()});

            inMemoryKeyStore.setKeyEntry("fooinvalid",
                                         _fooInvalid.getPrivateKey(),
                                         "password".toCharArray(),
                                         new X509Certificate[]{_fooInvalid.getCertificate()});

            inMemoryKeyStore.setKeyEntry("barinvalid",
                                         _barInvalid.getPrivateKey(),
                                         "password".toCharArray(),
                                         new X509Certificate[]{_barInvalid.getCertificate()});

            _keyStoreFile = File.createTempFile("keyStore", "jks");
            try (FileOutputStream os = new FileOutputStream(_keyStoreFile))
            {
                inMemoryKeyStore.store(os, "password".toCharArray());
            }

        }
        super.setUp();
    }

    @Override
    public void startDefaultBroker() throws Exception
    {
        // Do broker startup in tests
    }

    private void doBrokerStartup(boolean useMatching, String defaultAlias) throws Exception
    {
        getDefaultBrokerConfiguration().setObjectAttribute(org.apache.qpid.server.model.KeyStore.class, TestBrokerConfiguration.ENTRY_NAME_SSL_KEYSTORE,
                                                           FileKeyStore.STORE_URL, _keyStoreFile.toURI().toURL().toString());

        getDefaultBrokerConfiguration().setObjectAttribute(org.apache.qpid.server.model.KeyStore.class, TestBrokerConfiguration.ENTRY_NAME_SSL_KEYSTORE,
                                                           FileKeyStore.PASSWORD, "password");
        getDefaultBrokerConfiguration().setObjectAttribute(org.apache.qpid.server.model.KeyStore.class, TestBrokerConfiguration.ENTRY_NAME_SSL_KEYSTORE,
                                                           FileKeyStore.USE_HOST_NAME_MATCHING, String.valueOf(useMatching));

        getDefaultBrokerConfiguration().setObjectAttribute(org.apache.qpid.server.model.KeyStore.class, TestBrokerConfiguration.ENTRY_NAME_SSL_KEYSTORE,
                                                           FileKeyStore.CERTIFICATE_ALIAS, defaultAlias);

        Map<String, Object> sslPortAttributes = new HashMap<>();
        sslPortAttributes.put(Port.TRANSPORTS, Collections.singleton(Transport.SSL));
        sslPortAttributes.put(Port.PORT, DEFAULT_SSL_PORT);
        sslPortAttributes.put(Port.AUTHENTICATION_PROVIDER, TestBrokerConfiguration.ENTRY_NAME_AUTHENTICATION_PROVIDER);
        sslPortAttributes.put(Port.NEED_CLIENT_AUTH, false);
        sslPortAttributes.put(Port.WANT_CLIENT_AUTH, false);
        sslPortAttributes.put(Port.NAME, TestBrokerConfiguration.ENTRY_NAME_SSL_PORT);
        sslPortAttributes.put(Port.KEY_STORE, TestBrokerConfiguration.ENTRY_NAME_SSL_KEYSTORE);
        sslPortAttributes.put(Port.TRUST_STORES, Collections.singleton(TestBrokerConfiguration.ENTRY_NAME_SSL_TRUSTSTORE));
        sslPortAttributes.put(Port.PROTOCOLS, System.getProperty(TEST_AMQP_PORT_PROTOCOLS_PROPERTY));
        getDefaultBrokerConfiguration().addObjectConfiguration(Port.class, sslPortAttributes);

        Map<String, Object> aliasAttributes = new HashMap<>();
        aliasAttributes.put(VirtualHostAlias.NAME, "defaultAlias");
        aliasAttributes.put(VirtualHostAlias.TYPE, DefaultVirtualHostAlias.TYPE_NAME);
        getDefaultBrokerConfiguration().addObjectConfiguration(Port.class, TestBrokerConfiguration.ENTRY_NAME_SSL_PORT, VirtualHostAlias.class, aliasAttributes);

        aliasAttributes = new HashMap<>();
        aliasAttributes.put(VirtualHostAlias.NAME, "nameAlias");
        aliasAttributes.put(VirtualHostAlias.TYPE, VirtualHostNameAlias.TYPE_NAME);
        getDefaultBrokerConfiguration().addObjectConfiguration(Port.class, TestBrokerConfiguration.ENTRY_NAME_SSL_PORT, VirtualHostAlias.class, aliasAttributes);

        super.startDefaultBroker();
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
                             final SSLUtil.KeyCertPair expectedCert) throws Exception
    {
        if (isJavaBroker() && SSLUtil.canGenerateCerts())
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
            SSLSocket socket = (SSLSocket) socketFactory.createSocket();
            SSLParameters parameters = socket.getSSLParameters();
            if(sniHostName != null)
            {
                parameters.setServerNames(Collections.singletonList(new SNIHostName(sniHostName)));
            }
            socket.setSSLParameters(parameters);
            InetSocketAddress address =
                    new InetSocketAddress("localhost", getDefaultBroker().getAmqpTlsPort());
            socket.connect(address);
            final Certificate[] certs = socket.getSession().getPeerCertificates();
            assertEquals(1, certs.length);
            assertEquals(expectedCert.getCertificate(), certs[0]);
        }
    }
}
