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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.net.InetSocketAddress;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
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
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

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
import org.apache.qpid.test.utils.TestFileUtils;
import org.apache.qpid.test.utils.UnitTestBase;
import org.apache.qpid.test.utils.tls.AltNameType;
import org.apache.qpid.test.utils.tls.AlternativeName;
import org.apache.qpid.test.utils.tls.KeyCertificatePair;
import org.apache.qpid.test.utils.tls.PrivateKeyEntry;
import org.apache.qpid.test.utils.tls.TlsResource;
import org.apache.qpid.test.utils.tls.TlsResourceBuilder;

public class SNITest extends UnitTestBase
{
    @ClassRule
    public static final TlsResource TLS_RESOURCE = new TlsResource();

    private static final int SOCKET_TIMEOUT = 10000;

    private File _keyStoreFile;
    private KeyCertificatePair _fooValid;
    private KeyCertificatePair _fooInvalid;
    private KeyCertificatePair _barInvalid;
    private SystemLauncher _systemLauncher;
    private Broker<?> _broker;
    private int _boundPort;
    private File _brokerWork;

    @Before
    public void setUp() throws Exception
    {
        final Instant yesterday = Instant.now().minus(1, ChronoUnit.DAYS);
        final Instant inOneHour = Instant.now().plus(1, ChronoUnit.HOURS);
        _fooValid = TlsResourceBuilder.createSelfSigned("CN=foo",
                                                        yesterday,
                                                        yesterday.plus(365, ChronoUnit.DAYS));
        _fooInvalid = TlsResourceBuilder.createSelfSigned("CN=foo",
                                                          inOneHour,
                                                          inOneHour.plus(365, ChronoUnit.DAYS));

        _barInvalid = TlsResourceBuilder.createSelfSigned("CN=Qpid",
                                                          inOneHour,
                                                          inOneHour.plus(365, ChronoUnit.DAYS),
                                                          new AlternativeName(
                                                                  AltNameType.DNS_NAME, "bar"));




        _keyStoreFile = TLS_RESOURCE.createKeyStore(new PrivateKeyEntry("foovalid",
                                                                        _fooValid.getPrivateKey(),
                                                                        _fooValid.getCertificate()),
                                                    new PrivateKeyEntry("fooinvalid",
                                                                        _fooInvalid.getPrivateKey(),
                                                                        _fooInvalid.getCertificate()),
                                                    new PrivateKeyEntry("barinvalid",
                                                                        _barInvalid.getPrivateKey(),
                                                                        _barInvalid.getCertificate())).toFile();
    }

    @After
    public void tearDown() throws Exception
    {
        if (_systemLauncher != null)
        {
            _systemLauncher.shutdown();
        }

        if (_brokerWork != null)
        {
            _brokerWork.delete();
        }
    }

    @Test
    public void testValidCertChosen() throws Exception
    {
        performTest(true, "fooinvalid", "foo", _fooValid);
    }

    @Test
    public void testMatchCertChosenEvenIfInvalid() throws Exception
    {
        performTest(true, "fooinvalid", "bar", _barInvalid);
    }

    @Test
    public void testDefaultCertChose() throws Exception
    {
        performTest(true, "fooinvalid", null, _fooInvalid);
    }

    @Test
    public void testMatchingCanBeDisabled() throws Exception
    {
        performTest(false, "fooinvalid", "foo", _fooInvalid);
    }


    private void performTest(final boolean useMatching,
                             final String defaultAlias,
                             final String sniHostName,
                             final KeyCertificatePair expectedCert) throws Exception
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
                assertEquals((long) 1, (long) certs.length);
                assertEquals(expectedCert.getCertificate(), certs[0]);
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
        keyStoreAttr.put(FileKeyStore.PASSWORD, TLS_RESOURCE.getSecret());
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
