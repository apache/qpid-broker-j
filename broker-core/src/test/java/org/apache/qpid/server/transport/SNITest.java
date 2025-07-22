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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import javax.net.ssl.SNIServerName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

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
    @RegisterExtension
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

    @BeforeAll
    public void setUp() throws Exception
    {
        final Instant yesterday = Instant.now().minus(1, ChronoUnit.DAYS);
        final Instant inOneHour = Instant.now().plus(1, ChronoUnit.HOURS);
        _fooValid = TlsResourceBuilder.createSelfSigned("CN=foo", yesterday,
                yesterday.plus(365, ChronoUnit.DAYS));
        _fooInvalid = TlsResourceBuilder.createSelfSigned("CN=foo", inOneHour,
                inOneHour.plus(365, ChronoUnit.DAYS));

        _barInvalid = TlsResourceBuilder.createSelfSigned("CN=Qpid", inOneHour,
                inOneHour.plus(365, ChronoUnit.DAYS), new AlternativeName(AltNameType.DNS_NAME, "bar"));

        _keyStoreFile = TLS_RESOURCE.createKeyStore(new PrivateKeyEntry("foovalid", _fooValid.getPrivateKey(),
                _fooValid.getCertificate()),
                new PrivateKeyEntry("fooinvalid", _fooInvalid.getPrivateKey(), _fooInvalid.getCertificate()),
                new PrivateKeyEntry("barinvalid", _barInvalid.getPrivateKey(), _barInvalid.getCertificate())).toFile();
    }

    @AfterAll
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

    @Test
    public void testInvalidHostname()
    {
        assertThrows(SSLPeerUnverifiedException.class,
                () -> performTest(false, "fooinvalid", "_foo", _fooInvalid),
                "Expected exception not thrown");
    }

    @Test
    public void testBypassInvalidSniHostnameWithJava17()
    {
        assertThrows(SSLPeerUnverifiedException.class,
                () -> performTest(false, "foovalid", "_foo", _fooValid),
                "Expected exception not thrown");
    }

    private void performTest(final boolean useMatching,
                             final String defaultAlias,
                             final String sniHostName,
                             final KeyCertificatePair expectedCert) throws Exception
    {
        doBrokerStartup(useMatching, defaultAlias);
        final SSLContext context = SSLUtil.tryGetSSLContext();
        context.init(null, new TrustManager[]
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
                },null);

        final SSLSocketFactory socketFactory = context.getSocketFactory();
        try (final SSLSocket socket = (SSLSocket) socketFactory.createSocket())
        {
            final SSLParameters parameters = socket.getSSLParameters();
            if (sniHostName != null)
            {
                parameters.setServerNames(Collections.singletonList(new TestSNIHostName(sniHostName)));
            }
            socket.setSSLParameters(parameters);
            final InetSocketAddress address = new InetSocketAddress("localhost", _boundPort);
            socket.connect(address, SOCKET_TIMEOUT);

            final Certificate[] certs = socket.getSession().getPeerCertificates();
            assertEquals(1, (long) certs.length);
            assertEquals(expectedCert.getCertificate(), certs[0]);
        }
    }

    private void doBrokerStartup(final boolean useMatching,
                                 final String defaultAlias) throws Exception
    {
        final File initialConfiguration = createInitialContext();
        _brokerWork = TestFileUtils.createTestDirectory("qpid-work", true);

        final Map<String, String> context = Map.of("qpid.work_dir", _brokerWork.toString());
        final Map<String,Object> attributes = Map.of(SystemConfig.INITIAL_CONFIGURATION_LOCATION, initialConfiguration.getAbsolutePath(),
                SystemConfig.TYPE, JsonSystemConfigImpl.SYSTEM_CONFIG_TYPE,
                SystemConfig.CONTEXT, context);

        _systemLauncher = new SystemLauncher(new DefaultSystemLauncherListener()
        {
            @Override
            @SuppressWarnings("unchecked")
            public void onContainerResolve(final SystemConfig<?> systemConfig)
            {
                _broker = systemConfig.getContainer(Broker.class);
            }
        });
        _systemLauncher.startup(attributes);

        final Map<String, Object> authProviderAttr = Map.of(AuthenticationProvider.NAME, "myAuthProvider",
                AuthenticationProvider.TYPE, AnonymousAuthenticationManager.PROVIDER_TYPE);
        final AuthenticationProvider<?> authProvider = _broker.createChild(AuthenticationProvider.class, authProviderAttr);
        final Map<String, Object> keyStoreAttr = Map.of(FileKeyStore.NAME, "myKeyStore",
                FileKeyStore.STORE_URL, _keyStoreFile.toURI().toURL().toString(),
                FileKeyStore.PASSWORD, TLS_RESOURCE.getSecret(),
                FileKeyStore.USE_HOST_NAME_MATCHING, useMatching,
                FileKeyStore.CERTIFICATE_ALIAS, defaultAlias);
        final KeyStore<?> keyStore = _broker.createChild(KeyStore.class, keyStoreAttr);
        final Map<String, Object> portAttr = Map.of(Port.NAME, "myPort",
                Port.TYPE, "AMQP",
                Port.TRANSPORTS, Set.of(Transport.SSL),
                Port.PORT, 0,
                Port.AUTHENTICATION_PROVIDER, authProvider,
                Port.KEY_STORE, keyStore);
        final Port<?> port = _broker.createChild(Port.class, portAttr);

        _boundPort = port.getBoundPort();
    }

    private File createInitialContext() throws JsonProcessingException
    {
        // create empty initial configuration
        final Map<String,Object> initialConfig = Map.of(ConfiguredObject.NAME, "test",
                Broker.MODEL_VERSION, BrokerModel.MODEL_VERSION);

        final ObjectMapper mapper = new ObjectMapper();
        final String config = mapper.writeValueAsString(initialConfig);
        return TestFileUtils.createTempFile(this, ".initial-config.json", config);
    }

    private static final class TestSNIHostName extends SNIServerName
    {
        public TestSNIHostName(final String hostname)
        {
            super(0, hostname.getBytes(StandardCharsets.US_ASCII));
        }
    }
}
