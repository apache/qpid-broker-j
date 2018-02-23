/*
 *
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
package org.apache.qpid.tests.http.authentication;

import static javax.servlet.http.HttpServletResponse.SC_CREATED;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import static org.apache.qpid.server.transport.network.security.ssl.SSLUtil.generateSelfSignedCertificate;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayDeque;
import java.util.Base64;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

import javax.net.ssl.SSLHandshakeException;

import com.fasterxml.jackson.core.type.TypeReference;
import org.junit.After;
import org.junit.Test;

import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.security.FileKeyStore;
import org.apache.qpid.server.security.ManagedPeerCertificateTrustStore;
import org.apache.qpid.server.security.auth.manager.ExternalAuthenticationManager;
import org.apache.qpid.server.transport.network.security.ssl.SSLUtil.KeyCertPair;
import org.apache.qpid.server.util.BaseAction;
import org.apache.qpid.server.util.DataUrlUtils;
import org.apache.qpid.tests.http.HttpTestBase;
import org.apache.qpid.tests.http.HttpTestHelper;

public class TlsClientAuthenticationTest extends HttpTestBase
{

    private Deque<BaseAction<Void, Exception>> _tearDownActions;
    private int _clientAuthPort;
    private String _keyStore;

    @After
    public void tearDown() throws Exception
    {
        if (_tearDownActions != null)
        {
            Exception exception = null;
            while(!_tearDownActions.isEmpty())
            {
                try
                {
                    _tearDownActions.removeLast().performAction(null);
                }
                catch (Exception e)
                {
                    exception = e;
                }
            }

            if (exception != null)
            {
                throw exception;
            }
        }
    }

    @Test
    public void clientAuthenticationSuccess() throws Exception
    {
        configPortAndAuthProvider("CN=foo");

        HttpTestHelper helper = new HttpTestHelper(getBrokerAdmin(), null, _clientAuthPort);
        helper.setTls(true);
        helper.setKeyStore(_keyStore, "password");

        String userId = helper.getJson("broker/getUser", new TypeReference<String>() {}, SC_OK);
        assertThat(userId, startsWith("foo@"));
    }

    @Test
    public void unrecognisedCertification() throws Exception
    {
        configPortAndAuthProvider("CN=foo");

        String keyStore = createKeyStoreDataUrl(getKeyCertPair("CN=bar"), "password");

        HttpTestHelper helper = new HttpTestHelper(getBrokerAdmin(), null, _clientAuthPort);
        helper.setTls(true);
        helper.setKeyStore(keyStore, "password");

        try
        {
            helper.getJson("broker/getUser", new TypeReference<String>() {}, SC_OK);
            fail("Exception not thrown");
        }
        catch (SSLHandshakeException e)
        {
            // PASS
        }
    }

    private void configPortAndAuthProvider(final String x500Name) throws Exception
    {

        final KeyCertPair keyCertPair = getKeyCertPair(x500Name);
        final byte[] cert = keyCertPair.getCertificate().getEncoded();

        _keyStore = createKeyStoreDataUrl(keyCertPair, "password");


        final Deque<BaseAction<Void,Exception>> deleteActions = new ArrayDeque<>();

        final Map<String, Object> authAttr = new HashMap<>();
        authAttr.put(ExternalAuthenticationManager.TYPE, "External");
        authAttr.put(ExternalAuthenticationManager.ATTRIBUTE_USE_FULL_DN, false);

        getHelper().submitRequest("authenticationprovider/myexternal","PUT", authAttr, SC_CREATED);

        deleteActions.add(object -> getHelper().submitRequest("authenticationprovider/myexternal", "DELETE", SC_OK));

        final Map<String, Object> keystoreAttr = new HashMap<>();
        keystoreAttr.put(FileKeyStore.TYPE, "FileKeyStore");
        keystoreAttr.put(FileKeyStore.STORE_URL, "classpath:java_broker_keystore.jks");
        keystoreAttr.put(FileKeyStore.PASSWORD, "password");

        getHelper().submitRequest("keystore/mykeystore","PUT", keystoreAttr, SC_CREATED);
        deleteActions.add(object -> getHelper().submitRequest("keystore/mykeystore", "DELETE", SC_OK));

        final Map<String, Object> truststoreAttr = new HashMap<>();
        truststoreAttr.put(ManagedPeerCertificateTrustStore.TYPE, ManagedPeerCertificateTrustStore.TYPE_NAME);
        truststoreAttr.put(ManagedPeerCertificateTrustStore.STORED_CERTIFICATES, Collections.singletonList(Base64.getEncoder().encodeToString(cert)));

        getHelper().submitRequest("truststore/mytruststore","PUT", truststoreAttr, SC_CREATED);
        deleteActions.add(object -> getHelper().submitRequest("truststore/mytruststore", "DELETE", SC_OK));

        final Map<String, Object> portAttr = new HashMap<>();
        portAttr.put(Port.TYPE, "HTTP");
        portAttr.put(Port.PORT, 0);
        portAttr.put(Port.AUTHENTICATION_PROVIDER, "myexternal");
        portAttr.put(Port.PROTOCOLS, Collections.singleton(Protocol.HTTP));
        portAttr.put(Port.TRANSPORTS, Collections.singleton(Transport.SSL));
        portAttr.put(Port.NEED_CLIENT_AUTH, true);
        portAttr.put(Port.KEY_STORE, "mykeystore");
        portAttr.put(Port.TRUST_STORES, Collections.singletonList("mytruststore"));

        getHelper().submitRequest("port/myport","PUT", portAttr, SC_CREATED);
        deleteActions.add(object -> getHelper().submitRequest("port/myport", "DELETE", SC_OK));

        Map<String, Object> clientAuthPort = getHelper().getJsonAsMap("port/myport");
        int boundPort = Integer.parseInt(String.valueOf(clientAuthPort.get("boundPort")));

        assertThat(boundPort, is(greaterThan(0)));

        _tearDownActions = deleteActions;
        _clientAuthPort = boundPort;
    }

    private String createKeyStoreDataUrl(final KeyCertPair keyCertPair, final String password) throws Exception
    {
        final KeyStore keyStore = KeyStore.getInstance("JKS");
        keyStore.load(null, null);
        Certificate[] certChain = new Certificate[] {keyCertPair.getCertificate()};
        keyStore.setKeyEntry("key1", keyCertPair.getPrivateKey(), password.toCharArray(), certChain);
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream())
        {
            keyStore.store(bos, password.toCharArray());
            bos.toByteArray();
            return DataUrlUtils.getDataUrlForBytes(bos.toByteArray());
        }
    }

    private KeyCertPair getKeyCertPair(final String x500Name) throws Exception
    {
        return generateSelfSignedCertificate("RSA", "SHA256WithRSA",
                                             2048, Instant.now().toEpochMilli(),
                                             Duration.of(365, ChronoUnit.DAYS).getSeconds(),
                                             x500Name,
                                             Collections.emptySet(),
                                             Collections.emptySet());
    }

}
