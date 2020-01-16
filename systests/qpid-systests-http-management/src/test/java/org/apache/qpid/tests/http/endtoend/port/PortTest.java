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
package org.apache.qpid.tests.http.endtoend.port;

import static java.nio.charset.StandardCharsets.UTF_8;
import static javax.servlet.http.HttpServletResponse.SC_CREATED;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import static org.apache.qpid.test.utils.TestSSLConstants.JAVA_KEYSTORE_TYPE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeThat;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.NamingException;

import com.fasterxml.jackson.core.type.TypeReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.security.NonJavaKeyStore;
import org.apache.qpid.server.security.auth.manager.AnonymousAuthenticationManager;
import org.apache.qpid.server.transport.network.security.ssl.SSLUtil;
import org.apache.qpid.server.util.DataUrlUtils;
import org.apache.qpid.systests.ConnectionBuilder;
import org.apache.qpid.tests.http.HttpTestBase;
import org.apache.qpid.tests.http.HttpTestHelper;

public class PortTest extends HttpTestBase
{
    private static final String PASS = "changeit";
    private static final String QUEUE_NAME = "testQueue";
    private static final TypeReference<Boolean> BOOLEAN = new TypeReference<Boolean>()
    {
    };
    private String _portName;
    private String _authenticationProvider;
    private String _keyStoreName;
    private Set<File> _storeFiles;
    private File _storeFile;

    @Before
    public void setUp() throws Exception
    {
        assumeThat(SSLUtil.canGenerateCerts(), is(true));

        _portName = getTestName();
        _authenticationProvider = _portName + "AuthenticationProvider";
        _keyStoreName = _portName + "KeyStore";
        createAnonymousAuthenticationProvider();
        final SSLUtil.KeyCertPair keyCertPair = createKeyStore(_keyStoreName);
        final X509Certificate certificate = keyCertPair.getCertificate();

        _storeFiles = new HashSet<>();
        _storeFile = createTrustStore(certificate);

        getBrokerAdmin().createQueue(QUEUE_NAME);
    }


    @After
    public void tearDown()
    {
        _storeFiles.forEach(f -> assertTrue(f.delete()));
    }

    @Test
    public void testSwapKeyStoreAndUpdateTlsOnAmqpPort() throws Exception
    {
        final int port = createPort(Transport.SSL);
        final Connection connection = createConnection(port, _storeFile.getAbsolutePath());
        try
        {
            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final MessageProducer producer = session.createProducer(session.createQueue(QUEUE_NAME));
            producer.send(session.createTextMessage("A"));

            final SSLUtil.KeyCertPair keyCertPair = createKeyStoreAndUpdatePortTLS();
            final File storeFile = createTrustStore(keyCertPair.getCertificate());
            final Connection connection2 = createConnection(port, storeFile.getAbsolutePath());
            try
            {
                producer.send(session.createTextMessage("B"));

                final Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
                final MessageConsumer consumer = session2.createConsumer(session2.createQueue(QUEUE_NAME));
                connection2.start();

                assertMessage(consumer.receive(getReceiveTimeout()), "A");
                assertMessage(consumer.receive(getReceiveTimeout()), "B");
            }
            finally
            {
                connection2.close();
            }
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testUpdateKeyStoreAndUpdateTlsOnAmqpPort() throws Exception
    {
        final int port = createPort(Transport.SSL);
        final Connection connection = createConnection(port, _storeFile.getAbsolutePath());
        try
        {
            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final MessageProducer producer = session.createProducer(session.createQueue(QUEUE_NAME));
            producer.send(session.createTextMessage("A"));

            final SSLUtil.KeyCertPair keyCertPair = updateKeyStoreAndUpdatePortTLS();
            final File storeFile = createTrustStore(keyCertPair.getCertificate());
            final Connection connection2 = createConnection(port, storeFile.getAbsolutePath());
            try
            {
                producer.send(session.createTextMessage("B"));

                final Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
                final MessageConsumer consumer = session2.createConsumer(session2.createQueue(QUEUE_NAME));
                connection2.start();

                assertMessage(consumer.receive(getReceiveTimeout()), "A");
                assertMessage(consumer.receive(getReceiveTimeout()), "B");
            }
            finally
            {
                connection2.close();
            }
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testSwapKeyStoreAndUpdateTlsOnWssPort() throws Exception
    {
        assumeThat(getProtocol(), is(equalTo(Protocol.AMQP_1_0)));
        final int port = createPort(Transport.WSS);
        final Connection connection = createConnectionBuilder(port, _storeFile.getAbsolutePath())
                .setTransport("amqpws").build();
        try
        {
            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final MessageProducer producer = session.createProducer(session.createQueue(QUEUE_NAME));
            producer.send(session.createTextMessage("A"));

            final SSLUtil.KeyCertPair keyCertPair = createKeyStoreAndUpdatePortTLS();
            final File storeFile = createTrustStore(keyCertPair.getCertificate());
            final Connection connection2 = createConnectionBuilder(port, storeFile.getAbsolutePath())
                    .setTransport("amqpws").build();
            try
            {
                producer.send(session.createTextMessage("B"));

                final Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
                final MessageConsumer consumer = session2.createConsumer(session2.createQueue(QUEUE_NAME));
                connection2.start();

                assertMessage(consumer.receive(getReceiveTimeout()), "A");
                assertMessage(consumer.receive(getReceiveTimeout()), "B");
            }
            finally
            {
                connection2.close();
            }
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testSwapKeyStoreAndUpdateTlsOnHttpPort() throws Exception
    {
        final int port = createHttpPort();

        HttpTestHelper helper = new HttpTestHelper(getBrokerAdmin(), null, port);
        helper.setTls(true);
        helper.setKeyStore(_storeFile.getAbsolutePath(), PASS);

        final Map<String, Object> attributes = getHelper().getJsonAsMap("port/" + _portName);
        final Map<String, Object> ownAttributes = helper.getJsonAsMap("port/" + _portName);
        assertEquals(attributes, ownAttributes);

        final SSLUtil.KeyCertPair keyCertPair = createKeyStoreAndUpdatePortTLS();
        final File storeFile = createTrustStore(keyCertPair.getCertificate());
        helper.setKeyStore(storeFile.getAbsolutePath(), PASS);

        final Map<String, Object> attributes2 = getHelper().getJsonAsMap("port/" + _portName);
        final Map<String, Object> ownAttributes2 = helper.getJsonAsMap("port/" + _portName);
        assertEquals(attributes2, ownAttributes2);
    }

    private void createAnonymousAuthenticationProvider() throws IOException
    {
        final Map<String, Object> data = Collections.singletonMap(ConfiguredObject.TYPE,
                                                                  AnonymousAuthenticationManager.PROVIDER_TYPE);
        getHelper().submitRequest("authenticationprovider/" + _authenticationProvider, "PUT", data, SC_CREATED);
    }

    private SSLUtil.KeyCertPair createKeyStore(final String keyStoreName) throws Exception
    {
        return submitKeyStoreAttributes(keyStoreName, SC_CREATED);
    }

    private SSLUtil.KeyCertPair submitKeyStoreAttributes(final String keyStoreName, final int status) throws Exception
    {
        final SSLUtil.KeyCertPair keyCertPair = generateSelfSignedCertificate();

        final Map<String, Object> attributes = new HashMap<>();
        attributes.put(NonJavaKeyStore.NAME, keyStoreName);
        attributes.put(NonJavaKeyStore.PRIVATE_KEY_URL,
                       DataUrlUtils.getDataUrlForBytes(toPEM(keyCertPair.getPrivateKey()).getBytes(UTF_8)));
        attributes.put(NonJavaKeyStore.CERTIFICATE_URL,
                       DataUrlUtils.getDataUrlForBytes(toPEM(keyCertPair.getCertificate()).getBytes(UTF_8)));
        attributes.put(NonJavaKeyStore.TYPE, "NonJavaKeyStore");

        getHelper().submitRequest("keystore/" + keyStoreName, "PUT", attributes, status);
        return keyCertPair;
    }

    private ConnectionBuilder createConnectionBuilder(final int port, final String absolutePath)
    {
        return getConnectionBuilder().setPort(port)
                                     .setTls(true)
                                     .setVerifyHostName(false)
                                     .setTrustStoreLocation(absolutePath)
                                     .setTrustStorePassword(PASS);
    }

    private Connection createConnection(final int port, final String absolutePath)
            throws NamingException, JMSException
    {
        return createConnectionBuilder(port, absolutePath).build();
    }

    private int createPort(final Transport transport) throws IOException
    {
        return createPort("AMQP",  transport);
    }

    private int createHttpPort() throws IOException
    {
        return createPort("HTTP",  Transport.SSL);
    }

    private int createPort(final String type, final Transport transport) throws IOException
    {
        final Map<String, Object> port = new HashMap<>();
        port.put(Port.NAME, _portName);
        port.put(Port.AUTHENTICATION_PROVIDER, _authenticationProvider);
        port.put(Port.TYPE, type);
        port.put(Port.PORT, 0);
        port.put(Port.KEY_STORE, _keyStoreName);
        port.put(Port.TRANSPORTS, Collections.singleton(transport));

        getHelper().submitRequest("port/" + _portName, "PUT", port, SC_CREATED);

        return getBoundPort();
    }

    private int getBoundPort() throws IOException
    {
        final Map<String, Object> attributes = getHelper().getJsonAsMap("port/" + _portName);
        assertTrue(attributes.containsKey("boundPort"));
        assertTrue(attributes.get("boundPort") instanceof Number);

        return ((Number) attributes.get("boundPort")).intValue();
    }

    private File createTrustStore(final X509Certificate certificate)
            throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException
    {
        final java.security.KeyStore ks = java.security.KeyStore.getInstance(JAVA_KEYSTORE_TYPE);
        ks.load(null);
        ks.setCertificateEntry("certificate", certificate);
        final File storeFile = File.createTempFile(getTestName(), ".jks");
        try (FileOutputStream fos = new FileOutputStream(storeFile))
        {
            ks.store(fos, PASS.toCharArray());
        }
        finally
        {
            _storeFiles.add(storeFile);
        }
        return storeFile;
    }

    private SSLUtil.KeyCertPair generateSelfSignedCertificate() throws Exception
    {
        return SSLUtil.generateSelfSignedCertificate("RSA",
                                                     "SHA256WithRSA",
                                                     2048,
                                                     Instant.now()
                                                            .minus(1, ChronoUnit.DAYS)
                                                            .toEpochMilli(),
                                                     Duration.of(365, ChronoUnit.DAYS)
                                                             .getSeconds(),
                                                     "CN=foo",
                                                     Collections.emptySet(),
                                                     Collections.emptySet());
    }

    private String toPEM(final Certificate pub) throws CertificateEncodingException
    {
        return toPEM(pub.getEncoded(), "-----BEGIN CERTIFICATE-----", "-----END CERTIFICATE-----");
    }

    private String toPEM(final PrivateKey key)
    {
        return toPEM(key.getEncoded(), "-----BEGIN PRIVATE KEY-----", "-----END PRIVATE KEY-----");
    }

    private String toPEM(final byte[] bytes, final String header, final String footer)
    {
        StringBuilder pem = new StringBuilder();
        pem.append(header).append("\n");
        String base64encoded = Base64.getEncoder().encodeToString(bytes);
        while (base64encoded.length() > 76)
        {
            pem.append(base64encoded, 0, 76).append("\n");
            base64encoded = base64encoded.substring(76);
        }
        pem.append(base64encoded).append("\n");
        pem.append(footer).append("\n");
        return pem.toString();
    }

    private void assertMessage(final Message messageA, final String a) throws JMSException
    {
        assertThat(messageA, is(notNullValue()));
        assertThat(messageA, is(instanceOf(TextMessage.class)));
        assertThat(((TextMessage) messageA).getText(), is(equalTo(a)));
    }

    private SSLUtil.KeyCertPair createKeyStoreAndUpdatePortTLS() throws Exception
    {
        final SSLUtil.KeyCertPair keyCertPair = createKeyStore(_keyStoreName + "_2");
        final Map<String, Object> data = Collections.singletonMap(Port.KEY_STORE, _keyStoreName + "_2");
        getHelper().submitRequest("port/" + _portName, "POST", data, SC_OK);
        final boolean response = getHelper().postJson("port/" + _portName + "/updateTLS",
                                                      Collections.emptyMap(),
                                                      BOOLEAN,
                                                      SC_OK);
        assertTrue(response);

        return keyCertPair;
    }

    private SSLUtil.KeyCertPair updateKeyStoreAndUpdatePortTLS() throws Exception
    {
        final SSLUtil.KeyCertPair keyCertPair = submitKeyStoreAttributes(_keyStoreName, SC_OK);
        final boolean response = getHelper().postJson("port/" + _portName + "/updateTLS",
                                                      Collections.emptyMap(),
                                                      BOOLEAN,
                                                      SC_OK);
        assertTrue(response);

        return keyCertPair;
    }
}
