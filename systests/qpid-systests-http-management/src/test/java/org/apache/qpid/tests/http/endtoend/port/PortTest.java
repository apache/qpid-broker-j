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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.NamingException;

import com.fasterxml.jackson.core.type.TypeReference;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.security.NonJavaKeyStore;
import org.apache.qpid.server.security.auth.manager.AnonymousAuthenticationManager;
import org.apache.qpid.server.util.DataUrlUtils;
import org.apache.qpid.systests.ConnectionBuilder;
import org.apache.qpid.test.utils.tls.CertificateEntry;
import org.apache.qpid.test.utils.tls.TlsResourceBuilder;
import org.apache.qpid.test.utils.tls.KeyCertificatePair;
import org.apache.qpid.test.utils.tls.TlsResource;
import org.apache.qpid.test.utils.tls.TlsResourceHelper;
import org.apache.qpid.tests.http.HttpTestBase;
import org.apache.qpid.tests.http.HttpTestHelper;

public class PortTest extends HttpTestBase
{
    @ClassRule
    public static final TlsResource TLS_RESOURCE = new TlsResource();

    private static final String QUEUE_NAME = "testQueue";
    private static final TypeReference<Boolean> BOOLEAN = new TypeReference<Boolean>()
    {
    };
    private static final String CERTIFICATE_ALIAS = "certificate";
    private String _portName;
    private String _authenticationProvider;
    private String _keyStoreName;

    private File _storeFile;

    @Before
    public void setUp() throws Exception
    {

        _portName = getTestName();
        _authenticationProvider = _portName + "AuthenticationProvider";
        _keyStoreName = _portName + "KeyStore";
        createAnonymousAuthenticationProvider();
        final KeyCertificatePair keyCertPair = generateSelfSignedCertificate();
        final X509Certificate certificate = keyCertPair.getCertificate();
        submitKeyStoreAttributes(_keyStoreName, SC_CREATED, keyCertPair);
        _storeFile = TLS_RESOURCE.createKeyStore(new CertificateEntry(CERTIFICATE_ALIAS, certificate)).toFile();

        getBrokerAdmin().createQueue(QUEUE_NAME);
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

            final File storeFile = createNewKeyStoreAndSetItOnPort();

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

            final File trustStoreFile = updateKeyStoreAndUpdatePortTls();
            final Connection connection2 = createConnection(port, trustStoreFile.getAbsolutePath());
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

            final File storeFile = createNewKeyStoreAndSetItOnPort();
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
        helper.setKeyStore(_storeFile.getAbsolutePath(), TLS_RESOURCE.getSecret());

        final Map<String, Object> attributes = getHelper().getJsonAsMap("port/" + _portName);
        final Map<String, Object> ownAttributes = helper.getJsonAsMap("port/" + _portName);
        assertEquals(attributes, ownAttributes);

        final File storeFile = createNewKeyStoreAndSetItOnPort();
        helper.setKeyStore(storeFile.getAbsolutePath(), TLS_RESOURCE.getSecret());

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

    private void submitKeyStoreAttributes(final String keyStoreName,
                                          final int status,
                                          final KeyCertificatePair keyCertPair) throws Exception
    {

        final Map<String, Object> attributes = new HashMap<>();
        attributes.put(NonJavaKeyStore.NAME, keyStoreName);
        attributes.put(NonJavaKeyStore.PRIVATE_KEY_URL,
                       DataUrlUtils.getDataUrlForBytes(TlsResourceHelper.toPEM(keyCertPair.getPrivateKey())
                                                                        .getBytes(UTF_8)));
        attributes.put(NonJavaKeyStore.CERTIFICATE_URL,
                       DataUrlUtils.getDataUrlForBytes(TlsResourceHelper.toPEM(keyCertPair.getCertificate())
                                                                        .getBytes(UTF_8)));
        attributes.put(NonJavaKeyStore.TYPE, "NonJavaKeyStore");

        getHelper().submitRequest("keystore/" + keyStoreName, "PUT", attributes, status);
    }

    private ConnectionBuilder createConnectionBuilder(final int port, final String absolutePath)
    {
        return getConnectionBuilder().setPort(port)
                                     .setTls(true)
                                     .setVerifyHostName(false)
                                     .setTrustStoreLocation(absolutePath)
                                     .setTrustStorePassword(TLS_RESOURCE.getSecret());
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

    private KeyCertificatePair generateSelfSignedCertificate() throws Exception
    {
        return TlsResourceBuilder.createSelfSigned("CN=foo");
    }

    private void assertMessage(final Message messageA, final String a) throws JMSException
    {
        assertThat(messageA, is(notNullValue()));
        assertThat(messageA, is(instanceOf(TextMessage.class)));
        assertThat(((TextMessage) messageA).getText(), is(equalTo(a)));
    }

    private File createNewKeyStoreAndSetItOnPort() throws Exception
    {
        String newKeyStoreName = _keyStoreName + "_2";
        final KeyCertificatePair keyCertPair = generateSelfSignedCertificate();
        submitKeyStoreAttributes(newKeyStoreName, SC_CREATED, keyCertPair);
        getHelper().submitRequest("port/" + _portName, "POST",
                                  Collections.<String, Object>singletonMap(Port.KEY_STORE, newKeyStoreName), SC_OK);
        updatePortTls();
        return createTrustStore(keyCertPair);
    }

    private File updateKeyStoreAndUpdatePortTls() throws Exception
    {
        final KeyCertificatePair keyCertPair = generateSelfSignedCertificate();
        submitKeyStoreAttributes(_keyStoreName, SC_OK, keyCertPair);
        updatePortTls();
        return createTrustStore(keyCertPair);
    }

    private File createTrustStore(final KeyCertificatePair keyCertPair) throws Exception
    {
        CertificateEntry entry = new CertificateEntry(
                CERTIFICATE_ALIAS,
                keyCertPair.getCertificate());
        Path keyStore = TLS_RESOURCE.createKeyStore(entry);
        return keyStore.toFile();
    }

    private void updatePortTls() throws Exception
    {
        final boolean response = getHelper().postJson("port/" + _portName + "/updateTLS",
                                                      Collections.emptyMap(),
                                                      BOOLEAN,
                                                      SC_OK);
        assertTrue(response);

    }
}
