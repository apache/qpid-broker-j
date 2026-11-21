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

package org.apache.qpid.tests.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.KeyPair;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.JMSSecurityException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.security.FileTrustStore;
import org.apache.qpid.server.security.auth.manager.ExternalAuthenticationManager;
import org.apache.qpid.server.transport.AbstractAMQPConnection;
import org.apache.qpid.test.utils.tls.CertificateEntry;
import org.apache.qpid.test.utils.tls.KeyCertificatePair;
import org.apache.qpid.test.utils.tls.PrivateKeyEntry;
import org.apache.qpid.test.utils.tls.TlsResource;
import org.apache.qpid.test.utils.tls.TlsResourceBuilder;
import org.apache.qpid.test.utils.tls.TlsResourceHelper;

class WebSocketExternalAuthenticationTest extends WebSocketTestBase
{
    private static final String CLIENT_NAME = "websocket-client";
    private static final String CLIENT_DN = "CN=" + CLIENT_NAME + ",OU=clients,O=Qpid";

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void externalAuthenticatesClientCertificateAndExchangesMessages(final boolean useFullDN) throws Exception
    {
        try (final TlsResource tls = new TlsResource())
        {
            final KeyCertificatePair ca = TlsResourceBuilder.createKeyPairAndRootCA("CN=WebSocket client CA");
            final KeyPair clientKey = TlsResourceBuilder.createRSAKeyPair();
            final X509Certificate clientCertificate = TlsResourceBuilder
                    .createCertificateForClientAuthorization(clientKey, ca, CLIENT_DN);
            final PrivateKeyEntry privateKeyEntry = new PrivateKeyEntry("client", clientKey.getPrivate(),
                    clientCertificate, ca.certificate());
            final KeyStore clientStore = TlsResourceHelper
                    .createKeyStore(tls.getKeyStoreType(), tls.getSecretAsCharacters(), privateKeyEntry);
            final KeyManagerFactory keys = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keys.init(clientStore, tls.getSecretAsCharacters());
            final AmqpPort<?> port = createExternalPort(tls, ca.certificate(), useFullDN, true);
            final SSLContext sslContext = createClientContext(keys.getKeyManagers());
            final JmsConnectionFactory factory = externalConnectionFactory(port, sslContext);
            createQueue("external");
            try (final Connection connection = factory.createConnection())
            {
                connection.start();
                final AbstractAMQPConnection<?, ?> server = brokerConnection("external");
                final String principal = useFullDN
                        ? clientCertificate.getSubjectX500Principal().getName()
                        : CLIENT_NAME;
                assertEquals(Transport.WSS, server.getTransport());
                assertEquals(clientCertificate, server.getNetwork().getPeerCertificate());
                assertEquals(clientCertificate.getSubjectX500Principal(), server.getExternalPrincipal());
                assertEquals(principal, server.getPrincipal());

                final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                final Queue queue = session.createQueue("external");
                final MessageConsumer consumer = session.createConsumer(queue);
                final MessageProducer producer = session.createProducer(queue);
                producer.send(session.createTextMessage("authenticated with a client certificate"));
                final TextMessage received = (TextMessage) consumer.receive(TIMEOUT_MILLIS);
                assertNotNull(received);
                assertEquals("authenticated with a client certificate", received.getText());
            }
            await(() -> port.getConnectionCount() == 0 && _virtualHost.getConnectionCount() == 0,
                    "Closing the EXTERNAL client did not release its connection");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void externalRejectsMissingClientCertificate(final boolean needClientAuth) throws Exception
    {
        try (final TlsResource tls = new TlsResource())
        {
            final KeyCertificatePair ca = TlsResourceBuilder.createKeyPairAndRootCA("CN=WebSocket client CA");
            final AmqpPort<?> port = createExternalPort(tls, ca.certificate(), false, needClientAuth);
            final Callable<String> upgradeWithoutClientCertificate = () ->
            {
                try (final WebSocketTestPeer peer = new WebSocketTestPeer(port, _clientContext, "localhost",
                        "localhost", false))
                {
                    return peer.getUpgradeResponse();
                }
            };
            if (needClientAuth)
            {
                assertThrows(SSLHandshakeException.class, upgradeWithoutClientCertificate::call,
                        "TLS must reject a missing client certificate before the WebSocket upgrade");
            }
            else
            {
                // Optional TLS client authentication allows an upgrade; EXTERNAL still requires a certificate
                assertTrue(upgradeWithoutClientCertificate.call().startsWith("HTTP/1.1 101 "));
            }
            final JmsConnectionFactory factory = externalConnectionFactory(port, _clientContext);
            final JMSException failure = assertThrows(JMSException.class, () ->
            {
                try (final Connection connection = factory.createConnection())
                {
                    connection.start();
                }
            });
            if (!needClientAuth)
            {
                assertInstanceOf(JMSSecurityException.class, failure);
            }
            await(() -> port.getConnectionCount() == 0 && _virtualHost.getConnectionCount() == 0,
                    "Rejected EXTERNAL client left a transport or authenticated connection");
        }
    }

    private AmqpPort<?> createExternalPort(final TlsResource tls,
                                           final X509Certificate ca,
                                           final boolean useFullDN,
                                           final boolean needClientAuth) throws Exception
    {
        final String trustStoreUrl = tls.createKeyStoreAsDataUrl(new CertificateEntry("client-ca", ca));
        final TrustStore<?> trustStore = admin(() -> _broker.createChild(TrustStore.class,
                Map.of(ConfiguredObject.NAME, "clients", ConfiguredObject.TYPE, "FileTrustStore",
                        FileTrustStore.STORE_URL, trustStoreUrl, FileTrustStore.PASSWORD, tls.getSecret())));
        admin(() -> _broker.createChild(AuthenticationProvider.class,
                Map.of(ConfiguredObject.NAME, "external",
                        ConfiguredObject.TYPE, ExternalAuthenticationManager.PROVIDER_TYPE,
                        ExternalAuthenticationManager.ATTRIBUTE_USE_FULL_DN, useFullDN)));
        return createPort(Transport.WSS, "external", Map.of(),
                Map.of(Port.TRUST_STORES, List.of(trustStore), Port.NEED_CLIENT_AUTH, needClientAuth,
                        Port.WANT_CLIENT_AUTH, !needClientAuth));
    }

    private JmsConnectionFactory externalConnectionFactory(final AmqpPort<?> port, final SSLContext clientContext)
    {
        final JmsConnectionFactory factory = connectionFactory(port, "external");
        factory.setRemoteURI(factory.getRemoteURI()
                .replace("amqp.saslMechanisms=PLAIN", "amqp.saslMechanisms=EXTERNAL"));
        factory.setUsername(null);
        factory.setPassword(null);
        factory.setSslContext(clientContext);
        return factory;
    }
}
