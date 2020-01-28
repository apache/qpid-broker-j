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
package org.apache.qpid.systests.jms_1_1.extensions.sasl;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.naming.NamingException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.qpid.server.security.FileTrustStoreTest;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.User;
import org.apache.qpid.server.security.FileKeyStore;
import org.apache.qpid.server.security.FileTrustStore;
import org.apache.qpid.server.security.auth.manager.ExternalAuthenticationManager;
import org.apache.qpid.server.security.auth.manager.ExternalAuthenticationManagerImpl;
import org.apache.qpid.server.security.auth.manager.ScramSHA1AuthenticationManager;
import org.apache.qpid.server.security.auth.manager.ScramSHA256AuthenticationManager;
import org.apache.qpid.server.security.auth.sasl.crammd5.CramMd5HashedNegotiator;
import org.apache.qpid.systests.AmqpManagementFacade;
import org.apache.qpid.systests.JmsTestBase;
import org.apache.qpid.test.utils.TestSSLConstants;

public class AuthenticationTest extends JmsTestBase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AuthenticationTest.class);
    private static final String USER = "user";
    private static final String USER_PASSWORD = "user";
    // see how port is specified when certificates are generated in script
    // test-profiles/test_resources/ssl/generate_certificates.sh
    private static final int CRL_HTTP_PORT = 8186;
    private static final Server CRL_SERVER = new Server();
    private static final HandlerCollection HANDLERS = new HandlerCollection();

    @BeforeClass
    public static void setUp() throws Exception
    {
        System.setProperty("javax.net.debug", "ssl");

        // workaround for QPID-8069
        if (getProtocol() != Protocol.AMQP_1_0 && getProtocol() != Protocol.AMQP_0_10)
        {
            System.setProperty("amqj.MaximumStateWait", "4000");
        }

        // legacy client keystore/truststore types can only be configured with JVM settings
        if (getProtocol() != Protocol.AMQP_1_0)
        {
            System.setProperty("javax.net.ssl.trustStoreType", TestSSLConstants.JAVA_KEYSTORE_TYPE);
            System.setProperty("javax.net.ssl.keyStoreType", TestSSLConstants.JAVA_KEYSTORE_TYPE);
        }
        final ServerConnector connector = new ServerConnector(CRL_SERVER);
        connector.setPort(CRL_HTTP_PORT);
        connector.setHost("localhost");
        CRL_SERVER.addConnector(connector);
        createContext(Paths.get(TestSSLConstants.CA_CRL));
        createContext(Paths.get(TestSSLConstants.CA_CRL_EMPTY));
        createContext(Paths.get(TestSSLConstants.INTERMEDIATE_CA_CRL));
        CRL_SERVER.setHandler(HANDLERS);
        CRL_SERVER.start();
    }

    @AfterClass
    public static void tearDown() throws Exception
    {
        System.clearProperty("javax.net.debug");
        if (getProtocol() != Protocol.AMQP_1_0)
        {
            System.clearProperty("amqj.MaximumStateWait");
        }

        if (getProtocol() != Protocol.AMQP_1_0)
        {
            System.clearProperty("javax.net.ssl.trustStoreType");
            System.clearProperty("javax.net.ssl.keyStoreType");
        }
        CRL_SERVER.stop();
    }


    @Test
    public void md5() throws Exception
    {
        assumeThat("Qpid JMS Client does not support MD5 mechanisms",
                   getProtocol(),
                   is(not(equalTo(Protocol.AMQP_1_0))));

        final int port = createAuthenticationProviderAndUserAndPort(getTestName(), "MD5", USER, USER_PASSWORD);

        assertPlainConnectivity(port, USER, USER_PASSWORD, CramMd5HashedNegotiator.MECHANISM);
    }

    @Test
    public void sha256() throws Exception
    {
        final int port = createAuthenticationProviderAndUserAndPort(getTestName(),
                                                              ScramSHA256AuthenticationManager.PROVIDER_TYPE,
                                                              USER,
                                                              USER_PASSWORD);

        assertPlainConnectivity(port, USER, USER_PASSWORD, ScramSHA256AuthenticationManager.MECHANISM);
    }

    @Test
    public void sha1() throws Exception
    {
        final int port = createAuthenticationProviderAndUserAndPort(getTestName(),
                                                              ScramSHA1AuthenticationManager.PROVIDER_TYPE,
                                                              USER,
                                                              USER_PASSWORD);

        assertPlainConnectivity(port, USER, USER_PASSWORD, ScramSHA1AuthenticationManager.MECHANISM);
    }

    @Test
    public void external() throws Exception
    {
        final int port = createExternalProviderAndTlsPort();

        Connection connection = getConnectionBuilder().setPort(port)
                                                      .setTls(true)
                                                      .setSaslMechanisms(ExternalAuthenticationManagerImpl.MECHANISM_NAME)
                                                      .setKeyStoreLocation(TestSSLConstants.CLIENT_KEYSTORE)
                                                      .setKeyStorePassword(TestSSLConstants.PASSWORD)
                                                      .setTrustStoreLocation(TestSSLConstants.CLIENT_TRUSTSTORE)
                                                      .setTrustStorePassword(TestSSLConstants.PASSWORD)
                                                      .build();
        try
        {
            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            assertNotNull("Temporary queue was not created", session.createTemporaryQueue());
        }
        finally
        {
            connection.close();
        }
    }

    public void externalWithRevocationWithDataUrlCrlFileAndAllowedCertificate() throws Exception
    {
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes();
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_LIST_URL,
                FileTrustStoreTest.createDataUrlForFile(TestSSLConstants.CA_CRL));
        final int port = createExternalProviderAndTlsPort(trustStoreAttributes);
        assertTlsConnectivity(port, TestSSLConstants.CERT_ALIAS_ALLOWED);
    }

    @Test
    public void externalWithRevocationWithDataUrlCrlFileAndRevokedCertificate() throws Exception
    {
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes();
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_LIST_URL,
                FileTrustStoreTest.createDataUrlForFile(TestSSLConstants.CA_CRL));
        final int port = createExternalProviderAndTlsPort(trustStoreAttributes);
        assertNoTlsConnectivity(port, TestSSLConstants.CERT_ALIAS_REVOKED);
    }

    @Test
    public void externalWithRevocationWithCrlFileAndAllowedCertificate() throws Exception
    {
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes();
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_LIST_URL, TestSSLConstants.CA_CRL);
        final int port = createExternalProviderAndTlsPort(trustStoreAttributes);
        assertTlsConnectivity(port, TestSSLConstants.CERT_ALIAS_ALLOWED);
    }

    @Test
    public void externalWithRevocationWithCrlFileAndAllowedCertificateWithoutPreferCrls() throws Exception
    {
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes();
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_LIST_URL, TestSSLConstants.CA_CRL);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_WITH_PREFERRING_CERTIFICATE_REVOCATION_LIST, false);
        final int port = createExternalProviderAndTlsPort(trustStoreAttributes);
        assertNoTlsConnectivity(port, TestSSLConstants.CERT_ALIAS_ALLOWED);
    }

    @Test
    public void externalWithRevocationWithCrlFileAndRevokedCertificate() throws Exception
    {
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes();
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_LIST_URL, TestSSLConstants.CA_CRL);
        final int port = createExternalProviderAndTlsPort(trustStoreAttributes);
        assertNoTlsConnectivity(port, TestSSLConstants.CERT_ALIAS_REVOKED);
    }

    @Test
    public void externalWithRevocationWithEmptyCrlFileAndRevokedCertificate() throws Exception
    {
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes();
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_LIST_URL, TestSSLConstants.CA_CRL_EMPTY);
        final int port = createExternalProviderAndTlsPort(trustStoreAttributes);
        assertTlsConnectivity(port, TestSSLConstants.CERT_ALIAS_ALLOWED);
    }

    @Test
    public void externalWithRevocationAndAllowedCertificateWithCrlUrl() throws Exception
    {
        assumeThat("HTTP server failed to bind to port '" + CRL_HTTP_PORT + "'",
                CRL_SERVER, is(not(equalTo(null))));
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes();
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        final int port = createExternalProviderAndTlsPort(trustStoreAttributes);
        assertTlsConnectivity(port, TestSSLConstants.CERT_ALIAS_ALLOWED);
    }

    @Test
    public void externalWithRevocationAndRevokedCertificateWithCrlUrl() throws Exception
    {
        assumeThat("HTTP server failed to bind to port '" + CRL_HTTP_PORT + "'",
                CRL_SERVER, is(not(equalTo(null))));
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes();
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        final int port = createExternalProviderAndTlsPort(trustStoreAttributes);
        assertNoTlsConnectivity(port, TestSSLConstants.CERT_ALIAS_REVOKED);
    }

    @Test
    public void externalWithRevocationAndRevokedCertificateWithCrlUrlWithEmptyCrl() throws Exception
    {
        assumeThat("HTTP server failed to bind to port '" + CRL_HTTP_PORT + "'",
                CRL_SERVER, is(not(equalTo(null))));
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes();
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        final int port = createExternalProviderAndTlsPort(trustStoreAttributes);
        assertTlsConnectivity(port, TestSSLConstants.CERT_ALIAS_REVOKED_EMPTY_CRL);
    }

    @Test
    public void externalWithRevocationDisabledWithCrlFileAndRevokedCertificate() throws Exception
    {
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes();
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, false);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_LIST_URL, TestSSLConstants.CA_CRL);
        final int port = createExternalProviderAndTlsPort(trustStoreAttributes);
        assertTlsConnectivity(port, TestSSLConstants.CERT_ALIAS_REVOKED);
    }

    @Test
    public void externalWithRevocationDisabledWithCrlUrlInRevokedCertificate() throws Exception
    {
        assumeThat("HTTP server failed to bind to port '" + CRL_HTTP_PORT + "'",
                CRL_SERVER, is(not(equalTo(null))));
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes();
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, false);
        final int port = createExternalProviderAndTlsPort(trustStoreAttributes);
        assertTlsConnectivity(port, TestSSLConstants.CERT_ALIAS_REVOKED);
    }

    @Test
    public void externalWithRevocationAndRevokedCertificateWithCrlUrlWithSoftFail() throws Exception
    {
        assumeThat("HTTP server failed to bind to port '" + CRL_HTTP_PORT + "'",
                CRL_SERVER, is(not(equalTo(null))));
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes();
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_WITH_IGNORING_SOFT_FAILURES, true);
        final int port = createExternalProviderAndTlsPort(trustStoreAttributes);
        assertTlsConnectivity(port, TestSSLConstants.CERT_ALIAS_REVOKED_INVALID_CRL_PATH);
    }

    @Test
    public void externalWithRevocationAndRevokedCertificateWithCrlUrlWithoutPreferCrls() throws Exception
    {
        assumeThat("HTTP server failed to bind to port '" + CRL_HTTP_PORT + "'",
                CRL_SERVER, is(not(equalTo(null))));
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes();
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_WITH_PREFERRING_CERTIFICATE_REVOCATION_LIST, false);
        final int port = createExternalProviderAndTlsPort(trustStoreAttributes);
        assertNoTlsConnectivity(port, TestSSLConstants.CERT_ALIAS_ALLOWED);
    }

    @Test
    public void externalWithRevocationAndRevokedCertificateWithCrlUrlWithoutPreferCrlsWithFallback() throws Exception
    {
        assumeThat("HTTP server failed to bind to port '" + CRL_HTTP_PORT + "'",
                CRL_SERVER, is(not(equalTo(null))));
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes();
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_WITH_PREFERRING_CERTIFICATE_REVOCATION_LIST, false);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_WITH_NO_FALLBACK, false);
        final int port = createExternalProviderAndTlsPort(trustStoreAttributes);
        assertTlsConnectivity(port, TestSSLConstants.CERT_ALIAS_ALLOWED);
    }

    @Test
    public void externalWithRevocationAndRevokedIntermediateCertificateWithCrlUrl() throws Exception
    {
        assumeThat("HTTP server failed to bind to port '" + CRL_HTTP_PORT + "'",
                CRL_SERVER, is(not(equalTo(null))));
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes();
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_OF_ONLY_END_ENTITY_CERTIFICATES, false);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_WITH_IGNORING_SOFT_FAILURES, true);
        final int port = createExternalProviderAndTlsPort(trustStoreAttributes);
        assertNoTlsConnectivity(port, TestSSLConstants.CERT_ALIAS_ALLOWED_WITH_INTERMEDIATE);
    }

    @Test
    public void externalWithRevocationAndRevokedIntermediateCertificateWithCrlUrlOnlyEndEntity() throws Exception
    {
        assumeThat("HTTP server failed to bind to port '" + CRL_HTTP_PORT + "'",
                CRL_SERVER, is(not(equalTo(null))));
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes();
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_OF_ONLY_END_ENTITY_CERTIFICATES, true);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_WITH_IGNORING_SOFT_FAILURES, true);
        final int port = createExternalProviderAndTlsPort(trustStoreAttributes);
        assertTlsConnectivity(port, TestSSLConstants.CERT_ALIAS_ALLOWED_WITH_INTERMEDIATE);
    }

    @Test
    public void externalDeniesUntrustedClientCert() throws Exception
    {
        assumeThat("QPID-8069", getProtocol(), is(anyOf(equalTo(Protocol.AMQP_1_0), equalTo(Protocol.AMQP_0_10))));
        final int port = createExternalProviderAndTlsPort();
        assertNoTlsConnectivity(port, TestSSLConstants.CERT_ALIAS_UNTRUSTED_CLIENT);
    }

    @Test
    public void externalDeniesExpiredClientCert() throws Exception
    {
        assumeThat("QPID-8069", getProtocol(), is(anyOf(equalTo(Protocol.AMQP_1_0), equalTo(Protocol.AMQP_0_10))));

        final Map<String, Object> trustStoreAttributes = new HashMap<>();
        trustStoreAttributes.put(FileTrustStore.STORE_URL, TestSSLConstants.BROKER_PEERSTORE);
        trustStoreAttributes.put(FileTrustStore.PASSWORD, TestSSLConstants.PASSWORD);
        trustStoreAttributes.put(FileTrustStore.TRUST_ANCHOR_VALIDITY_ENFORCED, true);
        final int port = createExternalProviderAndTlsPort(trustStoreAttributes);

        try
        {
            getConnectionBuilder().setPort(port)
                                  .setTls(true)
                                  .setSaslMechanisms(ExternalAuthenticationManagerImpl.MECHANISM_NAME)
                                  .setKeyStoreLocation(TestSSLConstants.CLIENT_EXPIRED_KEYSTORE)
                                  .setKeyStorePassword(TestSSLConstants.PASSWORD)
                                  .setTrustStoreLocation(TestSSLConstants.CLIENT_TRUSTSTORE)
                                  .setTrustStorePassword(TestSSLConstants.PASSWORD)
                                  .build();
            fail("Connection should not succeed");
        }
        catch (JMSException e)
        {
            e.printStackTrace();
        }
    }

    @Test
    public void externalWithPeersOnlyTrustStore() throws Exception
    {
        final Map<String, Object> trustStoreAttributes = new HashMap<>();
        trustStoreAttributes.put(FileTrustStore.STORE_URL, TestSSLConstants.BROKER_PEERSTORE);
        trustStoreAttributes.put(FileTrustStore.PASSWORD, TestSSLConstants.PASSWORD);
        trustStoreAttributes.put(FileTrustStore.PEERS_ONLY, true);
        final int port = createExternalProviderAndTlsPort(trustStoreAttributes);
        assertTlsConnectivity(port, TestSSLConstants.CERT_ALIAS_APP1);

        assumeThat("QPID-8069", getProtocol(), is(anyOf(equalTo(Protocol.AMQP_1_0), equalTo(Protocol.AMQP_0_10))));
        assertNoTlsConnectivity(port, TestSSLConstants.CERT_ALIAS_APP2);
    }

    @Test
    public void externalWithRegularAndPeersOnlyTrustStores() throws Exception
    {
        final String trustStoreName = getTestName() + "RegularTrustStore";
        final Connection brokerConnection = getConnectionBuilder().setVirtualHost("$management").build();
        try
        {
            brokerConnection.start();

            final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes();
            trustStoreAttributes.put(FileTrustStore.TRUST_STORE_TYPE, TestSSLConstants.JAVA_KEYSTORE_TYPE);

            createEntity(trustStoreName,
                         FileTrustStore.class.getName(),
                         trustStoreAttributes,
                         brokerConnection);

        }
        finally
        {
            brokerConnection.close();
        }

        final Map<String, Object> trustStoreAttributes = new HashMap<>();
        trustStoreAttributes.put(FileTrustStore.STORE_URL, TestSSLConstants.BROKER_PEERSTORE);
        trustStoreAttributes.put(FileTrustStore.PASSWORD,TestSSLConstants.PASSWORD);
        trustStoreAttributes.put(FileTrustStore.PEERS_ONLY, true);
        final int port = createExternalProviderAndTlsPort(trustStoreAttributes, trustStoreName, false);
        assertTlsConnectivity(port, TestSSLConstants.CERT_ALIAS_APP1);

        //use the app2 cert, which is NOT in the peerstore (but is signed by the same CA as app1)
        assertTlsConnectivity(port, TestSSLConstants.CERT_ALIAS_APP2);
    }

    @Test
    public void externalUsernameAsDN() throws Exception
    {
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes();

        final String clientId = getTestName();
        final int port = createExternalProviderAndTlsPort(trustStoreAttributes, null, true);
        final Connection connection = getConnectionBuilder().setPort(port)
                                                      .setTls(true)
                                                      .setSaslMechanisms(ExternalAuthenticationManagerImpl.MECHANISM_NAME)
                                                      .setKeyStoreLocation(TestSSLConstants.CLIENT_KEYSTORE)
                                                      .setKeyStorePassword(TestSSLConstants.PASSWORD)
                                                      .setTrustStoreLocation(TestSSLConstants.CLIENT_TRUSTSTORE)
                                                      .setTrustStorePassword(TestSSLConstants.PASSWORD)
                                                      .setKeyAlias(TestSSLConstants.CERT_ALIAS_APP2)
                                                      .setClientId(clientId)
                                                      .build();
        try
        {
            assertConnectionPrincipal( clientId, "CN=app2@acme.org,OU=art,O=acme,L=Toronto,ST=ON,C=CA");
        }
        catch (JMSException e)
        {
            fail("Should be able to create a connection to the SSL port: " + e.getMessage());
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void externalUsernameAsCN() throws Exception
    {
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes();

        final String clientId = getTestName();
        final int port = createExternalProviderAndTlsPort(trustStoreAttributes, null, false);
        final Connection connection = getConnectionBuilder().setPort(port)
                                                      .setTls(true)
                                                      .setSaslMechanisms(ExternalAuthenticationManagerImpl.MECHANISM_NAME)
                                                      .setKeyStoreLocation(TestSSLConstants.CLIENT_KEYSTORE)
                                                      .setKeyStorePassword(TestSSLConstants.PASSWORD)
                                                      .setTrustStoreLocation(TestSSLConstants.CLIENT_TRUSTSTORE)
                                                      .setTrustStorePassword(TestSSLConstants.PASSWORD)
                                                      .setKeyAlias(TestSSLConstants.CERT_ALIAS_APP2)
                                                      .setClientId(clientId)
                                                      .build();
        try
        {
            assertConnectionPrincipal( clientId, "app2@acme.org");
        }
        catch (JMSException e)
        {
            fail("Should be able to create a connection to the SSL port: " + e.getMessage());
        }
        finally
        {
            connection.close();
        }
    }

    private void assertConnectionPrincipal(final String clientId, final String expectedPrincipal) throws Exception
    {
        final Connection brokerConnection = getConnectionBuilder().setVirtualHost("$management").build();
        try
        {
            brokerConnection.start();

            String principal = null;
            final List<Map<String, Object>> connections = queryEntitiesUsingAmqpManagement("org.apache.qpid.Connection", brokerConnection);
            for (final Map<String, Object> connection : connections)
            {
                final String name = String.valueOf(connection.get(ConfiguredObject.NAME));
                final Map<String, Object> attributes;
                try
                {
                    attributes = readEntityUsingAmqpManagement(
                            getPortName() + "/" + name,
                            "org.apache.qpid.Connection",
                            false,
                            brokerConnection);
                }
                catch (AmqpManagementFacade.OperationUnsuccessfulException e)
                {
                    LOGGER.error("Read operation failed for an existing object '{}' having attributes '{}': {}",
                                 getPortName() + "/" + name,
                                 connection,
                                 e.getMessage(),
                                 e);
                    throw e;
                }
                if (attributes.get(org.apache.qpid.server.model.Connection.CLIENT_ID).equals(clientId))
                {
                    principal = String.valueOf(attributes.get(org.apache.qpid.server.model.Connection.PRINCIPAL));
                    break;
                }
            }
            assertEquals("Unexpected principal", expectedPrincipal, principal);
        }
        finally
        {
            brokerConnection.close();
        }
    }

    private Map<String, Object> getBrokerTrustStoreAttributes()
    {
        final Map<String, Object> trustStoreAttributes = new HashMap<>();
        trustStoreAttributes.put(FileTrustStore.STORE_URL, TestSSLConstants.BROKER_TRUSTSTORE);
        trustStoreAttributes.put(FileTrustStore.PASSWORD, TestSSLConstants.PASSWORD);
        return trustStoreAttributes;
    }

    private int createExternalProviderAndTlsPort() throws Exception
    {
        return createExternalProviderAndTlsPort(getBrokerTrustStoreAttributes());
    }

    private int createExternalProviderAndTlsPort(final Map<String, Object> trustStoreAttributes) throws Exception
    {
        return createExternalProviderAndTlsPort(trustStoreAttributes, null, false);
    }

    private int createExternalProviderAndTlsPort(final Map<String, Object> trustStoreAttributes,
                                                 final String additionalTrustStore,
                                                 final boolean useFullDN) throws Exception
    {
        final String providerName = getTestName();
        final Connection connection = getConnectionBuilder().setVirtualHost("$management").build();
        try
        {
            connection.start();

            final Map<String, Object> providerAttributes = new HashMap<>();
            providerAttributes.put("qpid-type", ExternalAuthenticationManager.PROVIDER_TYPE);
            providerAttributes.put(ExternalAuthenticationManager.ATTRIBUTE_USE_FULL_DN, useFullDN);
            createEntity(providerName,
                         AuthenticationProvider.class.getName(),
                         providerAttributes,
                         connection);

            final Map<String, Object> keyStoreAttributes = new HashMap<>();
            keyStoreAttributes.put("storeUrl", TestSSLConstants.BROKER_KEYSTORE);
            keyStoreAttributes.put("password", TestSSLConstants.PASSWORD);
            keyStoreAttributes.put("keyStoreType", TestSSLConstants.JAVA_KEYSTORE_TYPE);

            final String keyStoreName = providerName + "KeyStore";
            createEntity(keyStoreName,
                         FileKeyStore.class.getName(),
                         keyStoreAttributes,
                         connection);

            final Map<String, Object> trustStoreSettings = new HashMap<>(trustStoreAttributes);
            trustStoreSettings.put("trustStoreType", TestSSLConstants.JAVA_KEYSTORE_TYPE);
            final String trustStoreName = providerName + "TrustStore";
            createEntity(trustStoreName,
                         FileTrustStore.class.getName(),
                         trustStoreSettings,
                         connection);

            final String portName = getPortName();
            final Map<String, Object> sslPortAttributes = new HashMap<>();
            sslPortAttributes.put(Port.TRANSPORTS, "[\"SSL\"]");
            sslPortAttributes.put(Port.PORT, 0);
            sslPortAttributes.put(Port.AUTHENTICATION_PROVIDER, providerName);
            sslPortAttributes.put(Port.NEED_CLIENT_AUTH, true);
            sslPortAttributes.put(Port.WANT_CLIENT_AUTH, false);
            sslPortAttributes.put(Port.NAME, portName);
            sslPortAttributes.put(Port.KEY_STORE, keyStoreName);
            final String trustStores = additionalTrustStore == null
                    ? "[\"" + trustStoreName + "\"]"
                    : "[\"" + trustStoreName + "\",\"" + additionalTrustStore + "\"]";
            sslPortAttributes.put(Port.TRUST_STORES, trustStores);

            createEntity(portName,
                         "org.apache.qpid.AmqpPort",
                         sslPortAttributes,
                         connection);

            final Map<String, Object> portEffectiveAttributes =
                    readEntityUsingAmqpManagement(portName, "org.apache.qpid.AmqpPort", false, connection);
            if (portEffectiveAttributes.containsKey("boundPort"))
            {
                return (int) portEffectiveAttributes.get("boundPort");
            }
            throw new RuntimeException("Bound port is not found");
        }
        finally
        {
            connection.close();
        }
    }

    private String getPortName()
    {
        return getTestName() + "TlsPort";
    }

    private int createAuthenticationProviderAndUserAndPort(final String providerName,
                                                           final String providerType,
                                                           final String userName,
                                                           final String userPassword) throws Exception
    {
        final Connection connection = getConnectionBuilder().setVirtualHost("$management").build();
        try
        {
            connection.start();

            createEntity(providerName,
                         AuthenticationProvider.class.getName(),
                         Collections.singletonMap("qpid-type", providerType),
                         connection);
            final Map<String, Object> userAttributes = new HashMap<>();
            userAttributes.put("qpid-type", "managed");
            userAttributes.put(User.PASSWORD, userPassword);
            userAttributes.put("object-path", providerName);
            createEntity(userName, User.class.getName(), userAttributes, connection);

            final String portName = providerName + "Port";
            final Map<String, Object> portAttributes = new HashMap<>();
            portAttributes.put(Port.AUTHENTICATION_PROVIDER, providerName);
            portAttributes.put(Port.PORT, 0);
            createEntity(portName, "org.apache.qpid.AmqpPort", portAttributes, connection);

            final Map<String, Object> portEffectiveAttributes =
                    readEntityUsingAmqpManagement(portName, "org.apache.qpid.AmqpPort", false, connection);
            if (portEffectiveAttributes.containsKey("boundPort"))
            {
                return (int) portEffectiveAttributes.get("boundPort");
            }
            throw new RuntimeException("Bound port is not found");
        }
        finally
        {
            connection.close();
        }
    }

    private Connection getConnection(int port, String certificateAlias) throws NamingException, JMSException
    {
        return getConnectionBuilder().setPort(port)
                .setTls(true)
                .setSaslMechanisms(ExternalAuthenticationManagerImpl.MECHANISM_NAME)
                .setKeyStoreLocation(TestSSLConstants.CLIENT_KEYSTORE)
                .setKeyStorePassword(TestSSLConstants.PASSWORD)
                .setKeyAlias(certificateAlias)
                .setTrustStoreLocation(TestSSLConstants.CLIENT_TRUSTSTORE)
                .setTrustStorePassword(TestSSLConstants.PASSWORD)
                .build();
    }

    private void assertTlsConnectivity(int port, String certificateAlias) throws NamingException, JMSException
    {
        final Connection connection = getConnection(port, certificateAlias);
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            assertNotNull("Temporary queue was not created", session.createTemporaryQueue());
        }
        finally
        {
            connection.close();
        }
    }

    private void assertNoTlsConnectivity(int port, String certificateAlias) throws NamingException
    {
        try
        {
            getConnection(port, certificateAlias);
            fail("Connection should not succeed");
        }
        catch (JMSException e)
        {
            // pass
        }
    }

    private void assertPlainConnectivity(final int port,
                                         final String userName,
                                         final String userPassword,
                                         final String mechanism) throws Exception
    {
        final Connection connection = getConnectionBuilder().setPort(port)
                                                      .setUsername(userName)
                                                      .setPassword(userPassword)
                                                      .setSaslMechanisms(mechanism)
                                                      .build();
        try
        {
            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final TemporaryQueue queue = session.createTemporaryQueue();
            assertNotNull("Temporary queue was not created", queue);
        }
        finally
        {
            connection.close();
        }

        try
        {
            getConnectionBuilder().setPort(port)
                                  .setUsername(userName)
                                  .setPassword("invalid" + userPassword)
                                  .setSaslMechanisms(mechanism)
                                  .build();
            fail("Connection is established for invalid password");
        }
        catch (JMSException e)
        {
            // pass
        }

        try
        {
            getConnectionBuilder().setPort(port)
                                  .setUsername("invalid" + userName)
                                  .setPassword(userPassword)
                                  .setSaslMechanisms(mechanism)
                                  .build();
            fail("Connection is established for invalid user name");
        }
        catch (JMSException e)
        {
            // pass
        }
    }

    private static void createContext(Path crlPath)
    {
        final ContextHandler contextHandler = new ContextHandler();
        contextHandler.setContextPath("/" + crlPath.getFileName());
        contextHandler.setHandler(new CrlServerHandler(crlPath));
        HANDLERS.addHandler(contextHandler);
    }

    private static class CrlServerHandler extends AbstractHandler
    {
        final Path crlPath;
        public CrlServerHandler(Path crlPath)
        {
            this.crlPath = crlPath;
        }

        @Override
        public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
                throws IOException
        {
            final byte[] crlBytes = Files.readAllBytes(crlPath);
            response.setStatus(HttpServletResponse.SC_OK);
            final OutputStream responseBody = response.getOutputStream();
            responseBody.write(crlBytes);
            responseBody.close();
        }
    }
}
