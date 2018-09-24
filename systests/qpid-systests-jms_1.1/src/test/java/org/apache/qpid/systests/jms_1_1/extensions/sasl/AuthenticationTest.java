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

import static org.apache.qpid.systests.jms_1_1.extensions.tls.TlsTest.BROKER_KEYSTORE;
import static org.apache.qpid.systests.jms_1_1.extensions.tls.TlsTest.BROKER_TRUSTSTORE;
import static org.apache.qpid.systests.jms_1_1.extensions.tls.TlsTest.KEYSTORE;
import static org.apache.qpid.systests.jms_1_1.extensions.tls.TlsTest.TEST_PROFILE_RESOURCE_BASE;
import static org.apache.qpid.systests.jms_1_1.extensions.tls.TlsTest.TRUSTSTORE;
import static org.apache.qpid.test.utils.TestSSLConstants.BROKER_KEYSTORE_PASSWORD;
import static org.apache.qpid.test.utils.TestSSLConstants.BROKER_PEERSTORE;
import static org.apache.qpid.test.utils.TestSSLConstants.BROKER_PEERSTORE_PASSWORD;
import static org.apache.qpid.test.utils.TestSSLConstants.BROKER_TRUSTSTORE_PASSWORD;
import static org.apache.qpid.test.utils.TestSSLConstants.CERT_ALIAS_APP1;
import static org.apache.qpid.test.utils.TestSSLConstants.CERT_ALIAS_APP2;
import static org.apache.qpid.test.utils.TestSSLConstants.EXPIRED_KEYSTORE;
import static org.apache.qpid.test.utils.TestSSLConstants.KEYSTORE_PASSWORD;
import static org.apache.qpid.test.utils.TestSSLConstants.TRUSTSTORE_PASSWORD;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.TemporaryQueue;

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

    @BeforeClass
    public static void setUp() throws Exception
    {
        System.setProperty("javax.net.debug", "ssl");

        // workaround for QPID-8069
        if (getProtocol() != Protocol.AMQP_1_0 && getProtocol() != Protocol.AMQP_0_10)
        {
            System.setProperty("amqj.MaximumStateWait", "4000");
        }
    }

    @AfterClass
    public static void tearDown() throws Exception
    {
        System.clearProperty("javax.net.debug");
        if (getProtocol() != Protocol.AMQP_1_0)
        {
            System.clearProperty("amqj.MaximumStateWait");
        }
    }


    @Test
    public void md5() throws Exception
    {
        assumeThat("Qpid JMS Client does not support MD5 mechanisms",
                   getProtocol(),
                   is(not(equalTo(Protocol.AMQP_1_0))));

        int port = createAuthenticationProviderAndUserAndPort(getTestName(), "MD5", USER, USER_PASSWORD);

        assertConnectivity(port, USER, USER_PASSWORD, CramMd5HashedNegotiator.MECHANISM);
    }

    @Test
    public void sha256() throws Exception
    {
        int port = createAuthenticationProviderAndUserAndPort(getTestName(),
                                                              ScramSHA256AuthenticationManager.PROVIDER_TYPE,
                                                              USER,
                                                              USER_PASSWORD);

        assertConnectivity(port, USER, USER_PASSWORD, ScramSHA256AuthenticationManager.MECHANISM);
    }

    @Test
    public void sha1() throws Exception
    {
        int port = createAuthenticationProviderAndUserAndPort(getTestName(),
                                                              ScramSHA1AuthenticationManager.PROVIDER_TYPE,
                                                              USER,
                                                              USER_PASSWORD);

        assertConnectivity(port, USER, USER_PASSWORD, ScramSHA1AuthenticationManager.MECHANISM);
    }

    @Test
    public void external() throws Exception
    {
        int port = createExternalProviderAndTlsPort();

        Connection connection = getConnectionBuilder().setSslPort(port)
                                                      .setTls(true)
                                                      .setSaslMechanisms(ExternalAuthenticationManagerImpl.MECHANISM_NAME)
                                                      .setKeyStoreLocation(KEYSTORE)
                                                      .setKeyStorePassword(KEYSTORE_PASSWORD)
                                                      .setTrustStoreLocation(TRUSTSTORE)
                                                      .setTrustStorePassword(TRUSTSTORE_PASSWORD)
                                                      .build();
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

    @Test
    public void externalDeniesUntrustedClientCert() throws Exception
    {
        assumeThat("QPID-8069", getProtocol(), is(anyOf(equalTo(Protocol.AMQP_1_0), equalTo(Protocol.AMQP_0_10))));

        int port = createExternalProviderAndTlsPort();

        try
        {
            getConnectionBuilder().setSslPort(port)
                                  .setTls(true)
                                  .setSaslMechanisms(ExternalAuthenticationManagerImpl.MECHANISM_NAME)
                                  .setKeyStoreLocation(KEYSTORE)
                                  .setKeyStorePassword(KEYSTORE_PASSWORD)
                                  .setTrustStoreLocation(TRUSTSTORE)
                                  .setTrustStorePassword(TRUSTSTORE_PASSWORD)
                                  .setKeyAlias(TestSSLConstants.CERT_ALIAS_UNTRUSTED_CLIENT)
                                  .build();
            fail("Connection should not succeed");
        }
        catch (JMSException e)
        {
            // pass
        }
    }

    @Test
    public void externalDeniesExpiredClientCert() throws Exception
    {
        assumeThat("QPID-8069", getProtocol(), is(anyOf(equalTo(Protocol.AMQP_1_0), equalTo(Protocol.AMQP_0_10))));

        Map<String, Object> trustStoreAttributes = new HashMap<>();
        trustStoreAttributes.put(FileTrustStore.STORE_URL, TEST_PROFILE_RESOURCE_BASE + BROKER_PEERSTORE);
        trustStoreAttributes.put(FileTrustStore.PASSWORD, BROKER_PEERSTORE_PASSWORD);
        trustStoreAttributes.put(FileTrustStore.TRUST_ANCHOR_VALIDITY_ENFORCED, true);
        int port = createExternalProviderAndTlsPort(trustStoreAttributes);

        try
        {
            getConnectionBuilder().setSslPort(port)
                                  .setTls(true)
                                  .setSaslMechanisms(ExternalAuthenticationManagerImpl.MECHANISM_NAME)
                                  .setKeyStoreLocation(TEST_PROFILE_RESOURCE_BASE + EXPIRED_KEYSTORE)
                                  .setKeyStorePassword(KEYSTORE_PASSWORD)
                                  .setTrustStoreLocation(TRUSTSTORE)
                                  .setTrustStorePassword(TRUSTSTORE_PASSWORD)
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
        Map<String, Object> trustStoreAttributes = new HashMap<>();
        trustStoreAttributes.put(FileTrustStore.STORE_URL, TEST_PROFILE_RESOURCE_BASE + BROKER_PEERSTORE);
        trustStoreAttributes.put(FileTrustStore.PASSWORD, BROKER_PEERSTORE_PASSWORD);
        trustStoreAttributes.put(FileTrustStore.PEERS_ONLY, true);
        int port = createExternalProviderAndTlsPort(trustStoreAttributes);

        Connection connection = getConnectionBuilder().setSslPort(port)
                                                      .setTls(true)
                                                      .setSaslMechanisms(ExternalAuthenticationManagerImpl.MECHANISM_NAME)
                                                      .setKeyStoreLocation(KEYSTORE)
                                                      .setKeyStorePassword(KEYSTORE_PASSWORD)
                                                      .setTrustStoreLocation(TRUSTSTORE)
                                                      .setTrustStorePassword(TRUSTSTORE_PASSWORD)
                                                      .setKeyAlias(CERT_ALIAS_APP1)
                                                      .build();
        try
        {
            connection.createSession(false, Session.AUTO_ACKNOWLEDGE).close();
        }
        finally
        {
            connection.close();
        }

        assumeThat("QPID-8069", getProtocol(), is(anyOf(equalTo(Protocol.AMQP_1_0), equalTo(Protocol.AMQP_0_10))));
        try
        {

            getConnectionBuilder().setSslPort(port)
                                  .setTls(true)
                                  .setSaslMechanisms(ExternalAuthenticationManagerImpl.MECHANISM_NAME)
                                  .setKeyStoreLocation(KEYSTORE)
                                  .setKeyStorePassword(KEYSTORE_PASSWORD)
                                  .setTrustStoreLocation(TRUSTSTORE)
                                  .setTrustStorePassword(TRUSTSTORE_PASSWORD)
                                  .setKeyAlias(CERT_ALIAS_APP2)
                                  .build();
            fail("app2 certificate is NOT in the peerstore");
        }
        catch (JMSException e)
        {
            // pass
        }

    }

    @Test
    public void externalWithRegularAndPeersOnlyTrustStores() throws Exception
    {
        String trustStoreName = getTestName() + "RegularTrustStore";
        Connection brokerConnection = getConnectionBuilder().setVirtualHost("$management").build();
        try
        {
            brokerConnection.start();

            Map<String, Object> trustStoreAttributes = new HashMap<>();
            trustStoreAttributes.put(FileTrustStore.STORE_URL, BROKER_TRUSTSTORE);
            trustStoreAttributes.put(FileTrustStore.PASSWORD, BROKER_TRUSTSTORE_PASSWORD);

            createEntity(trustStoreName,
                         FileTrustStore.class.getName(),
                         trustStoreAttributes,
                         brokerConnection);

        }
        finally
        {
            brokerConnection.close();
        }

        Map<String, Object> trustStoreAttributes = new HashMap<>();
        trustStoreAttributes.put(FileTrustStore.STORE_URL, TEST_PROFILE_RESOURCE_BASE + BROKER_PEERSTORE);
        trustStoreAttributes.put(FileTrustStore.PASSWORD, BROKER_PEERSTORE_PASSWORD);
        trustStoreAttributes.put(FileTrustStore.PEERS_ONLY, true);
        int port = createExternalProviderAndTlsPort(trustStoreAttributes, trustStoreName, false);

        Connection connection = getConnectionBuilder().setSslPort(port)
                                                      .setTls(true)
                                                      .setSaslMechanisms(ExternalAuthenticationManagerImpl.MECHANISM_NAME)
                                                      .setKeyStoreLocation(KEYSTORE)
                                                      .setKeyStorePassword(KEYSTORE_PASSWORD)
                                                      .setTrustStoreLocation(TRUSTSTORE)
                                                      .setTrustStorePassword(TRUSTSTORE_PASSWORD)
                                                      .setKeyAlias(CERT_ALIAS_APP1)
                                                      .build();
        try
        {
            connection.createSession(false, Session.AUTO_ACKNOWLEDGE).close();
        }
        finally
        {
            connection.close();
        }

        //use the app2 cert, which is NOT in the peerstore (but is signed by the same CA as app1)
        Connection connection2 = getConnectionBuilder().setSslPort(port)
                              .setTls(true)
                              .setSaslMechanisms(ExternalAuthenticationManagerImpl.MECHANISM_NAME)
                              .setKeyStoreLocation(KEYSTORE)
                              .setKeyStorePassword(KEYSTORE_PASSWORD)
                              .setTrustStoreLocation(TRUSTSTORE)
                              .setTrustStorePassword(TRUSTSTORE_PASSWORD)
                              .setKeyAlias(CERT_ALIAS_APP2)
                              .build();

        try
        {
            connection2.createSession(false, Session.AUTO_ACKNOWLEDGE).createTemporaryQueue();
        }
        finally
        {
            connection2.close();
        }
    }

    @Test
    public void externalUsernameAsDN() throws Exception
    {
        Map<String, Object> trustStoreAttributes = new HashMap<>();
        trustStoreAttributes.put(FileTrustStore.STORE_URL, BROKER_TRUSTSTORE);
        trustStoreAttributes.put(FileTrustStore.PASSWORD, BROKER_TRUSTSTORE_PASSWORD);

        String clientId = getTestName();
        int port = createExternalProviderAndTlsPort(trustStoreAttributes, null, true);
        Connection connection = getConnectionBuilder().setSslPort(port)
                                                      .setTls(true)
                                                      .setSaslMechanisms(ExternalAuthenticationManagerImpl.MECHANISM_NAME)
                                                      .setKeyStoreLocation(KEYSTORE)
                                                      .setKeyStorePassword(KEYSTORE_PASSWORD)
                                                      .setTrustStoreLocation(TRUSTSTORE)
                                                      .setTrustStorePassword(TRUSTSTORE_PASSWORD)
                                                      .setKeyAlias(CERT_ALIAS_APP2)
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
        Map<String, Object> trustStoreAttributes = new HashMap<>();
        trustStoreAttributes.put(FileTrustStore.STORE_URL, BROKER_TRUSTSTORE);
        trustStoreAttributes.put(FileTrustStore.PASSWORD, BROKER_TRUSTSTORE_PASSWORD);

        String clientId = getTestName();
        int port = createExternalProviderAndTlsPort(trustStoreAttributes, null, false);
        Connection connection = getConnectionBuilder().setSslPort(port)
                                                      .setTls(true)
                                                      .setSaslMechanisms(ExternalAuthenticationManagerImpl.MECHANISM_NAME)
                                                      .setKeyStoreLocation(KEYSTORE)
                                                      .setKeyStorePassword(KEYSTORE_PASSWORD)
                                                      .setTrustStoreLocation(TRUSTSTORE)
                                                      .setTrustStorePassword(TRUSTSTORE_PASSWORD)
                                                      .setKeyAlias(CERT_ALIAS_APP2)
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
        Connection brokerConnection = getConnectionBuilder().setVirtualHost("$management").build();
        try
        {
            brokerConnection.start();

            String principal = null;
            List<Map<String, Object>> connections = queryEntitiesUsingAmqpManagement("org.apache.qpid.Connection", brokerConnection);
            for (Map<String, Object> connection : connections)
            {
                String name = String.valueOf(connection.get(ConfiguredObject.NAME));
                Map<String, Object> attributes;
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

    private int createExternalProviderAndTlsPort() throws Exception
    {
        Map<String, Object> trustStoreAttributes = new HashMap<>();
        trustStoreAttributes.put(FileTrustStore.STORE_URL, BROKER_TRUSTSTORE);
        trustStoreAttributes.put(FileTrustStore.PASSWORD, BROKER_TRUSTSTORE_PASSWORD);
        return createExternalProviderAndTlsPort(trustStoreAttributes);
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
        Connection connection = getConnectionBuilder().setVirtualHost("$management").build();
        try
        {
            connection.start();

            Map<String, Object> providerAttributes = new HashMap<>();
            providerAttributes.put("qpid-type", ExternalAuthenticationManager.PROVIDER_TYPE);
            providerAttributes.put(ExternalAuthenticationManager.ATTRIBUTE_USE_FULL_DN, useFullDN);
            createEntity(providerName,
                         AuthenticationProvider.class.getName(),
                         providerAttributes,
                         connection);

            final Map<String, Object> keyStoreAttributes = new HashMap<>();
            keyStoreAttributes.put("storeUrl", BROKER_KEYSTORE);
            keyStoreAttributes.put("password", BROKER_KEYSTORE_PASSWORD);

            final String keyStoreName = providerName + "KeyStore";
            createEntity(keyStoreName,
                         FileKeyStore.class.getName(),
                         keyStoreAttributes,
                         connection);


            final String trustStoreName = providerName + "TrustStore";
            createEntity(trustStoreName,
                         FileTrustStore.class.getName(),
                         trustStoreAttributes,
                         connection);

            String portName = getPortName();
            Map<String, Object> sslPortAttributes = new HashMap<>();
            sslPortAttributes.put(Port.TRANSPORTS, "[\"SSL\"]");
            sslPortAttributes.put(Port.PORT, 0);
            sslPortAttributes.put(Port.AUTHENTICATION_PROVIDER, providerName);
            sslPortAttributes.put(Port.NEED_CLIENT_AUTH, true);
            sslPortAttributes.put(Port.WANT_CLIENT_AUTH, false);
            sslPortAttributes.put(Port.NAME, portName);
            sslPortAttributes.put(Port.KEY_STORE, keyStoreName);
            String trustStores = additionalTrustStore == null
                    ? "[\"" + trustStoreName + "\"]"
                    : "[\"" + trustStoreName + "\",\"" + additionalTrustStore + "\"]";
            sslPortAttributes.put(Port.TRUST_STORES, trustStores);

            createEntity(portName,
                         "org.apache.qpid.AmqpPort",
                         sslPortAttributes,
                         connection);

            Map<String, Object> portEffectiveAttributes =
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
        Connection connection = getConnectionBuilder().setVirtualHost("$management").build();
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

            String portName = providerName + "Port";
            final Map<String, Object> portAttributes = new HashMap<>();
            portAttributes.put(Port.AUTHENTICATION_PROVIDER, providerName);
            portAttributes.put(Port.PORT, 0);
            createEntity(portName, "org.apache.qpid.AmqpPort", portAttributes, connection);

            Map<String, Object> portEffectiveAttributes =
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

    private void assertConnectivity(final int port,
                                    final String userName,
                                    final String userPassword,
                                    final String mechanism) throws Exception
    {
        Connection connection = getConnectionBuilder().setPort(port)
                                                      .setUsername(userName)
                                                      .setPassword(userPassword)
                                                      .setSaslMechanisms(mechanism)
                                                      .build();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            TemporaryQueue queue = session.createTemporaryQueue();
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
}
