/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 *
 */
package org.apache.qpid.server.security.auth.manager;

import static org.apache.qpid.test.utils.TestSSLConstants.BROKER_PEERSTORE;
import static org.apache.qpid.test.utils.TestSSLConstants.BROKER_PEERSTORE_PASSWORD;
import static org.apache.qpid.test.utils.TestSSLConstants.KEYSTORE;
import static org.apache.qpid.test.utils.TestSSLConstants.KEYSTORE_PASSWORD;
import static org.apache.qpid.test.utils.TestSSLConstants.TRUSTSTORE;
import static org.apache.qpid.test.utils.TestSSLConstants.TRUSTSTORE_PASSWORD;
import static org.apache.qpid.test.utils.TestSSLConstants.UNTRUSTED_KEYSTORE;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.naming.InitialContext;

import org.apache.qpid.client.AMQConnectionURL;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.DefaultVirtualHostAlias;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.server.model.VirtualHostAlias;
import org.apache.qpid.server.model.VirtualHostNameAlias;
import org.apache.qpid.server.security.FileTrustStore;
import org.apache.qpid.systest.rest.RestTestHelper;
import org.apache.qpid.test.utils.QpidBrokerTestCase;
import org.apache.qpid.test.utils.TestBrokerConfiguration;
import org.apache.qpid.test.utils.TestSSLConstants;

public class ExternalAuthenticationTest extends QpidBrokerTestCase
{
    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
        setSystemProperty("javax.net.debug", "ssl");
        setSystemProperty("javax.net.ssl.keyStore", null);
        setSystemProperty("javax.net.ssl.keyStorePassword", null);
        setSystemProperty("javax.net.ssl.trustStore", null);
        setSystemProperty("javax.net.ssl.trustStorePassword", null);

    }

    @Override
    public void startDefaultBroker()
    {
        // each test start the broker in its own way
    }

    /**
     * Tests that when EXTERNAL authentication is used on the SSL port, clients presenting certificates are able to connect.
     * Also, checks that default authentication manager PrincipalDatabaseAuthenticationManager is used on non SSL port.
     */
    public void testExternalAuthenticationManagerOnSSLPort() throws Exception
    {
        setCommonBrokerSSLProperties(true);
        super.startDefaultBroker();

        try
        {
            final Connection connection =
                    getExternalSSLConnection(false, TRUSTSTORE, TRUSTSTORE_PASSWORD, KEYSTORE, KEYSTORE_PASSWORD, null);
            connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        }
        catch (JMSException e)
        {
            fail("Should be able to create a connection to the SSL port: " + e.getMessage());
        }

        try
        {
            getConnection();
        }
        catch (JMSException e)
        {
            fail("Should be able to create a connection with credentials to the standard port: " + e.getMessage());
        }

    }

    /**
     * Tests that when EXTERNAL authentication manager is set on the non-SSL port, clients with valid username and password
     * but not using ssl are unable to connect to the non-SSL port.
     */
    public void testExternalAuthenticationManagerOnNonSslPort() throws Exception
    {
        setCommonBrokerSSLProperties(true);
        getDefaultBrokerConfiguration().setObjectAttribute(Port.class, TestBrokerConfiguration.ENTRY_NAME_AMQP_PORT, Port.AUTHENTICATION_PROVIDER, TestBrokerConfiguration.ENTRY_NAME_EXTERNAL_PROVIDER);
        super.startDefaultBroker();

        try
        {
            getConnection();
            fail("Connection should not succeed");
        }
        catch (JMSException e)
        {
            // pass
        }
    }

    /**
     * Tests that when EXTERNAL authentication manager is used, clients without certificates are unable to connect to the SSL port
     * even with valid username and password.
     */
    public void testExternalAuthenticationManagerWithoutClientKeyStore() throws Exception
    {
        setCommonBrokerSSLProperties(false);
        super.startDefaultBroker();

        try
        {
            final Connection connection =
                    getExternalSSLConnection(true, TRUSTSTORE, TRUSTSTORE_PASSWORD, null, null, null);
            connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            fail("Connection should not succeed");
        }
        catch (JMSException e)
        {
            // pass
        }
    }

    /**
     * Tests that when using the EXTERNAL authentication provider and needing client auth, clients with
     * untrusted certificates are unable to connect to the SSL port.
     */
    public void testExternalAuthenticationDeniesUntrustedClientCert() throws Exception
    {
        setCommonBrokerSSLProperties(true);
        super.startDefaultBroker();

        try
        {
            getExternalSSLConnection(false,
                                     TRUSTSTORE,
                                     TRUSTSTORE_PASSWORD,
                                     UNTRUSTED_KEYSTORE,
                                     KEYSTORE_PASSWORD,
                                     TestSSLConstants.CERT_ALIAS_UNTRUSTED_CLIENT
                                    );
            fail("Connection should not succeed");
        }
        catch (JMSException e)
        {
            // pass
        }
    }

    /**
     * Tests that when using the EXTERNAL auth provider and a 'peersOnly' truststore, clients using certs directly in
     * in the store will be able to connect and clients using certs signed by the same CA but not in the store will not.
     */
    public void testExternalAuthenticationWithPeersOnlyTrustStore() throws Exception
    {
        externalAuthenticationWithPeersOnlyTrustStoreTestImpl(false);
    }

    /**
     * Tests that when using the EXTERNAL auth provider, with both the regular trust store and a 'peersOnly' truststore, clients
     * using certs signed by the CA in the trust store are allowed even if they are not present in the 'peersOnly' store.
     */
    public void testExternalAuthenticationWithRegularAndPeersOnlyTrustStores() throws Exception
    {
        externalAuthenticationWithPeersOnlyTrustStoreTestImpl(true);
    }

    private void externalAuthenticationWithPeersOnlyTrustStoreTestImpl(boolean useTrustAndPeerStore) throws Exception
    {
        String peerStoreName = "myPeerStore";

        List<String> storeNames = null;
        if(useTrustAndPeerStore)
        {
            //Use the regular trust store AND the 'peersOnly' store. The regular trust store trusts the CA that
            //signed both the app1 and app2 certs. The peersOnly store contains only app1 and so does not trust app2
            storeNames = Arrays.asList(TestBrokerConfiguration.ENTRY_NAME_SSL_TRUSTSTORE, peerStoreName);
        }
        else
        {
            //use only the 'peersOnly' store, which contains only app1 and so does not trust app2
            storeNames = Arrays.asList(peerStoreName);
        }

        //set the brokers SSL config, inc which SSL stores to use
        setCommonBrokerSSLProperties(true, storeNames);

        //add the peersOnly store to the config
        Map<String, Object> sslTrustStoreAttributes = new HashMap<String, Object>();
        sslTrustStoreAttributes.put(TrustStore.NAME, peerStoreName);
        sslTrustStoreAttributes.put(FileTrustStore.STORE_URL, BROKER_PEERSTORE);
        sslTrustStoreAttributes.put(FileTrustStore.PASSWORD, BROKER_PEERSTORE_PASSWORD);
        sslTrustStoreAttributes.put(FileTrustStore.PEERS_ONLY, true);
        getDefaultBrokerConfiguration().addObjectConfiguration(TrustStore.class, sslTrustStoreAttributes);

        super.startDefaultBroker();

        try
        {
            //use the app1 cert, which IS in the peerstore (and has CA in the trustStore)
            final Connection connection = getExternalSSLConnection(false,
                                                                   TRUSTSTORE,
                                                                   TRUSTSTORE_PASSWORD,
                                                                   KEYSTORE,
                                                                   KEYSTORE_PASSWORD,
                                                                   "app1");
            connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        }
        catch (JMSException e)
        {
            fail("Client's validation against the broker's multi store manager unexpectedly failed, when configured store was expected to allow.");
        }

        try
        {
            //use the app2 cert, which is NOT in the peerstore (but is signed by the same CA as app1)
            getExternalSSLConnection(false, TRUSTSTORE, TRUSTSTORE_PASSWORD, KEYSTORE, KEYSTORE_PASSWORD, "app2");
            if(!useTrustAndPeerStore)
            {
                fail("Client's validation against the broker's multi store manager unexpectedly passed, when configured store was expected to deny.");
            }
        }
        catch (JMSException e)
        {
            if(useTrustAndPeerStore)
            {
                fail("Client's validation against the broker's multi store manager unexpectedly failed, when configured store was expected to allow.");
            }
            else
            {
                //expected, the CA in trust store should allow both app1 and app2
            }
        }
    }

    /**
     * Tests the creation of usernames when EXTERNAL authentication is used.
     * The username should be created as CN@DC1.DC2...DCn by default.
     */
    public void testExternalAuthenticationManagerUsernameAsCN() throws Exception
    {
        getDefaultBrokerConfiguration().addHttpManagementConfiguration();
        setCommonBrokerSSLProperties(true);

        super.startDefaultBroker();

        try
        {
            final Connection connection = getExternalSSLConnection(false,
                                                                   TRUSTSTORE,
                                                                   TRUSTSTORE_PASSWORD,
                                                                   KEYSTORE,
                                                                   KEYSTORE_PASSWORD,
                                                                   "app2");

            connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        }
        catch (JMSException e)
        {
            fail("Should be able to create a connection to the SSL port: " + e.getMessage());
        }

        RestTestHelper restTestHelper = new RestTestHelper(getDefaultBroker().getHttpPort());
        try
        {
            // Getting the used username using REST
            Map<String, Object> connectionAttributes = restTestHelper.getJsonAsSingletonList("connection");
            assertEquals("Wrong authorized ID", "app2@acme.org", (String) connectionAttributes.get(org.apache.qpid.server.model.Connection.PRINCIPAL));
        }
        finally
        {
            restTestHelper.tearDown();
        }
    }

    /**
     * Tests the creation of usernames when EXTERNAL authentication is used.
     * The username should be created as full DN when the useFullDN option is used.
     */
    public void testExternalAuthenticationManagerUsernameAsDN() throws Exception
    {
        TestBrokerConfiguration brokerConfiguration = getDefaultBrokerConfiguration();
        brokerConfiguration.addHttpManagementConfiguration();
        setCommonBrokerSSLProperties(true);
        brokerConfiguration.setObjectAttribute(AuthenticationProvider.class,
                                                    TestBrokerConfiguration.ENTRY_NAME_EXTERNAL_PROVIDER,
                                                    ExternalAuthenticationManager.ATTRIBUTE_USE_FULL_DN,
                                                    "true");

        super.startDefaultBroker();

        try
        {
            final Connection connection = getExternalSSLConnection(false,
                                                                   TRUSTSTORE,
                                                                   TRUSTSTORE_PASSWORD,
                                                                   KEYSTORE,
                                                                   KEYSTORE_PASSWORD,
                                                                   "app2");
            connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        }
        catch (JMSException e)
        {
            fail("Should be able to create a connection to the SSL port: " + e.getMessage());
        }

        RestTestHelper restTestHelper = new RestTestHelper(getDefaultBroker().getHttpPort());
        try
        {
            // Getting the used username using REST
            Map<String, Object> connectionAttributes = restTestHelper.getJsonAsSingletonList("connection");
            assertEquals("Wrong authorized ID", "CN=app2@acme.org,OU=art,O=acme,L=Toronto,ST=ON,C=CA",
                         (String) connectionAttributes.get(org.apache.qpid.server.model.Connection.PRINCIPAL));
        }
        finally
        {
            restTestHelper.tearDown();
        }
    }

    private Connection getExternalSSLConnection(boolean includeUserNameAndPassword,
                                                final String trustStoreLocation,
                                                final String trustStorePassword,
                                                final String keyStoreLocation,
                                                final String keyStorePassword,
                                                final String certAlias) throws Exception
    {
        if(isBroker10())
        {
            final Hashtable<String, String> env = new Hashtable<>();
            final StringBuilder uri = new StringBuilder("amqps://localhost:").append(String.valueOf(getDefaultBroker().getAmqpTlsPort())).append("?amqp.vhost=test&amqp.saslMechanisms=EXTERNAL");
            if(trustStoreLocation != null)
            {
                uri.append("&transport.trustStoreLocation=").append(trustStoreLocation);
            }
            if(trustStorePassword != null)
            {
                uri.append("&transport.trustStorePassword=").append(trustStorePassword);
            }
            if(keyStoreLocation != null)
            {
                uri.append("&transport.keyStoreLocation=").append(keyStoreLocation);
            }
            if(keyStorePassword != null)
            {
                uri.append("&transport.keyStorePassword=").append(keyStorePassword);
            }
            if(certAlias != null)
            {
                uri.append("&transport.keyAlias=").append(certAlias);
            }
            env.put("connectionfactory.externalauth", uri.toString());
            InitialContext initialContext = new InitialContext(env);
            final ConnectionFactory connectionFactory = (ConnectionFactory) initialContext.lookup("externalauth");
            if(includeUserNameAndPassword)
            {
                return connectionFactory.createConnection("guest","guest");
            }
            else
            {
                return connectionFactory.createConnection();
            }
        }
        else
        {
            setSystemProperty("javax.net.ssl.keyStore", keyStoreLocation);
            setSystemProperty("javax.net.ssl.keyStorePassword", keyStorePassword);
            setSystemProperty("javax.net.ssl.trustStore", trustStoreLocation);
            setSystemProperty("javax.net.ssl.trustStorePassword", trustStorePassword);
            String certAliasOption = certAlias == null ? "" : "&ssl_cert_alias='"+certAlias+"'";
            int amqpTlsPort = getDefaultBroker().getAmqpTlsPort();
            String url = "amqp://%s@test/?brokerlist='tcp://localhost:%s?ssl='true'&sasl_mechs='EXTERNAL'%s'";
            if (includeUserNameAndPassword)
            {
                url = String.format(url, "guest:guest", String.valueOf(amqpTlsPort), certAliasOption);
            }
            else
            {
                url = String.format(url, ":", String.valueOf(amqpTlsPort), certAliasOption);
            }
            return getConnection(new AMQConnectionURL(url));
        }
    }

    private void setCommonBrokerSSLProperties(boolean needClientAuth)
    {
        setCommonBrokerSSLProperties(needClientAuth, Collections.singleton(TestBrokerConfiguration.ENTRY_NAME_SSL_TRUSTSTORE));
    }

    private void setCommonBrokerSSLProperties(boolean needClientAuth, Collection<String> trustStoreNames)
    {
        TestBrokerConfiguration config = getDefaultBrokerConfiguration();

        Map<String, Object> sslPortAttributes = new HashMap<String, Object>();
        sslPortAttributes.put(Port.TRANSPORTS, Collections.singleton(Transport.SSL));
        sslPortAttributes.put(Port.PORT, DEFAULT_SSL_PORT);
        sslPortAttributes.put(Port.NEED_CLIENT_AUTH, String.valueOf(needClientAuth));
        sslPortAttributes.put(Port.NAME, TestBrokerConfiguration.ENTRY_NAME_SSL_PORT);
        sslPortAttributes.put(Port.KEY_STORE, TestBrokerConfiguration.ENTRY_NAME_SSL_KEYSTORE);
        sslPortAttributes.put(Port.TRUST_STORES, trustStoreNames);
        sslPortAttributes.put(Port.PROTOCOLS, System.getProperty(TEST_AMQP_PORT_PROTOCOLS_PROPERTY));

        config.addObjectConfiguration(Port.class, sslPortAttributes);

        Map<String, Object> aliasAttributes = new HashMap<>();
        aliasAttributes.put(VirtualHostAlias.NAME, "defaultAlias");
        aliasAttributes.put(VirtualHostAlias.TYPE, DefaultVirtualHostAlias.TYPE_NAME);
        getDefaultBrokerConfiguration().addObjectConfiguration(Port.class, TestBrokerConfiguration.ENTRY_NAME_SSL_PORT, VirtualHostAlias.class, aliasAttributes);

        aliasAttributes = new HashMap<>();
        aliasAttributes.put(VirtualHostAlias.NAME, "nameAlias");
        aliasAttributes.put(VirtualHostAlias.TYPE, VirtualHostNameAlias.TYPE_NAME);
        getDefaultBrokerConfiguration().addObjectConfiguration(Port.class, TestBrokerConfiguration.ENTRY_NAME_SSL_PORT, VirtualHostAlias.class, aliasAttributes);


        Map<String, Object> externalAuthProviderAttributes = new HashMap<String, Object>();
        externalAuthProviderAttributes.put(AuthenticationProvider.NAME, TestBrokerConfiguration.ENTRY_NAME_EXTERNAL_PROVIDER);
        externalAuthProviderAttributes.put(AuthenticationProvider.TYPE, ExternalAuthenticationManager.PROVIDER_TYPE);
        config.addObjectConfiguration(AuthenticationProvider.class, externalAuthProviderAttributes);

        config.setObjectAttribute(Port.class, TestBrokerConfiguration.ENTRY_NAME_SSL_PORT, Port.AUTHENTICATION_PROVIDER, TestBrokerConfiguration.ENTRY_NAME_EXTERNAL_PROVIDER);
    }

}
