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
package org.apache.qpid.systests.jms_1_1.extensions.tls;

import static org.apache.qpid.test.utils.TestSSLConstants.BROKER_KEYSTORE_PASSWORD;
import static org.apache.qpid.test.utils.TestSSLConstants.BROKER_TRUSTSTORE_PASSWORD;
import static org.apache.qpid.test.utils.TestSSLConstants.KEYSTORE_PASSWORD;
import static org.apache.qpid.test.utils.TestSSLConstants.TRUSTSTORE_PASSWORD;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.Key;
import java.security.cert.Certificate;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Session;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.security.FileKeyStore;
import org.apache.qpid.server.security.FileTrustStore;
import org.apache.qpid.systests.AmqpManagementFacade;
import org.apache.qpid.systests.ConnectionBuilder;
import org.apache.qpid.systests.JmsTestBase;
import org.apache.qpid.test.utils.TestSSLConstants;
import org.apache.qpid.tests.utils.BrokerAdmin;

public class TlsTest extends JmsTestBase
{
    public static final String TEST_PROFILE_RESOURCE_BASE = System.getProperty("java.io.tmpdir") + "/";
    public static final String BROKER_KEYSTORE =
            TEST_PROFILE_RESOURCE_BASE + org.apache.qpid.test.utils.TestSSLConstants.BROKER_KEYSTORE;
    public static final String BROKER_TRUSTSTORE =
            TEST_PROFILE_RESOURCE_BASE + org.apache.qpid.test.utils.TestSSLConstants.BROKER_TRUSTSTORE;
    public static final String KEYSTORE =
            TEST_PROFILE_RESOURCE_BASE + org.apache.qpid.test.utils.TestSSLConstants.KEYSTORE;
    public static final String TRUSTSTORE =
            TEST_PROFILE_RESOURCE_BASE + org.apache.qpid.test.utils.TestSSLConstants.TRUSTSTORE;

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
    public void testCreateSSLConnectionUsingConnectionURLParams() throws Exception
    {
        //Start the broker (NEEDing client certificate authentication)
        int port = configureTlsPort(getTestPortName(), true, false, false);

        InetSocketAddress brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP);
        Connection connection = getConnectionBuilder().setSslPort(port)
                                                      .setHost(brokerAddress.getHostName())
                                                      .setTls(true)
                                                      .setKeyStoreLocation(KEYSTORE)
                                                      .setKeyStorePassword(KEYSTORE_PASSWORD)
                                                      .setTrustStoreLocation(TRUSTSTORE)
                                                      .setTrustStorePassword(TRUSTSTORE_PASSWORD)
                                                      .build();
        try
        {
            assertConnection(connection);
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testCreateSSLConnectionWithCertificateTrust() throws Exception
    {
        assumeThat("Qpid JMS Client does not support trusting of a certificate",
                   getProtocol(),
                   is(not(equalTo(Protocol.AMQP_1_0))));

        int port = configureTlsPort(getTestPortName(), false, false, false);
        File trustCertFile = extractCertFileFromTestTrustStore();

        InetSocketAddress brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP);
        Connection connection = getConnectionBuilder().setSslPort(port)
                                                      .setHost(brokerAddress.getHostName())
                                                      .setTls(true)
                                                      .setOptions(Collections.singletonMap("trusted_certs_path",
                                                                                           encodePathOption(trustCertFile.getCanonicalPath())))
                                                      .build();
        try
        {
            assertConnection(connection);
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testSSLConnectionToPlainPortRejected() throws Exception
    {
        assumeThat("QPID-8069", getProtocol(), is(anyOf(equalTo(Protocol.AMQP_1_0), equalTo(Protocol.AMQP_0_10))));

        setSslStoreSystemProperties();
        try
        {
            InetSocketAddress brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP);
            getConnectionBuilder().setSslPort(brokerAddress.getPort())
                                  .setHost(brokerAddress.getHostName())
                                  .setTls(true)
                                  .build();

            fail("Exception not thrown");
        }
        catch (JMSException e)
        {
            // PASS
        }
        finally
        {
            clearSslStoreSystemProperties();
        }
    }

    @Test
    public void testHostVerificationIsOnByDefault() throws Exception
    {
        assumeThat("QPID-8069", getProtocol(), is(anyOf(equalTo(Protocol.AMQP_1_0), equalTo(Protocol.AMQP_0_10))));

        //Start the broker (NEEDing client certificate authentication)
        int port = configureTlsPort(getTestPortName(), true, false, false);

        try
        {
            getConnectionBuilder().setSslPort(port)
                                  .setHost("127.0.0.1")
                                  .setTls(true)
                                  .setKeyStoreLocation(KEYSTORE)
                                  .setKeyStorePassword(KEYSTORE_PASSWORD)
                                  .setTrustStoreLocation(TRUSTSTORE)
                                  .setTrustStorePassword(TRUSTSTORE_PASSWORD)
                                  .build();
            fail("Exception not thrown");
        }
        catch (JMSException e)
        {
            // PASS
        }

        Connection connection = getConnectionBuilder().setSslPort(port)
                                                      .setHost("127.0.0.1")
                                                      .setTls(true)
                                                      .setKeyStoreLocation(KEYSTORE)
                                                      .setKeyStorePassword(KEYSTORE_PASSWORD)
                                                      .setTrustStoreLocation(TRUSTSTORE)
                                                      .setTrustStorePassword(TRUSTSTORE_PASSWORD)
                                                      .setVerifyHostName(false)
                                                      .build();
        try
        {
            assertConnection(connection);
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testCreateSslConnectionUsingJVMSettings() throws Exception
    {
        //Start the broker (NEEDing client certificate authentication)
        int port = configureTlsPort(getTestPortName(), true, false, false);
        setSslStoreSystemProperties();
        try
        {
            Connection connection = getConnectionBuilder().setSslPort(port)
                                                          .setTls(true)
                                                          .build();
            try
            {
                assertConnection(connection);
            }
            finally
            {
                connection.close();
            }
        }
        finally
        {
            clearSslStoreSystemProperties();
        }
    }

    @Test
    public void testMultipleCertsInSingleStore() throws Exception
    {
        //Start the broker (NEEDing client certificate authentication)
        int port = configureTlsPort(getTestPortName(), true, false, false);
        setSslStoreSystemProperties();
        try
        {
            Connection connection = getConnectionBuilder().setClientId(getTestName())
                                                          .setSslPort(port)
                                                          .setTls(true)
                                                          .setKeyAlias(TestSSLConstants.CERT_ALIAS_APP1)
                                                          .build();
            try
            {
                assertConnection(connection);
            }
            finally
            {
                connection.close();
            }

            Connection connection2 = getConnectionBuilder().setSslPort(port)
                                                           .setTls(true)
                                                           .setKeyAlias(TestSSLConstants.CERT_ALIAS_APP2)
                                                           .build();
            try
            {
                assertConnection(connection2);
            }
            finally
            {
                connection2.close();
            }
        }
        finally
        {
            clearSslStoreSystemProperties();
        }
    }

    @Test
    public void testVerifyHostNameWithIncorrectHostname() throws Exception
    {
        assumeThat("QPID-8069", getProtocol(), is(anyOf(equalTo(Protocol.AMQP_1_0), equalTo(Protocol.AMQP_0_10))));

        //Start the broker (WANTing client certificate authentication)
        int port = configureTlsPort(getTestPortName(), false, true, false);

        setSslStoreSystemProperties();
        try
        {
            getConnectionBuilder().setSslPort(port)
                                  .setHost("127.0.0.1")
                                  .setTls(true)
                                  .setVerifyHostName(true)
                                  .build();
            fail("Exception not thrown");
        }
        catch (JMSException e)
        {
            // PASS
        }
        finally
        {
            clearSslStoreSystemProperties();
        }
    }

    @Test
    public void testVerifyLocalHost() throws Exception
    {
        //Start the broker (WANTing client certificate authentication)
        int port = configureTlsPort(getTestPortName(), false, true, false);

        setSslStoreSystemProperties();
        try
        {
            Connection connection = getConnectionBuilder().setSslPort(port)
                                                          .setHost("localhost")
                                                          .setTls(true)
                                                          .build();
            try
            {
                assertConnection(connection);
            }
            finally
            {
                connection.close();
            }
        }
        finally
        {
            clearSslStoreSystemProperties();
        }
    }

    @Test
    public void testCreateSSLConnectionUsingConnectionURLParamsTrustStoreOnly() throws Exception
    {
        //Start the broker (WANTing client certificate authentication)
        int port = configureTlsPort(getTestPortName(), false, true, false);

        InetSocketAddress brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP);
        Connection connection = getConnectionBuilder().setSslPort(port)
                                                      .setHost(brokerAddress.getHostName())
                                                      .setTls(true)
                                                      .setTrustStoreLocation(TRUSTSTORE)
                                                      .setTrustStorePassword(TRUSTSTORE_PASSWORD)
                                                      .build();
        try
        {
            assertConnection(connection);
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testClientCertificateMissingWhilstNeeding() throws Exception
    {
        assumeThat("QPID-8069", getProtocol(), is(anyOf(equalTo(Protocol.AMQP_1_0), equalTo(Protocol.AMQP_0_10))));

        //Start the broker (NEEDing client certificate authentication)
        int port = configureTlsPort(getTestPortName(), true, false, false);

        try
        {
            getConnectionBuilder().setSslPort(port)
                                  .setHost(getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP).getHostName())
                                  .setTls(true)
                                  .setTrustStoreLocation(TRUSTSTORE)
                                  .setTrustStorePassword(TRUSTSTORE_PASSWORD)
                                  .build();
            fail("Connection was established successfully");
        }
        catch (JMSException e)
        {
            // PASS
        }
    }

    @Test
    public void testClientCertificateMissingWhilstWanting() throws Exception
    {
        //Start the broker (WANTing client certificate authentication)
        int port = configureTlsPort(getTestPortName(), false, true, false);

        InetSocketAddress brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP);
        Connection connection = getConnectionBuilder().setSslPort(port)
                                                      .setHost(brokerAddress.getHostName())
                                                      .setTls(true)
                                                      .setTrustStoreLocation(TRUSTSTORE)
                                                      .setTrustStorePassword(TRUSTSTORE_PASSWORD)
                                                      .build();
        try
        {
            assertConnection(connection);
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testClientCertMissingWhilstWantingAndNeeding() throws Exception
    {
        assumeThat("QPID-8069", getProtocol(), is(anyOf(equalTo(Protocol.AMQP_1_0), equalTo(Protocol.AMQP_0_10))));
        //Start the broker (NEEDing and WANTing client certificate authentication)
        int port = configureTlsPort(getTestPortName(), true, true, false);

        try
        {
            getConnectionBuilder().setSslPort(port)
                                  .setHost(getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP).getHostName())
                                  .setTls(true)
                                  .setTrustStoreLocation(TRUSTSTORE)
                                  .setTrustStorePassword(TRUSTSTORE_PASSWORD)
                                  .build();
            fail("Connection was established successfully");
        }
        catch (JMSException e)
        {
            // PASS
        }
    }

    @Test
    public void testCreateSSLandTCPonSamePort() throws Exception
    {

        //Start the broker (WANTing client certificate authentication)
        int port = configureTlsPort(getTestPortName(), false, true, true);

        InetSocketAddress brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP);
        Connection connection = getConnectionBuilder().setSslPort(port)
                                                      .setHost(brokerAddress.getHostName())
                                                      .setTls(true)
                                                      .setKeyStoreLocation(KEYSTORE)
                                                      .setKeyStorePassword(KEYSTORE_PASSWORD)
                                                      .setTrustStoreLocation(TRUSTSTORE)
                                                      .setTrustStorePassword(TRUSTSTORE_PASSWORD)
                                                      .build();
        try
        {
            assertConnection(connection);
        }
        finally
        {
            connection.close();
        }

        Connection connection2 = getConnectionBuilder().setPort(port)
                                                       .setHost(brokerAddress.getHostName())
                                                       .build();
        try
        {
            assertConnection(connection2);
        }
        finally
        {
            connection2.close();
        }
    }

    @Test
    public void testCreateSSLWithCertFileAndPrivateKey() throws Exception
    {
        assumeThat("Qpid JMS Client does not support trusting of a certificate",
                   getProtocol(),
                   is(not(equalTo(Protocol.AMQP_1_0))));

        assumeThat("QPID-8255: certificate can only be loaded using jks keystore ",
                   java.security.KeyStore.getDefaultType(),
                   is(equalTo("jks")));

        //Start the broker (NEEDing client certificate authentication)
        int port = configureTlsPort(getTestPortName(), true, false, false);

        clearSslStoreSystemProperties();
        File[] certAndKeyFiles = extractResourcesFromTestKeyStore();
        final Map<String, String> options = new HashMap<>();
        options.put("client_cert_path", encodePathOption(certAndKeyFiles[1].getCanonicalPath()));
        options.put("client_cert_priv_key_path", encodePathOption(certAndKeyFiles[0].getCanonicalPath()));
        InetSocketAddress brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP);
        Connection connection = getConnectionBuilder().setSslPort(port)
                                                      .setHost(brokerAddress.getHostName())
                                                      .setTls(true)
                                                      .setTrustStoreLocation(TRUSTSTORE)
                                                      .setTrustStorePassword(TRUSTSTORE_PASSWORD)
                                                      .setVerifyHostName(false)
                                                      .setOptions(options)
                                                      .build();
        try
        {
            assertConnection(connection);
        }
        finally
        {
            connection.close();
        }
    }


    private int configureTlsPort(final String portName,
                                 final boolean needClientAuth,
                                 final boolean wantClientAuth,
                                 final boolean samePort) throws Exception
    {

        return createTlsPort(portName,
                             needClientAuth,
                             wantClientAuth,
                             samePort,
                             getConnectionBuilder(),
                             new AmqpManagementFacade(getProtocol()),
                             getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP).getPort());
    }

    public static int createTlsPort(final String portName,
                                    final boolean needClientAuth,
                                    final boolean wantClientAuth,
                                    final boolean plainAndSsl,
                                    final ConnectionBuilder connectionBuilder,
                                    final AmqpManagementFacade managementFacade,
                                    final int brokerPort) throws Exception
    {
        Connection connection = connectionBuilder.setVirtualHost("$management").build();
        try
        {
            connection.start();
            String keyStoreName = portName + "KeyStore";
            String trustStoreName = portName + "TrustStore";
            String authenticationProvider = null;

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            try
            {
                List<Map<String, Object>> ports =
                        managementFacade.managementQueryObjects(session, "org.apache.qpid.AmqpPort");
                for (Map<String, Object> port : ports)
                {
                    String name = String.valueOf(port.get(Port.NAME));

                    Session s = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    try
                    {
                        Map<String, Object> attributes = managementFacade.readEntityUsingAmqpManagement(s,
                                                                                                        "org.apache.qpid.AmqpPort",
                                                                                                        name,
                                                                                                        false);
                        if (attributes.get("boundPort").equals(brokerPort))
                        {
                            authenticationProvider = String.valueOf(attributes.get(Port.AUTHENTICATION_PROVIDER));
                            break;
                        }
                    }
                    finally
                    {
                        s.close();
                    }
                }
            }
            finally
            {
                session.close();
            }

            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            try
            {
                final Map<String, Object> keyStoreAttributes = new HashMap<>();
                keyStoreAttributes.put("storeUrl", BROKER_KEYSTORE);
                keyStoreAttributes.put("password", BROKER_KEYSTORE_PASSWORD);
                managementFacade.createEntityAndAssertResponse(keyStoreName,
                                                               FileKeyStore.class.getName(),
                                                               keyStoreAttributes,
                                                               session);
            }
            finally
            {
                session.close();
            }

            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            try
            {
                final Map<String, Object> trustStoreAttributes = new HashMap<>();
                trustStoreAttributes.put("storeUrl", BROKER_TRUSTSTORE);
                trustStoreAttributes.put("password", BROKER_TRUSTSTORE_PASSWORD);
                managementFacade.createEntityAndAssertResponse(trustStoreName,
                                                               FileTrustStore.class.getName(),
                                                               trustStoreAttributes,
                                                               session);
            }
            finally
            {
                session.close();
            }

            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            try
            {
                Map<String, Object> sslPortAttributes = new HashMap<>();
                sslPortAttributes.put(Port.TRANSPORTS, plainAndSsl ? "[\"SSL\",\"TCP\"]" : "[\"SSL\"]");
                sslPortAttributes.put(Port.PORT, 0);
                sslPortAttributes.put(Port.AUTHENTICATION_PROVIDER, authenticationProvider);
                sslPortAttributes.put(Port.NEED_CLIENT_AUTH, needClientAuth);
                sslPortAttributes.put(Port.WANT_CLIENT_AUTH, wantClientAuth);
                sslPortAttributes.put(Port.NAME, portName);
                sslPortAttributes.put(Port.KEY_STORE, keyStoreName);
                sslPortAttributes.put(Port.TRUST_STORES, "[\"" + trustStoreName + "\"]");

                managementFacade.createEntityAndAssertResponse(portName,
                                                               "org.apache.qpid.AmqpPort",
                                                               sslPortAttributes,
                                                               session);
            }
            finally
            {
                session.close();
            }

            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            try
            {
                Map<String, Object> portEffectiveAttributes =
                        managementFacade.readEntityUsingAmqpManagement(session,
                                                                       "org.apache.qpid.AmqpPort",
                                                                       portName,
                                                                       false);
                if (portEffectiveAttributes.containsKey("boundPort"))
                {
                    return (int) portEffectiveAttributes.get("boundPort");
                }
                throw new RuntimeException("Bound port is not found");
            }
            finally
            {
                session.close();
            }
        }
        finally
        {
            connection.close();
        }
    }

    private void setSslStoreSystemProperties()
    {
        System.setProperty("javax.net.ssl.keyStore", KEYSTORE);
        System.setProperty("javax.net.ssl.keyStorePassword", KEYSTORE_PASSWORD);
        System.setProperty("javax.net.ssl.trustStore", TRUSTSTORE);
        System.setProperty("javax.net.ssl.trustStorePassword", TRUSTSTORE_PASSWORD);
    }

    private void clearSslStoreSystemProperties()
    {
        System.clearProperty("javax.net.ssl.keyStore");
        System.clearProperty("javax.net.ssl.keyStorePassword");
        System.clearProperty("javax.net.ssl.trustStore");
        System.clearProperty("javax.net.ssl.trustStorePassword");
    }

    private File[] extractResourcesFromTestKeyStore() throws Exception
    {
        java.security.KeyStore ks = java.security.KeyStore.getInstance(java.security.KeyStore.getDefaultType());
        try (InputStream is = new FileInputStream(KEYSTORE))
        {
            ks.load(is, KEYSTORE_PASSWORD.toCharArray());
        }

        File privateKeyFile = Files.createTempFile(getTestName(), ".private-key.der").toFile();
        try (FileOutputStream kos = new FileOutputStream(privateKeyFile))
        {
            Key pvt = ks.getKey(TestSSLConstants.CERT_ALIAS_APP1, KEYSTORE_PASSWORD.toCharArray());
            kos.write("-----BEGIN PRIVATE KEY-----\n".getBytes());
            String base64encoded = Base64.getEncoder().encodeToString(pvt.getEncoded());
            while (base64encoded.length() > 76)
            {
                kos.write(base64encoded.substring(0, 76).getBytes());
                kos.write("\n".getBytes());
                base64encoded = base64encoded.substring(76);
            }

            kos.write(base64encoded.getBytes());
            kos.write("\n-----END PRIVATE KEY-----".getBytes());
            kos.flush();
        }

        File certificateFile = Files.createTempFile(getTestName(), ".certificate.der").toFile();
        try (FileOutputStream cos = new FileOutputStream(certificateFile))
        {
            Certificate[] chain = ks.getCertificateChain(TestSSLConstants.CERT_ALIAS_APP1);
            for (Certificate pub : chain)
            {
                cos.write("-----BEGIN CERTIFICATE-----\n".getBytes());
                String base64encoded = Base64.getEncoder().encodeToString(pub.getEncoded());
                while (base64encoded.length() > 76)
                {
                    cos.write(base64encoded.substring(0, 76).getBytes());
                    cos.write("\n".getBytes());
                    base64encoded = base64encoded.substring(76);
                }
                cos.write(base64encoded.getBytes());

                cos.write("\n-----END CERTIFICATE-----\n".getBytes());
            }
            cos.flush();
        }

        return new File[]{privateKeyFile, certificateFile};
    }

    private File extractCertFileFromTestTrustStore() throws Exception
    {
        java.security.KeyStore ks = java.security.KeyStore.getInstance(java.security.KeyStore.getDefaultType());
        try (InputStream is = new FileInputStream(TRUSTSTORE))
        {
            ks.load(is, TRUSTSTORE_PASSWORD.toCharArray());
        }

        File certificateFile = Files.createTempFile(getTestName(), ".crt").toFile();

        try (FileOutputStream cos = new FileOutputStream(certificateFile))
        {

            for (String alias : Collections.list(ks.aliases()))
            {
                Certificate pub = ks.getCertificate(alias);
                cos.write("-----BEGIN CERTIFICATE-----\n".getBytes());
                String base64encoded = Base64.getEncoder().encodeToString(pub.getEncoded());
                while (base64encoded.length() > 76)
                {
                    cos.write(base64encoded.substring(0, 76).getBytes());
                    cos.write("\n".getBytes());
                    base64encoded = base64encoded.substring(76);
                }
                cos.write(base64encoded.getBytes());

                cos.write("\n-----END CERTIFICATE-----\n".getBytes());
            }
            cos.flush();
        }

        return certificateFile;
    }

    private String getTestPortName()
    {
        return getTestName() + "TlsPort";
    }

    private void assertConnection(final Connection connection) throws JMSException
    {
        assertNotNull("connection should be successful", connection);
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull("create session should be successful", session);
    }

    private String encodePathOption(final String canonicalPath)
    {
        try
        {
            return URLEncoder.encode(canonicalPath, StandardCharsets.UTF_8.name()).replace("+", "%20");
        }
        catch (UnsupportedEncodingException e)
        {
            throw new RuntimeException(e);
        }
    }
}
