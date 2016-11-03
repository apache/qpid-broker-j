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
package org.apache.qpid.systest.rest.acl;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;

import org.apache.qpid.server.logging.logback.BrokerFileLogger;
import org.apache.qpid.server.logging.logback.BrokerMemoryLogger;
import org.apache.qpid.server.logging.logback.BrokerNameAndLevelLogInclusionRule;
import org.apache.qpid.server.management.plugin.HttpManagement;
import org.apache.qpid.server.management.plugin.servlet.rest.AbstractServlet;
import org.apache.qpid.server.model.AccessControlProvider;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerLogger;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ExternalFileBasedAuthenticationManager;
import org.apache.qpid.server.model.GroupProvider;
import org.apache.qpid.server.model.KeyStore;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.server.model.adapter.FileBasedGroupProvider;
import org.apache.qpid.server.model.adapter.FileBasedGroupProviderImpl;
import org.apache.qpid.server.security.AllowAllAccessControlProvider;
import org.apache.qpid.server.security.FileKeyStore;
import org.apache.qpid.server.security.FileTrustStore;
import org.apache.qpid.server.security.access.plugins.AclFileAccessControlProvider;
import org.apache.qpid.server.security.acl.AbstractACLTestCase;
import org.apache.qpid.server.security.auth.manager.AnonymousAuthenticationManager;
import org.apache.qpid.server.security.auth.manager.PlainPasswordDatabaseAuthenticationManager;
import org.apache.qpid.systest.rest.QpidRestTestCase;
import org.apache.qpid.test.utils.TestBrokerConfiguration;
import org.apache.qpid.test.utils.TestFileUtils;
import org.apache.qpid.test.utils.TestSSLConstants;

public class BrokerACLTest extends QpidRestTestCase
{
    private static final String ALLOWED_USER = "user1";
    private static final String DENIED_USER = "user2";
    private String _secondaryAclFileContent = "";

    @Override
    protected void customizeConfiguration() throws Exception
    {
        super.customizeConfiguration();
        final TestBrokerConfiguration defaultBrokerConfiguration = getDefaultBrokerConfiguration();
        defaultBrokerConfiguration.configureTemporaryPasswordFile(ALLOWED_USER, DENIED_USER);

        AbstractACLTestCase.writeACLFileUtil(this, "ACL ALLOW-LOG ALL ACCESS MANAGEMENT",
                "ACL ALLOW-LOG " + ALLOWED_USER + " CONFIGURE BROKER",
                "ACL DENY-LOG " + DENIED_USER + " CONFIGURE BROKER",
                "ACL ALLOW-LOG " + ALLOWED_USER + " ACCESS_LOGS BROKER",
                "ACL DENY-LOG " + DENIED_USER + " ACCESS_LOGS BROKER",
                "ACL DENY-LOG ALL ALL");

                _secondaryAclFileContent =
                "ACL ALLOW-LOG ALL ACCESS MANAGEMENT\n" +
                "ACL ALLOW-LOG " + ALLOWED_USER + " CONFIGURE BROKER\n" +
                "ACL DENY-LOG " + DENIED_USER + " CONFIGURE BROKER\n" +
                "ACL DENY-LOG ALL ALL";
    }

    /* === AuthenticationProvider === */

    public void testCreateAuthenticationProviderAllowed() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        String authenticationProviderName = getTestName();

        int responseCode = createAuthenticationProvider(authenticationProviderName);
        assertEquals("Provider creation should be allowed", 201, responseCode);

        assertAuthenticationProviderExists(authenticationProviderName);
    }

    public void testCreateAuthenticationProviderDenied() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(DENIED_USER, DENIED_USER);

        String authenticationProviderName = getTestName();

        int responseCode = createAuthenticationProvider(authenticationProviderName);
        assertEquals("Provider creation should be denied", 403, responseCode);

        assertAuthenticationProviderDoesNotExist(authenticationProviderName);
    }

    public void testDeleteAuthenticationProviderAllowed() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        String providerName = getTestName();

        int responseCode = createAuthenticationProvider(providerName);
        assertEquals("Provider creation should be allowed", 201, responseCode);

        assertAuthenticationProviderExists(providerName);

        responseCode = getRestTestHelper().submitRequest("authenticationprovider/" + providerName, "DELETE");
        assertEquals("Provider deletion should be allowed", 200, responseCode);

        assertAuthenticationProviderDoesNotExist(TEST2_VIRTUALHOST);
    }

    public void testDeleteAuthenticationProviderDenied() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        String providerName = getTestName();

        int responseCode = createAuthenticationProvider(providerName);
        assertEquals("Provider creation should be allowed", 201, responseCode);

        assertAuthenticationProviderExists(providerName);

        getRestTestHelper().setUsernameAndPassword(DENIED_USER, DENIED_USER);
        responseCode = getRestTestHelper().submitRequest("authenticationprovider/" + providerName, "DELETE");
        assertEquals("Provider deletion should be denied", 403, responseCode);

        assertAuthenticationProviderExists(providerName);
    }

    public void testSetAuthenticationProviderAttributesAllowed() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        String providerName = TestBrokerConfiguration.ENTRY_NAME_AUTHENTICATION_PROVIDER;

        assertAuthenticationProviderExists(providerName);

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(AuthenticationProvider.NAME, providerName);
        attributes.put(AuthenticationProvider.TYPE, PlainPasswordDatabaseAuthenticationManager.PROVIDER_TYPE);
        attributes.put(AuthenticationProvider.STATE, State.DELETED.name());

        int responseCode = getRestTestHelper().submitRequest("authenticationprovider/" + providerName, "PUT", attributes);
        assertEquals("Setting of provider attribites should be allowed", 200, responseCode);
    }

    public void testSetAuthenticationProviderAttributesDenied() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        String providerName = TestBrokerConfiguration.ENTRY_NAME_AUTHENTICATION_PROVIDER;

        Map<String, Object> providerData = getRestTestHelper().getJsonAsSingletonList("authenticationprovider/" + providerName);

        File file = TestFileUtils.createTempFile(this, ".users", "guest:guest\n" + ALLOWED_USER + ":" + ALLOWED_USER + "\n"
                + DENIED_USER + ":" + DENIED_USER);

        getRestTestHelper().setUsernameAndPassword(DENIED_USER, DENIED_USER);

        Map<String, Object> attributes = new HashMap<String, Object>();
        attributes.put(AuthenticationProvider.NAME, providerName);
        attributes.put(AuthenticationProvider.TYPE, AnonymousAuthenticationManager.PROVIDER_TYPE);
        attributes.put(ExternalFileBasedAuthenticationManager.PATH, file.getAbsolutePath());

        int responseCode = getRestTestHelper().submitRequest("authenticationprovider/" + providerName, "PUT", attributes);
        assertEquals("Setting of provider attribites should be allowed", 403, responseCode);

        Map<String, Object> provider = getRestTestHelper().getJsonAsSingletonList("authenticationprovider/" + providerName);
        assertEquals("Unexpected STORE_URL attribute value",
                providerData.get(ExternalFileBasedAuthenticationManager.PATH),
                provider.get(ExternalFileBasedAuthenticationManager.PATH));
    }

    /* === Port === */

    public void testCreatePortAllowed() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        String portName = getTestName();

        int responseCode = createPort(portName);
        assertEquals("Port creation should be allowed", 201, responseCode);

        assertPortExists(portName);
    }

    public void testCreatePortDenied() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(DENIED_USER, DENIED_USER);

        String portName = getTestName();

        int responseCode = createPort(portName);
        assertEquals("Port creation should be denied", 403, responseCode);

        assertPortDoesNotExist(portName);
    }

    public void testDeletePortDenied() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        String portName = TestBrokerConfiguration.ENTRY_NAME_AMQP_PORT;
        assertPortExists(portName);

        getRestTestHelper().setUsernameAndPassword(DENIED_USER, DENIED_USER);

        int responseCode = getRestTestHelper().submitRequest("port/" + portName, "DELETE");
        assertEquals("Port deletion should be denied", 403, responseCode);

        assertPortExists(portName);
    }

    public void testDeletePortAllowed() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        String portName = TestBrokerConfiguration.ENTRY_NAME_AMQP_PORT;
        assertPortExists(portName);

        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        int responseCode = getRestTestHelper().submitRequest("port/" + portName, "DELETE");
        assertEquals("Port deletion should be allowed", 200, responseCode);

        assertPortDoesNotExist(portName);
    }


    public void testSetPortAttributesDenied() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        String portName = getTestName();

        int responseCode = createPort(portName);
        assertEquals("Port creation should be allowed", 201, responseCode);

        assertPortExists(portName);

        getRestTestHelper().setUsernameAndPassword(DENIED_USER, DENIED_USER);

        Map<String, Object> attributes = new HashMap<String, Object>();
        attributes.put(Port.NAME, portName);
        attributes.put(Port.PROTOCOLS, Arrays.asList(Protocol.AMQP_0_9));
        attributes.put(Port.AUTHENTICATION_PROVIDER, TestBrokerConfiguration.ENTRY_NAME_AUTHENTICATION_PROVIDER);
        responseCode = getRestTestHelper().submitRequest("port/" + portName, "PUT", attributes);
        assertEquals("Setting of port attribites should be denied", 403, responseCode);

        Map<String, Object> port = getRestTestHelper().getJsonAsSingletonList("port/" + portName);
        assertEquals("Unexpected authentication provider attribute value",
                TestBrokerConfiguration.ENTRY_NAME_AUTHENTICATION_PROVIDER, port.get(Port.AUTHENTICATION_PROVIDER));
    }

    /* === KeyStore === */

    public void testCreateKeyStoreAllowed() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        String keyStoreName = getTestName();

        assertKeyStoreExistence(keyStoreName, false);

        int responseCode = createKeyStore(keyStoreName, TestSSLConstants.CERT_ALIAS_APP1);
        assertEquals("keyStore creation should be allowed", 201, responseCode);

        assertKeyStoreExistence(keyStoreName, true);
    }

    public void testCreateKeyStoreDenied() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(DENIED_USER, DENIED_USER);

        String keyStoreName = getTestName();

        assertKeyStoreExistence(keyStoreName, false);

        int responseCode = createKeyStore(keyStoreName, TestSSLConstants.CERT_ALIAS_APP1);
        assertEquals("keyStore creation should be allowed", 403, responseCode);

        assertKeyStoreExistence(keyStoreName, false);
    }

    public void testDeleteKeyStoreDenied() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        String keyStoreName = getTestName();

        assertKeyStoreExistence(keyStoreName, false);

        int responseCode = createKeyStore(keyStoreName, TestSSLConstants.CERT_ALIAS_APP1);
        assertEquals("keyStore creation should be allowed", 201, responseCode);

        assertKeyStoreExistence(keyStoreName, true);

        getRestTestHelper().setUsernameAndPassword(DENIED_USER, DENIED_USER);

        responseCode = getRestTestHelper().submitRequest("keystore/" + keyStoreName, "DELETE");
        assertEquals("keystore deletion should be denied", 403, responseCode);

        assertKeyStoreExistence(keyStoreName, true);
    }

    public void testDeleteKeyStoreAllowed() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        String keyStoreName = getTestName();

        assertKeyStoreExistence(keyStoreName, false);

        int responseCode = createKeyStore(keyStoreName, TestSSLConstants.CERT_ALIAS_APP1);
        assertEquals("keyStore creation should be allowed", 201, responseCode);

        assertKeyStoreExistence(keyStoreName, true);

        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        responseCode = getRestTestHelper().submitRequest("keystore/" + keyStoreName, "DELETE");
        assertEquals("keystore deletion should be allowed", 200, responseCode);

        assertKeyStoreExistence(keyStoreName, false);
    }

    public void testSetKeyStoreAttributesAllowed() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        String keyStoreName = getTestName();
        String initialCertAlias = TestSSLConstants.CERT_ALIAS_APP1;
        String updatedCertAlias = TestSSLConstants.CERT_ALIAS_APP2;

        assertKeyStoreExistence(keyStoreName, false);

        int responseCode = createKeyStore(keyStoreName, initialCertAlias);
        assertEquals("keyStore creation should be allowed", 201, responseCode);

        assertKeyStoreExistence(keyStoreName, true);
        Map<String, Object> keyStore = getRestTestHelper().getJsonAsSingletonList("keystore/" + keyStoreName);
        assertEquals("Unexpected certificateAlias attribute value", initialCertAlias, keyStore.get(FileKeyStore.CERTIFICATE_ALIAS));

        Map<String, Object> attributes = new HashMap<String, Object>();
        attributes.put(KeyStore.NAME, keyStoreName);
        attributes.put(FileKeyStore.CERTIFICATE_ALIAS, updatedCertAlias);
        responseCode = getRestTestHelper().submitRequest("keystore/" + keyStoreName, "PUT", attributes);
        assertEquals("Setting of keystore attributes should be allowed", 200, responseCode);

        keyStore = getRestTestHelper().getJsonAsSingletonList("keystore/" + keyStoreName);
        assertEquals("Unexpected certificateAlias attribute value", updatedCertAlias, keyStore.get(FileKeyStore.CERTIFICATE_ALIAS));
    }

    public void testSetKeyStoreAttributesDenied() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        String keyStoreName = getTestName();
        String initialCertAlias = TestSSLConstants.CERT_ALIAS_APP1;
        String updatedCertAlias = TestSSLConstants.CERT_ALIAS_APP2;

        assertKeyStoreExistence(keyStoreName, false);

        int responseCode = createKeyStore(keyStoreName, initialCertAlias);
        assertEquals("keyStore creation should be allowed", 201, responseCode);

        assertKeyStoreExistence(keyStoreName, true);
        Map<String, Object> keyStore = getRestTestHelper().getJsonAsSingletonList("keystore/" + keyStoreName);
        assertEquals("Unexpected certificateAlias attribute value", initialCertAlias, keyStore.get(FileKeyStore.CERTIFICATE_ALIAS));

        getRestTestHelper().setUsernameAndPassword(DENIED_USER, DENIED_USER);

        Map<String, Object> attributes = new HashMap<String, Object>();
        attributes.put(KeyStore.NAME, keyStoreName);
        attributes.put(FileKeyStore.CERTIFICATE_ALIAS, updatedCertAlias);
        responseCode = getRestTestHelper().submitRequest("keystore/" + keyStoreName, "PUT", attributes);
        assertEquals("Setting of keystore attributes should be denied", 403, responseCode);

        keyStore = getRestTestHelper().getJsonAsSingletonList("keystore/" + keyStoreName);
        assertEquals("Unexpected certificateAlias attribute value", initialCertAlias, keyStore.get(FileKeyStore.CERTIFICATE_ALIAS));
    }

    /* === TrustStore === */

    public void testCreateTrustStoreAllowed() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        String trustStoreName = getTestName();

        assertTrustStoreExistence(trustStoreName, false);

        int responseCode = createTrustStore(trustStoreName, false);
        assertEquals("trustStore creation should be allowed", 201, responseCode);

        assertTrustStoreExistence(trustStoreName, true);
    }

    public void testCreateTrustStoreDenied() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(DENIED_USER, DENIED_USER);

        String trustStoreName = getTestName();

        assertTrustStoreExistence(trustStoreName, false);

        int responseCode = createTrustStore(trustStoreName, false);
        assertEquals("trustStore creation should be allowed", 403, responseCode);

        assertTrustStoreExistence(trustStoreName, false);
    }

    public void testDeleteTrustStoreDenied() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        String trustStoreName = getTestName();

        assertTrustStoreExistence(trustStoreName, false);

        int responseCode = createTrustStore(trustStoreName, false);
        assertEquals("trustStore creation should be allowed", 201, responseCode);

        assertTrustStoreExistence(trustStoreName, true);

        getRestTestHelper().setUsernameAndPassword(DENIED_USER, DENIED_USER);

        responseCode = getRestTestHelper().submitRequest("truststore/" + trustStoreName, "DELETE");
        assertEquals("truststore deletion should be denied", 403, responseCode);

        assertTrustStoreExistence(trustStoreName, true);
    }

    public void testDeleteTrustStoreAllowed() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        String trustStoreName = getTestName();

        assertTrustStoreExistence(trustStoreName, false);

        int responseCode = createTrustStore(trustStoreName, false);
        assertEquals("trustStore creation should be allowed", 201, responseCode);

        assertTrustStoreExistence(trustStoreName, true);

        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        responseCode = getRestTestHelper().submitRequest("truststore/" + trustStoreName, "DELETE");
        assertEquals("truststore deletion should be allowed", 200, responseCode);

        assertTrustStoreExistence(trustStoreName, false);
    }

    public void testSetTrustStoreAttributesAllowed() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        String trustStoreName = getTestName();
        boolean initialPeersOnly = false;
        boolean updatedPeersOnly = true;

        assertTrustStoreExistence(trustStoreName, false);

        int responseCode = createTrustStore(trustStoreName, initialPeersOnly);
        assertEquals("trustStore creation should be allowed", 201, responseCode);

        assertTrustStoreExistence(trustStoreName, true);
        Map<String, Object> trustStore = getRestTestHelper().getJsonAsSingletonList("truststore/" + trustStoreName);
        assertEquals("Unexpected peersOnly attribute value", initialPeersOnly, trustStore.get(FileTrustStore.PEERS_ONLY));

        Map<String, Object> attributes = new HashMap<String, Object>();
        attributes.put(TrustStore.NAME, trustStoreName);
        attributes.put(FileTrustStore.PEERS_ONLY, updatedPeersOnly);
        responseCode = getRestTestHelper().submitRequest("truststore/" + trustStoreName, "PUT", attributes);
        assertEquals("Setting of truststore attributes should be allowed", 200, responseCode);

        trustStore = getRestTestHelper().getJsonAsSingletonList("truststore/" + trustStoreName);
        assertEquals("Unexpected peersOnly attribute value", updatedPeersOnly, trustStore.get(FileTrustStore.PEERS_ONLY));
    }

    public void testSetTrustStoreAttributesDenied() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        String trustStoreName = getTestName();
        boolean initialPeersOnly = false;
        boolean updatedPeersOnly = true;

        assertTrustStoreExistence(trustStoreName, false);

        int responseCode = createTrustStore(trustStoreName, initialPeersOnly);
        assertEquals("trustStore creation should be allowed", 201, responseCode);

        assertTrustStoreExistence(trustStoreName, true);
        Map<String, Object> trustStore = getRestTestHelper().getJsonAsSingletonList("truststore/" + trustStoreName);
        assertEquals("Unexpected peersOnly attribute value", initialPeersOnly, trustStore.get(FileTrustStore.PEERS_ONLY));

        getRestTestHelper().setUsernameAndPassword(DENIED_USER, DENIED_USER);

        Map<String, Object> attributes = new HashMap<String, Object>();
        attributes.put(TrustStore.NAME, trustStoreName);
        attributes.put(FileTrustStore.PEERS_ONLY, updatedPeersOnly);
        responseCode = getRestTestHelper().submitRequest("truststore/" + trustStoreName, "PUT", attributes);
        assertEquals("Setting of truststore attributes should be denied", 403, responseCode);

        trustStore = getRestTestHelper().getJsonAsSingletonList("truststore/" + trustStoreName);
        assertEquals("Unexpected peersOnly attribute value", initialPeersOnly, trustStore.get(FileTrustStore.PEERS_ONLY));
    }

    /* === Broker === */

    public void testSetBrokerAttributesAllowed() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        int initialSessionCountLimit = 256;
        int updatedSessionCountLimit = 299;

        Map<String, Object> brokerAttributes = getRestTestHelper().getJsonAsSingletonList("broker");
        assertEquals("Unexpected alert repeat gap", initialSessionCountLimit,
                brokerAttributes.get(Broker.CONNECTION_SESSION_COUNT_LIMIT));

        Map<String, Object> newAttributes = new HashMap<String, Object>();
        newAttributes.put(Broker.CONNECTION_SESSION_COUNT_LIMIT, updatedSessionCountLimit);

        int responseCode = getRestTestHelper().submitRequest("broker", "PUT", newAttributes);
        assertEquals("Setting of port attribites should be allowed", 200, responseCode);

        brokerAttributes = getRestTestHelper().getJsonAsSingletonList("broker");
        assertEquals("Unexpected default alert repeat gap", updatedSessionCountLimit,
                brokerAttributes.get(Broker.CONNECTION_SESSION_COUNT_LIMIT));
    }

    public void testSetBrokerAttributesDenied() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        int initialSessionCountLimit = 256;
        int updatedSessionCountLimit = 299;

        Map<String, Object> brokerAttributes = getRestTestHelper().getJsonAsSingletonList("broker");
        assertEquals("Unexpected alert repeat gap", initialSessionCountLimit,
                brokerAttributes.get(Broker.CONNECTION_SESSION_COUNT_LIMIT));

        getRestTestHelper().setUsernameAndPassword(DENIED_USER, DENIED_USER);
        Map<String, Object> newAttributes = new HashMap<String, Object>();
        newAttributes.put(Broker.CONNECTION_SESSION_COUNT_LIMIT, updatedSessionCountLimit);

        int responseCode = getRestTestHelper().submitRequest("broker", "PUT", newAttributes);
        assertEquals("Setting of port attribites should be allowed", 403, responseCode);

        brokerAttributes = getRestTestHelper().getJsonAsSingletonList("broker");
        assertEquals("Unexpected default alert repeat gap", initialSessionCountLimit,
                brokerAttributes.get(Broker.CONNECTION_SESSION_COUNT_LIMIT));
    }

    /* === GroupProvider === */

    public void testCreateGroupProviderAllowed() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        String groupProviderName = getTestName();

        assertGroupProviderExistence(groupProviderName, false);

        int responseCode = createGroupProvider(groupProviderName);
        assertEquals("Group provider creation should be allowed", 201, responseCode);

        assertGroupProviderExistence(groupProviderName, true);
    }

    public void testCreateGroupProviderDenied() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(DENIED_USER, DENIED_USER);

        String groupProviderName = getTestName();

        assertGroupProviderExistence(groupProviderName, false);

        int responseCode = createGroupProvider(groupProviderName);
        assertEquals("Group provider creation should be denied", 403, responseCode);

        assertGroupProviderExistence(groupProviderName, false);
    }

    public void testDeleteGroupProviderDenied() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        String groupProviderName = getTestName();

        assertGroupProviderExistence(groupProviderName, false);

        int responseCode = createGroupProvider(groupProviderName);
        assertEquals("Group provider creation should be allowed", 201, responseCode);

        assertGroupProviderExistence(groupProviderName, true);

        getRestTestHelper().setUsernameAndPassword(DENIED_USER, DENIED_USER);

        responseCode = getRestTestHelper().submitRequest("groupprovider/" + groupProviderName, "DELETE");
        assertEquals("Group provider deletion should be denied", 403, responseCode);

        assertGroupProviderExistence(groupProviderName, true);
    }

    public void testDeleteGroupProviderAllowed() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        String groupProviderName = getTestName();

        assertGroupProviderExistence(groupProviderName, false);

        int responseCode = createGroupProvider(groupProviderName);
        assertEquals("Group provider creation should be allowed", 201, responseCode);

        assertGroupProviderExistence(groupProviderName, true);

        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        responseCode = getRestTestHelper().submitRequest("groupprovider/" + groupProviderName, "DELETE");
        assertEquals("Group provider deletion should be allowed", 200, responseCode);

        assertGroupProviderExistence(groupProviderName, false);
    }

    public void testSetGroupProviderAttributesAllowed() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        String groupProviderName = getTestName();

        assertGroupProviderExistence(groupProviderName, false);

        int responseCode = createGroupProvider(groupProviderName);
        assertEquals("Group provider creation should be allowed", 201, responseCode);

        assertGroupProviderExistence(groupProviderName, true);

        Map<String, Object> attributes = new HashMap<String, Object>();
        attributes.put(GroupProvider.NAME, groupProviderName);
        attributes.put(GroupProvider.TYPE, FileBasedGroupProviderImpl.GROUP_FILE_PROVIDER_TYPE);
        attributes.put(FileBasedGroupProvider.PATH, "/path/to/file");
        responseCode = getRestTestHelper().submitRequest("groupprovider/" + groupProviderName, "PUT", attributes);
        assertEquals("Setting of group provider attributes should be allowed but not supported", AbstractServlet.SC_UNPROCESSABLE_ENTITY, responseCode);
    }

    public void testSetGroupProviderAttributesDenied() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        String groupProviderName = getTestName();

        assertGroupProviderExistence(groupProviderName, false);

        int responseCode = createGroupProvider(groupProviderName);
        assertEquals("Group provider creation should be allowed", 201, responseCode);

        assertGroupProviderExistence(groupProviderName, true);

        getRestTestHelper().setUsernameAndPassword(DENIED_USER, DENIED_USER);

        Map<String, Object> attributes = new HashMap<String, Object>();
        attributes.put(GroupProvider.NAME, groupProviderName);
        attributes.put(GroupProvider.TYPE, FileBasedGroupProviderImpl.GROUP_FILE_PROVIDER_TYPE);
        attributes.put(FileBasedGroupProvider.PATH, "/path/to/file");
        responseCode = getRestTestHelper().submitRequest("groupprovider/" + groupProviderName, "PUT", attributes);
        assertEquals("Setting of group provider attributes should be denied", 403, responseCode);
    }

    /* === AccessControlProvider === */


    public void testCreateAccessControlProviderDenied() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(DENIED_USER, DENIED_USER);

        String accessControlProviderName = getTestName();

        assertAccessControlProviderExistence(accessControlProviderName, false);

        int responseCode = createAccessControlProvider(accessControlProviderName);
        assertEquals("Access control provider creation should be denied", 403, responseCode);

        assertAccessControlProviderExistence(accessControlProviderName, false);
    }

    public void testDeleteAccessControlProviderDenied() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        String accessControlProviderName = TestBrokerConfiguration.ENTRY_NAME_ACL_FILE;

        assertAccessControlProviderExistence(accessControlProviderName, true);

        getRestTestHelper().setUsernameAndPassword(DENIED_USER, DENIED_USER);

        int responseCode = getRestTestHelper().submitRequest("accesscontrolprovider/" + accessControlProviderName, "DELETE");
        assertEquals("Access control provider deletion should be denied", 403, responseCode);

        assertAccessControlProviderExistence(accessControlProviderName, true);
    }

    public void testDeleteAccessControlProviderAllowed() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        String accessControlProviderName = TestBrokerConfiguration.ENTRY_NAME_ACL_FILE;


        assertAccessControlProviderExistence(accessControlProviderName, true);
        // add a second, low priority AllowAll provider
        Map<String,Object> attributes = new HashMap<>();
        final String secondProviderName = "AllowAll";
        attributes.put(ConfiguredObject.NAME, secondProviderName);
        attributes.put(ConfiguredObject.TYPE, AllowAllAccessControlProvider.ALLOW_ALL);
        attributes.put(AccessControlProvider.PRIORITY, 9999);
        int responseCode = getRestTestHelper().submitRequest("accesscontrolprovider/" + secondProviderName, "PUT", attributes);
        assertEquals("Access control provider creation should be allowed", 201, responseCode);

        responseCode = getRestTestHelper().submitRequest("accesscontrolprovider/" + accessControlProviderName, "DELETE");
        assertEquals("Access control provider deletion should be allowed", 200, responseCode);

        assertAccessControlProviderExistence(accessControlProviderName, false);
    }

    public void testSetAccessControlProviderAttributesAllowed() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        String accessControlProviderName = TestBrokerConfiguration.ENTRY_NAME_ACL_FILE;

        assertAccessControlProviderExistence(accessControlProviderName, true);

        File aclFile = TestFileUtils.createTempFile(this, ".acl", "ACL ALLOW all all");

        Map<String, Object> attributes = new HashMap<String, Object>();
        attributes.put(AccessControlProvider.NAME, accessControlProviderName);
        attributes.put(FileBasedGroupProvider.PATH, aclFile.getAbsolutePath());
        int responseCode = getRestTestHelper().submitRequest("accesscontrolprovider/" + accessControlProviderName, "PUT", attributes);
        assertEquals("Setting of access control provider attributes should be allowed", 200, responseCode);
    }

    public void testSetAccessControlProviderAttributesDenied() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        String accessControlProviderName = TestBrokerConfiguration.ENTRY_NAME_ACL_FILE;

        assertAccessControlProviderExistence(accessControlProviderName, true);

        getRestTestHelper().setUsernameAndPassword(DENIED_USER, DENIED_USER);

        Map<String, Object> attributes = new HashMap<String, Object>();
        attributes.put(GroupProvider.NAME, accessControlProviderName);
        attributes.put(GroupProvider.TYPE, FileBasedGroupProviderImpl.GROUP_FILE_PROVIDER_TYPE);
        attributes.put(FileBasedGroupProvider.PATH, "/path/to/file");
        int responseCode = getRestTestHelper().submitRequest("accesscontrolprovider/" + accessControlProviderName, "PUT", attributes);
        assertEquals("Setting of access control provider attributes should be denied", 403, responseCode);
    }

    /* === HTTP management === */

    public void testSetHttpManagementAttributesAllowed() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        Map<String, Object> attributes = new HashMap<String, Object>();
        attributes.put(HttpManagement.NAME, TestBrokerConfiguration.ENTRY_NAME_HTTP_MANAGEMENT);
        attributes.put(HttpManagement.HTTPS_BASIC_AUTHENTICATION_ENABLED, false);
        attributes.put(HttpManagement.HTTPS_SASL_AUTHENTICATION_ENABLED, false);
        attributes.put(HttpManagement.HTTP_SASL_AUTHENTICATION_ENABLED, false);
        attributes.put(HttpManagement.TIME_OUT, 10000);

        int responseCode = getRestTestHelper().submitRequest(
                "plugin/" + TestBrokerConfiguration.ENTRY_NAME_HTTP_MANAGEMENT, "PUT", attributes);
        assertEquals("Setting of http management should be allowed", 200, responseCode);

        Map<String, Object> details = getRestTestHelper().getJsonAsSingletonList(
                "plugin/" + TestBrokerConfiguration.ENTRY_NAME_HTTP_MANAGEMENT);

        assertEquals("Unexpected session timeout", 10000, details.get(HttpManagement.TIME_OUT));
        assertEquals("Unexpected http basic auth enabled", true, details.get(HttpManagement.HTTP_BASIC_AUTHENTICATION_ENABLED));
        assertEquals("Unexpected https basic auth enabled", false, details.get(HttpManagement.HTTPS_BASIC_AUTHENTICATION_ENABLED));
        assertEquals("Unexpected http sasl auth enabled", false, details.get(HttpManagement.HTTP_SASL_AUTHENTICATION_ENABLED));
        assertEquals("Unexpected https sasl auth enabled", false, details.get(HttpManagement.HTTPS_SASL_AUTHENTICATION_ENABLED));
    }

    public void testSetHttpManagementAttributesDenied() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(DENIED_USER, DENIED_USER);

        Map<String, Object> attributes = new HashMap<String, Object>();
        attributes.put(HttpManagement.NAME, TestBrokerConfiguration.ENTRY_NAME_HTTP_MANAGEMENT);
        attributes.put(HttpManagement.HTTPS_BASIC_AUTHENTICATION_ENABLED, false);
        attributes.put(HttpManagement.HTTPS_SASL_AUTHENTICATION_ENABLED, false);
        attributes.put(HttpManagement.HTTP_SASL_AUTHENTICATION_ENABLED, false);
        attributes.put(HttpManagement.TIME_OUT, 10000);

        int responseCode = getRestTestHelper().submitRequest(
                "plugin/" + TestBrokerConfiguration.ENTRY_NAME_HTTP_MANAGEMENT, "PUT", attributes);
        assertEquals("Setting of http management should be denied", 403, responseCode);

        Map<String, Object> details = getRestTestHelper().getJsonAsSingletonList(
                "plugin/" + TestBrokerConfiguration.ENTRY_NAME_HTTP_MANAGEMENT);

        assertEquals("Unexpected session timeout", HttpManagement.DEFAULT_TIMEOUT_IN_SECONDS,
                details.get(HttpManagement.TIME_OUT));
        assertEquals("Unexpected http basic auth enabled", true,
                details.get(HttpManagement.HTTP_BASIC_AUTHENTICATION_ENABLED));
        assertEquals("Unexpected https basic auth enabled", true,
                details.get(HttpManagement.HTTPS_BASIC_AUTHENTICATION_ENABLED));
        assertEquals("Unexpected http sasl auth enabled", true,
                details.get(HttpManagement.HTTP_SASL_AUTHENTICATION_ENABLED));
        assertEquals("Unexpected https sasl auth enabled", true,
                details.get(HttpManagement.HTTPS_SASL_AUTHENTICATION_ENABLED));
    }

    /* === Broker Logger === */

    public void testCreateBrokerLoggerAllowedDenied() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(BrokerLogger.NAME, "testLogger1");
        attributes.put(ConfiguredObject.TYPE, BrokerMemoryLogger.TYPE);

        getRestTestHelper().submitRequest("brokerlogger", "PUT", attributes, HttpServletResponse.SC_CREATED);
        getRestTestHelper().submitRequest("brokerlogger/testLogger1", "GET", HttpServletResponse.SC_OK);

        getRestTestHelper().setUsernameAndPassword(DENIED_USER, DENIED_USER);
        attributes.put(BrokerNameAndLevelLogInclusionRule.NAME, "testLogger2");
        getRestTestHelper().submitRequest("brokerlogger", "PUT", attributes, HttpServletResponse.SC_FORBIDDEN);
        getRestTestHelper().submitRequest("brokerlogger/testLogger2", "GET", HttpServletResponse.SC_NOT_FOUND);
    }

    public void testDeleteBrokerLoggerAllowedDenied() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(BrokerLogger.NAME, "testLogger1");
        attributes.put(ConfiguredObject.TYPE, BrokerMemoryLogger.TYPE);

        getRestTestHelper().submitRequest("brokerlogger", "PUT", attributes, HttpServletResponse.SC_CREATED);
        getRestTestHelper().submitRequest("brokerlogger/testLogger1", "GET", HttpServletResponse.SC_OK);

        getRestTestHelper().setUsernameAndPassword(DENIED_USER, DENIED_USER);
        getRestTestHelper().submitRequest("brokerlogger/testLogger1", "DELETE", null, HttpServletResponse.SC_FORBIDDEN);
        getRestTestHelper().submitRequest("brokerlogger/testLogger1", "GET", HttpServletResponse.SC_OK);

        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);
        getRestTestHelper().submitRequest("brokerlogger/testLogger1", "DELETE", null, HttpServletResponse.SC_OK);
        getRestTestHelper().submitRequest("brokerlogger/testLogger1", "GET", HttpServletResponse.SC_NOT_FOUND);
    }

    public void testDownloadBrokerLoggerFileAllowedDenied() throws Exception
    {
        final String loggerName = "testFileLogger";
        final String loggerPath = "brokerlogger/" + loggerName;

        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(BrokerLogger.NAME, loggerName);
        attributes.put(ConfiguredObject.TYPE, BrokerFileLogger.TYPE);
        getRestTestHelper().submitRequest("brokerlogger", "PUT", attributes, HttpServletResponse.SC_CREATED);

        getRestTestHelper().submitRequest(loggerPath + "/getFile?fileName=qpid.log", "GET", HttpServletResponse.SC_OK);
        getRestTestHelper().submitRequest(loggerPath + "/getFiles?fileName=qpid.log", "GET", HttpServletResponse.SC_OK);
        getRestTestHelper().submitRequest(loggerPath + "/getAllFiles", "GET", HttpServletResponse.SC_OK);

        getRestTestHelper().setUsernameAndPassword(DENIED_USER, DENIED_USER);
        getRestTestHelper().submitRequest(loggerPath + "/getFile?fileName=qpid.log", "GET", HttpServletResponse.SC_FORBIDDEN);
        getRestTestHelper().submitRequest(loggerPath + "/getFiles?fileName=qpid.log", "GET", HttpServletResponse.SC_FORBIDDEN);
        getRestTestHelper().submitRequest(loggerPath + "/getAllFiles", "GET", HttpServletResponse.SC_FORBIDDEN);
    }

    public void testViewMemoryLoggerEntriesAllowedDenied() throws Exception
    {
        final String loggerName = "testMemoryLogger";
        final String loggerPath = "brokerlogger/" + loggerName;

        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(BrokerLogger.NAME, loggerName);
        attributes.put(ConfiguredObject.TYPE, BrokerMemoryLogger.TYPE);
        getRestTestHelper().submitRequest("brokerlogger", "PUT", attributes, HttpServletResponse.SC_CREATED);

        getRestTestHelper().submitRequest(loggerPath + "/getLogEntries?lastLogId=0", "GET", HttpServletResponse.SC_OK);

        getRestTestHelper().setUsernameAndPassword(DENIED_USER, DENIED_USER);
        getRestTestHelper().submitRequest(loggerPath + "/getLogEntries?lastLogId=0", "GET", HttpServletResponse.SC_FORBIDDEN);
    }

    /* === Broker Log Inclusion Rules === */

    public void testCreateBrokerLoggerRuleAllowedDenied() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        Map<String, Object> loggerAttributes = new HashMap<>();
        loggerAttributes.put(BrokerLogger.NAME, "testLogger1");
        loggerAttributes.put(ConfiguredObject.TYPE, BrokerMemoryLogger.TYPE);
        getRestTestHelper().submitRequest("brokerlogger", "PUT", loggerAttributes, HttpServletResponse.SC_CREATED);


        Map<String, Object> attributes = new HashMap<>();
        attributes.put(BrokerNameAndLevelLogInclusionRule.NAME, "rule1");
        attributes.put(ConfiguredObject.TYPE, BrokerNameAndLevelLogInclusionRule.TYPE);
        attributes.put(BrokerNameAndLevelLogInclusionRule.LEVEL, "DEBUG");

        getRestTestHelper().submitRequest("brokerloginclusionrule/testLogger1", "PUT", attributes, HttpServletResponse.SC_CREATED);
        getRestTestHelper().submitRequest("brokerloginclusionrule/testLogger1/rule1", "GET", HttpServletResponse.SC_OK);

        getRestTestHelper().setUsernameAndPassword(DENIED_USER, DENIED_USER);
        attributes.put(BrokerNameAndLevelLogInclusionRule.NAME, "rule2");
        getRestTestHelper().submitRequest("brokerloginclusionrule/testLogger1", "PUT", attributes, HttpServletResponse.SC_FORBIDDEN);
        getRestTestHelper().submitRequest("brokerloginclusionrule/testLogger1/rule2", "GET", HttpServletResponse.SC_NOT_FOUND);
    }

    public void testUpdateBrokerLoggerRuleAllowedDenied() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        Map<String, Object> loggerAttributes = new HashMap<>();
        loggerAttributes.put(BrokerLogger.NAME, "testLogger1");
        loggerAttributes.put(ConfiguredObject.TYPE, BrokerMemoryLogger.TYPE);
        getRestTestHelper().submitRequest("brokerlogger", "PUT", loggerAttributes, HttpServletResponse.SC_CREATED);

        Map<String, Object> ruleAttributes = new HashMap<>();
        ruleAttributes.put(BrokerNameAndLevelLogInclusionRule.NAME, "rule1");
        ruleAttributes.put(ConfiguredObject.TYPE, BrokerNameAndLevelLogInclusionRule.TYPE);
        ruleAttributes.put(BrokerNameAndLevelLogInclusionRule.LEVEL, "INFO");
        getRestTestHelper().submitRequest("brokerloginclusionrule/testLogger1", "PUT", ruleAttributes, HttpServletResponse.SC_CREATED);

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(BrokerNameAndLevelLogInclusionRule.NAME, "rule1");
        attributes.put(ConfiguredObject.TYPE, BrokerNameAndLevelLogInclusionRule.TYPE);
        attributes.put(BrokerNameAndLevelLogInclusionRule.LEVEL, "DEBUG");

        getRestTestHelper().submitRequest("brokerloginclusionrule/testLogger1/rule1", "PUT", attributes, HttpServletResponse.SC_OK);
        Map<String,Object> resultAfterUpdate = getRestTestHelper().getJsonAsSingletonList("brokerloginclusionrule/testLogger1/rule1");
        assertEquals("Log level was not changed", "DEBUG", resultAfterUpdate.get(BrokerNameAndLevelLogInclusionRule.LEVEL));

        getRestTestHelper().setUsernameAndPassword(DENIED_USER, DENIED_USER);
        attributes.put(BrokerNameAndLevelLogInclusionRule.LEVEL, "INFO");
        getRestTestHelper().submitRequest("brokerloginclusionrule/testLogger1/rule1", "PUT", attributes, HttpServletResponse.SC_FORBIDDEN);

        Map<String,Object> resultAfterDeniedUpdate = getRestTestHelper().getJsonAsSingletonList("brokerloginclusionrule/testLogger1/rule1");
        assertEquals("Log level was changed by not allowed user", "DEBUG", resultAfterDeniedUpdate.get(BrokerNameAndLevelLogInclusionRule.LEVEL));
    }

    public void testDeleteBrokerLoggerRuleAllowedDenied() throws Exception
    {
        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);

        Map<String, Object> loggerAttributes = new HashMap<>();
        loggerAttributes.put(BrokerLogger.NAME, "testLogger1");
        loggerAttributes.put(ConfiguredObject.TYPE, BrokerMemoryLogger.TYPE);
        getRestTestHelper().submitRequest("brokerlogger", "PUT", loggerAttributes, HttpServletResponse.SC_CREATED);


        Map<String, Object> attributes = new HashMap<>();
        attributes.put(BrokerNameAndLevelLogInclusionRule.NAME, "rule1");
        attributes.put(ConfiguredObject.TYPE, BrokerNameAndLevelLogInclusionRule.TYPE);
        attributes.put(BrokerNameAndLevelLogInclusionRule.LEVEL, "DEBUG");

        getRestTestHelper().submitRequest("brokerloginclusionrule/testLogger1", "PUT", attributes, HttpServletResponse.SC_CREATED);
        getRestTestHelper().submitRequest("brokerloginclusionrule/testLogger1/rule1", "GET", HttpServletResponse.SC_OK);

        getRestTestHelper().setUsernameAndPassword(DENIED_USER, DENIED_USER);
        getRestTestHelper().submitRequest("brokerloginclusionrule/testLogger1/rule1", "DELETE", null, HttpServletResponse.SC_FORBIDDEN);
        getRestTestHelper().submitRequest("brokerloginclusionrule/testLogger1/rule1", "GET", HttpServletResponse.SC_OK);

        getRestTestHelper().setUsernameAndPassword(ALLOWED_USER, ALLOWED_USER);
        getRestTestHelper().submitRequest("brokerloginclusionrule/testLogger1/rule1", "DELETE", null, HttpServletResponse.SC_OK);
        getRestTestHelper().submitRequest("brokerloginclusionrule/testLogger1/rule1", "GET", HttpServletResponse.SC_NOT_FOUND);
    }

    /* === Utility Methods === */

    private int createPort(String portName) throws Exception
    {
        Map<String, Object> attributes = new HashMap<String, Object>();
        attributes.put(Port.NAME, portName);
        attributes.put(Port.PORT, 0);
        attributes.put(Port.AUTHENTICATION_PROVIDER, TestBrokerConfiguration.ENTRY_NAME_AUTHENTICATION_PROVIDER);

        return getRestTestHelper().submitRequest("port/" + portName, "PUT", attributes);
    }

    private void assertPortExists(String portName) throws Exception
    {
        assertPortExistence(portName, true);
    }

    private void assertPortDoesNotExist(String portName) throws Exception
    {
        assertPortExistence(portName, false);
    }

    private void assertPortExistence(String portName, boolean exists) throws Exception
    {
        String path = "port/" + portName;
        int expectedResponseCode = exists ? HttpServletResponse.SC_OK : HttpServletResponse.SC_NOT_FOUND;
        getRestTestHelper().submitRequest(path, "GET", expectedResponseCode);

    }

    private void assertKeyStoreExistence(String keyStoreName, boolean exists) throws Exception
    {
        String path = "keystore/" + keyStoreName;
        int expectedResponseCode = exists ? HttpServletResponse.SC_OK : HttpServletResponse.SC_NOT_FOUND;
        getRestTestHelper().submitRequest(path, "GET", expectedResponseCode);
    }

    private void assertTrustStoreExistence(String trustStoreName, boolean exists) throws Exception
    {
        String path = "truststore/" + trustStoreName;
        int expectedResponseCode = exists ? HttpServletResponse.SC_OK : HttpServletResponse.SC_NOT_FOUND;
        getRestTestHelper().submitRequest(path, "GET", expectedResponseCode);
    }

    private int createAuthenticationProvider(String authenticationProviderName) throws Exception
    {
        Map<String, Object> attributes = new HashMap<String, Object>();
        attributes.put(AuthenticationProvider.NAME, authenticationProviderName);
        attributes.put(AuthenticationProvider.TYPE, AnonymousAuthenticationManager.PROVIDER_TYPE);

        return getRestTestHelper().submitRequest("authenticationprovider/" + authenticationProviderName, "PUT", attributes);
    }

    private void assertAuthenticationProviderDoesNotExist(String authenticationProviderName) throws Exception
    {
        assertAuthenticationProviderExistence(authenticationProviderName, false);
    }

    private void assertAuthenticationProviderExists(String authenticationProviderName) throws Exception
    {
        assertAuthenticationProviderExistence(authenticationProviderName, true);
    }

    private void assertAuthenticationProviderExistence(String authenticationProviderName, boolean exists) throws Exception
    {
        String path = "authenticationprovider/" + authenticationProviderName;
        int expectedResponseCode = exists ? HttpServletResponse.SC_OK : HttpServletResponse.SC_NOT_FOUND;
        getRestTestHelper().submitRequest(path, "GET", expectedResponseCode);
    }

    private int createKeyStore(String name, String certAlias) throws IOException
    {
        Map<String, Object> keyStoreAttributes = new HashMap<String, Object>();
        keyStoreAttributes.put(KeyStore.NAME, name);
        keyStoreAttributes.put(FileKeyStore.STORE_URL, TestSSLConstants.KEYSTORE);
        keyStoreAttributes.put(FileKeyStore.PASSWORD, TestSSLConstants.KEYSTORE_PASSWORD);
        keyStoreAttributes.put(FileKeyStore.CERTIFICATE_ALIAS, certAlias);

        return getRestTestHelper().submitRequest("keystore/" + name, "PUT", keyStoreAttributes);
    }

    private int createTrustStore(String name, boolean peersOnly) throws IOException
    {
        Map<String, Object> trustStoreAttributes = new HashMap<String, Object>();
        trustStoreAttributes.put(TrustStore.NAME, name);
        trustStoreAttributes.put(FileTrustStore.STORE_URL, TestSSLConstants.KEYSTORE);
        trustStoreAttributes.put(FileTrustStore.PASSWORD, TestSSLConstants.KEYSTORE_PASSWORD);
        trustStoreAttributes.put(FileTrustStore.PEERS_ONLY, peersOnly);

        return getRestTestHelper().submitRequest("truststore/" + name, "PUT", trustStoreAttributes);
    }

    private void assertGroupProviderExistence(String groupProviderName, boolean exists) throws Exception
    {
        String path = "groupprovider/" + groupProviderName;
        int expectedResponseCode = exists ? HttpServletResponse.SC_OK : HttpServletResponse.SC_NOT_FOUND;
        getRestTestHelper().submitRequest(path, "GET", expectedResponseCode);
    }

    private int createGroupProvider(String groupProviderName) throws Exception
    {
        File file = TestFileUtils.createTempFile(this, ".groups");
        Map<String, Object> attributes = new HashMap<String, Object>();
        attributes.put(GroupProvider.NAME, groupProviderName);
        attributes.put(GroupProvider.TYPE, FileBasedGroupProviderImpl.GROUP_FILE_PROVIDER_TYPE);
        attributes.put(FileBasedGroupProvider.PATH, file.getAbsoluteFile());

        return getRestTestHelper().submitRequest("groupprovider/" + groupProviderName, "PUT", attributes);
    }

    private void assertAccessControlProviderExistence(String accessControlProviderName, boolean exists) throws Exception
    {
        String path = "accesscontrolprovider/" + accessControlProviderName;
        int expectedResponseCode = exists ? HttpServletResponse.SC_OK : HttpServletResponse.SC_NOT_FOUND;
        getRestTestHelper().submitRequest(path, "GET", expectedResponseCode);
    }

    private int createAccessControlProvider(String accessControlProviderName) throws Exception
    {
        File file = TestFileUtils.createTempFile(this, ".acl", _secondaryAclFileContent);
        Map<String, Object> attributes = new HashMap<String, Object>();
        attributes.put(AccessControlProvider.NAME, accessControlProviderName);
        attributes.put(AccessControlProvider.TYPE, AclFileAccessControlProvider.ACL_FILE_PROVIDER_TYPE);
        attributes.put(AclFileAccessControlProvider.PATH, file.getAbsoluteFile());

        return getRestTestHelper().submitRequest("accesscontrolprovider/" + accessControlProviderName, "PUT", attributes);
    }
}
