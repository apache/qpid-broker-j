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
package org.apache.qpid.server.security.auth.manager.ldap;

import static org.apache.qpid.server.security.auth.manager.CachingAuthenticationProvider.AUTHENTICATION_CACHE_MAX_SIZE;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.number.OrderingComparison.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeThat;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.server.security.FileTrustStore;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.manager.SimpleLDAPAuthenticationManager;
import org.apache.qpid.server.util.FileUtils;
import org.apache.qpid.test.utils.JvmVersion;
import org.apache.qpid.test.utils.PortHelper;
import org.apache.qpid.test.utils.UnitTestBase;
import org.apache.qpid.test.utils.tls.CertificateEntry;
import org.apache.qpid.test.utils.tls.KeyCertificatePair;
import org.apache.qpid.test.utils.tls.PrivateKeyEntry;
import org.apache.qpid.test.utils.tls.TlsResource;
import org.apache.qpid.test.utils.tls.TlsResourceBuilder;

/**
 * Performs test of SimpleLDAPAuthenticationManager using SSL connection to LDAP server
 */
public class SimpleLDAPAuthenticationManagerTest extends UnitTestBase
{
    @ClassRule
    public static final TlsResource TLS_RESOURCE = new TlsResource("pk", "localhost", "secret", "pkcs12");

    private static final String LDAP_FOLDER = TMP_FOLDER + File.separator + "test-ldap";

    private static final PortHelper PORT_HELPER = new PortHelper();
    private static final int PORT = PORT_HELPER.getNextAvailable();

    private static final String DN_LOCALHOST = "CN=localhost";
    private static final String LDAP_USERNAME = "test1";
    private static final String LDAP_PASSWORD = "password1";

    private static Broker<?> _broker;

    private static EmbeddedLDAPServer _ldapServer;

    private static SimpleLDAPAuthenticationManager<?> _authenticationManager;

    @BeforeClass
    public static void setUp() throws Exception
    {
        assumeThat(JvmVersion.getVersion(), is(greaterThanOrEqualTo(11)));

        _broker = BrokerTestHelper.createBrokerMock();

        final KeyCertificatePair keyCertPair = TlsResourceBuilder.createSelfSigned(DN_LOCALHOST);
        final PrivateKeyEntry privateKeyEntry = new PrivateKeyEntry(TLS_RESOURCE.getPrivateKeyAlias(),
                                                                     keyCertPair.getPrivateKey(),
                                                                     keyCertPair.getCertificate());
        final CertificateEntry certificateEntry =
                new CertificateEntry(TLS_RESOURCE.getCertificateAlias(), keyCertPair.getCertificate());
        final Path keyStoreFile = TLS_RESOURCE.createKeyStore("pkcs12", privateKeyEntry);
        Path trustStoreFile = TLS_RESOURCE.createKeyStore("pkcs12", certificateEntry);

        final File workDir = new File(LDAP_FOLDER);
        if (workDir.exists())
        {
            FileUtils.delete(new File(LDAP_FOLDER), true);
        }
        Files.createDirectory(workDir.toPath());

        _ldapServer = new EmbeddedLDAPServer(workDir, keyStoreFile.toString(), TLS_RESOURCE.getSecret(), PORT);
        _ldapServer.startServer();

        _authenticationManager = createSimpleLDAPAuthenticationManager(trustStoreFile);
    }

    @AfterClass
    public static void tearDown() throws Exception
    {
        if (_authenticationManager != null)
        {
            _authenticationManager.close();
        }
        if (_ldapServer != null)
        {
            _ldapServer.stopServer();
        }
        FileUtils.delete(new File(LDAP_FOLDER), true);
    }

    @Test
    public void authenticateSuccess()
    {
        assumeThat(JvmVersion.getVersion(), is(greaterThanOrEqualTo(11)));

        final AuthenticationResult result = _authenticationManager.authenticate(LDAP_USERNAME, LDAP_PASSWORD);
        assertEquals(AuthenticationResult.AuthenticationStatus.SUCCESS, result.getStatus());
    }

    @Test
    public void authenticateFailure()
    {
        assumeThat(JvmVersion.getVersion(), is(greaterThanOrEqualTo(11)));

        final AuthenticationResult result = _authenticationManager.authenticate(LDAP_USERNAME, LDAP_PASSWORD + "1");
        assertEquals(AuthenticationResult.AuthenticationStatus.ERROR, result.getStatus());
    }

    @SuppressWarnings("unchecked")
    private static TrustStore<?> createTrustStore(final Path trustStoreFile)
    {
        final Map<String, Object> attributesMap = new HashMap<>();
        attributesMap.put(FileTrustStore.NAME, trustStoreFile.getFileName());
        attributesMap.put(FileTrustStore.TYPE, "FileTrustStore");
        attributesMap.put(FileTrustStore.STORE_URL, trustStoreFile.toUri());
        attributesMap.put(FileTrustStore.PASSWORD, TLS_RESOURCE.getSecret());
        return _broker.getObjectFactory().create(TrustStore.class, attributesMap, _broker);
    }

    @SuppressWarnings("unchecked")
    private static SimpleLDAPAuthenticationManager<?> createSimpleLDAPAuthenticationManager(final Path trustStorePath)
    {
        final TrustStore<?> trustStore = createTrustStore(trustStorePath);

        final String LDAP_URL = "ldaps://localhost:" + PORT;
        final String ROOT = "dc=qpid,dc=org";
        final String SEARCH_CONTEXT_VALUE = "ou=users," + ROOT;
        final String SEARCH_FILTER_VALUE = "(uid={0})";

        final Map<String, Object> attributesMap = new HashMap<>();
        attributesMap.put(SimpleLDAPAuthenticationManager.NAME, "SimpleLDAPAuthenticationManager");
        attributesMap.put(SimpleLDAPAuthenticationManager.ID, UUID.randomUUID());
        attributesMap.put(SimpleLDAPAuthenticationManager.TYPE, SimpleLDAPAuthenticationManager.PROVIDER_TYPE);
        attributesMap.put(SimpleLDAPAuthenticationManager.SEARCH_CONTEXT, SEARCH_CONTEXT_VALUE);
        attributesMap.put(SimpleLDAPAuthenticationManager.PROVIDER_URL, LDAP_URL);
        attributesMap.put(SimpleLDAPAuthenticationManager.SEARCH_FILTER, SEARCH_FILTER_VALUE);
        attributesMap.put(SimpleLDAPAuthenticationManager.CONTEXT,
                          Collections.singletonMap(AUTHENTICATION_CACHE_MAX_SIZE, "0"));
        attributesMap.put(SimpleLDAPAuthenticationManager.TRUST_STORE, trustStore);
        return (SimpleLDAPAuthenticationManager<?>) _broker.getObjectFactory()
                                                   .create(AuthenticationProvider.class, attributesMap, _broker);
    }
}
