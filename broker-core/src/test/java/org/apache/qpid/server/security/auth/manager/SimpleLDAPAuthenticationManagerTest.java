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
package org.apache.qpid.server.security.auth.manager;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.qpid.server.security.auth.manager.CachingAuthenticationProvider.AUTHENTICATION_CACHE_MAX_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;

import org.apache.directory.api.ldap.model.constants.SupportedSaslMechanisms;
import org.apache.directory.api.ldap.model.entry.DefaultEntry;
import org.apache.directory.api.ldap.model.entry.Entry;
import org.apache.directory.api.ldap.model.exception.LdapException;
import org.apache.directory.api.util.Strings;
import org.apache.directory.server.annotations.CreateKdcServer;
import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.annotations.SaslMechanism;
import org.apache.directory.server.core.annotations.ApplyLdifFiles;
import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreatePartition;
import org.apache.directory.server.core.api.DirectoryService;
import org.apache.directory.server.core.kerberos.KeyDerivationInterceptor;
import org.apache.directory.server.factory.ServerAnnotationProcessor;
import org.apache.directory.server.kerberos.kdc.KdcServer;
import org.apache.directory.server.kerberos.shared.crypto.encryption.KerberosKeyFactory;
import org.apache.directory.server.kerberos.shared.keytab.Keytab;
import org.apache.directory.server.kerberos.shared.keytab.KeytabEntry;
import org.apache.directory.server.ldap.LdapServer;
import org.apache.directory.server.ldap.handlers.sasl.gssapi.GssapiMechanismHandler;
import org.apache.directory.server.ldap.handlers.sasl.plain.PlainMechanismHandler;
import org.apache.directory.shared.kerberos.KerberosTime;
import org.apache.directory.shared.kerberos.codec.types.EncryptionType;
import org.apache.directory.shared.kerberos.components.EncryptionKey;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.SocketConnectionPrincipal;
import org.apache.qpid.server.security.auth.sasl.SaslNegotiator;
import org.apache.qpid.server.security.auth.sasl.SaslSettings;
import org.apache.qpid.test.utils.CreateLdapServerExtension;
import org.apache.qpid.test.utils.JvmVendor;
import org.apache.qpid.server.test.KerberosUtilities;
import org.apache.qpid.test.utils.SystemPropertySetter;
import org.apache.qpid.test.utils.TestFileUtils;
import org.apache.qpid.test.utils.UnitTestBase;

@CreateDS(
    name = "testDS",
    partitions =
    {
        @CreatePartition(name = "test", suffix = "dc=qpid,dc=org")
    },
    additionalInterceptors =
    {
        KeyDerivationInterceptor.class
    }
)
@CreateLdapServer(
    transports =
    {
        @CreateTransport(protocol = "LDAP")
    },
    allowAnonymousAccess = true,
    saslHost = "localhost",
    saslPrincipal = "ldap/localhost@QPID.ORG",
    saslMechanisms =
    {
        @SaslMechanism(name = SupportedSaslMechanisms.PLAIN, implClass = PlainMechanismHandler.class),
        @SaslMechanism(name = SupportedSaslMechanisms.GSSAPI, implClass = GssapiMechanismHandler.class)
    }
)
@CreateKdcServer(
    transports =
    {
        @CreateTransport(protocol = "TCP", port = 0)
    },
    kdcPrincipal="krbtgt/QPID.ORG@QPID.ORG",
    primaryRealm="QPID.ORG",
    searchBaseDn = "ou=users,dc=qpid,dc=org")
@ApplyLdifFiles("users.ldif")
public class SimpleLDAPAuthenticationManagerTest extends UnitTestBase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleLDAPAuthenticationManagerTest.class);
    private static final String ROOT = "dc=qpid,dc=org";
    private static final String USERS_DN = "ou=users," + ROOT;
    private static final String SEARCH_CONTEXT_VALUE = USERS_DN;
    private static final String SEARCH_FILTER_VALUE = "(uid={0})";
    private static final String LDAP_URL_TEMPLATE = "ldap://localhost:%d";
    private static final String USER_1_NAME = "test1";
    private static final String USER_1_PASSWORD = "password1";
    private static final String USER_1_DN = "cn=integration-test1,ou=users,dc=qpid,dc=org";
    private static final String GROUP_SEARCH_CONTEXT_VALUE = "ou=groups,dc=qpid,dc=org";
    private static final String GROUP_SEARCH_FILTER_VALUE = "(member={0})";
    private static final String LDAP_SERVICE_NAME = "ldap";
    private static final String REALM = "QPID.ORG";
    private static final String HOSTNAME = "localhost";
    private static final String BROKER_PRINCIPAL = "service/" + HOSTNAME;
    private static final String LINE_SEPARATOR = System.lineSeparator();
    private static final String LOGIN_SCOPE = "ldap-gssapi-bind";
    private static final AtomicBoolean KERBEROS_SETUP = new AtomicBoolean();
    private static final KerberosUtilities UTILS = new KerberosUtilities();

    @RegisterExtension
    public static CreateLdapServerExtension LDAP = new CreateLdapServerExtension();

    @RegisterExtension
    public static final SystemPropertySetter SYSTEM_PROPERTY_SETTER = new SystemPropertySetter();

    private SimpleLDAPAuthenticationManager<?> _authenticationProvider;

    @BeforeEach
    public void setUp()
    {
        _authenticationProvider = createAuthenticationProvider();
    }

    @AfterEach
    public void tearDown()
    {
        if (_authenticationProvider != null)
        {
            _authenticationProvider.close();
        }
    }

    @Test
    public void testAuthenticateSuccess()
    {
        final AuthenticationResult result = _authenticationProvider.authenticate(USER_1_NAME, USER_1_PASSWORD);
        assertEquals(AuthenticationResult.AuthenticationStatus.SUCCESS, result.getStatus());
        assertEquals(USER_1_DN, result.getMainPrincipal().getName());
    }

    @Test
    public void testAuthenticateFailure()
    {
        final AuthenticationResult result = _authenticationProvider.authenticate(USER_1_NAME, USER_1_PASSWORD + "_");
        assertEquals(AuthenticationResult.AuthenticationStatus.ERROR, result.getStatus());
    }

    @Test
    public void testSaslPlainNegotiatorPlain()
    {
        final SaslSettings saslSettings = mock(SaslSettings.class);
        when(saslSettings.getLocalFQDN()).thenReturn(HOSTNAME);

        final SaslNegotiator negotiator = _authenticationProvider.createSaslNegotiator("PLAIN", saslSettings, null);
        assertNotNull(negotiator, "Could not create SASL negotiator for mechanism 'PLAIN'");

        final AuthenticationResult result = negotiator.handleResponse(new byte[0]);
        assertEquals(AuthenticationResult.AuthenticationStatus.CONTINUE, result.getStatus(),
                "Unexpected authentication status");

        final AuthenticationResult result2 =
                negotiator.handleResponse(String.format("\0%s\0%s", USER_1_NAME, USER_1_PASSWORD).getBytes(UTF_8));

        assertEquals(AuthenticationResult.AuthenticationStatus.SUCCESS, result2.getStatus(),
                "Unexpected authentication status");
    }

    @Test
    public void testGroups()
    {
        _authenticationProvider.close();
        final Map<String, Object> groupSetUp = Map.of(SimpleLDAPAuthenticationManager.GROUP_SEARCH_CONTEXT, GROUP_SEARCH_CONTEXT_VALUE,
                SimpleLDAPAuthenticationManager.GROUP_SEARCH_FILTER, GROUP_SEARCH_FILTER_VALUE);
        _authenticationProvider = createAuthenticationProvider(groupSetUp);

        final AuthenticationResult result = _authenticationProvider.authenticate(USER_1_NAME, USER_1_PASSWORD);
        assertEquals(AuthenticationResult.AuthenticationStatus.SUCCESS, result.getStatus());
        assertEquals(USER_1_DN, result.getMainPrincipal().getName());

        final Set<Principal> principals = result.getPrincipals();
        assertNotNull(principals);

        final Principal groupPrincipal = principals.stream()
                .filter(p -> "cn=group1,ou=groups,dc=qpid,dc=org".equalsIgnoreCase(p.getName()))
                .findFirst()
                .orElse(null);
        assertNotNull(groupPrincipal);
    }

    @Test
    public void testAuthenticateSuccessWhenCachingEnabled()
    {
        _authenticationProvider.close();
        _authenticationProvider = createCachingAuthenticationProvider();

        final SocketConnectionPrincipal principal = mock(SocketConnectionPrincipal.class);
        when(principal.getRemoteAddress()).thenReturn(new InetSocketAddress(HOSTNAME, 5672));
        final Subject subject = new Subject(true, Set.of(principal), Set.of(), Set.of());
        final AuthenticationResult result = Subject.doAs(subject, (PrivilegedAction<AuthenticationResult>) () ->
                _authenticationProvider.authenticate(USER_1_NAME, USER_1_PASSWORD));
        assertEquals(AuthenticationResult.AuthenticationStatus.SUCCESS, result.getStatus());
        assertEquals(USER_1_DN, result.getMainPrincipal().getName());
    }

    @Test
    public void testGssapiBindWithKeyTab() throws Exception
    {
        setUpKerberosAndJaas();

        final Map<String, Object> attributes = Map.of(SimpleLDAPAuthenticationManager.AUTHENTICATION_METHOD, LdapAuthenticationMethod.GSSAPI.name(),
                SimpleLDAPAuthenticationManager.LOGIN_CONFIG_SCOPE, LOGIN_SCOPE);
        final SimpleLDAPAuthenticationManagerImpl authenticationProvider = createAuthenticationProvider(attributes);
        final AuthenticationResult result = authenticationProvider.authenticate(USER_1_NAME, USER_1_PASSWORD);
        assertEquals(AuthenticationResult.AuthenticationStatus.SUCCESS, result.getStatus());
        assertEquals(USER_1_DN, result.getMainPrincipal().getName());
    }

    @Test
    public void testChangeAuthenticationToGssapi() throws Exception
    {
        setUpKerberosAndJaas();

        final Map<String, Object> attributes = Map.of(SimpleLDAPAuthenticationManager.AUTHENTICATION_METHOD, LdapAuthenticationMethod.GSSAPI.name(),
                SimpleLDAPAuthenticationManager.LOGIN_CONFIG_SCOPE, LOGIN_SCOPE);
        _authenticationProvider.setAttributes(attributes);

        final AuthenticationResult result = _authenticationProvider.authenticate(USER_1_NAME, USER_1_PASSWORD);
        assertEquals(AuthenticationResult.AuthenticationStatus.SUCCESS, result.getStatus());
        assertEquals(USER_1_DN, result.getMainPrincipal().getName());
    }

    @Test
    public void testChangeAuthenticationToGssapiWithInvalidScope() throws Exception
    {
        setUpKerberosAndJaas();

        final Map<String, Object> attributes = Map.of(SimpleLDAPAuthenticationManager.AUTHENTICATION_METHOD, LdapAuthenticationMethod.GSSAPI.name(),
                SimpleLDAPAuthenticationManager.LOGIN_CONFIG_SCOPE, "non-existing");

        assertThrows(IllegalConfigurationException.class,
                () -> _authenticationProvider.setAttributes(attributes),
                "Exception is expected");
    }

    @Test
    public void testChangeAuthenticationToGssapiWhenConfigIsBroken() throws Exception
    {
        setUpKerberosAndJaas();

        final Map<String, Object> attributes = Map.of(SimpleLDAPAuthenticationManager.AUTHENTICATION_METHOD, LdapAuthenticationMethod.GSSAPI.name(),
                SimpleLDAPAuthenticationManager.LOGIN_CONFIG_SCOPE, "ldap-gssapi-bind-broken");

        assertThrows(IllegalConfigurationException.class,
                () -> _authenticationProvider.setAttributes(attributes),
                "Exception is expected");
    }

    @Test
    public void testChangeAuthenticationToGssapiNoScopeProvided() throws Exception
    {
        setUpKerberosAndJaas();

        final Map<String, Object> attributes = Map.of(SimpleLDAPAuthenticationManager.AUTHENTICATION_METHOD,
                LdapAuthenticationMethod.GSSAPI.name());
        _authenticationProvider.setAttributes(attributes);

        final AuthenticationResult result = _authenticationProvider.authenticate(USER_1_NAME, USER_1_PASSWORD);
        assertEquals(AuthenticationResult.AuthenticationStatus.SUCCESS, result.getStatus());
        assertEquals(USER_1_DN, result.getMainPrincipal().getName());
    }

    private SimpleLDAPAuthenticationManagerImpl createAuthenticationProvider()
    {
        return createAuthenticationProvider(Map.of());
    }

    private SimpleLDAPAuthenticationManagerImpl createCachingAuthenticationProvider()
    {
        final Map<String, String> context = Map.of(AUTHENTICATION_CACHE_MAX_SIZE, "1");
        final Map<String, Object> attributes =Map.of(SimpleLDAPAuthenticationManager.CONTEXT, context);
        return createAuthenticationProvider(attributes);
    }

    private SimpleLDAPAuthenticationManagerImpl createAuthenticationProvider(final Map<String, Object> settings)
    {
        final Broker<?> broker = BrokerTestHelper.createBrokerMock();
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put(SimpleLDAPAuthenticationManager.NAME, getTestName());
        attributes.put(SimpleLDAPAuthenticationManager.SEARCH_CONTEXT, SEARCH_CONTEXT_VALUE);
        attributes.put(SimpleLDAPAuthenticationManager.PROVIDER_URL,
                       String.format(LDAP_URL_TEMPLATE, LDAP.getLdapServer().getPort()));
        attributes.put(SimpleLDAPAuthenticationManager.SEARCH_FILTER, SEARCH_FILTER_VALUE);
        attributes.put(SimpleLDAPAuthenticationManager.CONTEXT, Map.of(AUTHENTICATION_CACHE_MAX_SIZE, "0"));
        attributes.putAll(settings);
        final SimpleLDAPAuthenticationManagerImpl authenticationProvider =
                new SimpleLDAPAuthenticationManagerImpl(attributes, broker);
        authenticationProvider.open();
        return authenticationProvider;
    }

    private void setUpKerberosAndJaas() throws Exception
    {
        assumeFalse(Objects.equals(getJvmVendor(), JvmVendor.IBM));
        if (KERBEROS_SETUP.compareAndSet(false, true))
        {
            setUpKerberos();
            setUpJaas();
        }
    }

    private void setUpKerberos() throws Exception
    {
        final LdapServer ldapServer = LDAP.getLdapServer();
        final KdcServer kdcServer =
                ServerAnnotationProcessor.getKdcServer(LDAP.getDirectoryService(), ldapServer.getPort() + 1);
        kdcServer.getConfig().setPaEncTimestampRequired(false);

        final int port = kdcServer.getTransports()[0].getPort();
        final String krb5confPath = createKrb5Conf(port);
        SYSTEM_PROPERTY_SETTER.setSystemProperty("java.security.krb5.conf", krb5confPath);
        SYSTEM_PROPERTY_SETTER.setSystemProperty("java.security.krb5.realm", null);
        SYSTEM_PROPERTY_SETTER.setSystemProperty("java.security.krb5.kdc", null);

        final KerberosPrincipal servicePrincipal =
                new KerberosPrincipal(LDAP_SERVICE_NAME + "/" + HOSTNAME + "@" + REALM,
                                      KerberosPrincipal.KRB_NT_SRV_HST);
        final String servicePrincipalName = servicePrincipal.getName();
        ldapServer.setSaslHost(servicePrincipalName.substring(servicePrincipalName.indexOf("/") + 1,
                                                              servicePrincipalName.indexOf("@")));
        ldapServer.setSaslPrincipal(servicePrincipalName);
        ldapServer.setSearchBaseDn(USERS_DN);

        createPrincipal("KDC", "KDC", "krbtgt", randomUUID().toString(), "krbtgt/" + REALM + "@" + REALM);
        createPrincipal("Service", "LDAP Service", "ldap", randomUUID().toString(), servicePrincipalName);
    }

    private void setUpJaas() throws Exception
    {
        createKeyTab(BROKER_PRINCIPAL);
        UTILS.prepareConfiguration(KerberosUtilities.HOST_NAME, SYSTEM_PROPERTY_SETTER);
    }

    private String createKrb5Conf(final int port) throws IOException
    {
        final File file = createFile("krb5", ".conf");
        final String config = String.format("[libdefaults]%1$s"
                                            + "    default_realm = %2$s%1$s"
                                            + "    udp_preference_limit = 1%1$s"
                                            + "    default_tkt_enctypes = aes128-cts-hmac-sha1-96 rc4-hmac%1$s"
                                            + "    default_tgs_enctypes = aes128-cts-hmac-sha1-96  rc4-hmac%1$s"
                                            + "    permitted_enctypes = aes128-cts-hmac-sha1-96 rc4-hmac%1$s"
                                            + "[realms]%1$s"
                                            + "    %2$s = {%1$s"
                                            + "    kdc = %3$s%1$s"
                                            + "    }%1$s"
                                            + "[domain_realm]%1$s"
                                            + "    .%4$s = %2$s%1$s"
                                            + "    %4$s = %2$s%1$s",
                                            LINE_SEPARATOR,
                                            REALM,
                                            HOSTNAME + ":" + port,
                                            Strings.toLowerCaseAscii(REALM));
        LOGGER.debug("krb5.conf:" + config);
        TestFileUtils.saveTextContentInFile(config, file);
        return file.getAbsolutePath();
    }

    private void createPrincipal(final String sn,
                                 final String cn,
                                 final String uid,
                                 final String userPassword,
                                 final String kerberosPrincipalName) throws LdapException
    {
        final DirectoryService directoryService = LDAP.getDirectoryService();
        final Entry entry = new DefaultEntry(directoryService.getSchemaManager());
        entry.setDn(String.format("uid=%s,%s", uid, USERS_DN));
        entry.add("objectClass", "top", "person", "inetOrgPerson", "krb5principal", "krb5kdcentry");
        entry.add("cn", cn);
        entry.add("sn", sn);
        entry.add("uid", uid);
        entry.add("userPassword", userPassword);
        entry.add("krb5PrincipalName", kerberosPrincipalName);
        entry.add("krb5KeyVersionNumber", "0");
        directoryService.getAdminSession().add(entry);
    }

    private void createPrincipal(String uid, String userPassword) throws LdapException
    {
        createPrincipal(uid, uid, uid, userPassword, uid + "@" + REALM);
    }

    private void createPrincipal(final File keyTabFile, final String... principals) throws LdapException, IOException
    {
        final Keytab keytab = new Keytab();
        final List<KeytabEntry> entries = new ArrayList<>();
        final String password = randomUUID().toString();
        for (final String principal : principals)
        {
            createPrincipal(principal, password);
            final String principalName = principal + "@" + REALM;
            final KerberosTime timestamp = new KerberosTime();
            final Map<EncryptionType, EncryptionKey> keys = KerberosKeyFactory.getKerberosKeys(principalName, password);
            keys.forEach((type, key) -> entries.add(new KeytabEntry(principalName,
                                                                    1,
                                                                    timestamp,
                                                                    (byte) key.getKeyVersion(),
                                                                    key)));
        }
        keytab.setEntries(entries);
        keytab.write(keyTabFile);
    }

    private void createKeyTab(String... principals) throws LdapException, IOException
    {
        final File keyTabFile = createFile("kerberos", ".keytab");
        createPrincipal(keyTabFile, principals);
    }

    private File createFile(final String prefix, final String suffix) throws IOException
    {
        final Path targetDir = FileSystems.getDefault().getPath("target");
        final File file = new File(targetDir.toFile(), prefix + suffix);
        if (file.exists())
        {
            if (!file.delete())
            {
                throw new IOException(String.format("Cannot delete existing file '%s'", file.getAbsolutePath()));
            }
        }
        if (!file.createNewFile())
        {
            throw new IOException(String.format("Cannot create file '%s'", file.getAbsolutePath()));
        }
        return file;
    }
}
