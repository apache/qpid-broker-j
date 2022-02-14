package org.apache.qpid.server.security.auth.manager;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.apache.qpid.server.security.auth.AuthenticationResult.AuthenticationStatus.SUCCESS;
import static org.apache.qpid.server.security.auth.AuthenticationResult.AuthenticationStatus.ERROR;
import static org.apache.qpid.server.security.auth.manager.CachingAuthenticationProvider.AUTHENTICATION_CACHE_MAX_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.directory.api.ldap.model.constants.SupportedSaslMechanisms;
import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.annotations.SaslMechanism;
import org.apache.directory.server.core.annotations.ApplyLdifFiles;
import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreatePartition;
import org.apache.directory.server.core.integ.CreateLdapServerRule;
import org.apache.directory.server.core.kerberos.KeyDerivationInterceptor;
import org.apache.directory.server.ldap.handlers.sasl.gssapi.GssapiMechanismHandler;
import org.apache.directory.server.ldap.handlers.sasl.plain.PlainMechanismHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.sasl.SaslNegotiator;
import org.apache.qpid.server.security.auth.sasl.SaslSettings;
import org.apache.qpid.server.security.auth.sasl.SaslUtil;
import org.apache.qpid.server.security.auth.sasl.crammd5.CramMd5Base64HashedNegotiator;
import org.apache.qpid.server.security.auth.sasl.crammd5.CramMd5Base64HexNegotiator;
import org.apache.qpid.server.security.auth.sasl.crammd5.CramMd5Negotiator;
import org.apache.qpid.server.util.Strings;
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
@ApplyLdifFiles("users.ldif")
public class CompositeUsernamePasswordAuthenticationManagerTest extends UnitTestBase
{
    @ClassRule
    public static CreateLdapServerRule LDAP = new CreateLdapServerRule();
    private final List<AuthenticationProvider<?>> _authenticationProviders = new ArrayList<>();
    private Broker<?> _broker;
    private TaskExecutor _executor;

    private static final String USERNAME = "user1";
    private static final String PASSWORD = "password1";

    @Before
    public void setUp() throws Exception
    {
        _executor = new CurrentThreadTaskExecutor();
        _executor.start();
        _broker = BrokerTestHelper.createBrokerMock();
        when(_broker.getTaskExecutor()).thenReturn(_executor);
        when(_broker.getChildExecutor()).thenReturn(_executor);
        when(_broker.getAuthenticationProviders()).thenReturn(_authenticationProviders);
        SaslHelper._clientNonce = UUID.randomUUID().toString();
    }

    @After
    public void tearDown() throws Exception
    {
        _executor.stop();
        _authenticationProviders.clear();
    }

    @SuppressWarnings("unchecked")
    private CompositeUsernamePasswordAuthenticationManager<?> createCompositeAuthenticationManager(
            UsernamePasswordAuthenticationProvider<?>... authenticationProviders
                                                                                                  )
    {
        final Map<String, Object> attributesMap = new HashMap<>();
        attributesMap.put(AuthenticationProvider.TYPE, CompositeUsernamePasswordAuthenticationManager.PROVIDER_TYPE);
        attributesMap.put(AuthenticationProvider.NAME, "CompositeAuthenticationProvider");
        attributesMap.put(AuthenticationProvider.ID, UUID.randomUUID());
        if (authenticationProviders.length > 0)
        {
            attributesMap.put(
                    "delegates",
                    Arrays.stream(authenticationProviders).map(ConfiguredObject::getName).collect(Collectors.toList())
            );
        }

        AuthenticationProvider<?> authProvider =
                _broker.getObjectFactory().create(AuthenticationProvider.class, attributesMap, _broker);
        _authenticationProviders.add(authProvider);
        return (CompositeUsernamePasswordAuthenticationManager<?>) authProvider;
    }

    @SuppressWarnings("unchecked")
    private MD5AuthenticationProvider createMD5AuthenticationProvider()
    {
        final Map<String, Object> attributesMap = new HashMap<>();
        attributesMap.put(AuthenticationProvider.NAME, "MD5AuthenticationProvider");
        attributesMap.put(AuthenticationProvider.TYPE, "MD5");
        attributesMap.put(AuthenticationProvider.ID, UUID.randomUUID());
        AuthenticationProvider<?> authProvider =
                _broker.getObjectFactory().create(AuthenticationProvider.class, attributesMap, _broker);
        _authenticationProviders.add(authProvider);
        return (MD5AuthenticationProvider) authProvider;
    }

    @SuppressWarnings("unchecked")
    private PlainAuthenticationProvider createPlainAuthenticationProvider(String... names)
    {
        final Map<String, Object> attributesMap = new HashMap<>();
        attributesMap.put(AuthenticationProvider.NAME, names.length == 0 ? "PlainAuthenticationProvider" : names[0]);
        attributesMap.put(AuthenticationProvider.TYPE, "Plain");
        attributesMap.put(AuthenticationProvider.ID, UUID.randomUUID());
        PlainAuthenticationProvider authProvider = (PlainAuthenticationProvider) _broker.getObjectFactory()
            .create(AuthenticationProvider.class, attributesMap, _broker);
        _authenticationProviders.add(authProvider);
        return authProvider;
    }

    @SuppressWarnings("unchecked")
    private ScramSHA256AuthenticationManager createScramSHA256AuthenticationManager(String... names)
    {
        final Map<String, Object> attributesMap = new HashMap<>();
        attributesMap.put(AuthenticationProvider.NAME,
                          names.length == 0 ? "ScramSHA256AuthenticationManager" : names[0]);
        attributesMap.put(AuthenticationProvider.TYPE, "SCRAM-SHA-256");
        attributesMap.put(AuthenticationProvider.ID, UUID.randomUUID());
        ScramSHA256AuthenticationManager authProvider = (ScramSHA256AuthenticationManager) _broker.getObjectFactory()
            .create(AuthenticationProvider.class, attributesMap, _broker);
        _authenticationProviders.add(authProvider);
        return authProvider;
    }

    @SuppressWarnings("unchecked")
    private ScramSHA1AuthenticationManager createScramSHA1AuthenticationManager(String... names)
    {
        final Map<String, Object> attributesMap = new HashMap<>();
        attributesMap.put(AuthenticationProvider.NAME,
                          names.length == 0 ? "ScramSHA1AuthenticationManager" : names[0]);
        attributesMap.put(AuthenticationProvider.TYPE, "SCRAM-SHA-1");
        attributesMap.put(AuthenticationProvider.ID, UUID.randomUUID());
        ScramSHA1AuthenticationManager authProvider = (ScramSHA1AuthenticationManager) _broker.getObjectFactory()
            .create(AuthenticationProvider.class, attributesMap, _broker);
        _authenticationProviders.add(authProvider);
        return authProvider;
    }

    @SuppressWarnings("unchecked")
    private SimpleLDAPAuthenticationManager<?> createSimpleLDAPAuthenticationManager()
    {
        final String LDAP_URL_TEMPLATE = "ldap://localhost:%d";
        final String ROOT = "dc=qpid,dc=org";
        final String SEARCH_CONTEXT_VALUE = "ou=users," + ROOT;
        final String SEARCH_FILTER_VALUE = "(uid={0})";

        final Map<String, Object> attributesMap = new HashMap<>();
        attributesMap.put(SimpleLDAPAuthenticationManager.NAME, "SimpleLDAPAuthenticationManager");
        attributesMap.put(SimpleLDAPAuthenticationManager.ID, UUID.randomUUID());
        attributesMap.put(SimpleLDAPAuthenticationManager.TYPE, SimpleLDAPAuthenticationManager.PROVIDER_TYPE);
        attributesMap.put(SimpleLDAPAuthenticationManager.SEARCH_CONTEXT, SEARCH_CONTEXT_VALUE);
        attributesMap.put(SimpleLDAPAuthenticationManager.PROVIDER_URL,
                          String.format(LDAP_URL_TEMPLATE, LDAP.getLdapServer().getPort()));
        attributesMap.put(SimpleLDAPAuthenticationManager.SEARCH_FILTER, SEARCH_FILTER_VALUE);
        attributesMap.put(SimpleLDAPAuthenticationManager.CONTEXT,
                          Collections.singletonMap(AUTHENTICATION_CACHE_MAX_SIZE, "0"));
        final SimpleLDAPAuthenticationManager<?> authProvider =
                (SimpleLDAPAuthenticationManager<?>) _broker.getObjectFactory()
                .create(AuthenticationProvider.class, attributesMap, _broker);
        _authenticationProviders.add(authProvider);
        return authProvider;
    }

    @Test(expected = IllegalArgumentException.class)
    public void failToCreateCompositeAuthenticationManager()
    {
        createCompositeAuthenticationManager();
    }

    @Test()
    public void authenticateAgainstPlainAuthenticationProvider() throws Exception
    {

        final PlainAuthenticationProvider plainAuthenticationProvider = createPlainAuthenticationProvider();
        plainAuthenticationProvider.createUser(USERNAME, PASSWORD, Collections.emptyMap());
        final CompositeUsernamePasswordAuthenticationManager<?> authManager = createCompositeAuthenticationManager(
                plainAuthenticationProvider
        );

        final AuthenticationResult result = authManager.authenticate(USERNAME, PASSWORD);
        assertEquals("Unexpected result status", SUCCESS, result.getStatus());
        assertEquals("Unexpected result principal", USERNAME, result.getMainPrincipal().getName());

        // authenticate via SASL PLAIN
        final String RESPONSE = String.format("\0%s\0%s", USERNAME, PASSWORD);
        final SaslNegotiator plainSaslNegotiator = authManager.createSaslNegotiator("PLAIN", null, null);
        final AuthenticationResult plainAuthResult = plainSaslNegotiator.handleResponse(RESPONSE.getBytes(US_ASCII));
        assertEquals("Unexpected result status",
                     SUCCESS,
                     plainAuthResult.getStatus());

        // authenticate via SASL CRAM-MD5
        saslCramMd(
            CramMd5Negotiator.MECHANISM,
            authManager.createSaslNegotiator(
                CramMd5Negotiator.MECHANISM,
                CRAM_MD_SASL_SETTINGS,
                null
            ),
            USERNAME,
            PASSWORD
        );

        // authenticate via SASL SCRAM-SHA-1
        saslScramSha(
            ScramSHA1AuthenticationManager.MECHANISM,
            authManager.createSaslNegotiator(ScramSHA1AuthenticationManager.MECHANISM, null, null),
            USERNAME,
            PASSWORD
        );

        // authenticate via SASL SCRAM-SHA-256
        saslScramSha(
            ScramSHA256AuthenticationManager.MECHANISM,
            authManager.createSaslNegotiator(ScramSHA256AuthenticationManager.MECHANISM, null, null),
            USERNAME,
            PASSWORD
        );
    }

    @Test()
    public void authenticateAgainstMD5AuthenticationProvider() throws Exception
    {

        final PlainAuthenticationProvider plainAuthenticationProvider = createPlainAuthenticationProvider();
        final MD5AuthenticationProvider md5AuthenticationProvider = createMD5AuthenticationProvider();
        md5AuthenticationProvider.createUser(USERNAME, PASSWORD, Collections.emptyMap());
        final CompositeUsernamePasswordAuthenticationManager<?> authManager = createCompositeAuthenticationManager(
                plainAuthenticationProvider, md5AuthenticationProvider
        );
        AuthenticationResult result = authManager.authenticate(USERNAME, PASSWORD);
        assertEquals("Unexpected result status", SUCCESS, result.getStatus());
        assertEquals("Unexpected result principal", USERNAME, result.getMainPrincipal().getName());

        // authenticate via SASL PLAIN
        final String RESPONSE = String.format("\0%s\0%s", USERNAME, PASSWORD);
        final SaslNegotiator plainSaslNegotiator = authManager.createSaslNegotiator("PLAIN", null, null);
        final AuthenticationResult plainAuthResult = plainSaslNegotiator.handleResponse(RESPONSE.getBytes(US_ASCII));
        assertEquals("Unexpected result status", SUCCESS, plainAuthResult.getStatus());

        // authenticate via SASL CRAM-MD5-HASHED
        saslCramMd(
            CramMd5Base64HashedNegotiator.MECHANISM,
            authManager.createSaslNegotiator(
                CramMd5Base64HashedNegotiator.MECHANISM,
                CRAM_MD_SASL_SETTINGS,
               null
            ),
            USERNAME,
            PASSWORD
        );

        // authenticate via SASL CRAM-MD5-HEX
        saslCramMd(
            CramMd5Base64HexNegotiator.MECHANISM,
            authManager.createSaslNegotiator(
                CramMd5Base64HexNegotiator.MECHANISM,
                CRAM_MD_SASL_SETTINGS,
               null
            ),
            USERNAME,
            PASSWORD
        );
    }

    @Test()
    public void authenticateAgainstScramSHA1AuthenticationManager() throws Exception
    {

        final ScramSHA1AuthenticationManager scramSHA1AuthenticationManager =
                createScramSHA1AuthenticationManager();
        scramSHA1AuthenticationManager.createUser(USERNAME, PASSWORD, Collections.emptyMap());
        final CompositeUsernamePasswordAuthenticationManager<?> authManager = createCompositeAuthenticationManager(
                scramSHA1AuthenticationManager
        );
        AuthenticationResult result = authManager.authenticate(USERNAME, PASSWORD);
        assertEquals("Unexpected result status", SUCCESS, result.getStatus());
        assertEquals("Unexpected result principal", USERNAME, result.getMainPrincipal().getName());

        saslScramSha(
                ScramSHA1AuthenticationManager.MECHANISM,
                authManager.createSaslNegotiator(ScramSHA1AuthenticationManager.MECHANISM, null, null),
                USERNAME,
                PASSWORD
        );
    }

    @Test()
    public void authenticateAgainstScramSHA256AuthenticationManager() throws Exception
    {

        final ScramSHA256AuthenticationManager scramSHA256AuthenticationManager =
                createScramSHA256AuthenticationManager();
        scramSHA256AuthenticationManager.createUser(USERNAME, PASSWORD, Collections.emptyMap());
        final CompositeUsernamePasswordAuthenticationManager<?> authManager = createCompositeAuthenticationManager(
                scramSHA256AuthenticationManager
                                                                                                                  );
        AuthenticationResult result = authManager.authenticate(USERNAME, PASSWORD);
        assertEquals("Unexpected result status", SUCCESS, result.getStatus());
        assertEquals("Unexpected result principal", USERNAME, result.getMainPrincipal().getName());

        saslScramSha(
            ScramSHA256AuthenticationManager.MECHANISM,
            authManager.createSaslNegotiator(ScramSHA256AuthenticationManager.MECHANISM, null, null),
            USERNAME,
            PASSWORD
        );
    }

    @Test()
    public void authenticateAgainstSimpleLDAPAuthenticationManager()
    {
        final String LDAP_USERNAME = "test1";

        final SimpleLDAPAuthenticationManager<?> simpleLDAPAuthenticationManager =
                createSimpleLDAPAuthenticationManager();

        final CompositeUsernamePasswordAuthenticationManager<?> authManager = createCompositeAuthenticationManager(
                simpleLDAPAuthenticationManager
        );

        AuthenticationResult result = authManager.authenticate(LDAP_USERNAME, PASSWORD);
        assertEquals("Unexpected result status", SUCCESS, result.getStatus());
        assertEquals("Unexpected result principal", "cn=integration-test1,ou=users,dc=qpid,dc=org", result.getMainPrincipal().getName());

        // authenticate via SASL PLAIN
        final String RESPONSE = String.format("\0%s\0%s", LDAP_USERNAME, PASSWORD);
        final SaslNegotiator plainSaslNegotiator = authManager.createSaslNegotiator("PLAIN", null, null);
        final AuthenticationResult plainAuthResult = plainSaslNegotiator.handleResponse(RESPONSE.getBytes(US_ASCII));
        assertEquals("Unexpected result status", SUCCESS, plainAuthResult.getStatus());
    }

    @Test()
    public void authenticateAgainstPlainAndMd5AndSimpleLdap() throws Exception
    {
        final String MD5_USERNAME = "user2";
        final String MD5_PASSWORD = "password2";
        final String LDAP_USERNAME = "test1";
        final String LDAP_PASSWORD = "password1";

        final PlainAuthenticationProvider plainAuthenticationProvider = createPlainAuthenticationProvider();
        plainAuthenticationProvider.createUser(USERNAME, PASSWORD, Collections.emptyMap());

        final MD5AuthenticationProvider md5AuthenticationProvider = createMD5AuthenticationProvider();
        md5AuthenticationProvider.createUser(MD5_USERNAME, MD5_PASSWORD, Collections.emptyMap());

        final SimpleLDAPAuthenticationManager<?> simpleLDAPAuthenticationManager =
                createSimpleLDAPAuthenticationManager();

        final CompositeUsernamePasswordAuthenticationManager<?> authManager = createCompositeAuthenticationManager(
                plainAuthenticationProvider, md5AuthenticationProvider, simpleLDAPAuthenticationManager
        );

        // authenticate against PlainAuthenticationProvider via SASL PLAIN
        String RESPONSE = String.format("\0%s\0%s", USERNAME, PASSWORD);
        final SaslNegotiator plainSaslNegotiator = authManager.createSaslNegotiator("PLAIN", null, null);
        final AuthenticationResult plainAuthResult = plainSaslNegotiator.handleResponse(RESPONSE.getBytes(US_ASCII));
        assertEquals("Unexpected result status", SUCCESS, plainAuthResult.getStatus());
        assertEquals(
                "Unexpected result principal",
                USERNAME,
                plainAuthResult.getMainPrincipal().getName()
        );

        // authenticate against PlainAuthenticationProvider via SASL CRAM-MD5
        saslCramMd(
                CramMd5Negotiator.MECHANISM,
                authManager.createSaslNegotiator(CramMd5Negotiator.MECHANISM, CRAM_MD_SASL_SETTINGS, null),
                USERNAME,
                PASSWORD
        );

        // authenticate against PlainAuthenticationProvider via SASL SCRAM-SHA-1
        saslScramSha(
                ScramSHA1AuthenticationManager.MECHANISM,
                authManager.createSaslNegotiator(ScramSHA1AuthenticationManager.MECHANISM, null, null),
                USERNAME,
                PASSWORD
        );

        // authenticate against PlainAuthenticationProvider via SASL SCRAM-SHA-256
        saslScramSha(
                ScramSHA256AuthenticationManager.MECHANISM,
                authManager.createSaslNegotiator(ScramSHA256AuthenticationManager.MECHANISM, null, null),
                USERNAME,
                PASSWORD
        );

        // authenticate against MD5AuthenticationProvider via SASL PLAIN
        RESPONSE = String.format("\0%s\0%s", MD5_USERNAME, MD5_PASSWORD);
        final SaslNegotiator md5SaslNegotiator = authManager.createSaslNegotiator("PLAIN", null, null);
        final AuthenticationResult md5AuthResult = md5SaslNegotiator.handleResponse(RESPONSE.getBytes(US_ASCII));
        assertEquals("Unexpected result status", SUCCESS, md5AuthResult.getStatus());
        assertEquals(
                "Unexpected result principal",
                MD5_USERNAME,
                md5AuthResult.getMainPrincipal().getName()
        );

        // authenticate against MD5AuthenticationProvider via SASL CRAM-MD5-HASHED
        saslCramMd(
                CramMd5Base64HashedNegotiator.MECHANISM,
                authManager.createSaslNegotiator(CramMd5Base64HashedNegotiator.MECHANISM, CRAM_MD_SASL_SETTINGS, null),
                MD5_USERNAME,
                MD5_PASSWORD
        );

        // authenticate against MD5AuthenticationProvider via SASL CRAM-MD5-HEX
        saslCramMd(
                CramMd5Base64HexNegotiator.MECHANISM,
                authManager.createSaslNegotiator(CramMd5Base64HexNegotiator.MECHANISM, CRAM_MD_SASL_SETTINGS,null),
                MD5_USERNAME,
                MD5_PASSWORD
        );

        // authenticate against SimpleLdapAuthenticationProvider via SASL PLAIN
        RESPONSE = String.format("\0%s\0%s", LDAP_USERNAME, LDAP_PASSWORD);
        final SaslNegotiator ldapSaslNegotiator = authManager.createSaslNegotiator("PLAIN", null, null);
        final AuthenticationResult ldapAuthResult = ldapSaslNegotiator.handleResponse(RESPONSE.getBytes(US_ASCII));
        assertEquals("Unexpected result status", SUCCESS, ldapAuthResult.getStatus());
        assertEquals(
                "Unexpected result principal",
                "cn=integration-test1,ou=users,dc=qpid,dc=org",
                ldapAuthResult.getMainPrincipal().getName()
        );
    }

    @Test()
    public void authenticateAgainstPlainAndSha256AndSimpleLdap() throws Exception
    {
        final String SHA256_USERNAME = "user2";
        final String SHA256_PASSWORD = "password2";
        final String LDAP_USERNAME = "test1";
        final String LDAP_PASSWORD = "password1";

        final PlainAuthenticationProvider plainAuthenticationProvider = createPlainAuthenticationProvider();
        plainAuthenticationProvider.createUser(USERNAME, PASSWORD, Collections.emptyMap());

        final ScramSHA256AuthenticationManager sha256AuthenticationProvider = createScramSHA256AuthenticationManager();
        sha256AuthenticationProvider.createUser(SHA256_USERNAME, SHA256_PASSWORD, Collections.emptyMap());

        final SimpleLDAPAuthenticationManager<?> simpleLDAPAuthenticationManager =
                createSimpleLDAPAuthenticationManager();

        final CompositeUsernamePasswordAuthenticationManager<?> authManager = createCompositeAuthenticationManager(
                plainAuthenticationProvider, sha256AuthenticationProvider, simpleLDAPAuthenticationManager
        );

        // authenticate against PlainAuthenticationProvider via SASL PLAIN
        String RESPONSE = String.format("\0%s\0%s", USERNAME, PASSWORD);
        final SaslNegotiator plainSaslNegotiator = authManager.createSaslNegotiator("PLAIN", null, null);
        final AuthenticationResult plainAuthResult = plainSaslNegotiator.handleResponse(RESPONSE.getBytes(US_ASCII));
        assertEquals("Unexpected result status", SUCCESS, plainAuthResult.getStatus());
        assertEquals("Unexpected result principal", USERNAME, plainAuthResult.getMainPrincipal().getName());

        // authenticate against PlainAuthenticationProvider via SASL CRAM-MD5
        saslCramMd(
                CramMd5Negotiator.MECHANISM,
                authManager.createSaslNegotiator(CramMd5Negotiator.MECHANISM, CRAM_MD_SASL_SETTINGS, null),
                USERNAME, PASSWORD);

        // authenticate against PlainAuthenticationProvider via SASL SCRAM-SHA-1
        saslScramSha(
                ScramSHA1AuthenticationManager.MECHANISM,
                authManager.createSaslNegotiator(ScramSHA1AuthenticationManager.MECHANISM, null, null),
                USERNAME, PASSWORD);

        // authenticate against PlainAuthenticationProvider via SASL SCRAM-SHA-256
        saslScramSha(
                ScramSHA256AuthenticationManager.MECHANISM,
                authManager.createSaslNegotiator(ScramSHA256AuthenticationManager.MECHANISM, null, null),
                USERNAME, PASSWORD);

        // authenticate against ScramSHA256AuthenticationManager via SASL SCRAM-SHA-256
        saslScramSha(
                ScramSHA256AuthenticationManager.MECHANISM,
                authManager.createSaslNegotiator(ScramSHA256AuthenticationManager.MECHANISM, null, null),
                SHA256_USERNAME, SHA256_PASSWORD);

        // authenticate against SimpleLdapAuthenticationProvider via SASL PLAIN
        RESPONSE = String.format("\0%s\0%s", LDAP_USERNAME, LDAP_PASSWORD);
        final SaslNegotiator ldapSaslNegotiator = authManager.createSaslNegotiator("PLAIN", null, null);
        final AuthenticationResult ldapAuthResult = ldapSaslNegotiator.handleResponse(RESPONSE.getBytes(US_ASCII));
        assertEquals("Unexpected result status", SUCCESS, ldapAuthResult.getStatus());
        assertEquals(
                "Unexpected result principal",
                "cn=integration-test1,ou=users,dc=qpid,dc=org",
                ldapAuthResult.getMainPrincipal().getName()
        );
    }

    @Test()
    public void usernameCollision() throws Exception
    {
        final String PLAIN_PASSWORD = "password1";
        final String SHA256_PASSWORD = "password2";

        final PlainAuthenticationProvider plainAuthenticationProvider = createPlainAuthenticationProvider();
        plainAuthenticationProvider.createUser(USERNAME, PLAIN_PASSWORD, Collections.emptyMap());

        final ScramSHA256AuthenticationManager sha256AuthenticationProvider = createScramSHA256AuthenticationManager();
        sha256AuthenticationProvider.createUser(USERNAME, SHA256_PASSWORD, Collections.emptyMap());

        final CompositeUsernamePasswordAuthenticationManager<?> authManager = createCompositeAuthenticationManager(
                plainAuthenticationProvider, sha256AuthenticationProvider);

        // authenticate against PlainAuthenticationProvider via SASL SCRAM-SHA-256
        saslScramSha(
                ScramSHA256AuthenticationManager.MECHANISM,
                authManager.createSaslNegotiator(ScramSHA256AuthenticationManager.MECHANISM, null, null),
                USERNAME, PLAIN_PASSWORD);

        // authenticate against ScramSHA256AuthenticationManager via SASL SCRAM-SHA-256 (fails due username collision)
        saslScramShaInvalidCredentials(
                ScramSHA256AuthenticationManager.MECHANISM,
                authManager.createSaslNegotiator(ScramSHA256AuthenticationManager.MECHANISM, null, null),
                USERNAME, SHA256_PASSWORD);
    }

    @Test()
    public void differentUsersInScramSHA256AuthenticationManagers() throws Exception
    {
        final String SHA256_USERNAME2 = "user2";
        final String SHA256_PASSWORD2 = "password2";
        final String SHA256_USERNAME3 = "user3";
        final String SHA256_PASSWORD3 = "password4";

        final ScramSHA256AuthenticationManager sha256AuthenticationProvider1 = createScramSHA256AuthenticationManager("ScramSHA256AuthenticationManager1");
        sha256AuthenticationProvider1.createUser(USERNAME, PASSWORD, Collections.emptyMap());

        final ScramSHA256AuthenticationManager sha256AuthenticationProvider2 = createScramSHA256AuthenticationManager("ScramSHA256AuthenticationManager2");
        sha256AuthenticationProvider2.createUser(SHA256_USERNAME2, SHA256_PASSWORD2, Collections.emptyMap());

        final ScramSHA256AuthenticationManager sha256AuthenticationProvider3 = createScramSHA256AuthenticationManager("ScramSHA256AuthenticationManager3");
        sha256AuthenticationProvider3.createUser(SHA256_USERNAME3, SHA256_PASSWORD3, Collections.emptyMap());

        final CompositeUsernamePasswordAuthenticationManager<?> authManager = createCompositeAuthenticationManager(
                sha256AuthenticationProvider1, sha256AuthenticationProvider2, sha256AuthenticationProvider3);

        // authenticate against first ScramSHA256AuthenticationManager via SASL SCRAM-SHA-256
        saslScramSha(
                ScramSHA256AuthenticationManager.MECHANISM,
                authManager.createSaslNegotiator(ScramSHA256AuthenticationManager.MECHANISM, null, null),
                USERNAME, PASSWORD);

        // authenticate against second ScramSHA256AuthenticationManager via SASL SCRAM-SHA-256
        saslScramSha(
                ScramSHA256AuthenticationManager.MECHANISM,
                authManager.createSaslNegotiator(ScramSHA256AuthenticationManager.MECHANISM, null, null),
                SHA256_USERNAME2, SHA256_PASSWORD2);

        // authenticate against third ScramSHA256AuthenticationManager via SASL SCRAM-SHA-256
        saslScramSha(
                ScramSHA256AuthenticationManager.MECHANISM,
                authManager.createSaslNegotiator(ScramSHA256AuthenticationManager.MECHANISM, null, null),
                SHA256_USERNAME3, SHA256_PASSWORD3);

    }

    @Test()
    public void userNotFound() throws Exception
    {
        final String SHA1_USERNAME = "user2";
        final String SHA1_PASSWORD = "password2";
        final String NON_EXISTING_USERNAME = "test99";

        final ScramSHA256AuthenticationManager sha256AuthenticationProvider = createScramSHA256AuthenticationManager();
        sha256AuthenticationProvider.createUser(USERNAME, PASSWORD, Collections.emptyMap());

        final ScramSHA1AuthenticationManager sha1AuthenticationProvider = createScramSHA1AuthenticationManager();
        sha1AuthenticationProvider.createUser(SHA1_USERNAME, SHA1_PASSWORD, Collections.emptyMap());

        final SimpleLDAPAuthenticationManager<?> simpleLDAPAuthenticationManager =
                createSimpleLDAPAuthenticationManager();

        final CompositeUsernamePasswordAuthenticationManager<?> authManager = createCompositeAuthenticationManager(
                sha256AuthenticationProvider, sha1AuthenticationProvider, simpleLDAPAuthenticationManager);

        // authenticate against ScramSHA256AuthenticationManager via SASL SCRAM-SHA-256
        saslScramShaInvalidCredentials(
                ScramSHA256AuthenticationManager.MECHANISM,
                authManager.createSaslNegotiator(ScramSHA256AuthenticationManager.MECHANISM, null, null),
                NON_EXISTING_USERNAME, PASSWORD);

        // authenticate against ScramSHA256AuthenticationManager via SASL SCRAM-SHA-1
        saslScramShaInvalidCredentials(
                ScramSHA1AuthenticationManager.MECHANISM,
                authManager.createSaslNegotiator(ScramSHA1AuthenticationManager.MECHANISM, null, null),
                NON_EXISTING_USERNAME, SHA1_PASSWORD);

        // authenticate against SimpleLdapAuthenticationProvider via SASL PLAIN
        String RESPONSE = String.format("\0%s\0%s", NON_EXISTING_USERNAME, PASSWORD);
        final SaslNegotiator ldapSaslNegotiator = authManager.createSaslNegotiator("PLAIN", null, null);
        final AuthenticationResult ldapAuthResult = ldapSaslNegotiator.handleResponse(RESPONSE.getBytes(US_ASCII));
        assertEquals("Unexpected result status", ERROR, ldapAuthResult.getStatus());
    }

    @Test(expected = IllegalConfigurationException.class)
    public void nestedComposteUsernamePasswordAuthenticationManager()
    {
        final ScramSHA256AuthenticationManager sha256AuthenticationProvider = createScramSHA256AuthenticationManager();
        sha256AuthenticationProvider.createUser(USERNAME, PASSWORD, Collections.emptyMap());
        final CompositeUsernamePasswordAuthenticationManager<?> composite1 = createCompositeAuthenticationManager(sha256AuthenticationProvider);
        createCompositeAuthenticationManager(composite1);
    }

    @Test(expected = IllegalConfigurationException.class)
    public void duplicateDelegates()
    {
        final ScramSHA256AuthenticationManager sha256AuthenticationProvider = createScramSHA256AuthenticationManager();
        sha256AuthenticationProvider.createUser(USERNAME, PASSWORD, Collections.emptyMap());
        createCompositeAuthenticationManager(sha256AuthenticationProvider, sha256AuthenticationProvider);
    }

    private void saslCramMd(
        String mechanism,
        SaslNegotiator saslNegotiator,
        String username,
        String password
    ) throws Exception
    {

        final AuthenticationResult firstResult = saslNegotiator.handleResponse(new byte[0]);
        assertEquals(
                "Unexpected first result status",
                AuthenticationResult.AuthenticationStatus.CONTINUE,
                firstResult.getStatus()
        );

        byte[] responseBytes = SaslUtil.generateCramMD5ClientResponse(
                mechanism, username, password, firstResult.getChallenge()
        );

        final AuthenticationResult secondResult = saslNegotiator.handleResponse(responseBytes);

        assertEquals("Unexpected second result status",
                     SUCCESS,
                     secondResult.getStatus());
        assertNull("Unexpected second result challenge", secondResult.getChallenge());
        assertEquals("Unexpected second result main principal",
                     username,
                     secondResult.getMainPrincipal().getName());

        final AuthenticationResult thirdResult = saslNegotiator.handleResponse(new byte[0]);
        assertEquals("Unexpected third result status",
                     AuthenticationResult.AuthenticationStatus.ERROR,
                     thirdResult.getStatus());
    }

    private void saslScramSha(
        String mechanism,
        SaslNegotiator saslNegotiator,
        String username,
        String password
    ) throws Exception
    {

        final byte[] initialResponse = SaslHelper.createInitialResponse(username);

        final AuthenticationResult firstResult = saslNegotiator.handleResponse(initialResponse);
        assertEquals(
                "Unexpected first result status",
                AuthenticationResult.AuthenticationStatus.CONTINUE,
                firstResult.getStatus()
         );
        assertNotNull("Unexpected first result challenge", firstResult.getChallenge());

        final byte[] response = SaslHelper.calculateClientProof(
                firstResult.getChallenge(),
                ScramSHA256AuthenticationManager.MECHANISM.equals(mechanism)
                    ? ScramSHA256AuthenticationManager.HMAC_NAME : ScramSHA1AuthenticationManager.HMAC_NAME,
                ScramSHA256AuthenticationManager.MECHANISM.equals(mechanism)
                    ? ScramSHA256AuthenticationManager.DIGEST_NAME : ScramSHA1AuthenticationManager.DIGEST_NAME,
                password
        );

        final AuthenticationResult secondResult = saslNegotiator.handleResponse(response);
        assertEquals(
                "Unexpected second result status",
                SUCCESS,
                secondResult.getStatus()
        );
        assertNotNull("Unexpected second result challenge", secondResult.getChallenge());
        assertEquals(
                "Unexpected second result principal",
                username,
                secondResult.getMainPrincipal().getName()
        );

        final String serverFinalMessage = new String(secondResult.getChallenge(), SaslHelper.ASCII);
        final String[] parts = serverFinalMessage.split(",");
        if (!parts[0].startsWith("v="))
        {
            fail("Server final message did not contain verifier");
        }
        final byte[] serverSignature = Strings.decodeBase64(parts[0].substring(2));
        if (!Arrays.equals(SaslHelper._serverSignature, serverSignature))
        {
            fail("Server signature did not match");
        }

        final AuthenticationResult thirdResult = saslNegotiator.handleResponse(initialResponse);
        assertEquals("Unexpected result status after completion of negotiation",
                     AuthenticationResult.AuthenticationStatus.ERROR,
                     thirdResult.getStatus());
        assertNull("Unexpected principal after completion of negotiation", thirdResult.getMainPrincipal());
    }

    private void saslScramShaInvalidCredentials(
            String mechanism,
            SaslNegotiator saslNegotiator,
            String username,
            String password) throws Exception
    {

        final byte[] initialResponse = SaslHelper.createInitialResponse(username);

        final AuthenticationResult firstResult = saslNegotiator.handleResponse(initialResponse);
        assertEquals(
                "Unexpected first result status",
                AuthenticationResult.AuthenticationStatus.CONTINUE,
                firstResult.getStatus()
                    );
        assertNotNull("Unexpected first result challenge", firstResult.getChallenge());

        final byte[] response = SaslHelper.calculateClientProof(
                firstResult.getChallenge(),
                ScramSHA256AuthenticationManager.MECHANISM.equals(mechanism)
                        ? ScramSHA256AuthenticationManager.HMAC_NAME : ScramSHA1AuthenticationManager.HMAC_NAME,
                ScramSHA256AuthenticationManager.MECHANISM.equals(mechanism)
                        ? ScramSHA256AuthenticationManager.DIGEST_NAME : ScramSHA1AuthenticationManager.DIGEST_NAME,
                password
                                                               );

        final AuthenticationResult secondResult = saslNegotiator.handleResponse(response);
        assertEquals(
                "Unexpected second result status",
                ERROR,
                secondResult.getStatus());
        assertNull("Unexpected second result challenge", secondResult.getChallenge());

    }

    private static class SaslHelper
    {

        private static final String GS2_HEADER = "n,,";
        private static final Charset ASCII = US_ASCII;
        private static String _clientFirstMessageBare;
        private static String _clientNonce;
        private static byte[] _serverSignature;

        private static byte[] calculateClientProof(
                final byte[] challenge,
                String hmacName,
                String digestName,
                String userPassword
        ) throws Exception
        {

            final String serverFirstMessage = new String(challenge, ASCII);
            final String[] parts = serverFirstMessage.split(",");
            if (parts.length < 3)
            {
                fail("Server challenge '" + serverFirstMessage + "' cannot be parsed");
            }
            else if (parts[0].startsWith("m="))
            {
                fail("Server requires mandatory extension which is not supported: " + parts[0]);
            }
            else if (!parts[0].startsWith("r="))
            {
                fail("Server challenge '" + serverFirstMessage + "' cannot be parsed, cannot find nonce");
            }
            final String nonce = parts[0].substring(2);
            if (!nonce.startsWith(_clientNonce))
            {
                fail("Server challenge did not use correct client nonce");
            }
            if (!parts[1].startsWith("s="))
            {
                fail("Server challenge '" + serverFirstMessage + "' cannot be parsed, cannot find salt");
            }
            final byte[] salt = Strings.decodeBase64(parts[1].substring(2));
            if (!parts[2].startsWith("i="))
            {
                fail("Server challenge '" + serverFirstMessage + "' cannot be parsed, cannot find iteration count");
            }
            final int _iterationCount = Integer.parseInt(parts[2].substring(2));
            if (_iterationCount <= 0)
            {
                fail("Iteration count " + _iterationCount + " is not a positive integer");
            }
            final byte[] passwordBytes = saslPrep(userPassword).getBytes(StandardCharsets.UTF_8);
            final byte[] saltedPassword = generateSaltedPassword(passwordBytes, hmacName, _iterationCount, salt);

            final String clientFinalMessageWithoutProof =
                    "c=" + Base64.getEncoder().encodeToString(GS2_HEADER.getBytes(ASCII))
                    + ",r=" + nonce;

            final String authMessage =
                    _clientFirstMessageBare + "," + serverFirstMessage + "," + clientFinalMessageWithoutProof;
            final byte[] clientKey = computeHmac(saltedPassword, "Client Key", hmacName);
            final byte[] storedKey = MessageDigest.getInstance(digestName).digest(clientKey);
            final byte[] clientSignature = computeHmac(storedKey, authMessage, hmacName);
            final byte[] clientProof = clientKey.clone();
            for (int i = 0; i < clientProof.length; i++)
            {
                clientProof[i] ^= clientSignature[i];
            }
            final byte[] serverKey = computeHmac(saltedPassword, "Server Key", hmacName);
            _serverSignature = computeHmac(serverKey, authMessage, hmacName);
            final String finalMessageWithProof = clientFinalMessageWithoutProof
                                                 + ",p=" + Base64.getEncoder().encodeToString(clientProof);
            return finalMessageWithProof.getBytes();
        }

        private static byte[] computeHmac(final byte[] key, final String string, String hmacName)
                throws Exception
        {
            final Mac mac = createHmac(key, hmacName);
            mac.update(string.getBytes(ASCII));
            return mac.doFinal();
        }

        private static byte[] generateSaltedPassword(
                final byte[] passwordBytes,
                String hmacName,
                final int iterationCount,
                final byte[] salt
        ) throws Exception
        {
            final Mac mac = createHmac(passwordBytes, hmacName);
            mac.update(salt);
            mac.update(new byte[]{0, 0, 0, 1});
            final byte[] result = mac.doFinal();

            byte[] previous = null;
            for (int i = 1; i < iterationCount; i++)
            {
                mac.update(previous != null ? previous : result);
                previous = mac.doFinal();
                for (int x = 0; x < result.length; x++)
                {
                    result[x] ^= previous[x];
                }
            }

            return result;
        }

        private static Mac createHmac(final byte[] keyBytes, String hmacName) throws Exception
        {
            final SecretKeySpec key = new SecretKeySpec(keyBytes, hmacName);
            final Mac mac = Mac.getInstance(hmacName);
            mac.init(key);
            return mac;
        }

        private static String saslPrep(String name)
        {
            name = name.replace("=", "=3D");
            name = name.replace(",", "=2C");
            return name;
        }

        private static byte[] createInitialResponse(final String userName)
        {
            _clientFirstMessageBare = "n=" + saslPrep(userName) + ",r=" + _clientNonce;
            return (GS2_HEADER + _clientFirstMessageBare).getBytes(ASCII);
        }
    }

    private static final SaslSettings CRAM_MD_SASL_SETTINGS = new SaslSettings()
    {
        @Override
        public String getLocalFQDN()
        {
            return "example.com";
        }

        @Override
        public Principal getExternalPrincipal()
        {
            return null;
        }
    };
}
