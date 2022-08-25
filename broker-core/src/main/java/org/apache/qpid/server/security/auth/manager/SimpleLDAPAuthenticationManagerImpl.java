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

import static java.util.Collections.disjoint;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;

import java.security.GeneralSecurityException;
import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import javax.naming.AuthenticationException;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.CommonProperties;
import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Container;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.AuthenticationResult.AuthenticationStatus;
import org.apache.qpid.server.security.auth.UsernamePrincipal;
import org.apache.qpid.server.security.auth.manager.ldap.ThreadLocalLdapSslSocketFactory;
import org.apache.qpid.server.security.auth.sasl.SaslNegotiator;
import org.apache.qpid.server.security.auth.sasl.SaslSettings;
import org.apache.qpid.server.security.auth.sasl.plain.PlainNegotiator;
import org.apache.qpid.server.security.group.GroupPrincipal;
import org.apache.qpid.server.transport.network.security.ssl.SSLUtil;
import org.apache.qpid.server.util.CipherSuiteAndProtocolRestrictingSSLSocketFactory;
import org.apache.qpid.server.util.ParameterizedTypes;
import org.apache.qpid.server.util.ServerScopedRuntimeException;

/**
 * Simple LDAP authentication manager.
 * <p>
 * Supports username / password authentication.
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public class SimpleLDAPAuthenticationManagerImpl
        extends AbstractAuthenticationManager<SimpleLDAPAuthenticationManagerImpl>
        implements SimpleLDAPAuthenticationManager<SimpleLDAPAuthenticationManagerImpl>
{
    /** Logger */
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleLDAPAuthenticationManagerImpl.class);

    /** LDAP connectivity attributes (used to validate configuration changes) */
    private static final List<String> CONNECTIVITY_ATTRS = unmodifiableList(Arrays.asList(PROVIDER_URL,
                                                                                          PROVIDER_AUTH_URL,
                                                                                          SEARCH_CONTEXT,
                                                                                          LDAP_CONTEXT_FACTORY,
                                                                                          SEARCH_USERNAME,
                                                                                          SEARCH_PASSWORD,
                                                                                          TRUST_STORE,
                                                                                          LOGIN_CONFIG_SCOPE,
                                                                                          AUTHENTICATION_METHOD));

    /** Environment key to instruct {@link InitialDirContext} to override the socket factory. */
    private static final String JAVA_NAMING_LDAP_FACTORY_SOCKET = "java.naming.ldap.factory.socket";

    /** LDAP provider URL (used for search) */
    @ManagedAttributeField
    private String _providerUrl;

    /** LDAP provider URL (used for authentication) */
    @ManagedAttributeField
    private String _providerAuthUrl;

    /** LDAP search context */
    @ManagedAttributeField
    private String _searchContext;

    /** LDAP search filter */
    @ManagedAttributeField
    private String _searchFilter;

    /** LDAP context factory */
    @ManagedAttributeField
    private String _ldapContextFactory;

    /**
     * Trust store - typically used when the Directory has been secured with a certificate signed by a
     * private CA (or self-signed certificate).
     */
    @ManagedAttributeField
    private TrustStore<?> _trustStore;

    /** Flag defining whether LDAP bind should be done without search or not */
    @ManagedAttributeField
    private boolean _bindWithoutSearch;

    /** LDAP username used for search */
    @ManagedAttributeField
    private String _searchUsername;

    /** LDAP password used for search */
    @ManagedAttributeField
    private String _searchPassword;

    /** LDAP group attribute name */
    @ManagedAttributeField
    private String _groupAttributeName;

    /** LDAP group search context */
    @ManagedAttributeField
    private String _groupSearchContext;

    /** LDAP group search filter */
    @ManagedAttributeField
    private String _groupSearchFilter;

    /** LDAP group subtree search scope */
    @ManagedAttributeField
    private boolean _groupSubtreeSearchScope;

    /** LDAP authentication method */
    @ManagedAttributeField
    private LdapAuthenticationMethod _authenticationMethod;

    /** Config login scope */
    @ManagedAttributeField
    private String _loginConfigScope;

    /** TLS protocol allow list */
    private List<String> _tlsProtocolAllowList;

    /** TLS protocol deny list */
    private List<String> _tlsProtocolDenyList;

    /** TLS cipher suite allow list */
    private List<String> _tlsCipherSuiteAllowList;

    /** TLS cipher suite deny list */
    private List<String> _tlsCipherSuiteDenyList;

    /** Authentication result cacher */
    private AuthenticationResultCacher _authenticationResultCacher;

    /**
     * Constructor creates configured object
     *
     * @param attributes Attributes
     * @param container  Parent container
     */
    @ManagedObjectFactoryConstructor
    protected SimpleLDAPAuthenticationManagerImpl(final Map<String, Object> attributes, final Container<?> container)
    {
        super(attributes, container);
    }

    /** Validates LDAP connectivity on creation */
    @Override
    protected void validateOnCreate()
    {
        super.validateOnCreate();
        validateInitialDirContext(this);
    }

    /**
     * Validate changes
     *
     * @param proxyForValidation ConfiguredObject
     * @param changedAttributes  Attribute names
     */
    @Override
    protected void validateChange(final ConfiguredObject<?> proxyForValidation, final Set<String> changedAttributes)
    {
        super.validateChange(proxyForValidation, changedAttributes);

        if (!disjoint(changedAttributes, CONNECTIVITY_ATTRS))
        {
            final SimpleLDAPAuthenticationManager<?> changed = (SimpleLDAPAuthenticationManager<?>) proxyForValidation;
            validateInitialDirContext(changed);
        }
    }

    /** Retrieves protocol / cipher allow and deny lists from context. Creates authentication result cacher. */
    @Override
    protected void onOpen()
    {
        super.onOpen();

        _tlsProtocolAllowList = getContextValue(List.class,
                                                ParameterizedTypes.LIST_OF_STRINGS,
                                                CommonProperties.QPID_SECURITY_TLS_PROTOCOL_ALLOW_LIST);
        _tlsProtocolDenyList = getContextValue(List.class,
                                               ParameterizedTypes.LIST_OF_STRINGS,
                                               CommonProperties.QPID_SECURITY_TLS_PROTOCOL_DENY_LIST);
        _tlsCipherSuiteAllowList = getContextValue(List.class,
                                                   ParameterizedTypes.LIST_OF_STRINGS,
                                                   CommonProperties.QPID_SECURITY_TLS_CIPHER_SUITE_ALLOW_LIST);
        _tlsCipherSuiteDenyList = getContextValue(List.class,
                                                  ParameterizedTypes.LIST_OF_STRINGS,
                                                  CommonProperties.QPID_SECURITY_TLS_CIPHER_SUITE_DENY_LIST);

        Integer cacheMaxSize = getContextValue(Integer.class, AUTHENTICATION_CACHE_MAX_SIZE);
        Long cacheExpirationTime = getContextValue(Long.class, AUTHENTICATION_CACHE_EXPIRATION_TIME);
        Integer cacheIterationCount = getContextValue(Integer.class, AUTHENTICATION_CACHE_ITERATION_COUNT);
        if (cacheMaxSize == null || cacheMaxSize <= 0 ||
                cacheExpirationTime == null || cacheExpirationTime <= 0 ||
                cacheIterationCount == null || cacheIterationCount < 0)
        {
            LOGGER.debug("disabling authentication result caching");
            cacheMaxSize = 0;
            cacheExpirationTime = 1L;
            cacheIterationCount = 0;
        }
        _authenticationResultCacher =
                new AuthenticationResultCacher(cacheMaxSize, cacheExpirationTime, cacheIterationCount);
    }

    @Override
    public String getProviderUrl()
    {
        return _providerUrl;
    }

    @Override
    public String getProviderAuthUrl()
    {
        return _providerAuthUrl;
    }

    @Override
    public String getSearchContext()
    {
        return _searchContext;
    }

    @Override
    public String getSearchFilter()
    {
        return _searchFilter;
    }

    @Override
    public String getLdapContextFactory()
    {
        return _ldapContextFactory;
    }

    @Override
    public TrustStore<?> getTrustStore()
    {
        return _trustStore;
    }

    @Override
    public String getSearchUsername()
    {
        return _searchUsername;
    }

    @Override
    public String getSearchPassword()
    {
        return _searchPassword;
    }

    @Override
    public String getGroupAttributeName()
    {
        return _groupAttributeName;
    }

    @Override
    public String getGroupSearchContext()
    {
        return _groupSearchContext;
    }

    @Override
    public String getGroupSearchFilter()
    {
        return _groupSearchFilter;
    }

    @Override
    public boolean isGroupSubtreeSearchScope()
    {
        return _groupSubtreeSearchScope;
    }

    @Override
    public LdapAuthenticationMethod getAuthenticationMethod()
    {
        return _authenticationMethod;
    }

    @Override
    public String getLoginConfigScope()
    {
        return _loginConfigScope;
    }

    @Override
    public List<String> getMechanisms()
    {
        return singletonList(PlainNegotiator.MECHANISM);
    }

    /**
     * Creates SASL negotiator based on available options
     *
     * @param mechanism    Mechanism name
     * @param saslSettings SaslSettings
     * @param addressSpace NamedAddressSpace
     * @return SaslNegotiator
     */
    @Override
    public SaslNegotiator createSaslNegotiator(final String mechanism,
                                               final SaslSettings saslSettings,
                                               final NamedAddressSpace addressSpace)
    {
        return PlainNegotiator.MECHANISM.equals(mechanism) ? new PlainNegotiator(this) : null;
    }

    /**
     * Authenticates username / password against LDAP
     *
     * @param username username
     * @param password password
     * @return AuthenticationResult
     */
    @Override
    public AuthenticationResult authenticate(final String username, final String password)
    {
        return getOrLoadAuthenticationResult(username, password);
    }

    /**
     * Authenticates username / password against LDAP.
     * <p>
     * Tries to retrieve authentication result from cache, if result is absent, performs authentication against LDAP.
     *
     * @param userId   userId
     * @param password password
     * @return AuthenticationResult
     */
    private AuthenticationResult getOrLoadAuthenticationResult(final String userId, final String password)
    {
        return _authenticationResultCacher.getOrLoad(new String[]{userId, password},
                                                     () -> doLDAPNameAuthentication(userId, password));
    }

    /**
     * Authenticates username / password against LDAP.
     *
     * @param userId   userId
     * @param password password
     * @return AuthenticationResult
     */
    private AuthenticationResult doLDAPNameAuthentication(final String userId, final String password)
    {
        Subject gssapiIdentity = null;
        if (LdapAuthenticationMethod.GSSAPI.equals(getAuthenticationMethod()))
        {
            try
            {
                gssapiIdentity = doGssApiLogin(getLoginConfigScope());
            }
            catch (LoginException e)
            {
                LOGGER.warn("JAAS Login failed", e);
                return new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR, e);
            }
        }

        final String name;
        try
        {
            name = getNameFromId(userId, gssapiIdentity);
        }
        catch (NamingException e)
        {
            LOGGER.warn("Retrieving LDAP name for user '{}' resulted in error.", userId, e);
            return new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR, e);
        }

        if (name == null)
        {
            //The search didn't return anything, class as not-authenticated before it NPEs below
            return new AuthenticationResult(AuthenticationStatus.ERROR);
        }

        final String providerAuthUrl = isSpecified(getProviderAuthUrl()) ? getProviderAuthUrl() : getProviderUrl();
        final Hashtable<String, Object> env = createInitialDirContextEnvironment(providerAuthUrl);

        env.put(Context.SECURITY_AUTHENTICATION, "simple");
        env.put(Context.SECURITY_PRINCIPAL, name);
        env.put(Context.SECURITY_CREDENTIALS, password);

        InitialDirContext ctx = null;
        try
        {
            ctx = createInitialDirContext(env, gssapiIdentity);

            Set<Principal> groups = Collections.emptySet();
            if (isGroupSearchRequired())
            {
                if (!providerAuthUrl.equals(getProviderUrl()))
                {
                    closeSafely(ctx);
                    ctx = createSearchInitialDirContext(gssapiIdentity);
                }
                groups = findGroups(ctx, name, gssapiIdentity);
            }

            //Authentication succeeded
            return new AuthenticationResult(new UsernamePrincipal(name, this), groups, null);
        }
        catch (AuthenticationException ae)
        {
            //Authentication failed
            return new AuthenticationResult(AuthenticationStatus.ERROR);
        }
        catch (NamingException e)
        {
            //Some other failure
            LOGGER.warn("LDAP authentication attempt for username '{}' resulted in error.", name, e);
            return new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR, e);
        }
        finally
        {
            closeSafely(ctx);
        }
    }

    private boolean isGroupSearchRequired()
    {
        if (isSpecified(getGroupAttributeName()))
        {
            return true;
        }
        return (isSpecified(getGroupSearchContext()) && isSpecified(getGroupSearchFilter()));
    }

    private boolean isSpecified(String value)
    {
        return (value != null && !value.isEmpty());
    }

    private Set<Principal> findGroups(final DirContext context, final String userDN, final Subject gssapiIdentity)
            throws NamingException
    {
        final Set<Principal> groupPrincipals = new HashSet<>();
        if (getGroupAttributeName() != null && !getGroupAttributeName().isEmpty())
        {
            final Attributes attributes = context.getAttributes(userDN, new String[]{getGroupAttributeName()});
            final NamingEnumeration<? extends Attribute> namingEnum = attributes.getAll();
            while (namingEnum.hasMore())
            {
                final Attribute attribute = namingEnum.next();
                if (attribute != null)
                {
                    final NamingEnumeration<?> attributeValues = attribute.getAll();
                    while (attributeValues.hasMore())
                    {
                        final Object attributeValue = attributeValues.next();
                        if (attributeValue != null)
                        {
                            final String groupDN = String.valueOf(attributeValue);
                            groupPrincipals.add(new GroupPrincipal(groupDN, this));
                        }
                    }
                }
            }
        }

        if (getGroupSearchContext() != null && !getGroupSearchContext().isEmpty() &&
                getGroupSearchFilter() != null && !getGroupSearchFilter().isEmpty())
        {
            final SearchControls searchControls = new SearchControls();
            searchControls.setReturningAttributes(new String[]{});
            searchControls.setSearchScope(isGroupSubtreeSearchScope()
                                                  ? SearchControls.SUBTREE_SCOPE
                                                  : SearchControls.ONELEVEL_SCOPE);
            final PrivilegedExceptionAction<NamingEnumeration<?>> search = () -> context.search(getGroupSearchContext(),
                                                                                                getGroupSearchFilter(),
                                                                                                new String[]{encode(
                                                                                                        userDN)},
                                                                                                searchControls);
            final NamingEnumeration<?> groupEnumeration = invokeContextOperationAs(gssapiIdentity, search);
            while (groupEnumeration.hasMore())
            {
                final SearchResult result = (SearchResult) groupEnumeration.next();
                final String groupDN = result.getNameInNamespace();
                groupPrincipals.add(new GroupPrincipal(groupDN, this));
            }
        }

        return groupPrincipals;
    }

    private String encode(final String value)
    {
        final StringBuilder encoded = new StringBuilder(value.length());
        final char[] chars = value.toCharArray();
        for (final char ch : chars)
        {
            switch (ch)
            {
                case '\0':
                    encoded.append("\\00");
                    break;
                case '(':
                    encoded.append("\\28");
                    break;
                case ')':
                    encoded.append("\\29");
                    break;
                case '*':
                    encoded.append("\\2a");
                    break;
                case '\\':
                    encoded.append("\\5c");
                    break;
                default:
                    encoded.append(ch);
                    break;
            }
        }
        return encoded.toString();
    }

    @SuppressWarnings("java:S1149")
    // Hashtable use if forced by JNDI API
    private Hashtable<String, Object> createInitialDirContextEnvironment(final String providerUrl)
    {
        final Hashtable<String, Object> env = new Hashtable<>();
        env.put(Context.INITIAL_CONTEXT_FACTORY, _ldapContextFactory);
        env.put(Context.PROVIDER_URL, providerUrl);
        return env;
    }

    /**
     * Creates InitialDirContext instance using supplied properties
     *
     * @param env            Hashtable containing properties for context creation
     * @param gssapiIdentity Subject
     * @return InitialDirContext
     * @throws NamingException exception throws during LDAP context creation
     */
    @SuppressWarnings("java:S1149")
    // Hashtable use if forced by JNDI API
    private InitialDirContext createInitialDirContext(final Hashtable<String, Object> env, final Subject gssapiIdentity)
            throws NamingException
    {
        final boolean isLdaps =
                String.valueOf(env.get(Context.PROVIDER_URL)).trim().toLowerCase(Locale.US).startsWith("ldaps:");
        if (isLdaps)
        {
            ThreadLocalLdapSslSocketFactory.set(createSslSocketFactory(_trustStore));
            env.put(JAVA_NAMING_LDAP_FACTORY_SOCKET, ThreadLocalLdapSslSocketFactory.class.getCanonicalName());
        }
        return invokeContextOperationAs(gssapiIdentity, () -> new InitialDirContext(env));
    }

    /**
     * Creates SSLSocketFactory
     *
     * @param trustStore TrustStore instance
     * @return SSLSocketFactory instance
     */
    private SSLSocketFactory createSslSocketFactory(final TrustStore<?> trustStore)
    {
        final SSLContext sslContext;
        try
        {
            sslContext = SSLUtil.tryGetSSLContext();
            sslContext.init(null, trustStore == null ? null : trustStore.getTrustManagers(), null);
        }
        catch (GeneralSecurityException e)
        {
            LOGGER.error("Exception creating SSLContext", e);
            if (trustStore != null)
            {
                throw new IllegalConfigurationException("Error creating SSLContext with trust store : "
                                                        + trustStore.getName(), e);
            }
            else
            {
                throw new IllegalConfigurationException("Error creating SSLContext (no trust store)", e);
            }
        }
        return new CipherSuiteAndProtocolRestrictingSSLSocketFactory(sslContext.getSocketFactory(),
                                                                     _tlsCipherSuiteAllowList,
                                                                     _tlsCipherSuiteDenyList,
                                                                     _tlsProtocolAllowList,
                                                                     _tlsProtocolDenyList);
    }

    @Override
    public String toString()
    {
        return "SimpleLDAPAuthenticationManagerImpl [id=" + getId() +
                ", name=" + getName() +
                ", providerUrl=" + _providerUrl +
                ", providerAuthUrl=" + _providerAuthUrl +
                ", searchContext=" + _searchContext +
                ", state=" + getState() +
                ", searchFilter=" + _searchFilter +
                ", ldapContextFactory=" + _ldapContextFactory +
                ", bindWithoutSearch=" + _bindWithoutSearch +
                ", trustStore=" + _trustStore +
                ", searchUsername=" + _searchUsername +
                ", loginConfigScope=" + _loginConfigScope +
                ", authenticationMethod=" + _authenticationMethod + "]";
    }

    /**
     * Validates LDAP InitialDirContext
     *
     * @param authenticationProvider SimpleLDAPAuthenticationManager instance
     */
    @SuppressWarnings("java:S1149")
    // Hashtable use if forced by JNDI API
    private void validateInitialDirContext(final SimpleLDAPAuthenticationManager<?> authenticationProvider)
    {
        final Hashtable<String, Object> env =
                createInitialDirContextEnvironment(authenticationProvider.getProviderUrl());
        setAuthenticationProperties(env,
                                    authenticationProvider.getSearchUsername(),
                                    authenticationProvider.getSearchPassword(),
                                    authenticationProvider.getAuthenticationMethod());

        InitialDirContext ctx = null;
        try
        {
            Subject gssapiIdentity = null;
            if (LdapAuthenticationMethod.GSSAPI.equals(authenticationProvider.getAuthenticationMethod()))
            {
                gssapiIdentity = doGssApiLogin(authenticationProvider.getLoginConfigScope());
            }
            ctx = createInitialDirContext(env, gssapiIdentity);
        }
        catch (NamingException e)
        {
            LOGGER.debug("Failed to establish connectivity to the ldap server for '{}'",
                         authenticationProvider.getProviderUrl(),
                         e);
            throw new IllegalConfigurationException("Failed to establish connectivity to the ldap server.", e);
        }
        catch (LoginException e)
        {
            LOGGER.debug("JAAS login failed ", e);
            throw new IllegalConfigurationException("JAAS login failed.", e);
        }
        finally
        {
            closeSafely(ctx);
        }
    }

    /**
     * Sets authentication parameters into hashtable containing properties for context creation
     *
     * @param env                  Hashtable containing properties for context creation
     * @param userName             LDAP username
     * @param password             LDAP password
     * @param authenticationMethod LDAP authentication method
     */
    @SuppressWarnings("java:S1149")
    // Hashtable use if forced by JNDI API
    private void setAuthenticationProperties(final Hashtable<String, Object> env,
                                             final String userName,
                                             final String password,
                                             final LdapAuthenticationMethod authenticationMethod)
    {
        if (LdapAuthenticationMethod.GSSAPI.equals(authenticationMethod))
        {
            env.put(Context.SECURITY_AUTHENTICATION, "GSSAPI");
        }
        else if (LdapAuthenticationMethod.SIMPLE.equals(authenticationMethod))
        {
            env.put(Context.SECURITY_AUTHENTICATION, "simple");
            if (userName != null)
            {
                env.put(Context.SECURITY_PRINCIPAL, userName);
            }
            if (password != null)
            {
                env.put(Context.SECURITY_CREDENTIALS, password);
            }
        }
        else
        {
            env.put(Context.SECURITY_AUTHENTICATION, "none");
        }
    }

    /**
     * Searches userId in LDAP returning full LDAP username
     *
     * @param id             UserId
     * @param gssapiIdentity Subject
     * @return Full username in LDAP
     * @throws NamingException exception thrown during LDAP search
     */
    private String getNameFromId(final String id, final Subject gssapiIdentity) throws NamingException
    {
        if (!isBindWithoutSearch())
        {
            final InitialDirContext ctx = createSearchInitialDirContext(gssapiIdentity);

            try
            {
                final SearchControls searchControls = new SearchControls();
                searchControls.setReturningAttributes(new String[]{});
                searchControls.setCountLimit(1L);
                searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);

                LOGGER.debug("Searching for '{}'", id);
                final NamingEnumeration<?> namingEnum = invokeContextOperationAs(gssapiIdentity,
                                                                                 (PrivilegedExceptionAction<NamingEnumeration<?>>) () -> ctx.search(
                                                                                         _searchContext,
                                                                                         _searchFilter,
                                                                                         new String[]{id},
                                                                                         searchControls));

                if (namingEnum.hasMore())
                {
                    final SearchResult result = (SearchResult) namingEnum.next();
                    final String name = result.getNameInNamespace();
                    LOGGER.debug("Found '{}' DN '{}'", id, name);
                    return name;
                }
                else
                {
                    LOGGER.debug("Not found '{}'", id);
                    return null;
                }
            }
            finally
            {
                closeSafely(ctx);
            }
        }
        else
        {
            return id;
        }
    }

    private <T> T invokeContextOperationAs(final Subject identity, final PrivilegedExceptionAction<T> action)
            throws NamingException
    {
        try
        {
            return Subject.doAs(identity, action);
        }
        catch (PrivilegedActionException e)
        {
            final Exception exception = e.getException();
            if (exception instanceof NamingException)
            {
                throw (NamingException) exception;
            }
            else if (exception instanceof RuntimeException)
            {
                throw (RuntimeException) exception;
            }
            else
            {
                throw new ServerScopedRuntimeException(exception);
            }
        }
    }

    private Subject doGssApiLogin(final String configScope) throws LoginException
    {
        final LoginContext loginContext = new LoginContext(configScope);
        loginContext.login();
        return loginContext.getSubject();
    }

    /**
     * Creates InitialDirContext instance for search
     *
     * @param gssapiIdentity Subject
     * @return InitialDirContext instance
     * @throws NamingException exception thrown during InitialDirContext creation
     */
    @SuppressWarnings("java:S1149")
    // Hashtable use if forced by JNDI API
    private InitialDirContext createSearchInitialDirContext(final Subject gssapiIdentity) throws NamingException
    {
        final Hashtable<String, Object> env = createInitialDirContextEnvironment(_providerUrl);
        setAuthenticationProperties(env, _searchUsername, _searchPassword, _authenticationMethod);
        return createInitialDirContext(env, gssapiIdentity);
    }

    @Override
    public boolean isBindWithoutSearch()
    {
        return _bindWithoutSearch;
    }

    @Override
    public List<String> getTlsProtocolAllowList()
    {
        return Collections.unmodifiableList(_tlsProtocolAllowList);
    }

    @Override
    public List<String> getTlsProtocolDenyList()
    {
        return Collections.unmodifiableList(_tlsProtocolDenyList);
    }

    @Override
    public List<String> getTlsCipherSuiteAllowList()
    {
        return Collections.unmodifiableList(_tlsCipherSuiteAllowList);
    }

    @Override
    public List<String> getTlsCipherSuiteDenyList()
    {
        return Collections.unmodifiableList(_tlsCipherSuiteDenyList);
    }

    /**
     * Closes InitialDirContext
     *
     * @param ctx InitialDirContext instance
     */
    private void closeSafely(InitialDirContext ctx)
    {
        try
        {
            if (ctx != null)
            {
                ctx.close();
            }
        }
        catch (NamingException e)
        {
            LOGGER.warn("Exception closing InitialDirContext", e);
        }
        ThreadLocalLdapSslSocketFactory.remove();
    }
}
