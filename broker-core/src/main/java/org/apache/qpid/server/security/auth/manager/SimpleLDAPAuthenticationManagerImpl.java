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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

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
import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import com.google.common.util.concurrent.ListenableFuture;
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
import org.apache.qpid.server.security.auth.manager.ldap.AbstractLDAPSSLSocketFactory;
import org.apache.qpid.server.security.auth.manager.ldap.LDAPSSLSocketFactoryGenerator;
import org.apache.qpid.server.security.auth.sasl.SaslNegotiator;
import org.apache.qpid.server.security.auth.sasl.SaslSettings;
import org.apache.qpid.server.security.auth.sasl.plain.PlainNegotiator;
import org.apache.qpid.server.security.group.GroupPrincipal;
import org.apache.qpid.server.transport.network.security.ssl.SSLUtil;
import org.apache.qpid.server.util.CipherSuiteAndProtocolRestrictingSSLSocketFactory;
import org.apache.qpid.server.util.ParameterizedTypes;
import org.apache.qpid.server.util.ServerScopedRuntimeException;
import org.apache.qpid.server.util.StringUtil;

public class SimpleLDAPAuthenticationManagerImpl
        extends AbstractAuthenticationManager<SimpleLDAPAuthenticationManagerImpl>
        implements SimpleLDAPAuthenticationManager<SimpleLDAPAuthenticationManagerImpl>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleLDAPAuthenticationManagerImpl.class);

    private static final List<String> CONNECTIVITY_ATTRS = unmodifiableList(Arrays.asList(PROVIDER_URL,
                                                                                          PROVIDER_AUTH_URL,
                                                                                          SEARCH_CONTEXT,
                                                                                          LDAP_CONTEXT_FACTORY,
                                                                                          SEARCH_USERNAME,
                                                                                          SEARCH_PASSWORD,
                                                                                          TRUST_STORE,
                                                                                          LOGIN_CONFIG_SCOPE,
                                                                                          AUTHENTICATION_METHOD));

    /**
     * Environment key to instruct {@link InitialDirContext} to override the socket factory.
     */
    private static final String JAVA_NAMING_LDAP_FACTORY_SOCKET = "java.naming.ldap.factory.socket";

    @ManagedAttributeField
    private String _providerUrl;
    @ManagedAttributeField
    private String _providerAuthUrl;
    @ManagedAttributeField
    private String _searchContext;
    @ManagedAttributeField
    private String _searchFilter;
    @ManagedAttributeField
    private String _ldapContextFactory;


    /**
     * Trust store - typically used when the Directory has been secured with a certificate signed by a
     * private CA (or self-signed certificate).
     */
    @ManagedAttributeField
    private TrustStore _trustStore;

    @ManagedAttributeField
    private boolean _bindWithoutSearch;

    @ManagedAttributeField
    private String _searchUsername;
    @ManagedAttributeField
    private String _searchPassword;

    @ManagedAttributeField
    private String _groupAttributeName;

    @ManagedAttributeField
    private String _groupSearchContext;

    @ManagedAttributeField
    private String _groupSearchFilter;

    @ManagedAttributeField
    private boolean _groupSubtreeSearchScope;

    @ManagedAttributeField
    private LdapAuthenticationMethod _authenticationMethod;

    @ManagedAttributeField
    private String _loginConfigScope;

    private List<String> _tlsProtocolWhiteList;
    private List<String>  _tlsProtocolBlackList;

    private List<String> _tlsCipherSuiteWhiteList;
    private List<String> _tlsCipherSuiteBlackList;

    private AuthenticationResultCacher _authenticationResultCacher;

    /**
     * Dynamically created SSL Socket Factory implementation.
     */
    private Class<? extends SocketFactory> _sslSocketFactoryOverrideClass;

    @ManagedObjectFactoryConstructor
    protected SimpleLDAPAuthenticationManagerImpl(final Map<String, Object> attributes, final Container<?> container)
    {
        super(attributes, container);
    }

    @Override
    protected void validateOnCreate()
    {
        super.validateOnCreate();
        validateInitialDirContext(this);
    }

    @Override
    protected void validateChange(final ConfiguredObject<?> proxyForValidation, final Set<String> changedAttributes)
    {
        super.validateChange(proxyForValidation, changedAttributes);

        if (!disjoint(changedAttributes, CONNECTIVITY_ATTRS))
        {
            SimpleLDAPAuthenticationManager changed = (SimpleLDAPAuthenticationManager) proxyForValidation;
            validateInitialDirContext(changed);
        }
    }

    @Override
    protected void onOpen()
    {
        super.onOpen();

        _tlsProtocolWhiteList = getContextValue(List.class, ParameterizedTypes.LIST_OF_STRINGS, CommonProperties.QPID_SECURITY_TLS_PROTOCOL_WHITE_LIST);
        _tlsProtocolBlackList = getContextValue(List.class, ParameterizedTypes.LIST_OF_STRINGS, CommonProperties.QPID_SECURITY_TLS_PROTOCOL_BLACK_LIST);
        _tlsCipherSuiteWhiteList = getContextValue(List.class, ParameterizedTypes.LIST_OF_STRINGS, CommonProperties.QPID_SECURITY_TLS_CIPHER_SUITE_WHITE_LIST);
        _tlsCipherSuiteBlackList = getContextValue(List.class, ParameterizedTypes.LIST_OF_STRINGS, CommonProperties.QPID_SECURITY_TLS_CIPHER_SUITE_BLACK_LIST);

        Integer cacheMaxSize = getContextValue(Integer.class, AUTHENTICATION_CACHE_MAX_SIZE);
        Long cacheExpirationTime = getContextValue(Long.class, AUTHENTICATION_CACHE_EXPIRATION_TIME);
        Integer cacheIterationCount = getContextValue(Integer.class, AUTHENTICATION_CACHE_ITERATION_COUNT);
        if (cacheMaxSize == null || cacheMaxSize <= 0
            || cacheExpirationTime == null || cacheExpirationTime <= 0
            || cacheIterationCount == null || cacheIterationCount < 0)
        {
            LOGGER.debug("disabling authentication result caching");
            cacheMaxSize = 0;
            cacheExpirationTime = 1L;
            cacheIterationCount = 0;
        }
        _authenticationResultCacher = new AuthenticationResultCacher(cacheMaxSize, cacheExpirationTime, cacheIterationCount);
    }

    @Override
    protected ListenableFuture<Void> activate()
    {
        _sslSocketFactoryOverrideClass = createSslSocketFactoryOverrideClass(_trustStore);
        return super.activate();
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
    public TrustStore getTrustStore()
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

    @Override
    public SaslNegotiator createSaslNegotiator(final String mechanism,
                                               final SaslSettings saslSettings,
                                               final NamedAddressSpace addressSpace)
    {
        if(PlainNegotiator.MECHANISM.equals(mechanism))
        {
            return new PlainNegotiator(this);
        }
        else
        {
            return null;
        }
    }

    @Override
    public AuthenticationResult authenticate(String username, String password)
    {
        return getOrLoadAuthenticationResult(username, password);
    }

    private AuthenticationResult getOrLoadAuthenticationResult(final String userId, final String password)
    {
        return _authenticationResultCacher.getOrLoad(new String[]{userId, password}, new Callable<AuthenticationResult>()
        {
            @Override
            public AuthenticationResult call()
            {
                return doLDAPNameAuthentication(userId, password);
            }
        });
    }

    private AuthenticationResult doLDAPNameAuthentication(String userId, String password)
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

        if(name == null)
        {
            //The search didn't return anything, class as not-authenticated before it NPEs below
            return new AuthenticationResult(AuthenticationStatus.ERROR);
        }

        String providerAuthUrl = isSpecified(getProviderAuthUrl()) ? getProviderAuthUrl() : getProviderUrl();
        Hashtable<String, Object> env = createInitialDirContextEnvironment(providerAuthUrl);

        env.put(Context.SECURITY_AUTHENTICATION, "simple");
        env.put(Context.SECURITY_PRINCIPAL, name);
        env.put(Context.SECURITY_CREDENTIALS, password);

        InitialDirContext ctx = null;
        try
        {
            ctx = createInitialDirContext(env, _sslSocketFactoryOverrideClass, gssapiIdentity);

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
        catch(AuthenticationException ae)
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
            if(ctx != null)
            {
                closeSafely(ctx);
            }
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

    private Set<Principal> findGroups(DirContext context, String userDN, final Subject gssapiIdentity)
            throws NamingException
    {
        Set<Principal> groupPrincipals = new HashSet<>();
        if (getGroupAttributeName() != null && !getGroupAttributeName().isEmpty())
        {
            Attributes attributes = context.getAttributes(userDN, new String[]{getGroupAttributeName()});
            NamingEnumeration<? extends Attribute> namingEnum = attributes.getAll();
            while (namingEnum.hasMore())
            {
                Attribute attribute = namingEnum.next();
                if (attribute != null)
                {
                    NamingEnumeration<?> attributeValues = attribute.getAll();
                    while (attributeValues.hasMore())
                    {
                        Object attributeValue = attributeValues.next();
                        if (attributeValue != null)
                        {
                            String groupDN = String.valueOf(attributeValue);
                            groupPrincipals.add(new GroupPrincipal(groupDN, this));
                        }
                    }
                }
            }
        }

        if (getGroupSearchContext() != null && !getGroupSearchContext().isEmpty() &&
                getGroupSearchFilter() != null && !getGroupSearchFilter().isEmpty())
        {
            SearchControls searchControls = new SearchControls();
            searchControls.setReturningAttributes(new String[]{});
            searchControls.setSearchScope(isGroupSubtreeSearchScope()
                                                  ? SearchControls.SUBTREE_SCOPE
                                                  : SearchControls.ONELEVEL_SCOPE);
            PrivilegedExceptionAction<NamingEnumeration<?>> search = () -> context.search(getGroupSearchContext(),
                                                                                          getGroupSearchFilter(),
                                                                                          new String[]{encode(userDN)},
                                                                                          searchControls);
            NamingEnumeration<?> groupEnumeration = invokeContextOperationAs(gssapiIdentity, search);
            while (groupEnumeration.hasMore())
            {
                SearchResult result = (SearchResult) groupEnumeration.next();
                String groupDN = result.getNameInNamespace();
                groupPrincipals.add(new GroupPrincipal(groupDN, this));
            }
        }

        return groupPrincipals;
    }

    private String encode(String value)
    {
        StringBuilder encoded = new StringBuilder(value.length());
        char[] chars = value.toCharArray();
        for (char ch : chars) {
            switch (ch) {
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

    private Hashtable<String, Object> createInitialDirContextEnvironment(String providerUrl)
    {
        Hashtable<String,Object> env = new Hashtable<>();
        env.put(Context.INITIAL_CONTEXT_FACTORY, _ldapContextFactory);
        env.put(Context.PROVIDER_URL, providerUrl);
        return env;
    }

    private InitialDirContext createInitialDirContext(final Hashtable<String, Object> env,
                                                      final Class<? extends SocketFactory> sslSocketFactoryOverrideClass,
                                                      final Subject gssapiIdentity) throws NamingException
    {
        ClassLoader existingContextClassLoader = null;

        boolean isLdaps = String.valueOf(env.get(Context.PROVIDER_URL)).trim().toLowerCase().startsWith("ldaps:");

        boolean revertContentClassLoader = false;
        try
        {
            if (isLdaps)
            {
                existingContextClassLoader = Thread.currentThread().getContextClassLoader();
                env.put(JAVA_NAMING_LDAP_FACTORY_SOCKET, sslSocketFactoryOverrideClass.getName());
                Thread.currentThread().setContextClassLoader(sslSocketFactoryOverrideClass.getClassLoader());
                revertContentClassLoader = true;
            }
            return invokeContextOperationAs(gssapiIdentity, () -> new InitialDirContext(env));
        }
        finally
        {
            if (revertContentClassLoader)
            {
                Thread.currentThread().setContextClassLoader(existingContextClassLoader);
            }
        }
    }

    private Class<? extends SocketFactory> createSslSocketFactoryOverrideClass(final TrustStore trustStore)
    {
        String managerName = String.format("%s_%s_%s", getName(), getId(), trustStore == null ? "none" : trustStore.getName());
        String clazzName = new StringUtil().createUniqueJavaName(managerName);
        SSLContext sslContext = null;
        try
        {
            sslContext = SSLUtil.tryGetSSLContext();
            sslContext.init(null,
                            trustStore == null ? null : trustStore.getTrustManagers(),
                            null);
        }
        catch (GeneralSecurityException e)
        {
            LOGGER.error("Exception creating SSLContext", e);
            if (trustStore != null)
            {
                throw new IllegalConfigurationException("Error creating SSLContext with trust store : " +
                                                        trustStore.getName() , e);
            }
            else
            {
                throw new IllegalConfigurationException("Error creating SSLContext (no trust store)", e);
            }
        }

        SSLSocketFactory sslSocketFactory = new CipherSuiteAndProtocolRestrictingSSLSocketFactory(sslContext.getSocketFactory(),
                                                                                                 _tlsCipherSuiteWhiteList,
                                                                                                 _tlsCipherSuiteBlackList,
                                                                                                 _tlsProtocolWhiteList,
                                                                                                 _tlsProtocolBlackList);
        Class<? extends AbstractLDAPSSLSocketFactory> clazz = LDAPSSLSocketFactoryGenerator.createSubClass(clazzName,
                                                                                                           sslSocketFactory);
        LOGGER.debug("Connection to Directory will use custom SSL socket factory : {}",  clazz);
        return clazz;
    }


    @Override
    public String toString()
    {
        return "SimpleLDAPAuthenticationManagerImpl [id=" + getId() + ", name=" + getName() +
               ", providerUrl=" + _providerUrl + ", providerAuthUrl=" + _providerAuthUrl +
               ", searchContext=" + _searchContext + ", state=" + getState() +
               ", searchFilter=" + _searchFilter + ", ldapContextFactory=" + _ldapContextFactory +
               ", bindWithoutSearch=" + _bindWithoutSearch + ", trustStore=" + _trustStore +
               ", searchUsername=" + _searchUsername + ", loginConfigScope=" + _loginConfigScope +
               ", authenticationMethod=" + _authenticationMethod + "]";
    }

    private void validateInitialDirContext(final SimpleLDAPAuthenticationManager<?> authenticationProvider)
    {
        final TrustStore truststore = authenticationProvider.getTrustStore();
        final Class<? extends SocketFactory> sslSocketFactoryOverrideClass =
                createSslSocketFactoryOverrideClass(truststore);

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
            ctx = createInitialDirContext(env, sslSocketFactoryOverrideClass, gssapiIdentity);
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

    private String getNameFromId(final String id, final Subject gssapiIdentity)
            throws NamingException
    {
        if (!isBindWithoutSearch())
        {
            InitialDirContext ctx = createSearchInitialDirContext(gssapiIdentity);

            try
            {
                SearchControls searchControls = new SearchControls();
                searchControls.setReturningAttributes(new String[]{});
                searchControls.setCountLimit(1L);
                searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);

                LOGGER.debug("Searching for '{}'", id);
                NamingEnumeration<?> namingEnum = invokeContextOperationAs(gssapiIdentity,
                                                      (PrivilegedExceptionAction<NamingEnumeration<?>>) () -> ctx.search(
                                                              _searchContext,
                                                              _searchFilter,
                                                              new String[]{id},
                                                              searchControls));

                if (namingEnum.hasMore())
                {
                    SearchResult result = (SearchResult) namingEnum.next();
                    String name = result.getNameInNamespace();
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
        LoginContext loginContext = new LoginContext(configScope);
        loginContext.login();
        return loginContext.getSubject();
    }

    private InitialDirContext createSearchInitialDirContext(final Subject gssapiIdentity) throws NamingException
    {
        Hashtable<String, Object> env = createInitialDirContextEnvironment(_providerUrl);
        setAuthenticationProperties(env, _searchUsername, _searchPassword, _authenticationMethod);
        return createInitialDirContext(env, _sslSocketFactoryOverrideClass, gssapiIdentity);
    }


    @Override
    public boolean isBindWithoutSearch()
    {
        return _bindWithoutSearch;
    }

    @Override
    public List<String> getTlsProtocolWhiteList()
    {
        return _tlsProtocolWhiteList;
    }

    @Override
    public List<String> getTlsProtocolBlackList()
    {
        return _tlsProtocolBlackList;
    }

    @Override
    public List<String> getTlsCipherSuiteWhiteList()
    {
        return _tlsCipherSuiteWhiteList;
    }

    @Override
    public List<String> getTlsCipherSuiteBlackList()
    {
        return _tlsCipherSuiteBlackList;
    }

    private void closeSafely(InitialDirContext ctx)
    {
        try
        {
            if (ctx != null)
            {
                ctx.close();
            }
        }
        catch (Exception e)
        {
            LOGGER.warn("Exception closing InitialDirContext", e);
        }
    }

}
