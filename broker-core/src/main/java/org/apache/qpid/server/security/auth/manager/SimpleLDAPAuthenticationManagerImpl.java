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

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.Principal;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.naming.AuthenticationException;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.configuration.CommonProperties;
import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.AuthenticationResult.AuthenticationStatus;
import org.apache.qpid.server.security.auth.UsernamePrincipal;
import org.apache.qpid.server.security.auth.manager.ldap.AbstractLDAPSSLSocketFactory;
import org.apache.qpid.server.security.auth.manager.ldap.LDAPSSLSocketFactoryGenerator;
import org.apache.qpid.server.security.auth.sasl.plain.PlainPasswordCallback;
import org.apache.qpid.server.security.auth.sasl.plain.PlainSaslServer;
import org.apache.qpid.server.util.CipherSuiteAndProtocolRestrictingSSLSocketFactory;
import org.apache.qpid.server.util.ParameterizedTypes;
import org.apache.qpid.server.util.StringUtil;
import org.apache.qpid.transport.network.security.ssl.SSLUtil;

public class SimpleLDAPAuthenticationManagerImpl extends AbstractAuthenticationManager<SimpleLDAPAuthenticationManagerImpl>
        implements SimpleLDAPAuthenticationManager<SimpleLDAPAuthenticationManagerImpl>
{
    private static final Logger _logger = LoggerFactory.getLogger(SimpleLDAPAuthenticationManagerImpl.class);

    private static final List<String> CONNECTIVITY_ATTRS = unmodifiableList(Arrays.asList(PROVIDER_URL,
                                                                             PROVIDER_AUTH_URL,
                                                                             SEARCH_CONTEXT,
                                                                             LDAP_CONTEXT_FACTORY,
                                                                             SEARCH_USERNAME,
                                                                             SEARCH_PASSWORD,
                                                                             TRUST_STORE));

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

    private List<String> _tlsProtocolWhiteList;
    private List<String>  _tlsProtocolBlackList;

    private List<String> _tlsCipherSuiteWhiteList;
    private List<String> _tlsCipherSuiteBlackList;

    /**
     * Dynamically created SSL Socket Factory implementation.
     */
    private Class<? extends SocketFactory> _sslSocketFactoryOverrideClass;

    @ManagedObjectFactoryConstructor
    protected SimpleLDAPAuthenticationManagerImpl(final Map<String, Object> attributes, final Broker broker)
    {
        super(attributes, broker);
    }

    @Override
    protected void validateOnCreate()
    {
        super.validateOnCreate();

        Class<? extends SocketFactory> sslSocketFactoryOverrideClass = createSslSocketFactoryOverrideClass(_trustStore);
        validateInitialDirContext(sslSocketFactoryOverrideClass, _providerUrl, _searchUsername, _searchPassword);
    }

    @Override
    protected void validateChange(final ConfiguredObject<?> proxyForValidation, final Set<String> changedAttributes)
    {
        super.validateChange(proxyForValidation, changedAttributes);

        if (!disjoint(changedAttributes, CONNECTIVITY_ATTRS))
        {
            SimpleLDAPAuthenticationManager changed = (SimpleLDAPAuthenticationManager)proxyForValidation;
            TrustStore changedTruststore = changed.getTrustStore();
            Class<? extends SocketFactory> sslSocketFactoryOverrideClass = createSslSocketFactoryOverrideClass(changedTruststore);
            validateInitialDirContext(sslSocketFactoryOverrideClass, changed.getProviderUrl(), changed.getSearchUsername(),
                                      changed.getSearchPassword());
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
    public List<String> getMechanisms()
    {
        return singletonList(PlainSaslServer.MECHANISM);
    }

    @Override
    public SaslServer createSaslServer(String mechanism, String localFQDN, Principal externalPrincipal) throws SaslException
    {
        if(PlainSaslServer.MECHANISM.equals(mechanism))
        {
            return new PlainSaslServer(new SimpleLDAPPlainCallbackHandler());
        }
        else
        {
            throw new SaslException("Unknown mechanism: " + mechanism);
        }
    }

    @Override
    public AuthenticationResult authenticate(SaslServer server, byte[] response)
    {
        try
        {
            // Process response from the client
            byte[] challenge = server.evaluateResponse(response != null ? response : new byte[0]);

            if (server.isComplete())
            {
                String authorizationID = server.getAuthorizationID();
                _logger.debug("Authenticated as {}", authorizationID);

                return new AuthenticationResult(new UsernamePrincipal(authorizationID));
            }
            else
            {
                return new AuthenticationResult(challenge, AuthenticationResult.AuthenticationStatus.CONTINUE);
            }
        }
        catch (SaslException e)
        {
            return new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR, e);
        }
    }

    @Override
    public AuthenticationResult authenticate(String username, String password)
    {
        try
        {
            AuthenticationResult result = doLDAPNameAuthentication(getNameFromId(username), password);
            if(result.getStatus() == AuthenticationStatus.SUCCESS)
            {
                //Return a result based on the supplied username rather than the search name
                return new AuthenticationResult(new UsernamePrincipal(username));
            }
            else
            {
                return result;
            }
        }
        catch (NamingException e)
        {
            return new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR, e);
        }
    }

    private AuthenticationResult doLDAPNameAuthentication(String name, String password)
    {
        if(name == null)
        {
            //The search didn't return anything, class as not-authenticated before it NPEs below
            return new AuthenticationResult(AuthenticationStatus.CONTINUE);
        }

        String providerAuthUrl = _providerAuthUrl == null ? _providerUrl : _providerAuthUrl;
        Hashtable<String, Object> env = createInitialDirContextEnvironment(providerAuthUrl);

        env.put(Context.SECURITY_AUTHENTICATION, "simple");
        env.put(Context.SECURITY_PRINCIPAL, name);
        env.put(Context.SECURITY_CREDENTIALS, password);

        InitialDirContext ctx = null;
        try
        {
            ctx = createInitialDirContext(env, _sslSocketFactoryOverrideClass);

            //Authentication succeeded
            return new AuthenticationResult(new UsernamePrincipal(name));
        }
        catch(AuthenticationException ae)
        {
            //Authentication failed
            return new AuthenticationResult(AuthenticationStatus.CONTINUE);
        }
        catch (NamingException e)
        {
            //Some other failure
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

    private Hashtable<String, Object> createInitialDirContextEnvironment(String providerUrl)
    {
        Hashtable<String,Object> env = new Hashtable<>();
        env.put(Context.INITIAL_CONTEXT_FACTORY, _ldapContextFactory);
        env.put(Context.PROVIDER_URL, providerUrl);
        return env;
    }

    private InitialDirContext createInitialDirContext(Hashtable<String, Object> env,
                                                      Class<? extends SocketFactory> sslSocketFactoryOverrideClass) throws NamingException
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
            return new InitialDirContext(env);
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
            _logger.error("Exception creating SSLContext", e);
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
        _logger.debug("Connection to Directory will use custom SSL socket factory : {}",  clazz);
        return clazz;
    }


    @Override
    public String toString()
    {
        return "SimpleLDAPAuthenticationManagerImpl [id=" + getId() + ", name=" + getName() +
               ", providerUrl=" + _providerUrl + ", providerAuthUrl=" + _providerAuthUrl +
               ", searchContext=" + _searchContext + ", state=" + getState() +
               ", searchFilter=" + _searchFilter + ", ldapContextFactory=" + _ldapContextFactory +
               ", bindWithoutSearch=" + _bindWithoutSearch  + ", trustStore=" + _trustStore  +
               ", searchUsername=" + _searchUsername + "]";
    }

    private void validateInitialDirContext(Class<? extends SocketFactory> sslSocketFactoryOverrideClass,
                                           final String providerUrl,
                                           final String searchUsername, final String searchPassword)
    {
        Hashtable<String,Object> env = createInitialDirContextEnvironment(providerUrl);

        setupSearchContext(env, searchUsername, searchPassword);

        InitialDirContext ctx = null;
        try
        {
            ctx = createInitialDirContext(env, sslSocketFactoryOverrideClass);
        }
        catch (NamingException e)
        {
            _logger.error("Failed to establish connectivity to the ldap server for '{}'", providerUrl, e);
            throw new IllegalConfigurationException("Failed to establish connectivity to the ldap server." , e);
        }
        finally
        {
            closeSafely(ctx);
        }
    }

    private void setupSearchContext(final Hashtable<String, Object> env,
                                    final String searchUsername, final String searchPassword)
    {
        if(_searchUsername != null && _searchUsername.trim().length()>0)
        {
            env.put(Context.SECURITY_AUTHENTICATION, "simple");
            env.put(Context.SECURITY_PRINCIPAL, searchUsername);
            env.put(Context.SECURITY_CREDENTIALS, searchPassword);
        }
        else
        {
            env.put(Context.SECURITY_AUTHENTICATION, "none");
        }
    }


    private class SimpleLDAPPlainCallbackHandler implements CallbackHandler
    {
        @Override
        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException
        {
            String name = null;
            String password = null;
            AuthenticationResult authenticated = null;
            for(Callback callback : callbacks)
            {
                if (callback instanceof NameCallback)
                {
                    String id = ((NameCallback) callback).getDefaultName();
                    try
                    {
                        name = getNameFromId(id);
                    }
                    catch (NamingException e)
                    {
                        _logger.warn("SASL Authentication Exception", e);
                    }
                    if(password != null)
                    {
                        authenticated = doLDAPNameAuthentication(name, password);
                    }
                }
                else if (callback instanceof PlainPasswordCallback)
                {
                    password = ((PlainPasswordCallback)callback).getPlainPassword();
                    if(name != null)
                    {
                        authenticated = doLDAPNameAuthentication(name, password);
                        if(authenticated.getStatus()== AuthenticationResult.AuthenticationStatus.SUCCESS)
                        {
                            ((PlainPasswordCallback)callback).setAuthenticated(true);
                        }
                    }
                }
                else if (callback instanceof AuthorizeCallback)
                {
                    ((AuthorizeCallback) callback).setAuthorized(authenticated != null && authenticated.getStatus() == AuthenticationResult.AuthenticationStatus.SUCCESS);
                }
                else
                {
                    throw new UnsupportedCallbackException(callback);
                }
            }
        }
    }

    private String getNameFromId(String id) throws NamingException
    {
        if(!isBindWithoutSearch())
        {
            Hashtable<String, Object> env = createInitialDirContextEnvironment(_providerUrl);

            setupSearchContext(env, _searchUsername, _searchPassword);

            InitialDirContext ctx = createInitialDirContext(env, _sslSocketFactoryOverrideClass);

            try
            {
                SearchControls searchControls = new SearchControls();
                searchControls.setReturningAttributes(new String[]{});
                searchControls.setCountLimit(1l);
                searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);
                NamingEnumeration<?> namingEnum = null;

                _logger.debug("Searching for '{}'", id);
                namingEnum = ctx.search(_searchContext, _searchFilter, new String[]{id}, searchControls);
                if (namingEnum.hasMore())
                {
                    SearchResult result = (SearchResult) namingEnum.next();
                    String name = result.getNameInNamespace();
                    _logger.debug("Found '{}' DN '{}'", id, name);
                    return name;
                }
                else
                {
                    _logger.debug("Not found '{}'", id);
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
                ctx = null;
            }
        }
        catch (Exception e)
        {
            _logger.warn("Exception closing InitialDirContext", e);
        }
    }

}
