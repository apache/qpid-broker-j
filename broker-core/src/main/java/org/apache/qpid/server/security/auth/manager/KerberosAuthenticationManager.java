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

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Container;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.sasl.SaslNegotiator;
import org.apache.qpid.server.security.auth.sasl.SaslSettings;
import org.apache.qpid.server.security.auth.sasl.kerberos.KerberosNegotiator;

@ManagedObject( category = false, type = "Kerberos" )
public class KerberosAuthenticationManager extends AbstractAuthenticationManager<KerberosAuthenticationManager>
{
    private static final String JAAS_CONFIG_PROPERTY = "java.security.auth.login.config";
    private static final String GSSAPI_SERVER_NAME = "qpid.auth.gssapi.serverName";
    private static final String GSSAPI_SPNEGO_STRIP_REALM = "qpid.auth.gssapi.spnegoStripRealmFromPrincipalName";
    static final String GSSAPI_SPNEGO_CONFIG = "qpid.auth.gssapi.spnegoConfigScope";
    public static final String PROVIDER_TYPE = "Kerberos";
    public static final String GSSAPI_MECHANISM = "GSSAPI";

    private final Container<?> _container;
    private final SpnegoAuthenticator _authenticator;
    private volatile String _serverName;
    private volatile String _spnegoConfig;
    private volatile boolean _stripRealmFromPrincipalName;

    @ManagedObjectFactoryConstructor
    protected KerberosAuthenticationManager(final Map<String, Object> attributes, final Container<?> container)
    {
        super(attributes, container);
        _container = container;
        _authenticator = new SpnegoAuthenticator(this);
    }

    @Override
    protected void onOpen()
    {
        super.onOpen();
        final Set<String> contextKeys = getContextKeys(false);
        _serverName = contextKeys.contains(GSSAPI_SERVER_NAME) ?
                getContextValue(String.class, GSSAPI_SERVER_NAME) : null;
        _spnegoConfig = contextKeys.contains(GSSAPI_SPNEGO_CONFIG) ?
                getContextValue(String.class, GSSAPI_SPNEGO_CONFIG) : null;
        _stripRealmFromPrincipalName = contextKeys.contains(GSSAPI_SPNEGO_STRIP_REALM) ?
                getContextValue(Boolean.class, GSSAPI_SPNEGO_STRIP_REALM) : false;
    }

    @Override
    public List<String> getMechanisms()
    {
        return Collections.singletonList(GSSAPI_MECHANISM);
    }

    @Override
    public SaslNegotiator createSaslNegotiator(final String mechanism,
                                               final SaslSettings saslSettings,
                                               final NamedAddressSpace addressSpace)
    {
        if(GSSAPI_MECHANISM.equals(mechanism))
        {
            final String serverName = _serverName == null ? saslSettings.getLocalFQDN(): _serverName;
            return new KerberosNegotiator(this, serverName);
        }
        else
        {
            return null;
        }
    }

    public AuthenticationResult authenticate(String authorizationHeader)
    {
        return _authenticator.authenticate(authorizationHeader);
    }

    public String getSpnegoLoginConfigScope()
    {
        return _spnegoConfig;
    }

    public boolean isStripRealmFromPrincipalName()
    {
        return _stripRealmFromPrincipalName;
    }

    @Override
    protected void validateOnCreate()
    {
        super.validateOnCreate();
        validate(this);
    }

    @Override
    protected void validateChange(final ConfiguredObject<?> proxyForValidation, final Set<String> changedAttributes)
    {
        super.validateChange(proxyForValidation, changedAttributes);
        validate(proxyForValidation);
    }

    private void validate(final ConfiguredObject<?> authenticationProvider)
    {
        final String config = System.getProperty(JAAS_CONFIG_PROPERTY);
        if (config != null && !new File(config).exists())
        {
            throw new IllegalConfigurationException(String.format(
                    "A path to non-existing file is specified in JVM system property '%s'", JAAS_CONFIG_PROPERTY));
        }

        if (_container.getChildren(AuthenticationProvider.class)
                      .stream()
                      .anyMatch(p -> p instanceof KerberosAuthenticationManager
                                     && p != authenticationProvider))
        {
            throw new IllegalConfigurationException("Another Kerberos authentication provider already exists."
                                                    + " Only one Kerberos authentication provider can be created.");
        }
    }
}
