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

package org.apache.qpid.server.model.port;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.CommonProperties;
import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.messages.PortMessages;
import org.apache.qpid.server.model.*;
import org.apache.qpid.server.security.ManagedPeerCertificateTrustStore;
import org.apache.qpid.server.security.SubjectCreator;
import org.apache.qpid.server.util.ParameterizedTypes;
import org.apache.qpid.server.util.PortUtil;

public abstract class AbstractPort<X extends AbstractPort<X>> extends AbstractConfiguredObject<X> implements Port<X>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractPort.class);

    private final Container<?> _container;
    private final EventLogger _eventLogger;

    @ManagedAttributeField
    private int _port;

    @ManagedAttributeField
    private KeyStore<?> _keyStore;

    @ManagedAttributeField
    private Collection<TrustStore> _trustStores;

    @ManagedAttributeField
    private Set<Transport> _transports;

    @ManagedAttributeField
    private Set<Protocol> _protocols;

    @ManagedAttributeField
    private AuthenticationProvider _authenticationProvider;

    @ManagedAttributeField
    private boolean _needClientAuth;

    @ManagedAttributeField
    private boolean _wantClientAuth;

    @ManagedAttributeField
    private TrustStore<?> _clientCertRecorder;

    @ManagedAttributeField
    private boolean _allowConfidentialOperationsOnInsecureChannels;

    @ManagedAttributeField
    private String _bindingAddress;

    private List<String> _tlsProtocolBlackList;
    private List<String> _tlsProtocolWhiteList;

    private List<String> _tlsCipherSuiteWhiteList;
    private List<String> _tlsCipherSuiteBlackList;

    public AbstractPort(Map<String, Object> attributes,
                        Container<?> container)
    {
        super(container, attributes);

        _container = container;
        _eventLogger = container.getEventLogger();
        _eventLogger.message(PortMessages.CREATE(getName()));
    }

    @Override
    public String getBindingAddress()
    {
        return _bindingAddress;
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
    public void validateOnCreate()
    {
        super.validateOnCreate();
        String bindingAddress = getBindingAddress();
        if (!PortUtil.isPortAvailable(bindingAddress, getPort()))
        {
            throw new IllegalConfigurationException(String.format("Cannot bind to port %d and binding address '%s'. Port is already is use.",
                    getPort(), bindingAddress == null || "".equals(bindingAddress) ? "*" : bindingAddress));
        }
    }

    @Override
    public void onValidate()
    {
        super.onValidate();

        boolean useTLSTransport = isUsingTLSTransport();

        if(useTLSTransport && getKeyStore() == null)
        {
            throw new IllegalConfigurationException("Can't create a port which uses a secure transport but has no KeyStore");
        }

        if(!isDurable())
        {
            throw new IllegalArgumentException(getClass().getSimpleName() + " must be durable");
        }

        if (getPort() != 0)
        {
            for (Port p : _container.getChildren(Port.class))
            {
                if (p != this && (p.getPort() == getPort() || p.getBoundPort() == getPort()))
                {
                    throw new IllegalConfigurationException("Can't add port "
                                                            + getName()
                                                            + " because port number "
                                                            + getPort()
                                                            + " is already configured for port "
                                                            + p.getName());
                }
            }
        }

        AuthenticationProvider<?> authenticationProvider = getAuthenticationProvider();
        final Set<Transport> transports = getTransports();
        validateAuthenticationMechanisms(authenticationProvider, transports);

        boolean useClientAuth = getNeedClientAuth() || getWantClientAuth();

        if(useClientAuth && (getTrustStores() == null || getTrustStores().isEmpty()))
        {
            throw new IllegalConfigurationException("Can't create port which requests SSL client certificates but has no trust stores configured.");
        }

        if(useClientAuth && !useTLSTransport)
        {
            throw new IllegalConfigurationException(
                    "Can't create port which requests SSL client certificates but doesn't use SSL transport.");
        }

        if(useClientAuth && getClientCertRecorder() != null)
        {
            if(!(getClientCertRecorder() instanceof ManagedPeerCertificateTrustStore))
            {
                throw new IllegalConfigurationException("Only trust stores of type " + ManagedPeerCertificateTrustStore.TYPE_NAME + " may be used as the client certificate recorder");
            }
        }
    }

    private void validateAuthenticationMechanisms(final AuthenticationProvider<?> authenticationProvider,
                                                  final Set<Transport> transports)
    {
        List<String> availableMechanisms = new ArrayList<>(authenticationProvider.getMechanisms());
        if(authenticationProvider.getDisabledMechanisms() != null)
        {
            availableMechanisms.removeAll(authenticationProvider.getDisabledMechanisms());
        }
        if (availableMechanisms.isEmpty())
        {
            throw new IllegalConfigurationException("The authentication provider '"
                                                    + authenticationProvider.getName()
                                                    + "' on port '"
                                                    + getName()
                                                    + "' has all authentication mechanisms disabled.");
        }
        if (hasNonTLSTransport(transports) && authenticationProvider.getSecureOnlyMechanisms() != null)
        {
            availableMechanisms.removeAll(authenticationProvider.getSecureOnlyMechanisms());
            if(availableMechanisms.isEmpty())
            {
                throw new IllegalConfigurationException("The port '"
                                                        + getName()
                                                        + "' allows for non TLS connections, but all authentication "
                                                        + "mechanisms of the authentication provider '"
                                                        + authenticationProvider.getName()
                                                        + "' are disabled on non-secure connections.");
            }
        }
    }

    @Override
    public AuthenticationProvider getAuthenticationProvider()
    {
        SystemConfig<?> systemConfig = getAncestor(SystemConfig.class);
        if(systemConfig.isManagementMode())
        {
            return _container.getManagementModeAuthenticationProvider();
        }
        return _authenticationProvider;
    }

    @Override
    public boolean isAllowConfidentialOperationsOnInsecureChannels()
    {
        return _allowConfidentialOperationsOnInsecureChannels;
    }

    private boolean isUsingTLSTransport()
    {
        return isUsingTLSTransport(getTransports());
    }

    private boolean isUsingTLSTransport(final Collection<Transport> transports)
    {
        return hasTransportOfType(transports, true);
    }

    private boolean hasNonTLSTransport(final Collection<Transport> transports)
    {
        return hasTransportOfType(transports, false);
    }

    private boolean hasTransportOfType(Collection<Transport> transports, boolean secure)
    {

        boolean hasTransport = false;
        if(transports != null)
        {
            for (Transport transport : transports)
            {
                if (secure == transport.isSecure())
                {
                    hasTransport = true;
                    break;
                }
            }
        }
        return hasTransport;
    }

    @Override
    protected void validateChange(final ConfiguredObject<?> proxyForValidation, final Set<String> changedAttributes)
    {
        super.validateChange(proxyForValidation, changedAttributes);
        Port<?> updated = (Port<?>)proxyForValidation;


        if(!getName().equals(updated.getName()))
        {
            throw new IllegalConfigurationException("Changing the port name is not allowed");
        }

        if(changedAttributes.contains(PORT))
        {
            int newPort = updated.getPort();
            if (getPort() != newPort && newPort != 0)
            {
                for (Port p : _container.getChildren(Port.class))
                {
                    if (p.getBoundPort() == newPort || p.getPort() == newPort)
                    {
                        throw new IllegalConfigurationException("Port number "
                                                                + newPort
                                                                + " is already in use by port "
                                                                + p.getName());
                    }
                }
            }
        }


        Collection<Transport> transports = updated.getTransports();

        Collection<Protocol> protocols = updated.getProtocols();


        boolean usesSsl = isUsingTLSTransport(transports);
        if (usesSsl)
        {
            if (updated.getKeyStore() == null)
            {
                throw new IllegalConfigurationException("Can't create port which requires SSL but has no key store configured.");
            }
        }

        if(changedAttributes.contains(Port.AUTHENTICATION_PROVIDER) || changedAttributes.contains(Port.TRANSPORTS))
        {
            validateAuthenticationMechanisms(updated.getAuthenticationProvider(), updated.getTransports());
        }

        boolean requiresCertificate = updated.getNeedClientAuth() || updated.getWantClientAuth();

        if (usesSsl)
        {
            if ((updated.getTrustStores() == null || updated.getTrustStores().isEmpty() ) && requiresCertificate)
            {
                throw new IllegalConfigurationException("Can't create port which requests SSL client certificates but has no trust store configured.");
            }
        }
        else
        {
            if (requiresCertificate)
            {
                throw new IllegalConfigurationException("Can't create port which requests SSL client certificates but doesn't use SSL transport.");
            }
        }


        if(requiresCertificate && updated.getClientCertRecorder() != null)
        {
            if(!(updated.getClientCertRecorder() instanceof ManagedPeerCertificateTrustStore))
            {
                throw new IllegalConfigurationException("Only trust stores of type " + ManagedPeerCertificateTrustStore.TYPE_NAME + " may be used as the client certificate recorder");
            }
        }
    }

    @Override
    public int getPort()
    {
        return _port;
    }

    @Override
    public Set<Transport> getTransports()
    {
        return _transports;
    }

    @Override
    public Set<Protocol> getProtocols()
    {
        return _protocols;
    }

    @Override
    public Collection<Connection> getConnections()
    {
        return getChildren(Connection.class);
    }

    @Override
    protected ListenableFuture<Void> onDelete()
    {
        _eventLogger.message(PortMessages.DELETE(getType(), getName()));
        return super.onDelete();
    }

    @StateTransition( currentState = {State.UNINITIALIZED, State.QUIESCED, State.ERRORED}, desiredState = State.ACTIVE )
    protected ListenableFuture<Void> activate()
    {
        try
        {
            setState(onActivate());
        }
        catch (RuntimeException e)
        {
            setState(State.ERRORED);
            throw new IllegalConfigurationException("Unable to active port '" + getName() + "'of type " + getType() + " on " + getPort(), e);
        }
        return Futures.immediateFuture(null);
    }

    @StateTransition( currentState = State.UNINITIALIZED, desiredState = State.QUIESCED)
    private ListenableFuture<Void> startQuiesced()
    {
        setState(State.QUIESCED);
        return Futures.immediateFuture(null);
    }


    @Override
    public NamedAddressSpace getAddressSpace(String name)
    {
        Collection<VirtualHostAlias> aliases = new TreeSet<>(VirtualHostAlias.COMPARATOR);

        aliases.addAll(getChildren(VirtualHostAlias.class));

        for(VirtualHostAlias alias : aliases)
        {
            NamedAddressSpace addressSpace = alias.getAddressSpace(name);
            if (addressSpace != null)
            {
                return addressSpace;
            }
        }
        return null;
    }

    protected State onActivate()
    {
        // no-op: expected to be overridden by subclass
        return State.ACTIVE;
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

    @Override
    public KeyStore getKeyStore()
    {
        return _keyStore;
    }

    @Override
    public Collection<TrustStore> getTrustStores()
    {
        return _trustStores;
    }

    @Override
    public boolean getNeedClientAuth()
    {
        return _needClientAuth;
    }

    @Override
    public TrustStore<?> getClientCertRecorder()
    {
        return _clientCertRecorder;
    }

    @Override
    public boolean getWantClientAuth()
    {
        return _wantClientAuth;
    }

    @Override
    public SubjectCreator getSubjectCreator(boolean secure, String host)
    {
        Collection children = _container.getChildren(GroupProvider.class);
        NamedAddressSpace addressSpace;
        if(host != null)
        {
            addressSpace = getAddressSpace(host);
        }
        else
        {
            addressSpace = null;
        }
        return new SubjectCreator(getAuthenticationProvider(), children, addressSpace);
    }

    @Override
    protected void logOperation(final String operation)
    {
        _eventLogger.message(PortMessages.OPERATION(operation));
    }

    @Override
    public String toString()
    {
        return getCategoryClass().getSimpleName() + "[id=" + getId() + ", name=" + getName() + ", type=" + getType() +  ", port=" + getPort() + "]";
    }

    @Override
    public boolean isTlsSupported()
    {
        return getSSLContext() != null;
    }

    @Override
    public boolean updateTLS()
    {
        if (isTlsSupported())
        {
            return updateSSLContext();
        }
        return false;
    }

    protected abstract boolean updateSSLContext();

}
