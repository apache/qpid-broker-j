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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.configuration.CommonProperties;
import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.messages.BrokerMessages;
import org.apache.qpid.server.logging.messages.PortMessages;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Connection;
import org.apache.qpid.server.model.Container;
import org.apache.qpid.server.model.KeyStore;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.StateTransition;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.server.util.ParameterizedTypes;

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

    private List<String> _tlsProtocolBlackList;
    private List<String> _tlsProtocolWhiteList;

    private List<String> _tlsCipherSuiteWhiteList;
    private List<String> _tlsCipherSuiteBlackList;

    public AbstractPort(Map<String, Object> attributes,
                        Container<?> container)
    {
        super(parentsMap(container), attributes);

        _container = container;
        _eventLogger = container.getEventLogger();
        _eventLogger.message(PortMessages.CREATE(getName()));
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

        for (Port p : _container.getChildren(Port.class))
        {
            if (p.getPort() == getPort() && p.getPort() != 0 && p != this)
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

    protected final boolean isUsingTLSTransport()
    {
        return isUsingTLSTransport(getTransports());
    }

    protected final boolean isUsingTLSTransport(final Collection<Transport> transports)
    {
        return hasTransportOfType(transports, true);
    }

    protected final boolean hasNonTLSTransport()
    {
        return hasNonTLSTransport(getTransports());
    }
    protected final boolean hasNonTLSTransport(final Collection<Transport> transports)
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
            if (getPort() != newPort)
            {
                for (Port p : _container.getChildren(Port.class))
                {
                    if (p.getPort() == newPort)
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

    @StateTransition(currentState = { State.ACTIVE, State.QUIESCED, State.ERRORED}, desiredState = State.DELETED )
    private ListenableFuture<Void> doDelete()
    {
        return doAfterAlways(closeAsync(), new Runnable()
        {
            @Override
            public void run()
            {
                deleted();
                setState(State.DELETED);
                _eventLogger.message(PortMessages.DELETE(getType(), getName()));
            }
        });
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
    protected void logOperation(final String operation)
    {
        _eventLogger.message(PortMessages.OPERATION(operation));
    }

    @Override
    public String toString()
    {
        return getCategoryClass().getSimpleName() + "[id=" + getId() + ", name=" + getName() + ", type=" + getType() +  ", port=" + getPort() + "]";
    }

}
