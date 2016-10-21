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
package org.apache.qpid.server.federation;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import org.apache.qpid.configuration.CommonProperties;
import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.KeyStore;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.RemoteHost;
import org.apache.qpid.server.model.RemoteHostAddress;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.StateTransition;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.server.util.ParameterizedTypes;

@ManagedObject( category = false, type = RemoteHostAddress.REMOTE_HOST_ADDRESS_TYPE )
class RemoteHostAddressImpl extends AbstractConfiguredObject<RemoteHostAddressImpl> implements RemoteHostAddress<RemoteHostAddressImpl>
{
    @ManagedAttributeField
    private String _address;
    @ManagedAttributeField
    private int _port;
    @ManagedAttributeField
    private String _hostName;
    @ManagedAttributeField
    private Protocol _protocol;
    @ManagedAttributeField
    private Transport _transport;
    @ManagedAttributeField
    private KeyStore _keyStore;
    @ManagedAttributeField
    private Collection<TrustStore> _trustStores;
    @ManagedAttributeField
    private int _desiredHeartbeatInterval;

    private List<String> _tlsProtocolBlackList;
    private List<String> _tlsProtocolWhiteList;

    private List<String> _tlsCipherSuiteWhiteList;
    private List<String> _tlsCipherSuiteBlackList;


    @ManagedObjectFactoryConstructor
    public RemoteHostAddressImpl(Map<String, Object> attributes, RemoteHost<?> remoteHost)
    {
        super(parentsMap(remoteHost), attributes);
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
    public String getAddress()
    {
        return _address;
    }

    @Override
    public int getPort()
    {
        return _port;
    }

    @Override
    public String getHostName()
    {
        return _hostName;
    }

    @Override
    public Protocol getProtocol()
    {
        return _protocol;
    }

    @Override
    public Transport getTransport()
    {
        return _transport;
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
    public int getDesiredHeartbeatInterval()
    {
        return _desiredHeartbeatInterval;
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

    @StateTransition( currentState = {State.UNINITIALIZED, State.QUIESCED, State.ERRORED}, desiredState = State.ACTIVE )
    protected ListenableFuture<Void> activate()
    {
        try
        {
            setState(State.ACTIVE);
        }
        catch (RuntimeException e)
        {
            setState(State.ERRORED);
            throw new IllegalConfigurationException("Unable to active remote host address '" + getName() + "'", e);
        }
        return Futures.immediateFuture(null);
    }


}
