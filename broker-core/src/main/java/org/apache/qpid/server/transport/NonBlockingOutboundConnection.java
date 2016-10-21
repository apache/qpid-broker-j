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
package org.apache.qpid.server.transport;

import java.nio.channels.SocketChannel;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collection;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.federation.OutboundProtocolEngine;
import org.apache.qpid.server.model.Container;
import org.apache.qpid.server.model.KeyStore;
import org.apache.qpid.server.model.RemoteHostAddress;
import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.plugin.OutboundProtocolEngineCreator;
import org.apache.qpid.server.plugin.QpidServiceLoader;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.virtualhost.AbstractVirtualHost;
import org.apache.qpid.transport.network.security.ssl.QpidMultipleTrustManager;
import org.apache.qpid.transport.network.security.ssl.SSLUtil;

public class NonBlockingOutboundConnection extends NonBlockingConnection
{
    private static final Logger LOGGER = LoggerFactory.getLogger(NonBlockingOutboundConnection.class);

    private final AbstractVirtualHost<?> _virtualHost;
    private final RemoteHostAddress<?> _address;
    private final int _networkBufferSize;
    private final long _outboundMessageBufferLimit;
    private volatile boolean _connected;

    public NonBlockingOutboundConnection(SocketChannel socketChannel,
                                         final RemoteHostAddress<?> address,
                                         final NetworkConnectionScheduler networkConnectionScheduler,
                                         final AbstractVirtualHost<?> virtualHost,
                                         final Action<Boolean> onConnectionLoss)
    {
        super(socketChannel, createProtocolEngine(address, virtualHost), networkConnectionScheduler, address.getAddress()+":"+address.getPort());
        OutboundProtocolEngine protocolEngine = (OutboundProtocolEngine) getProtocolEngine();
        protocolEngine.setConnection(this);
        protocolEngine.setOnClosedTask(onConnectionLoss);
        _virtualHost = virtualHost;
        _address = address;
        _networkBufferSize = virtualHost.getAncestor(Container.class).getNetworkBufferSize();
        _outboundMessageBufferLimit = (long) virtualHost.getContextValue(Long.class,
                                                                   AmqpPort.PORT_AMQP_OUTBOUND_MESSAGE_BUFFER_SIZE);

        final NonBlockingConnectionDelegate delegate;
        switch(address.getTransport())
        {
            case TCP:
                delegate = new NonBlockingConnectionPlainDelegate(this, getNetworkBufferSize());
                break;
            case SSL:
                delegate = new NonBlockingConnectionTLSDelegate(this, getNetworkBufferSize(), createSSLEngine(address));
                break;
            default:
                throw new IllegalArgumentException("Transport '"+address.getTransport()+"' is not supported");
        }
        setDelegate(delegate);
    }

    private static ProtocolEngine createProtocolEngine(final RemoteHostAddress<?> address, final AbstractVirtualHost<?> virtualHost)
    {
        for(OutboundProtocolEngineCreator engineCreator : (new QpidServiceLoader()).instancesOf(OutboundProtocolEngineCreator.class))
        {
            if(engineCreator.getVersion().equals(address.getProtocol()))
            {
                return engineCreator.newProtocolEngine(address, virtualHost);
            }
        }

        return null;
    }


    @Override
    protected long getOutboundMessageBufferLimit()
    {
        return _outboundMessageBufferLimit;
    }

    @Override
    protected int getNetworkBufferSize()
    {
        return _networkBufferSize;
    }


    @Override
    public boolean wantsConnect()
    {
        return !_connected && !(_connected = getSocketChannel().isConnected());
    }

    static SSLEngine createSSLEngine(RemoteHostAddress<?> address)
    {
        SSLEngine sslEngine = createSslContext(address).createSSLEngine();
        sslEngine.setUseClientMode(true);
        SSLUtil.updateEnabledTlsProtocols(sslEngine, address.getTlsProtocolWhiteList(), address.getTlsProtocolBlackList());
        SSLUtil.updateEnabledCipherSuites(sslEngine, address.getTlsCipherSuiteWhiteList(), address.getTlsCipherSuiteBlackList());
        if(address.getTlsCipherSuiteWhiteList() != null && !address.getTlsCipherSuiteWhiteList().isEmpty())
        {
            SSLUtil.useCipherOrderIfPossible(sslEngine);
        }

        return sslEngine;
    }

    private static SSLContext createSslContext(RemoteHostAddress<?> address)
    {
        KeyStore keyStore = address.getKeyStore();
        Collection<TrustStore> trustStores = address.getTrustStores();

        try
        {
            SSLContext sslContext = SSLUtil.tryGetSSLContext();

            KeyManager[] keyManagers = keyStore.getKeyManagers();

            TrustManager[] trustManagers;
            if(trustStores == null || trustStores.isEmpty())
            {
                trustManagers = null;
            }
            else if(trustStores.size() == 1)
            {
                trustManagers = trustStores.iterator().next().getTrustManagers();
            }
            else
            {
                Collection<TrustManager> trustManagerList = new ArrayList<>();
                final QpidMultipleTrustManager mulTrustManager = new QpidMultipleTrustManager();

                for(TrustStore ts : trustStores)
                {
                    TrustManager[] managers = ts.getTrustManagers();
                    if(managers != null)
                    {
                        for(TrustManager manager : managers)
                        {
                            if(manager instanceof X509TrustManager)
                            {
                                mulTrustManager.addTrustManager((X509TrustManager)manager);
                            }
                            else
                            {
                                trustManagerList.add(manager);
                            }
                        }
                    }
                }
                if(!mulTrustManager.isEmpty())
                {
                    trustManagerList.add(mulTrustManager);
                }
                trustManagers = trustManagerList.toArray(new TrustManager[trustManagerList.size()]);
            }
            sslContext.init(keyManagers, trustManagers, null);

            return sslContext;

        }
        catch (GeneralSecurityException e)
        {
            throw new IllegalArgumentException("Unable to create SSLContext for key or trust store", e);
        }
    }

}
