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
import java.util.Set;

import javax.net.ssl.SSLEngine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.transport.network.TransportEncryption;
import org.apache.qpid.transport.network.security.ssl.SSLUtil;

public class NonBlockingInboundConnection extends NonBlockingConnection
{
    private static final Logger LOGGER = LoggerFactory.getLogger(NonBlockingInboundConnection.class);

    private final Runnable _onTransportEncryptionAction;
    private final long _outboundMessageBufferLimit;


    private final AmqpPort _port;

    public NonBlockingInboundConnection(SocketChannel socketChannel,
                                        ProtocolEngine protocolEngine,
                                        final Set<TransportEncryption> encryptionSet,
                                        final Runnable onTransportEncryptionAction,
                                        final NetworkConnectionScheduler scheduler,
                                        final AmqpPort port)
    {
        super(socketChannel, protocolEngine, scheduler, socketChannel.socket().getRemoteSocketAddress().toString());
        _onTransportEncryptionAction = onTransportEncryptionAction;

        _port = port;

        _outboundMessageBufferLimit = (long) _port.getContextValue(Long.class,
                                                                   AmqpPort.PORT_AMQP_OUTBOUND_MESSAGE_BUFFER_SIZE);


        if(encryptionSet.size() == 1)
        {
            setTransportEncryption(encryptionSet.iterator().next());
        }
        else
        {
            setDelegate(new NonBlockingConnectionUndecidedDelegate(this));
        }

    }

    static SSLEngine createSSLEngine(AmqpPort<?> port)
    {
        SSLEngine sslEngine = port.getSSLContext().createSSLEngine();
        sslEngine.setUseClientMode(false);
        SSLUtil.updateEnabledTlsProtocols(sslEngine, port.getTlsProtocolWhiteList(), port.getTlsProtocolBlackList());
        SSLUtil.updateEnabledCipherSuites(sslEngine, port.getTlsCipherSuiteWhiteList(), port.getTlsCipherSuiteBlackList());
        if(port.getTlsCipherSuiteWhiteList() != null && !port.getTlsCipherSuiteWhiteList().isEmpty())
        {
            SSLUtil.useCipherOrderIfPossible(sslEngine);
        }

        if(port.getNeedClientAuth())
        {
            sslEngine.setNeedClientAuth(true);
        }
        else if(port.getWantClientAuth())
        {
            sslEngine.setWantClientAuth(true);
        }
        return sslEngine;
    }

    @Override
    protected long getOutboundMessageBufferLimit()
    {
        return _outboundMessageBufferLimit;
    }

    @Override
    protected int getNetworkBufferSize()
    {
        return _port.getNetworkBufferSize();
    }

    @Override
    public boolean wantsConnect()
    {
        return false;
    }


    @Override
    public String toString()
    {
        return "[InboundConnection " + getRemoteAddressString() + "]";
    }

    public void setTransportEncryption(TransportEncryption transportEncryption)
    {
        NonBlockingConnectionDelegate oldDelegate = getDelegate();
        switch (transportEncryption)
        {
            case TLS:
                _onTransportEncryptionAction.run();
                setDelegate(new NonBlockingConnectionTLSDelegate(this,
                                                                 _port.getNetworkBufferSize(),
                                                                 createSSLEngine(_port)));
                break;
            case NONE:
                setDelegate(new NonBlockingConnectionPlainDelegate(this, _port.getNetworkBufferSize()));
                break;
            default:
                throw new IllegalArgumentException("unknown TransportEncryption " + transportEncryption);
        }
        if(oldDelegate != null)
        {
            QpidByteBuffer src = oldDelegate.getNetInputBuffer().duplicate();
            src.flip();
            getDelegate().getNetInputBuffer().put(src);
            src.dispose();
        }
        LOGGER.debug("Identified transport encryption as " + transportEncryption);
    }


}
