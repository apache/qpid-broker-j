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

import java.util.EnumSet;
import java.util.Set;

import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.transport.network.TransportEncryption;

class TCPandSSLTransport implements AcceptingTransport
{
    private NonBlockingNetworkTransport _networkTransport;
    private volatile Set<Transport> _transports;
    private AmqpPort<?> _port;
    private Set<Protocol> _supported;
    private Protocol _defaultSupportedProtocolReply;
    private NetworkConnectionScheduler _scheduler;

    TCPandSSLTransport(final Set<Transport> transports,
                       final AmqpPort<?> port,
                       final Set<Protocol> supported,
                       final Protocol defaultSupportedProtocolReply)
    {
        _transports = transports;
        _port = port;
        _supported = supported;
        _defaultSupportedProtocolReply = defaultSupportedProtocolReply;
    }

    @Override
    public void start()
    {
        final MultiVersionProtocolEngineFactory protocolEngineFactory =
                new MultiVersionProtocolEngineFactory(
                        (Broker<?>) _port.getParent(),
                        _supported,
                        _defaultSupportedProtocolReply,
                        _port,
                        _transports.contains(Transport.TCP) ? Transport.TCP : Transport.SSL);

        EnumSet<TransportEncryption> encryptionSet = buildEncryptionSet(_transports);

        long threadPoolKeepAliveTimeout = _port.getContextValue(Long.class, AmqpPort.PORT_AMQP_THREAD_POOL_KEEP_ALIVE_TIMEOUT);

        _scheduler = new NetworkConnectionScheduler("Port-"+_port.getName(), _port.getNumberOfSelectors(),
                                                    _port.getThreadPoolSize(), threadPoolKeepAliveTimeout);
        _scheduler.start();
        _networkTransport = new NonBlockingNetworkTransport(protocolEngineFactory,
                                                            encryptionSet, _scheduler, _port);
        _networkTransport.start();
    }

    private EnumSet<TransportEncryption> buildEncryptionSet(final Set<Transport> transports)
    {
        EnumSet<TransportEncryption> encryptionSet = EnumSet.noneOf(TransportEncryption.class);
        if(transports.contains(Transport.TCP))
        {
            encryptionSet.add(TransportEncryption.NONE);
        }
        if(transports.contains(Transport.SSL))
        {
            encryptionSet.add(TransportEncryption.TLS);
        }
        return encryptionSet;
    }

    @Override
    public int getAcceptingPort()
    {
        NonBlockingNetworkTransport networkTransport = _networkTransport;
        return networkTransport == null ? _port.getPort() : networkTransport.getAcceptingPort();
    }

    @Override
    public boolean updatesSSLContext()
    {
        Set<Transport> transports = _port.getTransports();
        _transports = transports;
        _networkTransport.setEncryptionSet(buildEncryptionSet(transports));
        return true;
    }

    @Override
    public void close()
    {
        if (_networkTransport != null)
        {
            _networkTransport.close();
        }
        if(_scheduler != null)
        {
            _scheduler.close();
        }
    }
}
