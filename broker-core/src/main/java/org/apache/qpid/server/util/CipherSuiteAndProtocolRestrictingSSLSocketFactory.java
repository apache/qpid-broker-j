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

package org.apache.qpid.server.util;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import org.apache.qpid.server.transport.network.security.ssl.SSLUtil;

public class CipherSuiteAndProtocolRestrictingSSLSocketFactory extends SSLSocketFactory
{
    private final SSLSocketFactory _wrappedSocketFactory;
    private final List<String> _tlsCipherSuiteWhiteList;
    private final List<String> _tlsCipherSuiteBlackList;
    private final List<String> _tlsProtocolWhiteList;
    private final List<String> _tlsProtocolBlackList;

    public CipherSuiteAndProtocolRestrictingSSLSocketFactory(final SSLSocketFactory wrappedSocketFactory,
                                                             final List<String> tlsCipherSuiteWhiteList,
                                                             final List<String> tlsCipherSuiteBlackList,
                                                             final List<String> tlsProtocolWhiteList,
                                                             final List<String> tlsProtocolBlackList)
    {
        _wrappedSocketFactory = wrappedSocketFactory;
        _tlsCipherSuiteWhiteList = tlsCipherSuiteWhiteList == null ? null : new ArrayList<>(tlsCipherSuiteWhiteList);
        _tlsCipherSuiteBlackList = tlsCipherSuiteBlackList == null ? null : new ArrayList<>(tlsCipherSuiteBlackList);
        _tlsProtocolWhiteList = tlsProtocolWhiteList == null ? null : new ArrayList<>(tlsProtocolWhiteList);
        _tlsProtocolBlackList = tlsProtocolBlackList == null ? null : new ArrayList<>(tlsProtocolBlackList);
    }

    @Override
    public String[] getDefaultCipherSuites()
    {
        return SSLUtil.filterEnabledCipherSuites(_wrappedSocketFactory.getDefaultCipherSuites(),
                                                 _wrappedSocketFactory.getSupportedCipherSuites(),
                                                 _tlsCipherSuiteWhiteList,
                                                 _tlsCipherSuiteBlackList);
    }

    @Override
    public String[] getSupportedCipherSuites()
    {
        return _wrappedSocketFactory.getSupportedCipherSuites();
    }

    @Override
    public Socket createSocket(final Socket socket, final String host, final int port, final boolean autoClose)
            throws IOException
    {
        final SSLSocket newSocket = (SSLSocket) _wrappedSocketFactory.createSocket(socket, host, port, autoClose);
        SSLUtil.updateEnabledCipherSuites(newSocket, _tlsCipherSuiteWhiteList, _tlsCipherSuiteBlackList);
        SSLUtil.updateEnabledTlsProtocols(newSocket, _tlsProtocolWhiteList, _tlsProtocolBlackList);
        return newSocket;
    }

    @Override
    public Socket createSocket(final String host, final int port) throws IOException, UnknownHostException
    {
        final SSLSocket socket = (SSLSocket) _wrappedSocketFactory.createSocket(host, port);
        SSLUtil.updateEnabledCipherSuites(socket, _tlsCipherSuiteWhiteList, _tlsCipherSuiteBlackList);
        SSLUtil.updateEnabledTlsProtocols(socket, _tlsProtocolWhiteList, _tlsProtocolBlackList);
        return socket;
    }

    @Override
    public Socket createSocket(final String host, final int port, final InetAddress localhost, final int localPort)
            throws IOException, UnknownHostException
    {
        final SSLSocket socket = (SSLSocket) _wrappedSocketFactory.createSocket(host, port, localhost, localPort);
        SSLUtil.updateEnabledCipherSuites(socket, _tlsCipherSuiteWhiteList, _tlsCipherSuiteBlackList);
        SSLUtil.updateEnabledTlsProtocols(socket, _tlsProtocolWhiteList, _tlsProtocolBlackList);
        return socket;
    }

    @Override
    public Socket createSocket(final InetAddress host, final int port) throws IOException
    {
        final SSLSocket socket = (SSLSocket) _wrappedSocketFactory.createSocket(host, port);
        SSLUtil.updateEnabledCipherSuites(socket, _tlsCipherSuiteWhiteList, _tlsCipherSuiteBlackList);
        SSLUtil.updateEnabledTlsProtocols(socket, _tlsProtocolWhiteList, _tlsProtocolBlackList);
        return socket;
    }

    @Override
    public Socket createSocket(final InetAddress address,
                               final int port,
                               final InetAddress localAddress,
                               final int localPort) throws IOException
    {
        final SSLSocket socket =
                (SSLSocket) _wrappedSocketFactory.createSocket(address, port, localAddress, localPort);
        SSLUtil.updateEnabledCipherSuites(socket, _tlsCipherSuiteWhiteList, _tlsCipherSuiteBlackList);
        SSLUtil.updateEnabledTlsProtocols(socket, _tlsProtocolWhiteList, _tlsProtocolBlackList);
        return socket;
    }
}
