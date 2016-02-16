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
package org.apache.qpid.server.jmx;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.rmi.ssl.SslRMIServerSocketFactory;

import org.apache.qpid.server.util.Action;
import org.apache.qpid.transport.network.security.ssl.SSLUtil;

public class QpidSslRMIServerSocketFactory extends SslRMIServerSocketFactory
{
    private final SSLContext _sslContext;
    private final List<String> _tlsProtocolWhiteList;
    private final List<String> _tlsProtocolBlackList;
    private final List<String> _tlsCipherSuiteWhiteList;
    private final List<String> _tlsCipherSuiteBlackList;
    private final Action<Integer> _portAllocationAction;

    /**
     * SslRMIServerSocketFactory which creates the ServerSocket using the
     * supplied SSLContext rather than the system default context normally
     * used by the superclass, allowing us to use a configuration-specified
     * key store.
     * @param sslContext previously created sslContext using the desired key store.
     * @param tlsProtocolWhiteList if provided only TLS protocols matching the regular expressions in this list will be enabled
     * @param tlsProtocolBlackList if provided none of the TLS protocols matching the regular expressions in this list will be enabled
     * @param tlsCipherSuiteWhiteList if provided only TLS cipher suites matching the regular expressions in this list will be enabled
     * @param tlsCipherSuiteBlackList if provided none of the TLS cipher suites matching the regular expressions in this list will be enabled
     * @throws NullPointerException if the provided {@link SSLContext} is null.
     * @param action
     */
    public QpidSslRMIServerSocketFactory(SSLContext sslContext,
                                         final List<String> tlsProtocolWhiteList,
                                         final List<String> tlsProtocolBlackList,
                                         final List<String> tlsCipherSuiteWhiteList,
                                         final List<String> tlsCipherSuiteBlackList,
                                         final Action<Integer> action) throws NullPointerException
    {
        super();

        if(sslContext == null)
        {
            throw new NullPointerException("The provided SSLContext must not be null");
        }

        _sslContext = sslContext;
        _tlsProtocolWhiteList = tlsProtocolWhiteList == null ? null : new ArrayList<>(tlsProtocolWhiteList);
        _tlsProtocolBlackList = tlsProtocolBlackList == null ? null : new ArrayList<>(tlsProtocolBlackList);
        _tlsCipherSuiteWhiteList = tlsCipherSuiteWhiteList == null ? null : new ArrayList<>(tlsCipherSuiteWhiteList);
        _tlsCipherSuiteBlackList = tlsCipherSuiteBlackList == null ? null : new ArrayList<>(tlsCipherSuiteBlackList);
        _portAllocationAction = action;

        //TODO: settings + implementation for SSL client auth, updating equals and hashCode appropriately.
    }

    @Override
    public ServerSocket createServerSocket(int port) throws IOException
    {
        final SSLSocketFactory factory = _sslContext.getSocketFactory();

        ServerSocket serverSocket = new ServerSocket()
        {
            public Socket accept() throws IOException
            {
                Socket socket = super.accept();

                SSLSocket sslSocket =
                        (SSLSocket) factory.createSocket(socket,
                                                         socket.getInetAddress().getHostName(),
                                                         socket.getPort(),
                                                         true);
                sslSocket.setUseClientMode(false);
                SSLUtil.updateEnabledTlsProtocols(sslSocket, _tlsProtocolWhiteList, _tlsProtocolBlackList);
                SSLUtil.updateEnabledCipherSuites(sslSocket, _tlsCipherSuiteWhiteList, _tlsCipherSuiteBlackList);
                return sslSocket;
            }
        };
        serverSocket.setReuseAddress(true);
        serverSocket.bind(new InetSocketAddress(port));
        _portAllocationAction.performAction(serverSocket.getLocalPort());
        return serverSocket;
    }

    /**
     * One QpidSslRMIServerSocketFactory is equal to
     * another if their (non-null) SSLContext are equal.
     */
    @Override
    public boolean equals(Object object)
    {
        if (!(object instanceof QpidSslRMIServerSocketFactory))
        {
            return false;
        }

        QpidSslRMIServerSocketFactory that = (QpidSslRMIServerSocketFactory) object;

        return _sslContext.equals(that._sslContext);
    }

    @Override
    public int hashCode()
    {
        return _sslContext.hashCode();
    }

}
