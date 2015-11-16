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
package org.apache.qpid.transport.network.io;

import java.net.Socket;
import java.net.SocketAddress;
import java.security.Principal;
import java.security.cert.Certificate;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSocket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.transport.ByteBufferSender;
import org.apache.qpid.transport.ExceptionHandlingByteBufferReceiver;
import org.apache.qpid.transport.network.NetworkConnection;
import org.apache.qpid.transport.network.Ticker;

public class IoNetworkConnection implements NetworkConnection
{
    private static final Logger LOGGER = LoggerFactory.getLogger(IoNetworkConnection.class);
    private final Socket _socket;
    private final long _timeout;
    private final IoSender _ioSender;
    private final IoReceiver _ioReceiver;
    private Principal _principal;
    private boolean _principalChecked;
    private final Object _lock = new Object();
    private Certificate _certificate;
    private long _maxWriteIdleMillis;
    private long _maxReadIdleMillis;

    public IoNetworkConnection(Socket socket, ExceptionHandlingByteBufferReceiver delegate,
            int sendBufferSize, int receiveBufferSize, long timeout, Ticker ticker)
    {
        _socket = socket;
        _timeout = timeout;

        _ioReceiver = new IoReceiver(_socket, delegate, receiveBufferSize,_timeout);
        _ioReceiver.setTicker(ticker);

        _ioSender = new IoSender(_socket, 2 * sendBufferSize, _timeout);

        _ioSender.setReceiver(_ioReceiver);

    }

    public void start()
    {
        _ioSender.initiate();
        _ioReceiver.initiate();
    }

    public ByteBufferSender getSender()
    {
        return _ioSender;
    }

    public void close()
    {
        try
        {
            _ioSender.close();
        }
        finally
        {
            _ioReceiver.close(false);
        }
    }

    public SocketAddress getRemoteAddress()
    {
        return _socket.getRemoteSocketAddress();
    }

    public SocketAddress getLocalAddress()
    {
        return _socket.getLocalSocketAddress();
    }

    @Override
    public void setMaxWriteIdleMillis(final long millis)
    {
        _maxWriteIdleMillis = millis;
    }

    @Override
    public void setMaxReadIdleMillis(final long millis)
    {
        _maxReadIdleMillis = millis;
    }

    @Override
    public Principal getPeerPrincipal()
    {
        synchronized (_lock)
        {
            if(!_principalChecked)
            {
                if(_socket instanceof SSLSocket)
                {
                    try
                    {
                        _principal = ((SSLSocket) _socket).getSession().getPeerPrincipal();
                        final Certificate[] certs =
                                ((SSLSocket) _socket).getSession().getPeerCertificates();
                        if(certs != null && certs.length != 0)
                        {
                            _certificate = certs[0];
                        }
                    }
                    catch(SSLPeerUnverifiedException e)
                    {
                        _principal = null;
                    }
                }

                _principalChecked = true;
            }

            return _principal;
        }
    }

    @Override
    public Certificate getPeerCertificate()
    {
        synchronized (_lock)
        {
            if(!_principalChecked)
            {
                getPeerPrincipal();
            }
        }
        return _certificate;
    }

    @Override
    public long getMaxReadIdleMillis()
    {
        return _maxReadIdleMillis;
    }

    @Override
    public long getMaxWriteIdleMillis()
    {
        return _maxWriteIdleMillis;
    }
}
