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

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.LoggerFactory;

import org.apache.qpid.configuration.CommonProperties;
import org.apache.qpid.transport.ConnectionSettings;
import org.apache.qpid.transport.ExceptionHandlingByteBufferReceiver;
import org.apache.qpid.transport.TransportException;
import org.apache.qpid.transport.network.NetworkConnection;
import org.apache.qpid.transport.network.TransportActivity;

public class IoNetworkTransport
{


    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IoNetworkTransport.class);
    private static final int TIMEOUT = Integer.getInteger(CommonProperties.IO_NETWORK_TRANSPORT_TIMEOUT_PROP_NAME,
                                                              CommonProperties.IO_NETWORK_TRANSPORT_TIMEOUT_DEFAULT);
    private NetworkConnection _connection;

    private final static Map<String, Socket> _registeredSockets = new ConcurrentHashMap<>();

    protected IoNetworkConnection createNetworkConnection(final Socket socket,
                                                       final ExceptionHandlingByteBufferReceiver engine,
                                                       final Integer sendBufferSize,
                                                       final Integer receiveBufferSize,
                                                       final int timeout,
                                                       final IdleTimeoutTicker ticker)
    {
        return new IoNetworkConnection(socket, engine, sendBufferSize, receiveBufferSize, timeout,
                                ticker);
    }

    public NetworkConnection connect(ConnectionSettings settings,
                                     ExceptionHandlingByteBufferReceiver delegate,
                                     TransportActivity transportActivity)
    {

        final Socket socket;
        if("tcp".equalsIgnoreCase(settings.getTransport()))
        {
            socket = connectTcp(settings);
        }
        else if("socket".equalsIgnoreCase(settings.getTransport()))
        {
            socket = _registeredSockets.remove(settings.getHost());
            if(socket == null)
            {
                throw new TransportException("No socket registered with id '"+settings.getHost()+"'");
            }
        }
        else
        {
            throw new TransportException("Unknown transport '" + settings.getTransport() + "'");
        }

        int sendBufferSize = settings.getWriteBufferSize();
        int receiveBufferSize = settings.getReadBufferSize();
        try
        {
            IdleTimeoutTicker ticker = new IdleTimeoutTicker(transportActivity, TIMEOUT);
            _connection = createNetworkConnection(socket, delegate, sendBufferSize, receiveBufferSize, TIMEOUT, ticker);
            ticker.setConnection(_connection);
            _connection.start();
        }
        catch(Exception e)
        {
            try
            {
                socket.close();
            }
            catch(IOException ioe)
            {
                //ignored, throw based on original exception
            }

            throw new TransportException("Error creating network connection", e);
        }

        return _connection;
    }

    private Socket connectTcp(final ConnectionSettings settings)
    {
        final Socket socket = new Socket();
        try
        {
            socket.setReuseAddress(true);
            socket.setTcpNoDelay(settings.isTcpNodelay());
            socket.setSendBufferSize(settings.getWriteBufferSize());
            socket.setReceiveBufferSize(settings.getReadBufferSize());

            if (LOGGER.isDebugEnabled())
            {
                LOGGER.debug("Socket options SO_RCVBUF : {}, SO_SNDBUF : {}, TCP_NODELAY : {}",
                             socket.getReceiveBufferSize(),
                             socket.getSendBufferSize(),
                             socket.getTcpNoDelay());
            }

            InetAddress address = InetAddress.getByName(settings.getHost());

            InetSocketAddress socketAddress = new InetSocketAddress(address, settings.getPort());
            socket.connect(socketAddress, settings.getConnectTimeout());

            if (LOGGER.isDebugEnabled())
            {
                LOGGER.debug("Socket connection from {} to {} established",
                             socket.getLocalSocketAddress(),
                             socket.getRemoteSocketAddress());
            }

        }
        catch (IOException e)
        {
            try
            {
                socket.close();
            }
            catch (IOException ignore)
            {
            }

            throw new TransportException("Error connecting to broker", e);
        }
        return socket;
    }

    public void close()
    {
        if(_connection != null)
        {
            _connection.close();
        }
    }

    public static void registerOpenSocket(String id, Socket socket)
    {
        _registeredSockets.put(id, socket);
    }

    public NetworkConnection getConnection()
    {
        return _connection;
    }
}
