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
        int sendBufferSize = settings.getWriteBufferSize();
        int receiveBufferSize = settings.getReadBufferSize();

        final Socket socket;
        try
        {
            socket = new Socket();
            socket.setReuseAddress(true);
            socket.setTcpNoDelay(settings.isTcpNodelay());
            socket.setSendBufferSize(sendBufferSize);
            socket.setReceiveBufferSize(receiveBufferSize);

            if(LOGGER.isDebugEnabled())
            {
                LOGGER.debug("SO_RCVBUF : " + socket.getReceiveBufferSize());
                LOGGER.debug("SO_SNDBUF : " + socket.getSendBufferSize());
                LOGGER.debug("TCP_NODELAY : " + socket.getTcpNoDelay());
            }

            InetAddress address = InetAddress.getByName(settings.getHost());

            socket.connect(new InetSocketAddress(address, settings.getPort()), settings.getConnectTimeout());
        }
        catch (IOException e)
        {
            throw new TransportException("Error connecting to broker", e);
        }

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

    public void close()
    {
        if(_connection != null)
        {
            _connection.close();
        }
    }

    public NetworkConnection getConnection()
    {
        return _connection;
    }
}
