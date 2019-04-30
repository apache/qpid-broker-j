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

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.EnumSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.transport.network.TransportEncryption;

public class NonBlockingNetworkTransport
{
    public static final String WILDCARD_ADDRESS = "*";

    private static final Logger LOGGER = LoggerFactory.getLogger(NonBlockingNetworkTransport.class);

    private volatile Set<TransportEncryption> _encryptionSet;
    private final MultiVersionProtocolEngineFactory _factory;
    private final ServerSocketChannel _serverSocket;
    private final NetworkConnectionScheduler _scheduler;
    private final AmqpPort<?> _port;
    private final InetSocketAddress _address;

    public NonBlockingNetworkTransport(final MultiVersionProtocolEngineFactory factory,
                                       final EnumSet<TransportEncryption> encryptionSet,
                                       final NetworkConnectionScheduler scheduler,
                                       final AmqpPort<?> port)
    {
        try
        {

            _factory = factory;

            String bindingAddress = port.getBindingAddress();
            if (WILDCARD_ADDRESS.equals(bindingAddress))
            {
                bindingAddress = null;
            }
            int portNumber = port.getPort();

            if ( bindingAddress == null )
            {
                _address = new InetSocketAddress(portNumber);
            }
            else
            {
                _address = new InetSocketAddress(bindingAddress, portNumber);
            }

            int acceptBacklog = port.getContextValue(Integer.class, AmqpPort.PORT_AMQP_ACCEPT_BACKLOG);
            _serverSocket =  ServerSocketChannel.open();

            _serverSocket.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            try
            {
                _serverSocket.bind(_address, acceptBacklog);
            }
            catch (BindException e)
            {
                throw new PortBindFailureException(_address);
            }
            _serverSocket.configureBlocking(false);
            _encryptionSet = encryptionSet;
            _scheduler = scheduler;
            _port = port;

        }
        catch (IOException e)
        {
            throw new TransportException("Failed to start AMQP on port : " + port, e);
        }
    }

    public void start()
    {
        _scheduler.addAcceptingSocket(_serverSocket, this);
    }


    public void close()
    {
        _scheduler.cancelAcceptingSocket(_serverSocket);
        try
        {
            _serverSocket.close();
        }
        catch (IOException e)
        {
            LOGGER.warn("Error closing the server socket for : " +  _address.toString(), e);
        }
    }

    public int getAcceptingPort()
    {
        return _serverSocket.socket().getLocalPort();
    }

    void acceptSocketChannel(final ServerSocketChannel serverSocketChannel)
    {
        SocketChannel socketChannel = null;
        boolean success = false;
        try
        {

            while ((socketChannel = serverSocketChannel.accept()) != null)
            {
                SocketAddress remoteSocketAddress = socketChannel.socket().getRemoteSocketAddress();
                final MultiVersionProtocolEngine engine =
                        _factory.newProtocolEngine(remoteSocketAddress);

                if (engine != null)
                {
                    socketChannel.setOption(StandardSocketOptions.TCP_NODELAY, _port.isTcpNoDelay());

                    final int bufferSize = _port.getNetworkBufferSize();

                    socketChannel.setOption(StandardSocketOptions.SO_SNDBUF, bufferSize);
                    socketChannel.setOption(StandardSocketOptions.SO_RCVBUF, bufferSize);

                    socketChannel.configureBlocking(false);



                    NonBlockingConnection connection =
                            new NonBlockingConnection(socketChannel,
                                                      engine,
                                                      _encryptionSet,
                                                      new Runnable()
                                                      {

                                                          @Override
                                                          public void run()
                                                          {
                                                              engine.encryptedTransport();
                                                          }
                                                      },
                                                      _scheduler,
                                                      _port);

                    engine.setNetworkConnection(connection);

                    connection.start();

                    _scheduler.addConnection(connection);

                    success = true;
                }
                else
                {
                    LOGGER.error("No Engine available.");
                    try
                    {
                        socketChannel.close();
                    }
                    catch (IOException e)
                    {
                        LOGGER.debug("Failed to close socket " + socketChannel, e);
                    }                }
            }
        }
        catch (IOException e)
        {
            LOGGER.error("Failed to process incoming socket", e);
        }
        finally
        {
            if (!success && socketChannel != null)
            {
                try
                {
                    socketChannel.close();
                }
                catch (IOException e)
                {
                    LOGGER.debug("Failed to close socket " + socketChannel, e);
                }
            }
        }
    }

    void setEncryptionSet(final Set<TransportEncryption> encryptionSet)
    {
        _encryptionSet = encryptionSet;
    }
}
