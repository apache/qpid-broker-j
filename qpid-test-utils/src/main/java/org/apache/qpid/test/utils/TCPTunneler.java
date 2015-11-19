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
package org.apache.qpid.test.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collection;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A basic implementation of TCP traffic forwarder between ports.
 * It is intended to use in tests.
 */
public class TCPTunneler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TCPTunneler.class);

    private final TCPWorker _tcpWorker;
    private final ExecutorService _executor;

    public TCPTunneler(final String targetHost,
                       final int targetPort,
                       final int proxyPort,
                       final int numberOfConcurrentClients)
    {
        _executor = Executors.newFixedThreadPool(numberOfConcurrentClients * 2 + 1);
        _tcpWorker = new TCPWorker(proxyPort, targetHost, targetPort, _executor);
    }

    public void start() throws IOException
    {
        _tcpWorker.start();
    }

    public void stop()
    {
        try
        {
            _tcpWorker.stop();
        }
        finally
        {
            _executor.shutdown();
        }
    }

    public void addClientListener(TunnelListener listener)
    {
        _tcpWorker.addClientListener(listener);
    }

    public void removeClientListener(TunnelListener listener)
    {
        _tcpWorker.removeClientListener(listener);
    }

    public void disconnect(InetSocketAddress address)
    {
        LOGGER.info("Disconnecting {}", address);
        if (address != null)
        {
            _tcpWorker.disconnect(address);
        }
    }

    interface TunnelListener
    {
        void clientConnected(InetSocketAddress clientAddress);

        void clientDisconnected(InetSocketAddress clientAddress);
    }

    public static class NoopTunnelListener implements TunnelListener
    {
        @Override
        public void clientConnected(final InetSocketAddress clientAddress)
        {
        }

        @Override
        public void clientDisconnected(final InetSocketAddress clientAddress)
        {
        }
    }

    public static class TCPWorker implements Runnable
    {
        private final String _targetHost;
        private final int _targetPort;
        private final int _localPort;
        private final String _hostPort;
        private final AtomicBoolean _closed;
        private final Collection<SocketTunnel> _tunnels;
        private final Collection<TunnelListener> _tunnelListeners;
        private final TunnelListener _notifyingListener;
        private volatile ServerSocket _serverSocket;
        private volatile ExecutorService _executor;

        public TCPWorker(final int localPort,
                         final String targetHost,
                         final int targetPort,
                         final ExecutorService executor)
        {
            _closed = new AtomicBoolean();
            _targetHost = targetHost;
            _targetPort = targetPort;
            _localPort = localPort;
            _hostPort = _targetHost + ":" + _targetPort;
            _executor = executor;
            _tunnels = new CopyOnWriteArrayList<>();
            _tunnelListeners = new CopyOnWriteArrayList<>();
            _notifyingListener = new NoopTunnelListener()
            {
                @Override
                public void clientConnected(final InetSocketAddress clientAddress)
                {
                    notifyClientConnected(clientAddress);
                }

                @Override
                public void clientDisconnected(final InetSocketAddress clientAddress)
                {
                    try
                    {
                        notifyClientDisconnected(clientAddress);
                    }
                    finally
                    {
                        removeTunnel(clientAddress);
                    }
                }
            };
        }

        @Override
        public void run()
        {
            String threadName = Thread.currentThread().getName();
            try
            {
                Thread.currentThread().setName("TCPTunnelerAcceptingThread");
                while (!_closed.get())
                {
                    Socket clientSocket = _serverSocket.accept();
                    LOGGER.debug("Client opened socket {}", clientSocket);

                    createTunnel(clientSocket);
                }
            }
            catch (IOException e)
            {
                if (!_closed.get())
                {
                    LOGGER.error("Exception in accepting thread", e);
                }
            }
            finally
            {
                closeServerSocket();
                _closed.set(true);
                Thread.currentThread().setName(threadName);
            }
        }

        public void start()
        {
            LOGGER.info("Starting TCPTunneler forwarding from port {} to {}", _localPort, _hostPort);
            try
            {
                _serverSocket = new ServerSocket(_localPort);
                _serverSocket.setReuseAddress(true);
            }
            catch (IOException e)
            {
                throw new RuntimeException("Cannot start TCPTunneler on port " + _localPort, e);
            }

            if (_serverSocket != null)
            {
                LOGGER.info("Listening on port {}", _localPort);
                try
                {
                    _executor.execute(this);
                }
                catch (Exception e)
                {
                    try
                    {
                        closeServerSocket();
                    }
                    finally
                    {
                        throw new RuntimeException("Cannot start acceptor thread for TCPTunneler on port " + _localPort,
                                                   e);
                    }
                }
            }
        }

        public void stop()
        {
            if (_closed.compareAndSet(false, true))
            {
                LOGGER.info("Stopping TCPTunneler forwarding from port {} to {}",
                            _localPort,
                            _hostPort);
                try
                {
                    for (SocketTunnel tunnel : _tunnels)
                    {
                        tunnel.close();
                    }
                }
                finally
                {
                    closeServerSocket();
                }

                LOGGER.info("TCPTunneler forwarding from port {} to {} is stopped",
                            _localPort,
                            _hostPort);
            }
        }

        public void addClientListener(TunnelListener listener)
        {
            _tunnelListeners.add(listener);
            for (SocketTunnel socketTunnel : _tunnels)
            {
                try
                {
                    listener.clientConnected(socketTunnel.getClientAddress());
                }
                catch (Exception e)
                {
                    LOGGER.warn("Exception on notifying client listener about connected client", e);
                }
            }
        }

        public void removeClientListener(TunnelListener listener)
        {
            _tunnelListeners.remove(listener);
        }

        public void disconnect(final InetSocketAddress address)
        {
            SocketTunnel client = removeTunnel(address);
            if (client != null && !client.isClosed())
            {
                client.close();
                LOGGER.info("Tunnel for {} is disconnected", address);
            }
            else
            {
                LOGGER.info("Tunnel for {} not found", address);
            }
        }


        private void createTunnel(final Socket clientSocket)
        {
            Socket serverSocket = null;
            try
            {
                LOGGER.debug("Opening socket to {} for {}", _hostPort, clientSocket);
                serverSocket = new Socket(_targetHost, _targetPort);
                LOGGER.debug("Opened socket to {} for {}", serverSocket, clientSocket);
                SocketTunnel tunnel = new SocketTunnel(clientSocket, serverSocket, _notifyingListener);
                LOGGER.debug("Socket tunnel is created from {} to {}", clientSocket, serverSocket);
                _tunnels.add(tunnel);
                tunnel.start(_executor);
            }
            catch (Exception e)
            {
                LOGGER.error("Cannot forward i/o traffic between {} and {}", clientSocket, _hostPort, e);
                SocketTunnel.closeSocket(clientSocket);
                SocketTunnel.closeSocket(serverSocket);
            }
        }

        private void notifyClientConnected(final InetSocketAddress clientAddress)
        {
            for (TunnelListener listener : _tunnelListeners)
            {
                try
                {
                    listener.clientConnected(clientAddress);
                }
                catch (Exception e)
                {
                    LOGGER.warn("Exception on notifying client listener about connected client", e);
                }
            }
        }


        private void notifyClientDisconnected(final InetSocketAddress clientAddress)
        {
            for (TunnelListener listener : _tunnelListeners)
            {
                try
                {
                    listener.clientDisconnected(clientAddress);
                }
                catch (Exception e)
                {
                    LOGGER.warn("Exception on notifying client listener about disconnected client", e);
                }
            }
        }

        private void closeServerSocket()
        {
            if (_serverSocket != null)
            {
                try
                {
                    _serverSocket.close();
                }
                catch (IOException e)
                {
                    LOGGER.warn("Exception on closing of accepting socket", e);
                }
                finally
                {
                    _serverSocket = null;
                }
            }
        }


        private SocketTunnel removeTunnel(final InetSocketAddress clientAddress)
        {
            SocketTunnel client = null;
            for (SocketTunnel c : _tunnels)
            {
                if (c.isClientAddress(clientAddress))
                {
                    client = c;
                    break;
                }
            }
            if (client != null)
            {
                _tunnels.remove(client);
            }
            return client;
        }

    }

    public static class SocketTunnel
    {
        private final Socket _clientSocket;
        private final Socket _serverSocket;
        private final TunnelListener _tunnelListener;
        private final AtomicBoolean _closed;
        private final ClosableStreamForwarder _inputStreamForwarder;
        private final ClosableStreamForwarder _outputStreamForwarder;
        private final InetSocketAddress _clientSocketAddress;

        public SocketTunnel(final Socket clientSocket,
                            final Socket serverSocket,
                            final TunnelListener tunnelListener) throws IOException
        {
            _clientSocket = clientSocket;
            _clientSocketAddress =
                    new InetSocketAddress(clientSocket.getInetAddress().getHostName(), _clientSocket.getPort());
            _serverSocket = serverSocket;
            _closed = new AtomicBoolean();
            _tunnelListener = tunnelListener;
            _clientSocket.setKeepAlive(true);
            _serverSocket.setKeepAlive(true);
            _inputStreamForwarder = new ClosableStreamForwarder(new StreamForwarder(_clientSocket, _serverSocket));
            _outputStreamForwarder = new ClosableStreamForwarder(new StreamForwarder(_serverSocket, _clientSocket));
        }

        public void close()
        {
            if (_closed.compareAndSet(false, true))
            {
                try
                {
                    closeSocket(_serverSocket);
                    closeSocket(_clientSocket);
                }
                finally
                {
                    _tunnelListener.clientDisconnected(getClientAddress());
                }
            }
        }

        public void start(Executor executor) throws IOException
        {
            executor.execute(_inputStreamForwarder);
            executor.execute(_outputStreamForwarder);
            _tunnelListener.clientConnected(getClientAddress());
        }

        public boolean isClosed()
        {
            return _closed.get();
        }

        public boolean isClientAddress(final InetSocketAddress clientAddress)
        {
            return getClientAddress().equals(clientAddress);
        }

        public InetSocketAddress getClientAddress()
        {
            return _clientSocketAddress;
        }


        private static void closeSocket(Socket socket)
        {
            if (socket != null)
            {
                try
                {
                    socket.close();
                }
                catch (IOException e)
                {
                    LOGGER.warn("Exception on closing of socket {}", socket, e);
                }
            }
        }


        private class ClosableStreamForwarder implements Runnable
        {
            private StreamForwarder _streamForwarder;

            public ClosableStreamForwarder(StreamForwarder streamForwarder)
            {
                _streamForwarder = streamForwarder;
            }

            @Override
            public void run()
            {
                Thread currentThread = Thread.currentThread();
                String originalThreadName = currentThread.getName();
                try
                {
                    currentThread.setName(_streamForwarder.getName());
                    _streamForwarder.run();
                }
                finally
                {
                    close();
                    currentThread.setName(originalThreadName);
                }
            }
        }
    }

    public static class StreamForwarder implements Runnable
    {
        private static final int BUFFER_SIZE = 4096;

        private final InputStream _inputStream;
        private final OutputStream _outputStream;
        private final String _name;

        public StreamForwarder(Socket input, Socket output) throws IOException
        {
            _inputStream = input.getInputStream();
            _outputStream = output.getOutputStream();
            _name = "Forwarder-" + input.getInetAddress().getHostName() + ":" + input.getPort() + "->"
                    + output.getInetAddress().getHostName() + ":" + output.getPort();
        }

        @Override
        public void run()
        {
            byte[] buffer = new byte[BUFFER_SIZE];
            int bytesRead;
            try
            {
                while ((bytesRead = _inputStream.read(buffer)) != -1)
                {
                    _outputStream.write(buffer, 0, bytesRead);
                    _outputStream.flush();
                }
            }
            catch (IOException e)
            {
                LOGGER.warn("Exception on forwarding data for {}: {}", _name, e.getMessage());
            }
            finally
            {
                try
                {
                    _inputStream.close();
                }
                catch (IOException e)
                {
                    // ignore
                }

                try
                {
                    _outputStream.close();
                }
                catch (IOException e)
                {
                    // ignore
                }
            }
        }


        public String getName()
        {
            return _name;
        }
    }
}
