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
import java.net.InetAddress;
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
public class TCPTunneler implements AutoCloseable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TCPTunneler.class);
    private static final int BUFFER_SIZE = 4096;
    private final TCPWorker _tcpWorker;

    private final ExecutorService _executor;
    private final int _bufferSize;

    public TCPTunneler(final int localPort, final String remoteHost,
                       final int remotePort,
                       final int numberOfConcurrentClients,
                       final int bufferSize)

    {
        _executor = Executors.newFixedThreadPool(numberOfConcurrentClients * 2 + 1);
        _tcpWorker = new TCPWorker(localPort, remoteHost, remotePort, bufferSize, _executor);
        _bufferSize = bufferSize;
    }

    public TCPTunneler(final int localPort, final String remoteHost,
                       final int remotePort,
                       final int numberOfConcurrentClients)
    {
        this(localPort, remoteHost, remotePort, numberOfConcurrentClients, BUFFER_SIZE);
    }

    public void start() throws IOException
    {
        _tcpWorker.start();
    }

    public void stopClientToServerForwarding(final InetSocketAddress clientAddress)
    {
        _tcpWorker.stopClientToServerForwarding(clientAddress);
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

    public int getLocalPort()
    {
        return _tcpWorker.getLocalPort();
    }

    @Override
    public void close() throws Exception
    {
        stop();
    }

    public interface TunnelListener
    {
        void clientConnected(InetSocketAddress clientAddress);

        void clientDisconnected(InetSocketAddress clientAddress);

        void notifyClientToServerBytesDelivered(InetAddress inetAddress, int numberOfBytesForwarded);

        void notifyServerToClientBytesDelivered(InetAddress inetAddress, int numberOfBytesForwarded);
    }

    private static class TCPWorker implements Runnable
    {
        private final String _remoteHost;
        private final int _remotePort;
        private final int _localPort;
        private final String _remoteHostPort;
        private final AtomicBoolean _closed;
        private final Collection<SocketTunnel> _tunnels;
        private final Collection<TunnelListener> _tunnelListeners;
        private final TunnelListener _notifyingListener;
        private final int _bufferSize;
        private volatile ServerSocket _serverSocket;
        private volatile ExecutorService _executor;
        private int _actualLocalPort;

        public TCPWorker(final int localPort,
                         final String remoteHost,
                         final int remotePort,
                         final int bufferSize, final ExecutorService executor)
        {
            _bufferSize = bufferSize;
            _closed = new AtomicBoolean();
            _remoteHost = remoteHost;
            _remotePort = remotePort;
            _localPort = localPort;
            _remoteHostPort = _remoteHost + ":" + _remotePort;
            _executor = executor;
            _tunnels = new CopyOnWriteArrayList<>();
            _tunnelListeners = new CopyOnWriteArrayList<>();
            _notifyingListener = new TunnelListener()
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

                @Override
                public void notifyClientToServerBytesDelivered(final InetAddress inetAddress,
                                                               final int numberOfBytesForwarded)
                {
                    for (TunnelListener listener : _tunnelListeners)
                    {
                        listener.notifyClientToServerBytesDelivered(inetAddress, numberOfBytesForwarded);
                    }
                }

                @Override
                public void notifyServerToClientBytesDelivered(final InetAddress inetAddress,
                                                               final int numberOfBytesForwarded)
                {
                    for (TunnelListener listener : _tunnelListeners)
                    {
                        listener.notifyClientToServerBytesDelivered(inetAddress, numberOfBytesForwarded);
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
                    Socket acceptedSocket = _serverSocket.accept();
                    LOGGER.debug("Client opened socket {}", acceptedSocket);

                    createTunnel(acceptedSocket);
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
            _actualLocalPort = _localPort;
            try
            {
                _serverSocket = new ServerSocket(_localPort);
                _actualLocalPort = _serverSocket.getLocalPort();
                LOGGER.info("Starting TCPTunneler forwarding from port {} to {}",
                            _actualLocalPort, _remoteHostPort);
                _serverSocket.setReuseAddress(true);
            }
            catch (IOException e)
            {
                throw new RuntimeException("Cannot start TCPTunneler on port " + _actualLocalPort, e);
            }

            if (_serverSocket != null)
            {
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
                        throw new RuntimeException("Cannot start acceptor thread for TCPTunneler on port " + _actualLocalPort,
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
                            _actualLocalPort,
                            _remoteHostPort);
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
                            _actualLocalPort,
                            _remoteHostPort);
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


        private void createTunnel(final Socket localSocket)
        {
            Socket remoteSocket = null;
            try
            {
                LOGGER.debug("Opening socket to {} for {}", _remoteHostPort, localSocket);
                remoteSocket = new Socket(_remoteHost, _remotePort);
                LOGGER.debug("Opened socket to {} for {}", remoteSocket, localSocket);
                SocketTunnel tunnel = new SocketTunnel(localSocket, remoteSocket, _notifyingListener, _bufferSize);
                LOGGER.debug("Socket tunnel is created from {} to {}", localSocket, remoteSocket);
                _tunnels.add(tunnel);
                tunnel.start(_executor);
            }
            catch (Exception e)
            {
                LOGGER.error("Cannot forward i/o traffic between {} and {}", localSocket, _remoteHostPort, e);
                SocketTunnel.closeSocket(localSocket);
                SocketTunnel.closeSocket(remoteSocket);
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

        public void stopClientToServerForwarding(final InetSocketAddress clientAddress)
        {
            SocketTunnel target = null;
            for (SocketTunnel tunnel : _tunnels)
            {
                if (tunnel.getClientAddress().equals(clientAddress))
                {
                    target = tunnel;
                    break;
                }
            }
            if (target != null)
            {
                LOGGER.debug("Stopping forwarding from client {} to server", clientAddress);
                target.stopClientToServerForwarding();
            }
            else
            {
                throw new IllegalArgumentException("Could not find tunnel for address " + clientAddress);
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

        public int getLocalPort()
        {
            if (_serverSocket == null)
            {
                return -1;
            }
            return _serverSocket.getLocalPort();
        }
    }

    private static class SocketTunnel
    {
        private final Socket _clientSocket;
        private final Socket _serverSocket;
        private final TunnelListener _tunnelListener;
        private final AtomicBoolean _closed;
        private final AutoClosingStreamForwarder _clientToServer;
        private final AutoClosingStreamForwarder _serverToClient;
        private final InetSocketAddress _clientSocketAddress;

        SocketTunnel(final Socket clientSocket,
                     final Socket serverSocket,
                     final TunnelListener tunnelListener,
                     final int bufferSize) throws IOException
        {
            _clientSocket = clientSocket;
            _clientSocketAddress =
                    new InetSocketAddress(clientSocket.getInetAddress().getHostName(), _clientSocket.getPort());
            _serverSocket = serverSocket;
            _closed = new AtomicBoolean();
            _tunnelListener = tunnelListener;
            _clientSocket.setKeepAlive(true);
            _serverSocket.setKeepAlive(true);
            _clientToServer = new AutoClosingStreamForwarder(new StreamForwarder(_clientSocket,
                                                                                 _serverSocket,
                                                                                 bufferSize,
                                                                                 numBytes -> _tunnelListener.notifyClientToServerBytesDelivered(_clientSocket.getInetAddress(),
                                                                                                                                                numBytes)));
            _serverToClient = new AutoClosingStreamForwarder(new StreamForwarder(_serverSocket,
                                                                                 _clientSocket,
                                                                                 bufferSize,
                                                                                 numBytes -> _tunnelListener.notifyServerToClientBytesDelivered(_serverSocket.getInetAddress(), numBytes)));
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
            executor.execute(_clientToServer);
            executor.execute(_serverToClient);
            _tunnelListener.clientConnected(getClientAddress());
        }

        public void stopClientToServerForwarding()
        {
            _clientToServer.stopForwarding();
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


        private class AutoClosingStreamForwarder implements Runnable
        {
            private StreamForwarder _streamForwarder;

            public AutoClosingStreamForwarder(StreamForwarder streamForwarder)
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

            public void stopForwarding()
            {
                _streamForwarder.stopForwarding();
            }
        }
    }

    private static class StreamForwarder implements Runnable
    {
        private final int _bufferSize;

        private final InputStream _inputStream;
        private final OutputStream _outputStream;
        private final String _name;
        private final ForwardCallback _forwardCallback;
        private final AtomicBoolean _stopForwarding = new AtomicBoolean();

        public StreamForwarder(Socket input,
                               Socket output,
                               final int bufferSize,
                               final ForwardCallback forwardCallback) throws IOException
        {
            _inputStream = input.getInputStream();
            _outputStream = output.getOutputStream();
            _forwardCallback = forwardCallback == null ? numberOfBytesForwarded -> {} : forwardCallback;
            _name = "Forwarder-" + input.getLocalSocketAddress() + "->" + output.getRemoteSocketAddress();
            _bufferSize = bufferSize;
        }

        @Override
        public void run()
        {
            byte[] buffer = new byte[_bufferSize];
            int bytesRead;
            try
            {
                while ((bytesRead = _inputStream.read(buffer)) != -1)
                {
                    if (!_stopForwarding.get())
                    {
                        _outputStream.write(buffer, 0, bytesRead);
                        _outputStream.flush();
                        _forwardCallback.notify(bytesRead);
                        LOGGER.debug("Forwarded {} byte(s)", bytesRead);
                    }
                    else
                    {
                        LOGGER.debug("Discarded {} byte(s)", bytesRead);
                    }
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

        public void stopForwarding()
        {
            _stopForwarding.set(true);
        }

    }

    public interface ForwardCallback
    {
        void notify(int numberOfBytesForwarded);
    }
}
