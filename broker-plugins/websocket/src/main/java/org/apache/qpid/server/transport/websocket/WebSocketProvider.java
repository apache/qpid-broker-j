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
package org.apache.qpid.server.transport.websocket;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.Principal;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.AbstractConnector;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.server.ssl.SslSelectChannelConnector;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.ThreadPool;
import org.eclipse.jetty.websocket.WebSocket;
import org.eclipse.jetty.websocket.WebSocketHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.transport.MultiVersionProtocolEngine;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.transport.MultiVersionProtocolEngineFactory;
import org.apache.qpid.server.transport.AcceptingTransport;
import org.apache.qpid.server.transport.ProtocolEngine;
import org.apache.qpid.server.transport.SchedulingDelayNotificationListener;
import org.apache.qpid.server.transport.ServerNetworkConnection;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.util.ServerScopedRuntimeException;
import org.apache.qpid.transport.ByteBufferSender;
import org.apache.qpid.transport.network.security.ssl.SSLUtil;

class WebSocketProvider implements AcceptingTransport
{
    private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketProvider.class);
    public static final String AMQP_WEBSOCKET_SUBPROTOCOL = "AMQPWSB10";
    public static final String X509_CERTIFICATES = "javax.servlet.request.X509Certificate";
    private final Transport _transport;
    private final SSLContext _sslContext;
    private final AmqpPort<?> _port;
    private final Set<Protocol> _supported;
    private final Protocol _defaultSupportedProtocolReply;
    private final MultiVersionProtocolEngineFactory _factory;
    private Server _server;

    WebSocketProvider(final Transport transport,
                      final SSLContext sslContext,
                      final AmqpPort<?> port,
                      final Set<Protocol> supported,
                      final Protocol defaultSupportedProtocolReply)
    {
        _transport = transport;
        _sslContext = sslContext;
        _port = port;
        _supported = supported;
        _defaultSupportedProtocolReply = defaultSupportedProtocolReply;

        _factory = new MultiVersionProtocolEngineFactory(
                        _port.getParent(Broker.class),
                        _supported,
                        _defaultSupportedProtocolReply,
                        _port,
                        _transport);

    }

    @Override
    public void start()
    {
        _server = new Server();

        final AbstractConnector connector;


        if (_transport == Transport.WS)
        {
            connector = new SelectChannelConnector();
        }
        else if (_transport == Transport.WSS)
        {
            SslContextFactory factory = new SslContextFactory()
                                        {
                                            @Override
                                            public String[] selectProtocols(String[] enabledProtocols, String[] supportedProtocols)
                                            {
                                                return SSLUtil.filterEnabledProtocols(enabledProtocols, supportedProtocols,
                                                                                      _port.getTlsProtocolWhiteList(),
                                                                                      _port.getTlsProtocolBlackList());
                                            }

                                            @Override
                                            public String[] selectCipherSuites(String[] enabledCipherSuites, String[] supportedCipherSuites)
                                            {
                                                return SSLUtil.filterEnabledCipherSuites(enabledCipherSuites, supportedCipherSuites,
                                                                                         _port.getTlsCipherSuiteWhiteList(),
                                                                                         _port.getTlsCipherSuiteBlackList());
                                            }

                                            @Override
                                            public void customize(final SSLEngine sslEngine)
                                            {
                                                super.customize(sslEngine);
                                                useCipherOrderIfPossible(sslEngine);
                                            }

                                            private void useCipherOrderIfPossible(final SSLEngine sslEngine)
                                            {
                                                if(_port.getTlsCipherSuiteWhiteList() != null
                                                   && !_port.getTlsCipherSuiteWhiteList().isEmpty())
                                                {
                                                    SSLUtil.useCipherOrderIfPossible(sslEngine);
                                                }
                                            }
                                        };
            factory.setSslContext(_sslContext);

            factory.setNeedClientAuth(_port.getNeedClientAuth());
            factory.setWantClientAuth(_port.getWantClientAuth());
            connector = new SslSelectChannelConnector(factory);
        }
        else
        {
            throw new IllegalArgumentException("Unexpected transport on port " + _port.getName() + ":" + _transport);
        }

        String bindingAddress = null;

        bindingAddress = _port.getBindingAddress();

        if (bindingAddress != null && !bindingAddress.trim().equals("") && !bindingAddress.trim().equals("*"))
        {
            connector.setHost(bindingAddress.trim());
        }

        connector.setPort(_port.getPort());
        _server.addConnector(connector);

        WebSocketHandler wshandler = new WebSocketHandler()
        {
            @Override
            public WebSocket doWebSocketConnect(final HttpServletRequest request, final String protocol)
            {

                Certificate certificate = null;

                if(Collections.list(request.getAttributeNames()).contains(X509_CERTIFICATES))
                {
                    X509Certificate[] certificates =
                            (X509Certificate[]) request.getAttribute(X509_CERTIFICATES);
                    if(certificates != null && certificates.length != 0)
                    {

                        certificate = certificates[0];
                    }
                }

                SocketAddress remoteAddress = new InetSocketAddress(request.getRemoteHost(), request.getRemotePort());
                SocketAddress localAddress = new InetSocketAddress(request.getLocalName(), request.getLocalPort());
                return new AmqpWebSocket(_transport, localAddress, remoteAddress, certificate, connector.getThreadPool());
            }
        };

        _server.setHandler(wshandler);
        _server.setSendServerVersion(false);
        wshandler.setHandler(new AbstractHandler()
        {
            @Override
            public void handle(final String target,
                               final Request baseRequest,
                               final HttpServletRequest request,
                               final HttpServletResponse response)
                    throws IOException, ServletException
            {
                if (response.isCommitted() || baseRequest.isHandled())
                {
                    return;
                }
                baseRequest.setHandled(true);
                response.setStatus(HttpServletResponse.SC_FORBIDDEN);


            }
        });
        try
        {
            _server.start();
        }
        catch(RuntimeException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            throw new ServerScopedRuntimeException(e);
        }

    }

    @Override
    public void close()
    {

    }

    @Override
    public int getAcceptingPort()
    {
        return _server == null || _server.getConnectors() == null || _server.getConnectors().length == 0 ? _port.getPort() : _server.getConnectors()[0].getLocalPort();
    }

    private class AmqpWebSocket implements WebSocket,WebSocket.OnBinaryMessage
    {
        private final SocketAddress _localAddress;
        private final SocketAddress _remoteAddress;
        private final Certificate _userCertificate;
        private final ThreadPool _threadPool;
        private volatile MultiVersionProtocolEngine _protocolEngine;
        private volatile ConnectionWrapper _connectionWrapper;

        private AmqpWebSocket(final Transport transport,
                              final SocketAddress localAddress,
                              final SocketAddress remoteAddress,
                              final Certificate userCertificate,
                              final ThreadPool threadPool)
        {
            _localAddress = localAddress;
            _remoteAddress = remoteAddress;
            _userCertificate = userCertificate;
            _threadPool = threadPool;
        }

        @Override
        public void onMessage(final byte[] data, final int offset, final int length)
        {
            synchronized (_connectionWrapper)
            {

                _protocolEngine.clearWork();
                try
                {
                    _protocolEngine.setIOThread(Thread.currentThread());
                    Iterator<Runnable> iter = _protocolEngine.processPendingIterator();
                    while(iter.hasNext())
                    {
                        iter.next().run();
                    }

                    QpidByteBuffer buffer = QpidByteBuffer.allocateDirect(length);
                    buffer.put(data, offset, length);
                    buffer.flip();
                    _protocolEngine.received(buffer);
                    buffer.dispose();

                    _connectionWrapper.doWrite();

                }
                finally
                {
                    _protocolEngine.setIOThread(null);
                }
            }
        }

        @Override
        public void onOpen(final Connection connection)
        {

            _protocolEngine = _factory.newProtocolEngine(_remoteAddress);

            connection.setMaxBinaryMessageSize(0);

            _connectionWrapper =
                    new ConnectionWrapper(connection, _localAddress, _remoteAddress, _protocolEngine);
            _connectionWrapper.setPeerCertificate(_userCertificate);
            _protocolEngine.setNetworkConnection(_connectionWrapper);
            _protocolEngine.setWorkListener(new Action<ProtocolEngine>()
            {
                @Override
                public void performAction(final ProtocolEngine object)
                {
                    _threadPool.dispatch(new Runnable()
                    {
                        @Override
                        public void run()
                        {
                            _connectionWrapper.doWork();
                        }
                    });
                }
            });


        }

        @Override
        public void onClose(final int closeCode, final String message)
        {
            _protocolEngine.closed();
        }
    }

    private class ConnectionWrapper implements ServerNetworkConnection, ByteBufferSender
    {
        private final WebSocket.Connection _connection;
        private final SocketAddress _localAddress;
        private final SocketAddress _remoteAddress;
        private final ConcurrentLinkedQueue<QpidByteBuffer> _buffers = new ConcurrentLinkedQueue<>();
        private final MultiVersionProtocolEngine _protocolEngine;
        private final AtomicLong _usedOutboundMessageSpace = new AtomicLong();

        private Certificate _certificate;
        private long _maxWriteIdleMillis;
        private long _maxReadIdleMillis;

        public ConnectionWrapper(final WebSocket.Connection connection,
                                 final SocketAddress localAddress,
                                 final SocketAddress remoteAddress, final MultiVersionProtocolEngine protocolEngine)
        {
            _connection = connection;
            _localAddress = localAddress;
            _remoteAddress = remoteAddress;
            _protocolEngine = protocolEngine;
        }

        @Override
        public ByteBufferSender getSender()
        {
            return this;
        }

        @Override
        public void start()
        {

        }

        @Override
        public boolean isDirectBufferPreferred()
        {
            return false;
        }

        @Override
        public void send(final QpidByteBuffer msg)
        {
            if (msg.remaining() > 0)
            {
                _buffers.add(msg.duplicate());
            }
            msg.position(msg.limit());
        }

        @Override
        public void flush()
        {

        }

        @Override
        public void close()
        {
            _connection.close();
        }

        @Override
        public SocketAddress getRemoteAddress()
        {
            return _remoteAddress;
        }

        @Override
        public SocketAddress getLocalAddress()
        {
            return _localAddress;
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
            return _certificate instanceof X509Certificate ? ((X509Certificate)_certificate).getSubjectDN() : null;
        }

        @Override
        public Certificate getPeerCertificate()
        {
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

        @Override
        public void addSchedulingDelayNotificationListeners(final SchedulingDelayNotificationListener listener)
        {
        }

        @Override
        public void removeSchedulingDelayNotificationListeners(final SchedulingDelayNotificationListener listener)
        {
        }

        @Override
        public String getTransportInfo()
        {
            return _connection.getProtocol();
        }

        @Override
        public long getScheduledTime()
        {
            return 0;
        }

        void setPeerCertificate(final Certificate certificate)
        {
            _certificate = certificate;
        }

        public synchronized void doWrite()
        {
            int size = 0;
            List<QpidByteBuffer> toBeWritten = new ArrayList<>(_buffers.size());
            QpidByteBuffer buf;
            while((buf = _buffers.poll())!= null)
            {
                size += buf.remaining();
                toBeWritten.add(buf);
            }

            byte[] data = new byte[size];
            int offset = 0;

            for(QpidByteBuffer tmp : toBeWritten)
            {
                int remaining = tmp.remaining();
                tmp.get(data, offset, remaining);
                tmp.dispose();
                offset += remaining;
            }
            if(size > 0)
            {
                try
                {
                    _connection.sendMessage(data, 0, size);
                    _usedOutboundMessageSpace.set(0);
                }
                catch (IOException e)
                {
                    LOGGER.info("Exception on write: {}", e.getMessage());
                    close();
                }
            }
        }

        public synchronized void doWork()
        {
            _protocolEngine.clearWork();
            try
            {
                _protocolEngine.setIOThread(Thread.currentThread());

                Iterator<Runnable> iter = _protocolEngine.processPendingIterator();
                while(iter.hasNext())
                {
                    iter.next().run();
                }

                doWrite();

            }
            finally
            {
                _protocolEngine.setIOThread(null);
            }

        }
    }
}
