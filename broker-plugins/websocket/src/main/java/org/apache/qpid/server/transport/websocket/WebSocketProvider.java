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
import java.lang.reflect.Field;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.security.Principal;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.io.ssl.SslHandshakeListener;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.util.thread.ThreadPool;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketPolicy;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.eclipse.jetty.websocket.server.WebSocketHandler;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeRequest;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.transport.AcceptingTransport;
import org.apache.qpid.server.transport.ByteBufferSender;
import org.apache.qpid.server.transport.MultiVersionProtocolEngine;
import org.apache.qpid.server.transport.MultiVersionProtocolEngineFactory;
import org.apache.qpid.server.transport.ProtocolEngine;
import org.apache.qpid.server.transport.SchedulingDelayNotificationListener;
import org.apache.qpid.server.transport.ServerNetworkConnection;
import org.apache.qpid.server.transport.network.Ticker;
import org.apache.qpid.server.transport.network.security.ssl.SSLUtil;
import org.apache.qpid.server.util.ServerScopedRuntimeException;


class WebSocketProvider implements AcceptingTransport
{
    private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketProvider.class);
    private static final String AMQP_WEBSOCKET_SUBPROTOCOL = "amqp";

    private final Transport _transport;
    private final SslContextFactory _sslContextFactory;
    private final AmqpPort<?> _port;
    private final Broker<?> _broker;
    private final Set<Protocol> _supported;
    private final Protocol _defaultSupportedProtocolReply;
    private final MultiVersionProtocolEngineFactory _factory;

    private Server _server;

    private final List<ConnectionWrapper> _activeConnections = new CopyOnWriteArrayList<>();

    private final WebSocketIdleTimeoutChecker _idleTimeoutChecker = new WebSocketIdleTimeoutChecker();
    private final AtomicBoolean _closed = new AtomicBoolean();

    WebSocketProvider(final Transport transport,
                      final SSLContext sslContext,
                      final AmqpPort<?> port,
                      final Set<Protocol> supported,
                      final Protocol defaultSupportedProtocolReply)
    {
        _transport = transport;
        _sslContextFactory = transport == Transport.WSS ? createSslContextFactory(port) : null;
        _port = port;
        _broker = ((Broker<?>) port.getParent());
        _supported = supported;
        _defaultSupportedProtocolReply = defaultSupportedProtocolReply;

        _factory = new MultiVersionProtocolEngineFactory(
                        _broker,
                        _supported,
                        _defaultSupportedProtocolReply,
                        _port,
                        _transport);


    }

    @Override
    public void start()
    {
        _idleTimeoutChecker.start();

        _server = new Server(new QBBTrackingThreadPool());

        final ServerConnector connector;
        HttpConnectionFactory httpConnectionFactory = new HttpConnectionFactory();
        httpConnectionFactory.getHttpConfiguration().setSendServerVersion(false);
        httpConnectionFactory.getHttpConfiguration().setSendXPoweredBy(false);

        if (_transport == Transport.WS)
        {
            connector = new ServerConnector(_server, httpConnectionFactory);
        }
        else if (_transport == Transport.WSS)
        {
            connector = new ServerConnector(_server, _sslContextFactory, httpConnectionFactory);
            connector.addBean(new SslHandshakeListener()
            {
                @Override
                public void handshakeFailed(final Event event, final Throwable failure)
                {
                    SSLEngine sslEngine = event.getSSLEngine();
                    if (LOGGER.isDebugEnabled())
                    {
                        LOGGER.info("TLS handshake failed: host='{}', port={}",
                                    sslEngine.getPeerHost(),
                                    sslEngine.getPeerPort(),
                                    failure);
                    }
                    else
                    {
                        LOGGER.info("TLS handshake failed: host='{}', port={}: {}",
                                    sslEngine.getPeerHost(),
                                    sslEngine.getPeerPort(),
                                    String.valueOf(failure));
                    }
                }
            });
        }
        else
        {
            throw new IllegalArgumentException("Unexpected transport on port " + _port.getName() + ":" + _transport);
        }

        String bindingAddress = _port.getBindingAddress();

        if (bindingAddress != null && !bindingAddress.trim().equals("") && !bindingAddress.trim().equals("*"))
        {
            connector.setHost(bindingAddress.trim());
        }

        connector.setPort(_port.getPort());
        _server.addConnector(connector);

        WebSocketHandler wshandler = new WebSocketHandler()
        {
            @Override
            public void configure(final WebSocketServletFactory factory)
            {
                factory.setCreator((req, resp) ->
                                   {
                                       resp.setAcceptedSubProtocol(AMQP_WEBSOCKET_SUBPROTOCOL);
                                       return new AmqpWebSocket();
                                   });
            }

            @Override
            public void configurePolicy(final WebSocketPolicy policy)
            {
                super.configurePolicy(policy);
                // It seems Jetty internally makes provision for maxBinaryMessageSize to be unconstrained, but the
                // policy does not allow it to be configured it that way.
                // See https://github.com/eclipse/jetty.project/issues/488
                try
                {
                    Field maxBinaryMessageSize = policy.getClass().getDeclaredField("maxBinaryMessageSize");
                    maxBinaryMessageSize.setAccessible(true);
                    maxBinaryMessageSize.set(policy, 0);
                }
                catch (IllegalAccessException | NoSuchFieldException e)
                {
                    LOGGER.warn("Could not override maxBinaryMessageSize", e);
                }
            }
        };

        _server.setHandler(wshandler);
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

    private SslContextFactory createSslContextFactory(final AmqpPort<?> port)
    {
        SslContextFactory.Server sslContextFactory = new SslContextFactory.Server()
        {
            @Override
            public void customize(final SSLEngine sslEngine)
            {
                super.customize(sslEngine);
                SSLUtil.updateEnabledCipherSuites(sslEngine,
                                                  port.getTlsCipherSuiteWhiteList(),
                                                  port.getTlsCipherSuiteBlackList());
                SSLUtil.updateEnabledTlsProtocols(sslEngine,
                                                  port.getTlsProtocolWhiteList(),
                                                  port.getTlsProtocolBlackList());

                if (port.getTlsCipherSuiteWhiteList() != null
                    && !port.getTlsCipherSuiteWhiteList().isEmpty())
                {
                    SSLParameters sslParameters = sslEngine.getSSLParameters();
                    sslParameters.setUseCipherSuitesOrder(true);
                    sslEngine.setSSLParameters(sslParameters);
                }
            }
        };
        sslContextFactory.setSslContext(port.getSSLContext());
        sslContextFactory.setNeedClientAuth(port.getNeedClientAuth());
        sslContextFactory.setWantClientAuth(port.getWantClientAuth());
        return sslContextFactory;
    }

    @Override
    public void close()
    {
        _closed.set(true);
        _idleTimeoutChecker.wakeup();
        try
        {
            _server.stop();
        }
        catch (Exception e)
        {
            LOGGER.warn("Error closing the web socket for : " +  _port.getPort(), e);
            _server = null;
        }
    }

    @Override
    public int getAcceptingPort()
    {
        Server server = _server;
        return server == null || server.getConnectors().length == 0 || !(server.getConnectors()[0] instanceof ServerConnector) ?
                _port.getPort() :
                ((ServerConnector) server.getConnectors()[0]).getLocalPort();
    }

    @Override
    public boolean updatesSSLContext()
    {
        if (_sslContextFactory != null)
        {
            try
            {
                _sslContextFactory.reload(f -> {
                    f.setSslContext(_port.getSSLContext());
                    f.setNeedClientAuth(_port.getNeedClientAuth());
                    f.setWantClientAuth(_port.getWantClientAuth());
                });
                return true;
            }
            catch (Exception e)
            {
                throw new IllegalConfigurationException("Unexpected exception on reload of ssl context factory", e);
            }
        }
        return false;
    }

    private static class QBBTrackingThreadPool extends QueuedThreadPool
    {
        private final ThreadFactory _threadFactory = QpidByteBuffer.createQpidByteBufferTrackingThreadFactory(
                QBBTrackingThreadPool.super::newThread);

        @Override
        public Thread newThread(final Runnable runnable)
        {
            return _threadFactory.newThread(runnable);
        }
    }

    @WebSocket
    public class AmqpWebSocket
    {
        private volatile QpidByteBuffer _netInputBuffer;
        private volatile MultiVersionProtocolEngine _protocolEngine;
        private volatile ConnectionWrapper _connectionWrapper;
        private volatile boolean _unexpectedByteBufferSizeReported;

        AmqpWebSocket()
        {
            _netInputBuffer = QpidByteBuffer.allocateDirect(_broker.getNetworkBufferSize());
        }

        @OnWebSocketConnect @SuppressWarnings("unused")
        public void onWebSocketConnect(final Session session)
        {
            SocketAddress localAddress = session.getLocalAddress();
            SocketAddress remoteAddress = session.getRemoteAddress();
            _protocolEngine = _factory.newProtocolEngine(remoteAddress);

            // Let AMQP do timeout handling
            session.setIdleTimeout(0);

            _connectionWrapper = new ConnectionWrapper(session, localAddress, remoteAddress, _protocolEngine, _server.getThreadPool());
            if (session.getUpgradeRequest() instanceof ServletUpgradeRequest)
            {
                ServletUpgradeRequest upgradeRequest = (ServletUpgradeRequest) session.getUpgradeRequest();
                if (upgradeRequest.getCertificates() != null && upgradeRequest.getCertificates().length > 0)
                {
                    _connectionWrapper.setPeerCertificate(upgradeRequest.getCertificates()[0]);
                }
            }
            _protocolEngine.setNetworkConnection(_connectionWrapper);
            _protocolEngine.setWorkListener(object -> _server.getThreadPool().execute(() -> _connectionWrapper.doWork()));
            _activeConnections.add(_connectionWrapper);
            _idleTimeoutChecker.wakeup();

        }

        @OnWebSocketMessage @SuppressWarnings("unused")
        public void onWebSocketBinary(Session sess, final byte[] payload, int offset, final int len)
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

                    int lastRead;
                    int remaining = len;
                    do
                    {
                        int chunkLen = Math.min(remaining, _netInputBuffer.remaining());
                        _netInputBuffer.put(payload, offset, chunkLen);
                        remaining -= chunkLen;
                        offset += chunkLen;

                        _netInputBuffer.flip();
                        _protocolEngine.received(_netInputBuffer);
                        _connectionWrapper.doWrite();
                        restoreApplicationBufferForWrite();
                    }
                    while(remaining > 0);

                    if (LOGGER.isDebugEnabled())
                    {
                        LOGGER.debug("Read {} byte(s)", len);
                    }
                }
                finally
                {
                    _protocolEngine.setIOThread(null);
                }
            }
            _idleTimeoutChecker.wakeup();
        }

        private void restoreApplicationBufferForWrite()
        {
            try (QpidByteBuffer oldNetInputBuffer = _netInputBuffer)
            {
                int unprocessedDataLength = _netInputBuffer.remaining();

                _netInputBuffer.limit(_netInputBuffer.capacity());
                _netInputBuffer = oldNetInputBuffer.slice();
                _netInputBuffer.limit(unprocessedDataLength);
            }
            if (_netInputBuffer.limit() != _netInputBuffer.capacity())
            {
                _netInputBuffer.position(_netInputBuffer.limit());
                _netInputBuffer.limit(_netInputBuffer.capacity());
            }
            else
            {
                try (QpidByteBuffer currentBuffer = _netInputBuffer)
                {
                    int newBufSize;

                    if (currentBuffer.capacity() < _broker.getNetworkBufferSize())
                    {
                        newBufSize = _broker.getNetworkBufferSize();
                    }
                    else
                    {
                        newBufSize = currentBuffer.capacity() + _broker.getNetworkBufferSize();
                        reportUnexpectedByteBufferSizeUsage();
                    }

                    _netInputBuffer = QpidByteBuffer.allocateDirect(newBufSize);
                    _netInputBuffer.put(currentBuffer);
                }
            }
        }

        private void reportUnexpectedByteBufferSizeUsage()
        {
            if (!_unexpectedByteBufferSizeReported)
            {
                LOGGER.info("At least one frame unexpectedly does not fit into default byte buffer size ({}B) on a connection {}.",
                            _broker.getNetworkBufferSize(), this.toString());
                _unexpectedByteBufferSizeReported = true;
            }
        }
        
        /** AMQP frames MUST be sent as binary data payloads of WebSocket messages.*/
        @OnWebSocketMessage @SuppressWarnings("unused")
        public void onWebSocketText(Session sess, String text)
        {
            LOGGER.info("Unexpected websocket text message received, closing connection");
            sess.close();
        }

        @OnWebSocketClose @SuppressWarnings("unused")
        public void onWebSocketClose(final int statusCode, final String reason)
        {
            if (_protocolEngine != null)
            {
                _protocolEngine.closed();
            }
            _activeConnections.remove(_connectionWrapper);
            _idleTimeoutChecker.wakeup();
            _netInputBuffer.dispose();
        }
    }

    private class ConnectionWrapper implements ServerNetworkConnection, ByteBufferSender
    {
        private final Session _connection;
        private final SocketAddress _localAddress;
        private final SocketAddress _remoteAddress;
        private final ConcurrentLinkedQueue<QpidByteBuffer> _buffers = new ConcurrentLinkedQueue<>();
        private final MultiVersionProtocolEngine _protocolEngine;
        private final ThreadPool _threadPool;
        private final Runnable _tickJob;

        private Certificate _certificate;
        private long _maxWriteIdleMillis;
        private long _maxReadIdleMillis;

        public ConnectionWrapper(final Session connection,
                                 final SocketAddress localAddress,
                                 final SocketAddress remoteAddress,
                                 final MultiVersionProtocolEngine protocolEngine,
                                 final ThreadPool threadPool)
        {
            _connection = connection;
            _localAddress = localAddress;
            _remoteAddress = remoteAddress;
            _protocolEngine = protocolEngine;
            _threadPool = threadPool;
            _tickJob = new Runnable()
                        {
                            @Override
                            public void run()
                            {
                                synchronized (ConnectionWrapper.this)
                                {
                                    protocolEngine.getAggregateTicker().tick(System.currentTimeMillis());
                                    doWrite();
                                }
                            }
                        };
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
            return _connection.getProtocolVersion();
        }

        @Override
        public long getScheduledTime()
        {
            return 0;
        }

        @Override
        public String getSelectedHost()
        {
            return null;
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
                // TODO: For efficiency perhaps only coalesce sequential small buffers and let large buffers
                // go alone in a binary message.  This would likely avoid the memory copies of large transfer payloads
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
                    _connection.getRemote().sendBytes(ByteBuffer.wrap(data));
                    if (LOGGER.isDebugEnabled())
                    {
                        LOGGER.debug("Written {} byte(s)", data.length);
                    }
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
                _idleTimeoutChecker.wakeup();
            }
            finally
            {
                _protocolEngine.setIOThread(null);
            }

        }


        public void tick()
        {
            _threadPool.execute(_tickJob);
        }
    }



    private class WebSocketIdleTimeoutChecker extends Thread
    {

        public WebSocketIdleTimeoutChecker()
        {
            setName("WebSocket Idle Checker: " + _port);
        }

        @Override
        public void run()
        {
            while(!_closed.get())
            {
                ConnectionWrapper connectionToTick = null;
                long currentTime = System.currentTimeMillis();
                synchronized (this)
                {
                    long nextTick = Long.MAX_VALUE;
                    for(ConnectionWrapper connection : _activeConnections)
                    {
                        ProtocolEngine engine = connection._protocolEngine;
                        final Ticker ticker = engine.getAggregateTicker();
                        long tick = ticker.getTimeToNextTick(currentTime);
                        if(tick <= 0)
                        {
                            connectionToTick = connection;
                            nextTick = -1;
                            break;
                        }
                        else if(tick < nextTick)
                        {
                            nextTick = tick;
                        }
                    }
                    if(nextTick > 0)
                    {
                        try
                        {
                            wait(nextTick);
                        }
                        catch (InterruptedException e)
                        {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }
                if(connectionToTick != null)
                {
                    connectionToTick.tick();
                }
            }
        }

        private synchronized void wakeup()
        {
            notifyAll();
        }
    }
}
