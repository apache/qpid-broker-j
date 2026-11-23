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
package org.apache.qpid.server.transport.websocket.connection;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Objects;

import org.eclipse.jetty.io.Connection;
import org.eclipse.jetty.io.EndPoint;
import org.eclipse.jetty.util.thread.ThreadPool;
import org.eclipse.jetty.websocket.api.Callback;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.StatusCode;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketError;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketOpen;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.eclipse.jetty.websocket.common.WebSocketSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.transport.MultiVersionProtocolEngine;
import org.apache.qpid.server.transport.MultiVersionProtocolEngineFactory;
import org.apache.qpid.server.transport.websocket.connection.WebSocketServerConnector.WebSocketEndPoint;

@WebSocket
public final class AmqpWebSocket implements Connection.Listener
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AmqpWebSocket.class);

    private final MultiVersionProtocolEngineFactory _factory;
    private final ThreadPool _threadPool;
    private final WebSocketConnectionScheduler _connectionScheduler;
    private final WebSocketSettings _settings;
    private final X509Certificate[] _certificates;

    private volatile MultiVersionProtocolEngine _protocolEngine;
    private volatile WebSocketConnection _connection;
    private WebSocketReceiver _receiver;
    private boolean _resourcesReleased;

    public AmqpWebSocket(final MultiVersionProtocolEngineFactory factory,
                         final ThreadPool threadPool,
                         final WebSocketConnectionScheduler connectionScheduler,
                         final WebSocketSettings settings,
                         final X509Certificate[] certificates)
    {
        _factory = factory;
        _threadPool = threadPool;
        _connectionScheduler = connectionScheduler;
        _settings = Objects.requireNonNull(settings, "WebSocket settings must not be null");
        _certificates = certificates;
    }

    @OnWebSocketOpen
    public void onWebSocketConnect(final Session session)
    {
        Connection transport = null;
        try
        {
            transport = initializeConnection(session);
        }
        finally
        {
            if (transport == null)
            {
                completeClose();
            }
        }
        if (transport == null)
        {
            session.close(StatusCode.TRY_AGAIN_LATER, "Connection rejected", Callback.NOOP);
        }
        else if (!transport.getEndPoint().isOpen())
        {
            onClosed(transport);
        }
    }

    private synchronized Connection initializeConnection(final Session session)
    {
        if (_resourcesReleased)
        {
            return null;
        }
        final SocketAddress localAddress = session.getLocalSocketAddress();
        final SocketAddress remoteAddress = session.getRemoteSocketAddress();
        _protocolEngine = _factory.newProtocolEngine(remoteAddress);
        if (_protocolEngine == null)
        {
            return null;
        }
        _receiver = new WebSocketReceiver(_settings);

        session.setIdleTimeout(Duration.ZERO);

        final DeadlineWebSocketCoreSession coreSession =
                (DeadlineWebSocketCoreSession) ((WebSocketSession) session).getCoreSession();
        final Connection transport = coreSession.getConnection();
        final EndPoint endPoint = transport.getEndPoint();
        final EndPoint tcpEndPoint = endPoint instanceof EndPoint.Wrapper wrapper ? wrapper.unwrap() : endPoint;
        final WebSocketConnection connection = new WebSocketConnection(session, (WebSocketEndPoint) tcpEndPoint,
                localAddress, remoteAddress, _protocolEngine, _threadPool, _connectionScheduler, _settings);

        if (_certificates != null && _certificates.length > 0)
        {
            connection.setPeerCertificate(_certificates[0]);
        }
        _protocolEngine.setNetworkConnection(connection);
        _protocolEngine.setWorkListener(object -> connection.scheduleWork());

        transport.addEventListener(this);
        try
        {
            _connectionScheduler.registerConnection(connection);
        }
        catch (final RuntimeException | Error failure)
        {
            _connectionScheduler.unregisterConnection(connection);
            throw failure;
        }

        _connection = connection;
        coreSession.setBrokerConnection(connection);
        return transport;
    }

    @OnWebSocketMessage
    public void onWebSocketBinary(final ByteBuffer payload, final boolean last, final Callback callback)
    {
        final WebSocketConnection connection = _connection;
        if (connection == null)
        {
            callback.succeed();
            return;
        }

        final int length = payload.remaining();
        try
        {
            _receiver.receiveBinary(payload, connection, _protocolEngine);
            callback.succeed();

            if (LOGGER.isDebugEnabled())
            {
                LOGGER.debug("Read {} byte(s)", length);
            }
        }
        finally
        {
            _connectionScheduler.wakeup();
        }
    }

    private void disposeReceiver()
    {
        final WebSocketReceiver receiver = _receiver;
        if (receiver != null)
        {
            receiver.dispose();
        }
    }

    @OnWebSocketMessage
    public void onWebSocketText(final Session session, final String text)
    {
        LOGGER.info("Unexpected websocket text message received, closing connection");
        final WebSocketConnection connection = _connection;
        if (connection == null)
        {
            session.close(StatusCode.BAD_DATA, null, Callback.NOOP);
        }
        else
        {
            connection.close(StatusCode.BAD_DATA, false);
        }
    }

    @OnWebSocketError
    public void onWebSocketError(final Throwable failure)
    {
        final WebSocketConnection connection = _connection;
        if (connection == null)
        {
            LOGGER.warn("WebSocket error before connection initialization", failure);
        }
        else if (failure instanceof ClosedChannelException && connection.isCloseExpected())
        {
            if (LOGGER.isDebugEnabled())
            {
                LOGGER.debug("WebSocket connection {} closed after protocol or transport shutdown",
                        connection.getRemoteAddress(), failure);
            }
        }
        else
        {
            LOGGER.warn("WebSocket error for connection {}", connection.getRemoteAddress(), failure);
        }
    }

    @OnWebSocketClose
    public void onWebSocketClose(final int statusCode, final String reason, final Callback callback)
    {
        final WebSocketConnection connection;
        synchronized (this)
        {
            connection = _connection;
            if (connection == null)
            {
                completeClose();
            }
        }
        if (connection == null)
        {
            callback.succeed();
        }
        else
        {
            connection.webSocketCloseReceived(callback);
        }
    }

    @Override
    public void onClosed(final Connection transport)
    {
        final WebSocketConnection connection = _connection;
        if (connection != null)
        {
            connection.webSocketClosed(this::completeClose);
        }
    }

    private void completeClose()
    {
        synchronized (this)
        {
            if (_resourcesReleased)
            {
                return;
            }
            _resourcesReleased = true;
        }
        try
        {
            if (_protocolEngine != null)
            {
                _protocolEngine.closed();
            }
        }
        finally
        {
            disposeReceiver();
        }
    }
}
