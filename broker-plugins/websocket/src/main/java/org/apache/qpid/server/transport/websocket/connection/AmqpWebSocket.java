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

import org.eclipse.jetty.util.thread.ThreadPool;
import org.eclipse.jetty.websocket.api.Callback;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.StatusCode;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketError;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketOpen;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.transport.MultiVersionProtocolEngine;
import org.apache.qpid.server.transport.MultiVersionProtocolEngineFactory;

@WebSocket
public final class AmqpWebSocket
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AmqpWebSocket.class);

    private final MultiVersionProtocolEngineFactory _factory;
    private final ThreadPool _threadPool;
    private final WebSocketConnectionScheduler _connectionScheduler;
    private final WebSocketSettings _settings;
    private final X509Certificate[] _certificates;
    private final WebSocketReceiver _receiver;

    private volatile MultiVersionProtocolEngine _protocolEngine;
    private volatile WebSocketConnection _connection;

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
        _receiver = new WebSocketReceiver(settings);
    }

    @OnWebSocketOpen
    @SuppressWarnings("unused")
    public void onWebSocketConnect(final Session session)
    {
        final SocketAddress localAddress = session.getLocalSocketAddress();
        final SocketAddress remoteAddress = session.getRemoteSocketAddress();
        _protocolEngine = _factory.newProtocolEngine(remoteAddress);

        // Let AMQP do normal idle-timeout handling. Closing sessions are governed by a separate absolute deadline
        // below, so peer activity cannot postpone transport teardown.
        session.setIdleTimeout(Duration.ZERO);

        _connection = new WebSocketConnection(session, localAddress, remoteAddress, _protocolEngine,
                 _threadPool, _connectionScheduler, _settings);

        if (_certificates != null && _certificates.length > 0)
        {
            _connection.setPeerCertificate(_certificates[0]);
        }
        _protocolEngine.setNetworkConnection(_connection);
        _protocolEngine.setWorkListener(object -> _connection.scheduleWork());
        _connectionScheduler.registerConnection(_connection);
    }

    @OnWebSocketMessage
    @SuppressWarnings("unused")
    public void onWebSocketBinary(final ByteBuffer payload, final boolean last, final Callback callback)
    {
        final int length = payload.remaining();
        try
        {
            _receiver.receiveBinary(payload, _connection, _protocolEngine);
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
    @SuppressWarnings("unused")
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
    @SuppressWarnings("unused")
    public void onWebSocketError(final Throwable failure)
    {
        final WebSocketConnection connection = _connection;
        if (connection == null)
        {
            LOGGER.warn("WebSocket error before connection initialization", failure);
        }
        else if (failure instanceof ClosedChannelException &&
                (!connection.isOpen() || _protocolEngine.isProtocolCloseComplete()))
        {
            // AMQP can finish closing before transport shutdown starts. Jetty's own closed state alone
            // cannot distinguish that expected EOF from an abrupt loss of an active AMQP connection.
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
    @SuppressWarnings("unused")
    public void onWebSocketClose(final int statusCode, final String reason, final Callback callback)
    {
        final WebSocketConnection connection = _connection;
        if (connection == null)
        {
            completeClose();
        }
        else
        {
            connection.webSocketClosed(this::completeClose);
        }
        callback.succeed();
    }

    private void completeClose()
    {
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
