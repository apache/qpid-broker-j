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

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import jakarta.servlet.http.HttpServletResponse;
import org.eclipse.jetty.ee11.servlet.ServletContextHandler;
import org.eclipse.jetty.ee11.servlet.ServletHolder;
import org.eclipse.jetty.ee11.websocket.server.JettyWebSocketCreator;
import org.eclipse.jetty.ee11.websocket.server.JettyWebSocketServerContainer;
import org.eclipse.jetty.ee11.websocket.server.JettyWebSocketServlet;
import org.eclipse.jetty.ee11.websocket.server.JettyWebSocketServletFactory;
import org.eclipse.jetty.io.ssl.SslHandshakeListener;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.websocket.core.server.WebSocketServerComponents;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.transport.AcceptingTransport;
import org.apache.qpid.server.transport.MultiVersionProtocolEngineFactory;
import org.apache.qpid.server.transport.websocket.connection.AmqpWebSocket;
import org.apache.qpid.server.transport.websocket.connection.WebSocketConnectionScheduler;
import org.apache.qpid.server.transport.websocket.connection.WebSocketSettings;
import org.apache.qpid.server.util.ServerScopedRuntimeException;

class WebSocketProvider implements AcceptingTransport
{
    private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketProvider.class);
    private static final String AMQP_WEBSOCKET_SUBPROTOCOL = "amqp";

    private final Transport _transport;
    private final PortSslContextFactory _sslContextFactory;
    private final AmqpPort<?> _port;
    private final MultiVersionProtocolEngineFactory _factory;
    private final WebSocketConnectionScheduler _connectionScheduler;
    private final WebSocketSettings _settings;

    private Server _server;

    WebSocketProvider(final Transport transport,
                      final SSLContext sslContext,
                      final AmqpPort<?> port,
                      final Set<Protocol> supported,
                      final Protocol defaultSupportedProtocolReply)
    {
        this(transport, sslContext, port, supported, defaultSupportedProtocolReply,
             System::currentTimeMillis, System::nanoTime);
    }

    WebSocketProvider(final Transport transport,
                      final SSLContext sslContext,
                      final AmqpPort<?> port,
                      final Set<Protocol> supported,
                      final Protocol defaultSupportedProtocolReply,
                      final LongSupplier currentTimeSupplier,
                      final LongSupplier nanoTimeSupplier)
    {
        _transport = transport;
        _sslContextFactory = transport == Transport.WSS ? new PortSslContextFactory(port) : null;
        _port = port;
        final Broker<?> broker = (Broker<?>) port.getParent();
        // This timeout already bounds final transport writes for TCP. For WebSocket it bounds both draining the final
        // AMQP output and waiting for the peer's WebSocket CLOSE response.
        final Long finalWriteTimeout = port.getContextValue(Long.class, AmqpPort.FINAL_WRITE_TIMEOUT);
        final long finalWriteTimeoutMillis = finalWriteTimeout == null
                ? AmqpPort.DEFAULT_FINAL_WRITE_TIMEOUT
                : Math.max(0L, finalWriteTimeout);
        final long webSocketCloseTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(finalWriteTimeoutMillis);
        _connectionScheduler = new WebSocketConnectionScheduler("WebSocket Idle Checker: " + port,
                currentTimeSupplier, nanoTimeSupplier, webSocketCloseTimeoutNanos);
        _settings = new WebSocketSettings(broker.getNetworkBufferSize());
        _factory = new MultiVersionProtocolEngineFactory(broker, supported, defaultSupportedProtocolReply, _port,
                _transport);
    }

    @Override
    public void start()
    {
        _connectionScheduler.start();

        _server = new Server(new QpidByteBufferTrackingThreadPool());

        final ServerConnector connector = createConnector();
        _server.addConnector(connector);

        final ServletContextHandler servletContextHandler = createServletContext(createWebSocketCreator());

        final ContextHandlerCollection handlers = new ContextHandlerCollection();
        handlers.addHandler(servletContextHandler);
        handlers.addHandler(createFallbackHandler());
        _server.setHandler(handlers);

        try
        {
            _server.start();
        }
        catch (final RuntimeException e)
        {
            throw e;
        }
        catch (final Exception e)
        {
            throw new ServerScopedRuntimeException(e);
        }
    }

    private ServerConnector createConnector()
    {
        final ServerConnector connector;
        final HttpConnectionFactory httpConnectionFactory = new HttpConnectionFactory();
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
                    final SSLEngine sslEngine = event.getSSLEngine();
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

        final String bindingAddress = _port.getBindingAddress();

        if (bindingAddress != null && !bindingAddress.trim().isEmpty() && !"*".equals(bindingAddress.trim()))
        {
            connector.setHost(bindingAddress.trim());
        }

        connector.setPort(_port.getPort());
        return connector;
    }

    private JettyWebSocketCreator createWebSocketCreator()
    {
        return (request, response) ->
        {
            response.setAcceptedSubProtocol(AMQP_WEBSOCKET_SUBPROTOCOL);
            return new AmqpWebSocket(_factory, _server.getThreadPool(), _connectionScheduler, _settings,
                    request.getCertificates());
        };
    }

    private ServletContextHandler createServletContext(final JettyWebSocketCreator jettyWebSocketCreator)
    {
        final JettyWebSocketServlet websocketServlet = new JettyWebSocketServlet()
        {
            @Override
            public void configure(final JettyWebSocketServletFactory factory)
            {
                factory.setMaxBinaryMessageSize(0L);
                factory.setCreator(jettyWebSocketCreator);
            }
        };

        final ServletContextHandler servletContextHandler = new ServletContextHandler();
        servletContextHandler.setContextPath("/");
        servletContextHandler.addServlet(new ServletHolder(websocketServlet), "");
        servletContextHandler.setServer(_server);

        WebSocketServerComponents.ensureWebSocketComponents(_server, servletContextHandler);
        JettyWebSocketServerContainer.ensureContainer(servletContextHandler.getServletContext())
                .addMapping("/", jettyWebSocketCreator);
        return servletContextHandler;
    }

    private Handler createFallbackHandler()
    {
        return new Handler.Abstract()
        {
            @Override
            public boolean handle(final Request request,
                                  final Response response,
                                  final org.eclipse.jetty.util.Callback callback)
            {
                if (response.isCommitted())
                {
                    return false;
                }
                callback.succeeded();
                response.setStatus(HttpServletResponse.SC_FORBIDDEN);
                return true;
            }
        };
    }

    @Override
    public void close()
    {
        try
        {
            _server.stop();
        }
        catch (final Exception e)
        {
            LOGGER.warn("Error closing the web socket for port {}", _port.getPort(), e);
            _server = null;
        }
        finally
        {
            _connectionScheduler.shutdown();
        }
    }

    @Override
    public int getAcceptingPort()
    {
        final Server server = _server;
        return server == null || server.getConnectors().length == 0 ||
                !(server.getConnectors()[0] instanceof ServerConnector) ?
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
                _sslContextFactory.reloadFromPort();
                return true;
            }
            catch (final Exception e)
            {
                throw new IllegalConfigurationException("Unexpected exception on reload of ssl context factory", e);
            }
        }
        return false;
    }
}
