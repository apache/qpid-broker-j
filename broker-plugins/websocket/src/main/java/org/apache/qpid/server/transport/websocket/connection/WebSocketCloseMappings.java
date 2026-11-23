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

import org.eclipse.jetty.http.HttpException;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.io.EndPoint;
import org.eclipse.jetty.server.ConnectionMetaData;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.util.Callback;
import org.eclipse.jetty.websocket.core.Configuration;
import org.eclipse.jetty.websocket.core.FrameHandler;
import org.eclipse.jetty.websocket.core.Negotiated;
import org.eclipse.jetty.websocket.core.WebSocketComponents;
import org.eclipse.jetty.websocket.core.WebSocketConnection;
import org.eclipse.jetty.websocket.core.WebSocketCoreSession;
import org.eclipse.jetty.websocket.core.server.Handshaker;
import org.eclipse.jetty.websocket.core.server.WebSocketMappings;
import org.eclipse.jetty.websocket.core.server.WebSocketNegotiator;
import org.eclipse.jetty.websocket.core.server.internal.AbstractHandshaker;
import org.eclipse.jetty.websocket.core.server.internal.RFC6455Handshaker;
import org.eclipse.jetty.websocket.core.server.internal.RFC6455Negotiation;
import org.eclipse.jetty.websocket.core.server.internal.WebSocketNegotiation;
import org.eclipse.jetty.websocket.core.util.WebSocketUtils;

public final class WebSocketCloseMappings extends WebSocketMappings
{
    private final Handshaker _handshaker = new DeadlineHandshaker();

    public WebSocketCloseMappings(final WebSocketComponents components)
    {
        super(components);
    }

    @Override
    public Handshaker getHandshaker()
    {
        return _handshaker;
    }

    @Override
    public boolean upgrade(final WebSocketNegotiator negotiator,
                           final Request request,
                           final Response response,
                           final Callback callback,
                           final Configuration.Customizer defaultCustomizer)
    {
        return negotiator != null &&
                _handshaker.upgradeRequest(negotiator, request, response, callback,
                        getWebSocketComponents(), defaultCustomizer);
    }

    private static final class DeadlineHandshaker extends AbstractHandshaker
    {
        private final Handshaker _rfc6455 = new RFC6455Handshaker();

        @Override
        public boolean isWebSocketUpgradeRequest(final Request request)
        {
            return _rfc6455.isWebSocketUpgradeRequest(request);
        }

        @Override
        protected WebSocketNegotiation newNegotiation(final Request request,
                                                      final Response response,
                                                      final Callback callback,
                                                      final WebSocketComponents components)
        {
            return new RFC6455Negotiation(request, response, callback, components);
        }

        @Override
        protected boolean validateNegotiation(final WebSocketNegotiation negotiation)
        {
            if (!super.validateNegotiation(negotiation))
            {
                return false;
            }
            requireKey(negotiation);
            return true;
        }

        @Override
        protected WebSocketCoreSession newWebSocketCoreSession(final Request request,
                                                               final FrameHandler handler,
                                                               final Negotiated negotiated,
                                                               final WebSocketComponents components)
        {
            return new DeadlineWebSocketCoreSession(handler, negotiated, components, request.getContext()::run);
        }

        @Override
        protected WebSocketConnection createWebSocketConnection(final Request request,
                                                                final WebSocketCoreSession coreSession)
        {
            final ConnectionMetaData metadata = request.getConnectionMetaData();
            final Connector connector = metadata.getConnector();
            final EndPoint endPoint = metadata.getConnection().getEndPoint();
            return newWebSocketConnection(endPoint, connector.getExecutor(), connector.getScheduler(),
                    connector.getByteBufferPool(), coreSession);
        }

        @Override
        protected void prepareResponse(final Response response, final WebSocketNegotiation negotiation)
        {
            response.setStatus(HttpStatus.SWITCHING_PROTOCOLS_101);
            response.getHeaders().put(HttpHeader.UPGRADE, "websocket");
            response.getHeaders().put(HttpHeader.CONNECTION, HttpHeader.UPGRADE.asString());
            response.getHeaders().put(HttpHeader.SEC_WEBSOCKET_ACCEPT, WebSocketUtils.hashKey(requireKey(negotiation)));
        }

        private static String requireKey(final WebSocketNegotiation negotiation)
        {
            final String key = ((RFC6455Negotiation) negotiation).getKey();
            if (key == null)
            {
                throw new HttpException.RuntimeException(HttpStatus.BAD_REQUEST_400, "Missing Sec-WebSocket-Key");
            }
            return key;
        }
    }
}
