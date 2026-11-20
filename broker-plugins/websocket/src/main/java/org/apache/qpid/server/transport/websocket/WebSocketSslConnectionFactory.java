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

import javax.net.ssl.SSLEngine;

import org.eclipse.jetty.io.EndPoint;
import org.eclipse.jetty.io.ssl.SslConnection;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.websocket.core.WebSocketConnection;

final class WebSocketSslConnectionFactory extends SslConnectionFactory
{
    WebSocketSslConnectionFactory(final PortSslContextFactory sslContextFactory, final String nextProtocol)
    {
        super(sslContextFactory, nextProtocol);
    }

    @Override
    protected SslConnection newSslConnection(final Connector connector, final EndPoint endPoint, final SSLEngine engine)
    {
        return new SslConnection(connector.getByteBufferPool(), connector.getExecutor(), getSslContextFactory(),
                endPoint, engine, isDirectBuffersForEncryption(), isDirectBuffersForDecryption())
        {
            @Override
            protected SslEndPoint newSslEndPoint()
            {
                return new SslEndPoint()
                {
                    @Override
                    public void doShutdownOutput()
                    {
                        if (!(getConnection() instanceof WebSocketConnection))
                        {
                            super.doShutdownOutput();
                        }
                    }
                };
            }
        };
    }
}
