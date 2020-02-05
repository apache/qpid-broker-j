/*
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

package org.apache.qpid.server.management.plugin.portunification;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;

import org.eclipse.jetty.io.AbstractConnection;
import org.eclipse.jetty.io.Connection;
import org.eclipse.jetty.io.EndPoint;
import org.eclipse.jetty.io.ssl.SslConnection;
import org.eclipse.jetty.io.ssl.SslHandshakeListener;
import org.eclipse.jetty.server.AbstractConnectionFactory;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.util.BufferUtil;
import org.eclipse.jetty.util.Callback;
import org.eclipse.jetty.util.annotation.Name;
import org.eclipse.jetty.util.component.ContainerLifeCycle;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Logger;
import org.eclipse.jetty.util.ssl.SslContextFactory;

public class TlsOrPlainConnectionFactory extends AbstractConnectionFactory
{
    private final SslContextFactory _sslContextFactory;
    private final String _nextProtocol;

    public TlsOrPlainConnectionFactory(@Name("sslContextFactory") final SslContextFactory factory,
                                       @Name("nextProtocol") final String nextProtocol)
    {
        super("SSL");
        _sslContextFactory = factory == null ? new SslContextFactory.Server() : factory;
        _nextProtocol = nextProtocol;
        this.addBean(this._sslContextFactory);
    }

    @Override
    protected void doStart() throws Exception
    {
        super.doStart();

        final SSLEngine engine = _sslContextFactory.newSSLEngine();
        engine.setUseClientMode(false);
        final SSLSession session = engine.getSession();
        if (session.getPacketBufferSize() > this.getInputBufferSize())
        {
            this.setInputBufferSize(session.getPacketBufferSize());
        }
        engine.closeInbound();
        engine.closeOutbound();
    }

    @Override
    public PlainOrTlsConnection newConnection(final Connector connector, final EndPoint realEndPoint)
    {
        final MarkableEndPoint endPoint = new MarkableEndPoint(realEndPoint);
        final PlainOrTlsConnection plainOrTlsConnection = new PlainOrTlsConnection(connector, endPoint);
        endPoint.setConnection(plainOrTlsConnection);

        return plainOrTlsConnection;

    }

    @Override
    public String toString()
    {
        return String.format("%s@%x{%s->%s}",
                             this.getClass().getSimpleName(),
                             this.hashCode(),
                             this.getProtocol(),
                             this._nextProtocol);
    }

    class PlainOrTlsConnection implements Connection
    {
        private final Logger LOG = Log.getLogger(PlainOrTlsConnection.class);

        private static final int TLS_HEADER_SIZE = 6;

        private final long _created = System.currentTimeMillis();
        private final Connector _connector;
        private final MarkableEndPoint _endPoint;
        private final Callback _fillableCallback = new Callback()
        {
            @Override
            public void succeeded()
            {
                onFillable();
            }
        };

        private final ByteBuffer _tlsDeterminationBuf = BufferUtil.allocate(TLS_HEADER_SIZE);
        private final List<Listener> _listeners = new CopyOnWriteArrayList<>();
        private volatile AbstractConnection _actualConnection;

        PlainOrTlsConnection(final Connector connector, final MarkableEndPoint endPoint)
        {
            _connector = connector;
            _endPoint = endPoint;
            _endPoint.mark();
        }

        @Override
        public void addListener(Listener listener)
        {
            if (_actualConnection == null)
            {
                _listeners.add(listener);
            }
            else
            {
                _actualConnection.addListener(listener);
            }
        }

        @Override
        public void removeListener(Listener listener)
        {
            _listeners.remove(listener);
        }

        @Override
        public void onOpen()
        {
            if (LOG.isDebugEnabled())
            {
                LOG.debug("onOpen {}", this);
            }
            _endPoint.fillInterested(_fillableCallback);

            for (Listener listener : _listeners)
            {
                listener.onOpened(this);
            }

        }

        @Override
        public void onClose()
        {
            if (LOG.isDebugEnabled())
            {
                LOG.debug("onClose {}", this);
            }

            for (Listener listener : _listeners)
            {
                listener.onClosed(this);
            }
        }

        @Override
        public EndPoint getEndPoint()
        {
            return _endPoint;
        }

        @Override
        public void close()
        {
            try
            {
                if (_endPoint != null)
                {
                    _endPoint.close();
                }
            }
            finally
            {
                if (_actualConnection != null)
                {
                    _actualConnection.close();
                }
            }
        }

        @Override
        public boolean onIdleExpired()
        {
            return _actualConnection == null || _actualConnection.onIdleExpired();
        }

        @Override
        public long getMessagesIn()
        {
            return _actualConnection == null ? -1L : _actualConnection.getMessagesIn();
        }

        @Override
        public long getMessagesOut()
        {
            return _actualConnection == null ? -1L : _actualConnection.getMessagesOut();
        }

        @Override
        public long getBytesIn()
        {
            return _actualConnection == null ? -1L : _actualConnection.getBytesIn();
        }

        @Override
        public long getBytesOut()
        {
            return _actualConnection == null ? -1L : _actualConnection.getBytesOut();
        }

        @Override
        public long getCreatedTimeStamp()
        {
            return _created;
        }

        @Override
        public final String toString()
        {
            return String.format("%s<-%s",toConnectionString(),getEndPoint());
        }

        String toConnectionString()
        {
            return String.format("%s@%h",
                                 getClass().getSimpleName(),
                                 this);
        }

        void onFillable()
        {
            if (_actualConnection != null)
            {
                _actualConnection.onFillable();
            }
            else
            {
                try
                {
                    int filled = getEndPoint().fill(_tlsDeterminationBuf);
                    if (filled < 0)
                    {
                        close();
                        return;
                    }

                    int remaining = _tlsDeterminationBuf.remaining();
                    if (remaining >= TLS_HEADER_SIZE)
                    {
                        final byte[] array = _tlsDeterminationBuf.array();

                        boolean isTLS = looksLikeSSLv2ClientHello(array) || looksLikeSSLv3ClientHello(array);
                        LOG.debug("new connection tls={} endpoint address={}", isTLS, getEndPoint().getRemoteAddress());

                        _endPoint.rewind();

                        if (isTLS)
                        {
                            final SSLEngine engine = _sslContextFactory.newSSLEngine(_endPoint.getRemoteAddress());
                            engine.setUseClientMode(false);

                            final SslConnection sslConnection = newSslConnection(_connector, _endPoint, engine);
                            sslConnection.setInputBufferSize(getInputBufferSize());
                            sslConnection.setRenegotiationAllowed(_sslContextFactory.isRenegotiationAllowed());
                            _actualConnection = sslConnection;

                            if (_connector instanceof ContainerLifeCycle)
                            {
                                ContainerLifeCycle container = (ContainerLifeCycle)_connector;
                                container.getBeans(SslHandshakeListener.class).forEach(sslConnection::addHandshakeListener);
                            }
                            getBeans(SslHandshakeListener.class).forEach(sslConnection::addHandshakeListener);


                            ConnectionFactory next = _connector.getConnectionFactory(_nextProtocol);

                            final EndPoint decryptedEndPoint = sslConnection.getDecryptedEndPoint();
                            Connection connection = next.newConnection(_connector, decryptedEndPoint);
                            decryptedEndPoint.setConnection(connection);
                        }
                        else
                        {
                            final ConnectionFactory next = _connector.getConnectionFactory(_nextProtocol);
                            _actualConnection = (AbstractConnection) next.newConnection(_connector, _endPoint);
                            _endPoint.setConnection(_actualConnection);
                        }

                        _actualConnection.onOpen();

                        for (Listener listener : _listeners)
                        {
                            _actualConnection.addListener(listener);
                        }
                    }
                    else
                    {
                        LOG.debug("Too few bytes to make determination received : {} required: {}",
                                  remaining, TLS_HEADER_SIZE);

                        _endPoint.fillInterested(_fillableCallback);
                    }
                }
                catch (IOException e)
                {
                    close();
                }
            }
        }

        private boolean looksLikeSSLv3ClientHello(byte[] headerBytes)
        {
            return headerBytes[0] == 22 && // SSL Handshake
                   (headerBytes[1] == 3 && // SSL 3.0 / TLS 1.x
                    (headerBytes[2] == 0 || // SSL 3.0
                     headerBytes[2] == 1 || // TLS 1.0
                     headerBytes[2] == 2 || // TLS 1.1
                     headerBytes[2] == 3)) && // TLS1.2
                   (headerBytes[5] == 1); // client_hello
        }

        private boolean looksLikeSSLv2ClientHello(byte[] headerBytes)
        {
            return headerBytes[0] == -128 &&
                   headerBytes[3] == 3 && // SSL 3.0 / TLS 1.x
                   (headerBytes[4] == 0 || // SSL 3.0
                    headerBytes[4] == 1 || // TLS 1.0
                    headerBytes[4] == 2 || // TLS 1.1
                    headerBytes[4] == 3);
        }

        private SslConnection newSslConnection(final Connector connector, final EndPoint endPoint, final SSLEngine engine)
        {
            return new SslConnection(connector.getByteBufferPool(), connector.getExecutor(), endPoint, engine);
        }


    }
}
