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
package org.apache.qpid.server.connection;

import java.net.SocketAddress;
import java.util.UUID;

import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.security.auth.SocketConnectionPrincipal;
import org.apache.qpid.server.transport.AMQPConnection;

public class ConnectionPrincipal implements SocketConnectionPrincipal
{
    private static final long serialVersionUID = 1L;

    private final AMQPConnection<?> _connection;
    private AmqpConnectionMetaData _metadata;

    public ConnectionPrincipal(final AMQPConnection<?> connection)
    {
        _connection = connection;
        _metadata = new ConnectionMetaData(connection);
    }

    @Override
    public String getName()
    {
        return _connection.getRemoteAddressString();
    }

    @Override
    public SocketAddress getRemoteAddress()
    {
        return _connection.getRemoteSocketAddress();
    }

    @Override
    public AmqpConnectionMetaData getConnectionMetaData()
    {
        return _metadata;
    }

    public AMQPConnection<?> getConnection()
    {
        return _connection;
    }

    @Override
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        final ConnectionPrincipal that = (ConnectionPrincipal) o;

        if (!_connection.equals(that._connection))
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return _connection.hashCode();
    }

    private static class ConnectionMetaData implements AmqpConnectionMetaData
    {
        private final AMQPConnection<?> _connection;

        ConnectionMetaData(final AMQPConnection<?> connection)
        {
            _connection = connection;
        }

        @Override
        public UUID getConnectionId()
        {
            return _connection.getId();
        }

        @Override
        public Port getPort()
        {
            return _connection.getPort();
        }

        @Override
        public String getLocalAddress()
        {
            return _connection.getLocalAddress();
        }

        @Override
        public String getRemoteAddress()
        {
            return _connection.getRemoteAddress();
        }

        @Override
        public Protocol getProtocol()
        {
            return _connection.getProtocol();
        }

        @Override
        public Transport getTransport()
        {
            return _connection.getTransport();
        }

        @Override
        public String getClientId()
        {
            return _connection.getClientId();
        }

        @Override
        public String getClientVersion()
        {
            return _connection.getClientVersion();
        }

        @Override
        public String getClientProduct()
        {
            return _connection.getClientProduct();
        }
    }
}
