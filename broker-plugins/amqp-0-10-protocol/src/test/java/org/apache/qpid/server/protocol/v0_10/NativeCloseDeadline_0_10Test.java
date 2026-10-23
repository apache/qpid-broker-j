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
 */
package org.apache.qpid.server.protocol.v0_10;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.protocol.v0_10.transport.ConnectionClose;
import org.apache.qpid.server.protocol.v0_10.transport.ConnectionCloseCode;
import org.apache.qpid.server.protocol.v0_10.transport.ConnectionStartOk;
import org.apache.qpid.server.protocol.v0_10.transport.ProtocolHeader;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.transport.NativeCloseDeadlineTestBase;

class NativeCloseDeadline_0_10Test extends NativeCloseDeadlineTestBase
{
    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = "UNKNOWN")
    void testAuthenticationRejectionHasCloseDeadline(final String mechanism) throws Exception
    {
        setCloseTimeout(0);
        final AMQPConnection_0_10Impl connection = new AMQPConnection_0_10Impl(_broker, _network, _port, Transport.TCP,
                1L, _ticker);
        installProtocol(connection);
        final ServerConnection server = getServerConnection(connection);
        final ServerConnectionDelegate delegate = server.getConnectionDelegate();
        delegate.init(server, new ProtocolHeader(1, 0, 10));

        final ConnectionStartOk startOk = new ConnectionStartOk();
        startOk.setMechanism(mechanism);
        delegate.connectionStartOk(server, startOk);

        assertTrue(_network.doWork(), "Authentication rejection must honor the configured close deadline");
    }

    @Override
    protected void receivePeerClose(final AMQPConnection<?> connection) throws Exception
    {
        final ServerConnection server = getServerConnection(connection);
        final ConnectionClose connectionClose = new ConnectionClose(ConnectionCloseCode.NORMAL, "Peer close reproduction");
        server.getConnectionDelegate().connectionClose(server, connectionClose);
    }

    @Override
    protected AMQPConnection<?> createConnection(final Protocol protocol) throws Exception
    {
        final AMQPConnection_0_10Impl connection = new AMQPConnection_0_10Impl(_broker, _network, _port, Transport.TCP,
                1L, _ticker);
        getServerConnection(connection).setState(ServerConnection.State.OPEN);
        installProtocol(connection);
        return connection;
    }

    @Override
    protected Stream<Protocol> protocols()
    {
        return Stream.of(Protocol.AMQP_0_10);
    }

    private ServerConnection getServerConnection(final AMQPConnection<?> connection) throws ReflectiveOperationException
    {
        final Field delegate = AMQPConnection_0_10Impl.class.getDeclaredField("_connection");
        delegate.setAccessible(true);
        return (ServerConnection) delegate.get(connection);
    }
}
