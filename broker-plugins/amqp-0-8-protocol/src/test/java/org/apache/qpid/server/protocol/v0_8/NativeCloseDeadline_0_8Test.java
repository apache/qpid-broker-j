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
package org.apache.qpid.server.protocol.v0_8;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.stream.Stream;

import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.protocol.ProtocolVersion;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.transport.NativeCloseDeadlineTestBase;

class NativeCloseDeadline_0_8Test extends NativeCloseDeadlineTestBase
{
    @Override
    protected void receivePeerClose(final AMQPConnection<?> connection)
    {
        ((AMQPConnection_0_8Impl) connection).receiveConnectionClose(200, null, 0, 0);
    }

    @Override
    protected AMQPConnection<?> createConnection(final Protocol protocol) throws Exception
    {
        final AMQPConnection_0_8Impl connection = new AMQPConnection_0_8Impl(_broker, _network, _port, Transport.TCP,
                protocol, 1L, _ticker);
        final ProtocolVersion version = switch (protocol)
        {
            case AMQP_0_8 -> ProtocolVersion.v0_8;
            case AMQP_0_9 -> ProtocolVersion.v0_9;
            default -> ProtocolVersion.v0_91;
        };
        final Method setVersion = AMQPConnection_0_8Impl.class.getDeclaredMethod("setProtocolVersion",
                ProtocolVersion.class);
        setVersion.setAccessible(true);
        setVersion.invoke(connection, version);
        final Field state = AMQPConnection_0_8Impl.class.getDeclaredField("_state");
        state.setAccessible(true);
        state.set(connection, AMQPConnection_0_8Impl.ConnectionState.OPEN);
        installProtocol(connection);
        return connection;
    }

    @Override
    protected Stream<Protocol> protocols()
    {
        return Stream.of(Protocol.AMQP_0_8, Protocol.AMQP_0_9, Protocol.AMQP_0_9_1);
    }
}
