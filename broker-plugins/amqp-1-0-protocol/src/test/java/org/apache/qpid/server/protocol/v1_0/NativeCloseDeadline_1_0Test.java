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
package org.apache.qpid.server.protocol.v1_0;

import java.lang.reflect.Field;
import java.util.stream.Stream;

import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.protocol.v1_0.type.transport.Close;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.transport.NativeCloseDeadlineTestBase;

class NativeCloseDeadline_1_0Test extends NativeCloseDeadlineTestBase
{
    @Override
    protected void receivePeerClose(final AMQPConnection<?> connection)
    {
        ((AMQPConnection_1_0Impl) connection).receiveClose(0, new Close());
    }

    @Override
    protected AMQPConnection<?> createConnection(final Protocol protocol) throws Exception
    {
        final AMQPConnection_1_0Impl connection = new AMQPConnection_1_0Impl(_broker, _network, _port, Transport.TCP,
                1L, _ticker);
        final Field state = AMQPConnection_1_0Impl.class.getDeclaredField("_connectionState");
        state.setAccessible(true);
        state.set(connection, ConnectionState.OPENED);
        installProtocol(connection);
        return connection;
    }

    @Override
    protected Stream<Protocol> protocols()
    {
        return Stream.of(Protocol.AMQP_1_0);
    }
}
