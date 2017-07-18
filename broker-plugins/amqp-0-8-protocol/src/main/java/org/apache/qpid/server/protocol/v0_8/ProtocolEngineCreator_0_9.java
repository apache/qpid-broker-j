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
package org.apache.qpid.server.protocol.v0_8;

import org.apache.qpid.server.transport.ProtocolEngine;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.plugin.ProtocolEngineCreator;
import org.apache.qpid.server.transport.ServerNetworkConnection;
import org.apache.qpid.server.transport.AggregateTicker;

@PluggableService
public class ProtocolEngineCreator_0_9 implements ProtocolEngineCreator
{
    private static final byte[] AMQP_0_9_HEADER =
            new byte[] { (byte) 'A',
                         (byte) 'M',
                         (byte) 'Q',
                         (byte) 'P',
                         (byte) 1,
                         (byte) 1,
                         (byte) 0,
                         (byte) 9
            };

    public ProtocolEngineCreator_0_9()
    {
    }

    @Override
    public Protocol getVersion()
    {
        return Protocol.AMQP_0_9;
    }


    @Override
    public byte[] getHeaderIdentifier()
    {
        return AMQP_0_9_HEADER;
    }

    @Override
    public ProtocolEngine newProtocolEngine(Broker<?> broker,
                                            ServerNetworkConnection network,
                                            AmqpPort<?> port,
                                            Transport transport,
                                            long id, final AggregateTicker aggregateTicker)
    {
        final AMQPConnection_0_8Impl protocolEngine =
                new AMQPConnection_0_8Impl(broker, network, port, transport, getVersion(), id, aggregateTicker);
        protocolEngine.create();
        return protocolEngine;
    }

    @Override
    public byte[] getSuggestedAlternativeHeader()
    {
        return null;
    }

    private static ProtocolEngineCreator INSTANCE = new ProtocolEngineCreator_0_9();

    public static ProtocolEngineCreator getInstance()
    {
        return INSTANCE;
    }

    @Override
    public String getType()
    {
        return getVersion().toString();
    }
}
