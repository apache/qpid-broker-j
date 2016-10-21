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
package org.apache.qpid.server.protocol.v0_8.federation;

import org.apache.qpid.server.federation.OutboundProtocolEngine;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.RemoteHostAddress;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.plugin.OutboundProtocolEngineCreator;
import org.apache.qpid.server.plugin.PluggableService;

@PluggableService
public class OutboundProtocolEngineCreator_0_9 implements OutboundProtocolEngineCreator
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

    public OutboundProtocolEngineCreator_0_9()
    {
    }

    public Protocol getVersion()
    {
        return Protocol.AMQP_0_9;
    }

    @Override
    public OutboundProtocolEngine newProtocolEngine(final RemoteHostAddress<?> address, final VirtualHost<?> virtualHost)
    {
        return new OutboundConnection_0_8(address, virtualHost, Protocol.AMQP_0_9, AMQP_0_9_HEADER);
    }

    @Override
    public String getType()
    {
        return getVersion().toString();
    }
}
