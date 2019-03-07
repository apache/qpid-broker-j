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
package org.apache.qpid.tests.protocol.v0_10;

import java.net.InetSocketAddress;

import org.apache.qpid.server.protocol.v0_10.ProtocolEngineCreator_0_10;
import org.apache.qpid.tests.protocol.AbstractFrameTransport;


public class FrameTransport extends AbstractFrameTransport<Interaction>
{
    private final byte[] _protocolHeader;

    public FrameTransport(final InetSocketAddress brokerAddress)
    {
        super(brokerAddress, new FrameDecoder(new ProtocolEngineCreator_0_10().getHeaderIdentifier()), new FrameEncoder());
        _protocolHeader = new ProtocolEngineCreator_0_10().getHeaderIdentifier();
    }

    @Override
    public byte[] getProtocolHeader()
    {
        return _protocolHeader;
    }

    @Override
    public Interaction newInteraction()
    {
        return new Interaction(this);
    }

    @Override
    public FrameTransport connect()
    {
        super.connect();
        return this;
    }

}
