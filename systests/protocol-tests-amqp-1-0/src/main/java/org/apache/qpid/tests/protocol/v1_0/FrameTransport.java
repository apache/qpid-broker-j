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

package org.apache.qpid.tests.protocol.v1_0;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.qpid.tests.protocol.AbstractFrameTransport;
import org.apache.qpid.tests.utils.BrokerAdmin;

public class FrameTransport extends AbstractFrameTransport<Interaction>
{
    private final BrokerAdmin.PortType _portType;
    private final BrokerAdmin _brokerAdmin;

    public FrameTransport(final BrokerAdmin brokerAdmin)
    {
        this(brokerAdmin, brokerAdmin.getPreferredPortType());
    }

    public FrameTransport(final BrokerAdmin brokerAdmin, final BrokerAdmin.PortType portType)
    {
        super(brokerAdmin.getBrokerAddress(portType),
              new FrameDecoder(portType == BrokerAdmin.PortType.AMQP),
              new FrameEncoder());
        _portType = portType;
        _brokerAdmin = brokerAdmin;
    }

    @Override
    public FrameTransport connect()
    {
        super.connect();
        return this;
    }

    @Override
    public byte[] getProtocolHeader()
    {
        return "AMQP\0\1\0\0".getBytes(UTF_8);
    }

    public Interaction newInteraction()
    {
        return new Interaction(this, _brokerAdmin, _portType);
    }

}
