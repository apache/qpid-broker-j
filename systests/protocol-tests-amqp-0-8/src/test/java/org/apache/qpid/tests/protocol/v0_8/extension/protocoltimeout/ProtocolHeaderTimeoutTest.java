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
package org.apache.qpid.tests.protocol.v0_8.extension.protocoltimeout;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assume.assumeThat;

import java.net.InetSocketAddress;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.tests.protocol.v0_8.FrameTransport;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;
import org.apache.qpid.tests.utils.ConfigItem;

@ConfigItem(name = AmqpPort.PROTOCOL_HANDSHAKE_TIMEOUT, value = "500")
public class ProtocolHeaderTimeoutTest extends BrokerAdminUsingTestBase
{
    private InetSocketAddress _brokerAddress;

    @Before
    public void setUp()
    {
        _brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);
    }

    @Test
    public void noProtocolHeader() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            transport.assertNoMoreResponsesAndChannelClosed();
        }
    }

    @Test
    public void incompleteProtocolHeader() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {

            final byte[] protocolHeader = transport.getProtocolHeader();
            byte[] buf = new byte[1];
            for(int i = 0 ; i < (protocolHeader.length - 1 ); i++)
            {
                System.arraycopy(protocolHeader, i, buf, 0, 1);
                transport.sendBytes(buf);
                transport.flush();
            }
            transport.assertNoMoreResponsesAndChannelClosed();
        }
    }
}
