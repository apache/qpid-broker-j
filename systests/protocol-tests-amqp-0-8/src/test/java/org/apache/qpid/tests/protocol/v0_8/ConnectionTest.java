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
package org.apache.qpid.tests.protocol.v0_8;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.net.InetSocketAddress;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.protocol.v0_8.transport.ConnectionCloseBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionOpenOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionStartBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionTuneBody;
import org.apache.qpid.tests.protocol.SpecificationTest;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;

public class ConnectionTest extends BrokerAdminUsingTestBase
{
    private InetSocketAddress _brokerAddress;

    @Before
    public void setUp()
    {
        _brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);
    }

    @Test
    @SpecificationTest(section = "1.4.2.1", description = "start connection negotiation")
    public void connectionStart() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            ConnectionStartBody response =
                    interaction.negotiateProtocol().consumeResponse().getLatestResponse(ConnectionStartBody.class);

            assertThat(response.getVersionMajor(), is(equalTo((short)transport.getProtocolVersion().getMajorVersion())));
            assertThat(response.getVersionMinor(), is(equalTo((short)transport.getProtocolVersion().getActualMinorVersion())));
        }
    }

    @Test
    @SpecificationTest(section = "1.4.2.2", description = "select security mechanism and locale")
    public void connectionStartOk() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateProtocol()
                       .consumeResponse(ConnectionStartBody.class)
                       .connection().startOkMechanism("ANONYMOUS")
                                    .startOk()
                       .consumeResponse();

            interaction.getLatestResponse(ConnectionTuneBody.class);
        }
    }

    @Test
    @SpecificationTest(section = "1.4.2.6", description = "negotiate connection tuning parameters")
    public void connectionTuneOkAndOpen() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            ConnectionTuneBody response = interaction.negotiateProtocol()
                                                     .consumeResponse(ConnectionStartBody.class)
                                                     .connection().startOkMechanism("ANONYMOUS")
                                                     .startOk()
                                                     .consumeResponse().getLatestResponse(ConnectionTuneBody.class);

            interaction.connection().tuneOkChannelMax(response.getChannelMax())
                       .tuneOkFrameMax(response.getFrameMax())
                       .tuneOkHeartbeat(response.getHeartbeat())
                       .tuneOk()
                       .connection().open()
                       .consumeResponse().getLatestResponse(ConnectionOpenOkBody.class);
        }
    }

    @Test
    @SpecificationTest(section = "1.4.2.6", description = "[...] the minimum negotiated value for frame-max is also"
                                                          + " frame-min-size [4096].")
    public void tooSmallFrameSize() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            ConnectionTuneBody response = interaction.negotiateProtocol()
                                                     .consumeResponse(ConnectionStartBody.class)
                                                     .connection().startOkMechanism("ANONYMOUS")
                                                     .startOk()
                                                     .consumeResponse().getLatestResponse(ConnectionTuneBody.class);

            interaction.connection().tuneOkChannelMax(response.getChannelMax())
                       .tuneOkFrameMax(1024)
                       .tuneOkHeartbeat(response.getHeartbeat())
                       .tuneOk()
                       .consumeResponse().getLatestResponse(ConnectionCloseBody.class);
        }
    }

    @Test
    @SpecificationTest(section = "1.4.2.6.2.", description = "If the client specifies a frame max that is higher than"
                                                             + " the value provided by the server, the server MUST"
                                                             + " close the connection without attempting a negotiated"
                                                             + " close. The server may report the error in some fashion"
                                                             + " to assist implementors.")
    public void tooLargeFrameSize() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            ConnectionTuneBody response = interaction.negotiateProtocol()
                                                     .consumeResponse(ConnectionStartBody.class)
                                                     .connection().startOkMechanism("ANONYMOUS")
                                                     .startOk()
                                                     .consumeResponse().getLatestResponse(ConnectionTuneBody.class);

            interaction.connection().tuneOkChannelMax(response.getChannelMax())
                       .tuneOkFrameMax(Long.MAX_VALUE)
                       .tuneOkHeartbeat(response.getHeartbeat())
                       .tuneOk()
                       .consumeResponse().getLatestResponse(ConnectionCloseBody.class);
        }
    }

    @Test
    @SpecificationTest(section = "1.4.", description = "open connection = C:protocolheader S:START C:START OK"
                                                       + " *challenge S:TUNE C:TUNE OK C:OPEN S:OPEN OK")
    public void authenticationBypassBySendingTuneOk() throws Exception
    {
        final InetSocketAddress brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP);
        try (FrameTransport transport = new FrameTransport(brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateProtocol().consumeResponse(ConnectionStartBody.class)
                       .connection().tuneOk()
                       .consumeResponse().getLatestResponse(ConnectionCloseBody.class);
        }
    }


    @Test
    @SpecificationTest(section = "1.4.", description = "open connection = C:protocolheader S:START C:START OK"
                                                       + " *challenge S:TUNE C:TUNE OK C:OPEN S:OPEN OK")
    public void authenticationBypassBySendingOpen() throws Exception
    {
        final InetSocketAddress brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP);
        try (FrameTransport transport = new FrameTransport(brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateProtocol().consumeResponse(ConnectionStartBody.class)
                       .connection().open()
                       .consumeResponse().getLatestResponse(ConnectionCloseBody.class);
        }
    }


}
