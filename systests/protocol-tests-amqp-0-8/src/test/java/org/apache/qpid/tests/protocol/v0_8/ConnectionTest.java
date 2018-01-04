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

import static org.hamcrest.CoreMatchers.both;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

import java.net.InetSocketAddress;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.protocol.v0_8.transport.ChannelOpenOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionCloseBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionCloseOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionOpenOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionSecureBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionStartBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionTuneBody;
import org.apache.qpid.server.protocol.v0_8.transport.HeartbeatBody;
import org.apache.qpid.tests.protocol.ChannelClosedResponse;
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
                       .connection().open()
                       .consumeResponse(ConnectionCloseBody.class, ChannelClosedResponse.class);
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
                       .connection().open()
                       .consumeResponse(ConnectionCloseBody.class, ChannelClosedResponse.class);
        }
    }

    @Test
    @SpecificationTest(section = "4.2.3",
            description = "A peer MUST NOT send frames larger than the agreed-upon size. A peer that receives an "
                          + "oversized frame MUST signal a connection exception with reply code 501 (frame error).")
    public void overlySizedContentBodyFrame() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            ConnectionTuneBody response = interaction.negotiateProtocol()
                                                     .consumeResponse(ConnectionStartBody.class)
                                                     .connection().startOkMechanism("ANONYMOUS")
                                                     .startOk()
                                                     .consumeResponse().getLatestResponse(ConnectionTuneBody.class);

            final long frameMax = response.getFrameMax();
            // Older Qpid JMS Client 0-x had a defect that meant they could send content body frames that were too
            // large.  Rather then limiting the user content of each frame to frameSize - 8, it sent frameSize bytes
            // of user content meaning the resultant frame was too big.  The server accommodates this behaviour
            // by reducing the frame-size advertised to the client.
            final int overlyLargeFrameBodySize = (int) (frameMax + 1);  // Should be frameMax - 8 + 1.
            final byte[] bodyBytes = new byte[overlyLargeFrameBodySize];

            interaction.connection()
                       .tuneOkChannelMax(response.getChannelMax())
                       .tuneOkFrameMax(frameMax)
                       .tuneOkHeartbeat(response.getHeartbeat())
                       .tuneOk()
                       .connection().open()
                       .consumeResponse(ConnectionOpenOkBody.class)
                       .channel().open()
                       .consumeResponse(ChannelOpenOkBody.class)
                       .basic().publish()
                       .basic().contentHeader(bodyBytes.length)
                       .basic().contentBody(bodyBytes)
                       .sync();

            final ChannelClosedResponse closeResponse = interaction.consumeResponse()
                                                                   .getLatestResponse(ChannelClosedResponse.class);
            //TODO: The ChannelClosedResponse is wrong.
            //assertThat(res.getReplyCode(), CoreMatchers.is(CoreMatchers.equalTo(ErrorCodes.COMMAND_INVALID)));
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


    @Test
    @SpecificationTest(section = "9",
            description = "open-connection = C:protocol-header S:START C:START-OK *challenge S:TUNE C:TUNE-OK C:OPEN S:OPEN-OK")
    public void authenticationBypassAfterSendingStartOk() throws Exception
    {
        InetSocketAddress brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP);
        try(FrameTransport transport = new FrameTransport(brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateProtocol()
                       .consumeResponse(ConnectionStartBody.class)
                       .connection().startOkMechanism("PLAIN").startOk().consumeResponse(ConnectionSecureBody.class)
                       .connection().tuneOk()
                       .connection().open()
                       .consumeResponse(ConnectionCloseBody.class, ChannelClosedResponse.class);
        }
    }

    @Test
    @SpecificationTest(section = "4.2.7",
            description = "Heartbeat frames tell the recipient that the sender is still alive. The rate and timing of"
                          + " heartbeat frames is negotiated during connection tuning.")
    public void heartbeating() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            ConnectionTuneBody response = interaction.negotiateProtocol()
                                                     .consumeResponse(ConnectionStartBody.class)
                                                     .connection().startOkMechanism("ANONYMOUS")
                                                     .startOk()
                                                     .consumeResponse().getLatestResponse(ConnectionTuneBody.class);

            final Long heartbeatPeriod = 1L;

            interaction.connection()
                       .tuneOkChannelMax(response.getChannelMax())
                       .tuneOkFrameMax(response.getFrameMax())
                       .tuneOkHeartbeat(heartbeatPeriod.intValue())
                       .tuneOk()
                       .connection().open()
                       .consumeResponse(ConnectionOpenOkBody.class);

            final long startTime = System.currentTimeMillis();
            interaction.consumeResponse().getLatestResponse(HeartbeatBody.class);
            final long actualHeartbeatDelay = System.currentTimeMillis() - startTime;
            assertThat("Heartbeat not received within expected time frame",
                       actualHeartbeatDelay / 1000,
                       is(both(greaterThanOrEqualTo(heartbeatPeriod)).and(lessThanOrEqualTo(heartbeatPeriod * 2))));
            interaction.sendPerformative(new HeartbeatBody());

            interaction.consumeResponse(HeartbeatBody.class)
                       .sendPerformative(new HeartbeatBody());
        }
    }

    @Test
    @SpecificationTest(section = "4.2.7", description = "Any sent octet is a valid substitute for a heartbeat")
    public void heartbeatingIncomingTrafficIsNonHeartbeat() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            ConnectionTuneBody response = interaction.negotiateProtocol()
                                                     .consumeResponse(ConnectionStartBody.class)
                                                     .connection().startOkMechanism("ANONYMOUS")
                                                     .startOk()
                                                     .consumeResponse().getLatestResponse(ConnectionTuneBody.class);

            final Long heartbeatPeriod = 1L;

            interaction.connection()
                       .tuneOkChannelMax(response.getChannelMax())
                       .tuneOkFrameMax(response.getFrameMax())
                       .tuneOkHeartbeat(heartbeatPeriod.intValue())
                       .tuneOk()
                       .connection().open()
                       .consumeResponse(ConnectionOpenOkBody.class)
                       .channel().open()
                       .consumeResponse(ChannelOpenOkBody.class)
                       .consumeResponse(HeartbeatBody.class)
                       .exchange().declarePassive(true).declareNoWait(true).declare()
                       .consumeResponse(HeartbeatBody.class)
                       .sendPerformative(new HeartbeatBody())
                       .exchange().declarePassive(true).declareNoWait(true).declare();

            interaction.connection()
                       .close()
                       .consumeResponse().getLatestResponse(ConnectionCloseOkBody.class);
        }
    }

    @Test
    @SpecificationTest(section = "4.2.7",
            description = " If a peer detects no incoming traffic (i.e. received octets) for two heartbeat intervals "
                          + "or longer, it should close the connection without following the Connection.Close/Close-Ok handshaking")
    public void heartbeatingNoIncomingTraffic() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            ConnectionTuneBody response = interaction.negotiateProtocol()
                                                     .consumeResponse(ConnectionStartBody.class)
                                                     .connection().startOkMechanism("ANONYMOUS")
                                                     .startOk()
                                                     .consumeResponse().getLatestResponse(ConnectionTuneBody.class);

            final Long heartbeatPeriod = 1L;

            interaction.connection()
                       .tuneOkChannelMax(response.getChannelMax())
                       .tuneOkFrameMax(response.getFrameMax())
                       .tuneOkHeartbeat(heartbeatPeriod.intValue())
                       .tuneOk()
                       .connection().open()
                       .consumeResponse(ConnectionOpenOkBody.class)
                       .consumeResponse(HeartbeatBody.class);

            // Do not reflect a heartbeat so incoming line will be silent thus
            // requiring the broker to close the connection.
            transport.assertNoMoreResponsesAndChannelClosed();
        }
    }
}
