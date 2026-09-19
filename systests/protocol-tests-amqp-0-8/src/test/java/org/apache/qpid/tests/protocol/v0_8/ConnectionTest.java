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

import static org.apache.qpid.tests.protocol.SaslUtils.generateCramMD5ClientResponse;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.protocol.ProtocolVersion;
import org.apache.qpid.server.protocol.v0_8.AMQDecoder;
import org.apache.qpid.server.protocol.v0_8.transport.AMQDataBlock;
import org.apache.qpid.server.protocol.v0_8.transport.BasicQosBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelOpenOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionCloseBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionCloseOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionOpenOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionSecureBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionStartBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionTuneBody;
import org.apache.qpid.server.protocol.v0_8.transport.HeartbeatBody;
import org.apache.qpid.server.transport.ByteBufferSender;
import org.apache.qpid.tests.protocol.ChannelClosedResponse;
import org.apache.qpid.tests.protocol.Response;
import org.apache.qpid.tests.protocol.SpecificationTest;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;

public class ConnectionTest extends BrokerAdminUsingTestBase
{
    private static final String ANONYMOUS = "ANONYMOUS";
    private static final String PLAIN = "PLAIN";
    private static final String CRAM_MD5 = "CRAM-MD5";
    private static final byte FRAME_END = (byte) 0xCE;
    private static final int CONNECTION_CLOSE_METHOD_ID_0_8 = 60;
    private static final int CONNECTION_CLOSE_METHOD_ID_0_9 = 50;

    @Test
    @SpecificationTest(section = "1.4.2.1", description = "start connection negotiation")
    public void connectionStart() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
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
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.authenticateConnection();

            interaction.getLatestResponse(ConnectionTuneBody.class);
        }
    }

    @Test
    @SpecificationTest(section = "1.4.2.2",
            description = "If the mechanism field does not contain one of the security mechanisms proposed by the "
                          + "server in the Start method, the server MUST close the connection without sending any "
                          + "further data.")
    public void connectionStartOkUnsupportedMechanism() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateProtocol()
                       .consumeResponse(ConnectionStartBody.class)
                       .connection().startOkMechanism("NOT-A-MECHANISM")
                                    .startOk();

            interaction.sync();
            transport.assertNoMoreResponsesAndChannelClosed();
        }
    }

    @Test
    @SpecificationTest(section = "1.4.2.6", description = "negotiate connection tuning parameters")
    public void connectionTuneOkAndOpen() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            ConnectionTuneBody response = interaction.authenticateConnection().getLatestResponse(ConnectionTuneBody.class);

            interaction.connection().tuneOkChannelMax(response.getChannelMax())
                       .tuneOkFrameMax(response.getFrameMax())
                       .tuneOkHeartbeat(response.getHeartbeat())
                       .tuneOk()
                       .connection().open()
                       .consumeResponse().getLatestResponse(ConnectionOpenOkBody.class);
        }
    }

    @Test
    @SpecificationTest(section = "1.4.2.3", description = "security challenge data")
    public void connectionSecure() throws Exception
    {
        assumeTrue(getBrokerAdmin().isSASLMechanismSupported(PLAIN));
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin(), BrokerAdmin.PortType.AMQP).connect())
        {
            final byte[] initialResponse = String.format("\0%s\0%s",
                                                         getBrokerAdmin().getValidUsername(),
                                                         getBrokerAdmin().getValidPassword())
                                                 .getBytes(StandardCharsets.US_ASCII);

            final Interaction interaction = transport.newInteraction();
            final ConnectionStartBody start = interaction.negotiateProtocol()
                                                         .consumeResponse()
                                                         .getLatestResponse(ConnectionStartBody.class);

            assertThat(Arrays.asList(new String(start.getMechanisms()).split(" ")), hasItem(PLAIN));

            final ConnectionSecureBody secure = interaction.connection()
                                                           .startOkMechanism(PLAIN)
                                                           .startOk()
                                                           .consumeResponse().getLatestResponse(ConnectionSecureBody.class);
            assertThat(secure.getChallenge(), is(anyOf(nullValue(), equalTo(new byte[0]))));

            final ConnectionTuneBody tune = interaction.connection().secureOk(initialResponse)
                                                       .consumeResponse()
                                                       .getLatestResponse(ConnectionTuneBody.class);

            interaction.connection()
                       .tuneOkChannelMax(tune.getChannelMax())
                       .tuneOkFrameMax(tune.getFrameMax())
                       .tuneOk()
                       .connection().open()
                       .consumeResponse(ConnectionOpenOkBody.class);
        }
    }

    @Test
    @SpecificationTest(section = "1.4.2.3", description = "security challenge data")
    public void connectionSecureWithChallengeResponse() throws Exception
    {
        assumeTrue(getBrokerAdmin().isSASLMechanismSupported(CRAM_MD5));

        try (FrameTransport transport = new FrameTransport(getBrokerAdmin(), BrokerAdmin.PortType.AMQP).connect())
        {
            final Interaction interaction = transport.newInteraction();
            final ConnectionStartBody start = interaction.negotiateProtocol()
                                                         .consumeResponse()
                                                         .getLatestResponse(ConnectionStartBody.class);

            assertThat(Arrays.asList(new String(start.getMechanisms()).split(" ")), hasItem(CRAM_MD5));

            final ConnectionSecureBody secure = interaction.connection()
                                                           .startOkMechanism(CRAM_MD5)
                                                           .startOk()
                                                           .consumeResponse()
                                                           .getLatestResponse(ConnectionSecureBody.class);

            byte[] response = generateCramMD5ClientResponse(getBrokerAdmin().getValidUsername(),
                                                                      getBrokerAdmin().getValidPassword(),
                                                                      secure.getChallenge());

            final ConnectionTuneBody tune = interaction.connection()
                                                       .secureOk(response)
                                                       .consumeResponse()
                                                       .getLatestResponse(ConnectionTuneBody.class);

            interaction.connection()
                       .tuneOkChannelMax(tune.getChannelMax())
                       .tuneOkFrameMax(tune.getFrameMax())
                       .tuneOk()
                       .connection().open()
                       .consumeResponse(ConnectionOpenOkBody.class);
        }
    }

    @Test
    @SpecificationTest(section = "1.4.2.3", description = "security challenge data")
    public void connectionSecureUnsuccessfulAuthentication() throws Exception
    {
        assumeTrue(getBrokerAdmin().isSASLMechanismSupported(PLAIN));

        try (FrameTransport transport = new FrameTransport(getBrokerAdmin(), BrokerAdmin.PortType.AMQP).connect())
        {
            final byte[] initialResponse = String.format("\0%s\0%s",
                                                         getBrokerAdmin().getValidUsername(),
                                                         "badpassword")
                                                 .getBytes(StandardCharsets.US_ASCII);

            final Interaction interaction = transport.newInteraction();
            final ConnectionStartBody start = interaction.negotiateProtocol()
                                                         .consumeResponse()
                                                         .getLatestResponse(ConnectionStartBody.class);

            assertThat(Arrays.asList(new String(start.getMechanisms()).split(" ")), hasItem(PLAIN));

            final ConnectionCloseBody close = interaction.connection()
                                                         .startOkMechanism(PLAIN)
                                                         .startOk()
                                                         .consumeResponse(ConnectionSecureBody.class)
                                                         .connection()
                                                         .secureOk(initialResponse)
                                                         .consumeResponse()
                                                         .getLatestResponse(ConnectionCloseBody.class);

            assertThat(close.getReplyCode(), is(equalTo(ErrorCodes.NOT_ALLOWED)));
            assertThat(String.valueOf(close.getReplyText()).toLowerCase(), containsString("authentication failed"));

        }
    }

    @Test
    @SpecificationTest(section = "1.4.2.6", description = "[...] the minimum negotiated value for frame-max is also"
                                                          + " frame-min-size [4096].")
    public void tooSmallFrameSize() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            ConnectionTuneBody response = interaction.authenticateConnection().getLatestResponse(ConnectionTuneBody.class);

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
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            ConnectionTuneBody response = interaction.authenticateConnection().getLatestResponse(ConnectionTuneBody.class);

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
        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            ConnectionTuneBody response = interaction.authenticateConnection().getLatestResponse(ConnectionTuneBody.class);

            final long frameMax = response.getFrameMax();
            // Older Qpid JMS Client 0-x had a defect that meant they could send content body frames that were too
            // large.  Rather then limiting the user content of each frame to frameSize - 8, it sent frameSize bytes
            // of user content meaning the resultant frame was too big.  The server accommodates this behaviour
            // by reducing the frame-size advertised to the client.
            final int overlyLargeFrameBodySize = (int) (frameMax + 1);  // Should be frameMax - 8 + 1.
            final byte[] bodyBytes = new byte[overlyLargeFrameBodySize];

            final ConnectionCloseBody close = interaction.connection()
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
                    .consumeResponse()
                    .getLatestResponse(ConnectionCloseBody.class);

            assertThat(close.getReplyCode(), is(equalTo(ErrorCodes.FRAME_ERROR)));
        }
    }

    @Test
    @SpecificationTest(section = "4.2.3",
            description = "A Connection-class method on a non-zero channel must produce reply code 503.")
    public void connectionMethodOnNonZeroChannelSignalsConnectionException() throws Exception
    {
        try (final FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final ConnectionCloseBody close = transport.newInteraction()
                    .negotiateOpen()
                    .channelId(1)
                    .connection().close()
                    .consumeResponse()
                    .getLatestResponse(ConnectionCloseBody.class);

            assertThat(close.getReplyCode(), is(equalTo(ErrorCodes.COMMAND_INVALID)));
            assertThat(close.getClassId(), is(equalTo(ConnectionCloseBody.CLASS_ID)));
            final int expectedMethodId = ProtocolVersion.v0_8.equals(transport.getProtocolVersion())
                    ? CONNECTION_CLOSE_METHOD_ID_0_8
                    : CONNECTION_CLOSE_METHOD_ID_0_9;
            assertThat(close.getMethodId(), is(equalTo(expectedMethodId)));
        }
    }

    @Test
    @SpecificationTest(section = "4.2.3",
            description = "A malformed method frame must produce reply code 501 and identify the failing method.")
    public void truncatedMethodSignalsConnectionExceptionWithMethodIdentifiers() throws Exception
    {
        try (final FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final ConnectionCloseBody close = transport.newInteraction()
                    .negotiateOpen()
                    .sendPerformative(truncatedBasicQosFrame())
                    .consumeResponse()
                    .getLatestResponse(ConnectionCloseBody.class);

            assertThat(close.getReplyCode(), is(equalTo(ErrorCodes.FRAME_ERROR)));
            assertThat(close.getClassId(), is(equalTo(BasicQosBody.CLASS_ID)));
            assertThat(close.getMethodId(), is(equalTo(BasicQosBody.METHOD_ID)));
        }
    }

    @Test
    @SpecificationTest(section = "1.4.", description = "open connection = C:protocolheader S:START C:START OK"
                                                       + " *challenge S:TUNE C:TUNE OK C:OPEN S:OPEN OK")
    public void authenticationBypassBySendingTuneOk() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin(), BrokerAdmin.PortType.AMQP).connect())
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
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin(), BrokerAdmin.PortType.AMQP).connect())
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
        try(FrameTransport transport = new FrameTransport(getBrokerAdmin(), BrokerAdmin.PortType.AMQP).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateProtocol()
                       .consumeResponse(ConnectionStartBody.class)
                       .connection().startOkMechanism(PLAIN).startOk().consumeResponse(ConnectionSecureBody.class)
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
        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            ConnectionTuneBody response = interaction.authenticateConnection().getLatestResponse(ConnectionTuneBody.class);

            final Long heartbeatPeriod = 1L;

            interaction.connection()
                       .tuneOkChannelMax(response.getChannelMax())
                       .tuneOkFrameMax(response.getFrameMax())
                       .tuneOkHeartbeat(heartbeatPeriod.intValue())
                       .tuneOk();

            final long startTime = System.currentTimeMillis();
            interaction.connection().open()
                       .consumeResponse(ConnectionOpenOkBody.class)
                       .consumeResponse().getLatestResponse(HeartbeatBody.class);

            final long actualHeartbeatDelay = System.currentTimeMillis() - startTime;
            final long maximumExpectedHeartbeatDelay = heartbeatPeriod * 2 * 2; // Includes wiggle room to allow for slow boxes.
            assertThat("Heartbeat not received within expected time frame",
                       actualHeartbeatDelay / 1000,
                       is(both(greaterThanOrEqualTo(heartbeatPeriod)).and(lessThanOrEqualTo(maximumExpectedHeartbeatDelay))));
            interaction.sendPerformative(new HeartbeatBody());

            interaction.consumeResponse(HeartbeatBody.class)
                       .sendPerformative(new HeartbeatBody());
        }
    }

    @Test
    @SpecificationTest(section = "4.2.7", description = "Any sent octet is a valid substitute for a heartbeat")
    public void heartbeatingIncomingTrafficIsNonHeartbeat() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            ConnectionTuneBody response = interaction.authenticateConnection().getLatestResponse(ConnectionTuneBody.class);

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
        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            ConnectionTuneBody response = interaction.authenticateConnection().getLatestResponse(ConnectionTuneBody.class);

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

            Class[] classes = new Class[] {ChannelClosedResponse.class, HeartbeatBody.class};
            do
            {
                Response<?> latestResponse = interaction.consumeResponse(classes)
                                                        .getLatestResponse();
                if (latestResponse instanceof ChannelClosedResponse)
                {
                    break;
                }
                classes = new Class[] {ChannelClosedResponse.class};
            }
            while (true);
        }
    }

    @Test
    @SpecificationTest(section = "1.4.2.7", description = "The client tried to work with an unknown virtual host.")
    public void connectionOpenUnknownVirtualHost() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            ConnectionTuneBody tune = interaction.authenticateConnection().getLatestResponse(ConnectionTuneBody.class);

            ConnectionCloseBody close = interaction.connection()
                                                   .tuneOkChannelMax(tune.getChannelMax())
                                                   .tuneOkFrameMax(tune.getFrameMax())
                                                   .tuneOkHeartbeat(tune.getHeartbeat())
                                                   .tuneOk()
                                                   .connection()
                                                   .openVirtualHost("unknown-virtualhost")
                                                   .open()
                                                   .consumeResponse()
                                                   .getLatestResponse(ConnectionCloseBody.class);

            // Spec requires INVALID_PATH, but implementation uses NOT_FOUND
            assertThat(close.getReplyCode(), is(anyOf(equalTo(ErrorCodes.NOT_FOUND), equalTo(ErrorCodes.INVALID_PATH))));
            assertThat(String.valueOf(close.getReplyText()).toLowerCase(), containsString("unknown virtual host"));
        }
    }

    private static AMQDataBlock truncatedBasicQosFrame()
    {
        final ByteBuffer buffer = ByteBuffer.allocate(AMQDecoder.FRAME_HEADER_SIZE + Integer.BYTES + 1);
        buffer.put((byte) 1);
        buffer.putShort((short) 1);
        buffer.putInt(Integer.BYTES);
        buffer.putInt((BasicQosBody.CLASS_ID << 16) | BasicQosBody.METHOD_ID);
        buffer.put(FRAME_END);
        return rawFrame(buffer.array());
    }

    private static AMQDataBlock rawFrame(final byte[] bytes)
    {
        return new AMQDataBlock()
        {
            @Override
            public long getSize()
            {
                return bytes.length;
            }

            @Override
            public long writePayload(final ByteBufferSender sender)
            {
                sender.send(QpidByteBuffer.wrap(bytes));
                return bytes.length;
            }
        };
    }
}
