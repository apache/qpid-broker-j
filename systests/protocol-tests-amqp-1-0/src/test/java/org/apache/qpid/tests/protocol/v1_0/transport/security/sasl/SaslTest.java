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

package org.apache.qpid.tests.protocol.v1_0.transport.security.sasl;

import static org.apache.qpid.tests.protocol.SaslUtils.generateCramMD5ClientResponse;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assume.assumeThat;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.security.SaslChallenge;
import org.apache.qpid.server.protocol.v1_0.type.security.SaslCode;
import org.apache.qpid.server.protocol.v1_0.type.security.SaslMechanisms;
import org.apache.qpid.server.protocol.v1_0.type.security.SaslOutcome;
import org.apache.qpid.server.protocol.v1_0.type.transport.Open;
import org.apache.qpid.tests.protocol.SpecificationTest;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.protocol.v1_0.Interaction;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;

public class SaslTest extends BrokerAdminUsingTestBase
{
    private static final Symbol CRAM_MD5 = Symbol.getSymbol("CRAM-MD5");
    private static final Symbol PLAIN = Symbol.getSymbol("PLAIN");

    private static final byte[] SASL_AMQP_HEADER_BYTES = "AMQP\3\1\0\0".getBytes(StandardCharsets.UTF_8);
    private static final byte[] AMQP_HEADER_BYTES = "AMQP\0\1\0\0".getBytes(StandardCharsets.UTF_8);
    private String _username;
    private String _password;

    @Before
    public void setUp()
    {
        assumeThat(getBrokerAdmin().isSASLSupported(), is(true));
        assumeThat(getBrokerAdmin().isSASLMechanismSupported(PLAIN.toString()), is(true));
        _username = getBrokerAdmin().getValidUsername();
        _password = getBrokerAdmin().getValidPassword();
    }

    @Test
    @SpecificationTest(section = "5.3.2",
            description = "SASL Negotiation [...] challenge/response step occurs zero times")
    public void saslSuccessfulAuthentication() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin(), BrokerAdmin.PortType.AMQP).connect())
        {
            final Interaction interaction = transport.newInteraction();
            final byte[] saslHeaderResponse = interaction.protocolHeader(SASL_AMQP_HEADER_BYTES)
                                                         .negotiateProtocol().consumeResponse()
                                                         .getLatestResponse(byte[].class);
            assertThat(saslHeaderResponse, is(equalTo(SASL_AMQP_HEADER_BYTES)));

            SaslMechanisms saslMechanismsResponse = interaction.consumeResponse().getLatestResponse(SaslMechanisms.class);
            assumeThat(Arrays.asList(saslMechanismsResponse.getSaslServerMechanisms()), hasItem(PLAIN));

            final Binary initialResponse = new Binary(String.format("\0%s\0%s", _username, _password).getBytes(StandardCharsets.US_ASCII));
            SaslOutcome saslOutcome = interaction.saslMechanism(PLAIN)
                                                 .saslInitialResponse(initialResponse)
                                                 .saslInit().consumeResponse()
                                                 .getLatestResponse(SaslOutcome.class);
            assertThat(saslOutcome.getCode(), equalTo(SaslCode.OK));

            final byte[] headerResponse = interaction.protocolHeader(AMQP_HEADER_BYTES)
                                                     .negotiateProtocol().consumeResponse()
                                                     .getLatestResponse(byte[].class);
            assertThat(headerResponse, is(equalTo(AMQP_HEADER_BYTES)));

            transport.assertNoMoreResponses();
        }
    }

    @Test
    @SpecificationTest(section = "2.4.2",
            description = "For applications that use many short-lived connections,"
                          + " it MAY be desirable to pipeline the connection negotiation process."
                          + " A peer MAY do this by starting to send subsequent frames before receiving"
                          + " the partner’s connection header or open frame")
    public void saslSuccessfulAuthenticationWithPipelinedFrames() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin(), BrokerAdmin.PortType.AMQP).connect())
        {
            final Binary initialResponse =
                    new Binary(String.format("\0%s\0%s", _username, _password).getBytes(StandardCharsets.US_ASCII));
            final Interaction interaction = transport.newInteraction();
            interaction.protocolHeader(SASL_AMQP_HEADER_BYTES)
                       .negotiateProtocol()
                       .saslMechanism(PLAIN)
                       .saslInitialResponse(initialResponse)
                       .saslInit()
                       .protocolHeader(AMQP_HEADER_BYTES)
                       .negotiateProtocol()
                       .openContainerId("testContainerId")
                       .open();

            final byte[] saslHeaderResponse = interaction.consumeResponse().getLatestResponse(byte[].class);
            assertThat(saslHeaderResponse, is(equalTo(SASL_AMQP_HEADER_BYTES)));

            SaslMechanisms saslMechanismsResponse = interaction.consumeResponse().getLatestResponse(SaslMechanisms.class);
            assumeThat(Arrays.asList(saslMechanismsResponse.getSaslServerMechanisms()), hasItem(PLAIN));

            SaslOutcome saslOutcome = interaction.consumeResponse().getLatestResponse(SaslOutcome.class);
            assertThat(saslOutcome.getCode(), equalTo(SaslCode.OK));

            final byte[] headerResponse = interaction.consumeResponse().getLatestResponse(byte[].class);
            assertThat(headerResponse, is(equalTo(AMQP_HEADER_BYTES)));

            interaction.consumeResponse().getLatestResponse(Open.class);
            interaction.doCloseConnection();
        }
    }

    @Test
    @SpecificationTest(section = "5.3.2",
            description = "SASL Negotiation [...] challenge/response step occurs once")
    public void saslSuccessfulAuthenticationWithChallengeResponse() throws Exception
    {
        assumeThat(getBrokerAdmin().isSASLMechanismSupported(CRAM_MD5.toString()), is(true));
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin(), BrokerAdmin.PortType.AMQP).connect())
        {
            final Interaction interaction = transport.newInteraction();
            final byte[] saslHeaderResponse = interaction.protocolHeader(SASL_AMQP_HEADER_BYTES)
                                                         .negotiateProtocol().consumeResponse()
                                                         .getLatestResponse(byte[].class);
            assertThat(saslHeaderResponse, is(equalTo(SASL_AMQP_HEADER_BYTES)));

            SaslMechanisms saslMechanismsResponse = interaction.consumeResponse().getLatestResponse(SaslMechanisms.class);
            assertThat(Arrays.asList(saslMechanismsResponse.getSaslServerMechanisms()), hasItem(CRAM_MD5));

            SaslChallenge saslChallenge = interaction.saslMechanism(CRAM_MD5)
                                                     .saslInit().consumeResponse()
                                                     .getLatestResponse(SaslChallenge.class);
            assertThat(saslChallenge.getChallenge(), is(notNullValue()));

            byte[] response = generateCramMD5ClientResponse(_username, _password,
                                                                      saslChallenge.getChallenge().getArray());

            final SaslOutcome saslOutcome = interaction.saslResponseResponse(new Binary(response))
                                                       .saslResponse()
                                                       .consumeResponse()
                                                       .getLatestResponse(SaslOutcome.class);
            assertThat(saslOutcome.getCode(), equalTo(SaslCode.OK));

            final byte[] headerResponse = interaction.protocolHeader(AMQP_HEADER_BYTES)
                                                     .negotiateProtocol()
                                                     .consumeResponse()
                                                     .getLatestResponse(byte[].class);
            assertThat(headerResponse, is(equalTo(AMQP_HEADER_BYTES)));

            transport.assertNoMoreResponses();
        }
    }

    @Test
    @SpecificationTest(section = "5.3.2", description = "SASL Negotiation")
    public void saslUnsuccessfulAuthentication() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin(), BrokerAdmin.PortType.AMQP).connect())
        {
            final Interaction interaction = transport.newInteraction();
            final byte[] saslHeaderResponse = interaction.protocolHeader(SASL_AMQP_HEADER_BYTES)
                                                         .negotiateProtocol().consumeResponse()
                                                         .getLatestResponse(byte[].class);
            assertThat(saslHeaderResponse, is(equalTo(SASL_AMQP_HEADER_BYTES)));

            SaslMechanisms saslMechanismsResponse = interaction.consumeResponse().getLatestResponse(SaslMechanisms.class);
            assumeThat(Arrays.asList(saslMechanismsResponse.getSaslServerMechanisms()), hasItem(PLAIN));

            final Binary initialResponse =
                    new Binary(String.format("\0%s\0badpassword", _username).getBytes(StandardCharsets.US_ASCII));
            SaslOutcome saslOutcome = interaction.saslMechanism(PLAIN)
                                                 .saslInitialResponse(initialResponse)
                                                 .saslInit().consumeResponse()
                                                 .getLatestResponse(SaslOutcome.class);
            assertThat(saslOutcome.getCode(), equalTo(SaslCode.AUTH));

            transport.assertNoMoreResponsesAndChannelClosed();
        }
    }

    @Test
    @SpecificationTest(section = "5.3.2",
            description = "The partner MUST then choose one of the supported mechanisms and initiate a sasl exchange."
                          + "If the selected mechanism is not supported by the receiving peer, it MUST close the connection "
                          + "with the authentication-failure close-code.")
    public void unsupportedSaslMechanism() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin(), BrokerAdmin.PortType.AMQP).connect())
        {
            final Interaction interaction = transport.newInteraction();
            final byte[] saslHeaderResponse = interaction.protocolHeader(SASL_AMQP_HEADER_BYTES)
                                                         .negotiateProtocol().consumeResponse()
                                                         .getLatestResponse(byte[].class);
            assertThat(saslHeaderResponse, is(equalTo(SASL_AMQP_HEADER_BYTES)));

            interaction.consumeResponse(SaslMechanisms.class);

            SaslOutcome saslOutcome = interaction.saslMechanism(Symbol.getSymbol("NOT-A-MECHANISM"))
                                                 .saslInit().consumeResponse()
                                                 .getLatestResponse(SaslOutcome.class);
            assertThat(saslOutcome.getCode(), equalTo(SaslCode.AUTH));
            assertThat(saslOutcome.getAdditionalData(), is(nullValue()));

            transport.assertNoMoreResponsesAndChannelClosed();
        }
    }

    @Test
    @SpecificationTest(section = "5.3.2", description = "SASL Negotiation")
    public void authenticationBypassDisallowed() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin(), BrokerAdmin.PortType.AMQP).connect())
        {
            final Interaction interaction = transport.newInteraction();
            final byte[] saslHeaderResponse = interaction.protocolHeader(SASL_AMQP_HEADER_BYTES)
                                                         .negotiateProtocol().consumeResponse()
                                                         .getLatestResponse(byte[].class);
            assertThat(saslHeaderResponse, is(equalTo(SASL_AMQP_HEADER_BYTES)));

            interaction.consumeResponse(SaslMechanisms.class);
            interaction.open().sync();

            transport.assertNoMoreResponsesAndChannelClosed();
        }
    }

    @Test
    @SpecificationTest(section = "5.3.2",
            description = "The peer acting as the SASL server MUST announce supported authentication mechanisms using"
                          + "the sasl-mechanisms frame.")
    public void clientSendsSaslMechanisms() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin(), BrokerAdmin.PortType.AMQP).connect())
        {
            SaslMechanisms clientMechs = new SaslMechanisms();
            clientMechs.setSaslServerMechanisms(new Symbol[] {Symbol.valueOf("CLIENT-MECH")});
            transport.newInteraction()
                     .protocolHeader(SASL_AMQP_HEADER_BYTES)
                     .negotiateProtocol().consumeResponse()
                     .consumeResponse(SaslMechanisms.class)
                     .sendPerformative(clientMechs)
                     .sync();

            transport.assertNoMoreResponsesAndChannelClosed();
        }
    }

    @Test
    @SpecificationTest(section = "5.3.2", description = "SASL Negotiation")
    public void clientSendsSaslChallenge() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin(), BrokerAdmin.PortType.AMQP).connect())
        {
            SaslChallenge saslChallenge = new SaslChallenge();
            saslChallenge.setChallenge(new Binary(new byte[] {}));
            transport.newInteraction()
                     .protocolHeader(SASL_AMQP_HEADER_BYTES)
                     .negotiateProtocol().consumeResponse()
                     .consumeResponse(SaslMechanisms.class)
                     .sendPerformative(saslChallenge)
                     .sync();

            transport.assertNoMoreResponsesAndChannelClosed();
        }
    }

    @Test
    @SpecificationTest(section = "5.3.2", description = "SASL Negotiation")
    public void clientSendsSaslOutcome() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin(), BrokerAdmin.PortType.AMQP).connect())
        {
            SaslOutcome saslOutcome = new SaslOutcome();
            saslOutcome.setCode(SaslCode.OK);
            transport.newInteraction()
                     .protocolHeader(SASL_AMQP_HEADER_BYTES)
                     .negotiateProtocol().consumeResponse()
                     .consumeResponse(SaslMechanisms.class)
                     .sendPerformative(saslOutcome)
                     .sync();

            transport.assertNoMoreResponsesAndChannelClosed();
        }
    }

    @Test
    @SpecificationTest(section = "5.3.1", description = "Receipt of an empty frame is an irrecoverable error.")
    public void emptyFramesDisallowed() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin(), BrokerAdmin.PortType.AMQP).connect())
        {
            transport.newInteraction()
                     .protocolHeader(SASL_AMQP_HEADER_BYTES)
                     .negotiateProtocol()
                     .saslEmptyFrame()
                     .consumeResponse(byte[].class)
                     .consumeResponse(SaslMechanisms.class)
                     .sync();

            transport.assertNoMoreResponsesAndChannelClosed();
        }
    }
}
