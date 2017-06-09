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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;

import org.junit.Test;

import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.security.SaslChallenge;
import org.apache.qpid.server.protocol.v1_0.type.security.SaslCode;
import org.apache.qpid.server.protocol.v1_0.type.security.SaslInit;
import org.apache.qpid.server.protocol.v1_0.type.security.SaslMechanisms;
import org.apache.qpid.server.protocol.v1_0.type.security.SaslOutcome;
import org.apache.qpid.server.protocol.v1_0.type.security.SaslResponse;
import org.apache.qpid.server.protocol.v1_0.type.transport.Open;
import org.apache.qpid.tests.protocol.v1_0.BrokerAdmin;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.protocol.v1_0.HeaderResponse;
import org.apache.qpid.tests.protocol.v1_0.ProtocolTestBase;
import org.apache.qpid.tests.protocol.v1_0.SpecificationTest;

public class SaslTest extends ProtocolTestBase
{
    private static final Symbol CRAM_MD5 = Symbol.getSymbol("CRAM-MD5");
    private static final Symbol PLAIN = Symbol.getSymbol("PLAIN");

    private static final byte[] SASL_AMQP_HEADER_BYTES = "AMQP\3\1\0\0".getBytes(StandardCharsets.UTF_8);
    private static final byte[] AMQP_HEADER_BYTES = "AMQP\0\1\0\0".getBytes(StandardCharsets.UTF_8);

    @Test
    @SpecificationTest(section = "5.3.2",
            description = "SASL Negotiation [...] challenge/response step occurs zero times")
    public void saslSuccessfulAuthentication() throws Exception
    {
        final InetSocketAddress addr = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP);
        try (FrameTransport transport = new FrameTransport(addr, true))
        {
            transport.sendProtocolHeader(SASL_AMQP_HEADER_BYTES);
            HeaderResponse saslHeaderResponse = transport.getNextResponse(HeaderResponse.class);

            assertThat(saslHeaderResponse.getHeader(), is(equalTo(SASL_AMQP_HEADER_BYTES)));

            SaslMechanisms saslMechanismsResponse = transport.getNextPerformativeResponse(SaslMechanisms.class);
            assertThat(Arrays.asList(saslMechanismsResponse.getSaslServerMechanisms()), hasItem(PLAIN));


            SaslInit saslInit = new SaslInit();
            saslInit.setMechanism(PLAIN);
            saslInit.setInitialResponse(new Binary("\0guest\0guest".getBytes(StandardCharsets.US_ASCII)));
            transport.sendPerformative(saslInit);

            SaslOutcome saslOutcome = transport.getNextPerformativeResponse(SaslOutcome.class);
            assertThat(saslOutcome.getCode(), equalTo(SaslCode.OK));

            transport.sendProtocolHeader(AMQP_HEADER_BYTES);
            HeaderResponse headerResponse = transport.getNextResponse(HeaderResponse.class);
            assertThat(headerResponse.getHeader(), is(equalTo(AMQP_HEADER_BYTES)));

            transport.assertNoMoreResponses();
        }
    }

    @Test
    @SpecificationTest(section = "5.3.2",
            description = "SASL Negotiation [...] challenge/response step occurs once")
    public void saslSuccessfulAuthenticationWithChallengeResponse() throws Exception
    {
        final InetSocketAddress addr = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP);
        try (FrameTransport transport = new FrameTransport(addr, true))
        {
            transport.sendProtocolHeader(SASL_AMQP_HEADER_BYTES);
            HeaderResponse saslHeaderResponse = transport.getNextResponse(HeaderResponse.class);

            assertThat(saslHeaderResponse.getHeader(), is(equalTo(SASL_AMQP_HEADER_BYTES)));

            SaslMechanisms saslMechanismsResponse = transport.getNextPerformativeResponse(SaslMechanisms.class);
            assertThat(Arrays.asList(saslMechanismsResponse.getSaslServerMechanisms()), hasItem(CRAM_MD5));

            SaslInit saslInit = new SaslInit();
            saslInit.setMechanism(CRAM_MD5);
            transport.sendPerformative(saslInit);

            SaslChallenge challenge = transport.getNextPerformativeResponse(SaslChallenge.class);
            assertThat(challenge.getChallenge(), is(notNullValue()));

            byte[] response = generateCramMD5ClientResponse("guest", "guest", challenge.getChallenge().getArray());

            SaslResponse saslResponse = new SaslResponse();
            saslResponse.setResponse(new Binary(response));
            transport.sendPerformative(saslResponse);

            SaslOutcome saslOutcome = transport.getNextPerformativeResponse(SaslOutcome.class);
            assertThat(saslOutcome.getCode(), equalTo(SaslCode.OK));

            transport.sendProtocolHeader(AMQP_HEADER_BYTES);
            HeaderResponse headerResponse = transport.getNextResponse(HeaderResponse.class);
            assertThat(headerResponse.getHeader(), is(equalTo(AMQP_HEADER_BYTES)));

            transport.assertNoMoreResponses();
        }
    }

    @Test
    @SpecificationTest(section = "5.3.2", description = "SASL Negotiation")
    public void saslUnsuccessfulAuthentication() throws Exception
    {
        final InetSocketAddress addr = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP);
        try (FrameTransport transport = new FrameTransport(addr, true))
        {
            transport.sendProtocolHeader(SASL_AMQP_HEADER_BYTES);
            HeaderResponse saslHeaderResponse = transport.getNextResponse(HeaderResponse.class);

            assertThat(saslHeaderResponse.getHeader(), is(equalTo(SASL_AMQP_HEADER_BYTES)));

            SaslMechanisms saslMechanismsResponse = transport.getNextPerformativeResponse(SaslMechanisms.class);
            assertThat(Arrays.asList(saslMechanismsResponse.getSaslServerMechanisms()), hasItem(PLAIN));

            SaslInit saslInit = new SaslInit();
            saslInit.setMechanism(PLAIN);
            saslInit.setInitialResponse(new Binary("\0guest\0badpassword".getBytes(StandardCharsets.US_ASCII)));
            transport.sendPerformative(saslInit);

            SaslOutcome saslOutcome = transport.getNextPerformativeResponse(SaslOutcome.class);
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
        final InetSocketAddress addr = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP);
        try (FrameTransport transport = new FrameTransport(addr, true))
        {
            transport.sendProtocolHeader(SASL_AMQP_HEADER_BYTES);
            HeaderResponse saslHeaderResponse = transport.getNextResponse(HeaderResponse.class);

            assertThat(saslHeaderResponse.getHeader(), is(equalTo(SASL_AMQP_HEADER_BYTES)));

            transport.getNextPerformativeResponse(SaslMechanisms.class);

            SaslInit saslInit = new SaslInit();
            saslInit.setMechanism(Symbol.getSymbol("NOT-A-MECHANISM"));
            transport.sendPerformative(saslInit);

            SaslOutcome saslOutcome = transport.getNextPerformativeResponse(SaslOutcome.class);
            assertThat(saslOutcome.getCode(), equalTo(SaslCode.AUTH));
            assertThat(saslOutcome.getAdditionalData(), is(nullValue()));

            transport.assertNoMoreResponsesAndChannelClosed();
        }
    }

    @Test
    @SpecificationTest(section = "5.3.2", description = "SASL Negotiation")
    public void authenticationBypassDisallowed() throws Exception
    {
        final InetSocketAddress addr = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP);
        try (FrameTransport transport = new FrameTransport(addr, true))
        {
            transport.sendProtocolHeader(SASL_AMQP_HEADER_BYTES);
            HeaderResponse saslHeaderResponse = transport.getNextResponse(HeaderResponse.class);
            assertThat(saslHeaderResponse.getHeader(), is(equalTo(SASL_AMQP_HEADER_BYTES)));

            transport.getNextPerformativeResponse(SaslMechanisms.class);

            Open open = new Open();
            open.setContainerId("testContainerId");
            transport.sendPerformative(open);

            transport.assertNoMoreResponsesAndChannelClosed();
        }
    }

    @Test
    @SpecificationTest(section = "5.3.2",
            description = "The peer acting as the SASL server MUST announce supported authentication mechanisms using"
                          + "the sasl-mechanisms frame.")
    public void clientSendsSaslMechanisms() throws Exception
    {
        final InetSocketAddress addr = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP);
        try (FrameTransport transport = new FrameTransport(addr, true))
        {
            transport.sendProtocolHeader(SASL_AMQP_HEADER_BYTES);
            HeaderResponse saslHeaderResponse = transport.getNextResponse(HeaderResponse.class);
            assertThat(saslHeaderResponse.getHeader(), is(equalTo(SASL_AMQP_HEADER_BYTES)));

            transport.getNextPerformativeResponse(SaslMechanisms.class);

            SaslMechanisms clientMechs = new SaslMechanisms();
            clientMechs.setSaslServerMechanisms(new Symbol[] {Symbol.valueOf("CLIENT-MECH")});
            transport.sendPerformative(clientMechs);

            transport.assertNoMoreResponsesAndChannelClosed();
        }
    }

    @Test
    @SpecificationTest(section = "5.3.2", description = "SASL Negotiation")
    public void clientSendsSaslChallenge() throws Exception
    {
        final InetSocketAddress addr = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP);
        try (FrameTransport transport = new FrameTransport(addr, true))
        {
            transport.sendProtocolHeader(SASL_AMQP_HEADER_BYTES);
            HeaderResponse saslHeaderResponse = transport.getNextResponse(HeaderResponse.class);
            assertThat(saslHeaderResponse.getHeader(), is(equalTo(SASL_AMQP_HEADER_BYTES)));

            transport.getNextPerformativeResponse(SaslMechanisms.class);

            SaslChallenge saslChallenge = new SaslChallenge();
            saslChallenge.setChallenge(new Binary(new byte[] {}));
            transport.sendPerformative(saslChallenge);

            transport.assertNoMoreResponsesAndChannelClosed();
        }
    }

    @Test
    @SpecificationTest(section = "5.3.2", description = "SASL Negotiation")
    public void clientSendsSaslOutcome() throws Exception
    {
        final InetSocketAddress addr = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP);
        try (FrameTransport transport = new FrameTransport(addr, true))
        {
            transport.sendProtocolHeader(SASL_AMQP_HEADER_BYTES);
            HeaderResponse saslHeaderResponse = transport.getNextResponse(HeaderResponse.class);
            assertThat(saslHeaderResponse.getHeader(), is(equalTo(SASL_AMQP_HEADER_BYTES)));

            transport.getNextPerformativeResponse(SaslMechanisms.class);

            SaslOutcome saslOutcome = new SaslOutcome();
            saslOutcome.setCode(SaslCode.OK);
            transport.sendPerformative(saslOutcome);

            transport.assertNoMoreResponsesAndChannelClosed();
        }
    }

    private static byte[] generateCramMD5ClientResponse(String userName, String userPassword, byte[] challengeBytes)
            throws Exception
    {
        String macAlgorithm = "HmacMD5";
        Mac mac = Mac.getInstance(macAlgorithm);
        mac.init(new SecretKeySpec(userPassword.getBytes(StandardCharsets.UTF_8), macAlgorithm));
        final byte[] messageAuthenticationCode = mac.doFinal(challengeBytes);
        String responseAsString = userName + " " + DatatypeConverter.printHexBinary(messageAuthenticationCode)
                                                                    .toLowerCase();
        return responseAsString.getBytes();
    }
}
