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
package org.apache.qpid.tests.protocol.v1_0.extensions.websocket;

import static org.hamcrest.CoreMatchers.both;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedShort;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Open;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.tests.protocol.SpecificationTest;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.protocol.v1_0.Interaction;
import org.apache.qpid.tests.protocol.v1_0.Utils;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;

public class WebSocketTest extends BrokerAdminUsingTestBase
{
    private static final int LARGE_MESSAGE_SIZE = 300 * 1024;

    @BeforeEach
    public void setUp()
    {
        assumeTrue(getBrokerAdmin().isWebSocketSupported(), "Broker support for AMQP over websockets is required");
        assumeTrue(getBrokerAdmin().isAnonymousSupported(), "Broker support for Anonymous open is required");
    }

    @Test
    @SpecificationTest(section = "2.1", description = "Opening a WebSocket Connection")
    public void protocolHeader() throws Exception
    {
        try (FrameTransport transport = new WebSocketFrameTransport(getBrokerAdmin()).connect())
        {
            final byte[] response = transport.newInteraction().negotiateProtocol().consumeResponse().getLatestResponse(byte[].class);
            assertArrayEquals(transport.getProtocolHeader(), response, "Unexpected protocol header response");
        }
    }

    @Test
    @SpecificationTest(section = "2.3", description = "The AMQP protocol header is one WebSocket message.")
    public void pipelinedOpenDoesNotShareProtocolHeaderMessage() throws Exception
    {
        try (final WebSocketFrameTransport transport = new WebSocketFrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateProtocol()
                    .open()
                    .consumeResponse(byte[].class);

            assertThat(transport.getFirstBinaryWebSocketMessageSize(), is(8));

            interaction.consumeResponse(Open.class);
            interaction.doCloseConnection();
        }
    }

    @Test
    @SpecificationTest(section = "2.4",
            description = "[...] a single AMQP frame MAY be split over one or more consecutive WebSocket messages. ")
    public void amqpFramesSplitOverManyWebSocketFrames() throws Exception
    {
        try (FrameTransport transport = new WebSocketFrameTransport(getBrokerAdmin()).splitAmqpFrames().connect())
        {
            Interaction interaction = transport.newInteraction();
            final Open responseOpen = interaction.negotiateOpen().getLatestResponse(Open.class);

            assertThat(responseOpen.getContainerId(), is(notNullValue()));
            assertThat(responseOpen.getMaxFrameSize().longValue(),
                       is(both(greaterThanOrEqualTo(0L)).and(lessThan(UnsignedInteger.MAX_VALUE.longValue()))));
            assertThat(responseOpen.getChannelMax().intValue(),
                       is(both(greaterThanOrEqualTo(0)).and(lessThan(UnsignedShort.MAX_VALUE.intValue()))));

            interaction.doCloseConnection();
        }
    }

    @Test
    @SpecificationTest(section = "2.1", description = "Opening a WebSocket Connection")
    public void successfulOpen() throws Exception
    {
        assumeTrue(getBrokerAdmin().isWebSocketSupported());

        try (FrameTransport transport = new WebSocketFrameTransport(getBrokerAdmin()).connect())
        {
            Interaction interaction = transport.newInteraction();
            final Open responseOpen = interaction.negotiateOpen().getLatestResponse(Open.class);

            assertThat(responseOpen.getContainerId(), is(notNullValue()));
            assertThat(responseOpen.getMaxFrameSize().longValue(),
                       is(both(greaterThanOrEqualTo(0L)).and(lessThan(UnsignedInteger.MAX_VALUE.longValue()))));
            assertThat(responseOpen.getChannelMax().intValue(),
                       is(both(greaterThanOrEqualTo(0)).and(lessThan(UnsignedShort.MAX_VALUE.intValue()))));

            interaction.doCloseConnection();
        }
    }

    @Test
    public void compressedWebSocketReceivesLargeMessage() throws Exception
    {
        final String payload = createLargeMessagePayload();
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
        Utils.putMessageOnQueue(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME, payload);

        try (final WebSocketFrameTransport transport = new WebSocketFrameTransport(getBrokerAdmin(), true).connect())
        {
            assertThat(transport.getNegotiatedExtensions(), containsString("permessage-deflate"));

            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                    .begin().consumeResponse(Begin.class)
                    .attachRole(Role.RECEIVER)
                    .attachSourceAddress(BrokerAdmin.TEST_QUEUE_NAME)
                    .attach().consumeResponse(Attach.class)
                    .flowIncomingWindow(UnsignedInteger.ONE)
                    .flowNextIncomingIdFromPeerLatestSessionBeginAndDeliveryCount()
                    .flowOutgoingWindow(UnsignedInteger.ZERO)
                    .flowNextOutgoingId(UnsignedInteger.ZERO)
                    .flowLinkCredit(UnsignedInteger.ONE)
                    .flowHandleFromLinkHandle()
                    .flow()
                    .receiveDelivery()
                    .decodeLatestDelivery();

            assertThat(interaction.getDecodedLatestDelivery(), is(payload));
            assertThat(transport.getLargestBinaryWebSocketMessageSize(), greaterThanOrEqualTo(64 * 1024 + 1));
            interaction.detachEndCloseUnconditionally();
        }
    }

    private String createLargeMessagePayload()
    {
        final char[] payload = new char[LARGE_MESSAGE_SIZE];
        int value = 1;
        for (int i = 0; i < payload.length; i++)
        {
            value = 1_664_525 * value + 1_013_904_223;
            payload[i] = (char) (' ' + (value >>> 26));
        }
        return new String(payload);
    }
}
