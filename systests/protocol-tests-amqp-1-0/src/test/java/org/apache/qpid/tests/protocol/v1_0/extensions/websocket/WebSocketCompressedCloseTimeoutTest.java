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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.Connection;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Close;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.server.protocol.v1_0.type.transport.Transfer;
import org.apache.qpid.tests.protocol.v1_0.Interaction;
import org.apache.qpid.tests.protocol.v1_0.Utils;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;
import org.apache.qpid.tests.utils.BrokerSpecific;
import org.apache.qpid.tests.utils.ConfigItem;

/**
 * Verifies that a compressed connection is removed when a peer stops answering after message delivery begins.
 * The peer negotiates permessage-deflate, receives a delivery, and then withholds both the AMQP CLOSE response and
 * the WebSocket CLOSE response. Half closure keeps the client's writable half open after the broker closes its output.
 * The client keeps reading so the broker's WebSocket CLOSE can be observed. Pending-write deadlines are exercised
 * separately by the scheduler tests, which deliberately leave a write-completion callback unfinished.
 */
@BrokerSpecific(kind = BrokerAdmin.KIND_BROKER_J)
@ConfigItem(name = Connection.CLOSE_RESPONSE_TIMEOUT,
            value = WebSocketCompressedCloseTimeoutTest.CLOSE_RESPONSE_TIMEOUT_MILLIS)
@ConfigItem(name = AmqpPort.FINAL_WRITE_TIMEOUT,
            value = WebSocketCompressedCloseTimeoutTest.FINAL_WRITE_TIMEOUT_MILLIS)
public class WebSocketCompressedCloseTimeoutTest extends BrokerAdminUsingTestBase
{
    static final String CLOSE_RESPONSE_TIMEOUT_MILLIS = "250";
    static final String FINAL_WRITE_TIMEOUT_MILLIS = "5000";

    static final long LONG_CLOSE_RESPONSE_TIMEOUT_MILLIS = Long.parseLong(CLOSE_RESPONSE_TIMEOUT_MILLIS);
    static final long LONG_FINAL_WRITE_TIMEOUT_MILLIS = Long.parseLong(FINAL_WRITE_TIMEOUT_MILLIS);
    static final long TIMEOUT_MILLIS = LONG_CLOSE_RESPONSE_TIMEOUT_MILLIS + LONG_FINAL_WRITE_TIMEOUT_MILLIS;

    private static final long AWAIT_TIMEOUT_SECONDS = 30L;
    private static final Duration CLOSE_COMPLETION_TIMEOUT = Duration.ofMillis(TIMEOUT_MILLIS).plusSeconds(5L);
    private static final int QUEUED_MESSAGE_COUNT = 20;
    private static final int MESSAGE_SIZE = 8 * 1024;

    @BeforeEach
    void beforeEach()
    {
        assumeTrue(getBrokerAdmin().isWebSocketSupported(), "Broker support for AMQP over WebSocket is required");
        assumeTrue(getBrokerAdmin().isAnonymousSupported(), "Broker support for anonymous open is required");
        assumeTrue(getBrokerAdmin().isConnectionManagementSupported(), "Broker connection management is required");
    }

    @Test
    void brokerRemovesCompressedConnectionWhenPeerStopsAnsweringMidStream() throws Exception
    {
        final String containerId = getFullTestName();
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
        Utils.putMessageOnQueue(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME, payloads());

        try (final WebSocketFrameTransport transport = new WebSocketFrameTransport(getBrokerAdmin(), true)
                .withholdWebSocketCloseResponse()
                .connect())
        {
            assertThat(transport.getNegotiatedExtensions(), containsString("permessage-deflate"));

            final Interaction interaction = transport.newInteraction();
            interaction.openContainerId(containerId).negotiateOpen()
                       .begin().consumeResponse(Begin.class)
                       .attachRole(Role.RECEIVER)
                       .attachSourceAddress(BrokerAdmin.TEST_QUEUE_NAME)
                       .attach().consumeResponse(Attach.class)
                       .flowIncomingWindow(UnsignedInteger.valueOf(QUEUED_MESSAGE_COUNT))
                       .flowNextIncomingIdFromPeerLatestSessionBeginAndDeliveryCount()
                       .flowOutgoingWindow(UnsignedInteger.ZERO)
                       .flowNextOutgoingId(UnsignedInteger.ZERO)
                       .flowLinkCredit(UnsignedInteger.valueOf(QUEUED_MESSAGE_COUNT))
                       .flowHandleFromLinkHandle()
                       .flow();

            assertTrue(getBrokerAdmin().isConnectionRegistered(containerId),
                       "Connection was not registered after the AMQP open");

            assertTimeoutPreemptively(Duration.ofSeconds(AWAIT_TIMEOUT_SECONDS), () ->
            {
                interaction.consume(Transfer.class, Flow.class);
            }, "Broker did not deliver a message before close was requested");

            final CompletableFuture<Void> closeFuture = getBrokerAdmin().closeConnectionAsync(containerId);

            // All close stages share one timeout, including any intervening deliveries or flow updates
            assertTimeoutPreemptively(CLOSE_COMPLETION_TIMEOUT, () ->
            {
                interaction.consume(Close.class, Transfer.class, Flow.class);

                // Omit the peer's AMQP CLOSE response
                assertTrue(transport.awaitWebSocketCloseFrame(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS),
                           "Broker did not begin the WebSocket closing handshake");
                assertTrue(transport.isChannelOutputOpen(), "Test peer terminated TCP instead of preserving the " +
                        "writable half, so the broker was not left waiting on an unanswered handshake");

                closeFuture.get(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }, "Broker did not complete close within the configured deadlines and scheduling allowance");

            assertFalse(getBrokerAdmin().isConnectionRegistered(containerId), "Connection remained registered after " +
                    "the close deadlines expired");
        }
    }

    private static String[] payloads()
    {
        final String[] payloads = new String[QUEUED_MESSAGE_COUNT];
        for (int i = 0; i < QUEUED_MESSAGE_COUNT; i++)
        {
            final Random random = new Random(i);
            final StringBuilder payload = new StringBuilder(MESSAGE_SIZE);
            payload.append("message-").append(i).append(':');
            while (payload.length() < MESSAGE_SIZE)
            {
                payload.append((char) ('a' + random.nextInt(8)));
            }
            payloads[i] = payload.toString();
        }
        return payloads;
    }
}
