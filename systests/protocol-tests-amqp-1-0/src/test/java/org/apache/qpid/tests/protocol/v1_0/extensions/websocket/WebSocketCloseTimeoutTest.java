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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import org.apache.qpid.server.model.Connection;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.protocol.v1_0.type.transport.Close;
import org.apache.qpid.server.protocol.v1_0.type.transport.Open;
import org.apache.qpid.tests.protocol.v1_0.Interaction;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerSpecific;
import org.apache.qpid.tests.utils.ConfigItem;

@BrokerSpecific(kind = BrokerAdmin.KIND_BROKER_J)
@ConfigItem(name = AmqpPort.HEART_BEAT_DELAY, value = "1")
@ConfigItem(name = Connection.CLOSE_RESPONSE_TIMEOUT, value = WebSocketCloseTimeoutTest.CLOSE_RESPONSE_TIMEOUT_MILLIS)
@ConfigItem(name = AmqpPort.FINAL_WRITE_TIMEOUT, value = WebSocketCloseTimeoutTest.FINAL_WRITE_TIMEOUT_MILLIS)
public class WebSocketCloseTimeoutTest extends WebSocketCloseTestBase
{
    static final String CLOSE_RESPONSE_TIMEOUT_MILLIS = "6000";
    static final String FINAL_WRITE_TIMEOUT_MILLIS = "12000";

    private static final Duration PEER_REPLY_DELAY = Duration.ofMillis(2500L);
    private static final Duration CLOSE_RESPONSE_TIMEOUT =
            Duration.ofMillis(Long.parseLong(CLOSE_RESPONSE_TIMEOUT_MILLIS));
    private static final Duration CLOSE_COMPLETION_TIMEOUT = CLOSE_RESPONSE_TIMEOUT.plusSeconds(2L);

    @ParameterizedTest
    @CsvSource({"WS, false", "WS, true", "WSS, false", "WSS, true"})
    public void brokerRemovesConnectionWithinSharedCloseDeadline(final Transport transportType,
                                                                 final boolean respondToAmqpClose) throws Exception
    {
        final String containerId = getFullTestName();
        try (final WebSocketFrameTransport transport = createTransport(transportType))
        {
            transport.withholdWebSocketCloseResponse().connect();
            final Interaction interaction = transport.newInteraction();
            final Open open = interaction.openContainerId(containerId).negotiateOpen().getLatestResponse(Open.class);
            assertEquals(1000L, open.getIdleTimeOut().longValue());
            assertTrue(getBrokerAdmin().isConnectionRegistered(containerId),
                    "Connection was not registered after the AMQP open");

            final CompletableFuture<Long> tcpClosedAtNanos = new CompletableFuture<>();
            transport.getChannelCloseFuture().addListener(future -> tcpClosedAtNanos.complete(System.nanoTime()));
            assertTimeoutPreemptively(CLOSE_COMPLETION_TIMEOUT, () ->
            {
                final long closeRequestedNanos = System.nanoTime();
                final CompletableFuture<Long> closedAtNanos = getBrokerAdmin().closeConnectionAsync(containerId)
                        .thenApply(ignored -> System.nanoTime());
                interaction.consumeResponse(Close.class);
                // A one-second heartbeat gives the broker a two-second read-idle timeout.
                assertFalse(transport.awaitWebSocketCloseFrame(PEER_REPLY_DELAY.toMillis(), TimeUnit.MILLISECONDS),
                        "Read-idle timeout interrupted the AMQP Close exchange");
                if (respondToAmqpClose)
                {
                    interaction.close().sync();
                    assertTrue(transport.awaitWebSocketCloseFrame(CLOSE_COMPLETION_TIMEOUT.toMillis(),
                            TimeUnit.MILLISECONDS),
                            "Broker did not send WebSocket Close after the AMQP Close response");
                }

                assertTrue(closedAtNanos.get() - closeRequestedNanos >= CLOSE_RESPONSE_TIMEOUT.toNanos(),
                        "Broker disconnected before the shared close deadline");
                assertTrue(tcpClosedAtNanos.get() - closeRequestedNanos >= CLOSE_RESPONSE_TIMEOUT.toNanos(),
                        "TCP closed before the shared close deadline");
                assertFalse(getBrokerAdmin().isConnectionRegistered(containerId),
                        "Connection remained registered after the shared close deadline expired");
                if (!respondToAmqpClose)
                {
                    assertFalse(transport.awaitWebSocketCloseFrame(100L, TimeUnit.MILLISECONDS),
                            "Broker started WebSocket close before the AMQP Close exchange completed");
                }
            }, "Broker did not complete close within the shared deadline and scheduling allowance");
        }
    }
}
