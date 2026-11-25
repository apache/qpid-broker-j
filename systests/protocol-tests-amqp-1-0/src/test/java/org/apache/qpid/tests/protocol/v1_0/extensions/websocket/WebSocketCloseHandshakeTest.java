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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.apache.qpid.server.model.Connection;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.protocol.v1_0.type.transport.Close;
import org.apache.qpid.tests.protocol.v1_0.Interaction;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerSpecific;
import org.apache.qpid.tests.utils.ConfigItem;

@BrokerSpecific(kind = BrokerAdmin.KIND_BROKER_J)
@ConfigItem(name = AmqpPort.HEART_BEAT_DELAY, value = "1")
@ConfigItem(name = Connection.CLOSE_RESPONSE_TIMEOUT, value = "10000")
@ConfigItem(name = AmqpPort.FINAL_WRITE_TIMEOUT, value = "10000")
public class WebSocketCloseHandshakeTest extends WebSocketCloseTestBase
{
    private static final long AWAIT_TIMEOUT_SECONDS = 3L;
    private static final Duration CLOSE_COMPLETION_TIMEOUT = Duration.ofSeconds(5L);

    @ParameterizedTest
    @EnumSource(value = Transport.class, names = {"WS", "WSS"})
    public void brokerInitiatedCloseExchangesWebSocketCloseFramesBeforeTcpTeardown(final Transport transportType)
            throws Exception
    {
        final String containerId = getFullTestName();
        try (final WebSocketFrameTransport transport = createTransport(transportType))
        {
            transport.withholdWebSocketCloseResponse().connect();
            final Interaction interaction = transport.newInteraction();
            interaction.openContainerId(containerId).negotiateOpen();

            final Duration replyDelay = Duration.ofMillis(2500L);
            assertTimeoutPreemptively(CLOSE_COMPLETION_TIMEOUT.plus(replyDelay), () ->
            {
                final CompletableFuture<Void> closeFuture = getBrokerAdmin().closeConnectionAsync(containerId);
                interaction.consumeResponse(Close.class);
                assertFalse(transport.awaitWebSocketCloseFrame(replyDelay.toMillis(), TimeUnit.MILLISECONDS),
                        "Broker closed the WebSocket while the peer could still answer AMQP Close");
                interaction.close().sync();

                assertTrue(transport.awaitWebSocketCloseFrame(CLOSE_COMPLETION_TIMEOUT.toSeconds(), TimeUnit.SECONDS),
                        "Broker did not send WebSocket Close within the shared close budget");
                assertEquals(1000, transport.getWebSocketCloseStatusCode(),
                        "Expected normal status after the AMQP Close exchange completed");
                assertThrows(TimeoutException.class, () -> closeFuture.get(100L, TimeUnit.MILLISECONDS),
                        "Broker completed transport closure before the peer sent WebSocket Close");
                assertTrue(transport.isChannelOutputOpen(), "TCP closed before the peer could send WebSocket Close");

                transport.sendWebSocketCloseResponse().sync();

                transport.getChannelCloseFuture().get(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                closeFuture.get(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                assertFalse(getBrokerAdmin().isConnectionRegistered(containerId),
                        "Connection remained registered after both closing handshakes completed");
            });
        }
    }
}
