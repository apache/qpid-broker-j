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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

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
@ConfigItem(name = Connection.CLOSE_RESPONSE_TIMEOUT, value = "10000")
@ConfigItem(name = AmqpPort.FINAL_WRITE_TIMEOUT, value = "0")
public class WebSocketAmqpCloseReplyTest extends WebSocketCloseTestBase
{
    private static final long AWAIT_TIMEOUT_SECONDS = 3L;

    @ParameterizedTest
    @EnumSource(value = Transport.class, names = {"WS", "WSS"})
    public void clientReceivesAmqpCloseReplyAtZeroPortTimeout(final Transport transportType) throws Exception
    {
        try (final WebSocketFrameTransport transport = createTransport(transportType))
        {
            transport.withholdWebSocketCloseResponse().connect();
            final Interaction interaction = transport
                    .newInteraction().openContainerId(getFullTestName()).negotiateOpen();
            interaction.close().sync().consumeResponse(Close.class);
            assertNull(interaction.getLatestResponse(Close.class).getError());
            transport.getChannelCloseFuture().get(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            assertTrue(transport.awaitWebSocketCloseFrame(0L, TimeUnit.MILLISECONDS),
                    "Expected a WebSocket Close after the AMQP Close reply");
            assertEquals(1000, transport.getWebSocketCloseStatusCode());
        }
    }

    @ParameterizedTest
    @EnumSource(value = Transport.class, names = {"WS", "WSS"})
    public void brokerSendsWebSocketCloseAtZeroPortTimeout(final Transport transportType) throws Exception
    {
        final String containerId = getFullTestName();
        try (final WebSocketFrameTransport transport = createTransport(transportType))
        {
            transport.withholdWebSocketCloseResponse().connect();
            final Interaction interaction = transport.newInteraction().openContainerId(containerId).negotiateOpen();
            final CompletableFuture<Void> close = getBrokerAdmin().closeConnectionAsync(containerId);
            interaction.consumeResponse(Close.class);
            interaction.close().sync();

            transport.getChannelCloseFuture().get(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            close.get(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            assertTrue(transport.awaitWebSocketCloseFrame(0L, TimeUnit.MILLISECONDS),
                    "Expected a WebSocket Close after the AMQP Close exchange");
            assertEquals(1000, transport.getWebSocketCloseStatusCode());
        }
    }
}
