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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.Connection;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.protocol.v1_0.type.transport.Close;
import org.apache.qpid.tests.protocol.v1_0.Interaction;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;
import org.apache.qpid.tests.utils.BrokerSpecific;
import org.apache.qpid.tests.utils.ConfigItem;

@BrokerSpecific(kind = BrokerAdmin.KIND_BROKER_J)
@ConfigItem(name = Connection.CLOSE_RESPONSE_TIMEOUT, value = WebSocketCloseTimeoutTest.CLOSE_RESPONSE_TIMEOUT_MILLIS)
@ConfigItem(name = AmqpPort.FINAL_WRITE_TIMEOUT, value = WebSocketCloseTimeoutTest.FINAL_WRITE_TIMEOUT_MILLIS)
public class WebSocketCloseTimeoutTest extends BrokerAdminUsingTestBase
{
    static final String CLOSE_RESPONSE_TIMEOUT_MILLIS = "250";
    static final String FINAL_WRITE_TIMEOUT_MILLIS = "5000";
    private static final long AWAIT_TIMEOUT_SECONDS = 10L;

    @BeforeEach
    public void setUp()
    {
        assumeTrue(getBrokerAdmin().isWebSocketSupported(), "Broker support for AMQP over WebSocket is required");
        assumeTrue(getBrokerAdmin().isAnonymousSupported(), "Broker support for anonymous open is required");
        assumeTrue(getBrokerAdmin().isConnectionManagementSupported(), "Broker connection management is required");
    }

    @Test
    public void brokerRemovesConnectionWhenPeerWithholdsBothCloseResponses() throws Exception
    {
        final String containerId = getFullTestName();
        try (final WebSocketFrameTransport transport = new WebSocketFrameTransport(getBrokerAdmin())
                .withholdWebSocketCloseResponse()
                .connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.openContainerId(containerId).negotiateOpen();
            assertTrue(getBrokerAdmin().isConnectionRegistered(containerId),
                    "Connection was not registered after the AMQP open");

            final CompletableFuture<Void> closeFuture = getBrokerAdmin().closeConnectionAsync(containerId);
            interaction.consumeResponse(Close.class);
            // Deliberately omit the peer's AMQP CLOSE response.

            assertTrue(transport.awaitWebSocketCloseFrame(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS),
                       "Broker did not begin the WebSocket closing handshake");
            assertTrue(transport.isChannelOutputOpen(),
                       "Test peer terminated TCP instead of preserving the writable half");

            closeFuture.get(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            assertFalse(getBrokerAdmin().isConnectionRegistered(containerId),
                        "Connection remained registered after the close deadlines expired");
        }
    }
}
