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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.lang.reflect.Field;
import java.net.StandardSocketOptions;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.eclipse.jetty.io.SocketChannelEndPoint;
import org.eclipse.jetty.io.WriteFlusher;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.apache.qpid.server.model.Connection;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.server.transport.AbstractAMQPConnection;
import org.apache.qpid.server.transport.ServerNetworkConnection;
import org.apache.qpid.tests.protocol.v1_0.Interaction;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerSpecific;
import org.apache.qpid.tests.utils.ConfigItem;
import org.apache.qpid.tests.utils.EmbeddedBrokerPerClassAdminImpl;

@BrokerSpecific(kind = BrokerAdmin.KIND_BROKER_J)
@ConfigItem(name = AmqpPort.HEART_BEAT_DELAY, value = "0")
@ConfigItem(name = Connection.CLOSE_RESPONSE_TIMEOUT, value = WebSocketBackpressureCloseTest.CLOSE_TIMEOUT_MILLIS)
@ConfigItem(name = AmqpPort.FINAL_WRITE_TIMEOUT, value = "10000")
public class WebSocketBackpressureCloseTest extends WebSocketCloseTestBase
{
    static final String CLOSE_TIMEOUT_MILLIS = "2000";

    private static final Duration CLOSE_TIMEOUT = Duration.ofMillis(Long.parseLong(CLOSE_TIMEOUT_MILLIS));
    private static final Duration CLOSE_COMPLETION_TIMEOUT = CLOSE_TIMEOUT.plusSeconds(2L);
    private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(5L);
    private static final int SOCKET_BUFFER_SIZE = 8 * 1024;
    private static final int MESSAGE_COUNT = 16;
    private static final int MESSAGE_SIZE = 128 * 1024;

    @ParameterizedTest
    @EnumSource(value = Transport.class, names = {"WS", "WSS"})
    public void brokerClosesSocketWithinSharedDeadlineWhileWriteIsPending(final Transport transportType)
            throws Exception
    {
        assumeTrue(getBrokerAdmin() instanceof EmbeddedBrokerPerClassAdminImpl,
                "Observing the broker TCP endpoint requires an embedded broker");
        final EmbeddedBrokerPerClassAdminImpl admin = (EmbeddedBrokerPerClassAdminImpl) getBrokerAdmin();
        final String queueName = "backpressure-" + transportType;
        final String containerId = getFullTestName();
        final String[] messages = new String[MESSAGE_COUNT];
        Arrays.fill(messages, "x".repeat(MESSAGE_SIZE));
        admin.createQueue(queueName);
        admin.putMessageOnQueue(queueName, messages);

        try (final WebSocketFrameTransport transport = createTransport(transportType).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.openContainerId(containerId).negotiateOpen()
                    .begin().consumeResponse(Begin.class)
                    .attachRole(Role.RECEIVER).attachSourceAddress(queueName).attach().consumeResponse(Attach.class);

            final SocketChannelEndPoint endPoint = getTcpEndPoint(admin, containerId);
            final SocketChannel socket = endPoint.getChannel();
            socket.setOption(StandardSocketOptions.SO_SNDBUF, SOCKET_BUFFER_SIZE);
            transport.pauseReads(SOCKET_BUFFER_SIZE);
            interaction.flowIncomingWindow(UnsignedInteger.valueOf(1024))
                    .flowNextIncomingIdFromPeerLatestSessionBeginAndDeliveryCount()
                    .flowOutgoingWindow(UnsignedInteger.ZERO).flowNextOutgoingId(UnsignedInteger.ZERO)
                    .flowLinkCredit(UnsignedInteger.valueOf(MESSAGE_COUNT)).flowHandleFromLinkHandle().flow().sync();

            final WriteFlusher flusher = endPoint.getWriteFlusher();
            assertTimeoutPreemptively(AWAIT_TIMEOUT, () -> await(flusher::isPending),
                    "Broker did not have a pending TCP write");

            final CompletableFuture<Void> closeFuture = assertTimeoutPreemptively(CLOSE_COMPLETION_TIMEOUT, () ->
            {
                assertTrue(flusher.isPending(), "Broker must have a pending TCP write when close is requested");
                final long closeRequestedNanos = System.nanoTime();
                final CompletableFuture<Void> closed = admin.closeConnectionAsync(containerId);
                await(() -> !socket.isOpen());
                assertTrue(System.nanoTime() - closeRequestedNanos >= CLOSE_TIMEOUT.toNanos(),
                        "Broker closed the TCP socket before the shared close deadline");
                return closed;
            }, "Broker did not close the TCP socket within the shared deadline and scheduling allowance");

            closeFuture.get(AWAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            assertFalse(admin.isConnectionRegistered(containerId), "Broker did not unregister the closed connection");
        }
    }

    private SocketChannelEndPoint getTcpEndPoint(final EmbeddedBrokerPerClassAdminImpl admin, final String containerId)
            throws ReflectiveOperationException
    {
        final AbstractAMQPConnection<?, ?> connection = (AbstractAMQPConnection<?, ?>) admin.findConnection(containerId);
        assertNotNull(connection, "Broker did not register the AMQP connection");
        final ServerNetworkConnection network = connection.getNetwork();
        final Field tcpEndPoint = network.getClass().getDeclaredField("_tcpEndPoint");
        tcpEndPoint.setAccessible(true);
        return (SocketChannelEndPoint) tcpEndPoint.get(network);
    }

    private static void await(final BooleanSupplier condition) throws InterruptedException
    {
        while (!condition.getAsBoolean())
        {
            TimeUnit.MILLISECONDS.sleep(10L);
        }
    }
}
