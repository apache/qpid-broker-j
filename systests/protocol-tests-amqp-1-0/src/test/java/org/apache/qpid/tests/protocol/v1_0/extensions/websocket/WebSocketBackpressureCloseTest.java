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
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.eclipse.jetty.io.EndPoint;
import org.eclipse.jetty.io.SocketChannelEndPoint;
import org.eclipse.jetty.util.Callback;
import org.eclipse.jetty.websocket.common.WebSocketSession;
import org.eclipse.jetty.websocket.core.ExtensionStack;
import org.eclipse.jetty.websocket.core.OutgoingFrames;
import org.eclipse.jetty.websocket.core.WebSocketCoreSession;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.apache.qpid.server.model.Connection;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.port.AmqpPort;
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
    private static final Duration CLOSE_COMPLETION_TIMEOUT = CLOSE_TIMEOUT.plusSeconds(5L);
    private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(10L);

    @ParameterizedTest(name = "{0}")
    @EnumSource(value = Transport.class, names = {"WS", "WSS"})
    public void brokerClosesSocketWithinSharedDeadlineWhileWriteIsPending(final Transport transportType)
            throws Exception
    {
        assumeTrue(getBrokerAdmin() instanceof EmbeddedBrokerPerClassAdminImpl,
                "Observing the broker TCP endpoint requires an embedded broker");
        final EmbeddedBrokerPerClassAdminImpl admin = (EmbeddedBrokerPerClassAdminImpl) getBrokerAdmin();
        final String containerId = getFullTestName();

        try (final WebSocketFrameTransport transport = createTransport(transportType).connect())
        {
            final Interaction interaction = transport.newInteraction().openContainerId(containerId).negotiateOpen();
            final AbstractAMQPConnection<?, ?> connection =
                    (AbstractAMQPConnection<?, ?>) admin.findConnection(containerId);
            assertNotNull(connection, "Broker did not register the AMQP connection");
            final WebSocketCoreSession coreSession = getCoreSession(connection);
            final EndPoint endPoint = coreSession.getConnection().getEndPoint();
            final EndPoint tcpEndPoint = endPoint instanceof EndPoint.Wrapper wrapper ? wrapper.unwrap() : endPoint;
            final SocketChannel socket = ((SocketChannelEndPoint) tcpEndPoint).getChannel();

            final ExtensionStack extensions = coreSession.getExtensionStack();
            assertFalse(extensions.hasNegotiatedExtensions(), "The test requires a session without extensions");
            final OutgoingFrames outgoing = extensions.getNextOutgoing();
            final CompletableFuture<Callback> pendingWrite = new CompletableFuture<>();
            try
            {
                connection.doOnIOThreadAsync(() -> extensions.initialize(extensions.getNextIncoming(), entry ->
                {
                    if (!entry.getFrame().isDataFrame() || !pendingWrite.complete(entry.getCallback()))
                    {
                        outgoing.sendFrame(entry);
                    }
                }, coreSession)).get(AWAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

                interaction.begin().sync();
                assertTimeoutPreemptively(AWAIT_TIMEOUT, () -> { pendingWrite.get(); },
                        "Broker did not start the WebSocket write");

                final CompletableFuture<Void> closeFuture = assertTimeoutPreemptively(CLOSE_COMPLETION_TIMEOUT, () ->
                {
                    final long closeRequestedMillis = System.currentTimeMillis();
                    final CompletableFuture<Void> closed = admin.closeConnectionAsync(containerId);
                    while (socket.isOpen())
                    {
                        TimeUnit.MILLISECONDS.sleep(10L);
                    }
                    assertTrue(System.currentTimeMillis() - closeRequestedMillis >= CLOSE_TIMEOUT.toMillis(),
                            "Broker closed the TCP socket before the shared close deadline");
                    return closed;
                }, "Broker did not close the TCP socket within the shared deadline and scheduling allowance");

                closeFuture.get(AWAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                assertFalse(admin.isConnectionRegistered(containerId),
                        "Broker did not unregister the closed connection");
            }
            finally
            {
                pendingWrite.thenAccept(callback -> callback.failed(new ClosedChannelException()));
            }
        }
    }

    private static WebSocketCoreSession getCoreSession(final AbstractAMQPConnection<?, ?> connection)
            throws ReflectiveOperationException
    {
        final ServerNetworkConnection network = connection.getNetwork();
        final Field sessionField = network.getClass().getDeclaredField("_connection");
        sessionField.setAccessible(true);
        final WebSocketSession session = (WebSocketSession) sessionField.get(network);
        return (WebSocketCoreSession) session.getCoreSession();
    }
}
