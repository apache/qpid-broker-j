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

package org.apache.qpid.tests.protocol.v1_0.transport.connection;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.Connection;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.protocol.v1_0.type.transport.Close;
import org.apache.qpid.server.protocol.v1_0.type.transport.Open;
import org.apache.qpid.tests.protocol.ChannelClosedResponse;
import org.apache.qpid.tests.protocol.SpecificationTest;
import org.apache.qpid.tests.protocol.v1_0.EmptyResponse;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.protocol.v1_0.Interaction;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;
import org.apache.qpid.tests.utils.BrokerSpecific;
import org.apache.qpid.tests.utils.ConfigItem;

@BrokerSpecific(kind = BrokerAdmin.KIND_BROKER_J)
@ConfigItem(name = AmqpPort.HEART_BEAT_DELAY, value = IdleTimeoutTest.IDLE_SECONDS)
@ConfigItem(name = Connection.CLOSE_RESPONSE_TIMEOUT, value = IdleTimeoutTest.CLOSE_RESPONSE_TIMEOUT_MILLIS)
public class IdleTimeoutTest extends BrokerAdminUsingTestBase
{
    static final String IDLE_SECONDS = "1";
    static final String CLOSE_RESPONSE_TIMEOUT_MILLIS = "4000";
    private static final int IDLE_TIMEOUT_MS = Integer.parseInt(IDLE_SECONDS) * 1000;

    @Test
    @SpecificationTest(section = "2.4.5",
            description = "If the [idle timeout threshold] threshold is exceeded, then a peer SHOULD try to"
                          + "gracefully close the connection using a close frame with an error explaining why.")
    public void brokerClosesIdleConnection() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            Interaction interaction = transport.newInteraction();
            Open responseOpen = interaction
                    .openContainerId("testContainerId")
                    .negotiateOpen()
                    .getLatestResponse(Open.class);
            assertThat(responseOpen.getIdleTimeOut().intValue(), is(equalTo(IDLE_TIMEOUT_MS)));

            // TODO: defect - broker ought to be sending a close performative but it just closes the socket.
            interaction.consumeResponse().getLatestResponse(ChannelClosedResponse.class);
        }
    }

    @Test
    public void silentPeerKeepsTcpConnectionUntilCloseResponseDeadline() throws Exception
    {
        final BrokerAdmin admin = getBrokerAdmin();
        assumeTrue(admin.isConnectionManagementSupported());
        final String containerId = getFullTestName();
        final long timeoutMillis = Long.parseLong(CLOSE_RESPONSE_TIMEOUT_MILLIS);
        try (final FrameTransport transport = new FrameTransport(admin).connect())
        {
            final Interaction interaction = transport.newInteraction().openContainerId(containerId).negotiateOpen();
            final long requested = System.nanoTime();
            final CompletableFuture<Long> closedAt = admin.closeConnectionAsync(containerId)
                    .thenApply(ignored -> System.nanoTime());
            interaction.consumeResponse(Close.class);

            final long elapsed = closedAt.get(timeoutMillis + 2000L, TimeUnit.MILLISECONDS) - requested;
            assertTrue(elapsed >= TimeUnit.MILLISECONDS.toNanos(timeoutMillis),
                    "The broker disconnected an idle TCP peer before its AMQP Close response was due");
            interaction.consumeResponse(ChannelClosedResponse.class);
        }
    }

    @Test
    @SpecificationTest(section = "2.4.5",
            description = "If a peer needs to satisfy the need to send traffic to prevent idle timeout, and has "
                          + "nothing to send, it MAY send an empty frame.")
    public void idleLine() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            Interaction interaction = transport.newInteraction();
            Open responseOpen = interaction
                    .openContainerId("testContainerId")
                    .openIdleTimeOut(IDLE_TIMEOUT_MS)
                    .negotiateOpen()
                    .getLatestResponse(Open.class);
            assertThat(responseOpen.getIdleTimeOut().intValue(), is(equalTo(IDLE_TIMEOUT_MS)));

            // Reflect the broker's empty frames
            interaction.consumeResponse(EmptyResponse.class)
                       .emptyFrame();

            interaction.consumeResponse(EmptyResponse.class)
                       .emptyFrame();

            interaction.doCloseConnection();
        }
    }
}
