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
package org.apache.qpid.server.transport.websocket.connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.concurrent.TimeUnit;

import org.eclipse.jetty.websocket.api.Callback;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.server.protocol.v1_0.type.transport.SenderSettleMode;
import org.apache.qpid.server.transport.AbstractAMQPConnection;
import org.apache.qpid.tests.protocol.v1_0.Utils;
import org.apache.qpid.tests.protocol.v1_0.extensions.websocket.WebSocketFrameTransport;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;
import org.apache.qpid.tests.utils.ConfigItem;
import org.apache.qpid.tests.utils.EmbeddedBrokerPerClassAdminImpl;

@ConfigItem(name = AmqpPort.FINAL_WRITE_TIMEOUT, value = "30000")
public class WebSocketPeerCloseDeliveryTest extends BrokerAdminUsingTestBase
{
    @Test
    void messagePublishedAfterPeerCloseRemainsAvailableToAnotherReceiver() throws Exception
    {
        assumeTrue(getBrokerAdmin() instanceof EmbeddedBrokerPerClassAdminImpl);
        final EmbeddedBrokerPerClassAdminImpl admin = (EmbeddedBrokerPerClassAdminImpl) getBrokerAdmin();
        assumeTrue(admin.isWebSocketSupported() && admin.isAnonymousSupported());
        final String queue = BrokerAdmin.TEST_QUEUE_NAME;
        final String containerId = getFullTestName();
        admin.createQueue(queue);
        try (final WebSocketFrameTransport transport = new WebSocketFrameTransport(admin).connect())
        {
            transport.newInteraction().openContainerId(containerId).negotiateOpen()
                    .begin().consumeResponse(Begin.class)
                    .attachRole(Role.RECEIVER).attachSourceAddress(queue).attachSndSettleMode(SenderSettleMode.SETTLED)
                    .attach().consumeResponse(Attach.class)
                    .flowIncomingWindow(UnsignedInteger.ONE)
                    .flowNextIncomingIdFromPeerLatestSessionBeginAndDeliveryCount()
                    .flowOutgoingWindow(UnsignedInteger.ZERO).flowNextOutgoingId(UnsignedInteger.ZERO)
                    .flowLinkCredit(UnsignedInteger.ONE).flowHandleFromLinkHandle().flowEcho(true)
                    .flow().consumeResponse(Flow.class);

            final AbstractAMQPConnection<?, ?> amqp = (AbstractAMQPConnection<?, ?>) admin.findConnection(containerId);
            final WebSocketConnection connection = (WebSocketConnection) amqp.getNetwork();

            connection.webSocketCloseReceived(Callback.NOOP);
            admin.putMessageOnQueue(queue, "after-close");
            amqp.doOnIOThreadAsync(() -> { }).get(5L, TimeUnit.SECONDS);

            assertFalse(connection.isClosed());
            assertEquals(1, admin.getQueueDepthMessages(queue));
            assertEquals("after-close", Utils.receiveMessage(admin, queue));
        }
    }
}
