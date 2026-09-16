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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.protocol.v1_0.type.transport.Open;
import org.apache.qpid.tests.protocol.ChannelClosedResponse;
import org.apache.qpid.tests.protocol.SpecificationTest;
import org.apache.qpid.tests.protocol.v1_0.EmptyResponse;
import org.apache.qpid.tests.protocol.v1_0.Interaction;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;
import org.apache.qpid.tests.utils.BrokerSpecific;
import org.apache.qpid.tests.utils.ConfigItem;

@BrokerSpecific(kind = BrokerAdmin.KIND_BROKER_J)
@ConfigItem(name = AmqpPort.HEART_BEAT_DELAY, value = WebSocketIdleTimeoutTest.IDLE_SECONDS)
public class WebSocketIdleTimeoutTest extends BrokerAdminUsingTestBase
{
    static final String IDLE_SECONDS = "1";
    private static final int IDLE_TIMEOUT_MILLIS = Integer.parseInt(IDLE_SECONDS) * 1000;

    @BeforeEach
    public void setUp()
    {
        assumeTrue(getBrokerAdmin().isWebSocketSupported(), "Broker support for AMQP over WebSocket is required");
        assumeTrue(getBrokerAdmin().isAnonymousSupported(), "Broker support for anonymous open is required");
    }

    @Test
    @SpecificationTest(section = "2.4.5",
            description = "If the idle timeout threshold is exceeded, a peer SHOULD try to close the connection.")
    public void brokerClosesIdleWebSocketConnection() throws Exception
    {
        try (WebSocketFrameTransport transport = new WebSocketFrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            final Open responseOpen = interaction
                    .openContainerId("testContainerId")
                    .negotiateOpen()
                    .getLatestResponse(Open.class);
            assertThat(responseOpen.getIdleTimeOut().intValue(), is(equalTo(IDLE_TIMEOUT_MILLIS)));

            // TODO: The broker ought to send a close performative, but currently closes the transport directly.
            interaction.consumeResponse().getLatestResponse(ChannelClosedResponse.class);
        }
    }

    @Test
    @SpecificationTest(section = "2.4.5",
            description = "A peer with nothing to send MAY send an empty frame to prevent idle timeout.")
    public void compressedWebSocketReceivesIdleFrames() throws Exception
    {
        try (WebSocketFrameTransport transport = new WebSocketFrameTransport(getBrokerAdmin(), true).connect())
        {
            assertThat(transport.getNegotiatedExtensions(), containsString("permessage-deflate"));

            final Interaction interaction = transport.newInteraction();
            final Open responseOpen = interaction
                    .openContainerId("testContainerId")
                    .openIdleTimeOut(IDLE_TIMEOUT_MILLIS)
                    .negotiateOpen()
                    .getLatestResponse(Open.class);
            assertThat(responseOpen.getIdleTimeOut().intValue(), is(equalTo(IDLE_TIMEOUT_MILLIS)));

            interaction.consumeResponse(EmptyResponse.class)
                       .emptyFrame();
            interaction.consumeResponse(EmptyResponse.class)
                       .emptyFrame();

            interaction.doCloseConnection();
        }
    }
}
