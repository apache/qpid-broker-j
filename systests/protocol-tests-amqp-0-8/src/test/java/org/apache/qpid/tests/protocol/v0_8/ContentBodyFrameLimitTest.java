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
package org.apache.qpid.tests.protocol.v0_8;

import static org.apache.qpid.tests.utils.BrokerAdmin.KIND_BROKER_J;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelCloseOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelOpenOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionCloseBody;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;
import org.apache.qpid.tests.utils.BrokerSpecific;
import org.apache.qpid.tests.utils.ConfigItem;

@BrokerSpecific(kind = KIND_BROKER_J)
@ConfigItem(name = "connection.maxContentBodyFramesPerMessage", value = "2")
public class ContentBodyFrameLimitTest extends BrokerAdminUsingTestBase
{
    private static final List<Protocol> PROTOCOLS = List.of(Protocol.AMQP_0_8, Protocol.AMQP_0_9, Protocol.AMQP_0_9_1);

    @BeforeEach
    public void setUp()
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
    }

    @Test
    public void contentBodyFrameLimitExceededClosesConnection() throws Exception
    {
        for (final Protocol protocol : PROTOCOLS)
        {
            try (final FrameTransport transport = new FrameTransport(getBrokerAdmin(), getBrokerAdmin()
                    .getPreferredPortType(), protocol).connect())
            {
                final Interaction interaction = openChannel(transport);
                interaction.basic().publishExchange("")
                        .publishRoutingKey(BrokerAdmin.TEST_QUEUE_NAME)
                        .publish()
                        .basic().contentHeader(1)
                        .basic().contentBody(new byte[0])
                        .basic().contentBody(new byte[0])
                        .basic().contentBody(new byte[0]);

                final ConnectionCloseBody close = interaction.consumeResponse()
                        .getLatestResponse(ConnectionCloseBody.class);
                assertThat(protocol.toString(), close.getReplyCode(), is(equalTo(ErrorCodes.RESOURCE_ERROR)));
            }
        }
    }

    @Test
    public void messageAtExactContentBodyFrameLimitIsAccepted() throws Exception
    {
        for (final Protocol protocol : PROTOCOLS)
        {
            try (final FrameTransport transport = new FrameTransport(getBrokerAdmin(), getBrokerAdmin()
                    .getPreferredPortType(), protocol).connect())
            {
                final Interaction interaction = openChannel(transport);
                interaction.basic().publishExchange("")
                        .publishRoutingKey(BrokerAdmin.TEST_QUEUE_NAME)
                        .publish()
                        .basic().contentHeader(1)
                        .basic().contentBody(new byte[0])
                        .basic().contentBody(new byte[]{42})
                        .channel().close()
                        .consumeResponse(ChannelCloseOkBody.class);
            }
        }

        assertThat(getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(PROTOCOLS.size())));
    }

    private Interaction openChannel(final FrameTransport transport) throws Exception
    {
        return transport.newInteraction()
                .negotiateOpen()
                .channel().open().consumeResponse(ChannelOpenOkBody.class);
    }
}
