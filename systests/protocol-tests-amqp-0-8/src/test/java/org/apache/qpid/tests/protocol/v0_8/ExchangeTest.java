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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assume.assumeThat;

import java.net.InetSocketAddress;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelCloseBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelOpenOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeBoundOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeDeclareOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeDeleteOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueBindOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueUnbindOkBody;
import org.apache.qpid.tests.protocol.SpecificationTest;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;

public class ExchangeTest extends BrokerAdminUsingTestBase
{
    private static final String TEST_EXCHANGE = "testExchange";
    private InetSocketAddress _brokerAddress;

    @Before
    public void setUp()
    {
        _brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);
    }

    @Test
    @SpecificationTest(section = "1.6.2.1", description = "verify exchange exists, create if needed")
    public void exchangeDeclare() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.openAnonymousConnection()
                       .channel().open().consumeResponse(ChannelOpenOkBody.class)
                       .exchange().declareName(TEST_EXCHANGE).declare()
                       .consumeResponse().getLatestResponse(ExchangeDeclareOkBody.class);

            ExchangeBoundOkBody response = interaction.exchange()
                                                      .boundExchangeName(TEST_EXCHANGE)
                                                      .bound()
                                                      .consumeResponse()
                                                      .getLatestResponse(ExchangeBoundOkBody.class);
            assertThat(response.getReplyCode(), is(equalTo(ExchangeBoundOkBody.NO_BINDINGS)));

        }
    }

    @Test
    @SpecificationTest(section = "1.6.2.1",
            description = "If [durable is] set when creating a new exchange, the exchange will be marked as durable. "
                          + "Durable exchanges remain active when a server restarts. Non-durable exchanges (transient "
                          + "exchanges) are purged if/when a server restarts.")
    public void exchangeDeclareDurable() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.openAnonymousConnection()
                       .channel().open().consumeResponse(ChannelOpenOkBody.class)
                       .exchange().declareDurable(true).declareName(TEST_EXCHANGE).declare()
                       .consumeResponse().getLatestResponse(ExchangeDeclareOkBody.class);
        }

        assumeThat(getBrokerAdmin().supportsRestart(), Matchers.is(true));
        getBrokerAdmin().restart();

        try(FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            ExchangeBoundOkBody response = interaction.openAnonymousConnection()
                                                      .channel().open().consumeResponse(ChannelOpenOkBody.class)
                                                      .exchange()
                                                      .boundExchangeName(TEST_EXCHANGE)
                                                      .bound()
                                                      .consumeResponse()
                                                      .getLatestResponse(ExchangeBoundOkBody.class);
            assertThat(response.getReplyCode(), is(equalTo(ExchangeBoundOkBody.NO_BINDINGS)));
        }
    }

    @Test
    @SpecificationTest(section = "1.6.2.3",
            description = "This method deletes an exchange. When an exchange is deleted all queue bindings on the "
                          + "exchange are cancelled.")
    public void exchangeDelete() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.openAnonymousConnection()
                       .channel().open().consumeResponse(ChannelOpenOkBody.class)
                       .exchange().declareName(TEST_EXCHANGE).declare()
                       .consumeResponse().getLatestResponse(ExchangeDeclareOkBody.class);

            ExchangeBoundOkBody boundResponse = interaction.exchange()
                                                           .boundExchangeName(TEST_EXCHANGE)
                                                           .bound()
                                                           .consumeResponse()
                                                           .getLatestResponse(ExchangeBoundOkBody.class);
            assertThat(boundResponse.getReplyCode(), is(equalTo(ExchangeBoundOkBody.NO_BINDINGS)));

            interaction.exchange()
                       .deleteExchangeName(TEST_EXCHANGE)
                       .delete()
                       .consumeResponse()
                       .getLatestResponse(ExchangeDeleteOkBody.class);

            ExchangeBoundOkBody boundResponse2 = interaction.exchange()
                                                           .boundExchangeName(TEST_EXCHANGE)
                                                           .bound()
                                                           .consumeResponse()
                                                           .getLatestResponse(ExchangeBoundOkBody.class);

            assertThat(boundResponse2.getReplyCode(), is(equalTo(ExchangeBoundOkBody.EXCHANGE_NOT_FOUND)));
        }
    }

    @Test
    @SpecificationTest(section = "1.6.2.3",
            description = "If [if-unused is] set, the server will only delete the exchange if it has no queue"
                          + "bindings. If the exchange has queue bindings the server does not delete it but raises a "
                          + "channel exception instead.")
    public void exchangeDeleteInUse() throws Exception
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);

        try(FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.openAnonymousConnection()
                       .channel().open().consumeResponse(ChannelOpenOkBody.class)
                       .exchange().declareName(TEST_EXCHANGE).declare()
                       .consumeResponse(ExchangeDeclareOkBody.class)
                       .queue().bindName(TEST_EXCHANGE).bindQueueName(BrokerAdmin.TEST_QUEUE_NAME).bind()
                       .consumeResponse(QueueBindOkBody.class);

            ChannelCloseBody response = interaction.exchange()
                                                   .deleteExchangeName(TEST_EXCHANGE)
                                                   .deleteIfUnused(true)
                                                   .delete()
                                                   .consumeResponse()
                                                   .getLatestResponse(ChannelCloseBody.class);
            assertThat(response.getReplyCode(), is(equalTo(ErrorCodes.IN_USE)));
            interaction.channel().closeOk();

            ExchangeBoundOkBody boundResponse = interaction.channel().open().consumeResponse(ChannelOpenOkBody.class)
                                                           .exchange()
                                                           .boundExchangeName(TEST_EXCHANGE)
                                                           .bound()
                                                           .consumeResponse()
                                                           .getLatestResponse(ExchangeBoundOkBody.class);

            assertThat(boundResponse.getReplyCode(), is(equalTo(ExchangeBoundOkBody.OK)));

            interaction.queue().unbindName(TEST_EXCHANGE).unbindQueueName(BrokerAdmin.TEST_QUEUE_NAME).unbind()
                       .consumeResponse(QueueUnbindOkBody.class)
                       .exchange()
                       .deleteIfUnused(true)
                       .deleteExchangeName(TEST_EXCHANGE)
                       .delete()
                       .consumeResponse()
                       .getLatestResponse(ExchangeDeleteOkBody.class);
        }
    }

}
