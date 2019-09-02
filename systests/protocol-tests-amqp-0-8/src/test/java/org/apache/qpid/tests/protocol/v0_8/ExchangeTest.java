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

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assume.assumeThat;

import java.util.Collections;

import org.hamcrest.Matchers;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.qpid.server.exchange.ExchangeDefaults;
import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelCloseBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelOpenOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionCloseBody;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeBoundOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeDeclareOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeDeleteOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueBindOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueDeleteOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueUnbindOkBody;
import org.apache.qpid.tests.protocol.SpecificationTest;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;

public class ExchangeTest extends BrokerAdminUsingTestBase
{
    private static final String TEST_EXCHANGE = "testExchange";

    @Test
    @SpecificationTest(section = "1.6.2.1", description = "verify exchange exists, create if needed")
    public void exchangeDeclare() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .channel().open().consumeResponse(ChannelOpenOkBody.class)
                       .exchange().declareName(TEST_EXCHANGE).declare()
                       .consumeResponse(ExchangeDeclareOkBody.class);

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
                       description = "If [passive is] set, the server will reply with Declare-Ok if the exchange "
                                     + "already exists with the same name, and raise an error if not.  The client can "
                                     + "use this to check whether an exchange exists without modifying the server state.")
    public void exchangeDeclarePassive() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .channel().open().consumeResponse(ChannelOpenOkBody.class)
                       .exchange().declareName(TEST_EXCHANGE).declare()
                       .consumeResponse(ExchangeDeclareOkBody.class)
                       .exchange().declarePassive(true).declareName(TEST_EXCHANGE).declare()
                       .consumeResponse(ExchangeDeclareOkBody.class)
                       .exchange().deleteExchangeName(TEST_EXCHANGE).delete()
                       .consumeResponse(ExchangeDeleteOkBody.class);

            ChannelCloseBody response = interaction.exchange().declarePassive(true).declareName(TEST_EXCHANGE).declare()
                                                        .consumeResponse().getLatestResponse(ChannelCloseBody.class);
            assertThat(response.getReplyCode(), is(equalTo(ErrorCodes.NOT_FOUND)));
        }
    }

    @Test
    @SpecificationTest(section = "1.6.2.1",
            description = "Exchange names starting with \"amq.\" are reserved for pre-declared and standardised "
                          + "exchanges. The client MAY declare an exchange starting with  \"amq.\" if the passive "
                          + "option is set, or the exchange already exists.")
    public void exchangeDeclareAmqDisallowed() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .channel().open().consumeResponse(ChannelOpenOkBody.class)
                       .exchange().declarePassive(true).declareName(ExchangeDefaults.DIRECT_EXCHANGE_NAME).declare()
                       .consumeResponse(ExchangeDeclareOkBody.class)
                       .exchange().declarePassive(false).declareName(ExchangeDefaults.DIRECT_EXCHANGE_NAME).declare()
                       .consumeResponse(ExchangeDeclareOkBody.class);

            ConnectionCloseBody response = interaction.exchange()
                                                      .declarePassive(false)
                                                      .declareName("amq.illegal")
                                                      .declare()
                               .consumeResponse().getLatestResponse(ConnectionCloseBody.class);

            /* TODO: 0-91 specification requires 'access-refused' (403) but server uses 'not-allowed' (530) */
            assertThat(response.getReplyCode(), anyOf(equalTo(ErrorCodes.NOT_ALLOWED), equalTo(ErrorCodes.ACCESS_REFUSED)));
        }
    }

    @Test
    @SpecificationTest(section = "1.6.2.1",
            description = "The client MUST not attempt to redeclare an existing exchange with a different type than "
                          + "used in the original Exchange.Declare method")
    public void exchangeRedeclareDifferentType() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .channel().open().consumeResponse(ChannelOpenOkBody.class)
                       .exchange().declareType(ExchangeDefaults.DIRECT_EXCHANGE_CLASS).declareName(TEST_EXCHANGE).declare()
                       .consumeResponse(ExchangeDeclareOkBody.class);

            ConnectionCloseBody response = interaction.exchange()
                                                      .declareType(ExchangeDefaults.TOPIC_EXCHANGE_CLASS)
                                                      .declareName(TEST_EXCHANGE).declare()
                                                      .consumeResponse().getLatestResponse(ConnectionCloseBody.class);

            assertThat(response.getReplyCode(), is(equalTo(ErrorCodes.NOT_ALLOWED)));
        }
    }

    @Test
    @Ignore("The server does not implement this rule.")
    @SpecificationTest(section = "1.6.2.1",
            description = "When [passive] set, all other method fields [of declare] except name and no-wait are ignored.")
    public void exchangeRedeclarePassiveDifferentType() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .channel().open().consumeResponse(ChannelOpenOkBody.class)
                       .exchange().declareType(ExchangeDefaults.DIRECT_EXCHANGE_CLASS)
                       .declareName(TEST_EXCHANGE).declare()
                       .consumeResponse(ExchangeDeclareOkBody.class);

            interaction.exchange()
                       .declarePassive(true)
                       .declareType(ExchangeDefaults.TOPIC_EXCHANGE_CLASS)
                       .declareName(TEST_EXCHANGE).declare()
                       .consumeResponse(ExchangeDeclareOkBody.class);
        }
    }

    @Test
    @SpecificationTest(section = "1.6.2.1",
            description = "The client MUST NOT attempt to declare an exchange with a type that the server does not "
                          + "support.")
    public void exchangeUnsupportedExchangeType() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .channel().open().consumeResponse(ChannelOpenOkBody.class)
                       .exchange()
                       .declareType(ExchangeDefaults.DIRECT_EXCHANGE_CLASS)
                       .declareName(TEST_EXCHANGE).declare()
                       .consumeResponse(ExchangeDeclareOkBody.class);

            ConnectionCloseBody response = interaction.exchange().declarePassive(true)
                                                      .declareType(ExchangeDefaults.TOPIC_EXCHANGE_CLASS)
                                                      .declareName(TEST_EXCHANGE).declare()
                                                      .consumeResponse().getLatestResponse(ConnectionCloseBody.class);

            assertThat(response.getReplyCode(), is(equalTo(ErrorCodes.NOT_ALLOWED)));
        }
    }
    @Test
    @SpecificationTest(section = "1.6.2.1",
            description = "If [durable is] set when creating a new exchange, the exchange will be marked as durable. "
                          + "Durable exchanges remain active when a server restarts. Non-durable exchanges (transient "
                          + "exchanges) are purged if/when a server restarts.")
    public void exchangeDeclareDurable() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .channel().open().consumeResponse(ChannelOpenOkBody.class)
                       .exchange().declareDurable(true).declareName(TEST_EXCHANGE).declare()
                       .consumeResponse(ExchangeDeclareOkBody.class);
        }

        assumeThat(getBrokerAdmin().supportsRestart(), Matchers.is(true));
        getBrokerAdmin().restart();

        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            ExchangeBoundOkBody response = interaction.negotiateOpen()
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
        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
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
                       .deleteExchangeName(TEST_EXCHANGE).delete()
                       .consumeResponse(ExchangeDeleteOkBody.class);

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
            description = "The client MUST NOT attempt to delete an exchange that does not exist.")
    public void exchangeDeleteExchangeNotFound() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            ChannelCloseBody unknownExchange = interaction.negotiateOpen()
                                                          .channel().open().consumeResponse(ChannelOpenOkBody.class)
                                                          .exchange().deleteExchangeName("unknownExchange").delete()
                                                          .consumeResponse().getLatestResponse(ChannelCloseBody.class);
            assertThat(unknownExchange.getReplyCode(), is(equalTo(ErrorCodes.NOT_FOUND)));
            interaction.channel().closeOk();
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

        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
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
                       .consumeResponse(ExchangeDeleteOkBody.class);
        }
    }

    @Test
    @SpecificationTest(section = "1.6.2.3",
            description = "The server MUST, in each virtual host, pre-declare an exchange instance for each standard "
                          + "exchange type that it implements.")
    public void exchangeDeleteAmqDisallowed() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            ChannelCloseBody response = interaction.negotiateOpen()
                                                   .channel().open().consumeResponse(ChannelOpenOkBody.class)
                                                   .exchange()
                                                   .deleteExchangeName(ExchangeDefaults.DIRECT_EXCHANGE_NAME).delete()
                                                   .consumeResponse().getLatestResponse(ChannelCloseBody.class);
            assertThat(response.getReplyCode(), is(equalTo(ErrorCodes.NOT_ALLOWED)));
        }
    }

    /** Qpid specific extension */
    @Test
    public void exchangeDeclareAutoDelete() throws Exception
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);

        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .channel().open().consumeResponse(ChannelOpenOkBody.class)
                       .exchange().declareName(TEST_EXCHANGE).declareAutoDelete(true).declare()
                       .consumeResponse(ExchangeDeclareOkBody.class)
                       .queue().bindName(TEST_EXCHANGE).bindQueueName(BrokerAdmin.TEST_QUEUE_NAME).bind()
                       .consumeResponse(QueueBindOkBody.class)
                       .queue().deleteName(BrokerAdmin.TEST_QUEUE_NAME).delete()
                       .consumeResponse(QueueDeleteOkBody.class);

            ExchangeBoundOkBody boundResponse = interaction.exchange()
                                                           .boundExchangeName(TEST_EXCHANGE)
                                                           .bound()
                                                           .consumeResponse()
                                                           .getLatestResponse(ExchangeBoundOkBody.class);

            assertThat(boundResponse.getReplyCode(), is(equalTo(ExchangeBoundOkBody.EXCHANGE_NOT_FOUND)));
        }
    }

    /** Qpid specific extension */
    @Test
    public void exchangeDeclareWithAlternateExchange() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final String altExchName = "altExchange";
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .channel().open().consumeResponse(ChannelOpenOkBody.class)
                       .exchange()
                       .declareName(altExchName)
                       .declare()
                       .consumeResponse(ExchangeDeclareOkBody.class)
                       .exchange()
                       .declareName(TEST_EXCHANGE)
                       .declareArguments(Collections.singletonMap("alternateExchange", altExchName)).declare()
                       .consumeResponse(ExchangeDeclareOkBody.class);

            ChannelCloseBody inUseResponse = interaction.exchange()
                                                        .deleteExchangeName(altExchName)
                                                        .delete()
                                                        .consumeResponse().getLatestResponse(ChannelCloseBody.class);
            assertThat(inUseResponse.getReplyCode(), is(equalTo(ErrorCodes.NOT_ALLOWED)));
            interaction.channel().closeOk();

            interaction.channel().open().consumeResponse(ChannelOpenOkBody.class)
                       .exchange()
                       .deleteExchangeName(TEST_EXCHANGE)
                       .delete()
                       .consumeResponse(ExchangeDeleteOkBody.class)
                       .exchange()
                       .deleteExchangeName(altExchName)
                       .delete()
                       .consumeResponse(ExchangeDeleteOkBody.class);
        }
    }

    /** Qpid specific extension */
    @Test
    public void exchangeDeclareWithUnknownAlternateExchange() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            ConnectionCloseBody response = interaction.negotiateOpen()
                                                      .channel().open().consumeResponse(ChannelOpenOkBody.class)
                                                      .exchange()
                                                      .declareName(TEST_EXCHANGE)
                                                      .declareArguments(Collections.singletonMap("alternateExchange", "notKnown")).declare()
                                                      .consumeResponse().getLatestResponse(ConnectionCloseBody.class);
            // TODO server fails - jira required
            assertThat(response.getReplyCode(), is(equalTo(ErrorCodes.NOT_FOUND)));
        }
    }
}
