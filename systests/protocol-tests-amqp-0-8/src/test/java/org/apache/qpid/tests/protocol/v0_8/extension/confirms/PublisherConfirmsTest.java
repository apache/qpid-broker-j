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
package org.apache.qpid.tests.protocol.v0_8.extension.confirms;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Set;

import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.protocol.v0_8.transport.BasicAckBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicNackBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicReturnBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelOpenOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConfirmSelectOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.apache.qpid.server.protocol.v0_8.transport.TxCommitOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.TxSelectOkBody;
import org.apache.qpid.tests.protocol.Response;
import org.apache.qpid.tests.protocol.SpecificationTest;
import org.apache.qpid.tests.protocol.v0_8.FrameTransport;
import org.apache.qpid.tests.protocol.v0_8.Interaction;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;

/**
 * The specification for publisher confirms is:  https://www.rabbitmq.com/confirms.html
 */
public class PublisherConfirmsTest extends BrokerAdminUsingTestBase
{
    @Before
    public void setUp()
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
    }

    @Test
    @SpecificationTest(section = "https://www.rabbitmq.com/confirms.html",
            description = "Once a channel is in confirm mode, both the broker and the client count messages "
                          + "(counting starts at 1 on the first confirm.select)The broker then confirms messages as "
                          + "it handles them by sending a basic.ack on the same channel. The delivery-tag field "
                          + "contains the sequence number of the confirmed message." )
    public void publishMessage() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            BasicAckBody ackBody = interaction.negotiateOpen()
                                              .channel().open().consumeResponse(ChannelOpenOkBody.class)
                                              .basic().confirmSelect().consumeResponse(ConfirmSelectOkBody.class)
                                              .basic().publishExchange("")
                                              .publishRoutingKey(BrokerAdmin.TEST_QUEUE_NAME)
                                              .content("Test")
                                              .publishMessage()
                                              .consumeResponse().getLatestResponse(BasicAckBody.class);

            assertThat(ackBody.getDeliveryTag(), is(equalTo(1L)));
            assertThat(getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(1)));
        }
    }

    @Test
    @SpecificationTest(section = "https://www.rabbitmq.com/confirms.html",
            description = "[...] when the broker is unable to handle messages successfully, instead of a basic.ack,"
                          + "the broker will send a basic.nack.")
    public void publishUnrouteableMandatoryMessage() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            BasicNackBody nackBody = interaction.negotiateOpen()
                                                      .channel()
                                                      .open()
                                                      .consumeResponse(ChannelOpenOkBody.class)
                                                      .basic()
                                                      .confirmSelect()
                                                      .consumeResponse(ConfirmSelectOkBody.class)
                                                      .basic()
                                                      .publishExchange("")
                                                      .publishRoutingKey("unrouteable")
                                                      .publishMandatory(true)
                                                      .content("Test")
                                                      .publishMessage()
                                                      .consumeResponse()
                                                      .getLatestResponse(BasicNackBody.class);
            assertThat(nackBody.getDeliveryTag(), is(equalTo(1L)));
        }
    }

    @Test
    @SpecificationTest(section = "https://www.rabbitmq.com/confirms.html",
            description = "After a channel is put into confirm mode, all subsequently published messages will be "
                          + "confirmed or nack'd once")
    public void publishUnrouteableMessage() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .channel().open().consumeResponse(ChannelOpenOkBody.class)
                       .basic().confirmSelect().consumeResponse(ConfirmSelectOkBody.class)
                       .basic().publishExchange("")
                       .publishRoutingKey("unrouteable")
                       .publishMandatory(false)
                       .content("Test")
                       .publishMessage()
                       .consumeResponse(BasicAckBody.class);
        }
    }

    /** Qpid allows publisher confirms to be used with transactions.  This is beyond what RabbitMQ supports.  */
    @Test
    public void publishWithTransactionalConfirms() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            final BasicAckBody ackBody = interaction.negotiateOpen()
                                                    .channel().open().consumeResponse(ChannelOpenOkBody.class)
                                                    .tx().select()
                                                    .consumeResponse(TxSelectOkBody.class)
                                                    .basic()
                                                    .confirmSelect()
                                                    .consumeResponse(ConfirmSelectOkBody.class)
                                                    .basic().publishExchange("")
                                                    .publishRoutingKey(BrokerAdmin.TEST_QUEUE_NAME)
                                                    .content("Test")
                                                    .publishMessage()
                                                    .consumeResponse().getLatestResponse(BasicAckBody.class);

            assertThat(ackBody.getDeliveryTag(), is(equalTo(1L)));
            assertThat(getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(0)));

            interaction.tx().commit().consumeResponse(TxCommitOkBody.class);

            assertThat(getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(1)));

        }
    }

    @Test
    public void publishUnroutableMessageWithTransactionalConfirms() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .channel().open().consumeResponse(ChannelOpenOkBody.class)
                       .tx().select()
                       .consumeResponse(TxSelectOkBody.class)
                       .basic()
                       .confirmSelect()
                       .consumeResponse(ConfirmSelectOkBody.class)
                       .basic().publishExchange("")
                       .publishRoutingKey("unrouteable")
                       .publishMandatory(true)
                       .publishMessage()
                       .basic().publishExchange("")
                       .publishRoutingKey(BrokerAdmin.TEST_QUEUE_NAME)
                       .publishMandatory(true)
                       .publishMessage();

            final BasicNackBody nackBody = interaction.consumeResponse().getLatestResponse(BasicNackBody.class);
            assertThat(nackBody.getDeliveryTag(), is(equalTo(1L)));

            final BasicAckBody ackBody = interaction.consumeResponse().getLatestResponse(BasicAckBody.class);
            assertThat(ackBody.getDeliveryTag(), is(equalTo(2L)));

            assertThat(getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(0)));

            interaction.tx().commit();

            final Set<Class<?>> outstanding = Sets.newHashSet(TxCommitOkBody.class, BasicReturnBody.class,
                                                              ContentHeaderBody.class);

            while (!outstanding.isEmpty())
            {
                final Response<?> response =
                        interaction.consumeResponse(outstanding.toArray(new Class<?>[outstanding.size()]))
                                   .getLatestResponse();
                final boolean remove = outstanding.remove(response.getBody().getClass());
                assertThat("" + response, remove, is(equalTo(true)));
            }

            assertThat(getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(1)));

        }
    }
}
