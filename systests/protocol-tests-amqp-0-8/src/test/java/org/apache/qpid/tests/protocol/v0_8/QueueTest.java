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
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.transport.BasicCancelOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicConsumeOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelCloseBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelOpenOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueDeclareOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueDeleteOkBody;
import org.apache.qpid.tests.protocol.SpecificationTest;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;

public class QueueTest extends BrokerAdminUsingTestBase
{
    private InetSocketAddress _brokerAddress;

    @Before
    public void setUp()
    {
        _brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);
    }

    @Test
    @SpecificationTest(section = "1.7.2.1", description = "declare queue, create if needed")
    public void queueDeclare() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            QueueDeclareOkBody response = interaction.openAnonymousConnection()
                                                     .channel().open().consumeResponse(ChannelOpenOkBody.class)
                                                     .queue().declareName(BrokerAdmin.TEST_QUEUE_NAME).declare()
                                                     .consumeResponse().getLatestResponse(QueueDeclareOkBody.class);

            assertThat(response.getQueue(), is(equalTo(AMQShortString.valueOf(BrokerAdmin.TEST_QUEUE_NAME))));
            assertThat(response.getMessageCount(), is(equalTo(0L)));
            assertThat(response.getConsumerCount(), is(equalTo(0L)));
        }
    }

    @Test
    @SpecificationTest(section = "1.7.2.1",
            description = "If [declarePassive is] set, the server will reply with Declare-Ok if the queue already exists"
                          + "with the same name, and raise an error if not.")
    public void queueDeclarePassive() throws Exception
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);

        try(FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            QueueDeclareOkBody response = interaction.openAnonymousConnection()
                                                     .channel().open().consumeResponse(ChannelOpenOkBody.class)
                                                     .queue().declarePassive(true).declareName(BrokerAdmin.TEST_QUEUE_NAME).declare()
                                                     .consumeResponse().getLatestResponse(QueueDeclareOkBody.class);

            assertThat(response.getQueue(), is(equalTo(AMQShortString.valueOf(BrokerAdmin.TEST_QUEUE_NAME))));
            assertThat(response.getMessageCount(), is(equalTo(0L)));
            assertThat(response.getConsumerCount(), is(equalTo(0L)));

            getBrokerAdmin().deleteQueue(BrokerAdmin.TEST_QUEUE_NAME);

            ChannelCloseBody closeResponse = interaction.queue()
                                                        .deleteName(BrokerAdmin.TEST_QUEUE_NAME).delete()
                                                        .consumeResponse().getLatestResponse(ChannelCloseBody.class);
            assertThat(closeResponse.getReplyCode(), is(equalTo(ErrorCodes.NOT_FOUND)));
        }
    }

    @Test
    @SpecificationTest(section = "1.7.2.1",
            description = "If [declareDurable is] set when creating a new queue, the queue will be marked as durable."
                          + "Durable queues remain active when a server restarts.")
    public void queueDeclareDurable() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            QueueDeclareOkBody response = interaction.openAnonymousConnection()
                                                     .channel().open().consumeResponse(ChannelOpenOkBody.class)
                                                     .queue().declareDurable(true).declareName(BrokerAdmin.TEST_QUEUE_NAME).declare()
                                                     .consumeResponse().getLatestResponse(QueueDeclareOkBody.class);

            assertThat(response.getQueue(), is(equalTo(AMQShortString.valueOf(BrokerAdmin.TEST_QUEUE_NAME))));
            assertThat(response.getMessageCount(), is(equalTo(0L)));
            assertThat(response.getConsumerCount(), is(equalTo(0L)));
        }

        assumeThat(getBrokerAdmin().supportsRestart(), Matchers.is(true));
        getBrokerAdmin().restart();

        try(FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            QueueDeclareOkBody response = interaction.openAnonymousConnection()
                                                     .channel().open().consumeResponse(ChannelOpenOkBody.class)
                                                     .queue().declarePassive(true).declareName(BrokerAdmin.TEST_QUEUE_NAME).declare()
                                                     .consumeResponse().getLatestResponse(QueueDeclareOkBody.class);

            assertThat(response.getQueue(), is(equalTo(AMQShortString.valueOf(BrokerAdmin.TEST_QUEUE_NAME))));
            assertThat(response.getMessageCount(), is(equalTo(0L)));
            assertThat(response.getConsumerCount(), is(equalTo(0L)));
        }
    }

    @Test
    @SpecificationTest(section = "1.7.2.9", description = "delete a queue")
    public void queueDelete() throws Exception
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
        getBrokerAdmin().putMessageOnQueue(BrokerAdmin.TEST_QUEUE_NAME, "message");

        try(FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            QueueDeleteOkBody response = interaction.openAnonymousConnection()
                                                    .channel().open().consumeResponse(ChannelOpenOkBody.class)
                                                    .queue().deleteName(BrokerAdmin.TEST_QUEUE_NAME).delete()
                                                    .consumeResponse().getLatestResponse(QueueDeleteOkBody.class);

            assertThat(response.getMessageCount(), is(equalTo(1L)));
        }
    }

    @Test
    @SpecificationTest(section = "1.7.2.9",
                       description = "The client MUST NOT attempt to delete a queue that does not exist.")
    public void queueDeleteQueueNotFound() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            ChannelCloseBody response = interaction.openAnonymousConnection()
                                                   .channel().open().consumeResponse(ChannelOpenOkBody.class)
                                                   .queue().deleteName(BrokerAdmin.TEST_QUEUE_NAME).delete()
                                                   .consumeResponse().getLatestResponse(ChannelCloseBody.class);

            assertThat(response.getReplyCode(), is(equalTo(ErrorCodes.NOT_FOUND)));
        }
    }

    @Test
    @SpecificationTest(section = "1.7.2.9",
            description = "If [deleteIfUnused is] set, the server will only delete the queue if it has no consumers. "
                          + "If the queue has consumers the server does does not delete it but raises a channel "
                          + "exception instead..")
    public void queueDeleteWithConsumer() throws Exception
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);

        try(FrameTransport consumerTransport = new FrameTransport(_brokerAddress).connect();
            FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final String consumerTag = "A";
            final Interaction consumerInteraction = consumerTransport.newInteraction();
            final BasicInteraction basicInteraction = consumerInteraction.openAnonymousConnection()
                                                                         .channel()
                                                                         .open()
                                                                         .consumeResponse(ChannelOpenOkBody.class)
                                                                         .basic();
            basicInteraction.consumeConsumerTag(consumerTag).consumeQueue(BrokerAdmin.TEST_QUEUE_NAME).consume()
                            .consumeResponse(BasicConsumeOkBody.class);

            final Interaction deleterInteraction = transport.newInteraction();
            ChannelCloseBody response = deleterInteraction.openAnonymousConnection()
                                                          .channel().open().consumeResponse(ChannelOpenOkBody.class)
                                                          .queue().deleteName(BrokerAdmin.TEST_QUEUE_NAME).deleteIfUnused(true).delete()
                                                          .consumeResponse().getLatestResponse(ChannelCloseBody.class);

            assertThat(response.getReplyCode(), is(equalTo(ErrorCodes.IN_USE)));
            deleterInteraction.channel().closeOk();

            basicInteraction.consumeCancelTag(consumerTag).cancel()
                            .consumeResponse().getLatestResponse(BasicCancelOkBody.class);

            deleterInteraction.channel().open().consumeResponse(ChannelOpenOkBody.class)
                              .queue().deleteName(BrokerAdmin.TEST_QUEUE_NAME).delete()
                              .consumeResponse().getLatestResponse(QueueDeleteOkBody.class);
        }
    }
}
