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
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.isEmptyString;
import static org.junit.Assume.assumeThat;

import java.net.InetSocketAddress;
import java.util.Collections;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.transport.BasicCancelOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicConsumeOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelCloseBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelOpenOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionCloseBody;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeBoundOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeDeclareOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeDeleteOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueBindOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueDeclareOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueDeleteOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueuePurgeOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueUnbindOkBody;
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
    @SpecificationTest(section = "1.7.2.1", description = "If not set and the queue exists, the server MUST check "
                                                          + "that the existing queue has the same values for durable, "
                                                          + "exclusive, auto-delete, and arguments fields.")
    public void queueDeclareEquivalent() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            QueueInteraction queueInteraction = interaction.openAnonymousConnection()
                                                .channel().open().consumeResponse(ChannelOpenOkBody.class)
                                                .queue();
            QueueDeclareOkBody response = queueInteraction.declareName(BrokerAdmin.TEST_QUEUE_NAME)
                                                          .declareExclusive(false).declare()
                                                          .consumeResponse().getLatestResponse(QueueDeclareOkBody.class);

            assertThat(response.getQueue(), is(equalTo(AMQShortString.valueOf(BrokerAdmin.TEST_QUEUE_NAME))));

            QueueDeclareOkBody equalDeclareResponse = queueInteraction.declareName(BrokerAdmin.TEST_QUEUE_NAME)
                                                                      .declareExclusive(false).declare()
                                                                      .consumeResponse().getLatestResponse(QueueDeclareOkBody.class);
            assertThat(equalDeclareResponse.getQueue(), is(equalTo(AMQShortString.valueOf(BrokerAdmin.TEST_QUEUE_NAME))));

            ChannelCloseBody unequalDeclareResponse = queueInteraction.declareName(BrokerAdmin.TEST_QUEUE_NAME)
                                                                      .declareExclusive(true).declare()
                                                                      .consumeResponse().getLatestResponse(ChannelCloseBody.class);
            assertThat(unequalDeclareResponse.getReplyCode(), is(equalTo(ErrorCodes.ALREADY_EXISTS)));

            interaction.channel().closeOk();
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
            description = "If [durable is] set when creating a new queue, the queue will be marked as durable. "
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
    @SpecificationTest(section = "1.7.2.1",
            description = "If [auto-delete] set, the queue is deleted when all consumers have finished using it. The "
                          + "last consumer can be cancelled either explicitly or because its channel is closed. "
                          + "If there was no consumer ever on the queue, it won't be deleted.")
    public void queueDeclareAutoDelete() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            QueueDeclareOkBody response = interaction.openAnonymousConnection()
                                                     .channel().open().consumeResponse(ChannelOpenOkBody.class)
                                                     .queue().declareAutoDelete(true).declareName(BrokerAdmin.TEST_QUEUE_NAME).declare()
                                                     .consumeResponse().getLatestResponse(QueueDeclareOkBody.class);

            assertThat(response.getQueue(), is(equalTo(AMQShortString.valueOf(BrokerAdmin.TEST_QUEUE_NAME))));
        }

        try(FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();

            QueueDeclareOkBody response = interaction.openAnonymousConnection()
                                                     .channel().open().consumeResponse(ChannelOpenOkBody.class)
                                                     .queue().declarePassive(true).declareName(BrokerAdmin.TEST_QUEUE_NAME).declare()
                                                     .consumeResponse().getLatestResponse(QueueDeclareOkBody.class);
            assertThat(response.getQueue(), is(equalTo(AMQShortString.valueOf(BrokerAdmin.TEST_QUEUE_NAME))));

            final String consumerTag = "lastConsumer";
            interaction.basic()
                       .consumeConsumerTag(consumerTag).consumeQueue(BrokerAdmin.TEST_QUEUE_NAME).consume()
                       .consumeResponse(BasicConsumeOkBody.class)
                       .basic().consumeCancelTag(consumerTag).cancel()
                       .consumeResponse().getLatestResponse(BasicCancelOkBody.class);

            ChannelCloseBody closeResponse = interaction.queue()
                                                        .declarePassive(true)
                                                        .declareName(BrokerAdmin.TEST_QUEUE_NAME)
                                                        .declare()
                                                        .consumeResponse()
                                                        .getLatestResponse(ChannelCloseBody.class);
            assertThat(closeResponse.getReplyCode(), is(equalTo(ErrorCodes.NOT_FOUND)));
        }
    }

    @Test
    @SpecificationTest(section = "1.7.2.1",
            description = "The server MUST ignore the auto-delete field if the queue already exists.")
    @Ignore("The server does not ignore the auto-delete field if the queue already exists.")
    public void queueDeclareAutoDeletePreexistingQueue() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            QueueDeclareOkBody response = interaction.openAnonymousConnection()
                                                     .channel().open().consumeResponse(ChannelOpenOkBody.class)
                                                     .queue().declareName(BrokerAdmin.TEST_QUEUE_NAME).declare()
                                                     .consumeResponse().getLatestResponse(QueueDeclareOkBody.class);

            assertThat(response.getQueue(), is(equalTo(AMQShortString.valueOf(BrokerAdmin.TEST_QUEUE_NAME))));

            QueueDeclareOkBody passiveResponse =
                    interaction.queue().declareAutoDelete(true).declareName(BrokerAdmin.TEST_QUEUE_NAME).declare()
                               .consumeResponse().getLatestResponse(QueueDeclareOkBody.class);
            assertThat(passiveResponse.getQueue(), is(equalTo(AMQShortString.valueOf(BrokerAdmin.TEST_QUEUE_NAME))));
        }
    }

    @Test
    @SpecificationTest(section = "1.7.2.1",
            description = "The client MAY NOT attempt to use a queue that was declared as exclusive by another "
                          + "still-open connection.")
    public void queueDeclareExclusive() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            QueueDeclareOkBody response = interaction.openAnonymousConnection()
                                                     .channel().open().consumeResponse(ChannelOpenOkBody.class)
                                                     .queue().declareName(BrokerAdmin.TEST_QUEUE_NAME).declareExclusive(true).declare()
                                                     .consumeResponse().getLatestResponse(QueueDeclareOkBody.class);

            assertThat(response.getQueue(), is(equalTo(AMQShortString.valueOf(BrokerAdmin.TEST_QUEUE_NAME))));

            try(FrameTransport transport2 = new FrameTransport(_brokerAddress).connect())
            {
                final Interaction interaction2 = transport2.newInteraction();
                ConnectionCloseBody closeResponse = interaction2.openAnonymousConnection()
                                                                .channel().open().consumeResponse(ChannelOpenOkBody.class)
                                                                .queue().declareName(BrokerAdmin.TEST_QUEUE_NAME).declare()
                                                                .consumeResponse().getLatestResponse(ConnectionCloseBody.class);
                /* TODO: 0-91 specification requires 'resource-locked' (405) but server uses (530) */
                assertThat(closeResponse.getReplyCode(), anyOf(equalTo(ErrorCodes.NOT_ALLOWED), equalTo(405)));
            }
        }

        try(FrameTransport transport2 = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction2 = transport2.newInteraction();
            QueueDeclareOkBody response = interaction2.openAnonymousConnection()
                                                            .channel().open().consumeResponse(ChannelOpenOkBody.class)
                                                            .queue().declareName(BrokerAdmin.TEST_QUEUE_NAME).declare()
                                                            .consumeResponse().getLatestResponse(QueueDeclareOkBody.class);
            assertThat(response.getQueue(), is(equalTo(AMQShortString.valueOf(BrokerAdmin.TEST_QUEUE_NAME))));
        }
    }

    @Test
    @SpecificationTest(section = "1.7.2.1",
            description = "The queue name MAY be empty, in which case the server MUST create a new queue with a unique "
                          + "generated name and return this to the client in the Declare-Ok method.")
    public void queueDeclareServerAssignedName() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            QueueDeclareOkBody response = interaction.openAnonymousConnection()
                                                     .channel().open().consumeResponse(ChannelOpenOkBody.class)
                                                     .queue().declare()
                                                     .consumeResponse().getLatestResponse(QueueDeclareOkBody.class);

            String serverAssignedQueueName = response.getQueue().toString();
            assertThat(serverAssignedQueueName, is(not(isEmptyString())));

            QueueDeclareOkBody passive = interaction.queue()
                                                    .declareName(serverAssignedQueueName)
                                                    .declarePassive(true).declare()
                                                    .consumeResponse().getLatestResponse(QueueDeclareOkBody.class);
            assertThat(passive.getQueue(), is(equalTo(AMQShortString.valueOf(serverAssignedQueueName))));
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
            description = "If [if-unused is] set, the server will only delete the queue if it has no consumers. "
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

    @Test
    @SpecificationTest(section = "1.7.2.9",
            description = "The client MUST either specify a queue name or have previously declared a queue on the "
                          + "same channel")
    public void queueDeleteDefaultQueue() throws Exception
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
        getBrokerAdmin().putMessageOnQueue(BrokerAdmin.TEST_QUEUE_NAME, "message");

        try(FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            QueueDeleteOkBody deleteResponse = interaction.openAnonymousConnection()
                                                          .channel().open().consumeResponse(ChannelOpenOkBody.class)
                                                          .queue().declareName(BrokerAdmin.TEST_QUEUE_NAME).declare()
                                                          .consumeResponse(QueueDeclareOkBody.class)
                                                          .queue().delete()
                                                          .consumeResponse().getLatestResponse(QueueDeleteOkBody.class);
            assertThat(deleteResponse.getMessageCount(), is(equalTo(1L)));
        }
    }

    @Test
    @SpecificationTest(section = "1.7.2.7", description = "purge a queue")
    public void queuePurge() throws Exception
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
        getBrokerAdmin().putMessageOnQueue(BrokerAdmin.TEST_QUEUE_NAME, "message");

        try(FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            QueuePurgeOkBody response = interaction.openAnonymousConnection()
                                                   .channel().open().consumeResponse(ChannelOpenOkBody.class)
                                                   .queue().purgeName(BrokerAdmin.TEST_QUEUE_NAME).purge()
                                                   .consumeResponse().getLatestResponse(QueuePurgeOkBody.class);

            /* TODO purge currently always returns 0 */
            //assertThat(response.getMessageCount(), is(equalTo(1L)));

            QueueDeclareOkBody passive = interaction.queue()
                                                    .declareName(BrokerAdmin.TEST_QUEUE_NAME)
                                                    .declarePassive(true).declare()
                                                    .consumeResponse().getLatestResponse(QueueDeclareOkBody.class);
            assertThat(passive.getQueue(), is(equalTo(AMQShortString.valueOf(BrokerAdmin.TEST_QUEUE_NAME))));
            assertThat(response.getMessageCount(), is(equalTo(0L)));
        }
    }

    @Test
    @SpecificationTest(section = "1.7.2.7", description = "The client MUST NOT attempt to purge a queue that does not exist.")
    public void queuePurgeQueueNotFound() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            ChannelCloseBody response = interaction.openAnonymousConnection()
                                                   .channel().open().consumeResponse(ChannelOpenOkBody.class)
                                                   .queue().purgeName(BrokerAdmin.TEST_QUEUE_NAME).purge()
                                                   .consumeResponse().getLatestResponse(ChannelCloseBody.class);
            assertThat(response.getReplyCode(), is(equalTo(ErrorCodes.NOT_FOUND)));
        }
    }

    @Test
    @SpecificationTest(section = "1.7.2.3", description = "bind queue to an exchange")
    public void queueBind() throws Exception
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);

        try(FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            String testExchange = "testExchange";
            interaction.openAnonymousConnection()
                       .channel().open().consumeResponse(ChannelOpenOkBody.class)
                       .exchange().declareName(testExchange).declare()
                       .consumeResponse(ExchangeDeclareOkBody.class)
                       .queue().bindName(testExchange).bindQueueName(BrokerAdmin.TEST_QUEUE_NAME).bindRoutingKey("rk1").bind()
                       .consumeResponse(QueueBindOkBody.class);
        }
    }

    @Test
    @SpecificationTest(section = "1.7.2.3", description = "A server MUST allow ignore duplicate bindings")
    public void queueBindIgnoreDuplicates() throws Exception
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);

        try(FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            String testExchange = "testExchange";
            interaction.openAnonymousConnection()
                       .channel().open().consumeResponse(ChannelOpenOkBody.class)
                       .exchange().declareName(testExchange).declare()
                       .consumeResponse(ExchangeDeclareOkBody.class)
                       .queue().bindName(testExchange).bindQueueName(BrokerAdmin.TEST_QUEUE_NAME).bindRoutingKey("rk1").bind()
                       .consumeResponse(QueueBindOkBody.class)
                       .queue().bindName(testExchange).bindQueueName(BrokerAdmin.TEST_QUEUE_NAME).bindRoutingKey("rk1").bind()
                       .consumeResponse(QueueBindOkBody.class);

            ExchangeBoundOkBody response = interaction.exchange()
                                                      .boundExchangeName(testExchange)
                                                      .bound()
                                                      .consumeResponse()
                                                      .getLatestResponse(ExchangeBoundOkBody.class);
            assertThat(response.getReplyCode(), is(equalTo(ExchangeBoundOkBody.OK)));

            interaction.queue()
                       .unbindName(testExchange)
                       .unbindQueueName(BrokerAdmin.TEST_QUEUE_NAME)
                       .unbindRoutingKey("rk1")
                       .unbind()
                       .consumeResponse(QueueUnbindOkBody.class);

            response = interaction.exchange()
                                  .boundExchangeName(testExchange)
                                  .bound()
                                  .consumeResponse()
                                  .getLatestResponse(ExchangeBoundOkBody.class);
            assertThat(response.getReplyCode(), is(equalTo(ExchangeBoundOkBody.NO_BINDINGS)));
        }
    }

    @Test
    @SpecificationTest(section = "1.7.2.3",
            description = "The client MUST NOT attempt to bind a queue that does not exist.")
    public void queueBindUnknownQueue() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            String testExchange = "testExchange";
            final Interaction interaction = transport.newInteraction();
            ChannelCloseBody response = interaction.openAnonymousConnection()
                                              .channel().open().consumeResponse(ChannelOpenOkBody.class)
                                              .exchange().declareName(testExchange).declare()
                                              .consumeResponse(ExchangeDeclareOkBody.class)
                                              .queue().bindName(testExchange).bindQueueName(BrokerAdmin.TEST_QUEUE_NAME)
                                              .bind()
                                              .consumeResponse().getLatestResponse(ChannelCloseBody.class);
            assertThat(response.getReplyCode(), is(equalTo(ErrorCodes.NOT_FOUND)));
        }
    }

    @Test
    @SpecificationTest(section = "1.7.2.3",
            description = "The client MUST either specify a queue name or have previously declared a queue on the same channel")
    public void queueBindDefaultQueue() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            String testExchange = "testExchange";
            final Interaction interaction = transport.newInteraction();
            interaction.openAnonymousConnection()
                       .channel().open().consumeResponse(ChannelOpenOkBody.class)
                       .queue().declareName(BrokerAdmin.TEST_QUEUE_NAME).declare()
                       .consumeResponse(QueueDeclareOkBody.class)
                       .exchange().declareName(testExchange).declare()
                       .consumeResponse(ExchangeDeclareOkBody.class)
                       .queue().bindName(testExchange).bind()
                       .consumeResponse(QueueBindOkBody.class);

            ExchangeBoundOkBody response = interaction.exchange()
                                                      .boundExchangeName(testExchange)
                                                      .boundQueue(BrokerAdmin.TEST_QUEUE_NAME)
                                                      .bound()
                                                      .consumeResponse()
                                                      .getLatestResponse(ExchangeBoundOkBody.class);
            assertThat(response.getReplyCode(), is(equalTo(ExchangeBoundOkBody.OK)));
        }
    }

    @Test
    @SpecificationTest(section = "1.7.2.3",
            description = "Bindings of durable queues to durable exchanges are automatically durable and the server "
                          + "MUST restore such bindings after a server restart.")
    public void queueDurableBind() throws Exception
    {
        String testExchange = "testExchange";
        String testRoutingKey = "rk1";
        try(FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.openAnonymousConnection()
                       .channel().open().consumeResponse(ChannelOpenOkBody.class)
                       .queue().declareName(BrokerAdmin.TEST_QUEUE_NAME).declareDurable(true).declare()
                       .consumeResponse(QueueDeclareOkBody.class)
                       .exchange().declareName(testExchange).declareDurable(true).declare()
                       .consumeResponse(ExchangeDeclareOkBody.class)
                       .queue().bindName(testExchange).bindQueueName(BrokerAdmin.TEST_QUEUE_NAME).bindRoutingKey(testRoutingKey)
                       .bind()
                       .consumeResponse(QueueBindOkBody.class);

            ExchangeBoundOkBody response = interaction.exchange()
                                                      .boundExchangeName(testExchange)
                                                      .boundQueue(BrokerAdmin.TEST_QUEUE_NAME)
                                                      .boundRoutingKey(testRoutingKey)
                                                      .bound()
                                                      .consumeResponse()
                                                      .getLatestResponse(ExchangeBoundOkBody.class);
            assertThat(response.getReplyCode(), is(equalTo(ExchangeBoundOkBody.OK)));
        }

        assumeThat(getBrokerAdmin().supportsRestart(), Matchers.is(true));
        getBrokerAdmin().restart();

        try(FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            ExchangeBoundOkBody response = interaction.openAnonymousConnection()
                                                      .channel().open().consumeResponse(ChannelOpenOkBody.class)
                                                      .exchange()
                                                      .boundExchangeName(testExchange)
                                                      .boundQueue(BrokerAdmin.TEST_QUEUE_NAME)
                                                      .boundRoutingKey(testRoutingKey)
                                                      .bound()
                                                      .consumeResponse()
                                                      .getLatestResponse(ExchangeBoundOkBody.class);
            assertThat(response.getReplyCode(), is(equalTo(ExchangeBoundOkBody.OK)));
        }
    }

    @Test
    @SpecificationTest(section = "1.7.2.3",
            description = "The server MUST allow a durable queue to bind to a transient exchange.")
    public void queueBindDurableQueueToTransientExchange() throws Exception
    {
        String testExchange = "testExchange";
        String testRoutingKey = "rk1";
        try(FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.openAnonymousConnection()
                       .channel().open().consumeResponse(ChannelOpenOkBody.class)
                       .queue().declareName(BrokerAdmin.TEST_QUEUE_NAME).declareDurable(true).declare()
                       .consumeResponse(QueueDeclareOkBody.class)
                       .exchange().declareName(testExchange).declareDurable(false).declare()
                       .consumeResponse(ExchangeDeclareOkBody.class)
                       .queue().bindName(testExchange).bindQueueName(BrokerAdmin.TEST_QUEUE_NAME).bindRoutingKey(testRoutingKey)
                       .bind()
                       .consumeResponse(QueueBindOkBody.class);

            ExchangeBoundOkBody response = interaction.exchange()
                                                      .boundExchangeName(testExchange)
                                                      .boundQueue(BrokerAdmin.TEST_QUEUE_NAME)
                                                      .boundRoutingKey(testRoutingKey)
                                                      .bound()
                                                      .consumeResponse()
                                                      .getLatestResponse(ExchangeBoundOkBody.class);
            assertThat(response.getReplyCode(), is(equalTo(ExchangeBoundOkBody.OK)));
        }
    }

    @Test
    @SpecificationTest(section = "1.7.2.5", description = "unbind a queue from an exchange")
    public void queueUnbind() throws Exception
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);

        try(FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            String testExchange = "testExchange";
            interaction.openAnonymousConnection()
                       .channel().open().consumeResponse(ChannelOpenOkBody.class)
                       .exchange().declareName(testExchange).declare()
                       .consumeResponse(ExchangeDeclareOkBody.class)
                       .queue().bindName(testExchange).bindQueueName(BrokerAdmin.TEST_QUEUE_NAME).bindRoutingKey("rk1").bind()
                       .consumeResponse(QueueBindOkBody.class)
                       .queue().unbindName(testExchange).unbindQueueName(BrokerAdmin.TEST_QUEUE_NAME).unbindRoutingKey("rk1").unbind()
                       .consumeResponse(QueueUnbindOkBody.class);
        }
    }

    @Test
    @SpecificationTest(section = "1.7.2.5",
            description = "The client MUST either specify a queue name or have previously declared a queue on the "
                          + "same channel")
    public void queueUnbindDefaultQueue() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            String testExchange = "testExchange";
            interaction.openAnonymousConnection()
                       .channel().open().consumeResponse(ChannelOpenOkBody.class)
                       .queue().declareName(BrokerAdmin.TEST_QUEUE_NAME).declare()
                       .consumeResponse(QueueDeclareOkBody.class)
                       .exchange().declareName(testExchange).declare()
                       .consumeResponse(ExchangeDeclareOkBody.class)
                       .queue().bindName(testExchange).bindQueueName(BrokerAdmin.TEST_QUEUE_NAME).bindRoutingKey("rk1").bind()
                       .consumeResponse(QueueBindOkBody.class)
                       .queue().unbindName(testExchange).unbindRoutingKey("rk1").unbind()
                       .consumeResponse(QueueUnbindOkBody.class);
        }
    }

    @Test
    @SpecificationTest(section = "1.7.2.5", description = "The client MUST NOT attempt to unbind a queue that does "
                                                          + "not exist.")
    public void queueUnbindUnknownQueue() throws Exception
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);

        try(FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            String testExchange = "testExchange";
            ChannelCloseBody response = interaction.openAnonymousConnection()
                                                   .channel().open().consumeResponse(ChannelOpenOkBody.class)
                                                   .exchange().declareName(testExchange).declare()
                                                   .consumeResponse(ExchangeDeclareOkBody.class)
                                                   .queue()
                                                   .bindName(testExchange)
                                                   .bindQueueName(BrokerAdmin.TEST_QUEUE_NAME)
                                                   .bindRoutingKey("rk1")
                                                   .bind()
                                                   .consumeResponse(QueueBindOkBody.class)
                                                   .queue()
                                                   .unbindName(testExchange)
                                                   .unbindQueueName("unknownQueue")
                                                   .unbindRoutingKey("rk1")
                                                   .unbind()
                                                   .consumeResponse()
                                                   .getLatestResponse(ChannelCloseBody.class);
            assertThat(response.getReplyCode(), is(equalTo(ErrorCodes.NOT_FOUND)));
        }
    }

    @Test
    @SpecificationTest(section = "1.7.2.5", description = "The client MUST NOT attempt to unbind a queue from an "
                                                          + "exchange that does not exist.")
    public void queueUnbindUnknownExchange() throws Exception
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);

        try(FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            String testExchange = "testExchange";
            ChannelCloseBody response = interaction.openAnonymousConnection()
                                                   .channel().open().consumeResponse(ChannelOpenOkBody.class)
                                                   .exchange().declareName(testExchange).declare()
                                                   .consumeResponse(ExchangeDeclareOkBody.class)
                                                   .queue()
                                                   .bindName(testExchange)
                                                   .bindQueueName(BrokerAdmin.TEST_QUEUE_NAME)
                                                   .bindRoutingKey("rk1")
                                                   .bind()
                                                   .consumeResponse(QueueBindOkBody.class)
                                                   .queue()
                                                   .unbindName("unknownExchange")
                                                   .unbindQueueName(BrokerAdmin.TEST_QUEUE_NAME)
                                                   .unbindRoutingKey("rk1")
                                                   .unbind()
                                                   .consumeResponse()
                                                   .getLatestResponse(ChannelCloseBody.class);
            assertThat(response.getReplyCode(), is(equalTo(ErrorCodes.NOT_FOUND)));
        }
    }

    @Test
    @SpecificationTest(section = "1.7.2.5", description = "If a unbind fails, the server MUST raise a connection "
                                                          + "exception")
    public void queueUnbindUnknownRoutingKey() throws Exception
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);

        try(FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            String testExchange = "testExchange";
            ChannelCloseBody response = interaction.openAnonymousConnection()
                                                   .channel().open().consumeResponse(ChannelOpenOkBody.class)
                                                   .exchange().declareName(testExchange).declare()
                                                   .consumeResponse(ExchangeDeclareOkBody.class)
                                                   .queue()
                                                   .bindName(testExchange)
                                                   .bindQueueName(BrokerAdmin.TEST_QUEUE_NAME)
                                                   .bindRoutingKey("rk1")
                                                   .bind()
                                                   .consumeResponse(QueueBindOkBody.class)
                                                   .queue()
                                                   .unbindName(testExchange)
                                                   .unbindQueueName(BrokerAdmin.TEST_QUEUE_NAME)
                                                   .unbindRoutingKey("rk2")
                                                   .unbind()
                                                   .consumeResponse()
                                                   .getLatestResponse(ChannelCloseBody.class);
            assertThat(response.getReplyCode(), is(equalTo(ErrorCodes.NOT_FOUND)));
        }
    }

    /** Qpid specific extension */
    @Test
    public void queueDeclareWithAlternateExchange() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final String altExchName = "altExchange";
            final Interaction interaction = transport.newInteraction();
            interaction.openAnonymousConnection()
                       .channel().open().consumeResponse(ChannelOpenOkBody.class)
                       .exchange()
                       .declareName(altExchName)
                       .declare()
                       .consumeResponse(ExchangeDeclareOkBody.class)
                       .queue()
                       .declareName(BrokerAdmin.TEST_QUEUE_NAME)
                       .declareArguments(Collections.singletonMap("alternateExchange", altExchName)).declare()
                       .consumeResponse(QueueDeclareOkBody.class);

            ChannelCloseBody inUseResponse = interaction.exchange()
                                                        .deleteExchangeName(altExchName)
                                                        .delete()
                                                        .consumeResponse().getLatestResponse(ChannelCloseBody.class);
            assertThat(inUseResponse.getReplyCode(), is(equalTo(ErrorCodes.NOT_ALLOWED)));
            interaction.channel().closeOk();

            interaction.channel().open().consumeResponse(ChannelOpenOkBody.class)
                       .queue()
                       .deleteName(BrokerAdmin.TEST_QUEUE_NAME)
                       .delete()
                       .consumeResponse(QueueDeleteOkBody.class)
                       .exchange()
                       .deleteExchangeName(altExchName)
                       .delete()
                       .consumeResponse(ExchangeDeleteOkBody.class);
        }
    }

    /** Qpid specific extension */
    @Test
    public void queueDeclareWithUnknownAlternateExchange() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            ConnectionCloseBody response = interaction.openAnonymousConnection()
                                                      .channel().open().consumeResponse(ChannelOpenOkBody.class)
                                                      .queue()
                                                      .declareName(BrokerAdmin.TEST_QUEUE_NAME)
                                                      .declareArguments(Collections.singletonMap("alternateExchange", "notKnown")).declare()
                                                      .consumeResponse().getLatestResponse(ConnectionCloseBody.class);
            // TODO server fails - jira required
            assertThat(response.getReplyCode(), is(equalTo(ErrorCodes.NOT_FOUND)));
        }
    }
}
