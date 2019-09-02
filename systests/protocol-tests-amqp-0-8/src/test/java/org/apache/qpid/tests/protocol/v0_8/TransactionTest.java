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

import java.util.Collections;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.properties.ConnectionStartProperties;
import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.protocol.v0_8.transport.*;
import org.apache.qpid.tests.protocol.SpecificationTest;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;

public class TransactionTest extends BrokerAdminUsingTestBase
{

    @Before
    public void setUp()
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
    }

    @Test
    @SpecificationTest(section = "1.9.2.3", description = "The client MUST NOT use the Commit method on non-transacted "
                                                          + "channels.")
    public void commitNonTransactedChannel() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            ChannelCloseBody res = interaction.negotiateOpen()
                                              .channel().open().consumeResponse(ChannelOpenOkBody.class)
                                              .tx().commit()
                                              .consumeResponse().getLatestResponse(ChannelCloseBody.class);
            // TODO specification requires precondition-failed (406)
            assertThat(res.getReplyCode(), is(equalTo(ErrorCodes.COMMAND_INVALID)));
        }
    }

    @Test
    @SpecificationTest(section = "1.9.2.3", description = "The client MUST NOT use the Rollback method on non-transacted "
                                                          + "channels.")
    public void rollbackNonTransactedChannel() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            ChannelCloseBody res = interaction.negotiateOpen()
                                              .channel().open().consumeResponse(ChannelOpenOkBody.class)
                                              .tx().rollback()
                                              .consumeResponse().getLatestResponse(ChannelCloseBody.class);
            // TODO specification requires precondition-failed (406)
            assertThat(res.getReplyCode(), is(equalTo(ErrorCodes.COMMAND_INVALID)));
        }
    }

    @Test
    @SpecificationTest(section = "1.9.2.3", description = "commit the current transaction")
    public void publishMessage() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .channel().open().consumeResponse(ChannelOpenOkBody.class)
                       .tx().select().consumeResponse(TxSelectOkBody.class)
                       .basic().contentHeaderPropertiesContentType("text/plain")
                       .contentHeaderPropertiesHeaders(Collections.singletonMap("test", "testValue"))
                       .contentHeaderPropertiesDeliveryMode((byte)1)
                       .contentHeaderPropertiesPriority((byte)1)
                       .publishExchange("")
                       .publishRoutingKey(BrokerAdmin.TEST_QUEUE_NAME)
                       .content("Test")
                       .publishMessage()
                       .exchange().declarePassive(true).declare().consumeResponse(ExchangeDeclareOkBody.class);
            assertThat(getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(0)));

            interaction.tx().commit().consumeResponse(TxCommitOkBody.class);

            assertThat(getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(1)));
        }
    }
    @Test
    @SpecificationTest(section = "1.9.2.5", description = "abandon the current transaction")
    public void publishMessageRollback() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .channel().open().consumeResponse(ChannelOpenOkBody.class)
                       .tx().select().consumeResponse(TxSelectOkBody.class)
                       .basic()
                       .publishExchange("")
                       .publishRoutingKey(BrokerAdmin.TEST_QUEUE_NAME)
                       .content("Test")
                       .publishMessage()
                       .exchange().declarePassive(true).declare().consumeResponse(ExchangeDeclareOkBody.class);
            assertThat(getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(0)));

            interaction.tx().rollback().consumeResponse(TxRollbackOkBody.class);

            assertThat(getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(0)));
        }
    }

    @Test
    @SpecificationTest(section = "1.9.2.3", description = "commit the current transaction")
    public void consumeMessage() throws Exception
    {
        getBrokerAdmin().putMessageOnQueue(BrokerAdmin.TEST_QUEUE_NAME, "message");

        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();

            assertThat(getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(1)));

            interaction.negotiateOpen()
                       .channel().open().consumeResponse(ChannelOpenOkBody.class)
                       .tx().select().consumeResponse(TxSelectOkBody.class)
                       .basic().qosPrefetchCount(1)
                       .qos()
                       .consumeResponse(BasicQosOkBody.class)
                       .channel().flow(true)
                       .consumeResponse(ChannelFlowOkBody.class)
                       .basic()
                       .consumeConsumerTag("A")
                       .consumeQueue(BrokerAdmin.TEST_QUEUE_NAME)
                       .consume()
                       .consumeResponse(BasicConsumeOkBody.class)
                       .consumeResponse(BasicDeliverBody.class);

            BasicDeliverBody delivery = interaction.getLatestResponse(BasicDeliverBody.class);
            interaction.consumeResponse(ContentHeaderBody.class)
                       .consumeResponse(ContentBody.class);

            interaction.basic().ackDeliveryTag(delivery.getDeliveryTag())
                       .ack()
                       .exchange().declarePassive(true).declare().consumeResponse(ExchangeDeclareOkBody.class);

            assertThat(getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(1)));

            interaction.tx().commit().consumeResponse(TxCommitOkBody.class);

            assertThat(getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(0)));
        }
    }

    @Test
    @SpecificationTest(section = "1.9.2.5", description = "abandon the current transaction")
    public void consumeMessageRollbackAckdMessage() throws Exception
    {
        getBrokerAdmin().putMessageOnQueue(BrokerAdmin.TEST_QUEUE_NAME, "message");

        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();

            assertThat(getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(1)));

            interaction.negotiateOpen()
                       .channel().open().consumeResponse(ChannelOpenOkBody.class)
                       .tx().select().consumeResponse(TxSelectOkBody.class)
                       .basic().qosPrefetchCount(1)
                       .qos()
                       .consumeResponse(BasicQosOkBody.class)
                       .channel().flow(true)
                       .consumeResponse(ChannelFlowOkBody.class)
                       .basic()
                       .consumeConsumerTag("A")
                       .consumeQueue(BrokerAdmin.TEST_QUEUE_NAME)
                       .consume()
                       .consumeResponse(BasicConsumeOkBody.class)
                       .consumeResponse(BasicDeliverBody.class);

            BasicDeliverBody delivery = interaction.getLatestResponse(BasicDeliverBody.class);
            interaction.consumeResponse(ContentHeaderBody.class)
                       .consumeResponse(ContentBody.class);

            assertThat(delivery.getRedelivered(), is(equalTo(false)));

            interaction.basic().ackDeliveryTag(delivery.getDeliveryTag())
                       .ack()
                       .exchange().declarePassive(true).declare().consumeResponse(ExchangeDeclareOkBody.class);

            assertThat(getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(1)));

            interaction.tx().rollback().consumeResponse(TxRollbackOkBody.class);

            assertThat(getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(1)));

            // TODO: the server currently resends the messages again, which is not specification compliant but the
            // Qpid JMS AMQP 0-x relies on this behaviour.
            BasicDeliverBody redelivery = interaction.consumeResponse().getLatestResponse(BasicDeliverBody.class);
            interaction.consumeResponse(ContentHeaderBody.class)
                       .consumeResponse(ContentBody.class);

            assertThat(redelivery.getRedelivered(), is(equalTo(false)));

            // TODO: defect - the server actually sends the message again.
            redelivery = interaction.consumeResponse().getLatestResponse(BasicDeliverBody.class);
            interaction.consumeResponse(ContentHeaderBody.class)
                       .consumeResponse(ContentBody.class);

            interaction.channel().close()
                       .consumeResponse(ChannelCloseOkBody.class);
        }
    }

    @Test
    @SpecificationTest(section = "1.9.2.5", description = "abandon the current transaction")
    public void consumeMessageRollbackDeliveredMessage() throws Exception
    {
        getBrokerAdmin().putMessageOnQueue(BrokerAdmin.TEST_QUEUE_NAME, "message");

        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();

            assertThat(getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(1)));

            interaction.negotiateOpen()
                       .channel().open().consumeResponse(ChannelOpenOkBody.class)
                       .tx().select().consumeResponse(TxSelectOkBody.class)
                       .basic().qosPrefetchCount(1)
                       .qos()
                       .consumeResponse(BasicQosOkBody.class)
                       .channel().flow(true)
                       .consumeResponse(ChannelFlowOkBody.class)
                       .basic()
                       .consumeConsumerTag("A")
                       .consumeQueue(BrokerAdmin.TEST_QUEUE_NAME)
                       .consume()
                       .consumeResponse(BasicConsumeOkBody.class)
                       .consumeResponse(BasicDeliverBody.class);

            BasicDeliverBody delivery = interaction.getLatestResponse(BasicDeliverBody.class);
            interaction.consumeResponse(ContentHeaderBody.class)
                       .consumeResponse(ContentBody.class);

            assertThat(delivery.getRedelivered(), is(equalTo(false)));

            assertThat(getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(1)));

            interaction.tx().rollback().consumeResponse(TxRollbackOkBody.class);

            assertThat(getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(1)));

            // TODO: the server currently resends the messages again, which is not specification compliant but the
            // Qpid JMS AMQP 0-x relies on this behaviour.
            BasicDeliverBody redelivery = interaction.consumeResponse().getLatestResponse(BasicDeliverBody.class);
            interaction.consumeResponse(ContentHeaderBody.class)
                       .consumeResponse(ContentBody.class);

            assertThat(redelivery.getRedelivered(), is(equalTo(true)));

            interaction.channel().close()
                       .consumeResponse(ChannelCloseOkBody.class);
        }
    }

    /** Qpid specific extension */
    @Test
    public void publishUnrouteableMandatoryMessageCloseWhenNoRoute() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();

            Map<String, Object> clientProperties = Collections.singletonMap(ConnectionStartProperties.QPID_CLOSE_WHEN_NO_ROUTE, true);
            ConnectionCloseBody response = interaction.connection().startOkClientProperties(clientProperties)
                                                      .interaction().negotiateOpen()
                                                      .channel().open().consumeResponse(ChannelOpenOkBody.class)
                                                      .tx()
                                                      .select()
                                                      .consumeResponse(TxSelectOkBody.class)
                                                      .basic()
                                                      .publishExchange("")
                                                      .publishRoutingKey("unrouteable")
                                                      .publishMandatory(true)
                                                      .content("Test")
                                                      .publishMessage()
                                                      .consumeResponse().getLatestResponse(ConnectionCloseBody.class);

            assertThat(response.getReplyCode(), is(equalTo(ErrorCodes.NO_ROUTE)));
        }
    }

    /** Qpid specific extension */
    @Test
    public void publishUnrouteableMandatory() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();

            Map<String, Object> clientProperties = Collections.singletonMap(ConnectionStartProperties.QPID_CLOSE_WHEN_NO_ROUTE, false);
            final String messageContent = "Test";
            BasicReturnBody returned = interaction.connection().startOkClientProperties(clientProperties)
                                                  .interaction().negotiateOpen()
                                                  .channel()
                                                  .open()
                                                  .consumeResponse(ChannelOpenOkBody.class)
                                                  .tx()
                                                  .select()
                                                  .consumeResponse(TxSelectOkBody.class)
                                                  .basic()
                                                  .publishExchange("")
                                                  .publishRoutingKey("unrouteable")
                                                  .publishMandatory(true)
                                                  .content(messageContent)
                                                  .publishMessage()
                                                  .tx().commit()
                                                  .consumeResponse()
                                                  .getLatestResponse(BasicReturnBody.class);
            assertThat(returned.getReplyCode(), is(equalTo(ErrorCodes.NO_ROUTE)));

            interaction.consumeResponse(ContentHeaderBody.class)
                       .consumeResponse(ContentBody.class)
                       .consumeResponse(TxCommitOkBody.class)
                       .channel().close().consumeResponse(ChannelCloseOkBody.class);
        }
    }
}
