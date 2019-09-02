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
package org.apache.qpid.tests.protocol.v0_8.extension.tx;

import static org.apache.qpid.tests.utils.BrokerAdmin.KIND_BROKER_J;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.protocol.v0_8.transport.BasicConsumeOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.BasicDeliverBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicQosOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelFlowOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelOpenOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeDeclareOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.TxCommitOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.TxRollbackOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.TxSelectOkBody;
import org.apache.qpid.tests.protocol.v0_8.FrameTransport;
import org.apache.qpid.tests.protocol.v0_8.Interaction;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;
import org.apache.qpid.tests.utils.BrokerSpecific;

@BrokerSpecific(kind = KIND_BROKER_J)
public class AsyncTransactionTest extends BrokerAdminUsingTestBase
{
    private static final int MESSAGE_COUNT = 10;

    @Before
    public void setUp()
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
    }

    @Test
    public void subsequentCommit() throws Exception
    {
        publishPersistentMessages();
        assertThat(getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(MESSAGE_COUNT)));
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = createConsumerInteraction(transport);

            acknowledgeDeliveries(interaction, receiveBasicDeliverBodies(interaction));
            interaction.tx().commit();

            // subsequent commit
            interaction.tx().commit();

            interaction.consumeResponse(TxCommitOkBody.class);
            interaction.consumeResponse(TxCommitOkBody.class);

            assertThat(getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(0)));
        }
    }

    @Test
    public void subsequentRollback() throws Exception
    {
        publishPersistentMessages();
        assertThat(getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(MESSAGE_COUNT)));
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = createConsumerInteraction(transport);

            acknowledgeDeliveries(interaction, receiveBasicDeliverBodies(interaction));
            interaction.tx().commit();

            // subsequent rollback
            interaction.tx().rollback();

            interaction.consumeResponse(TxCommitOkBody.class);
            interaction.consumeResponse(TxRollbackOkBody.class);

            assertThat(getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(0)));
        }
    }


    private void publishPersistentMessages() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .channel().open().consumeResponse(ChannelOpenOkBody.class);
            for (int i = 0; i < MESSAGE_COUNT; i++)
            {
                interaction.basic()
                           .publishExchange("")
                           .publishRoutingKey(BrokerAdmin.TEST_QUEUE_NAME)
                           .contentHeaderPropertiesDeliveryMode(BasicContentHeaderProperties.PERSISTENT)
                           .content("message" + 1)
                           .publishMessage();
            }
            interaction.exchange().declarePassive(true).declare().consumeResponse(ExchangeDeclareOkBody.class);
        }
    }

    private Interaction createConsumerInteraction(final FrameTransport transport)
            throws Exception
    {
        final Interaction interaction = transport.newInteraction();
        interaction.negotiateOpen()
                   .channel().open().consumeResponse(ChannelOpenOkBody.class)
                   .tx().select().consumeResponse(TxSelectOkBody.class)
                   .basic().qosPrefetchCount(MESSAGE_COUNT)
                   .qos()
                   .consumeResponse(BasicQosOkBody.class)
                   .channel().flow(true)
                   .consumeResponse(ChannelFlowOkBody.class)
                   .basic()
                   .consumeConsumerTag("A")
                   .consumeQueue(BrokerAdmin.TEST_QUEUE_NAME)
                   .consume()
                   .consumeResponse(BasicConsumeOkBody.class);
        return interaction;
    }

    private BasicDeliverBody[] receiveBasicDeliverBodies(final Interaction interaction)
            throws Exception
    {
        final BasicDeliverBody[] deliveries = new BasicDeliverBody[MESSAGE_COUNT];
        for (int i = 0; i < MESSAGE_COUNT; i++)
        {
            deliveries[i] = interaction.consumeResponse(BasicDeliverBody.class).getLatestResponse(BasicDeliverBody.class);
            interaction.consumeResponse(ContentHeaderBody.class).consumeResponse(ContentBody.class);
        }
        return deliveries;
    }

    private void acknowledgeDeliveries(final Interaction interaction, final BasicDeliverBody[] deliveries)
            throws Exception
    {
        for (final BasicDeliverBody delivery : deliveries)
        {
            interaction.basic().ackDeliveryTag(delivery.getDeliveryTag()).ack();
        }
    }

}
