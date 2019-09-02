/*
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
package org.apache.qpid.tests.protocol.v0_8.extension.transactiontimeout;

import static org.apache.qpid.tests.utils.BrokerAdmin.KIND_BROKER_J;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.protocol.v0_8.transport.BasicConsumeOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicDeliverBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicQosOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelFlowOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelOpenOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionCloseBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeDeclareOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.TxSelectOkBody;
import org.apache.qpid.tests.protocol.v0_8.FrameTransport;
import org.apache.qpid.tests.protocol.v0_8.Interaction;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;
import org.apache.qpid.tests.utils.BrokerSpecific;
import org.apache.qpid.tests.utils.ConfigItem;

@BrokerSpecific(kind = KIND_BROKER_J)
@ConfigItem(name = "virtualhost.storeTransactionOpenTimeoutClose", value = "1000")
public class TransactionTimeoutTest extends BrokerAdminUsingTestBase
{

    @Before
    public void setUp()
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
    }

    @Test
    public void publishTransactionTimeout() throws Exception
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

            final ConnectionCloseBody close = interaction.consumeResponse().getLatestResponse(ConnectionCloseBody.class);
            assertThat(close.getReplyCode(), is(equalTo(ErrorCodes.RESOURCE_ERROR)));
            assertThat(close.getReplyText().toString(), containsString("transaction timed out"));
            interaction.connection().closeOk();

            assertThat(getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(0)));
        }
    }

    @Test
    public void consumeTransactionTimeout() throws Exception
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

            final ConnectionCloseBody close = interaction.consumeResponse().getLatestResponse(ConnectionCloseBody.class);
            assertThat(close.getReplyCode(), is(equalTo(ErrorCodes.RESOURCE_ERROR)));
            assertThat(close.getReplyText().toString(), containsString("transaction timed out"));
            interaction.connection().closeOk();

            assertThat(getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(1)));
        }
    }

}
