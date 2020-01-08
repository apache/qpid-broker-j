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
package org.apache.qpid.tests.protocol.v0_8.extension.queue;

import static org.apache.qpid.tests.utils.BrokerAdmin.KIND_BROKER_J;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.transport.BasicConsumeOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.BasicDeliverBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicQosOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelCloseOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelFlowOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelOpenOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionCloseBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueDeclareOkBody;
import org.apache.qpid.tests.protocol.v0_8.FrameTransport;
import org.apache.qpid.tests.protocol.v0_8.Interaction;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;
import org.apache.qpid.tests.utils.BrokerSpecific;

@BrokerSpecific(kind = KIND_BROKER_J)
public class QueueTest extends BrokerAdminUsingTestBase
{

    @Test
    public void queueDeclareUsingRealQueueAttributesInWireArguments() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            QueueDeclareOkBody response = interaction.negotiateOpen()
                                                     .channel().open().consumeResponse(ChannelOpenOkBody.class)
                                                     .queue().declareName(BrokerAdmin.TEST_QUEUE_NAME)
                                                     .declareArguments(Collections.singletonMap("defaultFilters",
                                                                                                "{\"selector\":{\"x-filter-jms-selector\":[\"id=2\"]}}"))
                                                     .declare()
                                                     .consumeResponse().getLatestResponse(QueueDeclareOkBody.class);

            assertThat(response.getQueue(), is(equalTo(AMQShortString.valueOf(BrokerAdmin.TEST_QUEUE_NAME))));
            assertThat(response.getMessageCount(), is(equalTo(0L)));
            assertThat(response.getConsumerCount(), is(equalTo(0L)));

            // make sure that wire arguments took effect
            // by publishing messages and consuming message matching filter

            String consumerTag = "test";
            interaction.basic().qosPrefetchCount(2)
                       .qos()
                       .consumeResponse(BasicQosOkBody.class)
                       .basic().consumeConsumerTag(consumerTag)
                       .consumeQueue(BrokerAdmin.TEST_QUEUE_NAME)
                       .consume()
                       .consumeResponse(BasicConsumeOkBody.class)
                       .channel().flow(true)
                       .consumeResponse(ChannelFlowOkBody.class);

            String content2 = "Test Content 2";
            Map<String, Object> messageHeaders2 = Collections.singletonMap("id", 2);
            String contentType = "text/plain";

            // first message is not matching queue default filter
            interaction.basic().publishExchange("")
                       .publishRoutingKey(BrokerAdmin.TEST_QUEUE_NAME)
                       .publishMandatory(true)
                       .contentHeaderPropertiesContentType(contentType)
                       .contentHeaderPropertiesHeaders(Collections.singletonMap("id", 1))
                       .content("Test1")
                       .publishMessage()

                       // second message is matching queue default filter
                       .basic().publishExchange("")
                       .publishRoutingKey(BrokerAdmin.TEST_QUEUE_NAME)
                       .publishMandatory(true)
                       .contentHeaderPropertiesContentType(contentType)
                       .contentHeaderPropertiesHeaders(messageHeaders2)
                       .content(content2)
                       .publishMessage();

            // second message should be received
            BasicDeliverBody delivery =
                    interaction.consumeResponse(BasicDeliverBody.class).getLatestResponse(BasicDeliverBody.class);

            assertThat(delivery.getConsumerTag(), is(equalTo(AMQShortString.valueOf(consumerTag))));
            assertThat(delivery.getConsumerTag(), is(notNullValue()));
            assertThat(delivery.getRedelivered(), is(equalTo(false)));
            assertThat(delivery.getExchange(), is(nullValue()));
            assertThat(delivery.getRoutingKey(), is(equalTo(AMQShortString.valueOf(BrokerAdmin.TEST_QUEUE_NAME))));

            ContentHeaderBody header =
                    interaction.consumeResponse(ContentHeaderBody.class).getLatestResponse(ContentHeaderBody.class);

            assertThat(header.getBodySize(), is(equalTo((long) content2.length())));
            BasicContentHeaderProperties properties = header.getProperties();
            Map<String, Object> receivedHeaders = new HashMap<>(properties.getHeadersAsMap());
            assertThat(receivedHeaders, is(equalTo(new HashMap<>(messageHeaders2))));
            assertThat(properties.getContentTypeAsString(), is(equalTo(contentType)));

            ContentBody content = interaction.consumeResponse(ContentBody.class).getLatestResponse(ContentBody.class);

            QpidByteBuffer payload = content.getPayload();
            byte[] contentData = new byte[payload.remaining()];
            payload.get(contentData);
            payload.dispose();
            String receivedContent = new String(contentData, StandardCharsets.UTF_8);

            assertThat(receivedContent, is(equalTo(receivedContent)));
            assertThat(getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(2)));

            interaction.basic().ackDeliveryTag(delivery.getDeliveryTag())
                       .ack()
                       .channel().close().consumeResponse(ChannelCloseOkBody.class);
            assertThat(getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(1)));
        }
    }

    @Test
    public void queueDeclareInvalidWireArguments() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            ConnectionCloseBody response = interaction.negotiateOpen()
                                                      .channel().open().consumeResponse(ChannelOpenOkBody.class)
                                                      .queue().declareName(BrokerAdmin.TEST_QUEUE_NAME)
                                                      .declareArguments(Collections.singletonMap("foo", "bar"))
                                                      .declare()
                                                      .consumeResponse().getLatestResponse(ConnectionCloseBody.class);

            assertThat(response.getReplyCode(), is(equalTo(ErrorCodes.INVALID_ARGUMENT)));
        }
    }
}
