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
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.transport.BasicConsumeOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.BasicDeliverBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicGetEmptyBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicGetOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicQosOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelCloseOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelFlowOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelOpenOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.apache.qpid.tests.protocol.SpecificationTest;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;

public class BasicTest extends BrokerAdminUsingTestBase
{
    private InetSocketAddress _brokerAddress;

    @Before
    public void setUp()
    {
        _brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
    }

    @Test
    @SpecificationTest(section = "1.8.3.7", description = "publish a message")
    public void publishMessage() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.openAnonymousConnection()
                       .channel().open().consumeResponse(ChannelOpenOkBody.class)
                       .basic().contentHeaderPropertiesContentType("text/plain")
                               .contentHeaderPropertiesHeaders(Collections.singletonMap("test", "testValue"))
                               .contentHeaderPropertiesDeliveryMode((byte)1)
                               .contentHeaderPropertiesPriority((byte)1)
                               .publishExchange("")
                               .publishRoutingKey(BrokerAdmin.TEST_QUEUE_NAME)
                               .content("Test")
                               .publishMessage()
                       .channel().close()
                       .consumeResponse(ChannelCloseOkBody.class);

            assertThat(getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(1)));
        }
    }


    @Test
    @SpecificationTest(section = "1.8.3.3", description = "start a queue consumer")
    public void consumeMessage() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            String messageContent = "Test";
            String consumerTag = "A";
            String queueName = BrokerAdmin.TEST_QUEUE_NAME;
            Map<String, Object> messageHeaders = Collections.singletonMap("test", "testValue");
            String messageContentType = "text/plain";
            byte deliveryMode = (byte) 1;
            byte priority = (byte) 2;
            interaction.openAnonymousConnection()
                       .channel().open()
                       .consumeResponse(ChannelOpenOkBody.class)
                       .basic().qosPrefetchCount(1)
                               .qos()
                       .consumeResponse(BasicQosOkBody.class)
                       .basic().consumeConsumerTag(consumerTag)
                               .consumeQueue(queueName)
                               .consume()
                       .consumeResponse(BasicConsumeOkBody.class)
                       .channel().flow(true)
                       .consumeResponse(ChannelFlowOkBody.class)
                       .basic().contentHeaderPropertiesContentType(messageContentType)
                               .contentHeaderPropertiesHeaders(messageHeaders)
                               .contentHeaderPropertiesDeliveryMode(deliveryMode)
                               .contentHeaderPropertiesPriority(priority)
                               .publishExchange("")
                               .publishRoutingKey(queueName)
                               .content(messageContent)
                               .publishMessage()
                       .consumeResponse(BasicDeliverBody.class);

            BasicDeliverBody delivery = interaction.getLatestResponse(BasicDeliverBody.class);
            assertThat(delivery.getConsumerTag(), is(equalTo(AMQShortString.valueOf(consumerTag))));
            assertThat(delivery.getConsumerTag(), is(notNullValue()));
            assertThat(delivery.getRedelivered(), is(equalTo(false)));
            assertThat(delivery.getExchange(), is(nullValue()));
            assertThat(delivery.getRoutingKey(), is(equalTo(AMQShortString.valueOf(queueName))));

            ContentHeaderBody header =
                    interaction.consumeResponse(ContentHeaderBody.class).getLatestResponse(ContentHeaderBody.class);

            assertThat(header.getBodySize(), is(equalTo((long)messageContent.length())));
            BasicContentHeaderProperties properties = header.getProperties();
            Map<String, Object> receivedHeaders = new HashMap<>(FieldTable.convertToMap(properties.getHeaders()));
            assertThat(receivedHeaders, is(equalTo(new HashMap<>(messageHeaders))));
            assertThat(properties.getContentTypeAsString(), is(equalTo(messageContentType)));
            assertThat(properties.getPriority(), is(equalTo(priority)));
            assertThat(properties.getDeliveryMode(), is(equalTo(deliveryMode)));

            ContentBody content = interaction.consumeResponse(ContentBody.class).getLatestResponse(ContentBody.class);

            String receivedContent = getContent(content);

            assertThat(receivedContent, is(equalTo(messageContent)));
            assertThat(getBrokerAdmin().getQueueDepthMessages(queueName), is(equalTo(1)));

            interaction.basic().ackDeliveryTag(delivery.getDeliveryTag())
                              .ack()
                       .channel().close().consumeResponse(ChannelCloseOkBody.class);
            assertThat(getBrokerAdmin().getQueueDepthMessages(queueName), is(equalTo(0)));
        }
    }

    @Test
    @SpecificationTest(section = "1.8.3.13",
            description = "The server MUST validate that a non-zero delivery-tag refers to a delivered message,"
                          + " and raise a channel exception if this is not the case. On a transacted channel,"
                          + " this check MUST be done immediately and not delayed until a Tx.Commit. Specifically,"
                          + " a client MUST not acknowledge the same message more than once."
                          + ""
                          + "Note current broker behaviour is spec incompliant: broker ignores not valid delivery tags")
    public void ackWithInvalidDeliveryTag() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            String consumerTag = "A";
            final long deliveryTag = 12345L;
            String queueName = BrokerAdmin.TEST_QUEUE_NAME;
            interaction.openAnonymousConnection()
                       .channel().open()
                       .consumeResponse(ChannelOpenOkBody.class)
                       .basic().qosPrefetchCount(1)
                       .qos()
                       .consumeResponse(BasicQosOkBody.class)
                       .basic().consumeConsumerTag(consumerTag)
                       .consumeQueue(queueName)
                       .consume()
                       .consumeResponse(BasicConsumeOkBody.class)
                       .channel().flow(true)
                       .consumeResponse(ChannelFlowOkBody.class)
                       .basic().ackDeliveryTag(deliveryTag).ack()
                       .channel().close().consumeResponse(ChannelCloseOkBody.class);
        }
    }

    @Test
    @SpecificationTest(section = "1.8.3.10", description = "direct access to a queue")
    public void get() throws Exception
    {
        String messageContent = "message";
        getBrokerAdmin().putMessageOnQueue(BrokerAdmin.TEST_QUEUE_NAME, messageContent);

        try(FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            BasicGetOkBody response = interaction.openAnonymousConnection()
                                                 .channel().open()
                                                 .consumeResponse(ChannelOpenOkBody.class)
                                                 .basic().getQueueName(BrokerAdmin.TEST_QUEUE_NAME).get()
                                                 .consumeResponse().getLatestResponse(BasicGetOkBody.class);

            long deliveryTag = response.getDeliveryTag();
            ContentBody content = interaction.consumeResponse(ContentHeaderBody.class)
                                             .consumeResponse().getLatestResponse(ContentBody.class);

            String receivedContent = getContent(content);
            assertThat(receivedContent, is(equalTo(messageContent)));

            assertThat(getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(1)));

            interaction.basic().ackDeliveryTag(deliveryTag).ack()
                       .channel().close().consumeResponse(ChannelCloseOkBody.class);

            assertThat(getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(0)));
        }
    }

    @Test
    @SpecificationTest(section = "1.8.3.10", description = "direct access to a queue")
    public void getNoAck() throws Exception
    {
        String messageContent = "message";
        getBrokerAdmin().putMessageOnQueue(BrokerAdmin.TEST_QUEUE_NAME, messageContent);

        try(FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.openAnonymousConnection()
                       .channel().open()
                       .consumeResponse(ChannelOpenOkBody.class)
                       .basic().getQueueName(BrokerAdmin.TEST_QUEUE_NAME).getNoAck(true).get()
                       .consumeResponse(BasicGetOkBody.class);

            ContentBody content = interaction.consumeResponse(ContentHeaderBody.class)
                                             .consumeResponse().getLatestResponse(ContentBody.class);

            String receivedContent = getContent(content);
            assertThat(receivedContent, is(equalTo(messageContent)));

            interaction.channel().close().consumeResponse(ChannelCloseOkBody.class);

            assertThat(getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(0)));
        }
    }

    @Test
    @SpecificationTest(section = "1.8.3.10", description = "direct access to a queue")
    public void getEmptyQueue() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.openAnonymousConnection()
                       .channel().open()
                       .consumeResponse(ChannelOpenOkBody.class)
                       .basic().getQueueName(BrokerAdmin.TEST_QUEUE_NAME).get()
                       .consumeResponse().getLatestResponse(BasicGetEmptyBody.class);
        }
    }

    private String getContent(final ContentBody content)
    {
        QpidByteBuffer payload = content.getPayload();
        byte[] contentData = new byte[payload.remaining()];
        payload.get(contentData);
        payload.dispose();
        return new String(contentData, StandardCharsets.UTF_8);
    }
}
