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
package org.apache.qpid.tests.protocol.v0_8.extension.basic;

import static org.apache.qpid.tests.utils.BrokerAdmin.KIND_BROKER_J;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assume.assumeThat;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.exchange.ExchangeDefaults;
import org.apache.qpid.server.filter.AMQPFilterTypes;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.AMQType;
import org.apache.qpid.server.protocol.v0_8.EncodingUtils;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.FieldTableFactory;
import org.apache.qpid.server.protocol.v0_8.transport.BasicConsumeOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.BasicDeliverBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicQosOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelCloseOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelFlowOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelOpenOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueBindOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.TxSelectOkBody;
import org.apache.qpid.tests.protocol.ChannelClosedResponse;
import org.apache.qpid.tests.protocol.v0_8.FrameTransport;
import org.apache.qpid.tests.protocol.v0_8.Interaction;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;
import org.apache.qpid.tests.utils.BrokerSpecific;
import org.apache.qpid.tests.utils.ConfigItem;

@BrokerSpecific(kind = KIND_BROKER_J)
@ConfigItem(name = "broker.flowToDiskThreshold", value = "1")
@ConfigItem(name = "connection.maxUncommittedInMemorySize", value = "1")
public class MalformedMessageTest extends BrokerAdminUsingTestBase
{
    private static final String CONTENT_TEXT = "Test";

    @Before
    public void setUp()
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
    }

    @Test
    public void malformedHeaderValue() throws Exception
    {
        final FieldTable malformedHeader = createHeadersWithMalformedLongString();
        byte[] contentBytes = CONTENT_TEXT.getBytes(StandardCharsets.UTF_8);
        publishMalformedMessage(malformedHeader, contentBytes);
        assertThat(getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(0)));
    }

    @Test
    public void malformedHeader() throws Exception
    {
        final FieldTable malformedHeader = createMalformedHeaders();
        byte[] contentBytes = CONTENT_TEXT.getBytes(StandardCharsets.UTF_8);
        publishMalformedMessage(malformedHeader, contentBytes);
        assertThat(getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(0)));
    }

    @Test
    public void publishMalformedMessageToQueueBoundWithSelector() throws Exception
    {
        final FieldTable malformedHeader = createMalformedHeadersWithMissingValue("prop");
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        basicContentHeaderProperties.setHeaders(malformedHeader);
        basicContentHeaderProperties.setContentType("text/plain");
        basicContentHeaderProperties.setDeliveryMode(BasicContentHeaderProperties.PERSISTENT);
        byte[] contentBytes = CONTENT_TEXT.getBytes(StandardCharsets.UTF_8);

        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .channel().open().consumeResponse(ChannelOpenOkBody.class)
                       .queue()
                       .bindName(ExchangeDefaults.TOPIC_EXCHANGE_NAME)
                       .bindRoutingKey(BrokerAdmin.TEST_QUEUE_NAME)
                       .bindQueueName(BrokerAdmin.TEST_QUEUE_NAME)
                       .bindArguments(Collections.singletonMap(AMQPFilterTypes.JMS_SELECTOR.getValue(), "prop = 1"))
                       .bind()
                       .consumeResponse(QueueBindOkBody.class)
                       .basic().publishExchange(ExchangeDefaults.TOPIC_EXCHANGE_NAME)
                               .publishRoutingKey(BrokerAdmin.TEST_QUEUE_NAME)
                               .contentHeaderPropertiesHeaders(malformedHeader)
                               .content(contentBytes)
                               .publishMessage()
                       .consumeResponse()
                       .getLatestResponse(ChannelClosedResponse.class);

            assertThat(getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(0)));
        }
    }

    @Test
    public void publishMalformedMessageInTransactionExceedingMaxUncommittedLimit() throws Exception
    {
        assumeThat("Persistent store is required for the test",
                   getBrokerAdmin().supportsRestart(),
                   is(equalTo(true)));

        final FieldTable malformedHeader = createMalformedHeadersWithMissingValue("prop");
        final BasicContentHeaderProperties basicContentHeaderProperties = new BasicContentHeaderProperties();
        basicContentHeaderProperties.setHeaders(malformedHeader);
        basicContentHeaderProperties.setContentType("text/plain");
        basicContentHeaderProperties.setDeliveryMode(BasicContentHeaderProperties.PERSISTENT);
        byte[] contentBytes = CONTENT_TEXT.getBytes(StandardCharsets.UTF_8);

        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .channel().open().consumeResponse(ChannelOpenOkBody.class)
                       .tx().select()
                       .consumeResponse(TxSelectOkBody.class)
                       .basic().publishExchange("")
                       .publishRoutingKey(BrokerAdmin.TEST_QUEUE_NAME)
                       .contentHeaderPropertiesHeaders(malformedHeader)
                       .content(contentBytes)
                       .publishMessage()
                       .tx().commit()
                       .consumeResponse()
                       .getLatestResponse(ChannelClosedResponse.class);

            assertThat(getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(0)));
        }
    }

    @Test
    public void consumeMalformedMessage() throws Exception
    {
        final FieldTable malformedHeader = createHeadersWithMalformedLongString();
        final byte[] contentBytes = CONTENT_TEXT.getBytes(StandardCharsets.UTF_8);

        final String content2 = "message2";
        final byte[] content2Bytes = content2.getBytes(StandardCharsets.UTF_8);

        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            String consumerTag = "A";
            interaction.negotiateOpen()
                       .channel().open()
                       .consumeResponse(ChannelOpenOkBody.class)
                       .basic().qosPrefetchCount(1)
                       .qos()
                       .consumeResponse(BasicQosOkBody.class)
                       .basic().consumeConsumerTag(consumerTag)
                       .consumeQueue(BrokerAdmin.TEST_QUEUE_NAME)
                       .consume()
                       .consumeResponse(BasicConsumeOkBody.class)
                       .channel().flow(true)
                       .consumeResponse(ChannelFlowOkBody.class)

                       .basic().publishExchange("")
                       .publishRoutingKey(BrokerAdmin.TEST_QUEUE_NAME)
                       .contentHeaderPropertiesHeaders(malformedHeader)
                       .content(contentBytes)
                       .publishMessage()

                       .basic().publishExchange("")
                       .publishRoutingKey(BrokerAdmin.TEST_QUEUE_NAME)
                       .contentHeaderPropertiesContentType("text/plain")
                       .contentHeaderPropertiesHeaders(Collections.emptyMap())
                       .content(content2Bytes)
                       .publishMessage();

            BasicDeliverBody delivery = interaction.consumeResponse(BasicDeliverBody.class)
                                                   .getLatestResponse(BasicDeliverBody.class);
            assertThat(delivery.getConsumerTag(), is(equalTo(AMQShortString.valueOf(consumerTag))));
            assertThat(delivery.getConsumerTag(), is(notNullValue()));
            assertThat(delivery.getRedelivered(), is(equalTo(false)));
            assertThat(delivery.getExchange(), is(nullValue()));
            assertThat(delivery.getRoutingKey(), is(equalTo(AMQShortString.valueOf(BrokerAdmin.TEST_QUEUE_NAME))));

            ContentHeaderBody header =
                    interaction.consumeResponse(ContentHeaderBody.class).getLatestResponse(ContentHeaderBody.class);

            assertThat(header.getBodySize(), is(equalTo((long) content2Bytes.length)));
            BasicContentHeaderProperties properties = header.getProperties();
            Map<String, Object> receivedHeaders = new HashMap<>(properties.getHeadersAsMap());
            assertThat(receivedHeaders.isEmpty(), is(equalTo(true)));

            String receivedContent =
                    interaction.consumeResponse(ContentBody.class).getLatestResponseContentBodyAsString();

            assertThat(receivedContent, is(equalTo(content2)));

            interaction.channel().close()
                       .consumeResponse(ChannelCloseOkBody.class, ChannelFlowOkBody.class);
        }
    }

    private void publishMalformedMessage(final FieldTable malformedHeader, final byte[] contentBytes) throws Exception
    {

        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .channel().open().consumeResponse(ChannelOpenOkBody.class)
                       .basic().publishExchange("")
                               .publishRoutingKey(BrokerAdmin.TEST_QUEUE_NAME)
                               .contentHeaderPropertiesHeaders(malformedHeader)
                               .content(contentBytes)
                               .publishMessage()
                       .channel().close()
                       .consumeResponse(ChannelCloseOkBody.class);
        }
    }

    private static FieldTable createMalformedHeaders()
    {

        final QpidByteBuffer buf = QpidByteBuffer.allocate(1);
        buf.put((byte) -1);

        buf.flip();

        return FieldTableFactory.createFieldTable(buf);
    }

    private FieldTable createHeadersWithMalformedLongString()
    {
        // korean (each character occupies 3 bytes)
        final byte[] valueBytes = {(byte) 0xED, (byte) 0x95, (byte) 0x9C,
                (byte) 0xEA, (byte) 0xB5, (byte) 0xAD,
                (byte) 0xEC, (byte) 0x96, (byte) 0xB4};
        final String value = new String(valueBytes, StandardCharsets.UTF_8);

        final String key = "test";
        final QpidByteBuffer buf = QpidByteBuffer.allocate(EncodingUtils.encodedShortStringLength(key)
                                                           + Byte.BYTES + Integer.BYTES + value.length());

        // write key
        EncodingUtils.writeShortStringBytes(buf, key);

        // write value as long string with incorrectly encoded characters
        buf.put(AMQType.LONG_STRING.identifier());
        buf.putUnsignedInt(value.length());
        value.chars().forEach(c -> buf.put((byte) c));

        buf.flip();

        return FieldTableFactory.createFieldTable(buf);
    }

    private FieldTable createMalformedHeadersWithMissingValue(String key)
    {
        final QpidByteBuffer buf = QpidByteBuffer.allocate(EncodingUtils.encodedShortStringLength(key));

        // write key
        EncodingUtils.writeShortStringBytes(buf, key);

        buf.flip();

        return FieldTableFactory.createFieldTable(buf);
    }

}
