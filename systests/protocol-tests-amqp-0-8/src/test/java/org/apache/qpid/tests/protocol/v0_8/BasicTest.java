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
import static org.junit.Assume.assumeThat;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.exchange.ExchangeDefaults;
import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.transport.BasicConsumeOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.BasicDeliverBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicGetEmptyBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicGetOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicQosOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicReturnBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelCloseOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelFlowOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelOpenOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeDeclareOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueDeclareOkBody;
import org.apache.qpid.tests.protocol.SpecificationTest;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;

public class BasicTest extends BrokerAdminUsingTestBase
{

    @Before
    public void setUp()
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
    }

    @Test
    @SpecificationTest(section = "1.8.3.7", description = "publish a message")
    public void publishMessage() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
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
    @SpecificationTest(section = "1.8.3.7", description = "indicate mandatory routing")
    public void publishMandatoryMessage() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .channel().open().consumeResponse(ChannelOpenOkBody.class)
                       .basic().publishExchange("")
                               .publishRoutingKey(BrokerAdmin.TEST_QUEUE_NAME)
                               .publishMandatory(true)
                               .content("Test")
                               .publishMessage()
                       .channel().close()
                       .consumeResponse(ChannelCloseOkBody.class);

            assertThat(getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(1)));
        }
    }

    @Test
    @SpecificationTest(section = "1.8.3.7",
            description = "This flag [mandatory] tells the server how to react if the message cannot be routed to a "
                          + "queue. If this flag is set, the server will return an unroutable message with a "
                          + "Return method.")
    public void publishUnrouteableMandatoryMessage() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            String messageContent = "Test";
            BasicReturnBody returned = interaction.negotiateOpen()
                                                  .channel().open().consumeResponse(ChannelOpenOkBody.class)
                                                  .basic().publishExchange("")
                                                  .publishRoutingKey("unrouteable")
                                                  .publishMandatory(true)
                                                  .content(messageContent)
                                                  .publishMessage()
                                                  .consumeResponse().getLatestResponse(BasicReturnBody.class);
            assertThat(returned.getReplyCode(), is(equalTo(ErrorCodes.NO_ROUTE)));

            ContentBody content = interaction.consumeResponse(ContentHeaderBody.class)
                                             .consumeResponse().getLatestResponse(ContentBody.class);

            assertThat(getContent(content), is(equalTo(messageContent)));

            interaction.channel().close().consumeResponse(ChannelCloseOkBody.class);
        }
    }

    @Test
    @SpecificationTest(section = "1.8.3.7",
            description = "This flag [mandatory] tells the server how to react if the message cannot be routed to a "
                          + "queue. If this flag is zero, the server silently drops the message.")
    public void publishUnrouteableMessage() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .channel().open().consumeResponse(ChannelOpenOkBody.class)
                       .basic().publishExchange("")
                       .publishRoutingKey("unrouteable")
                       .publishMandatory(false)
                       .content("Test")
                       .publishMessage()
                       .channel().close().consumeResponse(ChannelCloseOkBody.class);
        }
    }

    @Test
    @SpecificationTest(section = "1.8",
            description = "The server SHOULD respect the persistent property of basic messages and SHOULD make a best "
                          + "effort to hold persistent basic messages on a reliable storage mechanism.")
    public void messagePersistence() throws Exception
    {
        String queueName = "durableQueue";
        String messageContent = "Test";
        String messageContentType = "text/plain";
        byte deliveryMode = (byte) 2;
        Map<String, Object> messageHeaders = Collections.singletonMap("test", "testValue");

        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .channel().open().consumeResponse(ChannelOpenOkBody.class)
                       .queue().declareName(queueName).declareDurable(true).declare()
                       .consumeResponse(QueueDeclareOkBody.class)
                       .basic().contentHeaderPropertiesContentType(messageContentType)
                       .contentHeaderPropertiesHeaders(messageHeaders)
                       .contentHeaderPropertiesDeliveryMode(deliveryMode)
                       .publishExchange("")
                       .publishRoutingKey(queueName)
                       .content(messageContent)
                       .publishMessage()
                       .channel().close()
                       .consumeResponse(ChannelCloseOkBody.class);

            assertThat(getBrokerAdmin().getQueueDepthMessages(queueName), is(equalTo(1)));
        }

        assumeThat(getBrokerAdmin().supportsRestart(), Matchers.is(true));
        getBrokerAdmin().restart();
        assertThat(getBrokerAdmin().getQueueDepthMessages(queueName), is(equalTo(1)));

        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();

            interaction.negotiateOpen()
                       .channel().open()
                       .consumeResponse(ChannelOpenOkBody.class)
                       .basic().getQueueName(queueName).getNoAck(true).get()
                       .consumeResponse(BasicGetOkBody.class);

            ContentHeaderBody header = interaction.consumeResponse().getLatestResponse(ContentHeaderBody.class);
            ContentBody content = interaction.consumeResponse().getLatestResponse(ContentBody.class);

            String receivedContent = getContent(content);
            BasicContentHeaderProperties properties = header.getProperties();

            assertThat(receivedContent, is(equalTo(messageContent)));

            Map<String, Object> receivedHeaders = new HashMap<>(properties.getHeadersAsMap());
            assertThat(receivedHeaders, is(equalTo(new HashMap<>(messageHeaders))));
            assertThat(properties.getContentTypeAsString(), is(equalTo(messageContentType)));
            assertThat(properties.getDeliveryMode(), is(equalTo(deliveryMode)));

            interaction.channel().close().consumeResponse(ChannelCloseOkBody.class);

            assertThat(getBrokerAdmin().getQueueDepthMessages(queueName), is(equalTo(0)));
        }
    }

    @Test
    @SpecificationTest(section = "1.8.3.3", description = "start a queue consumer")
    public void consumeMessage() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            String messageContent = "Test";
            String consumerTag = "A";
            String queueName = BrokerAdmin.TEST_QUEUE_NAME;
            Map<String, Object> messageHeaders = Collections.singletonMap("test", "testValue");
            String messageContentType = "text/plain";
            byte deliveryMode = (byte) 1;
            byte priority = (byte) 2;
            interaction.negotiateOpen()
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
            Map<String, Object> receivedHeaders = new HashMap<>(properties.getHeadersAsMap());
            assertThat(receivedHeaders, is(equalTo(new HashMap<>(messageHeaders))));
            assertThat(properties.getContentTypeAsString(), is(equalTo(messageContentType)));
            assertThat(properties.getPriority(), is(equalTo(priority)));
            assertThat(properties.getDeliveryMode(), is(equalTo(deliveryMode)));

            interaction.consumeResponse(ContentBody.class);

            String receivedContent = interaction.getLatestResponseContentBodyAsString();

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
        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            String consumerTag = "A";
            final long deliveryTag = 12345L;
            String queueName = BrokerAdmin.TEST_QUEUE_NAME;
            interaction.negotiateOpen()
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

        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            BasicGetOkBody response = interaction.negotiateOpen()
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

        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
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
        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .channel().open()
                       .consumeResponse(ChannelOpenOkBody.class)
                       .basic().getQueueName(BrokerAdmin.TEST_QUEUE_NAME).get()
                       .consumeResponse().getLatestResponse(BasicGetEmptyBody.class);
        }
    }

    @Test
    @SpecificationTest(section = "1.8.3.3",
            description = "This field specifies the prefetch window size in octets. The server will send a message "
                          + "in advance if it is equal to or smaller in size than the available prefetch size (and "
                          + "also falls into other prefetch limits).")
    public void qosBytesPrefetch() throws Exception
    {
        String messageContent1 = String.join("", Collections.nCopies(128, "1"));
        String messageContent2 = String.join("", Collections.nCopies(128, "2"));
        String messageContent3 = String.join("", Collections.nCopies(256, "3"));

        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            String consumerTag = "A";

            interaction.negotiateOpen()
                       .channel().open()
                       .consumeResponse(ChannelOpenOkBody.class)
                       .basic()
                       .publishExchange("").publishRoutingKey(BrokerAdmin.TEST_QUEUE_NAME)
                       .content(messageContent1).publishMessage()
                       .basic()
                       .content(messageContent2).publishMessage()
                       .basic()
                       .content(messageContent3).publishMessage()
                       .channel().flow(true)
                       .consumeResponse(ChannelFlowOkBody.class)
                       .basic().qosPrefetchSize(256)
                       .qos()
                       .consumeResponse(BasicQosOkBody.class)
                       .basic().consumeConsumerTag(consumerTag)
                       .consumeQueue(BrokerAdmin.TEST_QUEUE_NAME)
                       .consume()
                       .consumeResponse(BasicConsumeOkBody.class);

            BasicDeliverBody delivery1 = interaction.consumeResponse()
                                                    .getLatestResponse(BasicDeliverBody.class);
            ContentBody content1 = interaction.consumeResponse(ContentHeaderBody.class)
                                              .consumeResponse().getLatestResponse(ContentBody.class);

            BasicDeliverBody delivery2 = interaction.consumeResponse()
                                                    .getLatestResponse(BasicDeliverBody.class);
            ContentBody content2 = interaction.consumeResponse(ContentHeaderBody.class)
                                              .consumeResponse().getLatestResponse(ContentBody.class);

            assertThat(getContent(content1), is(equalTo(messageContent1)));
            assertThat(getContent(content2), is(equalTo(messageContent2)));

            ensureSync(interaction);

            // Ack first.  There will be insufficient bytes credit for the third to be sent.
            interaction.basic().ackDeliveryTag(delivery1.getDeliveryTag()).ack();
            ensureSync(interaction);

            // Ack second.  Now there will be sufficient bytes credit so expect the third.
            interaction.basic().ackDeliveryTag(delivery2.getDeliveryTag()).ack();

            BasicDeliverBody delivery3 = interaction.consumeResponse()
                                                    .getLatestResponse(BasicDeliverBody.class);
            ContentBody content3 = interaction.consumeResponse(ContentHeaderBody.class)
                                              .consumeResponse().getLatestResponse(ContentBody.class);

            assertThat(getContent(content3), is(equalTo(messageContent3)));

            interaction.basic().ackDeliveryTag(delivery3.getDeliveryTag()).ack()
                       .channel().close().consumeResponse(ChannelCloseOkBody.class);
        }
    }

    @Test
    @SpecificationTest(section = "1.8.3.3",
            description = "The server MUST ignore this setting when the client is not processing any messages - i.e. "
                          + "the prefetch size does not limit the transfer of single messages to a client, only "
                          + "the sending in advance of more messages while the client still has one or more "
                          + "unacknowledged messages.")
    public void qosBytesSizeQosDoesNotPreventFirstMessage() throws Exception
    {
        String messageContent = String.join("", Collections.nCopies(1024, "*"));
        getBrokerAdmin().putMessageOnQueue(BrokerAdmin.TEST_QUEUE_NAME, messageContent);

        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            String consumerTag = "A";

            interaction.negotiateOpen()
                       .channel().open()
                       .consumeResponse(ChannelOpenOkBody.class)
                       .channel().flow(true)
                       .consumeResponse(ChannelFlowOkBody.class)
                       .basic().qosPrefetchSize(512)
                       .qos()
                       .consumeResponse(BasicQosOkBody.class)
                       .basic().consumeConsumerTag(consumerTag)
                       .consumeQueue(BrokerAdmin.TEST_QUEUE_NAME)
                       .consume()
                       .consumeResponse(BasicConsumeOkBody.class)
                       .consumeResponse(BasicDeliverBody.class);

            BasicDeliverBody delivery = interaction.getLatestResponse(BasicDeliverBody.class);

            ContentHeaderBody header = interaction.consumeResponse()
                                                  .getLatestResponse(ContentHeaderBody.class);

            assertThat(header.getBodySize(), is(equalTo((long)messageContent.length())));

            ContentBody content = interaction.consumeResponse()
                                             .getLatestResponse(ContentBody.class);

            String receivedContent = getContent(content);

            assertThat(receivedContent, is(equalTo(messageContent)));

            interaction.basic().ackDeliveryTag(delivery.getDeliveryTag())
                       .ack()
                       .channel().close().consumeResponse(ChannelCloseOkBody.class);

            assertThat(getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(0)));
        }
    }

    /**
     * The Qpid JMS AMQP 0-x client relies on being able to raise and lower qos count during a channels lifetime
     * to prevent channel starvation. This test supports this qos use-case.
     */
    @Test
    public void qosCountResized() throws Exception
    {
        getBrokerAdmin().putMessageOnQueue(BrokerAdmin.TEST_QUEUE_NAME, "A", "B", "C", "D", "E", "F");

        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            String consumerTag = "A";

            interaction.negotiateOpen()
                       .channel().open()
                       .consumeResponse(ChannelOpenOkBody.class)
                       .channel().flow(true)
                       .consumeResponse(ChannelFlowOkBody.class)
                       .basic().qosPrefetchCount(3)
                       .qos()
                       .consumeResponse(BasicQosOkBody.class)
                       .basic().consumeConsumerTag(consumerTag)
                       .consumeQueue(BrokerAdmin.TEST_QUEUE_NAME)
                       .consume()
                       .consumeResponse(BasicConsumeOkBody.class);

            final long deliveryTagA = receiveDeliveryHeaderAndBody(interaction, "A");
            receiveDeliveryHeaderAndBody(interaction, "B");
            final long deliveryTagC = receiveDeliveryHeaderAndBody(interaction, "C");

            ensureSync(interaction);

            // Raise qos count by one, expect D to arrive
            interaction.basic().qosPrefetchCount(4).qos()
                       .consumeResponse(BasicQosOkBody.class);

            long deliveryTagD = receiveDeliveryHeaderAndBody(interaction, "D");
            ensureSync(interaction);

            // Ack A, expect E to arrive
            interaction.basic().ackDeliveryTag(deliveryTagA).ack();

            receiveDeliveryHeaderAndBody(interaction, "E");
            ensureSync(interaction);

            // Lower qos back to 2 and ensure no more messages arrive (message credit will be negative at this point).
            interaction.basic().qosPrefetchCount(2).qos()
                       .consumeResponse(BasicQosOkBody.class);
            ensureSync(interaction);

            // Ack B and C and ensure still no more messages arrive (message credit will now be zero)
            interaction.basic()
                       .ackMultiple(true).ackDeliveryTag(deliveryTagC).ack();
            ensureSync(interaction);

            // Ack D and ensure F delivery arrives
            interaction.basic()
                       .ackMultiple(false).ackDeliveryTag(deliveryTagD).ack();

            receiveDeliveryHeaderAndBody(interaction, "F");

            interaction.channel().close().consumeResponse(ChannelCloseOkBody.class);

            assertThat(getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(2)));
        }
    }

    /**
     * The Qpid JMS AMQP 0-x client is capable of polling fors message.  It does this using a combination of
     * basic.qos (count one) and regulating the flow using channel.flow. This test supports this use-case.
     */
    @Test
    public void pollingUsingFlow() throws Exception
    {
        getBrokerAdmin().putMessageOnQueue(BrokerAdmin.TEST_QUEUE_NAME, "A", "B", "C");

        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            String consumerTag = "A";

            interaction.negotiateOpen()
                       .channel().open()
                       .consumeResponse(ChannelOpenOkBody.class)
                       .basic().qosPrefetchCount(1)
                       .qos()
                       .consumeResponse(BasicQosOkBody.class)
                       .channel().flow(false)
                       .consumeResponse(ChannelFlowOkBody.class)
                       .basic().consumeConsumerTag(consumerTag)
                       .consumeQueue(BrokerAdmin.TEST_QUEUE_NAME)
                       .consume()
                       .consumeResponse(BasicConsumeOkBody.class);

            ensureSync(interaction);

            interaction.channel().flow(true)
                       .consumeResponse(ChannelFlowOkBody.class);

            long deliveryTagA = receiveDeliveryHeaderAndBody(interaction, "A");

            interaction.channel().flow(false)
                       .consumeResponse(ChannelFlowOkBody.class)
                       .basic().ackDeliveryTag(deliveryTagA).ack();

            ensureSync(interaction);

            interaction.channel().flow(true)
                       .consumeResponse(ChannelFlowOkBody.class);

            long deliveryTagB = receiveDeliveryHeaderAndBody(interaction, "B");

            interaction.channel().flow(false)
                       .consumeResponse(ChannelFlowOkBody.class)
                       .basic().ackDeliveryTag(deliveryTagB).ack()
                       .channel().close().consumeResponse(ChannelCloseOkBody.class);

            assertThat(getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(1)));
        }
    }

    private long receiveDeliveryHeaderAndBody(final Interaction interaction, String expectedMessageContent) throws Exception
    {
        BasicDeliverBody delivery = interaction.consumeResponse().getLatestResponse(BasicDeliverBody.class);
        ContentBody content = interaction.consumeResponse(ContentHeaderBody.class)
                                         .consumeResponse().getLatestResponse(ContentBody.class);

        assertThat(getContent(content), is(equalTo(expectedMessageContent)));
        return delivery.getDeliveryTag();
    }

    private void ensureSync(final Interaction interaction) throws Exception
    {
        interaction.exchange()
                   .declarePassive(true).declareName(ExchangeDefaults.DIRECT_EXCHANGE_NAME).declare()
                   .consumeResponse(ExchangeDeclareOkBody.class);
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
