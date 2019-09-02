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

import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.transport.BasicConsumeOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicDeliverBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicQosOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelFlowOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelOpenOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionOpenOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionStartBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionTuneBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;

public class LargeMessageTest extends BrokerAdminUsingTestBase
{

    @Before
    public void setUp()
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
    }

    @Test
    public void multiBodyMessage() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            String consumerTag = "A";
            String queueName = "testQueue";

            ConnectionTuneBody connTune = interaction.authenticateConnection()
                                                     .getLatestResponse(ConnectionTuneBody.class);

            byte[] messageContent = new byte[(int)connTune.getFrameMax()];
            IntStream.range(0, messageContent.length).forEach(i -> {messageContent[i] = (byte) (i & 0xFF);});

            interaction.connection().tuneOk()
                       .connection().open()
                       .consumeResponse(ConnectionOpenOkBody.class)
                       .channel().open()
                       .consumeResponse(ChannelOpenOkBody.class)
                       .basic().qosPrefetchCount(1).qos()
                       .consumeResponse(BasicQosOkBody.class)
                       .basic().consumeConsumerTag(consumerTag)
                               .consumeQueue(queueName)
                               .consumeNoAck(true)
                               .consume()
                       .consumeResponse(BasicConsumeOkBody.class)
                       .channel().flow(true)
                       .consumeResponse(ChannelFlowOkBody.class)
                       .basic().publishExchange("")
                               .publishRoutingKey(queueName)
                               .content(messageContent)
                               .publishMessage()
                       .consumeResponse(BasicDeliverBody.class);

            BasicDeliverBody delivery = interaction.getLatestResponse(BasicDeliverBody.class);
            assertThat(delivery.getConsumerTag(), is(equalTo(AMQShortString.valueOf(consumerTag))));

            ContentHeaderBody header =
                    interaction.consumeResponse(ContentHeaderBody.class).getLatestResponse(ContentHeaderBody.class);

            assertThat(header.getBodySize(), is(equalTo((long)messageContent.length)));

            byte[] receivedContent = new byte[messageContent.length];

            ContentBody content1 = interaction.consumeResponse(ContentBody.class).getLatestResponse(ContentBody.class);
            ContentBody content2 = interaction.consumeResponse(ContentBody.class).getLatestResponse(ContentBody.class);

            try (QpidByteBuffer allContent = QpidByteBuffer.concatenate(content1.getPayload(), content2.getPayload()))
            {
                allContent.get(receivedContent);
            }

            assertThat(receivedContent, is(equalTo(messageContent)));
        }
    }
}
