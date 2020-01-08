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
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.transport.BasicConsumeOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.BasicDeliverBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicQosOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelFlowOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelOpenOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionOpenOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionTuneBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;

public class LargeHeadersTest extends BrokerAdminUsingTestBase
{

    @Before
    public void setUp()
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
    }

    @Test
    /** Tests boundary case where headers exactly fill the content header frame */
    public void headersFillContentHeaderFrame() throws Exception
    {

        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            String consumerTag = "A";

            ConnectionTuneBody connTune = interaction.authenticateConnection()
                                                     .getLatestResponse(ConnectionTuneBody.class);

            final String headerName = "test";
            final int headerValueSize = (int) (connTune.getFrameMax() - calculateContentHeaderFramingOverhead(headerName));
            final String headerValue = generateLongString(headerValueSize);
            final Map<String, Object> messageHeaders = Collections.singletonMap(headerName, headerValue);

            interaction.connection().tuneOk()
                       .connection().open()
                       .consumeResponse(ConnectionOpenOkBody.class)
                       .channel().open()
                       .consumeResponse(ChannelOpenOkBody.class)
                       .basic().qosPrefetchCount(1).qos()
                       .consumeResponse(BasicQosOkBody.class)
                       .basic().consumeConsumerTag(consumerTag)
                               .consumeQueue(BrokerAdmin.TEST_QUEUE_NAME)
                               .consumeNoAck(true)
                               .consume()
                       .consumeResponse(BasicConsumeOkBody.class)
                       .channel().flow(true)
                       .consumeResponse(ChannelFlowOkBody.class)
                       .basic().contentHeaderPropertiesHeaders(messageHeaders)
                               .publishExchange("")
                               .publishRoutingKey(BrokerAdmin.TEST_QUEUE_NAME)
                               .publishMessage()
                       .consumeResponse(BasicDeliverBody.class);

            BasicDeliverBody delivery = interaction.getLatestResponse(BasicDeliverBody.class);
            assertThat(delivery.getConsumerTag(), is(equalTo(AMQShortString.valueOf(consumerTag))));

            ContentHeaderBody header =
                    interaction.consumeResponse(ContentHeaderBody.class).getLatestResponse(ContentHeaderBody.class);

            assertThat(header.getBodySize(), is(equalTo(0L)));

            BasicContentHeaderProperties properties = header.getProperties();
            Map<String, Object> receivedHeaders = new HashMap<>(properties.getHeadersAsMap());
            assertThat(receivedHeaders, is(equalTo(new HashMap<>(messageHeaders))));
        }
    }

    private String generateLongString(final int count)
    {
        String pattern = "abcde";
        String str = String.join("", Collections.nCopies(count / pattern.length(), pattern)) + pattern.substring(0, count % pattern.length());
        assertThat(str.length(), is(equalTo(count)));
        return str;
    }

    private int calculateContentHeaderFramingOverhead(final String propertyName)
    {
        int frame = 1 + 2 + 4 + 1;
        int body = 2 + 2 + 8 + 2;
        int properties = 4  // Headers field table
                         + propertyName.length() + 1 + 1 + 4;
        return frame + body + properties;
    }


}
