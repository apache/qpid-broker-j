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
package org.apache.qpid.tests.protocol.v0_10.extensions.maxsize;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.qpid.tests.utils.BrokerAdmin.KIND_BROKER_J;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.stream.IntStream;

import org.hamcrest.core.IsEqual;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.protocol.v0_10.transport.ExecutionErrorCode;
import org.apache.qpid.server.protocol.v0_10.transport.ExecutionException;
import org.apache.qpid.server.protocol.v0_10.transport.MessageProperties;
import org.apache.qpid.server.protocol.v0_10.transport.SessionCommandPoint;
import org.apache.qpid.tests.protocol.v0_10.FrameTransport;
import org.apache.qpid.tests.protocol.v0_10.Interaction;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;
import org.apache.qpid.tests.utils.BrokerSpecific;
import org.apache.qpid.tests.utils.ConfigItem;

@BrokerSpecific(kind = KIND_BROKER_J)
@ConfigItem(name = "qpid.max_message_size", value = "1000")
public class MaximumMessageSizeTest extends BrokerAdminUsingTestBase
{

    @Before
    public void setUp()
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
    }

    @Test
    public void limitExceeded() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            byte[] sessionName = "test".getBytes(UTF_8);

            final byte[] messageContent = new byte[1001];
            IntStream.range(0, messageContent.length).forEach(i -> {messageContent[i] = (byte) (i & 0xFF);});

            MessageProperties messageProperties = new MessageProperties();
            messageProperties.setContentLength(messageContent.length);

            ExecutionException executionException = interaction.negotiateOpen()
                                                               .channelId(1)
                                                               .attachSession(sessionName)
                                                               .message()
                                                               .transferDestination(BrokerAdmin.TEST_QUEUE_NAME)
                                                               .transferId(0)
                                                               .transferBody(messageContent)
                                                               .transferHeader(null, messageProperties)
                                                               .transfer()
                                                               .session()
                                                               .flushCompleted()
                                                               .flush()
                                                               .consumeResponse(SessionCommandPoint.class)
                                                               .consumeResponse()
                                                               .getLatestResponse(ExecutionException.class);

            assertThat(executionException.getErrorCode(), IsEqual.equalTo(ExecutionErrorCode.RESOURCE_LIMIT_EXCEEDED));
        }
    }
}
