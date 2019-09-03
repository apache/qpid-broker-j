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
package org.apache.qpid.tests.protocol.v0_10.extensions.queue;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.qpid.tests.utils.BrokerAdmin.KIND_BROKER_J;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.Collections;

import org.junit.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_10.transport.ExecutionErrorCode;
import org.apache.qpid.server.protocol.v0_10.transport.ExecutionException;
import org.apache.qpid.server.protocol.v0_10.transport.MessageCreditUnit;
import org.apache.qpid.server.protocol.v0_10.transport.MessageProperties;
import org.apache.qpid.server.protocol.v0_10.transport.MessageTransfer;
import org.apache.qpid.server.protocol.v0_10.transport.SessionCommandPoint;
import org.apache.qpid.server.protocol.v0_10.transport.SessionCompleted;
import org.apache.qpid.server.protocol.v0_10.transport.SessionConfirmed;
import org.apache.qpid.tests.protocol.v0_10.FrameTransport;
import org.apache.qpid.tests.protocol.v0_10.Interaction;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;
import org.apache.qpid.tests.utils.BrokerSpecific;

@BrokerSpecific(kind = KIND_BROKER_J)
public class QueueTest extends BrokerAdminUsingTestBase
{
    private static final byte[] SESSION_NAME = "test".getBytes(UTF_8);

    @Test
    public void queueDeclareUsingRealQueueAttributesInWireArguments() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            SessionCompleted completed = interaction.negotiateOpen()
                                                    .channelId(1)
                                                    .attachSession(SESSION_NAME)
                                                    .queue()
                                                    .declareQueue(BrokerAdmin.TEST_QUEUE_NAME)
                                                    .declareArguments(Collections.singletonMap("defaultFilters",
                                                                                               "{\"selector\":{\"x-filter-jms-selector\":[\"id=2\"]}}"))
                                                    .declareId(0)
                                                    .declare()
                                                    .session()
                                                    .flushCompleted()
                                                    .flush()
                                                    .consumeResponse()
                                                    .getLatestResponse(SessionCompleted.class);

            assertThat(completed.getCommands().includes(0), is(equalTo(true)));
            assertThat(getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(0)));

            MessageProperties messageProperties1 = new MessageProperties();
            messageProperties1.setApplicationHeaders(Collections.singletonMap("id", 1));

            interaction.message()
                       .transferDestination(BrokerAdmin.TEST_QUEUE_NAME)
                       .transferId(0)
                       .transferHeader(null, messageProperties1)
                       .transferBody("Test1".getBytes(StandardCharsets.UTF_8))
                       .transfer()
                       .session()
                       .flushCompleted()
                       .flush()
                       .consumeResponse();

            MessageProperties messageProperties2 = new MessageProperties();
            messageProperties2.setApplicationHeaders(Collections.singletonMap("id", 2));
            final String body2 = "Message 2 Content";
            interaction.message()
                       .transferDestination(BrokerAdmin.TEST_QUEUE_NAME)
                       .transferId(1)
                       .transferHeader(null, messageProperties2)
                       .transferBody(body2.getBytes(StandardCharsets.UTF_8))
                       .transfer()
                       .session()
                       .flushCompleted()
                       .flush()
                       .consumeResponse();

            final String subscriberName = "Test";
            interaction.message()
                       .subscribeDestination(subscriberName)
                       .subscribeQueue(BrokerAdmin.TEST_QUEUE_NAME)
                       .subscribeId(0)
                       .subscribe()
                       .message()
                       .flowId(1)
                       .flowDestination(subscriberName)
                       .flowUnit(MessageCreditUnit.MESSAGE)
                       .flowValue(1)
                       .flow()
                       .message()
                       .flowId(2)
                       .flowDestination(subscriberName)
                       .flowUnit(MessageCreditUnit.BYTE)
                       .flowValue(-1)
                       .flow();

            MessageTransfer transfer = interaction.consume(MessageTransfer.class,
                                                           SessionCompleted.class,
                                                           SessionCommandPoint.class,
                                                           SessionConfirmed.class);

            try (QpidByteBuffer buffer = transfer.getBody())
            {
                final byte[] dst = new byte[buffer.remaining()];
                buffer.get(dst);
                assertThat(new String(dst, UTF_8), is(equalTo(body2)));
            }
        }
    }


    @Test
    public void queueDeclareInvalidWireArguments() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .channelId(1)
                       .attachSession(SESSION_NAME)
                       .queue()
                       .declareQueue(BrokerAdmin.TEST_QUEUE_NAME)
                       .declareArguments(Collections.singletonMap("foo", "bar"))
                       .declareId(0)
                       .declare()
                       .session()
                       .flushCompleted()
                       .flush();

            ExecutionException exception =
                    interaction.consume(ExecutionException.class, SessionCompleted.class, SessionCommandPoint.class);

            assertThat(exception.getErrorCode(), is(equalTo(ExecutionErrorCode.ILLEGAL_ARGUMENT)));
        }
    }
}
