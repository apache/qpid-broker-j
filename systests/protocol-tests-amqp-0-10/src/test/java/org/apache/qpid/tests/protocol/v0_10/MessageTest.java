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
package org.apache.qpid.tests.protocol.v0_10;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_10.transport.MessageAcceptMode;
import org.apache.qpid.server.protocol.v0_10.transport.MessageAcquireMode;
import org.apache.qpid.server.protocol.v0_10.transport.MessageCreditUnit;
import org.apache.qpid.server.protocol.v0_10.transport.MessageTransfer;
import org.apache.qpid.server.protocol.v0_10.transport.Method;
import org.apache.qpid.server.protocol.v0_10.transport.Range;
import org.apache.qpid.server.protocol.v0_10.transport.RangeSet;
import org.apache.qpid.server.protocol.v0_10.transport.SessionCommandPoint;
import org.apache.qpid.server.protocol.v0_10.transport.SessionCompleted;
import org.apache.qpid.server.protocol.v0_10.transport.SessionConfirmed;
import org.apache.qpid.server.protocol.v0_10.transport.SessionFlush;
import org.apache.qpid.tests.protocol.Response;
import org.apache.qpid.tests.protocol.SpecificationTest;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;

public class MessageTest extends BrokerAdminUsingTestBase
{
    private InetSocketAddress _brokerAddress;

    @Before
    public void setUp()
    {
        _brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
    }

    @Test
    @SpecificationTest(section = "10.message.transfer",
            description = "This command transfers a message between two peers.")
    public void sendTransfer() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            byte[] sessionName = "test".getBytes(UTF_8);
            SessionCompleted completed = interaction.openAnonymousConnection()
                                                    .channelId(1)
                                                    .attachSession(sessionName)
                                                    .message()
                                                    .transferDesitnation(BrokerAdmin.TEST_QUEUE_NAME)
                                                    .transferId(0)
                                                    .transfer()
                                                    .session()
                                                    .flushCompleted()
                                                    .flush()
                                                    .consumeResponse()
                                                    .getLatestResponse(SessionCompleted.class);

            assertThat(completed.getCommands().includes(0), is(equalTo(true)));
            int queueDepthMessages = getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME);
            assertThat(queueDepthMessages, is(equalTo(1)));
        }
    }

    @Test
    @SpecificationTest(section = "10.message.subscribe",
            description = "This command asks the server to start a \"subscription\","
                          + " which is a request for messages from a specific queue.")
    public void subscribe() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            byte[] sessionName = "testSession".getBytes(UTF_8);
            final String subscriberName = "testSubscriber";
            interaction.openAnonymousConnection()
                       .channelId(1)
                       .attachSession(sessionName)
                       .message()
                       .subscribeDestination(subscriberName)
                       .subscribeQueue(BrokerAdmin.TEST_QUEUE_NAME)
                       .subscribeId(0)
                       .subscribe()
                       .session()
                       .flushCompleted()
                       .flush();

            SessionCompleted completed = consumeResponse(interaction,
                                                         SessionCompleted.class,
                                                         SessionCommandPoint.class,
                                                         SessionConfirmed.class);

            assertThat(completed.getCommands(), is(notNullValue()));
            assertThat(completed.getCommands().includes(0), is(equalTo(true)));
        }
    }

    @Test
    @SpecificationTest(section = "10.message.transfer",
            description = "The client may request a broker to transfer messages to it, from a particular queue,"
                          + " by issuing a subscribe command. The subscribe command specifies the destination"
                          + " that the broker should use for any resulting transfers.")
    public void receiveTransfer() throws Exception
    {
        String testMessageBody = "testMessage";
        getBrokerAdmin().putMessageOnQueue(BrokerAdmin.TEST_QUEUE_NAME, testMessageBody);
        try (FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            byte[] sessionName = "testSession".getBytes(UTF_8);
            final String subscriberName = "testSubscriber";
            interaction.openAnonymousConnection()
                       .channelId(1)
                       .attachSession(sessionName)
                       .message()
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

            MessageTransfer transfer = consumeResponse(interaction,
                                                       MessageTransfer.class,
                                                       SessionCompleted.class,
                                                       SessionCommandPoint.class,
                                                       SessionConfirmed.class);

            try (QpidByteBuffer buffer = transfer.getBody())
            {
                final byte[] dst = new byte[buffer.remaining()];
                buffer.get(dst);
                assertThat(new String(dst, UTF_8), is(equalTo(testMessageBody)));
            }
        }
    }

    @Test
    @SpecificationTest(section = "10.message.accept",
            description = "Accepts the message.")
    public void acceptTransfer() throws Exception
    {
        String testMessageBody = "testMessage";
        getBrokerAdmin().putMessageOnQueue(BrokerAdmin.TEST_QUEUE_NAME, testMessageBody);
        try (FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            byte[] sessionName = "testSession".getBytes(UTF_8);
            final String subscriberName = "testSubscriber";
            interaction.openAnonymousConnection()
                       .channelId(1)
                       .attachSession(sessionName)
                       .message()
                       .subscribeAcceptMode(MessageAcceptMode.EXPLICIT)
                       .subscribeAcquireMode(MessageAcquireMode.PRE_ACQUIRED)
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

            MessageTransfer transfer = consumeResponse(interaction,
                                                       MessageTransfer.class,
                                                       SessionCompleted.class,
                                                       SessionCommandPoint.class,
                                                       SessionConfirmed.class);

            assertThat(getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(1)));

            RangeSet transfers = Range.newInstance(transfer.getId());
            interaction.message().acceptId(3).acceptTransfers(transfers).accept()
                       .session()
                       .flushCompleted()
                       .flush();

            SessionCompleted completed = consumeResponse(interaction,
                                                         SessionCompleted.class,
                                                         SessionCommandPoint.class,
                                                         SessionConfirmed.class,
                                                         SessionFlush.class);

            assertThat(completed.getCommands(), is(notNullValue()));
            assertThat(completed.getCommands().includes(3), is(equalTo(true)));

            assertThat(getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(0)));
        }
    }

    private <T extends Method> T consumeResponse(final Interaction interaction,
                                                 final Class<T> expected,
                                                 final Class<? extends Method>... ignore)
            throws Exception
    {
        List<Class<? extends Method>> possibleResponses = new ArrayList<>(Arrays.asList(ignore));
        possibleResponses.add(expected);

        T completed = null;
        do
        {
            interaction.consumeResponse(possibleResponses.toArray(new Class[possibleResponses.size()]));
            Response<?> response = interaction.getLatestResponse();
            if (expected.isAssignableFrom(response.getBody().getClass()))
            {
                completed = (T) response.getBody();
            }
        }
        while (completed == null);
        return completed;
    }

}
