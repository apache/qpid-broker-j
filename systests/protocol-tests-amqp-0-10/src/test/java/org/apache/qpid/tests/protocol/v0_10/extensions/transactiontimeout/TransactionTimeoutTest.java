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
package org.apache.qpid.tests.protocol.v0_10.extensions.transactiontimeout;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.qpid.tests.utils.BrokerAdmin.KIND_BROKER_J;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.protocol.v0_10.transport.ConnectionClose;
import org.apache.qpid.server.protocol.v0_10.transport.ConnectionCloseCode;
import org.apache.qpid.server.protocol.v0_10.transport.MessageAcceptMode;
import org.apache.qpid.server.protocol.v0_10.transport.MessageAcquireMode;
import org.apache.qpid.server.protocol.v0_10.transport.MessageCreditUnit;
import org.apache.qpid.server.protocol.v0_10.transport.MessageTransfer;
import org.apache.qpid.server.protocol.v0_10.transport.Range;
import org.apache.qpid.server.protocol.v0_10.transport.RangeSet;
import org.apache.qpid.server.protocol.v0_10.transport.SessionCompleted;
import org.apache.qpid.tests.protocol.Response;
import org.apache.qpid.tests.protocol.v0_10.FrameTransport;
import org.apache.qpid.tests.protocol.v0_10.Interaction;
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
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            byte[] sessionName = "test".getBytes(UTF_8);
            interaction.negotiateOpen()
                       .channelId(1)
                       .attachSession(sessionName)
                       .tx().selectId(0).select()
                       .message()
                       .transferDestination(BrokerAdmin.TEST_QUEUE_NAME)
                       .transferId(1)
                       .transfer()
                       .session()
                       .flushCompleted()
                       .flush();

            SessionCompleted completed;
            do
            {
                completed = interaction.consumeResponse().getLatestResponse(SessionCompleted.class);
            }
            while (!completed.getCommands().includes(1));

            int queueDepthMessages = getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME);
            assertThat(queueDepthMessages, is(equalTo(0)));

            ConnectionClose close = receiveResponse(interaction, ConnectionClose.class);
            assertThat(close.getReplyCode(), is(equalTo(ConnectionCloseCode.CONNECTION_FORCED)));
            assertThat(close.getReplyText(), containsString("transaction timed out"));

            assertThat(getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(0)));
        }
    }

    @Test
    public void consumeTransactionTimeout() throws Exception
    {
        String testMessageBody = "testMessage";
        getBrokerAdmin().putMessageOnQueue(BrokerAdmin.TEST_QUEUE_NAME, testMessageBody);
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            byte[] sessionName = "testSession".getBytes(UTF_8);
            final String subscriberName = "testSubscriber";
            interaction.negotiateOpen()
                       .channelId(1)
                       .attachSession(sessionName)
                       .tx().selectId(0).select()
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

            MessageTransfer transfer = receiveResponse(interaction, MessageTransfer.class);

            assertThat(getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(1)));

            RangeSet transfers = Range.newInstance(transfer.getId());
            interaction.message().acceptId(3).acceptTransfers(transfers).accept()
                       .session()
                       .flushCompleted()
                       .flush();

            SessionCompleted completed = receiveResponse(interaction, SessionCompleted.class);

            assertThat(completed.getCommands(), is(notNullValue()));
            assertThat(completed.getCommands().includes(3), is(equalTo(true)));

            ConnectionClose close = receiveResponse(interaction, ConnectionClose.class);
            assertThat(close.getReplyCode(), is(equalTo(ConnectionCloseCode.CONNECTION_FORCED)));
            assertThat(close.getReplyText(), containsString("transaction timed out"));
        }
    }

    private <T> T receiveResponse(final Interaction interaction, Class<T> clazz) throws Exception
    {
        T result = null;
        do
        {
            Response<?> response = interaction.consumeResponse().getLatestResponse();
            if (clazz.isInstance(response.getBody()))
            {
                result = (T) response.getBody();
            }
        }
        while (result == null);
        return result;
    }
}
