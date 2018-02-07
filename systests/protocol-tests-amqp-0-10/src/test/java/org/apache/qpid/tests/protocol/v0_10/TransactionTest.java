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
import static org.apache.qpid.tests.utils.BrokerAdmin.KIND_BROKER_J;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.net.InetSocketAddress;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.protocol.v0_10.transport.ExecutionErrorCode;
import org.apache.qpid.server.protocol.v0_10.transport.ExecutionException;
import org.apache.qpid.server.protocol.v0_10.transport.MessageAcceptMode;
import org.apache.qpid.server.protocol.v0_10.transport.MessageAcquireMode;
import org.apache.qpid.server.protocol.v0_10.transport.MessageCreditUnit;
import org.apache.qpid.server.protocol.v0_10.transport.MessageTransfer;
import org.apache.qpid.server.protocol.v0_10.transport.Range;
import org.apache.qpid.server.protocol.v0_10.transport.RangeSet;
import org.apache.qpid.server.protocol.v0_10.transport.SessionCommandPoint;
import org.apache.qpid.server.protocol.v0_10.transport.SessionCompleted;
import org.apache.qpid.server.protocol.v0_10.transport.SessionConfirmed;
import org.apache.qpid.server.protocol.v0_10.transport.SessionDetach;
import org.apache.qpid.server.protocol.v0_10.transport.SessionFlush;
import org.apache.qpid.tests.protocol.Response;
import org.apache.qpid.tests.protocol.SpecificationTest;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;
import org.apache.qpid.tests.utils.BrokerSpecific;

public class TransactionTest extends BrokerAdminUsingTestBase
{
    private InetSocketAddress _brokerAddress;

    @Before
    public void setUp()
    {
        _brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
    }

    @Test
    @SpecificationTest(section = "10.tx.commit",
            description = "This command commits all messages published and accepted in the current transaction.")
    public void messageSendCommit() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            byte[] sessionName = "test".getBytes(UTF_8);
            interaction.openAnonymousConnection()
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

            interaction.tx().commitId(2).commit()
                       .session()
                       .flushCompleted()
                       .flush();

            completed = interaction.consumeResponse().getLatestResponse(SessionCompleted.class);
            assertThat(completed.getCommands().includes(2), is(equalTo(true)));

            queueDepthMessages = getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME);
            assertThat(queueDepthMessages, is(equalTo(1)));
        }
    }

    @Test
    @BrokerSpecific(kind = KIND_BROKER_J)
    public void publishTransactionTimeout() throws Exception
    {
        int transactionTimeout = 1000;
        getBrokerAdmin().configure("storeTransactionOpenTimeoutClose", transactionTimeout);

        try (FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            byte[] sessionName = "test".getBytes(UTF_8);
            interaction.openAnonymousConnection()
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

            Thread.sleep(transactionTimeout + 1000);

            ExecutionException e = receiveResponse(interaction, ExecutionException.class);
            assertThat(e.getErrorCode(), is(equalTo(ExecutionErrorCode.RESOURCE_LIMIT_EXCEEDED)));
            assertThat(e.getDescription(), containsString("transaction timed out"));

            SessionDetach detach = receiveResponse(interaction, SessionDetach.class);
            assertThat(detach.getName(), is(equalTo(sessionName)));

            assertThat(getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(0)));
        }
    }

    @Test
    @BrokerSpecific(kind = KIND_BROKER_J)
    public void consumeTransactionTimeout() throws Exception
    {
        int transactionTimeout = 1000;
        getBrokerAdmin().configure("storeTransactionOpenTimeoutClose", transactionTimeout);

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

            Thread.sleep(transactionTimeout + 1000);

            ExecutionException e = receiveResponse(interaction, ExecutionException.class);
            assertThat(e.getErrorCode(), is(equalTo(ExecutionErrorCode.RESOURCE_LIMIT_EXCEEDED)));
            assertThat(e.getDescription(), containsString("transaction timed out"));

            SessionDetach detach = receiveResponse(interaction, SessionDetach.class);
            assertThat(detach.getName(), is(equalTo(sessionName)));
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
