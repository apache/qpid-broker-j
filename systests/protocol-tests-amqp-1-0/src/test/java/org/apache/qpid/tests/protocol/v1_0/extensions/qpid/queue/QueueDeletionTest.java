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
package org.apache.qpid.tests.protocol.v1_0.extensions.qpid.queue;

import static org.apache.qpid.tests.utils.BrokerAdmin.KIND_BROKER_J;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assume.assumeThat;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Accepted;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Rejected;
import org.apache.qpid.server.protocol.v1_0.type.transaction.TransactionError;
import org.apache.qpid.server.protocol.v1_0.type.transaction.TransactionalState;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Disposition;
import org.apache.qpid.server.protocol.v1_0.type.transport.Error;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.ReceiverSettleMode;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.tests.protocol.Response;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.protocol.v1_0.Interaction;
import org.apache.qpid.tests.protocol.v1_0.Utils;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;
import org.apache.qpid.tests.utils.BrokerSpecific;

@BrokerSpecific(kind = KIND_BROKER_J)
public class QueueDeletionTest extends BrokerAdminUsingTestBase
{

    @Before
    public void setUp()
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
    }

    @Test
    public void senderDetachedOnQueueDelete() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            Interaction interaction = transport.newInteraction();
            final Attach responseAttach = interaction.negotiateOpen()
                                                     .begin().consumeResponse(Begin.class)
                                                     .attachRole(Role.SENDER)
                                                     .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                                     .attach().consumeResponse()
                                                     .getLatestResponse(Attach.class);
            assertThat(responseAttach.getRole(), is(Role.RECEIVER));

            Flow flow = interaction.consumeResponse(Flow.class).getLatestResponse(Flow.class);
            assertThat(flow.getLinkCredit().intValue(), is(greaterThan(1)));

            getBrokerAdmin().deleteQueue(BrokerAdmin.TEST_QUEUE_NAME);

            final Detach receivedDetach = interaction.consumeResponse().getLatestResponse(Detach.class);
            assertThat(receivedDetach.getError(), is(notNullValue()));
            assertThat(receivedDetach.getError().getCondition(), is(AmqpError.RESOURCE_DELETED));
        }
    }

    @Test
    public void receiverDetachedOnQueueDelete() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            Interaction interaction = transport.newInteraction();
            final Attach responseAttach = interaction.negotiateOpen()
                                                     .begin()
                                                     .consumeResponse(Begin.class)
                                                     .attachRole(Role.RECEIVER)
                                                     .attachSourceAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                                     .attach()
                                                     .consumeResponse(Attach.class)
                                                     .getLatestResponse(Attach.class);

            assertThat(responseAttach.getRole(), is(Role.SENDER));

            getBrokerAdmin().deleteQueue(BrokerAdmin.TEST_QUEUE_NAME);

            final Detach receivedDetach = interaction.consumeResponse().getLatestResponse(Detach.class);
            assertThat(receivedDetach.getError(), is(notNullValue()));
            assertThat(receivedDetach.getError().getCondition(), is(AmqpError.RESOURCE_DELETED));
        }
    }

    @Test
    public void transactedSenderDetachedOnQueueDeletionWhenTransactionInProgress() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final UnsignedInteger linkHandle = UnsignedInteger.ONE;

            final Interaction interaction = transport.newInteraction();

            Attach attach = interaction.negotiateOpen()
                                       .begin()
                                       .consumeResponse(Begin.class)

                                       .txnAttachCoordinatorLink(UnsignedInteger.ZERO, this::coordinatorAttachExpected)
                                       .txnDeclare()

                                       .attachRole(Role.SENDER)
                                       .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                       .attachHandle(linkHandle)
                                       .attach().consumeResponse(Attach.class).getLatestResponse(Attach.class);

            Disposition responseDisposition = interaction.consumeResponse(Flow.class)

                                                         .transferHandle(linkHandle)
                                                         .transferPayloadData(getTestName())
                                                         .transferTransactionalStateFromCurrentTransaction()
                                                         .transfer()
                                                         .consumeResponse(Disposition.class)
                                                         .getLatestResponse(Disposition.class);

            assertThat(responseDisposition.getRole(), is(Role.RECEIVER));
            assertThat(responseDisposition.getSettled(), is(Boolean.TRUE));
            assertThat(responseDisposition.getState(), is(instanceOf(TransactionalState.class)));
            assertThat(((TransactionalState) responseDisposition.getState()).getOutcome(),
                       is(instanceOf(Accepted.class)));

            getBrokerAdmin().deleteQueue(BrokerAdmin.TEST_QUEUE_NAME);

            final Detach receivedDetach = interaction.consumeResponse().getLatestResponse(Detach.class);
            assertThat(receivedDetach.getError(), is(notNullValue()));
            assertThat(receivedDetach.getError().getCondition(), is(AmqpError.RESOURCE_DELETED));
            assertThat(receivedDetach.getHandle(), is(equalTo(attach.getHandle())));

            interaction.txnSendDischarge(false);

            assertTransactionRollbackOnly(interaction);
        }
    }

    @Test
    public void transactedReceiverDetachedOnQueueDeletionWhenTransactionInProgress() throws Exception
    {
        Utils.putMessageOnQueue(getBrokerAdmin(),
                                BrokerAdmin.TEST_QUEUE_NAME,
                                getTestName() + 1,
                                getTestName() + 2);
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            Attach attach = interaction.negotiateOpen()
                                       .begin()
                                       .consumeResponse(Begin.class)

                                       .txnAttachCoordinatorLink(UnsignedInteger.ZERO, this::coordinatorAttachExpected)
                                       .txnDeclare()

                                       .attachRole(Role.RECEIVER)
                                       .attachHandle(UnsignedInteger.ONE)
                                       .attachSourceAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                       .attachRcvSettleMode(ReceiverSettleMode.FIRST)
                                       .attach()
                                       .consumeResponse(Attach.class).getLatestResponse(Attach.class);

            interaction.flowIncomingWindow(UnsignedInteger.ONE)
                       .flowNextIncomingId(UnsignedInteger.ZERO)
                       .flowOutgoingWindow(UnsignedInteger.ZERO)
                       .flowNextOutgoingId(UnsignedInteger.ZERO)
                       .flowLinkCredit(UnsignedInteger.ONE)
                       .flowHandleFromLinkHandle()
                       .flow()

                       .receiveDelivery()
                       .decodeLatestDelivery();

            Object data = interaction.getDecodedLatestDelivery();
            assertThat(data, is(equalTo(getTestName() + 1)));

            interaction.dispositionSettled(true)
                       .dispositionRole(Role.RECEIVER)
                       .dispositionTransactionalStateFromCurrentTransaction(new Accepted())
                       .disposition();

            interaction.flowIncomingWindow(UnsignedInteger.valueOf(2))
                       .flowNextIncomingId(UnsignedInteger.ONE)
                       .flowOutgoingWindow(UnsignedInteger.ZERO)
                       .flowNextOutgoingId(UnsignedInteger.ZERO)
                       .flowLinkCredit(UnsignedInteger.ONE)
                       .flowHandleFromLinkHandle()
                       .flow()
                       .receiveDelivery()
                       .decodeLatestDelivery();

            data = interaction.getDecodedLatestDelivery();
            assertThat(data, is(equalTo(getTestName() + 2)));

            getBrokerAdmin().deleteQueue(BrokerAdmin.TEST_QUEUE_NAME);

            final Detach receivedDetach = interaction.consumeResponse().getLatestResponse(Detach.class);
            assertThat(receivedDetach.getError(), is(notNullValue()));
            assertThat(receivedDetach.getError().getCondition(), is(AmqpError.RESOURCE_DELETED));
            assertThat(receivedDetach.getHandle(), is(equalTo(attach.getHandle())));

            interaction.txnSendDischarge(false);

            assertTransactionRollbackOnly(interaction);
        }
    }

    private void assertTransactionRollbackOnly(final Interaction interaction) throws Exception
    {
        Disposition declareTransactionDisposition = null;
        Flow coordinatorFlow = null;
        do
        {
            interaction.consumeResponse(Disposition.class, Flow.class);
            Response<?> response = interaction.getLatestResponse();
            if (response.getBody() instanceof Disposition)
            {
                declareTransactionDisposition = (Disposition) response.getBody();
            }
            if (response.getBody() instanceof Flow)
            {
                final Flow flowResponse = (Flow) response.getBody();
                if (flowResponse.getHandle().equals(interaction.getCoordinatorHandle()))
                {
                    coordinatorFlow = flowResponse;
                }
            }
        } while (declareTransactionDisposition == null || coordinatorFlow == null);

        assertThat(declareTransactionDisposition.getSettled(), is(equalTo(true)));
        assertThat(declareTransactionDisposition.getState(), is(instanceOf(Rejected.class)));

        final Error error = ((Rejected) declareTransactionDisposition.getState()).getError();
        assertThat(error, is(notNullValue()));
        assertThat(error.getCondition(), is(equalTo(TransactionError.TRANSACTION_ROLLBACK)));
    }

    private void coordinatorAttachExpected(final Response<?> response)
    {
        assertThat(response, is(notNullValue()));
        assumeThat(response.getBody(), anyOf(instanceOf(Attach.class), instanceOf(Flow.class)));
    }
}
