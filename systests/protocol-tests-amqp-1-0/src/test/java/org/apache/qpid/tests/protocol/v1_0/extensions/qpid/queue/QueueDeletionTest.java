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
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

import java.net.InetSocketAddress;

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
import org.apache.qpid.server.protocol.v1_0.type.transport.Open;
import org.apache.qpid.server.protocol.v1_0.type.transport.ReceiverSettleMode;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.tests.protocol.Response;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.protocol.v1_0.Interaction;
import org.apache.qpid.tests.protocol.v1_0.InteractionTransactionalState;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;
import org.apache.qpid.tests.utils.BrokerSpecific;
import org.apache.qpid.tests.utils.ConfigItem;

@BrokerSpecific(kind = KIND_BROKER_J)
@ConfigItem(name = "qpid.resource.deleteOnlyWithNoLinkAttached", value = "false")
public class QueueDeletionTest extends BrokerAdminUsingTestBase
{

    private static final String TEST_MESSAGE_CONTENT = "test";

    @Test
    public void senderDetachedOnQueueDelete() throws Exception
    {
        final InetSocketAddress addr = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
        try (FrameTransport transport = new FrameTransport(addr).connect())
        {
            Interaction interaction = transport.newInteraction();
            final Attach responseAttach = interaction.negotiateProtocol().consumeResponse()
                                                     .open().consumeResponse(Open.class)
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
        final InetSocketAddress addr = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
        try (FrameTransport transport = new FrameTransport(addr).connect())
        {
            Interaction interaction = transport.newInteraction();
            final Attach responseAttach = interaction.negotiateProtocol()
                                                     .consumeResponse()
                                                     .open()
                                                     .consumeResponse(Open.class)
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
    public void queueInUseByEnqueuingTransaction() throws Exception
    {
        final InetSocketAddress addr = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
        try (FrameTransport transport = new FrameTransport(addr).connect())
        {
            final UnsignedInteger linkHandle = UnsignedInteger.ONE;

            final Interaction interaction = transport.newInteraction();
            final InteractionTransactionalState txnState = interaction.createTransactionalState(UnsignedInteger.ZERO);

            Attach attach = interaction.negotiateProtocol()
                                       .consumeResponse()
                                       .open()
                                       .consumeResponse(Open.class)
                                       .begin()
                                       .consumeResponse(Begin.class)

                                       .txnAttachCoordinatorLink(txnState)
                                       .txnDeclare(txnState)

                                       .attachRole(Role.SENDER)
                                       .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                       .attachHandle(linkHandle)
                                       .attach().consumeResponse(Attach.class).getLatestResponse(Attach.class);

            Disposition responseDisposition = interaction.consumeResponse(Flow.class)

                                                         .transferHandle(linkHandle)
                                                         .transferPayloadData(TEST_MESSAGE_CONTENT)
                                                         .transferTransactionalState(txnState.getCurrentTransactionId())
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

            interaction.discharge(txnState, false);

            assertTransactionRollbackOnly(interaction, txnState);
        }
    }

    @Test
    public void queueInUseByDequeuingTransaction() throws Exception
    {
        final InetSocketAddress addr = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
        getBrokerAdmin().putMessageOnQueue(BrokerAdmin.TEST_QUEUE_NAME,
                                           TEST_MESSAGE_CONTENT + 1,
                                           TEST_MESSAGE_CONTENT + 2);
        try (FrameTransport transport = new FrameTransport(addr).connect())
        {
            final Interaction interaction = transport.newInteraction();
            final InteractionTransactionalState txnState = interaction.createTransactionalState(UnsignedInteger.ZERO);
            Attach attach = interaction.negotiateProtocol()
                                       .consumeResponse()
                                       .open()
                                       .consumeResponse(Open.class)
                                       .begin()
                                       .consumeResponse(Begin.class)

                                       .txnAttachCoordinatorLink(txnState)
                                       .txnDeclare(txnState)

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
            assertThat(data, is(equalTo(TEST_MESSAGE_CONTENT + 1)));

            interaction.dispositionSettled(true)
                       .dispositionRole(Role.RECEIVER)
                       .dispositionTransactionalState(txnState.getCurrentTransactionId(), new Accepted())
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
            assertThat(data, is(equalTo(TEST_MESSAGE_CONTENT + 2)));

            getBrokerAdmin().deleteQueue(BrokerAdmin.TEST_QUEUE_NAME);

            final Detach receivedDetach = interaction.consumeResponse().getLatestResponse(Detach.class);
            assertThat(receivedDetach.getError(), is(notNullValue()));
            assertThat(receivedDetach.getError().getCondition(), is(AmqpError.RESOURCE_DELETED));
            assertThat(receivedDetach.getHandle(), is(equalTo(attach.getHandle())));

            interaction.discharge(txnState, false);

            assertTransactionRollbackOnly(interaction, txnState);
        }
    }

    private void assertTransactionRollbackOnly(final Interaction interaction,
                                               final InteractionTransactionalState txnState) throws Exception
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
                if (flowResponse.getHandle().equals(txnState.getHandle()))
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
}
