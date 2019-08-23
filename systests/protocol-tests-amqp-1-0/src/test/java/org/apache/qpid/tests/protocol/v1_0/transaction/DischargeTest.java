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

package org.apache.qpid.tests.protocol.v1_0.transaction;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assume.assumeThat;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Accepted;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Rejected;
import org.apache.qpid.server.protocol.v1_0.type.transaction.Declared;
import org.apache.qpid.server.protocol.v1_0.type.transaction.TransactionError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Disposition;
import org.apache.qpid.server.protocol.v1_0.type.transport.Error;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.ReceiverSettleMode;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.server.protocol.v1_0.type.transport.Transfer;
import org.apache.qpid.tests.protocol.Response;
import org.apache.qpid.tests.protocol.SpecificationTest;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.protocol.v1_0.Interaction;
import org.apache.qpid.tests.protocol.v1_0.Utils;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;

public class DischargeTest extends BrokerAdminUsingTestBase
{

    @Before
    public void setUp()
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
    }

    @Test
    @SpecificationTest(section = "4.3",
            description = "If the coordinator is unable to complete the discharge,"
                          + " the coordinator MUST convey the error to the controller as a transaction-error."
                          + " If the source for the link to the coordinator supports the rejected outcome, then the "
                          + " message MUST be rejected with this outcome carrying the transaction-error.")
    public void dischargeUnknownTransactionIdWhenSourceSupportsRejectedOutcome() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .begin().consumeResponse(Begin.class)

                       .txnAttachCoordinatorLink(UnsignedInteger.ZERO, this::coordinatorAttachExpected, Rejected.REJECTED_SYMBOL)
                       .txnDeclare();

            assertThat(interaction.getCoordinatorLatestDeliveryState(), is(instanceOf(Declared.class)));
            assertThat(interaction.getCurrentTransactionId(), is(notNullValue()));

            interaction.txnDischarge(new Binary("nonExistingTransaction".getBytes(UTF_8)), false);
            interaction.doCloseConnection();

            assertThat(interaction.getCoordinatorLatestDeliveryState(), is(instanceOf(Rejected.class)));
            final Error error = ((Rejected) interaction.getCoordinatorLatestDeliveryState()).getError();
            assertThat(error, is(notNullValue()));

            assertThat(error.getCondition(), is(equalTo(TransactionError.UNKNOWN_ID)));
        }
    }

    @Test
    @SpecificationTest(section = "4.3",
            description = "If the coordinator is unable to complete the discharge,"
                          + " the coordinator MUST convey the error to the controller as a transaction-error."
                          + " [...] If the source does not support the rejected outcome, the transactional resource"
                          + " MUST detach the link to the coordinator, with the detach performative carrying"
                          + " the transaction-error.")
    public void dischargeUnknownTransactionIdWhenSourceDoesNotSupportRejectedOutcome() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                                           .begin().consumeResponse(Begin.class)
                                           .txnAttachCoordinatorLink(UnsignedInteger.ZERO, this::coordinatorAttachExpected, Accepted.ACCEPTED_SYMBOL)
                                           .txnDeclare();

            assertThat(interaction.getCoordinatorLatestDeliveryState(), is(instanceOf(Declared.class)));
            assertThat(interaction.getCurrentTransactionId(), is(notNullValue()));

            interaction.txnSendDischarge(new Binary("nonExistingTransaction".getBytes(UTF_8)), false);

            final Detach detachResponse = interaction.consumeResponse(Detach.class).getLatestResponse(Detach.class);
            interaction.doCloseConnection();

            final Error error = detachResponse.getError();
            assertThat(error, is(notNullValue()));

            assertThat(error.getCondition(), is(equalTo(TransactionError.UNKNOWN_ID)));
        }
    }

    @Test
    @SpecificationTest(section = "4.4.2",
            description = "Transactional Retirement [...]"
                          + " To associate an outcome with a transaction the controller sends a disposition"
                          + " performative which sets the state of the delivery to a transactional-state with the"
                          + " desired transaction identifier and the outcome to be applied upon a successful discharge.")
    public void dischargeSettledAfterReceiverDetach() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .begin().consumeResponse(Begin.class)

                       .txnAttachCoordinatorLink(UnsignedInteger.ZERO, this::coordinatorAttachExpected)
                       .txnDeclare()

                       .attachRole(Role.RECEIVER)
                       .attachHandle(UnsignedInteger.ONE)
                       .attachSourceAddress(BrokerAdmin.TEST_QUEUE_NAME)
                       .attachRcvSettleMode(ReceiverSettleMode.FIRST)
                       .attach().consumeResponse(Attach.class)

                       .flowNextIncomingIdFromPeerLatestSessionBeginAndDeliveryCount()
                       .flowNextOutgoingId(UnsignedInteger.ZERO)
                       .flowOutgoingWindow(UnsignedInteger.ZERO)
                       .flowIncomingWindow(UnsignedInteger.ONE)
                       .flowLinkCredit(UnsignedInteger.ONE)
                       .flowHandleFromLinkHandle()
                       .flow();

            Utils.putMessageOnQueue(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME, getTestName());

            List<Transfer> transfers = interaction.receiveDelivery().getLatestDelivery();
            assertThat(transfers, is(notNullValue()));
            assertThat(transfers, is(not(empty())));
            final UnsignedInteger deliveryId = transfers.get(0).getDeliveryId();
            interaction.detach().consumeResponse(Detach.class)
                       .dispositionFirst(deliveryId)
                       .dispositionTransactionalStateFromCurrentTransaction(new Accepted())
                       .dispositionRole(Role.RECEIVER)
                       .disposition().txnDischarge(false);

            assertThat(interaction.getCoordinatorLatestDeliveryState(), is(instanceOf(Accepted.class)));

            interaction.doCloseConnection();
        }

        String secondMessage = getTestName() + "_2";
        Utils.putMessageOnQueue(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME, secondMessage);
        Object receivedMessage = Utils.receiveMessage(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME);
        assertThat(receivedMessage, is(equalTo(secondMessage)));
    }

    @Test
    @SpecificationTest(section = "4.4.4.1",
            description = "Transactional Posting [...]"
                          + " Delivery Sent Unsettled By Controller; Resource Settles [...]"
                          + " The resource MUST determine the outcome of the delivery before committing the"
                          + " transaction, and this MUST be communicated to the controller before the acceptance"
                          + " of a successful discharge. The outcome communicated by the resource MUST be associated"
                          + " with the same transaction with which the transfer from controller to resource"
                          + " was associated.")
    public void dischargeSettledAfterSenderDetach() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .begin().consumeResponse(Begin.class)

                       .txnAttachCoordinatorLink(UnsignedInteger.ZERO, this::coordinatorAttachExpected)
                       .txnDeclare()

                       .attachRole(Role.SENDER)
                       .attachHandle(UnsignedInteger.ONE)
                       .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                       .attach().consumeResponse(Attach.class)
                       .consumeResponse(Flow.class)

                       .transferDeliveryId()
                       .transferTransactionalStateFromCurrentTransaction()
                       .transferPayloadData(getTestName())
                       .transferHandle(UnsignedInteger.ONE)
                       .transfer().consume(Disposition.class, Flow.class);

            interaction.detachHandle(UnsignedInteger.ONE)
                       .detach().consumeResponse(Detach.class)
                       .txnDischarge(false);

            assertThat(interaction.getCoordinatorLatestDeliveryState(), is(instanceOf(Accepted.class)));
        }

        final Object receivedMessage = Utils.receiveMessage(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME);
        assertThat(receivedMessage, is(equalTo(getTestName())));
    }

    @Test
    @SpecificationTest(section = "4.4.4.1",
            description = "Transactional Posting [...]"
                          + " Delivery Sent Unsettled By Controller; Resource Does Not Settle [...]"
                          + " After a successful discharge, the state of unsettled deliveries at the resource MUST"
                          + " reflect the outcome that was applied.")
    public void dischargeUnsettledAfterSenderClose() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .begin().consumeResponse(Begin.class)

                       .txnAttachCoordinatorLink(UnsignedInteger.ZERO, this::coordinatorAttachExpected)
                       .txnDeclare()

                       .attachRole(Role.SENDER)
                       .attachHandle(UnsignedInteger.ONE)
                       .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                       .attachRcvSettleMode(ReceiverSettleMode.SECOND)
                       .attach().consumeResponse(Attach.class)
                       .assertLatestResponse(Attach.class,
                                             attach -> assumeThat(attach.getRcvSettleMode(),
                                                                  is(equalTo(ReceiverSettleMode.SECOND))))
                       .consumeResponse(Flow.class)

                       .transferDeliveryId()
                       .transferTransactionalStateFromCurrentTransaction()
                       .transferPayloadData(getTestName())
                       .transferHandle(UnsignedInteger.ONE)
                       .transfer().consumeResponse(Disposition.class)

                       .detachHandle(UnsignedInteger.ONE)
                       .detachClose(true)
                       .detach().consumeResponse(Detach.class);
            interaction.txnDischarge(false);

            assertThat(interaction.getCoordinatorLatestDeliveryState(), is(instanceOf(Accepted.class)));
        }

        final Object receivedMessage = Utils.receiveMessage(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME);
        assertThat(receivedMessage, is(equalTo(getTestName())));
    }

    private void coordinatorAttachExpected(final Response<?> response)
    {
        assertThat(response, is(notNullValue()));
        assumeThat(response.getBody(), anyOf(instanceOf(Attach.class), instanceOf(Flow.class)));
    }
}
