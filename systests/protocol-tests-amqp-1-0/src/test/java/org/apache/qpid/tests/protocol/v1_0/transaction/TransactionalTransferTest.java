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
package org.apache.qpid.tests.protocol.v1_0.transaction;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Accepted;
import org.apache.qpid.server.protocol.v1_0.type.transaction.TransactionError;
import org.apache.qpid.server.protocol.v1_0.type.transaction.TransactionalState;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Close;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Disposition;
import org.apache.qpid.server.protocol.v1_0.type.transport.End;
import org.apache.qpid.server.protocol.v1_0.type.transport.Error;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.Open;
import org.apache.qpid.server.protocol.v1_0.type.transport.ReceiverSettleMode;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.server.protocol.v1_0.type.transport.Transfer;
import org.apache.qpid.tests.protocol.Response;
import org.apache.qpid.tests.protocol.SpecificationTest;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.protocol.v1_0.Interaction;
import org.apache.qpid.tests.protocol.v1_0.InteractionTransactionalState;
import org.apache.qpid.tests.protocol.v1_0.Utils;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;

public class TransactionalTransferTest extends BrokerAdminUsingTestBase
{
    private InetSocketAddress _brokerAddress;

    @Before
    public void setUp()
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
        _brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);
    }

    @Test
    @SpecificationTest(section = "4.4.4",
            description = "Transactional Posting[...]the transaction controller wishes to associate an outgoing"
                          + " transfer with a transaction, it MUST set the state of the transfer with a"
                          + "transactional-state carrying the appropriate transaction identifier.")
    public void sendTransactionalPostingReceiverSettlesFirst() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final UnsignedInteger linkHandle = UnsignedInteger.ONE;

            final Interaction interaction = transport.newInteraction();
            final InteractionTransactionalState txnState = interaction.createTransactionalState(UnsignedInteger.ZERO);
            Disposition responseDisposition = interaction.negotiateProtocol()
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
                                                         .attach().consumeResponse(Attach.class)
                                                         .consumeResponse(Flow.class)

                                                         .transferHandle(linkHandle)
                                                         .transferPayloadData(getTestName())
                                                         .transferTransactionalState(txnState.getCurrentTransactionId())
                                                         .transfer()
                                                         .consumeResponse(Disposition.class)
                                                         .getLatestResponse(Disposition.class);

            assertThat(responseDisposition.getRole(), is(Role.RECEIVER));
            assertThat(responseDisposition.getSettled(), is(Boolean.TRUE));
            assertThat(responseDisposition.getState(), is(instanceOf(TransactionalState.class)));
            assertThat(((TransactionalState) responseDisposition.getState()).getOutcome(), is(instanceOf(Accepted.class)));

            interaction.txnDischarge(txnState, false);

            Object receivedMessage = Utils.receiveMessage(_brokerAddress, BrokerAdmin.TEST_QUEUE_NAME);
            assertThat(receivedMessage, is(equalTo(getTestName())));
        }
    }

    @Test
    @SpecificationTest(section = "4.4.4",
            description = "Transactional Posting[...]the transaction controller wishes to associate an outgoing"
                          + " transfer with a transaction, it MUST set the state of the transfer with a"
                          + "transactional-state carrying the appropriate transaction identifier.")
    public void sendTransactionalPostingDischargeFail() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final UnsignedInteger linkHandle = UnsignedInteger.ONE;

            final Interaction interaction = transport.newInteraction();
            final InteractionTransactionalState txnState = interaction.createTransactionalState(UnsignedInteger.ZERO);
            Disposition responseDisposition = interaction.negotiateProtocol()
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
                                                         .attach().consumeResponse(Attach.class)
                                                         .consumeResponse(Flow.class)

                                                         .transferHandle(linkHandle)
                                                         .transferPayloadData(getTestName())
                                                         .transferTransactionalState(txnState.getCurrentTransactionId())
                                                         .transfer()
                                                         .consumeResponse(Disposition.class)
                                                         .getLatestResponse(Disposition.class);

            assertThat(responseDisposition.getRole(), is(Role.RECEIVER));
            assertThat(responseDisposition.getSettled(), is(Boolean.TRUE));
            assertThat(responseDisposition.getState(), is(instanceOf(TransactionalState.class)));
            assertThat(((TransactionalState) responseDisposition.getState()).getOutcome(), is(instanceOf(Accepted.class)));

            interaction.txnDischarge(txnState, true);

            if (getBrokerAdmin().isQueueDepthSupported())
            {
                assertThat(getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(0)));
            }
            else
            {
                final String content = getTestName() + "_2";
                Utils.putMessageOnQueue(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME, content);
                assertThat(Utils.receiveMessage(_brokerAddress, BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(content)));
            }
        }
    }

    @Test
    @SpecificationTest(section = "4.4.4",
            description = "Transactional Posting[...]the transaction controller wishes to associate an outgoing"
                          + " transfer with a transaction, it MUST set the state of the transfer with a"
                          + "transactional-state carrying the appropriate transaction identifier.")
    public void sendTransactionalPostingReceiverSettlesSecond() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final UnsignedInteger linkHandle = UnsignedInteger.ONE;

            final Interaction interaction = transport.newInteraction();
            final InteractionTransactionalState txnState = interaction.createTransactionalState(UnsignedInteger.ZERO);
            Disposition responseDisposition = interaction.negotiateProtocol()
                                                         .consumeResponse()
                                                         .open()
                                                         .consumeResponse(Open.class)
                                                         .begin()
                                                         .consumeResponse(Begin.class)

                                                         .txnAttachCoordinatorLink(txnState)
                                                         .txnDeclare(txnState)

                                                         .attachRole(Role.SENDER)
                                                         .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                                         .attachRcvSettleMode(ReceiverSettleMode.SECOND)
                                                         .attachHandle(linkHandle)
                                                         .attach().consumeResponse(Attach.class)
                                                         .consumeResponse(Flow.class)

                                                         .transferHandle(linkHandle)
                                                         .transferPayloadData(getTestName())
                                                         .transferTransactionalState(txnState.getCurrentTransactionId())
                                                         .transfer()
                                                         .consumeResponse(Disposition.class)
                                                         .getLatestResponse(Disposition.class);

            assertThat(responseDisposition.getRole(), is(Role.RECEIVER));
            assertThat(responseDisposition.getSettled(), is(Boolean.FALSE));
            assertThat(responseDisposition.getState(), is(instanceOf(TransactionalState.class)));
            assertThat(((TransactionalState) responseDisposition.getState()).getOutcome(), is(instanceOf(Accepted.class)));

            interaction.dispositionRole(Role.SENDER)
                       .dispositionSettled(true)
                       .dispositionTransactionalState(txnState.getCurrentTransactionId(), new Accepted())
                       .disposition();

            interaction.txnDischarge(txnState, false);
        }
        assertThat(Utils.receiveMessage(_brokerAddress, BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(getTestName())));
    }

    @Test
    @SpecificationTest(section = "4.4.1",
            description = "If the transaction controller wishes to associate an outgoing transfer with a transaction,"
                          + " it MUST set the state of the transfer with a transactional-state carrying the appropriate"
                          + " transaction identifier.")
    public void sendTransactionalPostingTransferFailsDueToUnknownTransactionId() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final UnsignedInteger linkHandle = UnsignedInteger.ONE;

            final Interaction interaction = transport.newInteraction();
            final InteractionTransactionalState txnState = interaction.createTransactionalState(UnsignedInteger.ZERO);
            Response<?> response = interaction.negotiateProtocol().consumeResponse()
                                              .open().consumeResponse(Open.class)
                                              .begin().consumeResponse(Begin.class)

                                              .txnAttachCoordinatorLink(txnState)
                                              .txnDeclare(txnState)

                                              .attachRole(Role.SENDER)
                                              .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                              .attachHandle(linkHandle)
                                              .attach().consumeResponse(Attach.class)
                                              .consumeResponse(Flow.class)

                                              .transferHandle(linkHandle)
                                              .transferPayloadData(getTestName())
                                              .transferTransactionalState(integerToBinary(Integer.MAX_VALUE))
                                              .transfer()
                                              .consumeResponse()
                                              .getLatestResponse();

            assertUnknownTransactionIdError(response);
        }
    }


    @Test
    @SpecificationTest(section = "4.4.2", description = "Transactional Retirement[...] The transaction controller might"
                                                        + "wish to associate the outcome of a delivery with a transaction.")
    public void receiveTransactionalRetirementReceiverSettleFirst() throws Exception
    {
        Utils.putMessageOnQueue(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME, getTestName());
        try (FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            final InteractionTransactionalState txnState = interaction.createTransactionalState(UnsignedInteger.ZERO);
            interaction.negotiateProtocol()
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
                       .consumeResponse(Attach.class)

                       .flowIncomingWindow(UnsignedInteger.ONE)
                       .flowNextIncomingId(UnsignedInteger.ZERO)
                       .flowOutgoingWindow(UnsignedInteger.ZERO)
                       .flowNextOutgoingId(UnsignedInteger.ZERO)
                       .flowLinkCredit(UnsignedInteger.ONE)
                       .flowHandleFromLinkHandle()
                       .flow()

                       .receiveDelivery()
                       .decodeLatestDelivery();

            Object data = interaction.getDecodedLatestDelivery();
            assertThat(data, is(equalTo(getTestName())));

            interaction.dispositionSettled(true)
                       .dispositionRole(Role.RECEIVER)
                       .dispositionTransactionalState(txnState.getCurrentTransactionId(), new Accepted())
                       .disposition()
                       .txnDischarge(txnState, false);
        }
    }

    @Test
    @SpecificationTest(section = "4.4.2", description = "Transactional Retirement[...] The transaction controller might"
                                                        + "wish to associate the outcome of a delivery with a transaction.")
    public void receiveTransactionalRetirementDischargeFail() throws Exception
    {
        Utils.putMessageOnQueue(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME, getTestName());
        try (FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            final InteractionTransactionalState txnState = interaction.createTransactionalState(UnsignedInteger.ZERO);
            interaction.negotiateProtocol()
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
                       .consumeResponse(Attach.class)

                       .flowIncomingWindow(UnsignedInteger.ONE)
                       .flowNextIncomingId(UnsignedInteger.ZERO)
                       .flowOutgoingWindow(UnsignedInteger.ZERO)
                       .flowNextOutgoingId(UnsignedInteger.ZERO)
                       .flowLinkCredit(UnsignedInteger.ONE)
                       .flowHandleFromLinkHandle()
                       .flow()

                       .receiveDelivery()
                       .decodeLatestDelivery();

            Object data = interaction.getDecodedLatestDelivery();
            assertThat(data, is(equalTo(getTestName())));

            interaction.dispositionSettled(true)
                       .dispositionRole(Role.RECEIVER)
                       .dispositionTransactionalState(txnState.getCurrentTransactionId(), new Accepted())
                       .disposition()
                       .txnDischarge(txnState, true);

            Object receivedMessage = Utils.receiveMessage(_brokerAddress, BrokerAdmin.TEST_QUEUE_NAME);
            assertThat(receivedMessage, is(equalTo(getTestName())));
        }
    }

    @Test
    @SpecificationTest(section = "4.4.2", description = "Transactional Retirement[...]"
                                                        + " To associate an outcome with a transaction the controller"
                                                        + " sends a disposition performative which sets the state"
                                                        + " of the delivery to a transactional-state with the desired"
                                                        + " transaction identifier and the outcome to be applied"
                                                        + " upon a successful discharge.")
    public void receiveTransactionalRetirementDispositionFailsDueToUnknownTransactionId() throws Exception
    {
        Utils.putMessageOnQueue(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME, getTestName());
        try (FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            final InteractionTransactionalState txnState = interaction.createTransactionalState(UnsignedInteger.ZERO);
            List<Transfer> transfers = interaction.negotiateProtocol().consumeResponse()
                                                  .open().consumeResponse(Open.class)
                                                  .begin().consumeResponse(Begin.class)

                                                  .txnAttachCoordinatorLink(txnState)
                                                  .txnDeclare(txnState)

                                                  .attachRole(Role.RECEIVER)
                                                  .attachHandle(UnsignedInteger.ONE)
                                                  .attachSourceAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                                  .attachRcvSettleMode(ReceiverSettleMode.FIRST)
                                                  .attach().consumeResponse(Attach.class)

                                                  .flowIncomingWindow(UnsignedInteger.ONE)
                                                  .flowNextIncomingId(UnsignedInteger.ZERO)
                                                  .flowOutgoingWindow(UnsignedInteger.ZERO)
                                                  .flowNextOutgoingId(UnsignedInteger.ZERO)
                                                  .flowLinkCredit(UnsignedInteger.ONE)
                                                  .flowHandleFromLinkHandle()
                                                  .flow()

                                                  .receiveDelivery()
                                                  .getLatestDelivery();

            UnsignedInteger deliveryId = transfers.get(0).getDeliveryId();
            assertThat(deliveryId, is(notNullValue()));

            Object data = interaction.decodeLatestDelivery().getDecodedLatestDelivery();
            assertThat(data, is(equalTo(getTestName())));

            Response<?> response = interaction.dispositionSettled(true)
                                              .dispositionRole(Role.RECEIVER)
                                              .dispositionTransactionalState(integerToBinary(Integer.MAX_VALUE),
                                                                             new Accepted())
                                              .dispositionFirst(deliveryId)
                                              .disposition()
                                              .consumeResponse().getLatestResponse();
            assertUnknownTransactionIdError(response);
        }
        assertThat(Utils.receiveMessage(_brokerAddress, BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(getTestName())));
    }

    @Ignore("TODO disposition is currently not being sent by Broker")
    @Test
    @SpecificationTest(section = "4.4.2", description = "Transactional Retirement[...] The transaction controller might"
                                                        + "wish to associate the outcome of a delivery with a transaction.")
    public void receiveTransactionalRetirementReceiverSettleSecond() throws Exception
    {
        Utils.putMessageOnQueue(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME, getTestName());
        try (FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            final InteractionTransactionalState txnState = interaction.createTransactionalState(UnsignedInteger.ZERO);
            interaction.negotiateProtocol()
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
                       .attachRcvSettleMode(ReceiverSettleMode.SECOND)
                       .attach()
                       .consumeResponse(Attach.class)

                       .flowIncomingWindow(UnsignedInteger.ONE)
                       .flowNextIncomingId(UnsignedInteger.ZERO)
                       .flowOutgoingWindow(UnsignedInteger.ZERO)
                       .flowNextOutgoingId(UnsignedInteger.ZERO)
                       .flowLinkCredit(UnsignedInteger.ONE)
                       .flowHandleFromLinkHandle()
                       .flow()

                       .receiveDelivery()
                       .decodeLatestDelivery();

            Object data = interaction.getDecodedLatestDelivery();
            assertThat(data, is(equalTo(getTestName())));

            Disposition settledDisposition = interaction.dispositionSettled(false)
                                                    .dispositionRole(Role.RECEIVER)
                                                    .dispositionTransactionalState(txnState.getCurrentTransactionId(),
                                                                                   new Accepted())
                                                    .disposition()
                                                    .consumeResponse(Disposition.class)
                                                    .getLatestResponse(Disposition.class);

            assertThat(settledDisposition.getSettled(), is(true));
            assertThat(settledDisposition.getState(), is(instanceOf(TransactionalState.class)));
            assertThat(((TransactionalState) settledDisposition.getState()).getOutcome(), is(instanceOf(Accepted.class)));

            interaction.txnDischarge(txnState, false);
        }
    }

    @Test
    @SpecificationTest(section = "4.4.2", description = "Transactional Acquisition[...]In the case of the flow frame,"
                                                        + " the transactional work is not necessarily directly"
                                                        + " initiated or entirely determined when the flow frame"
                                                        + " arrives at the resource, but can in fact occur at some "
                                                        + " later point and in ways not necessarily"
                                                        + " anticipated by the controller.")
    public void receiveTransactionalAcquisitionReceiverSettleFirst() throws Exception
    {
        Utils.putMessageOnQueue(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME, getTestName());
        try (FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            final InteractionTransactionalState txnState = interaction.createTransactionalState(UnsignedInteger.ZERO);
            interaction.negotiateProtocol()
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
                       .consumeResponse(Attach.class)

                       .flowIncomingWindow(UnsignedInteger.ONE)
                       .flowNextIncomingId(UnsignedInteger.ZERO)
                       .flowOutgoingWindow(UnsignedInteger.ZERO)
                       .flowNextOutgoingId(UnsignedInteger.ZERO)
                       .flowLinkCredit(UnsignedInteger.ONE)
                       .flowHandleFromLinkHandle()
                       .flowProperties(Collections.singletonMap(Symbol.valueOf("txn-id"), txnState.getCurrentTransactionId()))
                       .flow()

                       .receiveDelivery();

            List<Transfer> transfers = interaction.getLatestDelivery();
            assertThat(transfers.size(), is(equalTo(1)));
            Transfer transfer = transfers.get(0);
            assertThat(transfer.getState(), is(instanceOf(TransactionalState.class)));
            assertThat(((TransactionalState) transfer.getState()).getTxnId(), is(equalTo(txnState.getCurrentTransactionId())));

            Object data = interaction.decodeLatestDelivery().getDecodedLatestDelivery();
            assertThat(data, is(equalTo(getTestName())));

            interaction.dispositionSettled(true)
                       .dispositionRole(Role.RECEIVER)
                       .dispositionTransactionalState(txnState.getCurrentTransactionId(), new Accepted())
                       .disposition()
                       .txnDischarge(txnState, false);

            if (getBrokerAdmin().isQueueDepthSupported())
            {
                assertThat(getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(0)));
            }
            else
            {
                final String content = getTestName() + "_2";
                Utils.putMessageOnQueue(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME, content);
                assertThat(Utils.receiveMessage(_brokerAddress, BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(content)));
            }
        }
    }

    @Test
    @SpecificationTest(section = "4.4.3", description = "Transactional Acquisition[...]In the case of the flow frame,"
                                                        + " the transactional work is not necessarily directly"
                                                        + " initiated or entirely determined when the flow frame"
                                                        + " arrives at the resource, but can in fact occur at some "
                                                        + " later point and in ways not necessarily"
                                                        + " anticipated by the controller.")
    public void receiveTransactionalAcquisitionDischargeFail() throws Exception
    {
        Utils.putMessageOnQueue(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME, getTestName());
        try (FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            final InteractionTransactionalState txnState = interaction.createTransactionalState(UnsignedInteger.ZERO);
            interaction.negotiateProtocol()
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
                       .consumeResponse(Attach.class)

                       .flowIncomingWindow(UnsignedInteger.ONE)
                       .flowNextIncomingId(UnsignedInteger.ZERO)
                       .flowOutgoingWindow(UnsignedInteger.ZERO)
                       .flowNextOutgoingId(UnsignedInteger.ZERO)
                       .flowLinkCredit(UnsignedInteger.ONE)
                       .flowHandleFromLinkHandle()
                       .flowProperties(Collections.singletonMap(Symbol.valueOf("txn-id"), txnState.getCurrentTransactionId()))
                       .flow()

                       .receiveDelivery();

            List<Transfer> transfers = interaction.getLatestDelivery();
            assertThat(transfers.size(), is(equalTo(1)));
            Transfer transfer = transfers.get(0);
            assertThat(transfer.getState(), is(instanceOf(TransactionalState.class)));
            assertThat(((TransactionalState) transfer.getState()).getTxnId(), is(equalTo(txnState.getCurrentTransactionId())));

            Object data = interaction.decodeLatestDelivery().getDecodedLatestDelivery();
            assertThat(data, is(equalTo(getTestName())));

            interaction.dispositionSettled(true)
                       .dispositionRole(Role.RECEIVER)
                       .dispositionTransactionalState(txnState.getCurrentTransactionId(), new Accepted())
                       .disposition()
                       .txnDischarge(txnState, true);

            if (getBrokerAdmin().isQueueDepthSupported())
            {
                assertThat(getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(1)));
            }
            assertThat(Utils.receiveMessage(_brokerAddress, BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(getTestName())));
        }
    }

    @Test
    @Ignore("QPID-7951")
    @SpecificationTest(section = "4.4.3", description = "Transactional Acquisition[...]"
                                                        + " the resource associates an additional piece of state with"
                                                        + " outgoing link endpoints, a txn-id that identifies"
                                                        + " the transaction with which acquired messages"
                                                        + " will be associated. This state is determined by"
                                                        + " the controller by specifying a txn-id entry in the"
                                                        + " properties map of the flow frame.")
    public void receiveTransactionalAcquisitionFlowFailsDueToUnknownTransactionId() throws Exception
    {
        Utils.putMessageOnQueue(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME, getTestName());
        try (FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            final InteractionTransactionalState txnState = interaction.createTransactionalState(UnsignedInteger.ZERO);
            Response<?> response = interaction.negotiateProtocol()
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
                                              .consumeResponse(Attach.class)

                                              .flowIncomingWindow(UnsignedInteger.ONE)
                                              .flowLinkCredit(UnsignedInteger.ONE)
                                              .flowHandleFromLinkHandle()
                                              .flowProperties(Collections.singletonMap(Symbol.valueOf("txn-id"),
                                                                                       integerToBinary(Integer.MAX_VALUE)))
                                              .flow()
                                              .consumeResponse()
                                              .getLatestResponse();

            assertUnknownTransactionIdError(response);
        }
        assertThat(Utils.receiveMessage(_brokerAddress, BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(getTestName())));
    }

    private void assertUnknownTransactionIdError(final Response<?> response)
    {
        assertThat(response, is(notNullValue()));
        final Object body = response.getBody();
        assertThat(body, is(notNullValue()));
        Error error = null;
        if (body instanceof Close)
        {
            error = ((Close) body).getError();
        }
        else if (body instanceof End)
        {
            error = ((End) body).getError();
        }
        else if (body instanceof Detach)
        {
            error = ((Detach) body).getError();
        }
        else
        {
            fail(String.format("Unexpected response %s", body.getClass().getSimpleName()));
        }

        assertThat(error, is(notNullValue()));
        assertThat(error.getCondition(), equalTo(TransactionError.UNKNOWN_ID));
    }

    Binary integerToBinary(final int txnId)
    {
        byte[] data = new byte[4];
        data[3] = (byte) (txnId & 0xff);
        data[2] = (byte) ((txnId & 0xff00) >> 8);
        data[1] = (byte) ((txnId & 0xff0000) >> 16);
        data[0] = (byte) ((txnId & 0xff000000) >> 24);
        return new Binary(data);
    }
}
