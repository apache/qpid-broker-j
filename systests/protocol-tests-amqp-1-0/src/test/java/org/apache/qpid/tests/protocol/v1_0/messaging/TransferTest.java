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

package org.apache.qpid.tests.protocol.v1_0.messaging;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.oneOf;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.ErrorCarryingFrameBody;
import org.apache.qpid.server.protocol.v1_0.type.Outcome;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedLong;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Accepted;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Header;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Received;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Rejected;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Close;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Disposition;
import org.apache.qpid.server.protocol.v1_0.type.transport.Error;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.LinkError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Open;
import org.apache.qpid.server.protocol.v1_0.type.transport.ReceiverSettleMode;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.server.protocol.v1_0.type.transport.SenderSettleMode;
import org.apache.qpid.server.protocol.v1_0.type.transport.Transfer;
import org.apache.qpid.tests.protocol.Response;
import org.apache.qpid.tests.protocol.SpecificationTest;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.protocol.v1_0.Interaction;
import org.apache.qpid.tests.protocol.v1_0.MessageDecoder;
import org.apache.qpid.tests.protocol.v1_0.MessageEncoder;
import org.apache.qpid.tests.protocol.v1_0.Utils;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;
import org.apache.qpid.tests.utils.BrokerSpecific;
import org.apache.qpid.tests.utils.ConfigItem;

@ConfigItem(name = "qpid.tests.mms.messagestore.persistence", value = "false", jvm = true)
public class TransferTest extends BrokerAdminUsingTestBase
{
    private static final long MAX_MAX_MESSAGE_SIZE_WE_ARE_WILLING_TO_TEST = 200 * 1024 * 1024L;

    @Before
    public void setUp()
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
    }

    @Test
    @SpecificationTest(section = "1.3.4",
            description = "mandatory [...] a non null value for the field is always encoded.")
    public void transferHandleUnspecified() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            Interaction interact = transport.newInteraction();
            Response<?> response = interact.negotiateOpen()
                                           .begin().consumeResponse(Begin.class)
                                           .attachRole(Role.SENDER)
                                           .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                           .attach().consumeResponse(Attach.class)
                                           .consumeResponse(Flow.class)
                                           .assertLatestResponse(Flow.class, this::assumeSufficientCredits)
                                           .transferHandle(null)
                                           .transferPayloadData(getTestName())
                                           .transfer()
                                           .consumeResponse()
                                           .getLatestResponse();

            assertThat(response, is(notNullValue()));
            assertThat(response.getBody(), is(notNullValue()));
            assertThat(response.getBody(), is(instanceOf(ErrorCarryingFrameBody.class)));

            final Error error = ((ErrorCarryingFrameBody) response.getBody()).getError();
            assertThat(error, is(notNullValue()));
            assertThat(error.getCondition(),
                       oneOf(AmqpError.DECODE_ERROR, AmqpError.INVALID_FIELD, AmqpError.ILLEGAL_STATE));
        }
    }

    @Test
    @SpecificationTest(section = "2.7.5",
            description = "The delivery-id MUST be supplied on the first transfer of a multi-transfer delivery.")
    public void transferDeliveryIdUnspecified() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            Interaction interact = transport.newInteraction();
            Response<?> response = interact.negotiateOpen()
                                           .begin().consumeResponse(Begin.class)
                                           .attachRole(Role.SENDER)
                                           .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                           .attach().consumeResponse(Attach.class)
                                           .consumeResponse(Flow.class)
                                           .assertLatestResponse(Flow.class, this::assumeSufficientCredits)
                                           .transferDeliveryId(null)
                                           .transferPayloadData(getTestName())
                                           .transfer()
                                           .consumeResponse()
                                           .getLatestResponse();

            assertThat(response, is(notNullValue()));
            assertThat(response.getBody(), is(notNullValue()));
            assertThat(response.getBody(), is(instanceOf(ErrorCarryingFrameBody.class)));

            final Error error = ((ErrorCarryingFrameBody)response.getBody()).getError();
            assertThat(error, is(notNullValue()));
            assertThat(error.getCondition(), anyOf(equalTo(AmqpError.DECODE_ERROR), equalTo(AmqpError.INVALID_FIELD)));
        }
    }

    @Test
    @SpecificationTest(section = "2.7.5",
            description = "[delivery-tag] MUST be specified for the first transfer "
                          + "[...] and can only be omitted for continuation transfers.")
    public void transferDeliveryTagUnspecified() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            Interaction interaction = transport.newInteraction()
                                               .negotiateOpen()
                                               .begin().consumeResponse(Begin.class)
                                               .attachRole(Role.SENDER)
                                               .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                               .attach().consumeResponse(Attach.class)
                                               .consumeResponse(Flow.class)
                                               .assertLatestResponse(Flow.class, this::assumeSufficientCredits)
                                               .transferDeliveryId()
                                               .transferDeliveryTag(null)
                                               .transferPayloadData(getTestName())
                                               .transfer()
                                               .consumeResponse();

            final Response<?> response = interaction.getLatestResponse();
            assertThat(response, is(notNullValue()));
            assertThat(response.getBody(), is(notNullValue()));
            assertThat(response.getBody(), is(instanceOf(ErrorCarryingFrameBody.class)));

            final Error error = ((ErrorCarryingFrameBody)response.getBody()).getError();
            assertThat(error, is(notNullValue()));
            assertThat(error.getCondition(), anyOf(equalTo(AmqpError.DECODE_ERROR), equalTo(AmqpError.INVALID_FIELD)));
        }
    }

    @Test
    @SpecificationTest(section = "2.6.12 Transferring A Message",
            description = "[...] the receiving application chooses to settle immediately upon processing the message"
                          + " rather than waiting for the sender to settle first, that yields an at-least-once guarantee.")
    public void transferUnsettled() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final UnsignedInteger linkHandle = UnsignedInteger.ONE;
            Disposition responseDisposition = transport.newInteraction()
                                                       .negotiateOpen()
                                                       .begin().consumeResponse(Begin.class)
                                                       .attachRole(Role.SENDER)
                                                       .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                                       .attachHandle(linkHandle)
                                                       .attach().consumeResponse(Attach.class)
                                                       .consumeResponse(Flow.class)
                                                       .assertLatestResponse(Flow.class, this::assumeSufficientCredits)
                                                       .transferDeliveryId()
                                                       .transferHandle(linkHandle)
                                                       .transferPayloadData(getTestName())
                                                       .transfer()
                                                       .consume(Disposition.class, Flow.class);
            assertThat(responseDisposition.getRole(), is(Role.RECEIVER));
            assertThat(responseDisposition.getSettled(), is(Boolean.TRUE));
            assertThat(responseDisposition.getState(), is(instanceOf(Accepted.class)));
        }
        assertThat(Utils.receiveMessage(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(getTestName())));
    }

    @Test
    @SpecificationTest(section = "2.6.12 Transferring A Message",
            description = "The delivery-tag MUST be unique amongst all deliveries"
                          + " that could be considered unsettled by either end of the link.")
    public void transferMessagesWithTheSameDeliveryTagOnSeparateLinksBelongingToTheSameSession() throws Exception
    {
        final String[] contents = Utils.createTestMessageContents(2, getTestName());
        try (final FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final UnsignedInteger link1Handle = UnsignedInteger.ONE;
            final UnsignedInteger link2Handle = UnsignedInteger.valueOf(2);
            final Binary deliveryTag = new Binary("deliveryTag".getBytes(StandardCharsets.UTF_8));
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                                     .begin().consumeResponse(Begin.class)

                                     .attachName("test1")
                                     .attachRole(Role.SENDER)
                                     .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                     .attachSndSettleMode(SenderSettleMode.UNSETTLED)
                                     .attachRcvSettleMode(ReceiverSettleMode.FIRST)
                                     .attachHandle(link1Handle)
                                     .attach().consumeResponse(Attach.class)
                                     .consumeResponse(Flow.class)
                                     .assertLatestResponse(Flow.class, this::assumeSufficientCredits)

                                     .attachName("test2")
                                     .attachHandle(link2Handle)
                                     .attach().consumeResponse(Attach.class)
                                     .consumeResponse(Flow.class)
                                     .assertLatestResponse(Flow.class, this::assumeSufficientCredits)

                                     .transferHandle(link1Handle)
                                     .transferPayloadData(contents[0])
                                     .transferDeliveryTag(deliveryTag)
                                     .transferDeliveryId(UnsignedInteger.ZERO)
                                     .transfer()
                                     .transferHandle(link2Handle)
                                     .transferDeliveryId(UnsignedInteger.ONE)
                                     .transferPayloadData(contents[1])
                                     .transferDeliveryTag(deliveryTag)
                                     .transfer();

            final Disposition disposition1 = interaction.consume(Disposition.class, Flow.class);
            final UnsignedInteger first = disposition1.getFirst();
            final UnsignedInteger last = disposition1.getLast();

            assertThat(first, anyOf(is(UnsignedInteger.ZERO), is(UnsignedInteger.ONE)));
            assertThat(last, anyOf(nullValue(), is(UnsignedInteger.ZERO), is(UnsignedInteger.ONE)));

            if (last == null || first.equals(last))
            {
                final Disposition disposition2 = interaction.consume(Disposition.class, Flow.class);
                assertThat(disposition2.getFirst(), anyOf(is(UnsignedInteger.ZERO), is(UnsignedInteger.ONE)));
                assertThat(disposition2.getLast(), anyOf(nullValue(), is(UnsignedInteger.ZERO), is(UnsignedInteger.ONE)));
                assertThat(disposition2.getFirst(), is(not(equalTo(first))));
            }
        }
        assertTestQueueMessages(contents);
    }

    @Test
    @SpecificationTest(section = "2.7.5",
            description = "If first, this indicates that the receiver MUST settle the delivery once"
                          + " it has arrived without waiting for the sender to settle first")
    public void transferReceiverSettleModeFirst() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            Disposition responseDisposition = transport.newInteraction()
                                                       .negotiateOpen()
                                                       .begin().consumeResponse(Begin.class)
                                                       .attachRole(Role.SENDER)
                                                       .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                                       .attach().consumeResponse(Attach.class)
                                                       .consumeResponse(Flow.class)
                                                       .assertLatestResponse(Flow.class, this::assumeSufficientCredits)
                                                       .transferPayloadData(getTestName())
                                                       .transferRcvSettleMode(ReceiverSettleMode.FIRST)
                                                       .transfer()
                                                       .consume(Disposition.class, Flow.class);
            assertThat(responseDisposition.getRole(), is(Role.RECEIVER));
            assertThat(responseDisposition.getSettled(), is(Boolean.TRUE));
            assertThat(responseDisposition.getState(), is(instanceOf(Accepted.class)));
        }
        assertThat(Utils.receiveMessage(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(getTestName())));
    }

    @Test
    @SpecificationTest(section = "2.7.5",
            description = "rcv-settle-mode "
                          + "If first, this indicates that the receiver MUST settle the delivery once it has arrived"
                          + " without waiting for the sender to settle first."
                          + " If second, this indicates that the receiver MUST NOT settle until sending its disposition"
                          + " to the sender and receiving a settled disposition from the sender."
                          + " If not set, this value is defaulted to the value negotiated on link attach."
                          + " If the negotiated link value is first, then it is illegal to set this field to second.")
    public void transferReceiverSettleModeCannotBeSecondWhenLinkModeIsFirst() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            Response<?> response = transport.newInteraction()
                                     .negotiateOpen()
                                     .begin().consumeResponse(Begin.class)
                                     .attachRole(Role.SENDER)
                                     .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                     .attachRcvSettleMode(ReceiverSettleMode.FIRST)
                                     .attach().consumeResponse(Attach.class)
                                     .consumeResponse(Flow.class)
                                     .assertLatestResponse(Flow.class, this::assumeSufficientCredits)
                                     .transferPayloadData(getTestName())
                                     .transferRcvSettleMode(ReceiverSettleMode.SECOND)
                                     .transfer()
                                     .consumeResponse()
                                     .getLatestResponse();

            assertThat(response, is(notNullValue()));
            assertThat(response.getBody(), is(notNullValue()));
            assertThat(response.getBody(), is(instanceOf(ErrorCarryingFrameBody.class)));

            final ErrorCarryingFrameBody performative = (ErrorCarryingFrameBody) response.getBody();
            final Error error = performative.getError();
            assertThat(error, is(notNullValue()));
            assumeThat(error.getCondition(), is(not(AmqpError.NOT_IMPLEMENTED)));
            assertThat(error.getCondition(), is(equalTo(AmqpError.INVALID_FIELD)));
        }
    }

    @Test
    @SpecificationTest(section = "2.6.12 Transferring A Message", description = "Pipelined message send")
    public void presettledPipelined() throws Exception
    {
        assumeThat(getBrokerAdmin().isAnonymousSupported(), equalTo(true));
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateProtocol()
                       .open()
                       .begin()
                       .attachRole(Role.SENDER)
                       .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                       .attach()
                       .transferPayloadData(getTestName())
                       .transferSettled(true)
                       .transfer()
                       .close()
                       .sync();

            final byte[] protocolResponse = interaction.consumeResponse().getLatestResponse(byte[].class);
            assertThat(protocolResponse, is(equalTo("AMQP\0\1\0\0".getBytes(UTF_8))));

            interaction.consumeResponse().getLatestResponse(Open.class);
            interaction.consumeResponse().getLatestResponse(Begin.class);
            interaction.consumeResponse().getLatestResponse(Attach.class);
            interaction.consumeResponse().getLatestResponse(Flow.class);
            interaction.consumeResponse().getLatestResponse(Close.class);
        }
        assertThat(Utils.receiveMessage(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(getTestName())));
    }

    @Test
    @SpecificationTest(section = "3.2.1",
            description = "Durable messages MUST NOT be lost even if an intermediary is unexpectedly terminated and "
                          + "restarted. A target which is not capable of fulfilling this guarantee MUST NOT accept messages "
                          + "where the durable header is set to true: if the source allows the rejected outcome then the "
                          + "message SHOULD be rejected with the precondition-failed error, otherwise the link MUST be "
                          + "detached by the receiver with the same error.")
    @BrokerSpecific(kind = BrokerAdmin.KIND_BROKER_J)
    public void durableTransferWithRejectedOutcome() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            MessageEncoder messageEncoder = new MessageEncoder();
            final Header header = new Header();
            header.setDurable(true);
            messageEncoder.setHeader(header);
            messageEncoder.addData(getTestName());
            final Disposition receivedDisposition = transport.newInteraction()
                                                             .negotiateOpen()
                                                             .begin().consumeResponse(Begin.class)
                                                             .attachRole(Role.SENDER)
                                                             .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                                             .attachSourceOutcomes(Accepted.ACCEPTED_SYMBOL,
                                                                                   Rejected.REJECTED_SYMBOL)
                                                             .attach().consumeResponse(Attach.class)
                                                             .consumeResponse(Flow.class)
                                                             .assertLatestResponse(Flow.class, this::assumeSufficientCredits)
                                                             .transferDeliveryId()
                                                             .transferPayload(messageEncoder.getPayload())
                                                             .transferRcvSettleMode(ReceiverSettleMode.FIRST)
                                                             .transfer()
                                                             .consumeResponse()
                                                             .getLatestResponse(Disposition.class);

            assertThat(receivedDisposition.getSettled(), is(true));
            assertThat(receivedDisposition.getState(), is(instanceOf(Outcome.class)));
            if (getBrokerAdmin().supportsRestart())
            {
                assertThat(((Outcome) receivedDisposition.getState()).getSymbol(), is(Accepted.ACCEPTED_SYMBOL));
            }
            else
            {
                assertThat(((Outcome) receivedDisposition.getState()).getSymbol(), is(Rejected.REJECTED_SYMBOL));
            }
        }
    }

    @Test
    @SpecificationTest(section = "3.2.1",
            description = "Durable messages MUST NOT be lost even if an intermediary is unexpectedly terminated and "
                          + "restarted. A target which is not capable of fulfilling this guarantee MUST NOT accept messages "
                          + "where the durable header is set to true: if the source allows the rejected outcome then the "
                          + "message SHOULD be rejected with the precondition-failed error, otherwise the link MUST be "
                          + "detached by the receiver with the same error.")
    @BrokerSpecific(kind = BrokerAdmin.KIND_BROKER_J)
    public void durableTransferWithoutRejectedOutcome() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            MessageEncoder messageEncoder = new MessageEncoder();
            final Header header = new Header();
            header.setDurable(true);
            messageEncoder.setHeader(header);
            messageEncoder.addData(getTestName());
            final Response<?> response = transport.newInteraction()
                                                  .negotiateOpen()
                                                  .begin().consumeResponse(Begin.class)
                                                  .attachRole(Role.SENDER)
                                                  .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                                  .attachSourceOutcomes(Accepted.ACCEPTED_SYMBOL)
                                                  .attach().consumeResponse(Attach.class)
                                                  .consumeResponse(Flow.class)
                                                  .assertLatestResponse(Flow.class, this::assumeSufficientCredits)
                                                  .transferDeliveryId()
                                                  .transferPayload(messageEncoder.getPayload())
                                                  .transferRcvSettleMode(ReceiverSettleMode.FIRST)
                                                  .transfer()
                                                  .consumeResponse()
                                                  .getLatestResponse();

            if (getBrokerAdmin().supportsRestart())
            {
                assertThat(response, is(notNullValue()));
                assertThat(response.getBody(), is(instanceOf(Disposition.class)));
                final Disposition receivedDisposition = (Disposition) response.getBody();
                assertThat(receivedDisposition.getSettled(), is(true));
                assertThat(receivedDisposition.getState(), is(instanceOf(Outcome.class)));
                assertThat(((Outcome) receivedDisposition.getState()).getSymbol(), is(Accepted.ACCEPTED_SYMBOL));
            }
            else
            {
                assertThat(response, is(notNullValue()));
                assertThat(response.getBody(), is(instanceOf(Detach.class)));
                final Detach receivedDetach = (Detach) response.getBody();
                assertThat(receivedDetach.getError(), is(notNullValue()));
                assertThat(receivedDetach.getError().getCondition(), is(AmqpError.PRECONDITION_FAILED));
            }
        }
    }

    @Test
    @SpecificationTest(section = "2.6.12", description = "Transferring A Message.")
    public void receiveTransferUnsettled() throws Exception
    {
        Utils.putMessageOnQueue(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME, getTestName());

        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction()
                                                     .negotiateOpen()
                                                     .begin().consumeResponse()
                                                     .attachRole(Role.RECEIVER)
                                                     .attachSourceAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                                     .attach().consumeResponse()
                                                     .flowIncomingWindow(UnsignedInteger.ONE)
                                                     .flowNextIncomingIdFromPeerLatestSessionBeginAndDeliveryCount()
                                                     .flowOutgoingWindow(UnsignedInteger.ZERO)
                                                     .flowNextOutgoingId(UnsignedInteger.ZERO)
                                                     .flowLinkCredit(UnsignedInteger.ONE)
                                                     .flowHandleFromLinkHandle()
                                                     .flow();

            MessageDecoder messageDecoder = new MessageDecoder();
            boolean hasMore;
            do
            {
                Transfer responseTransfer = interaction.consumeResponse().getLatestResponse(Transfer.class);
                messageDecoder.addTransfer(responseTransfer);
                hasMore = Boolean.TRUE.equals(responseTransfer.getMore());
            }
            while (hasMore);

            Object data = messageDecoder.getData();
            assertThat(data, is(equalTo(getTestName())));
        }
        assertThat(Utils.receiveMessage(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(getTestName())));
    }

    @Test
    @SpecificationTest(section = "2.6.12", description = "Transferring A Message.")
    public void receiveTransferReceiverSettleFirst() throws Exception
    {
        Utils.putMessageOnQueue(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME, getTestName());

        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction()
                                                     .negotiateOpen()
                                                     .begin().consumeResponse()
                                                     .attachRole(Role.RECEIVER)
                                                     .attachSourceAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                                     .attachRcvSettleMode(ReceiverSettleMode.FIRST)
                                                     .attach().consumeResponse()
                                                     .flowIncomingWindow(UnsignedInteger.ONE)
                                                     .flowNextIncomingIdFromPeerLatestSessionBeginAndDeliveryCount()
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
                       .dispositionState(new Accepted())
                       .dispositionRole(Role.RECEIVER)
                       .disposition();

            interaction.detachEndCloseUnconditionally();
        }
    }

    @Test
    @SpecificationTest(section = "2.6.12", description = "Transferring A Message.")
    public void receiveTransferReceiverSettleSecond() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction()
                                                     .negotiateOpen()
                                                     .begin().consumeResponse()
                                                     .attachRole(Role.RECEIVER)
                                                     .attachSourceAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                                     .attachRcvSettleMode(ReceiverSettleMode.SECOND)
                                                     .attach().consumeResponse()
                                                     .assertLatestResponse(this::assumeAttach)
                                                     .assertLatestResponse(Attach.class, this::assumeReceiverSettlesSecond)
                                                     .flowIncomingWindow(UnsignedInteger.ONE)
                                                     .flowNextIncomingIdFromPeerLatestSessionBeginAndDeliveryCount()
                                                     .flowOutgoingWindow(UnsignedInteger.ZERO)
                                                     .flowNextOutgoingId(UnsignedInteger.ZERO)
                                                     .flowLinkCredit(UnsignedInteger.ONE)
                                                     .flowHandleFromLinkHandle()
                                                     .flow();

            Utils.putMessageOnQueue(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME, getTestName());

            Object data = interaction.receiveDelivery()
                                     .decodeLatestDelivery()
                                     .getDecodedLatestDelivery();
            assertThat(data, is(equalTo(getTestName())));

            Disposition disposition = interaction.dispositionSettled(false)
                                                 .dispositionFirstFromLatestDelivery()
                                                 .dispositionRole(Role.RECEIVER)
                                                 .dispositionState(new Accepted())
                                                 .disposition()
                                                 .consume(Disposition.class, Flow.class);
            assertThat(disposition.getSettled(), is(true));

            interaction.dispositionSettled(true)
                       .dispositionFirstFromLatestDelivery()
                       .dispositionRole(Role.RECEIVER)
                       .dispositionState(new Accepted())
                       .disposition();
        }
    }

    @Test
    @SpecificationTest(section = "2.6.12", description = "Transferring A Message.")
    public void receiveTransferReceiverSettleSecondWithRejectedOutcome() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction()
                                                     .negotiateOpen()
                                                     .begin().consumeResponse()
                                                     .attachRole(Role.RECEIVER)
                                                     .attachSourceAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                                     .attachSourceOutcomes(Accepted.ACCEPTED_SYMBOL, Rejected.REJECTED_SYMBOL)
                                                     .attachRcvSettleMode(ReceiverSettleMode.SECOND)
                                                     .attach().consumeResponse()
                                                     .assertLatestResponse(this::assumeAttach)
                                                     .assertLatestResponse(Attach.class, this::assumeReceiverSettlesSecond)
                                                     .flowIncomingWindow(UnsignedInteger.ONE)
                                                     .flowNextIncomingIdFromPeerLatestSessionBeginAndDeliveryCount()
                                                     .flowOutgoingWindow(UnsignedInteger.ZERO)
                                                     .flowNextOutgoingId(UnsignedInteger.ZERO)
                                                     .flowLinkCredit(UnsignedInteger.ONE)
                                                     .flowHandleFromLinkHandle()
                                                     .flow();

            Utils.putMessageOnQueue(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME, getTestName());

            Object data = interaction.receiveDelivery().decodeLatestDelivery().getDecodedLatestDelivery();
            assertThat(data, is(equalTo(getTestName())));

            interaction.dispositionSettled(false)
                       .dispositionFirstFromLatestDelivery()
                       .dispositionRole(Role.RECEIVER)
                       .dispositionState(new Rejected())
                       .disposition()
                       .consumeResponse(Disposition.class, Flow.class);
            Response<?> response = interaction.getLatestResponse();
            if (response.getBody() instanceof Flow)
            {
                interaction.consumeResponse(Disposition.class);
            }

            Disposition disposition = interaction.getLatestResponse(Disposition.class);
            assertThat(disposition.getSettled(), is(true));

            interaction.dispositionSettled(true)
                       .dispositionFirstFromLatestDelivery()
                       .dispositionRole(Role.RECEIVER)
                       .dispositionState(new Rejected())
                       .disposition();

        }
        assertThat(Utils.receiveMessage(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(getTestName())));
    }

    @Ignore
    @Test
    @SpecificationTest(section = "2.6.12", description = "Transferring A Message.")
    public void receiveTransferReceiverSettleSecondWithImplicitDispositionState() throws Exception
    {
        Utils.putMessageOnQueue(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME, getTestName());

        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction()
                                                     .negotiateOpen()
                                                     .begin().consumeResponse()
                                                     .attachRole(Role.RECEIVER)
                                                     .attachSourceAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                                     .attachRcvSettleMode(ReceiverSettleMode.SECOND)
                                                     .attachSourceOutcomes()
                                                     .attachSourceDefaultOutcome(null)
                                                     .attach().consumeResponse()
                                                     .assertLatestResponse(Attach.class, this::assumeReceiverSettlesSecond)
                                                     .flowIncomingWindow(UnsignedInteger.ONE)
                                                     .flowNextIncomingIdFromPeerLatestSessionBeginAndDeliveryCount()
                                                     .flowOutgoingWindow(UnsignedInteger.ZERO)
                                                     .flowNextOutgoingId(UnsignedInteger.ZERO)
                                                     .flowLinkCredit(UnsignedInteger.ONE)
                                                     .flowHandleFromLinkHandle()
                                                     .flow()
                                                     .receiveDelivery()
                                                     .decodeLatestDelivery();

            Object data = interaction.getDecodedLatestDelivery();
            assertThat(data, is(equalTo(getTestName())));

            Disposition disposition = interaction.dispositionSettled(false)
                                                 .dispositionFirstFromLatestDelivery()
                                                 .dispositionRole(Role.RECEIVER)
                                                 .dispositionState(null)
                                                 .disposition()
                                                 .consume(Disposition.class, Flow.class);
            assertThat(disposition.getSettled(), is(true));

            interaction.consumeResponse(null, Flow.class);

        }
        assertThat(Utils.receiveMessage(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(getTestName())));
    }

    @Test
    @SpecificationTest(section = "2.6.12", description = "[...] the receiving application MAY wish to indicate"
                                                         + " non-terminal delivery states to the sender")
    public void receiveTransferReceiverIndicatesNonTerminalDeliveryState() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();

            Open open = interaction.openMaxFrameSize(UnsignedInteger.valueOf(4096))
                                   .negotiateOpen()
                                   .getLatestResponse(Open.class);
            interaction.begin().consumeResponse()
                       .attachRole(Role.RECEIVER)
                       .attachSourceAddress(BrokerAdmin.TEST_QUEUE_NAME)
                       .attachRcvSettleMode(ReceiverSettleMode.SECOND)
                       .attach().consumeResponse()
                       .assertLatestResponse(this::assumeAttach)
                       .assertLatestResponse(Attach.class, this::assumeReceiverSettlesSecond)
                       .flowIncomingWindow(UnsignedInteger.ONE)
                       .flowNextIncomingIdFromPeerLatestSessionBeginAndDeliveryCount()
                       .flowOutgoingWindow(UnsignedInteger.ZERO)
                       .flowNextOutgoingId(UnsignedInteger.ZERO)
                       .flowLinkCredit(UnsignedInteger.ONE)
                       .flowHandleFromLinkHandle()
                       .flow()
                       .sync();

            final int negotiatedFrameSize = open.getMaxFrameSize().intValue();
            final String testMessageData = Stream.generate(() -> "*").limit(negotiatedFrameSize).collect(Collectors.joining());

            Utils.putMessageOnQueue(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME, testMessageData);

            MessageDecoder messageDecoder = new MessageDecoder();

            Transfer first = interaction.consumeResponse(Transfer.class)
                                        .getLatestResponse(Transfer.class);
            assertThat(first.getMore(), is(equalTo(true)));
            messageDecoder.addTransfer(first);

            final long firstRemaining;
            try (QpidByteBuffer payload = first.getPayload())
            {
                firstRemaining = payload.remaining();
            }

            Received state = new Received();
            state.setSectionNumber(UnsignedInteger.ZERO);
            state.setSectionOffset(UnsignedLong.valueOf(firstRemaining + 1));

            interaction.dispositionSettled(false)
                       .dispositionRole(Role.RECEIVER)
                       .dispositionState(state)
                       .disposition()
                       .sync();

            Transfer second = interaction.consumeResponse(Transfer.class)
                                         .getLatestResponse(Transfer.class);
            assertThat(second.getMore(), oneOf(false, null));
            messageDecoder.addTransfer(second);

            assertThat(messageDecoder.getData(), is(equalTo(testMessageData)));

            interaction.dispositionSettled(true)
                       .dispositionRole(Role.RECEIVER)
                       .dispositionState(new Accepted())
                       .disposition().sync();
        }
    }

    @Test
    @SpecificationTest(section = "2.7.3", description = "The sender SHOULD respect the receiver’s desired settlement mode if"
                                                        + " the receiver initiates the attach exchange and the sender supports the desired mode.")
    public void receiveTransferSenderSettleModeSettled() throws Exception
    {
        Utils.putMessageOnQueue(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME, getTestName());

        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction()
                                                     .negotiateOpen()
                                                     .begin().consumeResponse()
                                                     .attachRole(Role.RECEIVER)
                                                     .attachSourceAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                                     .attachSndSettleMode(SenderSettleMode.SETTLED)
                                                     .attach().consumeResponse(Attach.class);
            Attach attach = interaction.getLatestResponse(Attach.class);
            assumeThat(attach.getSndSettleMode(), is(equalTo(SenderSettleMode.SETTLED)));

            interaction.flowIncomingWindow(UnsignedInteger.ONE)
                       .flowNextIncomingIdFromPeerLatestSessionBeginAndDeliveryCount()
                       .flowOutgoingWindow(UnsignedInteger.ZERO)
                       .flowNextOutgoingId(UnsignedInteger.ZERO)
                       .flowLinkCredit(UnsignedInteger.ONE)
                       .flowHandleFromLinkHandle()
                       .flow();

            final List<Transfer> transfers = interaction.receiveDelivery().getLatestDelivery();
            final AtomicBoolean isSettled = new AtomicBoolean();
            transfers.forEach(transfer -> { if (Boolean.TRUE.equals(transfer.getSettled())) { isSettled.set(true);}});

            assertThat(isSettled.get(), is(true));

            interaction.detachEndCloseUnconditionally();
        }

        Utils.putMessageOnQueue(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME, "test");
        assertThat(Utils.receiveMessage(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME), is(equalTo("test")));
    }

    @Test
    @SpecificationTest(section = "2.7.5",
            description = "[delivery-tag] uniquely identifies the delivery attempt for a given message on this link.")
    @Ignore("QPID-8346: test relies on receiver-settle-mode=second which is broken")
    public void transfersWithDuplicateUnsettledDeliveryTag() throws Exception
    {
        String content1 = getTestName() + "_1";
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Binary deliveryTag = new Binary("testDeliveryTag".getBytes(UTF_8));

            Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .begin()
                       .consumeResponse(Begin.class)
                       .attachRole(Role.SENDER)
                       .attachRcvSettleMode(ReceiverSettleMode.SECOND)
                       .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                       .attach()
                       .consumeResponse(Attach.class)
                       .assertLatestResponse(Attach.class, this::assumeReceiverSettlesSecond)
                       .consumeResponse(Flow.class)
                       .assertLatestResponse(Flow.class, this::assumeCreditsGreaterThanOne)
                       .transferDeliveryId()
                       .transferDeliveryTag(deliveryTag)
                       .transferPayloadData(content1)
                       .transfer()
                       .transferDeliveryTag(deliveryTag)
                       .transferDeliveryId()
                       .transferPayloadData(getTestName() + "_2")
                       .transfer()
                       .sync();

            do
            {
                interaction.consumeResponse();
                Response<?> response = interaction.getLatestResponse();
                assertThat(response, is(notNullValue()));

                Object body = response.getBody();
                if (body instanceof ErrorCarryingFrameBody)
                {
                    Error error = ((ErrorCarryingFrameBody) body).getError();
                    assertThat(error, is(notNullValue()));
                    break;
                }
                else if (body instanceof Disposition)
                {
                    Disposition disposition = (Disposition) body;
                    assertThat(disposition.getSettled(), is(equalTo(false)));
                    assertThat(disposition.getFirst(), is(not(equalTo(UnsignedInteger.ONE))));
                    assertThat(disposition.getLast(), is(not(equalTo(UnsignedInteger.ONE))));
                }
                else if (!(body instanceof Flow))
                {
                    fail("Unexpected response " + body);
                }
            } while (true);
        }
    }

    @Test
    @SpecificationTest(section = "2.6.12",
            description = "The delivery-tag MUST be unique amongst all deliveries that"
                          + " could be considered unsettled by either end of the link.")
    public void deliveryTagCanBeReusedAfterDeliveryIsSettled() throws Exception
    {
        final String[] contents = Utils.createTestMessageContents(2, getTestName());
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Binary deliveryTag = new Binary("testDeliveryTag".getBytes(UTF_8));

            Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .begin()
                       .consumeResponse(Begin.class)
                       .attachRole(Role.SENDER)
                       .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                       .attach()
                       .consumeResponse(Attach.class)
                       .consumeResponse(Flow.class)
                       .assertLatestResponse(Flow.class, this::assumeCreditsGreaterThanOne)
                       .transferDeliveryId(UnsignedInteger.ZERO)
                       .transferDeliveryTag(deliveryTag)
                       .transferPayloadData(contents[0])
                       .transferSettled(true)
                       .transfer()
                       .sync()

                       .transferDeliveryTag(deliveryTag)
                       .transferDeliveryId(UnsignedInteger.ONE)
                       .transferPayloadData(contents[1])
                       .transfer()
                       .sync();

            interaction.closeUnconditionally();

        }
        assertTestQueueMessages(contents);
    }

    @Test
    @SpecificationTest(section = "2.7.3",
            description = "max-message-size: This field indicates the maximum message size supported by the link"
                          + " endpoint. Any attempt to deliver a message larger than this results in a"
                          + " message-size-exceeded link-error. If this field is zero or unset, there is no maximum"
                          + " size imposed by the link endpoint.")
    public void exceedMaxMessageSizeLimit() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Binary deliveryTag = new Binary("testDeliveryTag".getBytes(UTF_8));

            Interaction interaction = transport.newInteraction();
            Open open = interaction.negotiateOpen()
                                   .getLatestResponse(Open.class);

            long maxFrameSize = open.getMaxFrameSize() == null ? Integer.MAX_VALUE : open.getMaxFrameSize().longValue();

            Attach attach = interaction.begin().consumeResponse(Begin.class)
                                       .attachRole(Role.SENDER)
                                       .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                       .attach().consumeResponse(Attach.class)
                                       .getLatestResponse(Attach.class);

            final UnsignedLong maxMessageSizeLimit = attach.getMaxMessageSize();
            assumeThat(maxMessageSizeLimit, is(notNullValue()));
            assumeThat(maxMessageSizeLimit.longValue(),
                       is(both(greaterThan(0L)).and(lessThan(MAX_MAX_MESSAGE_SIZE_WE_ARE_WILLING_TO_TEST))));

            Flow flow = interaction.consumeResponse(Flow.class)
                                   .getLatestResponse(Flow.class);
            assertThat(flow.getLinkCredit().intValue(), is(greaterThan(1)));

            final long chunkSize = Math.min(1024 * 1024, maxFrameSize - 100);
            byte[] payloadChunk = createTestPayload(chunkSize);
            interaction.transferDeliveryId(UnsignedInteger.ZERO)
                       .transferDeliveryTag(deliveryTag)
                       .transferPayloadData(payloadChunk)
                       .transferSettled(true)
                       .transferMore(true);
            int payloadSize = 0;
            while (payloadSize < maxMessageSizeLimit.longValue())
            {
                payloadSize += chunkSize;
                interaction.transfer().sync();
            }

            while (true)
            {
                Response<?> response = interaction.consumeResponse(Flow.class, Disposition.class, Detach.class).getLatestResponse();
                if (response != null)
                {
                    if (response.getBody() instanceof Detach)
                    {
                        break;
                    }
                    else if (response.getBody() instanceof Disposition)
                    {
                        assertThat(((Disposition) response.getBody()).getState(), is(instanceOf(Rejected.class)));
                        assertThat(((Rejected) ((Disposition) response.getBody()).getState()).getError(), is(notNullValue()));
                        assertThat(((Rejected) ((Disposition) response.getBody()).getState()).getError().getCondition(), is(equalTo(LinkError.MESSAGE_SIZE_EXCEEDED)));
                    }
                }
            }
            Detach detach = interaction.getLatestResponse(Detach.class);

            assertThat(detach.getError(), is(notNullValue()));
            assertThat(detach.getError().getCondition(), is(equalTo(LinkError.MESSAGE_SIZE_EXCEEDED)));
        }
    }

    @Test
    @SpecificationTest(section = "2.6.12", description = "Transferring A Message.")
    public void transferMultipleDeliveries() throws Exception
    {
        final String[] contents = Utils.createTestMessageContents(3, getTestName());
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction()
                                                     .negotiateOpen()
                                                     .begin().consumeResponse(Begin.class)
                                                     .attachRole(Role.SENDER)
                                                     .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                                     .attach().consumeResponse(Attach.class)
                                                     .consumeResponse(Flow.class);
            Flow flow = interaction.getLatestResponse(Flow.class);
            assumeThat("insufficient credit for the test", flow.getLinkCredit().intValue(), is(greaterThan(2)));

            interaction.transferDeliveryId(UnsignedInteger.ZERO)
                       .transferDeliveryTag(new Binary("A".getBytes(StandardCharsets.UTF_8)))
                       .transferPayloadData(contents[0])
                       .transfer()
                       .transferDeliveryId(UnsignedInteger.ONE)
                       .transferDeliveryTag(new Binary("B".getBytes(StandardCharsets.UTF_8)))
                       .transferPayloadData(contents[1])
                       .transfer()
                       .transferDeliveryId(UnsignedInteger.valueOf(2))
                       .transferDeliveryTag(new Binary("C".getBytes(StandardCharsets.UTF_8)))
                       .transferPayloadData(contents[2])
                       .transfer();

            TreeSet<UnsignedInteger> expectedDeliveryIds = Sets.newTreeSet(Arrays.asList(UnsignedInteger.ZERO,
                                                                                         UnsignedInteger.ONE,
                                                                                         UnsignedInteger.valueOf(2)));
            assertDeliveries(interaction, expectedDeliveryIds);

            // verify that no unexpected performative is received by closing
            interaction.doCloseConnection();
        }
        assertTestQueueMessages(contents);
    }


    @Test
    @SpecificationTest(section = "2.6.12", description = "Transferring A Message.")
    public void transferMixtureOfTransactionalAndNonTransactionalDeliveries() throws Exception
    {
        final String[] contents = Utils.createTestMessageContents(3, getTestName());
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction().negotiateOpen()
                                                     .begin().consumeResponse(Begin.class)
                                                     .attachRole(Role.SENDER)
                                                     .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                                     .attach().consumeResponse(Attach.class)
                                                     .consumeResponse(Flow.class);

            Flow flow = interaction.getLatestResponse(Flow.class);
            assumeThat("insufficient credit for the test", flow.getLinkCredit().intValue(), is(greaterThan(2)));

            interaction.txnAttachCoordinatorLink(UnsignedInteger.ONE, this::coordinatorAttachExpected)
                       .txnDeclare();

            interaction.transferDeliveryId()
                       .transferDeliveryTag(new Binary("A".getBytes(StandardCharsets.UTF_8)))
                       .transferPayloadData(contents[0])
                       .transfer()
                       .transferDeliveryId()
                       .transferDeliveryTag(new Binary("B".getBytes(StandardCharsets.UTF_8)))
                       .transferPayloadData(contents[1])
                       .transfer()
                       .transferDeliveryId()
                       .transferDeliveryTag(new Binary("C".getBytes(StandardCharsets.UTF_8)))
                       .transferTransactionalStateFromCurrentTransaction()
                       .transferPayloadData(contents[2])
                       .transfer();

            interaction.txnSendDischarge(false);

            assertDeliveries(interaction, Sets.newTreeSet(Arrays.asList(UnsignedInteger.ONE,
                                                                        UnsignedInteger.valueOf(2),
                                                                        UnsignedInteger.valueOf(3),
                                                                        UnsignedInteger.valueOf(4))));
        }
        assertTestQueueMessages(contents);
    }

    @Test
    @SpecificationTest(section = "2.6.12", description = "Transferring A Message.")
    public void receiveMultipleDeliveries() throws Exception
    {
        final int numberOfMessages = 4;
        final String[] contents = Utils.createTestMessageContents(numberOfMessages, getTestName());
        Utils.putMessageOnQueue(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME, contents);

        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction()
                                                     .negotiateOpen()
                                                     .begin().consumeResponse()
                                                     .attachRole(Role.RECEIVER)
                                                     .attachSourceAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                                     .attachRcvSettleMode(ReceiverSettleMode.FIRST)
                                                     .attach().consumeResponse()
                                                     .flowIncomingWindow(UnsignedInteger.valueOf(numberOfMessages))
                                                     .flowNextIncomingIdFromPeerLatestSessionBeginAndDeliveryCount()
                                                     .flowOutgoingWindow(UnsignedInteger.ZERO)
                                                     .flowNextOutgoingId(UnsignedInteger.ZERO)
                                                     .flowLinkCredit(UnsignedInteger.valueOf(numberOfMessages))
                                                     .flowHandleFromLinkHandle()
                                                     .flow();

            UnsignedInteger firstDeliveryId = null;
            for (final String content : contents)
            {
                interaction.receiveDelivery(Flow.class).decodeLatestDelivery();
                Object data = interaction.getDecodedLatestDelivery();
                assertThat(data, is(equalTo(content)));
                if (firstDeliveryId == null)
                {
                    firstDeliveryId = interaction.getLatestDeliveryId();
                }
            }

            interaction.dispositionSettled(true)
                       .dispositionRole(Role.RECEIVER)
                       .dispositionFirst(firstDeliveryId)
                       .dispositionLast(interaction.getLatestDeliveryId())
                       .dispositionState(new Accepted())
                       .disposition();
            interaction.detachEndCloseUnconditionally();
        }

        final String messageText = getTestName() + "_" + 4;
        Utils.putMessageOnQueue(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME, messageText);
        Object receivedMessage = Utils.receiveMessage(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME);
        assertThat(receivedMessage, is(equalTo(messageText)));
    }

    @Test
    @SpecificationTest(section = "2.6.12", description = "Transferring A Message.")
    public void receiveMixtureOfTransactionalAndNonTransactionalDeliveries() throws Exception
    {
        final int numberOfMessages = 4;
        final String[] contents = Utils.createTestMessageContents(numberOfMessages, getTestName());

        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction()
                                                     .negotiateOpen()
                                                     .begin().consumeResponse()
                                                     .attachRole(Role.RECEIVER)
                                                     .attachSourceAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                                     .attachRcvSettleMode(ReceiverSettleMode.FIRST)
                                                     .attachHandle(UnsignedInteger.ZERO)
                                                     .attach().consumeResponse()
                                                     .flowIncomingWindow(UnsignedInteger.valueOf(numberOfMessages))
                                                     .flowNextIncomingIdFromPeerLatestSessionBeginAndDeliveryCount()
                                                     .flowOutgoingWindow(UnsignedInteger.ZERO)
                                                     .flowNextOutgoingId(UnsignedInteger.ZERO)
                                                     .flowLinkCredit(UnsignedInteger.valueOf(numberOfMessages))
                                                     .flowHandleFromLinkHandle()
                                                     .flow();

            Utils.putMessageOnQueue(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME, contents);

            final List<UnsignedInteger> deliveryIds = new ArrayList<>();
            for (final String content : contents)
            {
                interaction.receiveDelivery(Flow.class).decodeLatestDelivery();
                Object data = interaction.getDecodedLatestDelivery();
                assertThat(data, is(equalTo(content)));
                deliveryIds.add(interaction.getLatestDeliveryId());
            }

            interaction.txnAttachCoordinatorLink(UnsignedInteger.ONE, this::coordinatorAttachExpected)
                       .txnDeclare();

            interaction.dispositionSettled(true)
                       .dispositionRole(Role.RECEIVER)
                       .dispositionFirst(deliveryIds.get(0))
                       .dispositionLast(deliveryIds.get(1))
                       .dispositionState(new Accepted())
                       .disposition()
                       .dispositionSettled(true)
                       .dispositionRole(Role.RECEIVER)
                       .dispositionFirst(deliveryIds.get(2))
                       .dispositionLast(deliveryIds.get(3))
                       .dispositionTransactionalStateFromCurrentTransaction(new Accepted())
                       .disposition();

            interaction.txnDischarge(false);
        }

        String messageText = getTestName() + "_" + 4;
        Utils.putMessageOnQueue(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME, messageText);
        Object receivedMessage = Utils.receiveMessage(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME);
        assertThat(receivedMessage, is(equalTo(messageText)));
    }

    private void assertDeliveries(final Interaction interaction, final TreeSet<UnsignedInteger> expectedDeliveryIds)
            throws Exception
    {
        do
        {
            Disposition disposition = interaction.consume(Disposition.class, Flow.class);
            LongStream.rangeClosed(disposition.getFirst().longValue(),
                                   disposition.getLast() == null
                                           ? disposition.getFirst().longValue()
                                           : disposition.getLast().longValue())
                      .forEach(value -> {
                          UnsignedInteger deliveryId = expectedDeliveryIds.first();
                          assertThat(value, is(equalTo(deliveryId.longValue())));
                          expectedDeliveryIds.remove(deliveryId);
                      });
        }
        while (!expectedDeliveryIds.isEmpty());
    }

    private byte[] createTestPayload(final long payloadSize)
    {
        if (payloadSize > 1024*1024*1024)
        {
            throw new IllegalArgumentException(String.format("Payload size (%.2f MB) too big", payloadSize / (1024. * 1024.)));
        }
        return new byte[(int) payloadSize];
    }

    private void assertTestQueueMessages(final String[] contents) throws Exception
    {
        for (final String content : contents)
        {
            assertThat(Utils.receiveMessage(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(content)));
        }
    }

    private void assumeSufficientCredits(final Flow flow)
    {
        assumeThat(flow.getLinkCredit(), is(notNullValue()));
        assumeThat(flow.getLinkCredit(), is(greaterThan(UnsignedInteger.ZERO)));
    }

    private void assumeCreditsGreaterThanOne(final Flow flow)
    {
        assumeThat(flow.getLinkCredit(), is(notNullValue()));
        assumeThat(flow.getLinkCredit(), is(greaterThan(UnsignedInteger.ONE)));
    }

    private void assumeReceiverSettlesSecond(final Attach attach)
    {
        assumeThat(attach.getRcvSettleMode(), is(equalTo(ReceiverSettleMode.SECOND)));
    }

    private void coordinatorAttachExpected(final Response<?> response)
    {
        assertThat(response, is(notNullValue()));
        assumeThat(response.getBody(), anyOf(instanceOf(Attach.class), instanceOf(Flow.class)));
    }

    private void assumeAttach(final Response<?> response)
    {
        assertThat(response, notNullValue());
        assumeThat(response.getBody(), is(instanceOf(Attach.class)));
    }

}
