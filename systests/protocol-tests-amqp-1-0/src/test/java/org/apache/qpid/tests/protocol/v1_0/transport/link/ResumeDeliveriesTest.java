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

package org.apache.qpid.tests.protocol.v1_0.transport.link;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.typeCompatibleWith;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;
import static org.junit.Assume.assumeTrue;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.DeliveryState;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Accepted;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Disposition;
import org.apache.qpid.server.protocol.v1_0.type.transport.End;
import org.apache.qpid.server.protocol.v1_0.type.transport.Error;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.Open;
import org.apache.qpid.server.protocol.v1_0.type.transport.ReceiverSettleMode;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.server.protocol.v1_0.type.transport.SenderSettleMode;
import org.apache.qpid.server.protocol.v1_0.type.transport.Transfer;
import org.apache.qpid.tests.protocol.Response;
import org.apache.qpid.tests.protocol.SpecificationTest;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.protocol.v1_0.Interaction;
import org.apache.qpid.tests.protocol.v1_0.Utils;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;
import org.apache.qpid.tests.utils.ConfigItem;

@ConfigItem(name = "qpid.tests.mms.messagestore.persistence", value = "false", jvm = true)
public class ResumeDeliveriesTest extends BrokerAdminUsingTestBase
{
    private static final int MIN_MAX_FRAME_SIZE = 512;

    @Before
    public void setUp()
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
    }

    @Ignore("QPID-7845")
    @Test
    @SpecificationTest(section = "2.6.13",
            description = "When a suspended link having unsettled deliveries is resumed, the unsettled field from the"
                          + " attach frame will carry the delivery-tags and delivery state of all deliveries"
                          + " considered unsettled by the issuing link endpoint.")
    public void resumeSendingLinkSingleUnsettledDelivery() throws Exception
    {
        final String destination = BrokerAdmin.TEST_QUEUE_NAME;
        final Binary deliveryTag = new Binary("testDeliveryTag".getBytes(StandardCharsets.UTF_8));
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {

            final UnsignedInteger linkHandle1 = UnsignedInteger.ZERO;
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .begin().consumeResponse(Begin.class);

            // 1. attach with ReceiverSettleMode.SECOND
            interaction.attachHandle(linkHandle1)
                       .attachRole(Role.SENDER)
                       .attachRcvSettleMode(ReceiverSettleMode.SECOND)
                       .attachTargetAddress(destination)
                       .attach().consumeResponse(Attach.class)
                       .assertLatestResponse(Attach.class, this::assumeReceiverSettlesSecond)
                       .consumeResponse(Flow.class);

            // 2. send a unsettled delivery
            final Disposition responseDisposition = interaction.transferHandle(linkHandle1)
                                                               .transferDeliveryId(UnsignedInteger.ZERO)
                                                               .transferDeliveryTag(deliveryTag)
                                                               .transferPayloadData(getTestName())
                                                               .transfer()
                                                               .consumeResponse()
                                                               .getLatestResponse(Disposition.class);
            assertThat(responseDisposition.getRole(), is(Role.RECEIVER));
            assertThat(responseDisposition.getSettled(), is(Boolean.FALSE));
            final DeliveryState remoteDeliveryState = responseDisposition.getState();

            // 3. detach the link
            interaction.detach().consumeResponse(Detach.class);

            // 4. resume the link
            final UnsignedInteger linkHandle2 = UnsignedInteger.ONE;
            final Attach responseAttach2 = interaction.attachHandle(linkHandle2)
                                                      .attachUnsettled(Collections.singletonMap(deliveryTag, null))
                                                      .attach().consumeResponse()
                                                      .getLatestResponse(Attach.class);

            // 5. assert content of unsettled map
            assertThat(responseAttach2.getTarget(), is(notNullValue()));
            final Map<Binary, DeliveryState> remoteUnsettled = responseAttach2.getUnsettled();
            assertThat(remoteUnsettled, is(notNullValue()));
            assertThat(remoteUnsettled.keySet(), is(equalTo(Collections.singleton(deliveryTag))));
            assertThat(remoteUnsettled.get(deliveryTag).getClass(), typeCompatibleWith(remoteDeliveryState.getClass()));
            assertThat(responseAttach2.getIncompleteUnsettled(), is(anyOf(nullValue(), equalTo(false))));
        }
    }

    @Ignore("QPID-7845")
    @Test
    @SpecificationTest(section = "2.7.3",
            description = "If the local unsettled map is too large to be encoded within a frame of the agreed maximum"
                          + " frame size then the session MAY be ended with the frame-size-too-small error. The"
                          + " endpoint SHOULD make use of the ability to send an incomplete unsettled map (see below)"
                          + " to avoid sending an error.")
    public void resumeSendingLinkWithIncompleteUnsettled() throws Exception
    {
        final String destination = BrokerAdmin.TEST_QUEUE_NAME;
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();

            // 0. Open connection with small max-frame-size
            final Open open = interaction.openMaxFrameSize(UnsignedInteger.valueOf(MIN_MAX_FRAME_SIZE))
                                         .negotiateOpen()
                                         .getLatestResponse(Open.class);
            interaction.begin().consumeResponse(Begin.class);

            // 1. attach with ReceiverSettleMode.SECOND
            final UnsignedInteger linkHandle1 = UnsignedInteger.ZERO;
            interaction.attachHandle(linkHandle1)
                       .attachRole(Role.SENDER)
                       .attachRcvSettleMode(ReceiverSettleMode.SECOND)
                       .attachTargetAddress(destination)
                       .attach().consumeResponse(Attach.class)
                       .assertLatestResponse(Attach.class, this::assumeReceiverSettlesSecond)
                       .consumeResponse(Flow.class);

            // 2. send enough unsettled deliveries to cause incomplete-unsettled to be true
            //    assume each delivery requires at least 1 byte, therefore max-frame-size deliveries should be enough
            interaction.transferHandle(linkHandle1)
                       .transferPayloadData(getTestName());
            Map<Binary, DeliveryState> localUnsettled = new HashMap<>(open.getMaxFrameSize().intValue());
            for (int i = 0; i < open.getMaxFrameSize().intValue(); ++i)
            {
                final Binary deliveryTag = new Binary(String.valueOf(i).getBytes(StandardCharsets.UTF_8));
                final Disposition responseDisposition = interaction.transferDeliveryId(UnsignedInteger.valueOf(i))
                                                                   .transferDeliveryTag(deliveryTag)
                                                                   .transfer()
                                                                   .consumeResponse(Disposition.class)
                                                                   .getLatestResponse(Disposition.class);
                assertThat(responseDisposition.getRole(), is(Role.RECEIVER));
                assertThat(responseDisposition.getSettled(), is(Boolean.FALSE));
                localUnsettled.put(deliveryTag, null);
            }

            // 3. detach the link
            interaction.detach().consumeResponse(Detach.class);

            // 4. resume the link
            final UnsignedInteger linkHandle2 = UnsignedInteger.ONE;
            final Binary sampleLocalUnsettled = localUnsettled.keySet().iterator().next();
            Map<Binary, DeliveryState> unsettled = Collections.singletonMap(sampleLocalUnsettled,
                                                                            localUnsettled.get(sampleLocalUnsettled));
            final Response<?> latestResponse = interaction.attachHandle(linkHandle2)
                                                          .attachUnsettled(unsettled)
                                                          .attachIncompleteUnsettled(true)
                                                          .attach().consumeResponse(End.class, Attach.class)
                                                          .getLatestResponse();

            if (latestResponse.getBody() instanceof End)
            {
                // 5.a assert session end error
                final End responseEnd = (End) latestResponse.getBody();
                final Error error = responseEnd.getError();
                assertThat(error, is(notNullValue()));
                assertThat(error.getCondition().getValue(), is(equalTo(AmqpError.FRAME_SIZE_TOO_SMALL)));
                assumeTrue("Broker does not support incomplete unsettled",  false);
            }
            else if (latestResponse.getBody() instanceof Attach)
            {
                // 5.b assert content of unsettled map
                final Attach responseAttach2 = (Attach) latestResponse.getBody();
                assertThat(responseAttach2.getTarget(), is(notNullValue()));
                final Map<Binary, DeliveryState> remoteUnsettled = responseAttach2.getUnsettled();
                assertThat(remoteUnsettled, is(notNullValue()));
                assertThat(remoteUnsettled.keySet(), is(not(empty())));
                for (Binary deliveryTag : remoteUnsettled.keySet())
                {
                    assertThat(deliveryTag, in(localUnsettled.keySet()));
                }
                assertThat(responseAttach2.getIncompleteUnsettled(), is(equalTo(true)));
            }
            else
            {
                fail(String.format("Unexpected response. Expected End or Attach. Got '%s'.", latestResponse.getBody()));
            }
        }
    }

    @Ignore("QPID-7845")
    @Test
    @SpecificationTest(section = "2.7.3", description =
            "If set to true [incomplete-unsettled] indicates that the unsettled map provided is not complete. "
            + "When the map is incomplete the recipient of the map cannot take the absence of a delivery tag from "
            + "the map as evidence of settlement. On receipt of an incomplete unsettled map a sending endpoint MUST "
            + "NOT send any new deliveries (i.e. deliveries where resume is not set to true) to its partner (and "
            + "a receiving endpoint which sent an incomplete unsettled map MUST detach with an error on "
            + "receiving a transfer which does not have the resume flag set to true).")
    public void rejectNewDeliveryWhilstUnsettledIncomplete() throws Exception
    {
        final String destination = BrokerAdmin.TEST_QUEUE_NAME;
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();

            // 0. Open connection with small max-frame-size
            final Open open = interaction.openMaxFrameSize(UnsignedInteger.valueOf(MIN_MAX_FRAME_SIZE))
                                         .negotiateOpen()
                                         .getLatestResponse(Open.class);
            interaction.begin().consumeResponse(Begin.class);

            // 1. attach with ReceiverSettleMode.SECOND
            final UnsignedInteger linkHandle1 = UnsignedInteger.ZERO;
            interaction.attachHandle(linkHandle1)
                       .attachRole(Role.SENDER)
                       .attachRcvSettleMode(ReceiverSettleMode.SECOND)
                       .attachTargetAddress(destination)
                       .attach().consumeResponse(Attach.class)
                       .assertLatestResponse(Attach.class, this::assumeReceiverSettlesSecond)
                       .consumeResponse(Flow.class);

            // 2. send enough unsettled deliverys to cause incomplete-unsettled to be true
            //    assume each delivery requires at least 1 byte, therefore max-frame-size deliveries should be enough
            interaction.transferHandle(linkHandle1)
                       .transferPayloadData(getTestName());
            Map<Binary, DeliveryState> localUnsettled = new HashMap<>(open.getMaxFrameSize().intValue());
            for (int i = 0; i < open.getMaxFrameSize().intValue(); ++i)
            {
                final Binary deliveryTag = new Binary(String.valueOf(i).getBytes(StandardCharsets.UTF_8));
                final Disposition responseDisposition = interaction.transferDeliveryId(UnsignedInteger.valueOf(i))
                                                                   .transferDeliveryTag(deliveryTag)
                                                                   .transfer()
                                                                   .consumeResponse(Disposition.class)
                                                                   .getLatestResponse(Disposition.class);
                assertThat(responseDisposition.getRole(), is(Role.RECEIVER));
                assertThat(responseDisposition.getSettled(), is(Boolean.FALSE));
                localUnsettled.put(deliveryTag, null);
            }

            // 3. detach the link
            interaction.detach().consumeResponse(Detach.class);

            // 4. resume the link
            final UnsignedInteger linkHandle2 = UnsignedInteger.ONE;
            final Binary sampleLocalUnsettled = localUnsettled.keySet().iterator().next();
            Map<Binary, DeliveryState> unsettled = Collections.singletonMap(sampleLocalUnsettled,
                                                                            localUnsettled.get(sampleLocalUnsettled));
            final Response<?> latestResponse = interaction.attachHandle(linkHandle2)
                                                          .attachUnsettled(unsettled)
                                                          .attachIncompleteUnsettled(true)
                                                          .attach().consumeResponse(End.class, Attach.class)
                                                          .getLatestResponse();
            assumeThat(latestResponse.getBody(), is(instanceOf(Attach.class)));

            // 5. ensure attach has incomplete-unsettled
            final Attach responseAttach = (Attach) latestResponse.getBody();
            assertThat(responseAttach.getIncompleteUnsettled(), is(equalTo(true)));

            // 6. send new transfer
            final Binary newDeliveryTag = new Binary("newTransfer".getBytes(StandardCharsets.UTF_8));
            final Detach detachWithError = interaction.transferHandle(linkHandle2)
                                                      .transferDeliveryId(UnsignedInteger.ONE)
                                                      .transferDeliveryTag(newDeliveryTag)
                                                      .transfer().consumeResponse()
                                                      .getLatestResponse(Detach.class);
            assertThat(detachWithError.getError(), is(notNullValue()));
            final Error detachError = detachWithError.getError();
            assertThat(detachError.getCondition(), is(equalTo(AmqpError.ILLEGAL_STATE)));
        }
    }

    @Ignore("QPID-7845")
    @Test
    @SpecificationTest(section = "2.7.3", description =
            "If set to true [incomplete-unsettled] indicates that the unsettled map provided is not complete. "
            + "When the map is incomplete the recipient of the map cannot take the absence of a delivery tag from "
            + "the map as evidence of settlement. On receipt of an incomplete unsettled map a sending endpoint MUST "
            + "NOT send any new deliveries (i.e. deliveries where resume is not set to true) to its partner (and "
            + "a receiving endpoint which sent an incomplete unsettled map MUST detach with an error on "
            + "receiving a transfer which does not have the resume flag set to true).")
    public void incompleteUnsettledReceiving() throws Exception
    {
        for (int i = 0; i < MIN_MAX_FRAME_SIZE; i++)
        {
            Utils.putMessageOnQueue(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME, getTestName() + "-" + i);
        }

        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            // 1. open with small max-frame=512, begin, attach receiver with
            //    with rcv-settle-mode=second, snd-settle-mode=unsettled,
            //    flow with incoming-window=MAX_INTEGER and link-credit=MAX_INTEGER
            final Interaction interaction = transport.newInteraction();
            interaction.openMaxFrameSize(UnsignedInteger.valueOf(MIN_MAX_FRAME_SIZE))
                       .negotiateOpen()
                       .begin()
                       .consumeResponse(Begin.class)
                       .attachRole(Role.RECEIVER)
                       .attachRcvSettleMode(ReceiverSettleMode.SECOND)
                       .attachSndSettleMode(SenderSettleMode.UNSETTLED)
                       .attachSourceAddress(BrokerAdmin.TEST_QUEUE_NAME)
                       .attach()
                       .consumeResponse(Attach.class)
                       .assertLatestResponse(Attach.class, this::assumeReceiverSettlesSecond);

            Attach attach = interaction.getLatestResponse(Attach.class);
            assumeThat(attach.getSndSettleMode(), is(equalTo(SenderSettleMode.UNSETTLED)));

            interaction.flowIncomingWindow(UnsignedInteger.valueOf(Integer.MAX_VALUE))
                       .flowLinkCredit(UnsignedInteger.valueOf(Integer.MAX_VALUE))
                       .flowHandleFromLinkHandle()
                       .flow();

            // 2. Receive transfers
            final Map<Binary, DeliveryState> localUnsettled = new HashMap<>();
            for (int i = 0; i < MIN_MAX_FRAME_SIZE; )
            {
                Response<?> response = interaction.consumeResponse().getLatestResponse();
                if (response.getBody() instanceof Transfer)
                {
                    assertThat(response.getBody(), Matchers.is(instanceOf(Transfer.class)));
                    Transfer responseTransfer = (Transfer) response.getBody();
                    assertThat(responseTransfer.getMore(), is(not(equalTo(true))));
                    assertThat(responseTransfer.getSettled(), is(not(equalTo(true))));
                    localUnsettled.putIfAbsent(responseTransfer.getDeliveryTag(), responseTransfer.getState());
                    i++;
                }
                else if (response.getBody() instanceof Flow || response.getBody() instanceof Disposition)
                {
                    // ignore
                }
                else
                {
                    fail("Unexpected frame " + response.getBody());
                }
            }

            // 3. detach the link
            interaction.detach().consumeResponse(Detach.class);

            // 4. resume the link
            final Binary sampleLocalUnsettled = localUnsettled.keySet().iterator().next();
            Map<Binary, DeliveryState> unsettled = Collections.singletonMap(sampleLocalUnsettled,
                                                                            localUnsettled.get(sampleLocalUnsettled));
            final UnsignedInteger linkHandle2 = UnsignedInteger.ONE;
            Response<?> latestResponse = interaction.attachHandle(linkHandle2)
                                                    .attachUnsettled(unsettled)
                                                    .attachIncompleteUnsettled(true)
                                                    .attach().consumeResponse(End.class, Attach.class)
                                                    .getLatestResponse();
            assumeThat(latestResponse.getBody(), is(instanceOf(Attach.class)));

            final Attach resumingAttach = (Attach) latestResponse.getBody();
            final Map<Binary, DeliveryState> remoteUnsettled = resumingAttach.getUnsettled();
            assertThat(remoteUnsettled, is(notNullValue()));
            assertThat(remoteUnsettled.keySet(), is(not(empty())));
            for (Binary deliveryTag : remoteUnsettled.keySet())
            {
                assertThat(deliveryTag, in(localUnsettled.keySet()));
            }
            assertThat(resumingAttach.getIncompleteUnsettled(), is(equalTo(true)));

            interaction.flowHandle(linkHandle2).flow();

            boolean received = false;
            while (!received)
            {
                Response<?> nextResponse = interaction.consumeResponse().getLatestResponse();
                assertThat(nextResponse, is(notNullValue()));

                if (nextResponse.getBody() instanceof Transfer)
                {
                    assertThat(nextResponse.getBody(), is(instanceOf(Transfer.class)));
                    Transfer responseTransfer = (Transfer) nextResponse.getBody();
                    assertThat(responseTransfer.getMore(), is(not(equalTo(true))));
                    assertThat(responseTransfer.getSettled(), is(not(equalTo(true))));
                    assertThat(responseTransfer.getDeliveryTag(), is(equalTo(sampleLocalUnsettled)));
                    received = true;
                }
                else if (nextResponse.getBody() instanceof Flow || nextResponse.getBody() instanceof Disposition)
                {
                    // ignore
                }
                else
                {
                    fail("Unexpected frame " + nextResponse.getBody());
                }
            }

            interaction.doCloseConnection();
        }
    }

    @Test
    @SpecificationTest(section = "2.6.13", description = "When a suspended link having unsettled deliveries is resumed,"
                                                         + " the unsettled field from the attach frame will carry"
                                                         + " the delivery-tags and delivery state of all deliveries"
                                                         + " considered unsettled by the issuing link endpoint.")
    public void resumeReceivingLinkEmptyUnsettled() throws Exception
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
                                                     .flowLinkCredit(UnsignedInteger.ONE)
                                                     .flowHandleFromLinkHandle()
                                                     .flowNextIncomingIdFromPeerLatestSessionBeginAndDeliveryCount()
                                                     .flow()
                                                     .receiveDelivery()
                                                     .decodeLatestDelivery();

            Object data = interaction.getDecodedLatestDelivery();
            assertThat(data, is(equalTo(getTestName())));

            interaction.dispositionSettled(true)
                       .dispositionState(new Accepted())
                       .dispositionFirstFromLatestDelivery()
                       .dispositionRole(Role.RECEIVER)
                       .disposition();

            Detach detach = interaction.detach().consume(Detach.class, Flow.class);
            assertThat(detach.getClosed(), anyOf(nullValue(), equalTo(false)));

            interaction.attachUnsettled(new HashMap<>())
                       .attach()
                       .consumeResponse(Attach.class);

            Attach attach = interaction.getLatestResponse(Attach.class);

            Map<Binary, DeliveryState> unsettled = attach.getUnsettled();
            assumeThat(unsettled, notNullValue());
            assertThat(unsettled.entrySet(), empty());
        }
    }

    @Ignore("QPID-7845")
    @Test
    @SpecificationTest(section = "2.6.13", description = "When a suspended link having unsettled deliveries is resumed,"
                                                         + " the unsettled field from the attach frame will carry"
                                                         + " the delivery-tags and delivery state of all deliveries"
                                                         + " considered unsettled by the issuing link endpoint.")
    public void resumeReceivingLinkWithSingleUnsettledAccepted() throws Exception
    {
        Utils.putMessageOnQueue(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME, getTestName());

        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction()
                                                     .negotiateOpen(
                                                                   )
                                                     .begin().consumeResponse()
                                                     .attachRole(Role.RECEIVER)
                                                     .attachSourceAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                                     .attachRcvSettleMode(ReceiverSettleMode.FIRST)
                                                     .attachSndSettleMode(SenderSettleMode.UNSETTLED)
                                                     .attach().consumeResponse();

            Attach attach = interaction.getLatestResponse(Attach.class);
            assumeThat(attach.getSndSettleMode(), is(equalTo(SenderSettleMode.UNSETTLED)));

            interaction.flowIncomingWindow(UnsignedInteger.ONE)
                       .flowLinkCredit(UnsignedInteger.ONE)
                       .flowHandleFromLinkHandle()
                       .flow()
                       .receiveDelivery();

            List<Transfer> transfers = interaction.getLatestDelivery();
            assertThat(transfers, hasSize(1));
            Transfer transfer = transfers.get(0);

            Binary deliveryTag = transfer.getDeliveryTag();
            assertThat(deliveryTag, is(notNullValue()));
            assertThat(transfer.getSettled(), is(not(equalTo(true))));
            Object data = interaction.decodeLatestDelivery().getDecodedLatestDelivery();
            assertThat(data, is(equalTo(getTestName())));

            Detach detach = interaction.detach().consumeResponse().getLatestResponse(Detach.class);
            assertThat(detach.getClosed(), anyOf(nullValue(), equalTo(false)));

            interaction.attachUnsettled(Collections.singletonMap(deliveryTag, new Accepted()))
                       .attach()
                       .consumeResponse(Attach.class);

            Attach resumeAttach = interaction.getLatestResponse(Attach.class);

            Map<Binary, DeliveryState> unsettled = resumeAttach.getUnsettled();
            assertThat(unsettled, is(notNullValue()));
            assertThat(unsettled.entrySet(), hasSize(1));
            Map.Entry<Binary, DeliveryState> entry = unsettled.entrySet().iterator().next();
            assertThat(entry.getKey(), is(equalTo(deliveryTag)));

            interaction.flowNextIncomingId(UnsignedInteger.ONE)
                       .flowLinkCredit(UnsignedInteger.ONE)
                       .flowHandleFromLinkHandle()
                       .flow()
                       .receiveDelivery();

            transfers = interaction.getLatestDelivery();
            assertThat(transfers, hasSize(1));
            Transfer resumeTransfer = transfers.get(0);

            assertThat(resumeTransfer.getResume(), is(equalTo(true)));
            assertThat(resumeTransfer.getDeliveryTag(), is(equalTo(deliveryTag)));
            assertThat(resumeTransfer.getPayload(), is(nullValue()));

            if (!Boolean.TRUE.equals(resumeTransfer.getSettled()))
            {
                interaction.dispositionSettled(true)
                           .dispositionState(new Accepted())
                           .dispositionRole(Role.RECEIVER)
                           .disposition();
            }

            interaction.doCloseConnection();

            final String content = getTestName() + "_2";
            Utils.putMessageOnQueue(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME, content);
            assertThat(Utils.receiveMessage(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME), Matchers.is(Matchers.equalTo(content)));
        }
    }

    @Ignore("QPID-7845")
    @Test
    @SpecificationTest(section = "2.6.13", description = "When a suspended link having unsettled deliveries is resumed,"
                                                         + " the unsettled field from the attach frame will carry"
                                                         + " the delivery-tags and delivery state of all deliveries"
                                                         + " considered unsettled by the issuing link endpoint.")
    public void resumeReceivingLinkOneUnsettledWithNoOutcome() throws Exception
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
                                                     .attachSndSettleMode(SenderSettleMode.UNSETTLED)
                                                     .attach().consumeResponse();

            Attach attach = interaction.getLatestResponse(Attach.class);
            assumeThat(attach.getSndSettleMode(), is(equalTo(SenderSettleMode.UNSETTLED)));

            interaction.flowIncomingWindow(UnsignedInteger.ONE)
                       .flowLinkCredit(UnsignedInteger.ONE)
                       .flowHandleFromLinkHandle()
                       .flow()
                       .receiveDelivery();

            List<Transfer> transfers = interaction.getLatestDelivery();
            assertThat(transfers, hasSize(1));
            Transfer transfer = transfers.get(0);

            Binary deliveryTag = transfer.getDeliveryTag();
            assertThat(deliveryTag, is(notNullValue()));
            Object data = interaction.decodeLatestDelivery().getDecodedLatestDelivery();
            assertThat(data, is(equalTo(getTestName())));

            Detach detach = interaction.detach().consumeResponse(Detach.class).getLatestResponse(Detach.class);
            assertThat(detach.getClosed(), anyOf(nullValue(), equalTo(false)));

            interaction.attachUnsettled(Collections.singletonMap(deliveryTag, null))
                       .attach()
                       .consumeResponse(Attach.class);

            Attach resumeAttach = interaction.getLatestResponse(Attach.class);

            Map<Binary, DeliveryState> unsettled = resumeAttach.getUnsettled();
            assertThat(unsettled, is(notNullValue()));
            assertThat(unsettled.entrySet(), hasSize(1));
            Map.Entry<Binary, DeliveryState> entry = unsettled.entrySet().iterator().next();
            assertThat(entry.getKey(), is(equalTo(deliveryTag)));

            interaction.flowNextIncomingId(UnsignedInteger.ONE)
                       .flowLinkCredit(UnsignedInteger.ONE)
                       .flowHandleFromLinkHandle()
                       .flow()
                       .receiveDelivery();

            transfers = interaction.getLatestDelivery();
            assertThat(transfers, hasSize(1));
            Transfer resumeTransfer = transfers.get(0);

            assertThat(resumeTransfer.getResume(), is(equalTo(true)));
            assertThat(resumeTransfer.getDeliveryTag(), is(equalTo(deliveryTag)));
            assertThat(resumeTransfer.getPayload(), is(notNullValue()));

            interaction.dispositionSettled(true)
                       .dispositionState(new Accepted())
                       .dispositionRole(Role.RECEIVER)
                       .disposition();

            interaction.doCloseConnection();

            final String content = getTestName() + "_2";
            Utils.putMessageOnQueue(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME, content);
            assertThat(Utils.receiveMessage(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME), Matchers.is(Matchers.equalTo(content)));
        }
    }

    @Ignore("QPID-7845")
    @Test
    @SpecificationTest(section = "2.6.13",
            description = "When a suspended link having unsettled deliveries is resumed, the unsettled field from the"
                          + " attach frame will carry the delivery-tags and delivery state of all deliveries"
                          + " considered unsettled by the issuing link endpoint.")
    public void resumeSendingLinkSinglePartialDelivery() throws Exception
    {
        final String destination = BrokerAdmin.TEST_QUEUE_NAME;
        final Binary deliveryTag = new Binary("testDeliveryTag".getBytes(StandardCharsets.UTF_8));

        QpidByteBuffer[] messagePayload = Utils.splitPayload(getTestName(), 2);
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {

            final UnsignedInteger linkHandle1 = UnsignedInteger.ZERO;
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .begin().consumeResponse(Begin.class);

            // 1. attach with ReceiverSettleMode.SECOND
            interaction.attachHandle(linkHandle1)
                       .attachRole(Role.SENDER)
                       .attachTargetAddress(destination)
                       .attach().consumeResponse(Attach.class)
                       .consumeResponse(Flow.class);

            // 2. send a partial delivery
            interaction.transferHandle(linkHandle1)
                       .transferDeliveryId(UnsignedInteger.ZERO)
                       .transferDeliveryTag(deliveryTag)
                       .transferMore(true)
                       .transferPayload(messagePayload[0])
                       .transfer();

            // 3. detach the link
            interaction.detach().consumeResponse(Detach.class);

            // 4. resume the link
            final UnsignedInteger linkHandle2 = UnsignedInteger.ONE;
            final Attach responseAttach2 = interaction.attachHandle(linkHandle2)
                                                      .attachUnsettled(Collections.singletonMap(deliveryTag, null))
                                                      .attach().consumeResponse()
                                                      .getLatestResponse(Attach.class);

            // 5. assert content of unsettled map
            assertThat(responseAttach2.getTarget(), is(notNullValue()));

            final Map<Binary, DeliveryState> remoteUnsettled = responseAttach2.getUnsettled();
            assertThat(remoteUnsettled, is(notNullValue()));
            assertThat(remoteUnsettled.keySet(), is(equalTo(Collections.singleton(deliveryTag))));

            interaction.transferHandle(linkHandle2)
                       .transferResume(true)
                       .transfer()
                       .sync()
                       .transferMore(false)
                       .transferPayload(messagePayload[1])
                       .transfer();

            for (final QpidByteBuffer payload : messagePayload)
            {
                payload.dispose();
            }

            boolean settled = false;
            do
            {
                interaction.consumeResponse();
                Response<?> response = interaction.getLatestResponse();
                assertThat(response, is(notNullValue()));

                Object body = response.getBody();

                if (body instanceof Disposition)
                {
                    Disposition disposition = (Disposition) body;
                    assertThat(disposition.getSettled(), is(Matchers.equalTo(true)));
                    assertThat(disposition.getFirst(), equalTo(UnsignedInteger.ZERO));
                    settled = true;
                }
                else if (!(body instanceof Flow))
                {
                    fail("Unexpected response " + body);
                }
            }
            while (!settled);

            interaction.doCloseConnection();
        }
    }

    private void assumeReceiverSettlesSecond(final Attach attach)
    {
        assumeThat(attach.getRcvSettleMode(), Matchers.is(Matchers.equalTo(ReceiverSettleMode.SECOND)));
    }
}
