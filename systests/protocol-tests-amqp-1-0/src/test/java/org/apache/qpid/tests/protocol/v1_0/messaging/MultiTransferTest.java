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
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Accepted;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Disposition;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.tests.protocol.SpecificationTest;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.protocol.v1_0.Interaction;
import org.apache.qpid.tests.protocol.v1_0.Utils;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;
import org.apache.qpid.tests.utils.ConfigItem;

@ConfigItem(name = "qpid.tests.mms.messagestore.persistence", value = "false", jvm = true)
public class MultiTransferTest extends BrokerAdminUsingTestBase
{

    @Before
    public void setUp()
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
    }

    @Test
    @SpecificationTest(section = "2.6.14",
            description = "For messages that are too large to fit within the maximum frame size, additional data MAY"
                          + " be transferred in additional transfer frames by setting the more flag on all"
                          + " but the last transfer frame")
    public void multiTransferMessage() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            QpidByteBuffer[] payloads = Utils.splitPayload(getTestName(), 2);

            final UnsignedInteger deliveryId = UnsignedInteger.ZERO;
            final Binary deliveryTag = new Binary("testTransfer".getBytes(UTF_8));

            Interaction interaction = transport.newInteraction();
            Disposition disposition = interaction.negotiateOpen()
                                                 .begin().consumeResponse(Begin.class)
                                                 .attachRole(Role.SENDER)
                                                 .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                                 .attach().consumeResponse(Attach.class)
                                                 .consumeResponse(Flow.class)
                                                 .transferPayload(payloads[0])
                                                 .transferDeliveryId(deliveryId)
                                                 .transferDeliveryTag(deliveryTag)
                                                 .transferMore(true)
                                                 .transfer()
                                                 .sync()
                                                 .transferMore(false)
                                                 .transferPayload(payloads[1])
                                                 .transfer()
                                                 .consume(Disposition.class, Flow.class);

            for (final QpidByteBuffer payload : payloads)
            {
                payload.dispose();
            }

            interaction.detachEndCloseUnconditionally();

            assertThat(disposition.getFirst(), is(equalTo(deliveryId)));
            assertThat(disposition.getLast(), oneOf(null, deliveryId));
            assertThat(disposition.getSettled(), is(equalTo(true)));
        }
        assertThat(Utils.receiveMessage(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(getTestName())));
    }

    @Test
    @SpecificationTest(section = "2.7.5",
            description = "[delivery-id] On continuation transfers the delivery-id MAY be omitted..."
                          + "[delivery-tag] field MUST be specified for the first transfer of a multi-transfer"
                          + " message and can only be omitted for continuation transfers.")
    public void multiTransferMessageOmittingOptionalTagAndID() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            QpidByteBuffer[] payloads = Utils.splitPayload(getTestName(), 4);
            final UnsignedInteger deliveryId = UnsignedInteger.ZERO;
            final Binary deliveryTag = new Binary("testTransfer".getBytes(UTF_8));

            Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .begin().consumeResponse(Begin.class)
                       .attachRole(Role.SENDER)
                       .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                       .attach().consumeResponse(Attach.class)
                       .consumeResponse(Flow.class)
                       .transferDeliveryId(deliveryId)
                       .transferDeliveryTag(deliveryTag)
                       .transferMore(true)
                       .transferPayload(payloads[0])
                       .transfer()
                       .sync()
                       .transferDeliveryId(deliveryId)
                       .transferDeliveryTag(null)
                       .transferMore(true)
                       .transferPayload(payloads[1])
                       .transfer()
                       .sync()
                       .transferDeliveryId(null)
                       .transferDeliveryTag(deliveryTag)
                       .transferMore(true)
                       .transferPayload(payloads[2])
                       .transfer()
                       .sync()
                       .transferDeliveryId(null)
                       .transferDeliveryTag(null)
                       .transferMore(false)
                       .transferPayload(payloads[3])
                       .transfer().sync();

            for (final QpidByteBuffer payload : payloads)
            {
                payload.dispose();
            }

            Disposition disposition = interaction.consume(Disposition.class, Flow.class);
            interaction.detachEndCloseUnconditionally();

            assertThat(disposition.getFirst(), is(equalTo(deliveryId)));
            assertThat(disposition.getLast(), oneOf(null, deliveryId));
            assertThat(disposition.getSettled(), is(equalTo(true)));
            assertThat(disposition.getState(), is(instanceOf(Accepted.class)));
        }
        assertThat(Utils.receiveMessage(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(getTestName())));
    }

    @Test
    @SpecificationTest(section = "2.6.14",
            description = "The sender MAY indicate an aborted attempt to deliver a message by setting the abort flag on the last transfer."
                          + "In this case the receiver MUST discard the message data that was transferred prior to the abort.")
    public void abortMultiTransferMessage() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            QpidByteBuffer[] payloads = Utils.splitPayload(getTestName(), 2);

            final UnsignedInteger deliveryId = UnsignedInteger.ZERO;
            final Binary deliveryTag = new Binary("testTransfer".getBytes(UTF_8));

            Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .begin().consumeResponse(Begin.class)
                       .attachRole(Role.SENDER)
                       .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                       .attach().consumeResponse(Attach.class)
                       .consumeResponse(Flow.class)
                       .transferPayload(payloads[0])
                       .transferDeliveryId(deliveryId)
                       .transferDeliveryTag(deliveryTag)
                       .transferMore(true)
                       .transferSettled(true)
                       .transfer()
                       .transferPayload(null)
                       .transferMore(null)
                       .transferAborted(true)
                       .transfer();

            for (final QpidByteBuffer payload : payloads)
            {
                payload.dispose();
            }

            interaction.detachEndCloseUnconditionally();
        }
        String secondMessage = getTestName() + "_2";
        Utils.putMessageOnQueue(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME, secondMessage);
        assertThat(Utils.receiveMessage(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(secondMessage)));
    }

    @Test
    @SpecificationTest(section = "2.6.14",
            description = "[...]messages being transferred along different links MAY be interleaved")
    public void multiTransferInterleaved() throws Exception
    {
        String messageContent1 = getTestName() + "_1";
        String messageContent2 = getTestName() + "_2";
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            QpidByteBuffer[] messagePayload1 = Utils.splitPayload(messageContent1, 2);
            QpidByteBuffer[] messagePayload2 = Utils.splitPayload(messageContent2, 2);

            UnsignedInteger linkHandle1 = UnsignedInteger.ZERO;
            UnsignedInteger linkHandle2 = UnsignedInteger.ONE;
            Binary deliveryTag1 = new Binary("testTransfer1".getBytes(UTF_8));
            Binary deliveryTag2 = new Binary("testTransfer2".getBytes(UTF_8));
            UnsignedInteger deliverId1 = UnsignedInteger.ZERO;
            UnsignedInteger deliveryId2 = UnsignedInteger.ONE;

            Interaction interaction = transport.newInteraction();

            interaction.negotiateOpen()
                       .begin().consumeResponse(Begin.class)

                       .attachName("testLink1")
                       .attachHandle(linkHandle1)
                       .attachRole(Role.SENDER)
                       .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                       .attach().consumeResponse(Attach.class)
                       .consumeResponse(Flow.class)

                       .attachName("testLink2")
                       .attachHandle(linkHandle2)
                       .attachRole(Role.SENDER)
                       .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                       .attach().consumeResponse(Attach.class)
                       .consumeResponse(Flow.class)

                       .transferHandle(linkHandle1)
                       .transferDeliveryId(deliverId1)
                       .transferDeliveryTag(deliveryTag1)
                       .transferMore(true)
                       .transferPayload(messagePayload1[0])
                       .transfer()
                       .sync()

                       .transferHandle(linkHandle2)
                       .transferDeliveryId(deliveryId2)
                       .transferDeliveryTag(deliveryTag2)
                       .transferMore(true)
                       .transferPayload(messagePayload2[0])
                       .transfer()
                       .sync()

                       .transferHandle(linkHandle1)
                       .transferDeliveryId(deliverId1)
                       .transferDeliveryTag(deliveryTag1)
                       .transferMore(false)
                       .transferPayload(messagePayload1[1])
                       .transfer()
                       .sync()

                       .transferHandle(linkHandle2)
                       .transferDeliveryId(deliveryId2)
                       .transferDeliveryTag(deliveryTag2)
                       .transferMore(false)
                       .transferPayload(messagePayload2[1])
                       .transfer()
                       .sync();

            for (final QpidByteBuffer payload : messagePayload1)
            {
                payload.dispose();
            }
            for (final QpidByteBuffer payload : messagePayload2)
            {
                payload.dispose();
            }

            Map<UnsignedInteger, Disposition> dispositionMap = new HashMap<>();
            for (int i = 0; i < 2; i++)
            {
                Disposition disposition = interaction.consume(Disposition.class, Flow.class);
                dispositionMap.put(disposition.getFirst(), disposition);

                assertThat(disposition.getLast(), oneOf(null, disposition.getFirst()));
                assertThat(disposition.getSettled(), is(equalTo(true)));
                assertThat(disposition.getState(), is(instanceOf(Accepted.class)));
            }

            interaction.detachEndCloseUnconditionally();

            assertThat("Unexpected number of dispositions", dispositionMap.size(), equalTo(2));
            assertThat(dispositionMap.containsKey(deliverId1), is(true));
            assertThat(dispositionMap.containsKey(deliveryId2), is(true));
        }
        assertThat(Utils.receiveMessage(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(messageContent1)));
        assertThat(Utils.receiveMessage(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(messageContent2)));
    }

    @Test
    @SpecificationTest(section = "2.6.14",
            description = "For messages that are too large to fit within the maximum frame size,"
                          + " additional data MAY be transferred in additional transfer frames by setting"
                          + " the more flag on all but the last transfer frame."
                          + " When a message is split up into multiple transfer frames in this manner,"
                          + " messages being transferred along different links MAY be interleaved."
                          + " However, messages transferred along a single link MUST NOT be interleaved.")
    public void illegallyInterleavedMultiTransferOnSingleLink() throws Exception
    {
        String messageContent1 = getTestName() + "_1";
        String messageContent2 = getTestName() + "_2";
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            QpidByteBuffer[] messagePayload1 = Utils.splitPayload(messageContent1, 2);
            QpidByteBuffer[] messagePayload2 = Utils.splitPayload(messageContent2, 2);

            Binary deliveryTag1 = new Binary("testTransfer1".getBytes(UTF_8));
            Binary deliveryTag2 = new Binary("testTransfer2".getBytes(UTF_8));
            UnsignedInteger deliverId1 = UnsignedInteger.ZERO;
            UnsignedInteger deliveryId2 = UnsignedInteger.ONE;

            Interaction interaction = transport.newInteraction();

            interaction.negotiateOpen()
                       .begin().consumeResponse(Begin.class)

                       .attachRole(Role.SENDER)
                       .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                       .attach().consumeResponse(Attach.class)
                       .consumeResponse(Flow.class)

                       .transferDeliveryId(deliverId1)
                       .transferDeliveryTag(deliveryTag1)
                       .transferMore(true)
                       .transferPayload(messagePayload1[0])
                       .transferSettled(true)
                       .transfer()

                       .transferDeliveryId(deliveryId2)
                       .transferDeliveryTag(deliveryTag2)
                       .transferMore(true)
                       .transferSettled(true)
                       .transferPayload(messagePayload2[0])
                       .transfer()

                       .transferDeliveryId(deliverId1)
                       .transferDeliveryTag(deliveryTag1)
                       .transferMore(false)
                       .transferPayload(messagePayload1[1])
                       .transfer()

                       .transferDeliveryId(deliveryId2)
                       .transferDeliveryTag(deliveryTag2)
                       .transferMore(false)
                       .transferPayload(messagePayload2[1])
                       .transfer()

                       .sync();
            for (final QpidByteBuffer payload : messagePayload1)
            {
                payload.dispose();
            }
            for (final QpidByteBuffer payload : messagePayload2)
            {
                payload.dispose();
            }

            interaction.closeUnconditionally();
        }

        final String controlMessage = getTestName() + "_Control";
        Utils.putMessageOnQueue(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME, controlMessage);
        assertThat(Utils.receiveMessage(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(controlMessage)));
    }
}
