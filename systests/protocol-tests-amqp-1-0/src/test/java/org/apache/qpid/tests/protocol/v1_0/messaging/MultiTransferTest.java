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
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Accepted;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Close;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Disposition;
import org.apache.qpid.server.protocol.v1_0.type.transport.End;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.Open;
import org.apache.qpid.server.protocol.v1_0.type.transport.ReceiverSettleMode;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.tests.protocol.v1_0.BrokerAdmin;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.protocol.v1_0.Interaction;
import org.apache.qpid.tests.protocol.v1_0.ProtocolTestBase;
import org.apache.qpid.tests.protocol.v1_0.Response;
import org.apache.qpid.tests.protocol.v1_0.SpecificationTest;
import org.apache.qpid.tests.protocol.v1_0.Utils;

public class MultiTransferTest extends ProtocolTestBase
{
    private InetSocketAddress _brokerAddress;
    private String _originalMmsMessageStorePersistence;

    @Before
    public void setUp()
    {
        _originalMmsMessageStorePersistence = System.getProperty("qpid.tests.mms.messagestore.persistence");
        System.setProperty("qpid.tests.mms.messagestore.persistence", "false");

        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
        _brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);
    }

    @After
    public void tearDown()
    {
        if (_originalMmsMessageStorePersistence != null)
        {
            System.setProperty("qpid.tests.mms.messagestore.persistence", _originalMmsMessageStorePersistence);
        }
        else
        {
            System.clearProperty("qpid.tests.mms.messagestore.persistence");
        }
    }

    @Test
    @SpecificationTest(section = "2.6.14",
            description = "For messages that are too large to fit within the maximum frame size, additional data MAY"
                          + " be transferred in additional transfer frames by setting the more flag on all"
                          + " but the last transfer frame")
    public void multiTransferMessage() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            QpidByteBuffer[] payloads = Utils.splitPayload("testData", 2);

            final UnsignedInteger deliveryId = UnsignedInteger.ZERO;
            final Binary deliveryTag = new Binary("testTransfer".getBytes(UTF_8));

            Interaction interaction = transport.newInteraction();
            Disposition disposition = interaction.negotiateProtocol().consumeResponse()
                                                 .open().consumeResponse(Open.class)
                                                 .begin().consumeResponse(Begin.class)
                                                 .attachRole(Role.SENDER)
                                                 .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                                 .attachRcvSettleMode(ReceiverSettleMode.SECOND)
                                                 .attachSourceOutcomes(Accepted.ACCEPTED_SYMBOL)
                                                 .attach().consumeResponse(Attach.class)
                                                 .consumeResponse(Flow.class)
                                                 .transferPayload(Collections.singletonList(payloads[0]))
                                                 .transferDeliveryId(deliveryId)
                                                 .transferDeliveryTag(deliveryTag)
                                                 .transferMore(true)
                                                 .transfer()
                                                 .sync()
                                                 .transferMore(false)
                                                 .transferPayload(Collections.singletonList(payloads[1]))
                                                 .transfer()
                                                 .consumeResponse()
                                                 .getLatestResponse(Disposition.class);

            assertThat(disposition.getFirst(), is(equalTo(deliveryId)));
            assertThat(disposition.getLast(), isOneOf(null, deliveryId));
            assertThat(disposition.getSettled(), is(equalTo(false)));
        }
    }

    @Test
    @SpecificationTest(section = "2.7.5",
            description = "[delivery-id] On continuation transfers the delivery-id MAY be omitted..."
                          + "[delivery-tag] field MUST be specified for the first transfer of a multi-transfer"
                          + " message and can only be omitted for continuation transfers.")
    public void multiTransferMessageOmittingOptionalTagAndID() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            QpidByteBuffer[] payloads = Utils.splitPayload("testData", 4);
            final UnsignedInteger deliveryId = UnsignedInteger.ZERO;
            final Binary deliveryTag = new Binary("testTransfer".getBytes(UTF_8));

            Interaction interaction = transport.newInteraction();
            interaction.negotiateProtocol().consumeResponse()
                       .open().consumeResponse(Open.class)
                       .begin().consumeResponse(Begin.class)
                       .attachRole(Role.SENDER)
                       .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                       .attachRcvSettleMode(ReceiverSettleMode.SECOND)
                       .attachSourceOutcomes(Accepted.ACCEPTED_SYMBOL)
                       .attach().consumeResponse(Attach.class)
                       .consumeResponse(Flow.class)
                       .transferDeliveryId(deliveryId)
                       .transferDeliveryTag(deliveryTag)
                       .transferMore(true)
                       .transferPayload(Collections.singletonList(payloads[0]))
                       .transfer()
                       .sync()
                       .transferDeliveryId(deliveryId)
                       .transferDeliveryTag(null)
                       .transferMore(true)
                       .transferPayload(Collections.singletonList(payloads[1]))
                       .transfer()
                       .sync()
                       .transferDeliveryId(null)
                       .transferDeliveryTag(deliveryTag)
                       .transferMore(true)
                       .transferPayload(Collections.singletonList(payloads[2]))
                       .transfer()
                       .sync()
                       .transferDeliveryId(null)
                       .transferDeliveryTag(null)
                       .transferMore(false)
                       .transferPayload(Collections.singletonList(payloads[3]))
                       .transfer()
                       .consumeResponse();

            Disposition disposition = interaction.getLatestResponse(Disposition.class);

            assertThat(disposition.getFirst(), is(equalTo(deliveryId)));
            assertThat(disposition.getLast(), isOneOf(null, deliveryId));
            assertThat(disposition.getSettled(), is(equalTo(false)));
            assertThat(disposition.getState(), is(instanceOf(Accepted.class)));
        }
    }


    //

    @Test
    @SpecificationTest(section = "2.6.14",
            description = "The sender MAY indicate an aborted attempt to deliver a message by setting the abort flag on the last transfer."
                          + "In this case the receiver MUST discard the message data that was transferred prior to the abort.")
    public void abortMultiTransferMessage() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            QpidByteBuffer[] payloads = Utils.splitPayload("testData", 2);

            final UnsignedInteger deliveryId = UnsignedInteger.ZERO;
            final Binary deliveryTag = new Binary("testTransfer".getBytes(UTF_8));

            Interaction interaction = transport.newInteraction();
            interaction.negotiateProtocol().consumeResponse()
                       .open().consumeResponse(Open.class)
                       .begin().consumeResponse(Begin.class)
                       .attachRole(Role.SENDER)
                       .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                       .attachRcvSettleMode(ReceiverSettleMode.SECOND)
                       .attachSourceOutcomes(Accepted.ACCEPTED_SYMBOL)
                       .attach().consumeResponse(Attach.class)
                       .consumeResponse(Flow.class)
                       .transferPayload(Collections.singletonList(payloads[0]))
                       .transferDeliveryId(deliveryId)
                       .transferDeliveryTag(deliveryTag)
                       .transferMore(true)
                       .transfer()
                       .sync()
                       .transferPayload(null)
                       .transferMore(null)
                       .transferAborted(true)
                       .transfer();

            Response<?> latestResponse = interaction.consumeResponse(new Class<?>[] {null}).getLatestResponse();
            assertThat(latestResponse, is(nullValue()));
        }
    }
    @Test
    @SpecificationTest(section = "2.6.14",
            description = "[...]messages being transferred along different links MAY be interleaved")
    public void multiTransferInterleaved() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            QpidByteBuffer[] messagePayload1 = Utils.splitPayload("testData1", 2);
            QpidByteBuffer[] messagePayload2 = Utils.splitPayload("testData2", 2);

            UnsignedInteger linkHandle1 = UnsignedInteger.ZERO;
            UnsignedInteger linkHandle2 = UnsignedInteger.ONE;
            Binary deliveryTag1 = new Binary("testTransfer1".getBytes(UTF_8));
            Binary deliveryTag2 = new Binary("testTransfer2".getBytes(UTF_8));
            UnsignedInteger deliverId1 = UnsignedInteger.ZERO;
            UnsignedInteger deliveryId2 = UnsignedInteger.ONE;

            Interaction interaction = transport.newInteraction();

            interaction.negotiateProtocol().consumeResponse()
                       .open().consumeResponse(Open.class)
                       .begin().consumeResponse(Begin.class)

                       .attachName("testLink1")
                       .attachHandle(linkHandle1)
                       .attachRole(Role.SENDER)
                       .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                       .attachRcvSettleMode(ReceiverSettleMode.SECOND)
                       .attachSourceOutcomes(Accepted.ACCEPTED_SYMBOL)
                       .attach().consumeResponse(Attach.class)
                       .consumeResponse(Flow.class)

                       .attachName("testLink2")
                       .attachHandle(linkHandle2)
                       .attachRole(Role.SENDER)
                       .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                       .attachRcvSettleMode(ReceiverSettleMode.SECOND)
                       .attachSourceOutcomes(Accepted.ACCEPTED_SYMBOL)
                       .attach().consumeResponse(Attach.class)
                       .consumeResponse(Flow.class)

                       .transferHandle(linkHandle1)
                       .transferDeliveryId(deliverId1)
                       .transferDeliveryTag(deliveryTag1)
                       .transferMore(true)
                       .transferPayload(Collections.singletonList(messagePayload1[0]))
                       .transfer()
                       .sync()

                       .transferHandle(linkHandle2)
                       .transferDeliveryId(deliveryId2)
                       .transferDeliveryTag(deliveryTag2)
                       .transferMore(true)
                       .transferPayload(Collections.singletonList(messagePayload2[0]))
                       .transfer()
                       .sync()

                       .transferHandle(linkHandle1)
                       .transferDeliveryId(deliverId1)
                       .transferDeliveryTag(deliveryTag1)
                       .transferMore(false)
                       .transferPayload(Collections.singletonList(messagePayload1[1]))
                       .transfer()
                       .sync()

                       .transferHandle(linkHandle2)
                       .transferDeliveryId(deliveryId2)
                       .transferDeliveryTag(deliveryTag2)
                       .transferMore(false)
                       .transferPayload(Collections.singletonList(messagePayload2[1]))
                       .transfer()
                       .sync();

            Map<UnsignedInteger, Disposition> dispositionMap = new HashMap<>();
            for (int i = 0; i < 2; i++)
            {
                Disposition disposition = interaction.consumeResponse(Disposition.class)
                                                     .getLatestResponse(Disposition.class);
                dispositionMap.put(disposition.getFirst(), disposition);

                assertThat(disposition.getLast(), isOneOf(null, disposition.getFirst()));
                assertThat(disposition.getSettled(), is(equalTo(false)));
                assertThat(disposition.getState(), is(instanceOf(Accepted.class)));
            }

            assertThat("Unexpected number of dispositions", dispositionMap.size(), equalTo(2));
            assertThat(dispositionMap.containsKey(deliverId1), is(true));
            assertThat(dispositionMap.containsKey(deliveryId2), is(true));
        }
    }

    @Test
    @SpecificationTest(section = "2.6.14",
            description = "[...]messages transferred along a single link MUST NOT be interleaved")
    public void illegallyInterleavedMultiTransferOnSingleLink() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            QpidByteBuffer[] messagePayload1 = Utils.splitPayload("testData1", 2);
            QpidByteBuffer[] messagePayload2 = Utils.splitPayload("testData2", 2);

            Binary deliveryTag1 = new Binary("testTransfer1".getBytes(UTF_8));
            Binary deliveryTag2 = new Binary("testTransfer2".getBytes(UTF_8));
            UnsignedInteger deliverId1 = UnsignedInteger.ZERO;
            UnsignedInteger deliveryId2 = UnsignedInteger.ONE;

            Interaction interaction = transport.newInteraction();

            interaction.negotiateProtocol().consumeResponse()
                       .open().consumeResponse(Open.class)
                       .begin().consumeResponse(Begin.class)

                       .attachRole(Role.SENDER)
                       .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                       .attachRcvSettleMode(ReceiverSettleMode.SECOND)
                       .attachSourceOutcomes(Accepted.ACCEPTED_SYMBOL)
                       .attach().consumeResponse(Attach.class)
                       .consumeResponse(Flow.class)

                       .transferDeliveryId(deliverId1)
                       .transferDeliveryTag(deliveryTag1)
                       .transferMore(true)
                       .transferPayload(Collections.singletonList(messagePayload1[0]))
                       .transfer()
                       .sync()

                       .transferDeliveryId(deliveryId2)
                       .transferDeliveryTag(deliveryTag2)
                       .transferMore(true)
                       .transferPayload(Collections.singletonList(messagePayload2[0]))
                       .transfer()
                       .sync();

            interaction.consumeResponse(Detach.class, End.class, Close.class);
        }
    }
}
