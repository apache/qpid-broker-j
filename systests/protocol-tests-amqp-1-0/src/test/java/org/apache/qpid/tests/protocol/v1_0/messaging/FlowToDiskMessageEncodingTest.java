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
package org.apache.qpid.tests.protocol.v1_0.messaging;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.HexFormat;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Accepted;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Disposition;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.ReceiverSettleMode;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.server.protocol.v1_0.type.transport.Transfer;
import org.apache.qpid.tests.protocol.SpecificationTest;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.protocol.v1_0.Interaction;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;
import org.apache.qpid.tests.utils.ConfigItem;

@ConfigItem(name = "broker.flowToDiskThreshold", value = "1")
public class FlowToDiskMessageEncodingTest extends BrokerAdminUsingTestBase
{
    @Test
    @SpecificationTest(section = "3.2", description = "The exact encoding of sections of the bare message " +
            "MUST NOT be modified")
    public void bareMessageBytesSurviveFlowToDisk() throws Exception
    {
        assumeTrue(getBrokerAdmin().isFlowToDiskSupported(), "Requires a persistent store and eviction statistics");
        final long evacuatedBeforeSend = getBrokerAdmin().getBytesEvacuatedFromMemory();
        final byte[] bytes = HexFormat.of().parseHex(
                "00800000000000000073d00000000400000000" +
                "00800000000000000074d10000001000000002b1000000016bb10000000176" +
                "00800000000000000075b00000000401020304");
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
        try (final FrameTransport sender = new FrameTransport(getBrokerAdmin()).connect();
             final QpidByteBuffer payload = QpidByteBuffer.wrap(bytes))
        {
            final Interaction interaction = sender.newInteraction().negotiateOpen()
                    .begin().consumeResponse(Begin.class)
                    .attachRole(Role.SENDER).attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                    .attach().consumeResponse(Attach.class).consumeResponse(Flow.class);
            final Disposition disposition = interaction.transferPayload(payload).transferSettled(false).transfer()
                    .consume(Disposition.class, Flow.class);
            assertInstanceOf(Accepted.class, disposition.getState());
            interaction.detachEndCloseUnconditionally();
        }

        assertTrue(getBrokerAdmin().getBytesEvacuatedFromMemory() > evacuatedBeforeSend,
                   "The message must be evicted before delivery");
        assertEquals(1, getBrokerAdmin().getQueueDepthMessages(BrokerAdmin.TEST_QUEUE_NAME));

        try (final FrameTransport receiver = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = receiver.newInteraction().negotiateOpen()
                    .begin().consumeResponse(Begin.class)
                    .attachRole(Role.RECEIVER).attachSourceAddress(BrokerAdmin.TEST_QUEUE_NAME)
                    .attachRcvSettleMode(ReceiverSettleMode.FIRST).attach().consumeResponse(Attach.class)
                    .flowIncomingWindow(UnsignedInteger.ONE)
                    .flowNextIncomingIdFromPeerLatestSessionBeginAndDeliveryCount()
                    .flowOutgoingWindow(UnsignedInteger.ZERO).flowNextOutgoingId(UnsignedInteger.ZERO)
                    .flowLinkCredit(UnsignedInteger.ONE).flowHandleFromLinkHandle().flow()
                    .receiveDelivery(Flow.class);
            final List<Transfer> transfers = interaction.getLatestDelivery();
            final List<QpidByteBuffer> payloads = transfers.stream().map(Transfer::getPayload)
                    .collect(Collectors.toList());
            try (final QpidByteBuffer received = QpidByteBuffer.concatenate(payloads))
            {
                final byte[] actual = new byte[received.remaining()];
                received.get(actual);
                assertArrayEquals(bytes, actual);
            }
            finally
            {
                payloads.forEach(QpidByteBuffer::dispose);
                transfers.forEach(Transfer::dispose);
            }
            interaction.dispositionSettled(true).dispositionState(Accepted.INSTANCE)
                    .dispositionRole(Role.RECEIVER).dispositionFirstFromLatestDelivery().disposition();
            interaction.detachEndCloseUnconditionally();
        }
    }
}
