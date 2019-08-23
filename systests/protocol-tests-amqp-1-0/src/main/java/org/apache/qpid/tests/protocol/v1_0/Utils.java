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

package org.apache.qpid.tests.protocol.v1_0;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.stream.IntStream;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Accepted;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Header;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.server.protocol.v1_0.type.transport.SenderSettleMode;
import org.apache.qpid.tests.utils.BrokerAdmin;

public class Utils
{
    public static boolean doesNodeExist(final BrokerAdmin brokerAdmin, final String nodeAddress) throws Exception
    {
        try (FrameTransport transport = new FrameTransport(brokerAdmin).connect())
        {
            final Interaction interaction = transport.newInteraction();
            final Attach attachValidationResponse = interaction.negotiateOpen()
                                                               .begin().consumeResponse()
                                                               .attachName("validationAttach")
                                                               .attachRole(Role.RECEIVER)
                                                               .attachSourceAddress(nodeAddress)
                                                               .attach().consumeResponse()
                                                               .getLatestResponse(Attach.class);
            final boolean queueExists;
            if (attachValidationResponse.getSource() != null)
            {
                queueExists = true;
                interaction.detachClose(true).detach().consumeResponse().getLatestResponse(Detach.class);
            }
            else
            {
                queueExists = false;
                interaction.consumeResponse().getLatestResponse(Detach.class);
            }
            return queueExists;
        }
    }

    public static Object receiveMessage(final BrokerAdmin brokerAdmin, final String queueName) throws Exception
    {
        try (FrameTransport transport = new FrameTransport(brokerAdmin).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .begin().consumeResponse()
                       .attachRole(Role.RECEIVER)
                       .attachName("utilsReceiverLink")
                       .attachSourceAddress(queueName)
                       .attach().consumeResponse()
                       .flowIncomingWindow(UnsignedInteger.ONE)
                       .flowNextIncomingIdFromPeerLatestSessionBeginAndDeliveryCount()
                       .flowOutgoingWindow(UnsignedInteger.ZERO)
                       .flowNextOutgoingId(UnsignedInteger.ZERO)
                       .flowLinkCredit(UnsignedInteger.ONE)
                       .flowHandleFromLinkHandle()
                       .flow()
                       .receiveDelivery()
                       .decodeLatestDelivery()
                       .dispositionSettled(true)
                       .dispositionRole(Role.RECEIVER)
                       .dispositionFirst(interaction.getLatestDeliveryId())
                       .dispositionLast(interaction.getLatestDeliveryId())
                       .dispositionState(new Accepted())
                       .disposition()
                       .detachEndCloseUnconditionally();
            return interaction.getDecodedLatestDelivery();
        }
    }

    public static  String[] createTestMessageContents(final int numberOfMessages, final String testName)
    {
        return IntStream.range(0, numberOfMessages)
                        .mapToObj(i -> String.format("%s_%d", testName, i))
                        .toArray(String[]::new);
    }

    public static  QpidByteBuffer[] splitPayload(final String messageContent, int numberOfParts)
    {
        MessageEncoder messageEncoder = new MessageEncoder();
        final Header header = new Header();
        messageEncoder.setHeader(header);
        messageEncoder.addData(messageContent);
        final QpidByteBuffer[] result;
        try (QpidByteBuffer payload = messageEncoder.getPayload())
        {
            long size = (long) payload.remaining();

            result = new QpidByteBuffer[numberOfParts];
            int chunkSize = (int) size / numberOfParts;
            int lastChunkSize = (int) size - chunkSize * (numberOfParts - 1);
            for (int i = 0; i < numberOfParts; i++)
            {
                result[i] = QpidByteBuffer.allocate(false, i == numberOfParts - 1 ? lastChunkSize : chunkSize);
                final int remaining = result[i].remaining();
                try (QpidByteBuffer view = payload.view(0, remaining))
                {
                    result[i].put(view);
                }
                result[i].flip();
                payload.position(payload.position() + remaining);
            }
        }

        return result;
    }

    public static void putMessageOnQueue(final BrokerAdmin brokerAdmin, final String queueName, final String... message)
            throws Exception
    {
        if (brokerAdmin.isPutMessageOnQueueSupported())
        {
            brokerAdmin.putMessageOnQueue(queueName, message);
        }
        else
        {
            try (FrameTransport transport = new FrameTransport(brokerAdmin).connect())
            {
                final Interaction interaction = transport.newInteraction();
                interaction.negotiateOpen()
                           .begin().consumeResponse(Begin.class)
                           .attachName("utilsSenderLink")
                           .attachRole(Role.SENDER)
                           .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                           .attachSndSettleMode(SenderSettleMode.SETTLED)
                           .attach().consumeResponse(Attach.class)
                           .consumeResponse(Flow.class);

                int tag = 0;
                for (String payload : message)
                {
                    interaction.transferPayloadData(payload)
                               .transferSettled(true)
                               .transferDeliveryId()
                               .transferDeliveryTag(new Binary(String.valueOf(tag).getBytes(UTF_8)))
                               .transfer()
                               .sync();
                    tag++;
                }
                interaction.detachEndCloseUnconditionally();
            }
        }
    }

}
