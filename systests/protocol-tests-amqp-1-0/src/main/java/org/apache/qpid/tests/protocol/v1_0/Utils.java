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

import java.net.InetSocketAddress;
import java.util.List;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.bytebuffer.QpidByteBufferUtils;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Header;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;

public class Utils
{
    public static boolean doesNodeExist(final InetSocketAddress brokerAddress,
                                        final String nodeAddress) throws Exception
    {
        try (FrameTransport transport = new FrameTransport(brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            final Attach attachValidationResponse = interaction.negotiateProtocol().consumeResponse()
                                                               .open().consumeResponse()
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

    public static Object receiveMessage(final InetSocketAddress brokerAddress,
                                        final String queueName) throws Exception
    {
        try (FrameTransport transport = new FrameTransport(brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction()
                                                     .negotiateProtocol().consumeResponse()
                                                     .open().consumeResponse()
                                                     .begin().consumeResponse()
                                                     .attachRole(Role.RECEIVER)
                                                     .attachSourceAddress(queueName)
                                                     .attach().consumeResponse()
                                                     .flowIncomingWindow(UnsignedInteger.ONE)
                                                     .flowNextIncomingId(UnsignedInteger.ZERO)
                                                     .flowOutgoingWindow(UnsignedInteger.ZERO)
                                                     .flowNextOutgoingId(UnsignedInteger.ZERO)
                                                     .flowLinkCredit(UnsignedInteger.ONE)
                                                     .flowHandleFromLinkHandle()
                                                     .flow()
                                                     .receiveDelivery()
                                                     .decodeLatestDelivery();

            return interaction.getDecodedLatestDelivery();
        }
    }

    public static  QpidByteBuffer[] splitPayload(final String messageContent, int numberOfParts)
    {
        MessageEncoder messageEncoder = new MessageEncoder();
        final Header header = new Header();
        messageEncoder.setHeader(header);
        messageEncoder.addData(messageContent);
        List<QpidByteBuffer> payload = messageEncoder.getPayload();
        long size = QpidByteBufferUtils.remaining(payload);

        QpidByteBuffer[] result = new QpidByteBuffer[numberOfParts];
        int chunkSize = (int) size / numberOfParts;
        int lastChunkSize = (int) size - chunkSize * (numberOfParts - 1);
        for (int i = 0; i < numberOfParts; i++)
        {
            result[i] = QpidByteBuffer.allocate(false, i == numberOfParts - 1 ? lastChunkSize : chunkSize);
        }

        int currentBufferIndex = 0;
        for (QpidByteBuffer p : payload)
        {
            final int limit = p.limit();

            while (p.hasRemaining())
            {
                QpidByteBuffer currentBuffer = result[currentBufferIndex];
                if (currentBuffer.hasRemaining())
                {
                    int length = Math.min(p.remaining(), currentBuffer.remaining());
                    p.limit(p.position() + length);
                    currentBuffer.put(p.slice());
                    p.position(p.position() + length);
                    p.limit(limit);
                }

                if (!currentBuffer.hasRemaining())
                {
                    currentBuffer.flip();
                    currentBufferIndex++;
                }
            }
        }
        return result;
    }
}
