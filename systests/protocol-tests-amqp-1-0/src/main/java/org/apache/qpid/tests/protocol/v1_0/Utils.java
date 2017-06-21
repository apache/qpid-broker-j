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

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.net.InetSocketAddress;

import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.server.protocol.v1_0.type.transport.Transfer;

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

            return messageDecoder.getData();
        }
    }
}
