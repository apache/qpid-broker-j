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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Accepted;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.server.protocol.v1_0.type.transport.SenderSettleMode;
import org.apache.qpid.server.util.StringUtil;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminException;
import org.apache.qpid.tests.utils.QueueAdmin;

@SuppressWarnings("unused")
public class ExistingQueueAdmin implements QueueAdmin
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ExistingQueueAdmin.class);
    private static final String ADMIN_LINK_NAME = "existingQueueAdminLink";
    private static final int DRAIN_CREDITS = 1000;
    private static final boolean DRAIN_UNSETTLED =
            Boolean.getBoolean("qpid.tests.protocol.broker.external.existingQueueAdmin.drainUnsettled");

    @Override
    public void createQueue(final BrokerAdmin brokerAdmin, final String queueName)
    {

    }

    @Override
    public void deleteQueue(final BrokerAdmin brokerAdmin, final String queueName)
    {
        try
        {
            drainQueue(brokerAdmin, queueName);
        }
        catch (Exception e)
        {
            throw new BrokerAdminException(String.format("Cannot drain queue '%s'", queueName), e);
        }
    }

    @Override
    public void putMessageOnQueue(final BrokerAdmin brokerAdmin, final String queueName, final String... message)
    {
        try
        {
            send(brokerAdmin, queueName, message);
        }
        catch (Exception e)
        {
            throw new BrokerAdminException(String.format("Cannot put %d messages on a queue '%s'",
                                                         message.length,
                                                         queueName), e);
        }
    }

    @Override
    public boolean isDeleteQueueSupported()
    {
        return false;
    }

    @Override
    public boolean isPutMessageOnQueueSupported()
    {
        return true;
    }

    private void send(final BrokerAdmin brokerAdmin,
                      final String queueName,
                      final String... message) throws Exception
    {
        try (FrameTransport transport = new FrameTransport(brokerAdmin).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .begin().consumeResponse(Begin.class)
                       .attachName(ADMIN_LINK_NAME)
                       .attachRole(Role.SENDER)
                       .attachTargetAddress(queueName)
                       .attachSndSettleMode(SenderSettleMode.SETTLED)
                       .attach().consumeResponse(Attach.class)
                       .consumeResponse(Flow.class)
                       .getLatestResponse(Flow.class);

            int tag = 0;
            for (final String payload : message)
            {
                interaction.transferPayloadData(payload)
                           .transferSettled(true)
                           .transferDeliveryId()
                           .transferDeliveryTag(new Binary(String.valueOf(tag).getBytes(UTF_8)))
                           .transfer()
                           .sync();
                tag++;
            }
            closeInteraction(interaction);
        }
    }

    private void closeInteraction(final Interaction interaction) throws Exception
    {
        interaction.detachClose(true)
                   .detach()
                   .end()
                   .close()
                   .sync();
    }


    private void drainQueue(final BrokerAdmin brokerAdmin, final String queueName) throws Exception
    {
        final String controlMessage = String.format("---%s---", new StringUtil().randomAlphaNumericString(32));
        send(brokerAdmin, queueName, controlMessage);
        try (FrameTransport transport = new FrameTransport(brokerAdmin).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .begin().consumeResponse()
                       .attachName(ADMIN_LINK_NAME)
                       .attachRole(Role.RECEIVER)
                       .attachSndSettleMode(DRAIN_UNSETTLED ? SenderSettleMode.UNSETTLED : SenderSettleMode.SETTLED)
                       .attachSourceAddress(queueName)
                       .attach().consumeResponse(Attach.class)
                       .flowIncomingWindow(UnsignedInteger.valueOf(DRAIN_CREDITS))
                       .flowNextIncomingIdFromPeerLatestSessionBeginAndDeliveryCount()
                       .flowLinkCredit(UnsignedInteger.valueOf(DRAIN_CREDITS))
                       .flowHandleFromLinkHandle()
                       .flowOutgoingWindow(UnsignedInteger.ZERO)
                       .flowNextOutgoingId(UnsignedInteger.ZERO)
                       .flow();

            boolean controlMessageReceived;
            do
            {
                interaction.receiveDelivery(Flow.class);
                try
                {
                    interaction.decodeLatestDelivery();
                }
                catch (Exception e)
                {
                    LOGGER.error("Message decoding failed", e);
                }

                final Object message = interaction.getDecodedLatestDelivery();
                if (DRAIN_UNSETTLED)
                {
                    interaction.dispositionSettled(true)
                               .dispositionRole(Role.RECEIVER)
                               .dispositionFirstFromLatestDelivery()
                               .dispositionState(new Accepted())
                               .disposition();
                }

                controlMessageReceived = controlMessage.equals(message);
            }
            while (!controlMessageReceived);
            closeInteraction(interaction);
        }
    }
}
