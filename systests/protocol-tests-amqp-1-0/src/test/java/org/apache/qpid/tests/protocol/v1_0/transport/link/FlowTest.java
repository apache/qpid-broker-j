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

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;

import org.junit.Test;

import org.apache.qpid.server.protocol.v1_0.type.ErrorCarryingFrameBody;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Accepted;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.End;
import org.apache.qpid.server.protocol.v1_0.type.transport.Error;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.server.protocol.v1_0.type.transport.SessionError;
import org.apache.qpid.tests.protocol.Response;
import org.apache.qpid.tests.protocol.SpecificationTest;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.protocol.v1_0.Interaction;
import org.apache.qpid.tests.protocol.v1_0.Utils;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;

public class FlowTest extends BrokerAdminUsingTestBase
{

    @Test
    @SpecificationTest(section = "1.3.4",
            description = "mandatory [...] a non null value for the field is always encoded.")
    public void emptyFlow() throws Exception
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Response<?> response = transport.newInteraction()
                                                  .negotiateOpen()
                                                  .begin().consumeResponse(Begin.class)
                                                  .flowIncomingWindow(null)
                                                  .flowNextIncomingId(null)
                                                  .flowOutgoingWindow(null)
                                                  .flowNextOutgoingId(null)
                                                  .flow()
                                                  .consumeResponse()
                                                  .getLatestResponse();
            assertThat(response, is(notNullValue()));
            assertThat(response.getBody(), is(instanceOf(ErrorCarryingFrameBody.class)));
            final Error error = ((ErrorCarryingFrameBody) response.getBody()).getError();
            if (error != null)
            {
                assertThat(error.getCondition(), anyOf(equalTo(AmqpError.DECODE_ERROR), equalTo(AmqpError.INVALID_FIELD)));
            }
        }
    }

    @Test
    @SpecificationTest(section = "2.7.4",
            description = "If set to true then the receiver SHOULD send its state at the earliest convenient opportunity.")
    public void sessionEchoFlow() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            Flow responseFlow = transport.newInteraction()
                                         .negotiateOpen()
                                         .begin().consumeResponse(Begin.class)
                                         .flowEcho(true)
                                         .flowOutgoingWindow(UnsignedInteger.ZERO)
                                         .flowNextOutgoingId(UnsignedInteger.ZERO)
                                         .flowNextIncomingIdFromPeerLatestSessionBeginAndDeliveryCount()
                                         .flowIncomingWindow(UnsignedInteger.ONE)
                                         .flowHandle(null)
                                         .flow()
                                         .consumeResponse()
                                         .getLatestResponse(Flow.class);

            assertThat(responseFlow.getEcho(), not(equalTo(Boolean.TRUE)));
            assertThat(responseFlow.getHandle(), is(nullValue()));
        }
    }

    @Test
    @SpecificationTest(section = "2.7.4",
            description = "If set to true then the receiver SHOULD send its state at the earliest convenient opportunity.")
    public void linkEchoFlow() throws Exception
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            Flow responseFlow = transport.newInteraction()
                                         .negotiateOpen()
                                         .begin().consumeResponse(Begin.class)
                                         .attachRole(Role.RECEIVER)
                                         .attachSourceAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                         .attach().consumeResponse(Attach.class)
                                         .flowEcho(true)
                                         .flowHandleFromLinkHandle()
                                         .flowAvailable(UnsignedInteger.valueOf(10))
                                         .flowDeliveryCount(UnsignedInteger.ZERO)
                                         .flowLinkCredit(UnsignedInteger.ONE)
                                         .flowOutgoingWindow(UnsignedInteger.ZERO)
                                         .flowNextOutgoingId(UnsignedInteger.ZERO)
                                         .flowNextIncomingIdFromPeerLatestSessionBeginAndDeliveryCount()
                                         .flow().consumeResponse()
                                         .getLatestResponse(Flow.class);
            assertThat(responseFlow.getEcho(), not(equalTo(Boolean.TRUE)));
            assertThat(responseFlow.getHandle(), is(notNullValue()));
        }
    }

    @Test
    @SpecificationTest(section = "2.6.8",
            description = "A synchronous get of a message from a link is accomplished by incrementing the link-credit,"
                          + " sending the updated flow state, and waiting indefinitely for a transfer to arrive.")
    public void synchronousGet() throws Exception
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
        Utils.putMessageOnQueue(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME, getTestName());
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
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
                       .flow()
                       .receiveDelivery()
                       .decodeLatestDelivery()
                       .dispositionSettled(true)
                       .dispositionRole(Role.RECEIVER)
                       .dispositionFirstFromLatestDelivery()
                       .dispositionLast(interaction.getLatestDeliveryId())
                       .dispositionState(new Accepted())
                       .disposition()
                       .sync();
            final Object data = interaction.getDecodedLatestDelivery();
            assertThat(data, is(equalTo(getTestName())));
        }
    }

    @Test
    @SpecificationTest(section = "2.6.7",
            description = "If the sender's drain flag is set and there are no available messages,"
                          + " the sender MUST advance its delivery-count until link-credit is zero,"
                          + " and send its updated flow state to the receiver.")
    public void drainEmptyQueue() throws Exception
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            Flow responseFlow = transport.newInteraction()
                                         .negotiateOpen()
                                         .begin().consumeResponse(Begin.class)
                                         .attachRole(Role.RECEIVER)
                                         .attachSourceAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                         .attach().consumeResponse(Attach.class)
                                         .flowIncomingWindow(UnsignedInteger.valueOf(2047))
                                         .flowNextIncomingId(UnsignedInteger.ZERO)
                                         .flowOutgoingWindow(UnsignedInteger.valueOf(2147483647))
                                         .flowNextOutgoingId(UnsignedInteger.ONE)
                                         .flowDeliveryCount(UnsignedInteger.ZERO)
                                         .flowLinkCredit(UnsignedInteger.ONE)
                                         .flowDrain(Boolean.TRUE)
                                         .flowHandleFromLinkHandle()
                                         .flow()
                                         .consumeResponse().getLatestResponse(Flow.class);

            assertThat(responseFlow.getHandle(), is(notNullValue()));
            assertThat(responseFlow.getDrain(), is(equalTo(Boolean.TRUE)));
        }
    }

    @Test
    @SpecificationTest(section = "2.7.4",
            description = "If set to a handle that is not currently associated with an attached link, the recipient"
                          + " MUST respond by ending the session with an unattached-handle session error.")
    public void flowWithUnknownHandle() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            End responseEnd = transport.newInteraction()
                                       .negotiateOpen()
                                       .begin().consumeResponse(Begin.class)
                                       .flowEcho(true)
                                       .flowIncomingWindow(UnsignedInteger.ONE)
                                       .flowNextIncomingIdFromPeerLatestSessionBeginAndDeliveryCount()
                                       .flowLinkCredit(UnsignedInteger.ONE)
                                       .flowHandle(UnsignedInteger.valueOf(Integer.MAX_VALUE))
                                       .flowOutgoingWindow(UnsignedInteger.ZERO)
                                       .flowNextOutgoingId(UnsignedInteger.ZERO)
                                       .flow()
                                       .consumeResponse().getLatestResponse(End.class);

            assertThat(responseEnd.getError(), is(notNullValue()));
            assertThat(responseEnd.getError().getCondition(), is(equalTo(SessionError.UNATTACHED_HANDLE)));
        }
    }

    @Test
    @SpecificationTest(section = "2.6.8",
            description = "Synchronous get with a timeout is accomplished by incrementing the link-credit,"
                          + " sending the updated flow state and waiting for the link-credit to be consumed."
                          + " When the desired time has elapsed the receiver then sets the drain flag and sends"
                          + " the newly updated flow state again, while continuing to wait for the link-credit"
                          + " to be consumed.")
    public void synchronousGetWithTimeoutEmptyQueue() throws Exception
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            Interaction interaction = transport.newInteraction()
                                               .negotiateOpen()
                                               .begin().consumeResponse(Begin.class)
                                               .attachRole(Role.RECEIVER)
                                               .attachSourceAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                               .attach().consumeResponse(Attach.class);

            Attach remoteAttach = interaction.getLatestResponse(Attach.class);
            UnsignedInteger remoteHandle = remoteAttach.getHandle();
            assertThat(remoteHandle, is(notNullValue()));

            interaction.flowIncomingWindow(UnsignedInteger.ONE)
                                           .flowNextIncomingIdFromPeerLatestSessionBeginAndDeliveryCount()
                                           .flowOutgoingWindow(UnsignedInteger.ZERO)
                                           .flowNextOutgoingId(UnsignedInteger.ZERO)
                                           .flowLinkCredit(UnsignedInteger.ONE)
                                           .flowDrain(Boolean.FALSE)
                                           .flowEcho(Boolean.TRUE)
                                           .flowHandleFromLinkHandle()
                                           .flow()
                                           .consumeResponse(null, Flow.class);

            Flow responseFlow = interaction.flowIncomingWindow(UnsignedInteger.ONE)
                                      .flowNextIncomingIdFromPeerLatestSessionBeginAndDeliveryCount()
                                      .flowOutgoingWindow(UnsignedInteger.ZERO)
                                      .flowNextOutgoingId(UnsignedInteger.ZERO)
                                      .flowLinkCredit(UnsignedInteger.ONE)
                                      .flowDrain(Boolean.TRUE)
                                      .flowEcho(Boolean.FALSE)
                                      .flowHandleFromLinkHandle()
                                      .flow()
                                      .consumeResponse().getLatestResponse(Flow.class);

            assertThat(responseFlow.getHandle(), is(equalTo(remoteHandle)));
            assertThat(responseFlow.getDrain(), is(equalTo(Boolean.TRUE)));
        }
    }


    @Test
    @SpecificationTest(section = "2.6.8",
            description = "Synchronous get with a timeout is accomplished by incrementing the link-credit,"
                          + " sending the updated flow state and waiting for the link-credit to be consumed."
                          + " When the desired time has elapsed the receiver then sets the drain flag and sends"
                          + " the newly updated flow state again, while continuing to wait for the link-credit"
                          + " to be consumed.")
    public void synchronousGetWithTimeoutNonEmptyQueue() throws Exception
    {
        BrokerAdmin brokerAdmin = getBrokerAdmin();
        brokerAdmin.createQueue(BrokerAdmin.TEST_QUEUE_NAME);

        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            Interaction interaction = transport.newInteraction()
                                               .negotiateOpen()
                                               .begin().consumeResponse(Begin.class)
                                               .attachRole(Role.RECEIVER)
                                               .attachSourceAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                               .attach().consumeResponse(Attach.class);

            Attach remoteAttach = interaction.getLatestResponse(Attach.class);
            UnsignedInteger remoteHandle = remoteAttach.getHandle();
            assertThat(remoteHandle, is(notNullValue()));

            interaction.flowIncomingWindow(UnsignedInteger.valueOf(1))
                                                       .flowNextIncomingIdFromPeerLatestSessionBeginAndDeliveryCount()
                                                       .flowLinkCredit(UnsignedInteger.ONE)
                                                       .flowDrain(Boolean.FALSE)
                                                       .flowEcho(Boolean.FALSE)
                                                       .flowHandleFromLinkHandle()
                                                       .flowOutgoingWindow(UnsignedInteger.ZERO)
                                                       .flowNextOutgoingId(UnsignedInteger.ZERO)
                                                       .flow()
                                                       .sync();

            Utils.putMessageOnQueue(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME, getTestName());

            final Object receivedMessageContent = interaction.receiveDelivery(Flow.class)
                                                             .decodeLatestDelivery()
                                                             .getDecodedLatestDelivery();

            assertThat(receivedMessageContent, is(equalTo(getTestName())));

            final Flow responseFlow = interaction.flowNextIncomingIdFromPeerLatestSessionBeginAndDeliveryCount()
                                                 .flowLinkCredit(UnsignedInteger.ONE)
                                                 .flowDrain(Boolean.TRUE)
                                                 .flowEcho(Boolean.FALSE)
                                                 .flowHandleFromLinkHandle()
                                                 .flowOutgoingWindow(UnsignedInteger.ZERO)
                                                 .flowNextOutgoingId(UnsignedInteger.ZERO)
                                                 .flowDeliveryCount()
                                                 .flow()
                                                 .consumeResponse().getLatestResponse(Flow.class);

            assertThat(responseFlow.getHandle(), is(equalTo(remoteHandle)));

            interaction.dispositionSettled(true)
                       .dispositionRole(Role.RECEIVER)
                       .dispositionFirst(interaction.getLatestDeliveryId())
                       .dispositionState(new Accepted())
                       .disposition()
                       .sync();
        }
    }


    @Test
    @SpecificationTest(section = "2.6.9",
            description = "Asynchronous notification can be accomplished as follows."
                          + " The receiver maintains a target amount of link-credit for that link."
                          + " As transfer arrive on the link, the sender’s link-credit decreases"
                          + " as the delivery-count increases. When the sender’s link-credit falls below a threshold,"
                          + " the flow state MAY be sent to increase the sender’s link-credit back"
                          + " to the desired target amount.")
    public void asynchronousNotification() throws Exception
    {
        BrokerAdmin brokerAdmin = getBrokerAdmin();
        brokerAdmin.createQueue(BrokerAdmin.TEST_QUEUE_NAME);
        final String[] contents = Utils.createTestMessageContents(3, getTestName());
        Utils.putMessageOnQueue(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME, contents);

        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            Interaction interaction = transport.newInteraction()
                                               .negotiateOpen()
                                               .begin().consumeResponse(Begin.class)
                                               .attachRole(Role.RECEIVER)
                                               .attachSourceAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                               .attach().consumeResponse(Attach.class);

            UnsignedInteger delta = UnsignedInteger.ONE;
            UnsignedInteger incomingWindow = UnsignedInteger.valueOf(3);
            Object receivedMessageContent1 = interaction.flowIncomingWindow(incomingWindow)
                                                        .flowNextIncomingIdFromPeerLatestSessionBeginAndDeliveryCount()
                                                        .flowLinkCredit(delta)
                                                        .flowHandleFromLinkHandle()
                                                        .flowDeliveryCount()
                                                        .flowOutgoingWindow(UnsignedInteger.ZERO)
                                                        .flowNextOutgoingId(UnsignedInteger.ZERO)
                                                        .flow()
                                                        .receiveDelivery()
                                                        .decodeLatestDelivery()
                                                        .getDecodedLatestDelivery();

            assertThat(receivedMessageContent1, is(equalTo(contents[0])));
            UnsignedInteger firstDeliveryId = interaction.getLatestDeliveryId();

            Object receivedMessageContent2 = interaction.flowIncomingWindow(incomingWindow)
                                                        .flowNextIncomingIdFromPeerLatestSessionBeginAndDeliveryCount()
                                                        .flowLinkCredit(delta)
                                                        .flowHandleFromLinkHandle()
                                                        .flowOutgoingWindow(UnsignedInteger.ZERO)
                                                        .flowNextOutgoingId(UnsignedInteger.ZERO)
                                                        .flowDeliveryCount()
                                                        .flow()
                                                        .receiveDelivery()
                                                        .decodeLatestDelivery()
                                                        .getDecodedLatestDelivery();

            assertThat(receivedMessageContent2, is(equalTo(contents[1])));
            UnsignedInteger secondDeliveryId = interaction.getLatestDeliveryId();

            interaction.dispositionSettled(true)
                       .dispositionRole(Role.RECEIVER)
                       .dispositionFirst(firstDeliveryId)
                       .dispositionLast(secondDeliveryId)
                       .dispositionState(new Accepted())
                       .disposition();

            // detach link and consume detach to verify that no transfer was delivered
            interaction.detachClose(true).detach().consume(Detach.class, Flow.class);
        }
        assertThat(Utils.receiveMessage(brokerAdmin, BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(contents[2])));
    }

    @Test
    @SpecificationTest(section = "2.6.10",
            description = "Stopping the transfers on a given link is accomplished by updating the link-credit"
                          + " to be zero and sending the updated flow state. [...]"
                          + " The echo field of the flow frame MAY be used to request the sender’s flow state"
                          + " be echoed back. This MAY be used to determine when the link has finally quiesced.")
    public void stoppingALink() throws Exception
    {
        BrokerAdmin brokerAdmin = getBrokerAdmin();
        brokerAdmin.createQueue(BrokerAdmin.TEST_QUEUE_NAME);
        Utils.putMessageOnQueue(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME, getTestName());
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            Interaction interaction = transport.newInteraction()
                                               .negotiateOpen()
                                               .begin().consumeResponse(Begin.class)
                                               .attachRole(Role.RECEIVER)
                                               .attachSourceAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                               .attach().consumeResponse(Attach.class);

            Attach remoteAttach = interaction.getLatestResponse(Attach.class);
            UnsignedInteger remoteHandle = remoteAttach.getHandle();
            assertThat(remoteHandle, is(notNullValue()));

            UnsignedInteger incomingWindow = UnsignedInteger.valueOf(2);
            Object receivedMessageContent1 = interaction.flowIncomingWindow(incomingWindow)
                                                        .flowNextIncomingIdFromPeerLatestSessionBeginAndDeliveryCount()
                                                        .flowLinkCredit(incomingWindow)
                                                        .flowNextOutgoingId()
                                                        .flowHandleFromLinkHandle()
                                                        .flowOutgoingWindow(UnsignedInteger.ZERO)
                                                        .flowNextOutgoingId(UnsignedInteger.ZERO)
                                                        .flow()
                                                        .receiveDelivery()
                                                        .decodeLatestDelivery()
                                                        .getDecodedLatestDelivery();
            assertThat(receivedMessageContent1, is(equalTo(getTestName())));

            final Response<?> response = interaction.flowIncomingWindow(incomingWindow)
                                                    .flowNextIncomingIdFromPeerLatestSessionBeginAndDeliveryCount()
                                                    .flowLinkCredit(UnsignedInteger.ZERO)
                                                    .flowHandleFromLinkHandle()
                                                    .flowEcho(Boolean.TRUE)
                                                    .flowOutgoingWindow(UnsignedInteger.ZERO)
                                                    .flowNextOutgoingId(UnsignedInteger.ZERO)
                                                    .flowDeliveryCount()
                                                    .flow()
                                                    .consumeResponse(null, Flow.class)
                                                    .getLatestResponse();

            if (response != null)
            {
                assertThat(response.getBody(), is(instanceOf(Flow.class)));
                final Flow responseFlow = (Flow) response.getBody();
                assertThat(responseFlow.getEcho(), not(equalTo(Boolean.TRUE)));
                assertThat(responseFlow.getHandle(), is(notNullValue()));
            }

            final String message2 = getTestName() + "_2";
            Utils.putMessageOnQueue(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME, message2);
            try
            {
                // send session flow with echo=true to verify that no message is delivered without issuing a credit
                interaction.flowIncomingWindow(incomingWindow)
                           .flowNextIncomingIdFromPeerLatestSessionBeginAndDeliveryCount()
                           .flowLinkCredit(null)
                           .flowHandle(null)
                           .flowDeliveryCount(null)
                           .flowEcho(Boolean.TRUE)
                           .flowOutgoingWindow(UnsignedInteger.ZERO)
                           .flowNextOutgoingId(UnsignedInteger.ZERO)
                           .flow()
                           .consumeResponse(null, Flow.class);
            }
            finally
            {
                assertThat(Utils.receiveMessage(brokerAdmin, BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(message2)));

                interaction.dispositionSettled(true)
                           .dispositionRole(Role.RECEIVER)
                           .dispositionFirst(interaction.getLatestDeliveryId())
                           .dispositionState(new Accepted())
                           .disposition()
                           .sync();
            }
        }
    }

    @Test
    @SpecificationTest(section = "2.6.7",
            description = "The drain flag indicates how the sender SHOULD behave when insufficient messages are"
                          + " available to consume the current link-credit. If set, the sender will"
                          + " (after sending all available messages) advance the delivery-count as much as possible,"
                          + " consuming all link-credit, and send the flow state to the receiver.")
    public void drain() throws Exception
    {
        BrokerAdmin brokerAdmin = getBrokerAdmin();
        brokerAdmin.createQueue(BrokerAdmin.TEST_QUEUE_NAME);
        Utils.putMessageOnQueue(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME, getTestName());

        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            Interaction interaction = transport.newInteraction()
                                               .negotiateOpen()
                                               .begin().consumeResponse(Begin.class)
                                               .attachRole(Role.RECEIVER)
                                               .attachSourceAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                               .attach().consumeResponse(Attach.class);

            Attach remoteAttach = interaction.getLatestResponse(Attach.class);
            UnsignedInteger remoteHandle = remoteAttach.getHandle();
            assertThat(remoteHandle, is(notNullValue()));

            Flow responseFlow = interaction.flowIncomingWindow(UnsignedInteger.valueOf(2))
                                           .flowNextIncomingIdFromPeerLatestSessionBeginAndDeliveryCount()
                                           .flowLinkCredit(UnsignedInteger.valueOf(2))
                                           .flowDrain(Boolean.TRUE)
                                           .flowHandleFromLinkHandle()
                                           .flowOutgoingWindow(UnsignedInteger.ZERO)
                                           .flowNextOutgoingId(UnsignedInteger.ZERO)
                                           .flow()
                                           .receiveDelivery()
                                           .decodeLatestDelivery()
                                           .consumeResponse(Flow.class).getLatestResponse(Flow.class);

            assertThat(responseFlow.getHandle(), is(equalTo(remoteHandle)));

            interaction.dispositionSettled(true)
                       .dispositionRole(Role.RECEIVER)
                       .dispositionFirstFromLatestDelivery()
                       .dispositionState(new Accepted())
                       .disposition()
                       .sync();
        }
    }
}
