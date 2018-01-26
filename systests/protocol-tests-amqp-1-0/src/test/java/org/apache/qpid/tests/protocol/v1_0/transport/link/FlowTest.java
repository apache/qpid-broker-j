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
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;

import java.net.InetSocketAddress;

import org.junit.Test;

import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Close;
import org.apache.qpid.server.protocol.v1_0.type.transport.End;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.Open;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.server.protocol.v1_0.type.transport.SessionError;
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
            description = "Flow without mandatory fields should result in a decoding error.")
    public void emptyFlow() throws Exception
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
        final InetSocketAddress addr = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);
        try (FrameTransport transport = new FrameTransport(addr).connect())
        {
            Close responseClose = transport.newInteraction()
                                           .negotiateProtocol().consumeResponse()
                                           .open().consumeResponse(Open.class)
                                           .begin().consumeResponse(Begin.class)
                                           .flowIncomingWindow(null)
                                           .flowNextIncomingId(null)
                                           .flowOutgoingWindow(null)
                                           .flowNextOutgoingId(null)
                                           .flow()
                                           .consumeResponse()
                                           .getLatestResponse(Close.class);
            assertThat(responseClose.getError(), is(notNullValue()));
            assertThat(responseClose.getError().getCondition(), is(AmqpError.DECODE_ERROR));
        }
    }

    @Test
    @SpecificationTest(section = "2.7.4",
            description = "If set to true then the receiver SHOULD send its state at the earliest convenient opportunity.")
    public void sessionEchoFlow() throws Exception
    {
        final InetSocketAddress addr = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);
        try (FrameTransport transport = new FrameTransport(addr).connect())
        {
            Flow responseFlow = transport.newInteraction()
                                         .negotiateProtocol().consumeResponse()
                                         .open().consumeResponse(Open.class)
                                         .begin().consumeResponse(Begin.class)
                                         .flowEcho(true)
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
        final InetSocketAddress addr = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);
        try (FrameTransport transport = new FrameTransport(addr).connect())
        {
            Flow responseFlow = transport.newInteraction()
                                         .negotiateProtocol().consumeResponse()
                                         .open().consumeResponse(Open.class)
                                         .begin().consumeResponse(Begin.class)
                                         .attachRole(Role.RECEIVER)
                                         .attachSourceAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                         .attach().consumeResponse(Attach.class)
                                         .flowEcho(true)
                                         .flowHandleFromLinkHandle()
                                         .flowAvailable(UnsignedInteger.valueOf(10))
                                         .flowDeliveryCount(UnsignedInteger.ZERO)
                                         .flowLinkCredit(UnsignedInteger.ZERO)
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
        getBrokerAdmin().putMessageOnQueue(BrokerAdmin.TEST_QUEUE_NAME, "foo");
        final InetSocketAddress addr = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);

        String data = (String) Utils.receiveMessage(addr, BrokerAdmin.TEST_QUEUE_NAME);
        assertThat(data, is(equalTo("foo")));
    }

    @Test
    @SpecificationTest(section = "2.6.7",
            description = "If the sender's drain flag is set and there are no available messages,"
                          + " the sender MUST advance its delivery-count until link-credit is zero,"
                          + " and send its updated flow state to the receiver.")
    public void drainEmptyQueue() throws Exception
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
        final InetSocketAddress addr = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);
        try (FrameTransport transport = new FrameTransport(addr).connect())
        {
            Flow responseFlow = transport.newInteraction()
                                         .negotiateProtocol().consumeResponse()
                                         .open().consumeResponse(Open.class)
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
            assertThat(responseFlow.getLinkCredit(), is(equalTo(UnsignedInteger.ZERO)));
            assertThat(responseFlow.getDrain(), is(equalTo(Boolean.TRUE)));
        }
    }

    @Test
    @SpecificationTest(section = "2.7.4",
            description = "If set to a handle that is not currently associated with an attached link, the recipient"
                          + " MUST respond by ending the session with an unattached-handle session error.")
    public void flowWithUnknownHandle() throws Exception
    {
        final InetSocketAddress addr = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);
        try (FrameTransport transport = new FrameTransport(addr).connect())
        {
            End responseEnd = transport.newInteraction()
                                       .negotiateProtocol().consumeResponse()
                                       .open().consumeResponse(Open.class)
                                       .begin().consumeResponse(Begin.class)
                                       .flowEcho(true)
                                       .flowHandle(UnsignedInteger.ONE)
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
        final InetSocketAddress addr = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);
        try (FrameTransport transport = new FrameTransport(addr).connect())
        {
            Interaction interaction = transport.newInteraction()
                                               .negotiateProtocol().consumeResponse()
                                               .open().consumeResponse(Open.class)
                                               .begin().consumeResponse(Begin.class)
                                               .attachRole(Role.RECEIVER)
                                               .attachSourceAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                               .attach().consumeResponse(Attach.class);

            Attach remoteAttach = interaction.getLatestResponse(Attach.class);
            UnsignedInteger remoteHandle = remoteAttach.getHandle();
            assertThat(remoteHandle, is(notNullValue()));

            Flow responseFlow = interaction.flowIncomingWindow(UnsignedInteger.valueOf(1))
                                           .flowNextIncomingId(UnsignedInteger.ZERO)
                                           .flowLinkCredit(UnsignedInteger.ONE)
                                           .flowDrain(Boolean.FALSE)
                                           .flowEcho(Boolean.TRUE)
                                           .flowHandleFromLinkHandle()
                                           .flow()
                                           .consumeResponse().getLatestResponse(Flow.class);

            assertThat(responseFlow.getHandle(), is(equalTo(remoteHandle)));
            assertThat(responseFlow.getLinkCredit(), is(equalTo(UnsignedInteger.ONE)));
            assertThat(responseFlow.getDrain(), is(equalTo(Boolean.FALSE)));

            responseFlow = interaction.flowLinkCredit(UnsignedInteger.ONE)
                                      .flowDrain(Boolean.TRUE)
                                      .flowEcho(Boolean.FALSE)
                                      .flowHandleFromLinkHandle()
                                      .flow()
                                      .consumeResponse().getLatestResponse(Flow.class);

            assertThat(responseFlow.getHandle(), is(equalTo(remoteHandle)));
            assertThat(responseFlow.getLinkCredit(), is(equalTo(UnsignedInteger.ZERO)));
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
        String messageContent = "Test";
        brokerAdmin.putMessageOnQueue(BrokerAdmin.TEST_QUEUE_NAME, messageContent);

        final InetSocketAddress addr = brokerAdmin.getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);
        try (FrameTransport transport = new FrameTransport(addr).connect())
        {
            Interaction interaction = transport.newInteraction()
                                               .negotiateProtocol().consumeResponse()
                                               .open().consumeResponse(Open.class)
                                               .begin().consumeResponse(Begin.class)
                                               .attachRole(Role.RECEIVER)
                                               .attachSourceAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                               .attach().consumeResponse(Attach.class);

            Attach remoteAttach = interaction.getLatestResponse(Attach.class);
            UnsignedInteger remoteHandle = remoteAttach.getHandle();
            assertThat(remoteHandle, is(notNullValue()));

            Object receivedMessageContent = interaction.flowIncomingWindow(UnsignedInteger.valueOf(1))
                                                       .flowNextIncomingId(UnsignedInteger.ZERO)
                                                       .flowLinkCredit(UnsignedInteger.ONE)
                                                       .flowDrain(Boolean.FALSE)
                                                       .flowEcho(Boolean.FALSE)
                                                       .flowHandleFromLinkHandle()
                                                       .flow()
                                                       .receiveDelivery()
                                                       .decodeLatestDelivery()
                                                       .getDecodedLatestDelivery();

            assertThat(receivedMessageContent, is(equalTo(messageContent)));
            assertThat(interaction.getLatestDeliveryId(), is(equalTo(UnsignedInteger.ZERO)));

            Flow responseFlow = interaction.flowNextIncomingId(UnsignedInteger.ONE)
                                           .flowLinkCredit(UnsignedInteger.ONE)
                                           .flowDrain(Boolean.TRUE)
                                           .flowEcho(Boolean.FALSE)
                                           .flowHandleFromLinkHandle()
                                           .flow()
                                           .consumeResponse().getLatestResponse(Flow.class);

            assertThat(responseFlow.getHandle(), is(equalTo(remoteHandle)));
            assertThat(responseFlow.getLinkCredit(), is(equalTo(UnsignedInteger.ZERO)));
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
        String messageContent1 = "Test1";
        String messageContent2 = "Test2";
        String messageContent3 = "Test2";
        brokerAdmin.putMessageOnQueue(BrokerAdmin.TEST_QUEUE_NAME, messageContent1);
        brokerAdmin.putMessageOnQueue(BrokerAdmin.TEST_QUEUE_NAME, messageContent2);
        brokerAdmin.putMessageOnQueue(BrokerAdmin.TEST_QUEUE_NAME, messageContent3);

        final InetSocketAddress addr = brokerAdmin.getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);
        try (FrameTransport transport = new FrameTransport(addr).connect())
        {
            Interaction interaction = transport.newInteraction()
                                               .negotiateProtocol().consumeResponse()
                                               .open().consumeResponse(Open.class)
                                               .begin().consumeResponse(Begin.class)
                                               .attachRole(Role.RECEIVER)
                                               .attachSourceAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                               .attach().consumeResponse(Attach.class);

            Attach remoteAttach = interaction.getLatestResponse(Attach.class);
            UnsignedInteger remoteHandle = remoteAttach.getHandle();
            assertThat(remoteHandle, is(notNullValue()));

            UnsignedInteger delta = UnsignedInteger.ONE;
            UnsignedInteger incomingWindow = UnsignedInteger.valueOf(3);
            Object receivedMessageContent1 = interaction.flowIncomingWindow(incomingWindow)
                                                        .flowNextIncomingId(UnsignedInteger.ZERO)
                                                        .flowLinkCredit(delta)
                                                        .flowHandleFromLinkHandle()
                                                        .flow()
                                                        .receiveDelivery()
                                                        .decodeLatestDelivery()
                                                        .getDecodedLatestDelivery();

            assertThat(receivedMessageContent1, is(equalTo(messageContent1)));
            assertThat(interaction.getLatestDeliveryId(), is(equalTo(UnsignedInteger.ZERO)));

            Object receivedMessageContent2 = interaction.flowIncomingWindow(incomingWindow)
                                                        .flowNextIncomingId(UnsignedInteger.ONE)
                                                        .flowLinkCredit(delta)
                                                        .flowHandleFromLinkHandle()
                                                        .flow()
                                                        .receiveDelivery()
                                                        .decodeLatestDelivery()
                                                        .getDecodedLatestDelivery();

            assertThat(receivedMessageContent2, is(equalTo(messageContent2)));
            assertThat(interaction.getLatestDeliveryId(), is(equalTo(UnsignedInteger.ONE)));

            // send session flow with echo=true to verify that no message is delivered without issuing a credit
            Flow responseFlow = interaction.flowNextIncomingId(UnsignedInteger.valueOf(2))
                                           .flowLinkCredit(null)
                                           .flowHandle(null)
                                           .flowEcho(Boolean.TRUE)
                                           .flow()
                                           .consumeResponse().getLatestResponse(Flow.class);

            assertThat(responseFlow.getHandle(), is(nullValue()));
        }
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
        String messageContent1 = "Test1";
        String messageContent2 = "Test2";
        brokerAdmin.putMessageOnQueue(BrokerAdmin.TEST_QUEUE_NAME, messageContent1);
        brokerAdmin.putMessageOnQueue(BrokerAdmin.TEST_QUEUE_NAME, messageContent2);

        final InetSocketAddress addr = brokerAdmin.getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);
        try (FrameTransport transport = new FrameTransport(addr).connect())
        {
            Interaction interaction = transport.newInteraction()
                                               .negotiateProtocol().consumeResponse()
                                               .open().consumeResponse(Open.class)
                                               .begin().consumeResponse(Begin.class)
                                               .attachRole(Role.RECEIVER)
                                               .attachSourceAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                               .attach().consumeResponse(Attach.class);

            Attach remoteAttach = interaction.getLatestResponse(Attach.class);
            UnsignedInteger remoteHandle = remoteAttach.getHandle();
            assertThat(remoteHandle, is(notNullValue()));

            Object receivedMessageContent1 = interaction.flowIncomingWindow(UnsignedInteger.valueOf(2))
                                                        .flowNextIncomingId(UnsignedInteger.ZERO)
                                                        .flowLinkCredit(UnsignedInteger.ONE)
                                                        .flowHandleFromLinkHandle()
                                                        .flow()
                                                        .receiveDelivery()
                                                        .decodeLatestDelivery()
                                                        .getDecodedLatestDelivery();

            assertThat(receivedMessageContent1, is(equalTo(messageContent1)));
            assertThat(interaction.getLatestDeliveryId(), is(equalTo(UnsignedInteger.ZERO)));

            Flow responseFlow = interaction.flowNextIncomingId(UnsignedInteger.ONE)
                                           .flowLinkCredit(UnsignedInteger.ZERO)
                                           .flowHandleFromLinkHandle()
                                           .flowEcho(Boolean.TRUE)
                                           .flow()
                                           .consumeResponse().getLatestResponse(Flow.class);

            assertThat(responseFlow.getHandle(), is(equalTo(remoteHandle)));
            assertThat(responseFlow.getLinkCredit(), is(equalTo(UnsignedInteger.ZERO)));
        }
    }

    @Test
    @SpecificationTest(section = "2.6.7",
            description = "The drain flag indicates how the sender SHOULD behave when insufficient messages are"
                          + " available to consume the current link-credit. If set, the sender will"
                          + " (after sending all available messages) advance the delivery-count as much as possible,"
                          + " consuming all link-credit, and send the flow state to the receiver.")
    public void drainWithZeroCredits() throws Exception
    {
        BrokerAdmin brokerAdmin = getBrokerAdmin();
        brokerAdmin.createQueue(BrokerAdmin.TEST_QUEUE_NAME);
        brokerAdmin.putMessageOnQueue(BrokerAdmin.TEST_QUEUE_NAME, "Test1");

        final InetSocketAddress addr = brokerAdmin.getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);
        try (FrameTransport transport = new FrameTransport(addr).connect())
        {
            Interaction interaction = transport.newInteraction()
                                               .negotiateProtocol().consumeResponse()
                                               .open().consumeResponse(Open.class)
                                               .begin().consumeResponse(Begin.class)
                                               .attachRole(Role.RECEIVER)
                                               .attachSourceAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                               .attach().consumeResponse(Attach.class);

            Attach remoteAttach = interaction.getLatestResponse(Attach.class);
            UnsignedInteger remoteHandle = remoteAttach.getHandle();
            assertThat(remoteHandle, is(notNullValue()));

            Flow responseFlow = interaction.flowIncomingWindow(UnsignedInteger.valueOf(2))
                                           .flowNextIncomingId(UnsignedInteger.ZERO)
                                           .flowLinkCredit(UnsignedInteger.ZERO)
                                           .flowDrain(Boolean.TRUE)
                                           .flowHandleFromLinkHandle()
                                           .flow()
                                           .consumeResponse().getLatestResponse(Flow.class);

            assertThat(responseFlow.getHandle(), is(equalTo(remoteHandle)));
            assertThat(responseFlow.getLinkCredit(), is(equalTo(UnsignedInteger.ZERO)));
        }
    }
}
