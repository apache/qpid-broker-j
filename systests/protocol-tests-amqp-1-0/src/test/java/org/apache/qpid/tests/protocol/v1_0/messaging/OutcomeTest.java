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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItemInArray;
import static org.hamcrest.Matchers.oneOf;
import static org.junit.Assume.assumeThat;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Accepted;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Modified;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
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

public class OutcomeTest extends BrokerAdminUsingTestBase
{

    @Before
    public void setUp()
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
    }


    @Test
    @SpecificationTest(section = "3.4.5", description = "If the undeliverable-here is set, then any messages released"
                                                        + " MUST NOT be redelivered to the modifying link endpoint.")
    public void modifiedOutcomeWithUndeliverableHere() throws Exception
    {
        String content1 = getTestName() + "_1";
        String content2 = getTestName() + "_2";
        Utils.putMessageOnQueue(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME, content1, content2);

        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction()
                                                     .negotiateOpen()
                                                     .begin().consumeResponse(Begin.class)
                                                     .attachRole(Role.RECEIVER)
                                                     .attachSourceAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                                     .attach().consumeResponse(Attach.class)
                                                     .assertLatestResponse(Attach.class, this::assumeModifiedSupportedBySource)
                                                     .flowIncomingWindow(UnsignedInteger.ONE)
                                                     .flowLinkCredit(UnsignedInteger.ONE)
                                                     .flowHandleFromLinkHandle()
                                                     .flowNextIncomingIdFromPeerLatestSessionBeginAndDeliveryCount()
                                                     .flow()
                                                     .receiveDelivery()

                                                     .decodeLatestDelivery();

            Object firstDeliveryPayload = interaction.getDecodedLatestDelivery();
            assertThat(firstDeliveryPayload, is(equalTo(content1)));

            Modified modifiedOutcome = new Modified();
            modifiedOutcome.setUndeliverableHere(Boolean.TRUE);
            interaction.dispositionSettled(true)
                       .dispositionRole(Role.RECEIVER)
                       .dispositionFirstFromLatestDelivery()
                       .dispositionState(modifiedOutcome)
                       .disposition()
                       .flowIncomingWindow(UnsignedInteger.valueOf(2))
                       .flowLinkCredit(UnsignedInteger.valueOf(2))
                       .flowNextIncomingIdFromPeerLatestSessionBeginAndDeliveryCount()
                       .flow()
                       .receiveDelivery(Flow.class)
                       .decodeLatestDelivery();

            Object secondDeliveryPayload = interaction.getDecodedLatestDelivery();
            assertThat(secondDeliveryPayload, is(equalTo(content2)));

            // verify that no unexpected performative is received by closing
            interaction.doCloseConnection();
        }
        assertThat(Utils.receiveMessage(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(content1)));
        assertThat(Utils.receiveMessage(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(content2)));
    }

    @Test
    @SpecificationTest(section = "3.5.3 Source",
            description = "outcomes descriptors for the outcomes that can be chosen on this link\n"
                          + "The values in this field are the symbolic descriptors of the outcomes that can be chosen"
                          + " on this link. This field MAY be empty, indicating that the default-outcome will be"
                          + " assumed for all message transfers (if the default-outcome is not set, and no outcomes"
                          + " are provided, then the accepted outcome MUST be supported by the source)."
                          + " When present, the values MUST be a symbolic descriptor of a valid outcome, e.g.,"
                          + " “amqp:accepted:list”.")
    public void transferMessageWithAttachSourceHavingExplicitlySetOutcomesToAccepted() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            Interaction interaction = transport.newInteraction();
            Disposition disposition = interaction.negotiateOpen()
                                                 .begin().consumeResponse(Begin.class)
                                                 .attachRole(Role.SENDER)
                                                 .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                                 .attachSourceOutcomes(Accepted.ACCEPTED_SYMBOL)
                                                 .attach().consumeResponse(Attach.class)
                                                 .consumeResponse(Flow.class)
                                                 .transferPayloadData(getTestName())
                                                 .transfer()
                                                 .consume(Disposition.class, Flow.class);

            interaction.detachEndCloseUnconditionally();

            assertThat(disposition.getFirst(), is(equalTo(UnsignedInteger.ZERO)));
            assertThat(disposition.getLast(), oneOf(null, UnsignedInteger.ZERO));
            assertThat(disposition.getSettled(), is(equalTo(true)));
        }
        assertThat(Utils.receiveMessage(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(getTestName())));
    }


    private void assumeModifiedSupportedBySource(final Attach attach)
    {
        assumeThat(attach.getSource(), instanceOf(Source.class));
        final Source source = (Source) attach.getSource();

        if (!(source.getDefaultOutcome() instanceof Modified))
        {
            assumeThat(source.getOutcomes(), hasItemInArray(Modified.MODIFIED_SYMBOL));
        }
    }
}
