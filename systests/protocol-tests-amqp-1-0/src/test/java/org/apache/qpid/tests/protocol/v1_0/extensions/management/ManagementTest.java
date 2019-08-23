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

package org.apache.qpid.tests.protocol.v1_0.extensions.management;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assume.assumeThat;

import java.util.Collections;

import org.junit.Test;

import org.apache.qpid.server.protocol.v1_0.Session_1_0;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.messaging.DeleteOnClose;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Target;
import org.apache.qpid.server.protocol.v1_0.type.messaging.TerminusExpiryPolicy;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.ReceiverSettleMode;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.tests.protocol.SpecificationTest;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.protocol.v1_0.Interaction;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;

public class ManagementTest extends BrokerAdminUsingTestBase
{

    @Test
    @SpecificationTest(section = "2.6.7",
            description = "The drain flag indicates how the sender SHOULD behave when insufficient messages"
                          + " are available to consume the current link-credit. If set, the sender will"
                          + " (after sending all available messages) advance the delivery-count as much as possible,"
                          + " consuming all link-credit, and send the flow state to the receiver.")
    public void drainTemporaryMessageSource() throws Exception
    {
        assumeThat(getBrokerAdmin().isManagementSupported(), is(equalTo(true)));

        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            Target target = new Target();
            target.setDynamicNodeProperties(Collections.singletonMap(Session_1_0.LIFETIME_POLICY, new DeleteOnClose()));
            target.setExpiryPolicy(TerminusExpiryPolicy.LINK_DETACH);
            target.setDynamic(true);
            target.setCapabilities(new Symbol[]{Symbol.valueOf("temporary-queue")});

            final Interaction interaction = transport.newInteraction();
            final Attach attachResponse = interaction.openHostname("$management")
                                                     .negotiateOpen()
                                                     .begin().consumeResponse(Begin.class)
                                                     .attachRole(Role.SENDER)
                                                     .attachTarget(target)
                                                     .attach().consumeResponse()
                                                     .getLatestResponse(Attach.class);

            assertThat(attachResponse.getSource(), is(notNullValue()));
            assertThat(attachResponse.getTarget(), is(notNullValue()));

            String newTemporaryNodeAddress = ((Target) attachResponse.getTarget()).getAddress();
            assertThat(newTemporaryNodeAddress, is(notNullValue()));

            interaction.consumeResponse().getLatestResponse(Flow.class);

            final Attach receiverResponse = interaction.attachHandle(UnsignedInteger.ONE).attachRole(Role.RECEIVER)
                                                       .attachSourceAddress(newTemporaryNodeAddress)
                                                       .attachRcvSettleMode(ReceiverSettleMode.FIRST)
                                                       .attach().consumeResponse().getLatestResponse(Attach.class);

            assertThat(receiverResponse.getSource(), is(instanceOf(Source.class)));
            assertThat(((Source)receiverResponse.getSource()).getAddress(), is(equalTo(newTemporaryNodeAddress)));

            // 2.6.8  Synchronous Get

            // grant credit of 1
            interaction.flowIncomingWindow(UnsignedInteger.ONE)
                       .flowNextIncomingId(UnsignedInteger.ZERO)
                       .flowOutgoingWindow(UnsignedInteger.ZERO)
                       .flowNextOutgoingId(UnsignedInteger.ZERO)
                       .flowLinkCredit(UnsignedInteger.ONE)
                       .flowHandleFromLinkHandle()
                       .flow();

            // send drain to ensure the sender promptly advances the delivery-count until link-credit is consumed
            interaction.flowDrain(true).flow();

            Flow flow = interaction.consumeResponse().getLatestResponse(Flow.class);
            assertThat(flow.getLinkCredit(), is(equalTo(UnsignedInteger.ZERO)));
            assertThat(flow.getHandle(), is(equalTo(receiverResponse.getHandle())));

            interaction.doCloseConnection();
        }
    }
}
