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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;

import org.junit.Ignore;
import org.junit.Test;

import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedShort;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Disposition;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.LinkError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.tests.protocol.SpecificationTest;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.protocol.v1_0.Interaction;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;
import org.apache.qpid.tests.utils.BrokerSpecific;

public class LinkStealingTest extends BrokerAdminUsingTestBase
{

    @Test
    @SpecificationTest(section = "2.6.1. Naming a link",
                       description = "Consequently, a link can only be active in one connection at a time."
                                     + " If an attempt is made to attach the link subsequently when it is not suspended,"
                                     + " then the link can be ’stolen’, i.e., the second attach succeeds and the first"
                                     + " attach MUST then be closed with a link error of stolen. This behavior ensures"
                                     + " that in the event of a connection failure occurring and being noticed"
                                     + " by one party, that re-establishment has the desired effect.")
    @Ignore("QPID-8328: Broker erroneously ends the session with internal error")
    public void subsequentAttachOnTheSameSession() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            final Attach responseAttach = interaction
                    .negotiateOpen()
                    .begin().consumeResponse(Begin.class)
                    .attachRole(Role.SENDER)
                    .attachInitialDeliveryCount(UnsignedInteger.ZERO)
                    .attachHandle(UnsignedInteger.ZERO)
                    .attach().consumeResponse()
                    .getLatestResponse(Attach.class);
            assertThat(responseAttach.getName(), is(notNullValue()));
            assertThat(responseAttach.getHandle().longValue(), is(both(greaterThanOrEqualTo(0L)).and(lessThan(UnsignedInteger.MAX_VALUE.longValue()))));
            assertThat(responseAttach.getRole(), is(Role.RECEIVER));
            assertThat(responseAttach.getTarget(), is(notNullValue()));
            assertThat(responseAttach.getSource(), is(notNullValue()));

            Detach stolenDetach = interaction.consumeResponse(Flow.class)
                                             .attachHandle(UnsignedInteger.ONE)
                                             .attach()
                                             .consume(Detach.class, Attach.class, Flow.class);

            assertThat(stolenDetach.getHandle().longValue(), is(equalTo(responseAttach.getHandle().longValue())));
            assertThat(stolenDetach.getError(), is(notNullValue()));
            assertThat(stolenDetach.getError().getCondition(), is(equalTo(LinkError.STOLEN)));
        }
    }



    @Test
    @SpecificationTest(section = "2.6.1. Naming a link",
            description = "Qpid Broker J extended stolen behaviour on sessions")
    @BrokerSpecific(kind = BrokerAdmin.KIND_BROKER_J)
    public void subsequentAttachOnDifferentSessions() throws Exception
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            final String linkName = "test";
            final Attach responseAttach = interaction.negotiateOpen()
                                                     .begin().consumeResponse(Begin.class)
                                                     .attachRole(Role.SENDER)
                                                     .attachName(linkName)
                                                     .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                                     .attach().consumeResponse()
                                                     .getLatestResponse(Attach.class);
            assertThat(responseAttach.getName(), is(notNullValue()));
            assertThat(responseAttach.getHandle().longValue(),
                       is(both(greaterThanOrEqualTo(0L)).and(lessThan(UnsignedInteger.MAX_VALUE.longValue()))));
            assertThat(responseAttach.getRole(), is(Role.RECEIVER));

            interaction.consumeResponse(Flow.class);

            final Detach stolenDetach = interaction.sessionChannel(UnsignedShort.valueOf(2))
                                                   .begin().consumeResponse(Begin.class)
                                                   .attachRole(Role.SENDER)
                                                   .attachName(linkName)
                                                   .attachInitialDeliveryCount(UnsignedInteger.ZERO)
                                                   .attach()
                                                   .consume(Detach.class, Attach.class, Flow.class, Disposition.class);

            assertThat(stolenDetach.getHandle().longValue(), is(equalTo(responseAttach.getHandle().longValue())));
            assertThat(stolenDetach.getError(), is(notNullValue()));
            assertThat(stolenDetach.getError().getCondition(), is(equalTo(LinkError.STOLEN)));

        }
    }

}
