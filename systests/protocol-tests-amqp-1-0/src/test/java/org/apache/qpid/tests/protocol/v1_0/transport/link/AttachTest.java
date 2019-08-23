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
 */

package org.apache.qpid.tests.protocol.v1_0.transport.link;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import org.junit.Test;

import org.apache.qpid.server.protocol.v1_0.type.ErrorCarryingFrameBody;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.End;
import org.apache.qpid.server.protocol.v1_0.type.transport.Error;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.tests.protocol.Response;
import org.apache.qpid.tests.protocol.SpecificationTest;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.protocol.v1_0.Interaction;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;
import org.apache.qpid.tests.utils.BrokerSpecific;

public class AttachTest extends BrokerAdminUsingTestBase
{
    @Test
    @SpecificationTest(section = "1.3.4",
            description = "mandatory [...] a non null value for the field is always encoded.")
    public void emptyAttach() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Response<?> response = transport.newInteraction()
                                                  .negotiateOpen()
                                                  .begin().consumeResponse(Begin.class)
                                                  .attachRole(null)
                                                  .attachHandle(null)
                                                  .attachName(null)
                                                  .attach().consumeResponse()
                                                  .getLatestResponse();
            assertThat(response.getBody(), is(notNullValue()));
            assertThat(response.getBody(), instanceOf(ErrorCarryingFrameBody.class));
            final Error error = ((ErrorCarryingFrameBody) response.getBody()).getError();
            if (error != null)
            {
                assertThat(error.getCondition(),
                           anyOf(equalTo(AmqpError.DECODE_ERROR), equalTo(AmqpError.INVALID_FIELD)));
            }
        }
    }

    @Test
    @SpecificationTest(section = "2.6.3",
            description = "Links are established and/or resumed by creating a link endpoint associated with a local terminus, "
                          + "assigning it to an unused handle, and sending an attach frame.")
    public void successfulAttachAsSender() throws Exception
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Attach responseAttach = transport.newInteraction()
                                                   .negotiateOpen()
                                                   .begin().consumeResponse(Begin.class)
                                                   .attachRole(Role.SENDER)
                                                   .attachInitialDeliveryCount(UnsignedInteger.ZERO)
                                                   .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                                   .attach().consumeResponse()
                                                   .getLatestResponse(Attach.class);
            assertThat(responseAttach.getName(), is(notNullValue()));
            assertThat(responseAttach.getHandle().longValue(), is(both(greaterThanOrEqualTo(0L)).and(lessThan(UnsignedInteger.MAX_VALUE.longValue()))));
            assertThat(responseAttach.getRole(), is(Role.RECEIVER));
            assertThat(responseAttach.getTarget(), is(notNullValue()));
            assertThat(responseAttach.getSource(), is(notNullValue()));
        }
    }

    @Test
    @SpecificationTest(section = "2.6.3",
            description = "Links are established and/or resumed by creating a link endpoint associated with a local terminus, "
                          + "assigning it to an unused handle, and sending an attach frame.")
    public void successfulAttachAsReceiver() throws Exception
    {
        String queueName = BrokerAdmin.TEST_QUEUE_NAME;
        getBrokerAdmin().createQueue(queueName);
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Attach responseAttach = transport.newInteraction()
                                                   .negotiateOpen()
                                                   .begin().consumeResponse(Begin.class)
                                                   .attachRole(Role.RECEIVER)
                                                   .attachSourceAddress(queueName)
                                                   .attach().consumeResponse()
                                                   .getLatestResponse(Attach.class);
            assertThat(responseAttach.getName(), is(notNullValue()));
            assertThat(responseAttach.getHandle().longValue(), is(both(greaterThanOrEqualTo(0L)).and(lessThan(UnsignedInteger.MAX_VALUE.longValue()))));
            assertThat(responseAttach.getRole(), is(Role.SENDER));
            assertThat(responseAttach.getSource(), is(notNullValue()));
        }
    }

    @Test
    @SpecificationTest(section = "2.6.3",
            description = "Note that if the application chooses not to create a terminus, the session endpoint will"
                          + " still create a link endpoint and issue an attach indicating that the link endpoint has"
                          + " no associated local terminus. In this case, the session endpoint MUST immediately"
                          + " detach the newly created link endpoint.")
    @BrokerSpecific(kind = BrokerAdmin.KIND_BROKER_J)
    public void attachReceiverWithNullTarget() throws Exception
    {
        String queueName = BrokerAdmin.TEST_QUEUE_NAME;
        getBrokerAdmin().createQueue(queueName);
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            Interaction interaction = transport.newInteraction();
            final Attach responseAttach = interaction.negotiateOpen()
                                                     .begin().consumeResponse(Begin.class)
                                                     .attachRole(Role.RECEIVER)
                                                     .attachSourceAddress(queueName)
                                                     .attachTarget(null)
                                                     .attach().consumeResponse()
                                                     .getLatestResponse(Attach.class);
            assertThat(responseAttach.getName(), is(notNullValue()));
            assertThat(responseAttach.getHandle().longValue(), is(both(greaterThanOrEqualTo(0L)).and(lessThan(UnsignedInteger.MAX_VALUE.longValue()))));
            assertThat(responseAttach.getRole(), is(Role.SENDER));
            assertThat(responseAttach.getSource(), is(nullValue()));
            assertThat(responseAttach.getTarget(), is(nullValue()));

            final Detach responseDetach = interaction.consumeResponse().getLatestResponse(Detach.class);
            assertThat(responseDetach.getClosed(), is(true));
            assertThat(responseDetach.getError(), is(notNullValue()));
            assertThat(responseDetach.getError().getCondition(), is(equalTo(AmqpError.INVALID_FIELD)));

            final End endResponse = interaction.flowHandleFromLinkHandle()
                                               .flowEcho(true)
                                               .flow()
                                               .consumeResponse().getLatestResponse(End.class);
            assertThat(endResponse.getError(), is(notNullValue()));
            // QPID-7954
            //assertThat(endResponse.getError().getCondition(), is(equalTo(SessionError.ERRANT_LINK)));
        }
    }
    @Test
    @SpecificationTest(section = "2.6.3",
            description = "Note that if the application chooses not to create a terminus, the session endpoint will"
                          + " still create a link endpoint and issue an attach indicating that the link endpoint has"
                          + " no associated local terminus. In this case, the session endpoint MUST immediately"
                          + " detach the newly created link endpoint.")
    @BrokerSpecific(kind = BrokerAdmin.KIND_BROKER_J)
    public void attachSenderWithNullSource() throws Exception
    {
        String queueName = "testQueue";
        getBrokerAdmin().createQueue(queueName);
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            Interaction interaction = transport.newInteraction();
            final Attach responseAttach = interaction.negotiateOpen()
                                                     .begin().consumeResponse(Begin.class)
                                                     .attachRole(Role.SENDER)
                                                     .attachSource(null)
                                                     .attachTargetAddress(queueName)
                                                     .attachInitialDeliveryCount(UnsignedInteger.ZERO)
                                                     .attach().consumeResponse()
                                                     .getLatestResponse(Attach.class);
            assertThat(responseAttach.getName(), is(notNullValue()));
            assertThat(responseAttach.getHandle().longValue(), is(both(greaterThanOrEqualTo(0L)).and(lessThan(UnsignedInteger.MAX_VALUE.longValue()))));
            assertThat(responseAttach.getRole(), is(Role.RECEIVER));
            assertThat(responseAttach.getSource(), is(nullValue()));
            assertThat(responseAttach.getTarget(), is(nullValue()));

            final Detach responseDetach = interaction.consumeResponse().getLatestResponse(Detach.class);
            assertThat(responseDetach.getClosed(), is(true));
            assertThat(responseDetach.getError(), is(notNullValue()));
            assertThat(responseDetach.getError().getCondition(), is(equalTo(AmqpError.INVALID_FIELD)));

            final End endResponse = interaction.flowHandleFromLinkHandle()
                                               .flowEcho(true)
                                               .flow()
                                               .consumeResponse().getLatestResponse(End.class);
            assertThat(endResponse.getError(), is(notNullValue()));
            // QPID-7954
            //assertThat(endResponse.getError().getCondition(), is(equalTo(SessionError.ERRANT_LINK)));
        }
    }
}
