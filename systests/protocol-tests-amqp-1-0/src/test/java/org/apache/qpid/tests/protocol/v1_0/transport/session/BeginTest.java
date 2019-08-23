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

package org.apache.qpid.tests.protocol.v1_0.transport.session;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assume.assumeThat;

import org.junit.Test;

import org.apache.qpid.server.protocol.v1_0.type.ErrorCarryingFrameBody;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedShort;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Close;
import org.apache.qpid.server.protocol.v1_0.type.transport.ConnectionError;
import org.apache.qpid.server.protocol.v1_0.type.transport.End;
import org.apache.qpid.server.protocol.v1_0.type.transport.Error;
import org.apache.qpid.server.protocol.v1_0.type.transport.Open;
import org.apache.qpid.tests.protocol.Response;
import org.apache.qpid.tests.protocol.SpecificationTest;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.protocol.v1_0.Interaction;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;

public class BeginTest extends BrokerAdminUsingTestBase
{

    @Test
    @SpecificationTest(section = "1.3.4",
            description = "mandatory [...] a non null value for the field is always encoded.")
    public void emptyBegin() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Response<?> response =  transport.newInteraction()
                                           .negotiateOpen()
                                           .beginNextOutgoingId(null)
                                           .beginIncomingWindow(null)
                                           .beginOutgoingWindow(null)
                                           .begin().consumeResponse()
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
    @SpecificationTest(section = "2.5.1",
            description = "Sessions are established by creating a session endpoint, assigning it to an unused channel number, "
                          + "and sending a begin announcing the association of the session endpoint with the outgoing channel.")
    public void successfulBegin() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final UnsignedShort channel = UnsignedShort.valueOf(37);
            Interaction interaction = transport.newInteraction();
            Begin responseBegin = interaction
                                           .negotiateOpen()
                                           .sessionChannel(channel)
                                           .begin().consumeResponse()
                                           .getLatestResponse(Begin.class);

            assertThat(responseBegin.getRemoteChannel(), equalTo(channel));
            assertThat(responseBegin.getIncomingWindow(), is(instanceOf(UnsignedInteger.class)));
            assertThat(responseBegin.getOutgoingWindow(), is(instanceOf(UnsignedInteger.class)));
            assertThat(responseBegin.getNextOutgoingId(), is(instanceOf(UnsignedInteger.class)));

            interaction.end().consumeResponse(End.class).close().consumeResponse(Close.class);
        }
    }

    @Test
    @SpecificationTest(section = "2.7.1",
            description = "A peer MUST not use channel numbers outside the range that its partner can handle."
                          + "A peer that receives a channel number outside the supported range MUST close "
                          + "the connection with the framing-error error-code..")
    public void channelMax() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            final int ourChannelMax = 5;
            final Open responseOpen = interaction.openChannelMax(UnsignedShort.valueOf(ourChannelMax))
                                                 .negotiateOpen().getLatestResponse(Open.class);

            final UnsignedShort remoteChannelMax = responseOpen.getChannelMax();
            assumeThat(remoteChannelMax, is(notNullValue()));
            assumeThat(remoteChannelMax.intValue(), is(lessThan(UnsignedShort.MAX_VALUE.intValue())));

            final int illegalSessionChannel =  remoteChannelMax.intValue() + 1;

            final Close responseClose = interaction.sessionChannel(UnsignedShort.valueOf(illegalSessionChannel))
                                                   .begin().consumeResponse()
                                                   .getLatestResponse(Close.class);
            assertThat(responseClose.getError(), is(notNullValue()));
            assertThat(responseClose.getError().getCondition(), equalTo(ConnectionError.FRAMING_ERROR));
        }
    }
}
