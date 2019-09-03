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
package org.apache.qpid.tests.protocol.v0_10;

import static org.hamcrest.MatcherAssert.assertThat;

import java.nio.charset.StandardCharsets;

import org.hamcrest.core.IsEqual;
import org.junit.Test;

import org.apache.qpid.server.protocol.v0_10.transport.SessionAttached;
import org.apache.qpid.server.protocol.v0_10.transport.SessionDetachCode;
import org.apache.qpid.server.protocol.v0_10.transport.SessionDetached;
import org.apache.qpid.tests.protocol.SpecificationTest;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;

public class SessionTest extends BrokerAdminUsingTestBase
{

    @Test
    @SpecificationTest(section = "9.session.attach",
            description = "Requests that the current transport be attached to the named session.")
    public void attach() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            byte[] sessionName = "test".getBytes(StandardCharsets.UTF_8);
            final int channelId = 1;
            SessionAttached sessionAttached = interaction.negotiateOpen()
                                                         .channelId(channelId)
                                                         .session()
                                                         .attachName(sessionName)
                                                         .attach()
                                                         .consumeResponse()
                                                         .getLatestResponse(SessionAttached.class);
            assertThat(sessionAttached.getName(), IsEqual.equalTo(sessionName));
            assertThat(sessionAttached.getChannel(), IsEqual.equalTo(channelId));
        }
    }


    @Test
    @SpecificationTest(section = "9.session.detach",
            description = "Detaches the current transport from the named session.")
    public void detach() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            byte[] sessionName = "test".getBytes(StandardCharsets.UTF_8);
            final int channelId = 1;
            SessionDetached sessionDetached = interaction.negotiateOpen()
                                                         .channelId(channelId)
                                                         .session()
                                                         .attachName(sessionName)
                                                         .attach()
                                                         .consumeResponse(SessionAttached.class)
                                                         .session()
                                                         .detachName(sessionName)
                                                         .detach()
                                                         .consumeResponse()
                                                         .getLatestResponse(SessionDetached.class);

            assertThat(sessionDetached.getName(), IsEqual.equalTo(sessionName));
            assertThat(sessionDetached.getChannel(), IsEqual.equalTo(channelId));
        }
    }

    @Test
    @SpecificationTest(section = "9.session",
            description = "The transport MUST be attached in order to use any control other than"
                          + " \"attach\", \"attached\", \"detach\", or \"detached\"."
                          + " A peer receiving any other control on a detached transport MUST discard it and send a"
                          + " session.detached with the \"not-attached\" reason code.")
    public void detachUnknownSession() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            byte[] sessionName = "test".getBytes(StandardCharsets.UTF_8);
            final int channelId = 1;
            SessionDetached sessionDetached = interaction.negotiateOpen()
                                                         .channelId(channelId)
                                                         .session()
                                                         .detachName(sessionName)
                                                         .detach()
                                                         .consumeResponse()
                                                         .getLatestResponse(SessionDetached.class);

            assertThat(sessionDetached.getName(), IsEqual.equalTo(sessionName));
            assertThat(sessionDetached.getChannel(), IsEqual.equalTo(channelId));
        }
    }

    @Test
    @SpecificationTest(section = "9.session",
            description = "A session MUST NOT be attached to more than one transport at a time.")
    public void attachSameSessionTwiceDisallowed() throws Exception
    {
        try (FrameTransport transport1 = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction1 = transport1.newInteraction();
            byte[] sessionName = "test".getBytes(StandardCharsets.UTF_8);
            final int channelId1 = 1;
            SessionAttached sessionAttached = interaction1.negotiateOpen()
                                                          .channelId(channelId1)
                                                          .session()
                                                          .attachName(sessionName)
                                                          .attach()
                                                          .consumeResponse()
                                                          .getLatestResponse(SessionAttached.class);

            assertThat(sessionAttached.getName(), IsEqual.equalTo(sessionName));
            assertThat(sessionAttached.getChannel(), IsEqual.equalTo(channelId1));


            try (FrameTransport transport2 = new FrameTransport(getBrokerAdmin()).connect())
            {
                final Interaction interaction2 = transport2.newInteraction();
                final int channelId2 = 2;
                SessionDetached sessionDetached = interaction2.negotiateOpen()
                                                              .channelId(channelId2)
                                                              .session()
                                                              .attachName(sessionName)
                                                              .attach()
                                                              .consumeResponse()
                                                              .getLatestResponse(SessionDetached.class);

                assertThat(sessionDetached.getName(), IsEqual.equalTo(sessionName));
                assertThat(sessionDetached.getCode(), IsEqual.equalTo(SessionDetachCode.SESSION_BUSY));
                assertThat(sessionDetached.getChannel(), IsEqual.equalTo(channelId2));
            }
        }
    }
}
