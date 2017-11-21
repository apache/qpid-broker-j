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

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import org.hamcrest.core.IsEqual;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.qpid.server.protocol.v0_10.transport.SessionAttached;
import org.apache.qpid.server.protocol.v0_10.transport.SessionDetached;
import org.apache.qpid.tests.protocol.SpecificationTest;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;

public class SessionTest extends BrokerAdminUsingTestBase
{
    private InetSocketAddress _brokerAddress;

    @Before
    public void setUp()
    {
        _brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);
    }

    @Test
    @SpecificationTest(section = "9.session.attach",
            description = "Requests that the current transport be attached to the named session.")
    public void attach() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            byte[] sessionName = "test".getBytes(StandardCharsets.UTF_8);
            SessionAttached sessionAttached = interaction.openAnonymousConnection()
                                                         .channelId(1)
                                                         .session()
                                                         .attachName(sessionName)
                                                         .attach()
                                                         .consumeResponse()
                                                         .getLatestResponse(SessionAttached.class);
            assertThat(sessionAttached.getName(), IsEqual.equalTo(sessionName));
        }
    }


    @Test
    @SpecificationTest(section = "9.session.detach",
            description = "Detaches the current transport from the named session.")
    public void detach() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            byte[] sessionName = "test".getBytes(StandardCharsets.UTF_8);
            SessionDetached sessionDetached = interaction.openAnonymousConnection()
                                                         .channelId(1)
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
        }
    }


    @Ignore("QPID-8047")
    @Test
    @SpecificationTest(section = "9.session",
            description = "The transport MUST be attached in order to use any control other than"
                          + " \"attach\", \"attached\", \"detach\", or \"detached\"."
                          + " A peer receiving any other control on a detached transport MUST discard it and send a"
                          + " session.detached with the \"not-attached\" reason code.")
    public void detachUnknownSession() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            byte[] sessionName = "test".getBytes(StandardCharsets.UTF_8);
            SessionDetached sessionDetached = interaction.openAnonymousConnection()
                                                         .channelId(1)
                                                         .session()
                                                         .detachName(sessionName)
                                                         .detach()
                                                         .consumeResponse()
                                                         .getLatestResponse(SessionDetached.class);

            assertThat(sessionDetached.getName(), IsEqual.equalTo(sessionName));
        }
    }



}
