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

import static org.hamcrest.CoreMatchers.both;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assume.assumeThat;

import java.net.InetSocketAddress;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.protocol.v0_10.transport.ConnectionClose;
import org.apache.qpid.server.protocol.v0_10.transport.ConnectionHeartbeat;
import org.apache.qpid.server.protocol.v0_10.transport.ConnectionOpenOk;
import org.apache.qpid.server.protocol.v0_10.transport.ConnectionSecure;
import org.apache.qpid.server.protocol.v0_10.transport.ConnectionStart;
import org.apache.qpid.server.protocol.v0_10.transport.ConnectionTune;
import org.apache.qpid.tests.protocol.ChannelClosedResponse;
import org.apache.qpid.tests.protocol.SpecificationTest;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;

public class ConnectionTest extends BrokerAdminUsingTestBase
{
    private static final String DEFAULT_LOCALE = "en_US";
    private InetSocketAddress _brokerAddress;

    @Before
    public void setUp()
    {
        _brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);
    }

    @Test
    @SpecificationTest(section = "9.connection.start-ok",
            description = "An AMQP client MUST handle incoming connection.start controls.")
    public void startOk() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateProtocol().consumeResponse()
                       .consumeResponse(ConnectionStart.class)
                       .connection().startOkMechanism(ConnectionInteraction.SASL_MECHANISM_ANONYMOUS).startOk()
                       .consumeResponse().getLatestResponse(ConnectionTune.class);
        }
    }

    @Test
    @SpecificationTest(section = "9.connection.tune-ok",
            description = "This control sends the client's connection tuning parameters to the server."
                          + " Certain fields are negotiated, others provide capability information.")
    public void tuneOkAndOpen() throws Exception
    {
        try(FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateProtocol().consumeResponse()
                       .consumeResponse(ConnectionStart.class)
                       .connection().startOkMechanism(ConnectionInteraction.SASL_MECHANISM_ANONYMOUS).startOk()
                       .consumeResponse(ConnectionTune.class)
                       .connection().tuneOk()
                       .connection().open()
                       .consumeResponse().getLatestResponse(ConnectionOpenOk.class);
        }
    }

    @Test
    @SpecificationTest(section = "9",
            description = "open-connection = C:protocol-header S:START C:START-OK *challenge S:TUNE C:TUNE-OK C:OPEN S:OPEN-OK")
    public void authenticationBypassBySendingTuneOk() throws Exception
    {
        InetSocketAddress brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP);
        try(FrameTransport transport = new FrameTransport(brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateProtocol().consumeResponse()
                       .consumeResponse(ConnectionStart.class)
                       .connection().tuneOk()
                       .connection().open()
                       .consumeResponse().getLatestResponse(ConnectionClose.class);
        }
    }

    @Test
    @SpecificationTest(section = "9",
            description = "open-connection = C:protocol-header S:START C:START-OK *challenge S:TUNE C:TUNE-OK C:OPEN S:OPEN-OK")
    public void authenticationBypassBySendingOpen() throws Exception
    {
        InetSocketAddress brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP);
        try(FrameTransport transport = new FrameTransport(brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateProtocol().consumeResponse().consumeResponse(ConnectionStart.class)
                       .connection().open()
                       .consumeResponse().getLatestResponse(ConnectionClose.class);
        }
    }

    @Test
    @SpecificationTest(section = "9",
            description = "open-connection = C:protocol-header S:START C:START-OK *challenge S:TUNE C:TUNE-OK C:OPEN S:OPEN-OK")
    public void authenticationBypassAfterSendingStartOk() throws Exception
    {
        InetSocketAddress brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP);
        try(FrameTransport transport = new FrameTransport(brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateProtocol().consumeResponse()
                       .consumeResponse(ConnectionStart.class)
                       .connection().startOkMechanism(ConnectionInteraction.SASL_MECHANISM_PLAIN).startOk().consumeResponse(ConnectionSecure.class)
                       .connection().tuneOk()
                       .connection().open()
                       .consumeResponse(ConnectionClose.class, ChannelClosedResponse.class);
        }
    }


    @Test
    @SpecificationTest(section = "9.connection.tune-ok.minimum",
            description = "[...] the minimum negotiated value for max-frame-size is also MIN-MAX-FRAME-SIZE [4096]")
    public void tooSmallFrameSize() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            ConnectionTune response = interaction.negotiateProtocol().consumeResponse()
                                                 .consumeResponse(ConnectionStart.class)
                                                 .connection().startOkMechanism(ConnectionInteraction.SASL_MECHANISM_ANONYMOUS).startOk()
                                                 .consumeResponse().getLatestResponse(ConnectionTune.class);

            interaction.connection().tuneOkChannelMax(response.getChannelMax())
                                    .tuneOkMaxFrameSize(1024)
                                    .tuneOk()
                       .connection().open()
                       .consumeResponse(ConnectionClose.class, ChannelClosedResponse.class);
        }
    }

    @Test
    @SpecificationTest(section = "9.connection.tune-ok.max-frame-size",
            description = "If the client specifies a channel max that is higher than the value provided by the server,"
                          + " the server MUST close the connection without attempting a negotiated close."
                          + " The server may report the error in some fashion to assist implementers.")
    public void tooLargeFrameSize() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            ConnectionTune response = interaction.negotiateProtocol().consumeResponse()
                                                 .consumeResponse(ConnectionStart.class)
                                                 .connection().startOkMechanism(ConnectionInteraction.SASL_MECHANISM_ANONYMOUS).startOk()
                                                 .consumeResponse().getLatestResponse(ConnectionTune.class);

            assumeThat(response.hasMaxFrameSize(), is(true));
            assumeThat(response.getMaxFrameSize(), is(lessThan(0xFFFF)));
            interaction.connection().tuneOkChannelMax(response.getChannelMax())
                                    .tuneOkMaxFrameSize(response.getMaxFrameSize() + 1)
                                    .tuneOk()
                       .connection().open()
                       .consumeResponse(ConnectionClose.class, ChannelClosedResponse.class);
        }
    }

    @Test
    @SpecificationTest(section = "9.connection",
            description = "The heartbeat control may be used to generate artificial network traffic when a connection "
                          + "is idle.")
    public void heartbeating() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            ConnectionTune response = interaction.negotiateProtocol().consumeResponse()
                                                 .consumeResponse(ConnectionStart.class)
                                                 .connection().startOkMechanism(ConnectionInteraction.SASL_MECHANISM_ANONYMOUS).startOk()
                                                 .consumeResponse().getLatestResponse(ConnectionTune.class);

            assumeThat(response.hasHeartbeatMin(), is(true));
            assumeThat(response.hasHeartbeatMax(), is(true));
            assumeThat(response.getHeartbeatMin(), is(greaterThanOrEqualTo(0)));
            assumeThat(response.getHeartbeatMax(), is(greaterThanOrEqualTo(1)));

            final int heartbeatPeriod = 1;

            interaction.connection()
                       .tuneOkChannelMax(response.getChannelMax())
                       .tuneOkMaxFrameSize(response.getMaxFrameSize())
                       .tuneOkHeartbeat(heartbeatPeriod)
                       .tuneOk()
                       .connection().open()
                       .consumeResponse(ConnectionOpenOk.class);

            final long startTime = System.currentTimeMillis();
            interaction.consumeResponse().getLatestResponse(ConnectionHeartbeat.class);
            final long actualHeartbeatDelay = System.currentTimeMillis() - startTime;
            assertThat("Heartbeat not received within expected time frame",
                       ((int)actualHeartbeatDelay / 1000),
                       is(both(greaterThanOrEqualTo(heartbeatPeriod)).and(lessThanOrEqualTo(heartbeatPeriod * 2))));
            interaction.connection().heartbeat();

            interaction.consumeResponse(ConnectionHeartbeat.class)
                       .connection().heartbeat();
        }
    }


    @Test
    @SpecificationTest(section = "9.connection",
            description = "If a connection is idle for more than twice the negotiated heartbeat delay, the peers MAY "
                          + "be considered disconnected.")
    public void heartbeatingIncomingIdle() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            ConnectionTune response = interaction.negotiateProtocol().consumeResponse()
                                                 .consumeResponse(ConnectionStart.class)
                                                 .connection().startOkMechanism(ConnectionInteraction.SASL_MECHANISM_ANONYMOUS).startOk()
                                                 .consumeResponse().getLatestResponse(ConnectionTune.class);

            assumeThat(response.hasHeartbeatMin(), is(true));
            assumeThat(response.hasHeartbeatMax(), is(true));
            assumeThat(response.getHeartbeatMin(), is(greaterThanOrEqualTo(0)));
            assumeThat(response.getHeartbeatMax(), is(greaterThanOrEqualTo(1)));

            final int heartbeatPeriod = 1;

            interaction.connection()
                       .tuneOkChannelMax(response.getChannelMax())
                       .tuneOkMaxFrameSize(response.getMaxFrameSize())
                       .tuneOkHeartbeat(heartbeatPeriod)
                       .tuneOk()
                       .connection().open()
                       .consumeResponse(ConnectionOpenOk.class);

            interaction.consumeResponse().getLatestResponse(ConnectionHeartbeat.class);

            transport.assertNoMoreResponsesAndChannelClosed();
        }
    }
}
