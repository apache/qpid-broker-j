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

package org.apache.qpid.tests.protocol.v1_0.extensions.soleconn;

import static org.apache.qpid.server.protocol.v1_0.type.extensions.soleconn.SoleConnectionConnectionProperties.SOLE_CONNECTION_DETECTION_POLICY;
import static org.apache.qpid.server.protocol.v1_0.type.extensions.soleconn.SoleConnectionConnectionProperties.SOLE_CONNECTION_ENFORCEMENT_POLICY;
import static org.apache.qpid.server.protocol.v1_0.type.extensions.soleconn.SoleConnectionConnectionProperties.SOLE_CONNECTION_FOR_CONTAINER;
import static org.apache.qpid.server.protocol.v1_0.type.extensions.soleconn.SoleConnectionEnforcementPolicy.REFUSE_CONNECTION;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isIn;
import static org.junit.Assume.assumeThat;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.extensions.soleconn.SoleConnectionDetectionPolicy;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Close;
import org.apache.qpid.server.protocol.v1_0.type.transport.Open;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.protocol.v1_0.Interaction;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;

public class RefuseConnectionPolicy extends BrokerAdminUsingTestBase
{
    private InetSocketAddress _brokerAddress;

    @Before
    public void setUp()
    {
        _brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);
    }

    @Test
    public void basicNegotiation() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(_brokerAddress).connect();)
        {
            final Interaction interaction = transport.newInteraction();
            final Open responseOpen = interaction.negotiateProtocol().consumeResponse()
                                                 .openContainerId("testContainerId")
                                                 .openDesiredCapabilities(SOLE_CONNECTION_FOR_CONTAINER)
                                                 .openProperties(Collections.singletonMap(
                                                         SOLE_CONNECTION_ENFORCEMENT_POLICY,
                                                         REFUSE_CONNECTION))
                                                 .open().consumeResponse()
                                                 .getLatestResponse(Open.class);

            assertThat(Arrays.asList(responseOpen.getOfferedCapabilities()), hasItem(SOLE_CONNECTION_FOR_CONTAINER));
            if (responseOpen.getProperties().containsKey(SOLE_CONNECTION_DETECTION_POLICY))
            {
                assertThat(responseOpen.getProperties().get(SOLE_CONNECTION_DETECTION_POLICY),
                           isIn(new UnsignedInteger[]{SoleConnectionDetectionPolicy.STRONG.getValue(),
                                   SoleConnectionDetectionPolicy.WEAK.getValue()}));
            }
        }
    }

    @Test
    public void newConnectionRefused() throws Exception
    {
        try (FrameTransport transport1 = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction1 = transport1.newInteraction();
            interaction1.negotiateProtocol().consumeResponse()
                        .openContainerId("testContainerId")
                        .openDesiredCapabilities(SOLE_CONNECTION_FOR_CONTAINER)
                        .openProperties(Collections.singletonMap(SOLE_CONNECTION_ENFORCEMENT_POLICY,
                                                                 REFUSE_CONNECTION))
                        .open().consumeResponse(Open.class);

            try (FrameTransport transport2 = new FrameTransport(_brokerAddress).connect())
            {
                final Interaction interaction2 = transport2.newInteraction();
                final Open responseOpen2 = interaction2.negotiateProtocol().consumeResponse()
                                                       .openContainerId("testContainerId")
                                                       .openDesiredCapabilities(SOLE_CONNECTION_FOR_CONTAINER)
                                                       .openProperties(Collections.singletonMap(
                                                               SOLE_CONNECTION_ENFORCEMENT_POLICY,
                                                               REFUSE_CONNECTION))
                                                       .open().consumeResponse()
                                                       .getLatestResponse(Open.class);
                assertThat(Arrays.asList(responseOpen2.getOfferedCapabilities()),
                           hasItem(SOLE_CONNECTION_FOR_CONTAINER));
                assertThat(responseOpen2.getProperties(),
                           hasKey(Symbol.valueOf("amqp:connection-establishment-failed")));
                assertThat(responseOpen2.getProperties().get(Symbol.valueOf("amqp:connection-establishment-failed")),
                           is(true));

                final Close close2 = interaction2.consumeResponse().getLatestResponse(Close.class);
                assertThat(close2.getError(), is(notNullValue()));
                assertThat(close2.getError().getCondition(), is(equalTo(AmqpError.INVALID_FIELD)));
                assertThat(close2.getError().getInfo(),
                           is(equalTo(Collections.singletonMap(Symbol.valueOf("invalid-field"),
                                                               Symbol.valueOf("container-id")))));
            }
        }
    }


    @Test
    public void weakDetection() throws Exception
    {
        try (FrameTransport transport1 = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction1 = transport1.newInteraction();
            // Omit setting the desired capability to test weak detection
            interaction1.negotiateProtocol().consumeResponse()
                        .openContainerId("testContainerId")
                        .open().consumeResponse(Open.class);

            try (FrameTransport transport2 = new FrameTransport(_brokerAddress).connect())
            {
                final Interaction interaction2 = transport2.newInteraction();
                final Open responseOpen2 = interaction2.negotiateProtocol().consumeResponse()
                                                       .openContainerId("testContainerId")
                                                       .openDesiredCapabilities(SOLE_CONNECTION_FOR_CONTAINER)
                                                       .openProperties(Collections.singletonMap(
                                                               SOLE_CONNECTION_ENFORCEMENT_POLICY,
                                                               REFUSE_CONNECTION))
                                                       .open().consumeResponse()
                                                       .getLatestResponse(Open.class);
                assertThat(Arrays.asList(responseOpen2.getOfferedCapabilities()),
                           hasItem(SOLE_CONNECTION_FOR_CONTAINER));
                assertThat(responseOpen2.getProperties(),
                           hasKey(Symbol.valueOf("amqp:connection-establishment-failed")));
                assertThat(responseOpen2.getProperties().get(Symbol.valueOf("amqp:connection-establishment-failed")),
                           is(true));

                final Close close2 = interaction2.consumeResponse().getLatestResponse(Close.class);
                assertThat(close2.getError(), is(notNullValue()));
                assertThat(close2.getError().getCondition(), is(equalTo(AmqpError.INVALID_FIELD)));
                assertThat(close2.getError().getInfo(),
                           is(equalTo(Collections.singletonMap(Symbol.valueOf("invalid-field"),
                                                               Symbol.valueOf("container-id")))));
            }
        }
    }

    @Test
    public void strongDetection() throws Exception
    {
        try (FrameTransport transport1 = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction1 = transport1.newInteraction();
            final Open responseOpen = interaction1.negotiateProtocol().consumeResponse()
                                                  .openContainerId("testContainerId")
                                                  .openDesiredCapabilities(SOLE_CONNECTION_FOR_CONTAINER)
                                                  .openProperties(Collections.singletonMap(
                                                          SOLE_CONNECTION_ENFORCEMENT_POLICY,
                                                          REFUSE_CONNECTION))
                                                  .open().consumeResponse()
                                                  .getLatestResponse(Open.class);
            assertThat(Arrays.asList(responseOpen.getOfferedCapabilities()), hasItem(SOLE_CONNECTION_FOR_CONTAINER));
            if (responseOpen.getProperties().containsKey(SOLE_CONNECTION_DETECTION_POLICY))
            {
                assumeThat(responseOpen.getProperties().get(SOLE_CONNECTION_DETECTION_POLICY),
                           is(equalTo(SoleConnectionDetectionPolicy.STRONG.getValue())));
            }

            try (FrameTransport transport2 = new FrameTransport(_brokerAddress).connect())
            {
                final Interaction interaction2 = transport2.newInteraction();
                // Omit setting the desired capability to test strong detection
                final Open responseOpen2 = interaction2.negotiateProtocol().consumeResponse()
                                                       .openContainerId("testContainerId")
                                                       .open().consumeResponse()
                                                       .getLatestResponse(Open.class);

                assertThat(Arrays.asList(responseOpen2.getOfferedCapabilities()),
                           hasItem(SOLE_CONNECTION_FOR_CONTAINER));
                assertThat(responseOpen2.getProperties(),
                           hasKey(Symbol.valueOf("amqp:connection-establishment-failed")));
                assertThat(responseOpen2.getProperties().get(Symbol.valueOf("amqp:connection-establishment-failed")),
                           is(true));

                final Close close2 = interaction2.consumeResponse().getLatestResponse(Close.class);
                assertThat(close2.getError(), is(notNullValue()));
                assertThat(close2.getError().getCondition(), is(equalTo(AmqpError.INVALID_FIELD)));
                assertThat(close2.getError().getInfo(),
                           is(equalTo(Collections.singletonMap(Symbol.valueOf("invalid-field"),
                                                               Symbol.valueOf("container-id")))));
            }
        }
    }

    @Test
    public void refuseIsDefault() throws Exception
    {
        try (FrameTransport transport1 = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction1 = transport1.newInteraction();
            // Omit setting the enforcement policy explicitly. The default is refuse.
            interaction1.negotiateProtocol().consumeResponse()
                        .openContainerId("testContainerId")
                        .openDesiredCapabilities(SOLE_CONNECTION_FOR_CONTAINER)
                        .open().consumeResponse(Open.class);

            try (FrameTransport transport2 = new FrameTransport(_brokerAddress).connect())
            {
                final Interaction interaction2 = transport2.newInteraction();
                // Omit setting the enforcement policy explicitly. The default is refuse.
                final Open responseOpen2 = interaction2.negotiateProtocol().consumeResponse()
                                                       .openContainerId("testContainerId")
                                                       .openDesiredCapabilities(SOLE_CONNECTION_FOR_CONTAINER)
                                                       .open().consumeResponse()
                                                       .getLatestResponse(Open.class);

                assertThat(Arrays.asList(responseOpen2.getOfferedCapabilities()),
                           hasItem(SOLE_CONNECTION_FOR_CONTAINER));
                assertThat(responseOpen2.getProperties(),
                           hasKey(Symbol.valueOf("amqp:connection-establishment-failed")));
                assertThat(responseOpen2.getProperties().get(Symbol.valueOf("amqp:connection-establishment-failed")),
                           is(true));

                final Close close2 = interaction2.consumeResponse().getLatestResponse(Close.class);
                assertThat(close2.getError(), is(notNullValue()));
                assertThat(close2.getError().getCondition(), is(equalTo(AmqpError.INVALID_FIELD)));
                assertThat(close2.getError().getInfo(),
                           is(equalTo(Collections.singletonMap(Symbol.valueOf("invalid-field"),
                                                               Symbol.valueOf("container-id")))));
            }
        }
    }
}
