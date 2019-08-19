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
import static org.apache.qpid.server.protocol.v1_0.type.extensions.soleconn.SoleConnectionEnforcementPolicy.CLOSE_EXISTING;
import static org.apache.qpid.tests.protocol.v1_0.extensions.soleconn.SoleConnectionAsserts.assertEnforcementPolicyCloseExisting;
import static org.apache.qpid.tests.protocol.v1_0.extensions.soleconn.SoleConnectionAsserts.assertResourceLocked;
import static org.apache.qpid.tests.protocol.v1_0.extensions.soleconn.SoleConnectionAsserts.assertSoleConnectionCapability;
import static org.apache.qpid.tests.protocol.v1_0.extensions.soleconn.SoleConnectionAsserts.assumeDetectionPolicyStrong;
import static org.apache.qpid.tests.protocol.v1_0.extensions.soleconn.SoleConnectionAsserts.assumeEnforcementPolicyCloseExisting;
import static org.apache.qpid.tests.protocol.v1_0.extensions.soleconn.SoleConnectionAsserts.assumeSoleConnectionCapability;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.in;

import java.net.InetSocketAddress;
import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.extensions.soleconn.SoleConnectionDetectionPolicy;
import org.apache.qpid.server.protocol.v1_0.type.transport.Close;
import org.apache.qpid.server.protocol.v1_0.type.transport.Open;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.protocol.v1_0.Interaction;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;

public class CloseExistingPolicy extends BrokerAdminUsingTestBase
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
        try (FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            Open responseOpen = transport.newInteraction()
                                         .negotiateProtocol().consumeResponse()
                                         .openContainerId("testContainerId")
                                         .openDesiredCapabilities(SOLE_CONNECTION_FOR_CONTAINER)
                                         .openProperties(Collections.singletonMap(
                                                 SOLE_CONNECTION_ENFORCEMENT_POLICY,
                                                 CLOSE_EXISTING))
                                         .open().consumeResponse()
                                         .getLatestResponse(Open.class);

            assumeSoleConnectionCapability(responseOpen);
            assumeEnforcementPolicyCloseExisting(responseOpen);

            if (responseOpen.getProperties().containsKey(SOLE_CONNECTION_DETECTION_POLICY))
            {
                assertThat(responseOpen.getProperties().get(SOLE_CONNECTION_DETECTION_POLICY),
                           in(new UnsignedInteger[]{SoleConnectionDetectionPolicy.STRONG.getValue(),
                                   SoleConnectionDetectionPolicy.WEAK.getValue()}));
            }
        }
    }

    @Test
    public void existingConnectionClosed() throws Exception
    {
        try (FrameTransport transport1 = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction1 = transport1.newInteraction();
            interaction1.negotiateProtocol().consumeResponse()
                        .openContainerId("testContainerId")
                        .openDesiredCapabilities(SOLE_CONNECTION_FOR_CONTAINER)
                        .openProperties(Collections.singletonMap(SOLE_CONNECTION_ENFORCEMENT_POLICY,
                                                                 CLOSE_EXISTING))
                        .open().consumeResponse(Open.class);

            final Open responseOpen = interaction1.getLatestResponse(Open.class);
            assumeSoleConnectionCapability(responseOpen);
            assumeEnforcementPolicyCloseExisting(responseOpen);

            try (FrameTransport transport2 = new FrameTransport(_brokerAddress).connect())
            {
                final Interaction interaction2 = transport2.newInteraction();
                interaction2.negotiateProtocol().consumeResponse()
                            .openContainerId("testContainerId")
                            .openDesiredCapabilities(SOLE_CONNECTION_FOR_CONTAINER)
                            .openProperties(Collections.singletonMap(SOLE_CONNECTION_ENFORCEMENT_POLICY,
                                                                     CLOSE_EXISTING))
                            .open()
                            .sync();

                assertResourceLocked(interaction1.consumeResponse().getLatestResponse(Close.class));

                final Open responseOpen2 = interaction2.consumeResponse().getLatestResponse(Open.class);
                assertSoleConnectionCapability(responseOpen2);
                assertEnforcementPolicyCloseExisting(responseOpen2);
            }
        }
    }


    @Test
    public void strongDetectionWhenConnectionWithoutSoleConnectionCapabilityOpened() throws Exception
    {
        try (FrameTransport transport1 = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction1 = transport1.newInteraction();
            // Omit setting the desired capability to test weak detection
            interaction1.negotiateProtocol().consumeResponse()
                        .openContainerId("testContainerId")
                        .open().consumeResponse(Open.class);

            final Open responseOpen = interaction1.getLatestResponse(Open.class);
            assumeSoleConnectionCapability(responseOpen);
            assumeDetectionPolicyStrong(responseOpen);

            try (FrameTransport transport2 = new FrameTransport(_brokerAddress).connect())
            {
                final Interaction interaction2 = transport2.newInteraction();
                interaction2.negotiateProtocol().consumeResponse()
                            .openContainerId("testContainerId")
                            .openDesiredCapabilities(SOLE_CONNECTION_FOR_CONTAINER)
                            .openProperties(Collections.singletonMap(SOLE_CONNECTION_ENFORCEMENT_POLICY,
                                                                     CLOSE_EXISTING))
                            .open()
                            .consumeResponse(Open.class);

                final Open responseOpen2 = interaction2.getLatestResponse(Open.class);
                assumeSoleConnectionCapability(responseOpen2);
                assumeEnforcementPolicyCloseExisting(responseOpen2);
                assumeDetectionPolicyStrong(responseOpen2);
                assertResourceLocked(interaction1.consumeResponse().getLatestResponse(Close.class));
            }
        }
    }

    @Test
    public void strongDetection() throws Exception
    {
        try (FrameTransport transport1 = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction1 = transport1.newInteraction();
            Open responseOpen = interaction1.negotiateProtocol().consumeResponse()
                                            .openContainerId("testContainerId")
                                            .openDesiredCapabilities(SOLE_CONNECTION_FOR_CONTAINER)
                                            .openProperties(Collections.singletonMap(
                                                    SOLE_CONNECTION_ENFORCEMENT_POLICY,
                                                    CLOSE_EXISTING))
                                            .open().consumeResponse()
                                            .getLatestResponse(Open.class);

            assumeSoleConnectionCapability(responseOpen);
            assumeEnforcementPolicyCloseExisting(responseOpen);
            assumeDetectionPolicyStrong(responseOpen);

            try (FrameTransport transport2 = new FrameTransport(_brokerAddress).connect())
            {
                final Interaction interaction2 = transport2.newInteraction();
                // Omit setting the desired capability to test strong detection
                interaction2.negotiateProtocol().consumeResponse()
                            .openContainerId("testContainerId")
                            .open().sync();

                assertResourceLocked(interaction1.consumeResponse().getLatestResponse(Close.class));
                assertSoleConnectionCapability(interaction2.consumeResponse().getLatestResponse(Open.class));
            }
        }
    }

}
