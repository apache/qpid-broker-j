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

import static org.apache.qpid.server.protocol.v1_0.type.extensions.soleconn.SoleConnectionConnectionProperties.SOLE_CONNECTION_ENFORCEMENT_POLICY;
import static org.apache.qpid.server.protocol.v1_0.type.extensions.soleconn.SoleConnectionConnectionProperties.SOLE_CONNECTION_FOR_CONTAINER;
import static org.apache.qpid.server.protocol.v1_0.type.extensions.soleconn.SoleConnectionEnforcementPolicy.CLOSE_EXISTING;
import static org.apache.qpid.server.protocol.v1_0.type.extensions.soleconn.SoleConnectionEnforcementPolicy.REFUSE_CONNECTION;
import static org.apache.qpid.tests.protocol.v1_0.extensions.soleconn.SoleConnectionAsserts.assumeEnforcementPolicyCloseExisting;
import static org.apache.qpid.tests.protocol.v1_0.extensions.soleconn.SoleConnectionAsserts.assumeEnforcementPolicyRefuse;
import static org.apache.qpid.tests.protocol.v1_0.extensions.soleconn.SoleConnectionAsserts.assumeSoleConnectionCapability;

import java.util.Collections;

import org.junit.Test;

import org.apache.qpid.server.protocol.v1_0.type.transport.Close;
import org.apache.qpid.server.protocol.v1_0.type.transport.Open;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.protocol.v1_0.Interaction;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;

public class MixedPolicyTest extends BrokerAdminUsingTestBase
{

    @Test
    public void firstCloseThenRefuse() throws Exception
    {
        try (FrameTransport transport1 = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction1 = transport1.newInteraction();
            interaction1.openContainerId("testContainerId")
                        .openDesiredCapabilities(SOLE_CONNECTION_FOR_CONTAINER)
                        .openProperties(Collections.singletonMap(SOLE_CONNECTION_ENFORCEMENT_POLICY,
                                                                 CLOSE_EXISTING))
                        .negotiateOpen();

            Open responseOpen = interaction1.getLatestResponse(Open.class);
            assumeSoleConnectionCapability(responseOpen);
            assumeEnforcementPolicyCloseExisting(responseOpen);

            try (FrameTransport transport2 = new FrameTransport(getBrokerAdmin()).connect())
            {
                final Interaction interaction2 = transport2.newInteraction();
                interaction2.openContainerId("testContainerId")
                            .openDesiredCapabilities(SOLE_CONNECTION_FOR_CONTAINER)
                            .openProperties(Collections.singletonMap(SOLE_CONNECTION_ENFORCEMENT_POLICY,
                                                                     REFUSE_CONNECTION))
                            .sendOpen().sync();

                interaction1.consumeResponse(Close.class);

                interaction2.consumeResponse(Open.class);

                try (FrameTransport transport3 = new FrameTransport(getBrokerAdmin()).connect())
                {
                    final Interaction interaction3 = transport3.newInteraction();
                    interaction3.openContainerId("testContainerId")
                                .openDesiredCapabilities(SOLE_CONNECTION_FOR_CONTAINER)
                                .openProperties(Collections.singletonMap(SOLE_CONNECTION_ENFORCEMENT_POLICY,
                                                                         CLOSE_EXISTING))
                                .negotiateOpen()
                                .consumeResponse(Close.class);
                }
            }
        }
    }

    @Test
    public void firstRefuseThenClose() throws Exception
    {
        try (FrameTransport transport1 = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction1 = transport1.newInteraction();
            interaction1.openContainerId("testContainerId")
                        .openDesiredCapabilities(SOLE_CONNECTION_FOR_CONTAINER)
                        .openProperties(Collections.singletonMap(SOLE_CONNECTION_ENFORCEMENT_POLICY,
                                                                 REFUSE_CONNECTION))
                        .negotiateOpen();

            final Open responseOpen = interaction1.getLatestResponse(Open.class);
            assumeSoleConnectionCapability(responseOpen);
            assumeEnforcementPolicyRefuse(responseOpen);

            try (FrameTransport transport2 = new FrameTransport(getBrokerAdmin()).connect())
            {
                final Interaction interaction2 = transport2.newInteraction();
                interaction2.openContainerId("testContainerId")
                            .openDesiredCapabilities(SOLE_CONNECTION_FOR_CONTAINER)
                            .openProperties(Collections.singletonMap(SOLE_CONNECTION_ENFORCEMENT_POLICY,
                                                                     CLOSE_EXISTING))
                            .negotiateOpen()
                            .consumeResponse(Close.class);
            }
        }
    }
}
