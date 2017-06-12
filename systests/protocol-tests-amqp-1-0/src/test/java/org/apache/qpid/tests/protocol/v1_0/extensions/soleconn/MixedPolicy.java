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

import static org.apache.qpid.server.protocol.v1_0.type.extensions.soleconn.SoleConnectionConnectionProperties.SOLE_CONNECTION_FOR_CONTAINER;
import static org.apache.qpid.server.protocol.v1_0.type.extensions.soleconn.SoleConnectionConnectionProperties.SOLE_CONNECTION_ENFORCEMENT_POLICY;
import static org.apache.qpid.server.protocol.v1_0.type.extensions.soleconn.SoleConnectionEnforcementPolicy.CLOSE_EXISTING;
import static org.apache.qpid.server.protocol.v1_0.type.extensions.soleconn.SoleConnectionEnforcementPolicy.REFUSE_CONNECTION;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.net.InetSocketAddress;
import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.transport.Close;
import org.apache.qpid.server.protocol.v1_0.type.transport.Open;
import org.apache.qpid.tests.protocol.v1_0.BrokerAdmin;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.protocol.v1_0.PerformativeResponse;
import org.apache.qpid.tests.protocol.v1_0.ProtocolTestBase;

public class MixedPolicy extends ProtocolTestBase
{
    private InetSocketAddress _brokerAddress;

    @Before
    public void setUp()
    {
        _brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);
    }

    @Test
    public void firstCloseThenRefuse() throws Exception
    {
        try (FrameTransport transport1 = new FrameTransport(_brokerAddress).connect())
        {
            transport1.doProtocolNegotiation();
            Open open = new Open();
            open.setContainerId("testContainerId");
            open.setDesiredCapabilities(new Symbol[]{SOLE_CONNECTION_FOR_CONTAINER});
            open.setProperties(Collections.singletonMap(SOLE_CONNECTION_ENFORCEMENT_POLICY,
                                                        CLOSE_EXISTING));

            transport1.sendPerformative(open);
            PerformativeResponse response = (PerformativeResponse) transport1.getNextResponse();

            assertThat(response, is(notNullValue()));
            assertThat(response.getBody(), is(instanceOf(Open.class)));

            try (FrameTransport transport2 = new FrameTransport(_brokerAddress).connect())
            {
                transport2.doProtocolNegotiation();
                Open open2 = new Open();
                open2.setContainerId("testContainerId");
                open2.setDesiredCapabilities(new Symbol[]{SOLE_CONNECTION_FOR_CONTAINER});
                open2.setProperties(Collections.singletonMap(SOLE_CONNECTION_ENFORCEMENT_POLICY,
                                                             REFUSE_CONNECTION));

                transport2.sendPerformative(open2);

                final PerformativeResponse closeResponse1 = (PerformativeResponse) transport1.getNextResponse();
                assertThat(closeResponse1, is(notNullValue()));
                assertThat(closeResponse1.getBody(), is(instanceOf(Close.class)));

                PerformativeResponse response2 = (PerformativeResponse) transport2.getNextResponse();
                assertThat(response2, is(notNullValue()));
                assertThat(response2.getBody(), is(instanceOf(Open.class)));

                try (FrameTransport transport3 = new FrameTransport(_brokerAddress).connect())
                {
                    transport3.doProtocolNegotiation();
                    Open open3 = new Open();
                    open3.setContainerId("testContainerId");
                    open3.setDesiredCapabilities(new Symbol[]{SOLE_CONNECTION_FOR_CONTAINER});
                    open3.setProperties(Collections.singletonMap(SOLE_CONNECTION_ENFORCEMENT_POLICY,
                                                                 CLOSE_EXISTING));

                    transport3.sendPerformative(open3);

                    PerformativeResponse closeResponse3 = (PerformativeResponse) transport3.getNextResponse();
                    assertThat(closeResponse3, is(notNullValue()));
                    assertThat(closeResponse3.getBody(), is(instanceOf(Open.class)));
                    PerformativeResponse closeResponse3b = (PerformativeResponse) transport3.getNextResponse();
                    assertThat(closeResponse3b, is(notNullValue()));
                    assertThat(closeResponse3b.getBody(), is(instanceOf(Close.class)));
                }
            }
        }
    }

    @Test
    public void firstRefuseThenClose() throws Exception
    {
        try (FrameTransport transport1 = new FrameTransport(_brokerAddress).connect())
        {
            transport1.doProtocolNegotiation();
            Open open = new Open();
            open.setContainerId("testContainerId");
            open.setDesiredCapabilities(new Symbol[]{SOLE_CONNECTION_FOR_CONTAINER});
            open.setProperties(Collections.singletonMap(SOLE_CONNECTION_ENFORCEMENT_POLICY,
                                                        REFUSE_CONNECTION));

            transport1.sendPerformative(open);
            PerformativeResponse response = (PerformativeResponse) transport1.getNextResponse();

            assertThat(response, is(notNullValue()));
            assertThat(response.getBody(), is(instanceOf(Open.class)));

            try (FrameTransport transport2 = new FrameTransport(_brokerAddress).connect())
            {
                transport2.doProtocolNegotiation();
                Open open2 = new Open();
                open2.setContainerId("testContainerId");
                open2.setDesiredCapabilities(new Symbol[]{SOLE_CONNECTION_FOR_CONTAINER});
                open2.setProperties(Collections.singletonMap(SOLE_CONNECTION_ENFORCEMENT_POLICY,
                                                             CLOSE_EXISTING));

                transport2.sendPerformative(open2);

                final PerformativeResponse openResponse2 = (PerformativeResponse) transport2.getNextResponse();
                assertThat(openResponse2, is(notNullValue()));
                assertThat(openResponse2.getBody(), is(instanceOf(Open.class)));
                final PerformativeResponse closeResponse2 = (PerformativeResponse) transport2.getNextResponse();
                assertThat(closeResponse2, is(notNullValue()));
                assertThat(closeResponse2.getBody(), is(instanceOf(Close.class)));
            }
        }
    }
}
