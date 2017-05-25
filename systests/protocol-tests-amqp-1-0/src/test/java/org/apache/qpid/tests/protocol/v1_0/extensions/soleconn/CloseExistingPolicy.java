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
import static org.apache.qpid.server.protocol.v1_0.type.extensions.soleconn.SoleConnectionConnectionProperties.SOLE_CONNECTION_DETECTION_POLICY;
import static org.apache.qpid.server.protocol.v1_0.type.extensions.soleconn.SoleConnectionConnectionProperties.SOLE_CONNECTION_ENFORCEMENT_POLICY;
import static org.apache.qpid.server.protocol.v1_0.type.extensions.soleconn.SoleConnectionEnforcementPolicy.CLOSE_EXISTING;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
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
import org.apache.qpid.tests.protocol.v1_0.BrokerAdmin;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.protocol.v1_0.PerformativeResponse;
import org.apache.qpid.tests.protocol.v1_0.ProtocolTestBase;

public class CloseExistingPolicy extends ProtocolTestBase
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
        try (FrameTransport transport = new FrameTransport(_brokerAddress))
        {
            transport.doProtocolNegotiation();
            Open open = new Open();
            open.setContainerId("testContainerId");
            open.setDesiredCapabilities(new Symbol[]{SOLE_CONNECTION_FOR_CONTAINER});
            open.setProperties(Collections.singletonMap(SOLE_CONNECTION_ENFORCEMENT_POLICY,
                                                        CLOSE_EXISTING));

            transport.sendPerformative(open);
            PerformativeResponse response = (PerformativeResponse) transport.getNextResponse();

            assertThat(response, is(notNullValue()));
            assertThat(response.getFrameBody(), is(instanceOf(Open.class)));
            Open responseOpen = (Open) response.getFrameBody();
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
    public void existingConnectionClosed() throws Exception
    {
        try (FrameTransport transport1 = new FrameTransport(_brokerAddress))
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
            assertThat(response.getFrameBody(), is(instanceOf(Open.class)));

            try (FrameTransport transport2 = new FrameTransport(_brokerAddress))
            {
                transport2.doProtocolNegotiation();
                Open open2 = new Open();
                open2.setContainerId("testContainerId");
                open2.setDesiredCapabilities(new Symbol[]{SOLE_CONNECTION_FOR_CONTAINER});
                open2.setProperties(Collections.singletonMap(SOLE_CONNECTION_ENFORCEMENT_POLICY,
                                                            CLOSE_EXISTING));

                transport2.sendPerformative(open2);

                final PerformativeResponse closeResponse1 = (PerformativeResponse) transport1.getNextResponse();
                assertThat(closeResponse1, is(notNullValue()));
                assertThat(closeResponse1.getFrameBody(), is(instanceOf(Close.class)));
                Close close1 = (Close) closeResponse1.getFrameBody();
                assertThat(close1.getError(), is(notNullValue()));
                assertThat(close1.getError().getCondition(), is(equalTo(AmqpError.RESOURCE_LOCKED)));
                assertThat(close1.getError().getInfo(), is(equalTo(Collections.singletonMap(Symbol.valueOf("sole-connection-enforcement"), true))));

                PerformativeResponse response2 = (PerformativeResponse) transport2.getNextResponse();
                assertThat(response2, is(notNullValue()));
                assertThat(response2.getFrameBody(), is(instanceOf(Open.class)));
                Open responseOpen2 = (Open) response2.getFrameBody();
                assertThat(Arrays.asList(responseOpen2.getOfferedCapabilities()), hasItem(SOLE_CONNECTION_FOR_CONTAINER));
                if (responseOpen2.getProperties().containsKey(SOLE_CONNECTION_DETECTION_POLICY))
                {
                    assertThat(responseOpen2.getProperties().get(SOLE_CONNECTION_DETECTION_POLICY),
                               isIn(new UnsignedInteger[]{SoleConnectionDetectionPolicy.STRONG.getValue(),
                                       SoleConnectionDetectionPolicy.WEAK.getValue()}));
                }
            }
        }
    }


    @Test
    public void weakDetection() throws Exception
    {
        try (FrameTransport transport1 = new FrameTransport(_brokerAddress))
        {
            transport1.doProtocolNegotiation();
            Open open = new Open();
            open.setContainerId("testContainerId");
            // Omit setting the desired capability to test weak detection

            transport1.sendPerformative(open);
            PerformativeResponse response = (PerformativeResponse) transport1.getNextResponse();

            assertThat(response, is(notNullValue()));
            assertThat(response.getFrameBody(), is(instanceOf(Open.class)));

            try (FrameTransport transport2 = new FrameTransport(_brokerAddress))
            {
                transport2.doProtocolNegotiation();
                Open open2 = new Open();
                open2.setContainerId("testContainerId");
                open2.setDesiredCapabilities(new Symbol[]{SOLE_CONNECTION_FOR_CONTAINER});
                open2.setProperties(Collections.singletonMap(SOLE_CONNECTION_ENFORCEMENT_POLICY,
                                                             CLOSE_EXISTING));

                transport2.sendPerformative(open2);

                final PerformativeResponse closeResponse1 = (PerformativeResponse) transport1.getNextResponse();
                assertThat(closeResponse1, is(notNullValue()));
                assertThat(closeResponse1.getFrameBody(), is(instanceOf(Close.class)));
                Close close1 = (Close) closeResponse1.getFrameBody();
                assertThat(close1.getError(), is(notNullValue()));
                assertThat(close1.getError().getCondition(), is(equalTo(AmqpError.RESOURCE_LOCKED)));
                assertThat(close1.getError().getInfo(), is(equalTo(Collections.singletonMap(Symbol.valueOf("sole-connection-enforcement"), true))));

                PerformativeResponse response2 = (PerformativeResponse) transport2.getNextResponse();
                assertThat(response2, is(notNullValue()));
                assertThat(response2.getFrameBody(), is(instanceOf(Open.class)));
                Open responseOpen2 = (Open) response2.getFrameBody();
                assertThat(Arrays.asList(responseOpen2.getOfferedCapabilities()), hasItem(SOLE_CONNECTION_FOR_CONTAINER));
                if (responseOpen2.getProperties().containsKey(SOLE_CONNECTION_DETECTION_POLICY))
                {
                    assertThat(responseOpen2.getProperties().get(SOLE_CONNECTION_DETECTION_POLICY),
                               isIn(new UnsignedInteger[]{SoleConnectionDetectionPolicy.STRONG.getValue(),
                                       SoleConnectionDetectionPolicy.WEAK.getValue()}));
                }
            }
        }
    }

    @Test
    public void strongDetection() throws Exception
    {
        try (FrameTransport transport1 = new FrameTransport(_brokerAddress))
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
            assertThat(response.getFrameBody(), is(instanceOf(Open.class)));
            Open responseOpen = (Open) response.getFrameBody();
            assertThat(Arrays.asList(responseOpen.getOfferedCapabilities()), hasItem(SOLE_CONNECTION_FOR_CONTAINER));
            if (responseOpen.getProperties().containsKey(SOLE_CONNECTION_DETECTION_POLICY))
            {
                assumeThat(responseOpen.getProperties().get(SOLE_CONNECTION_DETECTION_POLICY),
                           is(equalTo(SoleConnectionDetectionPolicy.STRONG.getValue())));
            }

            try (FrameTransport transport2 = new FrameTransport(_brokerAddress))
            {
                transport2.doProtocolNegotiation();
                Open open2 = new Open();
                open2.setContainerId("testContainerId");
                // Omit setting the desired capability to test strong detection

                transport2.sendPerformative(open2);

                final PerformativeResponse closeResponse1 = (PerformativeResponse) transport1.getNextResponse();
                assertThat(closeResponse1, is(notNullValue()));
                assertThat(closeResponse1.getFrameBody(), is(instanceOf(Close.class)));
                Close close1 = (Close) closeResponse1.getFrameBody();
                assertThat(close1.getError(), is(notNullValue()));
                assertThat(close1.getError().getCondition(), is(equalTo(AmqpError.RESOURCE_LOCKED)));
                assertThat(close1.getError().getInfo(), is(equalTo(Collections.singletonMap(Symbol.valueOf("sole-connection-enforcement"), true))));

                PerformativeResponse response2 = (PerformativeResponse) transport2.getNextResponse();
                assertThat(response2, is(notNullValue()));
                assertThat(response2.getFrameBody(), is(instanceOf(Open.class)));
                Open responseOpen2 = (Open) response2.getFrameBody();
                assertThat(Arrays.asList(responseOpen2.getOfferedCapabilities()), hasItem(SOLE_CONNECTION_FOR_CONTAINER));
                if (responseOpen2.getProperties().containsKey(SOLE_CONNECTION_DETECTION_POLICY))
                {
                    assertThat(responseOpen2.getProperties().get(SOLE_CONNECTION_DETECTION_POLICY),
                               isIn(new UnsignedInteger[]{SoleConnectionDetectionPolicy.STRONG.getValue(),
                                       SoleConnectionDetectionPolicy.WEAK.getValue()}));
                }
            }
        }
    }

}
