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

package org.apache.qpid.tests.protocol.v1_0.extensions.bindmapjms;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.net.InetSocketAddress;
import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.protocol.v1_0.Session_1_0;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.messaging.DeleteOnClose;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Target;
import org.apache.qpid.server.protocol.v1_0.type.messaging.TerminusExpiryPolicy;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.tests.protocol.v1_0.BrokerAdmin;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.protocol.v1_0.PerformativeResponse;
import org.apache.qpid.tests.protocol.v1_0.ProtocolTestBase;
import org.apache.qpid.tests.protocol.v1_0.SpecificationTest;
import org.apache.qpid.tests.protocol.v1_0.Utils;

public class TemporaryDestinationTest extends ProtocolTestBase
{
    private InetSocketAddress _brokerAddress;

    @Before
    public void setUp()
    {
        _brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);
    }

    @Test
    @SpecificationTest(section = "5.3",
            description = "To create a node with the required lifecycle properties, establish a uniquely named sending link with "
                          + "the dynamic field of target set true, the expiry-policy field of target set to symbol “link-detach”, and the "
                          + "dynamic-node-properties field of target containing the “lifetime-policy” symbol key mapped to delete-on-close.")
    public void deleteOnCloseWithLinkDetachForQueue() throws Exception
    {
        deleteOnCloseWithLinkDetach(new Symbol[]{Symbol.valueOf("temporary-queue")});
    }

    @Test
    @SpecificationTest(section = "5.3",
            description = "To create a node with the required lifecycle properties, establish a uniquely named sending link with "
                          + "the dynamic field of target set true, the expiry-policy field of target set to symbol “link-detach”, and the "
                          + "dynamic-node-properties field of target containing the “lifetime-policy” symbol key mapped to delete-on-close.")
    public void deleteOnCloseWithLinkDetachForTopic() throws Exception
    {
        deleteOnCloseWithLinkDetach(new Symbol[]{Symbol.valueOf("temporary-topic")});
    }

    private void deleteOnCloseWithLinkDetach(final Symbol[] targetCapabilities) throws Exception
    {
        String newTemporaryNodeAddress = null;

        try (FrameTransport transport = new FrameTransport(_brokerAddress))
        {
            transport.doBeginSession();

            Attach attach = new Attach();
            attach.setName("testSendingLink");
            attach.setHandle(UnsignedInteger.ZERO);
            attach.setRole(Role.SENDER);
            attach.setInitialDeliveryCount(UnsignedInteger.ZERO);

            attach.setSource(new Source());

            Target target = new Target();
            target.setDynamicNodeProperties(Collections.singletonMap(Session_1_0.LIFETIME_POLICY, new DeleteOnClose()));
            target.setExpiryPolicy(TerminusExpiryPolicy.LINK_DETACH);
            target.setDynamic(true);
            target.setCapabilities(targetCapabilities);
            attach.setTarget(target);

            transport.sendPerformative(attach);

            PerformativeResponse response = (PerformativeResponse) transport.getNextResponse();
            assertThat(response, is(notNullValue()));
            assertThat(response.getFrameBody(), is(instanceOf(Attach.class)));
            final Attach attachResponse = (Attach) response.getFrameBody();
            assertThat(attachResponse.getSource(), is(notNullValue()));
            assertThat(attachResponse.getTarget(), is(notNullValue()));

            newTemporaryNodeAddress = ((Target) attachResponse.getTarget()).getAddress();
            assertThat(newTemporaryNodeAddress, is(notNullValue()));

            assertThat(Utils.doesNodeExist(_brokerAddress, newTemporaryNodeAddress), is(true));

            final PerformativeResponse flowResponse = ((PerformativeResponse) transport.getNextResponse());
            if (flowResponse != null)
            {
                assertThat(flowResponse.getFrameBody(), is(instanceOf(Flow.class)));
            }

            transport.doCloseConnection();
        }

        try (FrameTransport transport = new FrameTransport(_brokerAddress))
        {
            transport.doBeginSession();
            assertThat(Utils.doesNodeExist(_brokerAddress, newTemporaryNodeAddress), is(false));
        }
    }
}
