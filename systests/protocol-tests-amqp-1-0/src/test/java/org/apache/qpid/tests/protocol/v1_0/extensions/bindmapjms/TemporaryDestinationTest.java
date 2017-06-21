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

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.net.InetSocketAddress;
import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.protocol.v1_0.Session_1_0;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.messaging.DeleteOnClose;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Target;
import org.apache.qpid.server.protocol.v1_0.type.messaging.TerminusExpiryPolicy;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.Open;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.tests.protocol.v1_0.BrokerAdmin;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.protocol.v1_0.Interaction;
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
    public void deleteOnCloseWithConnectionCloseForQueue() throws Exception
    {
        deleteOnCloseWithConnectionClose(new Symbol[]{Symbol.valueOf("temporary-queue")});
    }

    @Test
    @SpecificationTest(section = "5.3",
            description = "To create a node with the required lifecycle properties, establish a uniquely named sending link with "
                          + "the dynamic field of target set true, the expiry-policy field of target set to symbol “link-detach”, and the "
                          + "dynamic-node-properties field of target containing the “lifetime-policy” symbol key mapped to delete-on-close.")
    public void deleteOnCloseWithConnectionCloseForTopic() throws Exception
    {
        deleteOnCloseWithConnectionClose(new Symbol[]{Symbol.valueOf("temporary-topic")});
    }

    private void deleteOnCloseWithConnectionClose(final Symbol[] targetCapabilities) throws Exception
    {
        String newTemporaryNodeAddress = null;

        try (FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            Target target = new Target();
            target.setDynamicNodeProperties(Collections.singletonMap(Session_1_0.LIFETIME_POLICY, new DeleteOnClose()));
            target.setExpiryPolicy(TerminusExpiryPolicy.LINK_DETACH);
            target.setDynamic(true);
            target.setCapabilities(targetCapabilities);

            final Interaction interaction = transport.newInteraction();
            final Attach attachResponse = interaction.negotiateProtocol().consumeResponse()
                                                     .open().consumeResponse(Open.class)
                                                     .begin().consumeResponse(Begin.class)
                                                     .attachRole(Role.SENDER)
                                                     .attachTarget(target)
                                                     .attach().consumeResponse()
                                                     .getLatestResponse(Attach.class);

            assertThat(attachResponse.getSource(), is(notNullValue()));
            assertThat(attachResponse.getTarget(), is(notNullValue()));

            newTemporaryNodeAddress = ((Target) attachResponse.getTarget()).getAddress();
            assertThat(newTemporaryNodeAddress, is(notNullValue()));

            assertThat(Utils.doesNodeExist(_brokerAddress, newTemporaryNodeAddress), is(true));

            interaction.consumeResponse().getLatestResponse(Flow.class);

            transport.doCloseConnection();
        }

        assertThat(Utils.doesNodeExist(_brokerAddress, newTemporaryNodeAddress), is(false));
    }
}
