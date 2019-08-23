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
package org.apache.qpid.tests.protocol.v1_0.extensions.bindmapjms;

import static org.apache.qpid.tests.utils.BrokerAdmin.KIND_BROKER_J;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import org.junit.Test;

import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
import org.apache.qpid.server.protocol.v1_0.type.messaging.TerminusDurability;
import org.apache.qpid.server.protocol.v1_0.type.messaging.TerminusExpiryPolicy;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.tests.protocol.SpecificationTest;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.protocol.v1_0.Interaction;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;
import org.apache.qpid.tests.utils.BrokerSpecific;

@BrokerSpecific(kind = KIND_BROKER_J)
public class TopicDestinationTest extends BrokerAdminUsingTestBase
{
    private static final Symbol TOPIC = Symbol.valueOf("topic");
    private static final Symbol GLOBAL = Symbol.valueOf("global");
    private static final Symbol SHARED = Symbol.valueOf("shared");

    @Test
    @SpecificationTest(section = "5.2",
            description = "In order to facilitate these actions for the various Destination types that JMS supports,"
                          + " type information SHOULD be conveyed when creating producer or consumer links"
                          + " for the application by supplying a terminus capability for the particular"
                          + " Destination type to which the client expects to attach."
                          + " Destination Type = Topic, terminus capability (type) = topic")
    public void nonSharedVolatileSubscriptionLinkAttachDetach() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Source source = new Source();
            source.setExpiryPolicy(TerminusExpiryPolicy.LINK_DETACH);
            source.setCapabilities(new Symbol[]{TOPIC});
            source.setAddress("amq.topic");
            source.setDurable(TerminusDurability.NONE);

            final Interaction interaction = transport.newInteraction();
            final Attach responseAttach = interaction.negotiateOpen()
                                                     .begin().consumeResponse(Begin.class)
                                                     .attachRole(Role.RECEIVER)
                                                     .attachSource(source)
                                                     .attach().consumeResponse()
                                                     .getLatestResponse(Attach.class);
            assertThat(responseAttach.getName(), is(notNullValue()));
            assertThat(responseAttach.getHandle().longValue(),
                       is(both(greaterThanOrEqualTo(0L)).and(lessThan(UnsignedInteger.MAX_VALUE.longValue()))));
            assertThat(responseAttach.getRole(), is(Role.SENDER));

            final Detach responseDetach = interaction.detachClose(true)
                                                     .detach()
                                                     .consumeResponse()
                                                     .getLatestResponse(Detach.class);
            assertThat(responseDetach.getClosed(), is(equalTo(Boolean.TRUE)));
            assertThat(responseDetach.getError(), is(nullValue()));

            interaction.doCloseConnection();
        }
    }

    @Test
    public void nonSharedDurableSubscriptionLinkAttachDetach() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Source source = new Source();
            source.setExpiryPolicy(TerminusExpiryPolicy.NEVER);
            source.setCapabilities(new Symbol[]{TOPIC});
            source.setAddress("amq.topic");
            source.setDurable(TerminusDurability.UNSETTLED_STATE);

            final Interaction interaction = transport.newInteraction();
            final Attach responseAttach = interaction.negotiateOpen()
                                                     .begin().consumeResponse(Begin.class)
                                                     .attachRole(Role.RECEIVER)
                                                     .attachSource(source)
                                                     .attach().consumeResponse()
                                                     .getLatestResponse(Attach.class);
            assertThat(responseAttach.getName(), is(notNullValue()));
            assertThat(responseAttach.getHandle().longValue(),
                       is(both(greaterThanOrEqualTo(0L)).and(lessThan(UnsignedInteger.MAX_VALUE.longValue()))));
            assertThat(responseAttach.getRole(), is(Role.SENDER));

            final Detach responseDetach = interaction.detachClose(true)
                                                     .detach()
                                                     .consumeResponse()
                                                     .getLatestResponse(Detach.class);
            assertThat(responseDetach.getClosed(), is(equalTo(Boolean.TRUE)));
            assertThat(responseDetach.getError(), is(nullValue()));

            interaction.doCloseConnection();
        }
    }

    @Test
    public void sharedGlobalVolatileSubscriptionLinkAttachDetach() throws Exception
    {
        String subscriptionName = "foo";
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Source source = new Source();
            source.setExpiryPolicy(TerminusExpiryPolicy.LINK_DETACH);
            source.setCapabilities(new Symbol[]{TOPIC, GLOBAL, SHARED});
            source.setAddress("amq.topic");
            source.setDurable(TerminusDurability.NONE);

            final Interaction interaction = transport.newInteraction();
            final Attach responseAttach1 = interaction.negotiateOpen()
                                                      .begin().consumeResponse(Begin.class)
                                                      .attachName(subscriptionName + "|global-volatile")
                                                      .attachHandle(UnsignedInteger.ZERO)
                                                      .attachRole(Role.RECEIVER)
                                                      .attachSource(source)
                                                      .attach().consumeResponse()
                                                      .getLatestResponse(Attach.class);
            assertThat(responseAttach1.getName(), is(notNullValue()));
            assertThat(responseAttach1.getHandle().longValue(),
                       is(both(greaterThanOrEqualTo(0L)).and(lessThan(UnsignedInteger.MAX_VALUE.longValue()))));
            assertThat(responseAttach1.getRole(), is(Role.SENDER));

            final Attach responseAttach2 = interaction.attachName(subscriptionName + "|global-volatile2")
                                                      .attachHandle(UnsignedInteger.ONE)
                                                      .attachRole(Role.RECEIVER)
                                                      .attachSource(source)
                                                      .attach().consumeResponse()
                                                      .getLatestResponse(Attach.class);
            assertThat(responseAttach2.getName(), is(notNullValue()));
            assertThat(responseAttach2.getHandle().longValue(),
                       is(both(greaterThanOrEqualTo(0L)).and(lessThan(UnsignedInteger.MAX_VALUE.longValue()))));
            assertThat(responseAttach2.getRole(), is(Role.SENDER));


            final Detach responseDetach2 = interaction.detachClose(false)
                                                     .detachHandle(UnsignedInteger.ONE)
                                                     .detach()
                                                     .consumeResponse()
                                                     .getLatestResponse(Detach.class);
            assertThat(responseDetach2.getError(), is(nullValue()));


            final Detach responseDetach1 = interaction.detachClose(true)
                                                      .detachHandle(UnsignedInteger.ZERO)
                                                      .detach()
                                                      .consumeResponse()
                                                      .getLatestResponse(Detach.class);
            assertThat(responseDetach1.getClosed(), is(equalTo(Boolean.TRUE)));
            assertThat(responseDetach1.getError(), is(nullValue()));

            interaction.doCloseConnection();
        }
    }

    @Test
    public void sharedGlobalDurableSubscriptionLinkAttachDetach() throws Exception
    {
        String subscriptionName = "foo";
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Source source = new Source();
            source.setExpiryPolicy(TerminusExpiryPolicy.NEVER);
            source.setCapabilities(new Symbol[]{TOPIC, GLOBAL, SHARED});
            source.setAddress("amq.topic");
            source.setDurable(TerminusDurability.CONFIGURATION);

            final Interaction interaction = transport.newInteraction();
            final Attach responseAttach1 = interaction.negotiateOpen()
                                                      .begin().consumeResponse(Begin.class)
                                                      .attachName(subscriptionName + "|global")
                                                      .attachHandle(UnsignedInteger.ZERO)
                                                      .attachRole(Role.RECEIVER)
                                                      .attachSource(source)
                                                      .attach().consumeResponse()
                                                      .getLatestResponse(Attach.class);
            assertThat(responseAttach1.getName(), is(notNullValue()));
            assertThat(responseAttach1.getHandle().longValue(),
                       is(both(greaterThanOrEqualTo(0L)).and(lessThan(UnsignedInteger.MAX_VALUE.longValue()))));
            assertThat(responseAttach1.getRole(), is(Role.SENDER));

            final Attach responseAttach2 = interaction.attachName(subscriptionName + "|global2")
                                                      .attachHandle(UnsignedInteger.ONE)
                                                      .attachRole(Role.RECEIVER)
                                                      .attachSource(source)
                                                      .attach().consumeResponse()
                                                      .getLatestResponse(Attach.class);
            assertThat(responseAttach2.getName(), is(notNullValue()));
            assertThat(responseAttach2.getHandle().longValue(),
                       is(both(greaterThanOrEqualTo(0L)).and(lessThan(UnsignedInteger.MAX_VALUE.longValue()))));
            assertThat(responseAttach2.getRole(), is(Role.SENDER));


            final Detach responseDetach2 = interaction.detachClose(false)
                                                      .detachHandle(UnsignedInteger.ONE)
                                                      .detach()
                                                      .consumeResponse()
                                                      .getLatestResponse(Detach.class);
            assertThat(responseDetach2.getClosed(), is(not(equalTo(Boolean.TRUE))));
            assertThat(responseDetach2.getError(), is(nullValue()));


            final Detach responseDetach1 = interaction.detachClose(false)
                                                      .detachHandle(UnsignedInteger.ZERO)
                                                      .detach()
                                                      .consumeResponse()
                                                      .getLatestResponse(Detach.class);
            assertThat(responseDetach1.getClosed(), is(not(equalTo(Boolean.TRUE))));
            assertThat(responseDetach1.getError(), is(nullValue()));


            final Attach responseAttach3 = interaction.attachName(subscriptionName + "|global")
                                                      .attachHandle(UnsignedInteger.valueOf(2))
                                                      .attachRole(Role.RECEIVER)
                                                      .attachSource(null)
                                                      .attach().consumeResponse()
                                                      .getLatestResponse(Attach.class);
            assertThat(responseAttach3.getName(), is(notNullValue()));
            assertThat(responseAttach3.getHandle().longValue(),
                       is(both(greaterThanOrEqualTo(0L)).and(lessThan(UnsignedInteger.MAX_VALUE.longValue()))));
            assertThat(responseAttach3.getRole(), is(Role.SENDER));
            assertThat(responseAttach3.getSource(), is(notNullValue()));
            assertThat(responseAttach3.getSource(), is(instanceOf(Source.class)));
            assertThat(((Source)responseAttach3.getSource()).getAddress(), is(equalTo("amq.topic")));

            final Detach responseDetach3 = interaction.detachClose(true)
                                                      .detachHandle(UnsignedInteger.valueOf(2))
                                                      .detach()
                                                      .consumeResponse()
                                                      .getLatestResponse(Detach.class);
            assertThat(responseDetach3.getClosed(), is(equalTo(Boolean.TRUE)));
            assertThat(responseDetach3.getError(), is(nullValue()));

            interaction.doCloseConnection();
        }
    }

    @Test
    public void sharedGlobalDurableSubscriptionCloseWithActiveLink() throws Exception
    {
        String subscriptionName = "foo";
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Source source = new Source();
            source.setExpiryPolicy(TerminusExpiryPolicy.NEVER);
            source.setCapabilities(new Symbol[]{TOPIC, GLOBAL, SHARED});
            source.setAddress("amq.topic");
            source.setDurable(TerminusDurability.CONFIGURATION);

            final Interaction interaction = transport.newInteraction();
            final Attach responseAttach1 = interaction.negotiateOpen()
                                                      .begin().consumeResponse(Begin.class)
                                                      .attachName(subscriptionName + "|global")
                                                      .attachHandle(UnsignedInteger.ZERO)
                                                      .attachRole(Role.RECEIVER)
                                                      .attachSource(source)
                                                      .attach().consumeResponse()
                                                      .getLatestResponse(Attach.class);
            assertThat(responseAttach1.getName(), is(notNullValue()));
            assertThat(responseAttach1.getHandle().longValue(),
                       is(both(greaterThanOrEqualTo(0L)).and(lessThan(UnsignedInteger.MAX_VALUE.longValue()))));
            assertThat(responseAttach1.getRole(), is(Role.SENDER));

            final Attach responseAttach2 = interaction.attachName(subscriptionName + "|global2")
                                                      .attachHandle(UnsignedInteger.ONE)
                                                      .attachRole(Role.RECEIVER)
                                                      .attachSource(source)
                                                      .attach().consumeResponse()
                                                      .getLatestResponse(Attach.class);
            assertThat(responseAttach2.getName(), is(notNullValue()));
            assertThat(responseAttach2.getHandle().longValue(),
                       is(both(greaterThanOrEqualTo(0L)).and(lessThan(UnsignedInteger.MAX_VALUE.longValue()))));
            assertThat(responseAttach2.getRole(), is(Role.SENDER));


            final Detach responseDetach1 = interaction.detachClose(true)
                                                      .detachHandle(UnsignedInteger.ZERO)
                                                      .detach()
                                                      .consumeResponse()
                                                      .getLatestResponse(Detach.class);
            assertThat(responseDetach1.getClosed(), is(equalTo(Boolean.TRUE)));
            assertThat(responseDetach1.getError(), is(notNullValue()));
            assertThat(responseDetach1.getError().getCondition(), is(equalTo(AmqpError.RESOURCE_LOCKED)));

            interaction.doCloseConnection();
        }
    }
}
