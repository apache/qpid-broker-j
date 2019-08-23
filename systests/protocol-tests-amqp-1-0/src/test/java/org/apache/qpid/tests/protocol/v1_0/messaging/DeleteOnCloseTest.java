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

package org.apache.qpid.tests.protocol.v1_0.messaging;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assume.assumeThat;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Test;

import org.apache.qpid.server.protocol.v1_0.Session_1_0;
import org.apache.qpid.server.protocol.v1_0.type.messaging.DeleteOnClose;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Target;
import org.apache.qpid.server.protocol.v1_0.type.messaging.TerminusExpiryPolicy;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.tests.protocol.Response;
import org.apache.qpid.tests.protocol.SpecificationTest;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.protocol.v1_0.Interaction;
import org.apache.qpid.tests.protocol.v1_0.Utils;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;

public class DeleteOnCloseTest extends BrokerAdminUsingTestBase
{

    @Test
    @SpecificationTest(section = "3.5.10",
            description = "A node dynamically created with this lifetime policy will be deleted at the point that the link which caused its\n"
                          + "creation ceases to exist.")
    public void deleteOnCloseOnSource() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            Source source = new Source();
            source.setDynamicNodeProperties(Collections.singletonMap(Session_1_0.LIFETIME_POLICY, new DeleteOnClose()));
            source.setDynamic(true);
            final Interaction interaction = transport.newInteraction();
            final Attach attachResponse = interaction.negotiateOpen()
                                                     .begin().consumeResponse(Begin.class)
                                                     .attachRole(Role.RECEIVER)
                                                     .attachSource(source)
                                                     .attach().consumeResponse()
                                                     .assertLatestResponse(this::assumeAttach)
                                                     .getLatestResponse(Attach.class);
            assertThat(attachResponse.getSource(), is(notNullValue()));
            final String newTempQueueAddress = ((Source) attachResponse.getSource()).getAddress();

            try
            {
                assertThat(Utils.doesNodeExist(getBrokerAdmin(), newTempQueueAddress), is(true));
            }
            finally
            {
                interaction.detachClose(true).detach().consumeResponse().getLatestResponse(Detach.class);
            }

            assertThat(Utils.doesNodeExist(getBrokerAdmin(), newTempQueueAddress), is(false));
        }
    }

    @Test
    @SpecificationTest(section = "3.5.10",
            description = "A node dynamically created with this lifetime policy will be deleted at the point that the link which caused its\n"
                          + "creation ceases to exist.")
    public void deleteOnCloseOnTarget() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            Target target = new Target();
            target.setDynamicNodeProperties(Collections.singletonMap(Session_1_0.LIFETIME_POLICY, new DeleteOnClose()));
            target.setDynamic(true);
            final Interaction interaction = transport.newInteraction();
            final Attach attachResponse = interaction.negotiateOpen()
                                                     .begin().consumeResponse(Begin.class)
                                                     .attachRole(Role.SENDER)
                                                     .attachTarget(target)
                                                     .attach().consumeResponse()
                                                     .assertLatestResponse(this::assumeAttach)
                                                     .getLatestResponse(Attach.class);
            assertThat(attachResponse.getTarget(), is(notNullValue()));
            final String newTempQueueAddress = ((Target) attachResponse.getTarget()).getAddress();

            assertThat(Utils.doesNodeExist(getBrokerAdmin(), newTempQueueAddress), is(true));

            interaction.consumeResponse().getLatestResponse(Flow.class);
            try
            {
                assertThat(Utils.doesNodeExist(getBrokerAdmin(), newTempQueueAddress), is(true));
            }
            finally
            {
                interaction.detachClose(true).detach().consumeResponse().getLatestResponse(Detach.class);
            }

            assertThat(Utils.doesNodeExist(getBrokerAdmin(), newTempQueueAddress), is(false));
        }
    }

    @Test
    @SpecificationTest(section = "3.5.10",
            description = "A node dynamically created with this lifetime policy will be deleted at the point that the link which caused its\n"
                          + "creation ceases to exist.")
    public void doesNotDeleteOnDetach() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            Source source = new Source();
            source.setDynamicNodeProperties(Collections.singletonMap(Session_1_0.LIFETIME_POLICY, new DeleteOnClose()));
            source.setDynamic(true);
            final Interaction interaction = transport.newInteraction();
            final Attach attachResponse = interaction.negotiateOpen()
                                                     .begin().consumeResponse(Begin.class)
                                                     .attachRole(Role.RECEIVER)
                                                     .attachSource(source)
                                                     .attach().consumeResponse()
                                                     .assertLatestResponse(this::assumeAttach)
                                                     .getLatestResponse(Attach.class);
            assertThat(attachResponse.getSource(), is(notNullValue()));
            final String newTempQueueAddress = ((Source) attachResponse.getSource()).getAddress();

            try
            {
                assertThat(Utils.doesNodeExist(getBrokerAdmin(), newTempQueueAddress), is(true));
            }
            finally
            {
                interaction.detach().consumeResponse().getLatestResponse(Detach.class);
            }

            assertThat(Utils.doesNodeExist(getBrokerAdmin(), newTempQueueAddress), is(true));

            interaction.attach()
                       .consumeResponse(Attach.class)
                       .detachClose(true)
                       .detach()
                       .consumeResponse()
                       .getLatestResponse(Detach.class);
            assertThat(Utils.doesNodeExist(getBrokerAdmin(), newTempQueueAddress), is(false));
        }
    }

    @Test
    public void dynamicNodeIsPersisted() throws Exception
    {
        assumeThat(getBrokerAdmin().supportsRestart(), is(true));

        final String newTempQueueAddress;
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            Source source = new Source();
            source.setDynamicNodeProperties(Collections.singletonMap(Session_1_0.LIFETIME_POLICY, new DeleteOnClose()));
            source.setExpiryPolicy(TerminusExpiryPolicy.NEVER);
            source.setDynamic(true);
            final Interaction interaction = transport.newInteraction();
            final Attach attachResponse = interaction.negotiateOpen()
                                                     .begin().consumeResponse(Begin.class)
                                                     .attachRole(Role.RECEIVER)
                                                     .attachSource(source)
                                                     .attach().consumeResponse()
                                                     .assertLatestResponse(this::assumeAttach)
                                                     .getLatestResponse(Attach.class);
            assertThat(attachResponse.getSource(), is(notNullValue()));
            newTempQueueAddress = ((Source) attachResponse.getSource()).getAddress();

            assertThat(Utils.doesNodeExist(getBrokerAdmin(), newTempQueueAddress), is(true));
        }

        final ListenableFuture<Void> restart = getBrokerAdmin().restart();
        restart.get(BrokerAdmin.RESTART_TIMEOUT, TimeUnit.MILLISECONDS);
        assertThat(Utils.doesNodeExist(getBrokerAdmin(), newTempQueueAddress), is(true));
    }

    private void assumeAttach(final Response<?> response)
    {
        assertThat(response, notNullValue());
        assumeThat(response.getBody(), instanceOf(Attach.class));
    }
}
