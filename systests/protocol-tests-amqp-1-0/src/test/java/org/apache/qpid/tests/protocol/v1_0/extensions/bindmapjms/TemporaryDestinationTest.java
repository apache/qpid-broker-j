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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assume.assumeThat;

import java.util.Collections;

import org.hamcrest.Matchers;
import org.junit.Test;

import org.apache.qpid.server.protocol.v1_0.Session_1_0;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.messaging.DeleteOnClose;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Target;
import org.apache.qpid.server.protocol.v1_0.type.messaging.TerminusExpiryPolicy;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
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
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;

public class TemporaryDestinationTest extends BrokerAdminUsingTestBase
{
    private static final Symbol TEMPORARY_QUEUE = Symbol.valueOf("temporary-queue");
    private static final Symbol TEMPORARY_TOPIC = Symbol.valueOf("temporary-topic");

    @Test
    @SpecificationTest(section = "5.3",
            description = "To create a node with the required lifecycle properties, establish a uniquely named sending link with "
                          + "the dynamic field of target set true, the expiry-policy field of target set to symbol “link-detach”, and the "
                          + "dynamic-node-properties field of target containing the “lifetime-policy” symbol key mapped to delete-on-close.")
    public void deleteOnCloseWithConnectionCloseForQueue() throws Exception
    {
        deleteOnCloseWithConnectionClose(new Symbol[]{TEMPORARY_QUEUE});
    }

    @Test
    @SpecificationTest(section = "5.3",
            description = "To create a node with the required lifecycle properties, establish a uniquely named sending link with "
                          + "the dynamic field of target set true, the expiry-policy field of target set to symbol “link-detach”, and the "
                          + "dynamic-node-properties field of target containing the “lifetime-policy” symbol key mapped to delete-on-close.")
    public void deleteOnCloseWithConnectionCloseForTopic() throws Exception
    {
        deleteOnCloseWithConnectionClose(new Symbol[]{TEMPORARY_TOPIC});
    }

    private void deleteOnCloseWithConnectionClose(final Symbol[] targetCapabilities) throws Exception
    {
        String newTemporaryNodeAddress;

        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            Target target = createTarget(targetCapabilities);

            final Interaction interaction = transport.newInteraction();
            final Attach attachResponse = interaction.negotiateOpen()
                                                     .begin().consumeResponse(Begin.class)
                                                     .attachRole(Role.SENDER)
                                                     .attachTarget(target)
                                                     .attach().consumeResponse()
                                                     .assertLatestResponse(this::assumeAttachResponse)
                                                     .getLatestResponse(Attach.class);

            assertThat(attachResponse.getSource(), is(notNullValue()));
            assertThat(attachResponse.getTarget(), is(notNullValue()));

            newTemporaryNodeAddress = ((Target) attachResponse.getTarget()).getAddress();
            assertThat(newTemporaryNodeAddress, is(notNullValue()));

            interaction.consumeResponse().getLatestResponse(Flow.class);

            interaction.doCloseConnection();
        }

        assertThat(Utils.doesNodeExist(getBrokerAdmin(), newTemporaryNodeAddress), is(false));
    }


    @Test
    @SpecificationTest(section = "N/A",
            description = "JMS 2.0."
                          + " 6.2.2. Creating temporary destinations"
                          + "Temporary destinations ( TemporaryQueue or  TemporaryTopic objects) are destinations"
                          + " that are system - generated uniquely for their connection.  Only their own connection"
                          + " is allowed to create consumer objects for them."
                          + ""
                          + "4.1.5. TemporaryQueue"
                          + "A TemporaryQueue is a unique Queue object created for the duration of a connection."
                          + " It is a system-defined queue that can only be consumed by the connection that created it."
                          + ""
                          + "AMQP JMS Mapping."
                          + " 5.2. Destinations And Producers/Consumers"
                          + "[...] type information SHOULD be conveyed when creating producer or consumer links"
                          + " for the application by supplying a terminus capability for the particular Destination"
                          + " type to which the client expects to attach [...]"
                          + "TemporaryQueue Terminus capability : 'temporary-queue'")
    public void createTemporaryQueueReceivingLink() throws Exception
    {
        final Symbol[] capabilities = new Symbol[]{TEMPORARY_QUEUE};
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            Target target = createTarget(capabilities);

            final Interaction interaction = transport.newInteraction();
            final UnsignedInteger senderHandle = UnsignedInteger.ONE;
            final Attach senderAttachResponse = interaction.negotiateOpen()
                                                           .begin().consumeResponse(Begin.class)
                                                           .attachRole(Role.SENDER)
                                                           .attachHandle(senderHandle)
                                                           .attachTarget(target)
                                                           .attach().consumeResponse()
                                                           .assertLatestResponse(this::assumeAttachResponse)
                                                           .getLatestResponse(Attach.class);

            assertThat(senderAttachResponse.getSource(), is(notNullValue()));
            assertThat(senderAttachResponse.getTarget(), is(notNullValue()));

            String newTemporaryNodeAddress = ((Target) senderAttachResponse.getTarget()).getAddress();
            assertThat(newTemporaryNodeAddress, is(notNullValue()));

            interaction.consumeResponse().getLatestResponse(Flow.class);

            final Attach receiverAttachResponse = interaction.attachRole(Role.RECEIVER)
                                                             .attachSource(createSource(newTemporaryNodeAddress,
                                                                                        capabilities))
                                                             .attachHandle(UnsignedInteger.valueOf(2))
                                                             .attach().consumeResponse()
                                                             .getLatestResponse(Attach.class);

            assertThat(receiverAttachResponse.getSource(), is(notNullValue()));
            assertThat(receiverAttachResponse.getSource(), is(notNullValue()));
            assertThat(((Source) receiverAttachResponse.getSource()).getAddress(),
                       is(equalTo(newTemporaryNodeAddress)));

            interaction.doCloseConnection();
        }
    }

    @Test
    @SpecificationTest(section = "N/A",
            description = "JMS 2.0."
                          + " 6.2.2. Creating temporary destinations"
                          + "Temporary destinations ( TemporaryQueue or  TemporaryTopic objects) are destinations"
                          + " that are system - generated uniquely for their connection.  Only their own connection"
                          + " is allowed to create consumer objects for them."
                          + ""
                          + "4.1.5. TemporaryQueue"
                          + "A TemporaryQueue is a unique Queue object created for the duration of a connection."
                          + " It is a system-defined queue that can only be consumed by the connection that created it."
                          + ""
                          + "AMQP JMS Mapping."
                          + " 5.2. Destinations And Producers/Consumers"
                          + "[...] type information SHOULD be conveyed when creating producer or consumer links"
                          + " for the application by supplying a terminus capability for the particular Destination"
                          + " type to which the client expects to attach [...]"
                          + "TemporaryQueue Terminus capability : 'temporary-queue'")
    public void createTemporaryQueueReceivingLinkFromOtherConnectionDisallowed() throws Exception
    {
        final Symbol[] capabilities = new Symbol[]{TEMPORARY_QUEUE};
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            Target target = createTarget(capabilities);

            final Interaction interaction = transport.newInteraction();
            final UnsignedInteger senderHandle = UnsignedInteger.ONE;
            final Attach senderAttachResponse = interaction.negotiateOpen()
                                                           .begin().consumeResponse(Begin.class)
                                                           .attachRole(Role.SENDER)
                                                           .attachHandle(senderHandle)
                                                           .attachTarget(target)
                                                           .attach().consumeResponse()
                                                           .assertLatestResponse(this::assumeAttachResponse)
                                                           .getLatestResponse(Attach.class);

            assertThat(senderAttachResponse.getSource(), is(notNullValue()));
            assertThat(senderAttachResponse.getTarget(), is(notNullValue()));

            String newTemporaryNodeAddress = ((Target) senderAttachResponse.getTarget()).getAddress();
            assertThat(newTemporaryNodeAddress, is(notNullValue()));

            interaction.consumeResponse().getLatestResponse(Flow.class);

            assertReceivingLinkFails(createSource(newTemporaryNodeAddress, capabilities), AmqpError.RESOURCE_LOCKED);

            interaction.doCloseConnection();
        }
    }

    @Test
    @SpecificationTest(section = "N/A",
            description = "JMS 2.0."
                          + " 6.2.2. Creating temporary destinations"
                          + "Temporary destinations ( TemporaryQueue or  TemporaryTopic objects) are destinations"
                          + " that are system - generated uniquely for their connection.  Only their own connection"
                          + " is allowed to create consumer objects for them.")
    public void createTemporaryQueueSendingLinkFromOtherConnectionAllowed() throws Exception
    {
        final Symbol[] capabilities = new Symbol[]{TEMPORARY_QUEUE};
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            Target target = createTarget(capabilities);

            final Interaction interaction = transport.newInteraction();
            final UnsignedInteger senderHandle = UnsignedInteger.ONE;
            final Attach senderAttachResponse = interaction.negotiateOpen()
                                                           .begin().consumeResponse(Begin.class)
                                                           .attachRole(Role.SENDER)
                                                           .attachHandle(senderHandle)
                                                           .attachTarget(target)
                                                           .attach().consumeResponse()
                                                           .assertLatestResponse(this::assumeAttachResponse)
                                                           .getLatestResponse(Attach.class);

            assertThat(senderAttachResponse.getSource(), is(notNullValue()));
            assertThat(senderAttachResponse.getTarget(), is(notNullValue()));

            String newTemporaryNodeAddress = ((Target) senderAttachResponse.getTarget()).getAddress();
            assertThat(newTemporaryNodeAddress, is(notNullValue()));

            interaction.consumeResponse().getLatestResponse(Flow.class);

            assertSendingLinkSucceeds(newTemporaryNodeAddress);

            interaction.doCloseConnection();
        }
    }

    @Test
    @SpecificationTest(section = "N/A",
            description = "JMS 2.0."
                          + " 6.2.2. Creating temporary destinations"
                          + "Temporary destinations ( TemporaryQueue or  TemporaryTopic objects) are destinations"
                          + " that are system - generated uniquely for their connection.  Only their own connection"
                          + " is allowed to create consumer objects for them."
                          + ""
                          + "4.2.7. Temporary topics"
                          + "A TemporaryTopic is a unique Topic object created for the duration of a JMSContext,"
                          + " Connection or TopicConnection . It is a system defined Topic whose messages may be"
                          + " consumed only by the connection that created it."
                          + ""
                          + "AMQP JMS Mapping."
                          + " 5.2. Destinations And Producers/Consumers"
                          + "[...] type information SHOULD be conveyed when creating producer or consumer links"
                          + " for the application by supplying a terminus capability for the particular Destination"
                          + " type to which the client expects to attach"
                          + "TemporaryTopic Terminus capability : 'temporary-topic'")
    public void createTemporaryTopicSubscriptionReceivingLink() throws Exception
    {
        final Symbol[] capabilities = new Symbol[]{TEMPORARY_TOPIC};
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Source source = new Source();
            source.setDynamicNodeProperties(Collections.singletonMap(Session_1_0.LIFETIME_POLICY, new DeleteOnClose()));
            source.setExpiryPolicy(TerminusExpiryPolicy.LINK_DETACH);
            source.setCapabilities(capabilities);
            source.setDynamic(true);

            final Interaction interaction = transport.newInteraction();
            final UnsignedInteger receiverHandle = UnsignedInteger.ONE;
            final Attach receiverAttachResponse = interaction.negotiateOpen()
                                                             .begin().consumeResponse(Begin.class)
                                                             .attachRole(Role.RECEIVER)
                                                             .attachSource(source)
                                                             .attachHandle(receiverHandle)
                                                             .attach().consumeResponse()
                                                             .assertLatestResponse(this::assumeAttachResponse)
                                                             .getLatestResponse(Attach.class);

            assertThat(receiverAttachResponse.getSource(), is(notNullValue()));

            String newTemporaryNodeAddress = ((Source) receiverAttachResponse.getSource()).getAddress();
            assertThat(newTemporaryNodeAddress, is(notNullValue()));

            Target target = new Target();
            target.setDynamicNodeProperties(Collections.singletonMap(Session_1_0.LIFETIME_POLICY, new DeleteOnClose()));
            target.setExpiryPolicy(TerminusExpiryPolicy.LINK_DETACH);
            target.setCapabilities(capabilities);
            target.setAddress(newTemporaryNodeAddress);

            final UnsignedInteger senderHandle = UnsignedInteger.valueOf(2);
            interaction.attachRole(Role.SENDER)
                       .attachHandle(senderHandle)
                       .attachTarget(target)
                       .attach()
                       .consumeResponse(Attach.class)
                       .consumeResponse(Flow.class);

            interaction.doCloseConnection();
        }
    }

    @Test
    @SpecificationTest(section = "N/A",
            description = "JMS 2.0."
                          + " 6.2.2. Creating temporary destinations"
                          + "Temporary destinations ( TemporaryQueue or  TemporaryTopic objects) are destinations"
                          + " that are system - generated uniquely for their connection.  Only their own connection"
                          + " is allowed to create consumer objects for them."
                          + ""
                          + "4.2.7. Temporary topics"
                          + "A TemporaryTopic is a unique Topic object created for the duration of a JMSContext,"
                          + " Connection or TopicConnection . It is a system defined Topic whose messages may be"
                          + " consumed only by the connection that created it."
                          + ""
                          + "AMQP JMS Mapping."
                          + " 5.2. Destinations And Producers/Consumers"
                          + "[...] type information SHOULD be conveyed when creating producer or consumer links"
                          + " for the application by supplying a terminus capability for the particular Destination"
                          + " type to which the client expects to attach"
                          + "TemporaryTopic Terminus capability : 'temporary-topic'")
    public void createTemporaryTopicSubscriptionReceivingLinkFromOtherConnectionDisallowed() throws Exception
    {
        final Symbol[] capabilities = new Symbol[]{TEMPORARY_TOPIC};
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Source source = new Source();
            source.setDynamicNodeProperties(Collections.singletonMap(Session_1_0.LIFETIME_POLICY, new DeleteOnClose()));
            source.setExpiryPolicy(TerminusExpiryPolicy.LINK_DETACH);
            source.setCapabilities(capabilities);
            source.setDynamic(true);

            final Interaction interaction = transport.newInteraction();
            final UnsignedInteger receiverHandle = UnsignedInteger.ONE;
            final Attach receiverAttachResponse = interaction.negotiateOpen()
                                                             .begin().consumeResponse(Begin.class)
                                                             .attachRole(Role.RECEIVER)
                                                             .attachSource(source)
                                                             .attachHandle(receiverHandle)
                                                             .attach().consumeResponse()
                                                             .assertLatestResponse(this::assumeAttachResponse)
                                                             .getLatestResponse(Attach.class);

            assertThat(receiverAttachResponse.getSource(), is(notNullValue()));
            assertThat(receiverAttachResponse.getSource(), is(notNullValue()));

            String newTemporaryNodeAddress = ((Source) receiverAttachResponse.getSource()).getAddress();
            assertThat(newTemporaryNodeAddress, is(notNullValue()));

            assertReceivingLinkFails(createSource(newTemporaryNodeAddress, capabilities), AmqpError.RESOURCE_LOCKED);

            interaction.doCloseConnection();
        }
    }

    @Test
    @SpecificationTest(section = "N/A",
            description = "JMS 2.0."
                          + " 6.2.2. Creating temporary destinations"
                          + "Temporary destinations ( TemporaryQueue or  TemporaryTopic objects) are destinations"
                          + " that are system - generated uniquely for their connection.  Only their own connection"
                          + " is allowed to create consumer objects for them.")
    public void createTemporaryTopicSendingLinkFromOtherConnectionAllowed() throws Exception
    {
        final Symbol[] capabilities = new Symbol[]{TEMPORARY_TOPIC};
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Source source = new Source();
            source.setDynamicNodeProperties(Collections.singletonMap(Session_1_0.LIFETIME_POLICY, new DeleteOnClose()));
            source.setExpiryPolicy(TerminusExpiryPolicy.LINK_DETACH);
            source.setCapabilities(capabilities);
            source.setDynamic(true);

            final Interaction interaction = transport.newInteraction();
            final UnsignedInteger receiverHandle = UnsignedInteger.ONE;
            final Attach receiverAttachResponse = interaction.negotiateOpen()
                                                             .begin().consumeResponse(Begin.class)
                                                             .attachRole(Role.RECEIVER)
                                                             .attachSource(source)
                                                             .attachHandle(receiverHandle)
                                                             .attach().consumeResponse()
                                                             .assertLatestResponse(this::assumeAttachResponse)
                                                             .getLatestResponse(Attach.class);

            assertThat(receiverAttachResponse.getSource(), is(notNullValue()));

            String newTemporaryNodeAddress = ((Source) receiverAttachResponse.getSource()).getAddress();
            assertThat(newTemporaryNodeAddress, is(notNullValue()));

            assertSendingLinkSucceeds(newTemporaryNodeAddress);

            interaction.doCloseConnection();
        }
    }

    private void assertReceivingLinkFails(final Source source, final AmqpError expectedError) throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            final Detach responseDetach = interaction.negotiateOpen()
                                                     .begin()
                                                     .consumeResponse(Begin.class)
                                                     .attachRole(Role.RECEIVER)
                                                     .attachSource(source)
                                                     .attach()
                                                     .consumeResponse(Attach.class)
                                                     .consumeResponse(Detach.class)
                                                     .getLatestResponse(Detach.class);
            assertThat(responseDetach.getClosed(), is(true));
            assertThat(responseDetach.getError(), is(Matchers.notNullValue()));
            assertThat(responseDetach.getError().getCondition(), is(equalTo(expectedError)));
            interaction.doCloseConnection();
        }
    }

    private void assertSendingLinkSucceeds(final String address) throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            Target target = new Target();
            target.setAddress(address);

            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                        .begin()
                        .consumeResponse(Begin.class)
                        .attachRole(Role.SENDER)
                        .attachTarget(target)
                        .attach()
                        .consumeResponse(Attach.class)
                        .consumeResponse(Flow.class);
            interaction.doCloseConnection();
        }
    }

    private Target createTarget(final Symbol[] capabilities)
    {
        Target target = new Target();
        target.setDynamicNodeProperties(Collections.singletonMap(Session_1_0.LIFETIME_POLICY, new DeleteOnClose()));
        target.setExpiryPolicy(TerminusExpiryPolicy.LINK_DETACH);
        target.setDynamic(true);
        target.setCapabilities(capabilities);
        return target;
    }

    private Source createSource(final String name, final Symbol[] capabilities)
    {
        final Source source = new Source();
        source.setDynamicNodeProperties(Collections.singletonMap(Session_1_0.LIFETIME_POLICY, new DeleteOnClose()));
        source.setExpiryPolicy(TerminusExpiryPolicy.LINK_DETACH);
        source.setCapabilities(capabilities);
        source.setAddress(name);
        return source;
    }

    private void assumeAttachResponse(Response<?> response)
    {
        assertThat(response, notNullValue());
        assumeThat(response.getBody(), instanceOf(Attach.class));
    }
}
