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
package org.apache.qpid.tests.protocol.v1_0.extensions.filter;

import static org.apache.qpid.tests.utils.BrokerAdmin.KIND_BROKER_J;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assume.assumeThat;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Accepted;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Filter;
import org.apache.qpid.server.protocol.v1_0.type.messaging.JMSSelectorFilter;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.ReceiverSettleMode;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.tests.protocol.SpecificationTest;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.protocol.v1_0.Interaction;
import org.apache.qpid.tests.protocol.v1_0.MessageEncoder;
import org.apache.qpid.tests.protocol.v1_0.extensions.type.TestFilter;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;
import org.apache.qpid.tests.utils.BrokerSpecific;
import org.apache.qpid.tests.utils.ConfigItem;

@BrokerSpecific(kind = KIND_BROKER_J)
@ConfigItem(name = "qpid.tests.mms.messagestore.persistence", value = "false", jvm = true)
public class FilterTest extends BrokerAdminUsingTestBase
{
    @Before
    public void setUp()
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
    }

    @Test
    @SpecificationTest(section = "3.5.1", description = "A source can restrict the messages transferred from a source by specifying a filter.")
    public void selectorFilter() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .begin().consumeResponse(Begin.class)
                       .attachRole(Role.SENDER)
                       .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                       .attach().consumeResponse(Attach.class)
                       .consumeResponse(Flow.class);
            Flow flow = interaction.getLatestResponse(Flow.class);
            assumeThat("insufficient credit for the test", flow.getLinkCredit().intValue(), is(greaterThan(1)));

            for (int i = 0; i < 2; i++)
            {
                QpidByteBuffer payload =
                        generateMessagePayloadWithApplicationProperties(Collections.singletonMap("index", i),
                                                                        getTestName());
                interaction.transferPayload(payload)
                           .transferSettled(true)
                           .transfer();
            }
            interaction.detachClose(true).detach().close().sync();
        }

        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .begin().consumeResponse(Begin.class)
                       .attachRole(Role.RECEIVER)
                       .attachSourceAddress(BrokerAdmin.TEST_QUEUE_NAME)
                       .attachRcvSettleMode(ReceiverSettleMode.FIRST)
                       .attachSourceFilter(Collections.singletonMap(Symbol.valueOf("selector-filter"),
                                                                    new JMSSelectorFilter("index=1")))
                       .attach().consumeResponse()
                       .flowIncomingWindow(UnsignedInteger.ONE)
                       .flowNextIncomingId(UnsignedInteger.ZERO)
                       .flowOutgoingWindow(UnsignedInteger.ZERO)
                       .flowNextOutgoingId(UnsignedInteger.ZERO)
                       .flowLinkCredit(UnsignedInteger.ONE)
                       .flowHandleFromLinkHandle()
                       .flow();

            Object data = interaction.receiveDelivery().decodeLatestDelivery().getDecodedLatestDelivery();
            assertThat(data, is(equalTo(getTestName())));

            Map<String, Object> applicationProperties = interaction.getLatestDeliveryApplicationProperties();
            assertThat(applicationProperties, is(notNullValue()));
            assertThat(applicationProperties.get("index"), is(equalTo(1)));

            interaction.dispositionSettled(true)
                       .dispositionRole(Role.RECEIVER)
                       .dispositionState(new Accepted())
                       .disposition();
            interaction.close().sync();
        }

    }

    @Test
    @SpecificationTest(section = "3.5.1", description = "")
    public void unsupportedFilter() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            final Map<Symbol, Filter> filters = new HashMap<>();
            filters.put(Symbol.valueOf("selector-filter"), new JMSSelectorFilter("index=1"));
            filters.put(Symbol.valueOf("test-filter"), new TestFilter("foo"));
            final Attach responseAttach = interaction.negotiateOpen()
                                                     .begin().consumeResponse(Begin.class)
                                                     .attachRole(Role.RECEIVER)
                                                     .attachSourceAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                                     .attachSourceFilter(filters)
                                                     .attach().consumeResponse()
                                                     .getLatestResponse(Attach.class);
            assertThat(responseAttach.getName(), is(notNullValue()));
            assertThat(responseAttach.getHandle().longValue(),
                       is(both(greaterThanOrEqualTo(0L)).and(lessThan(UnsignedInteger.MAX_VALUE.longValue()))));
            assertThat(responseAttach.getRole(), is(Role.SENDER));
            assertThat(responseAttach.getSource(), is(nullValue()));
            assertThat(responseAttach.getTarget(), is(nullValue()));

            final Detach responseDetach = interaction.consumeResponse().getLatestResponse(Detach.class);
            assertThat(responseDetach.getClosed(), is(true));
            assertThat(responseDetach.getError(), is(notNullValue()));
            assertThat(responseDetach.getError().getCondition(), is(equalTo(AmqpError.NOT_IMPLEMENTED)));

            interaction.doCloseConnection();
        }
    }

    private QpidByteBuffer generateMessagePayloadWithApplicationProperties(final Map<String, Object> applicationProperties, String content)
    {
        MessageEncoder messageEncoder = new MessageEncoder();
        messageEncoder.setApplicationProperties(applicationProperties);
        messageEncoder.addData(content);
        return messageEncoder.getPayload();
    }
}
