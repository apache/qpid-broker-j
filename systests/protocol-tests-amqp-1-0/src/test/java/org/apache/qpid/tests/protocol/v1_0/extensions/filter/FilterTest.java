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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assume.assumeThat;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Accepted;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Filter;
import org.apache.qpid.server.protocol.v1_0.type.messaging.JMSSelectorFilter;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Rejected;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Disposition;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.Open;
import org.apache.qpid.server.protocol.v1_0.type.transport.ReceiverSettleMode;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.tests.protocol.Response;
import org.apache.qpid.tests.protocol.SpecificationTest;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.protocol.v1_0.Interaction;
import org.apache.qpid.tests.protocol.v1_0.MessageEncoder;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;
import org.apache.qpid.tests.utils.BrokerSpecific;

public class FilterTest extends BrokerAdminUsingTestBase
{
    public static final String TEST_MESSAGE_CONTENT = "testContent";
    private InetSocketAddress _brokerAddress;
    private String _originalMmsMessageStorePersistence;

    @Before
    public void setUp()
    {
        _originalMmsMessageStorePersistence = System.getProperty("qpid.tests.mms.messagestore.persistence");
        System.setProperty("qpid.tests.mms.messagestore.persistence", "false");

        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
        _brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);
    }

    @After
    public void tearDown()
    {
        if (_originalMmsMessageStorePersistence != null)
        {
            System.setProperty("qpid.tests.mms.messagestore.persistence", _originalMmsMessageStorePersistence);
        }
        else
        {
            System.clearProperty("qpid.tests.mms.messagestore.persistence");
        }
    }

    @Test
    @BrokerSpecific(kind = KIND_BROKER_J)
    @SpecificationTest(section = "3.5.1", description = "A source can restrict the messages transferred from a source by specifying a filter.")
    public void selectorFilter() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateProtocol().consumeResponse()
                       .open().consumeResponse(Open.class)
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
                                                                        TEST_MESSAGE_CONTENT);
                interaction.transferPayload(payload)
                           .transferSettled(true)
                           .transfer();
            }
            interaction.detachClose(true).detach().close().sync();
        }

        try (FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateProtocol().consumeResponse()
                       .open().consumeResponse(Open.class)
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
            assertThat(data, is(equalTo(TEST_MESSAGE_CONTENT)));

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

    private QpidByteBuffer generateMessagePayloadWithApplicationProperties(final Map<String, Object> applicationProperties, String content)
    {
        MessageEncoder messageEncoder = new MessageEncoder();
        messageEncoder.setApplicationProperties(applicationProperties);
        messageEncoder.addData(content);
        return messageEncoder.getPayload();
    }
}
