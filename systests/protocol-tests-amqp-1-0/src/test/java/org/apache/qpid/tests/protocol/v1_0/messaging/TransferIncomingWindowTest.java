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
package org.apache.qpid.tests.protocol.v1_0.messaging;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.End;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.server.protocol.v1_0.type.transport.SessionError;
import org.apache.qpid.tests.protocol.SpecificationTest;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.protocol.v1_0.Interaction;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;
import org.apache.qpid.tests.utils.BrokerSpecific;
import org.apache.qpid.tests.utils.ConfigItem;

@BrokerSpecific(kind = BrokerAdmin.KIND_BROKER_J)
@ConfigItem(name = "connection.sessionCreditWindowSize", value = "0")
public class TransferIncomingWindowTest extends BrokerAdminUsingTestBase
{
    @BeforeEach
    public void setUp()
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
    }

    @Test
    @SpecificationTest(section = "2.5.6",
            description = "The incoming-window limits how many transfer frames the session may receive.")
    public void transferExceedingInitialIncomingWindowEndsSession() throws Exception
    {
        try (final FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final End responseEnd = transport.newInteraction()
                    .negotiateOpen()
                    .begin().consumeResponse(Begin.class)
                    .transferHandle(UnsignedInteger.ZERO)
                    .transferDeliveryId(UnsignedInteger.ZERO)
                    .transferDeliveryTag(new Binary("window-violation".getBytes(UTF_8)))
                    .transferPayloadData("window-violation")
                    .transfer()
                    .consumeResponse().getLatestResponse(End.class);

            assertThat(responseEnd.getError(), is(notNullValue()));
            assertThat(responseEnd.getError().getCondition(), is(equalTo(SessionError.WINDOW_VIOLATION)));
        }
    }

    @Test
    @SpecificationTest(section = "2.5.6",
            description = "The incoming-window limits how many transfer frames the session may receive.")
    public void transferExceedingFlowAdvertisedIncomingWindowEndsSession() throws Exception
    {
        try (final FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .begin().consumeResponse(Begin.class)
                       .attachRole(Role.SENDER)
                       .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                       .attach().consumeResponse(Attach.class)
                       .consumeResponse(Flow.class)
                       .transferDeliveryId(UnsignedInteger.ZERO)
                       .transferDeliveryTag(new Binary("window-violation".getBytes(UTF_8)))
                       .transferPayloadData("window-violation")
                       .transferMore(false)
                       .transfer();

            final End end = interaction.consume(End.class, Detach.class, Flow.class);

            assertThat(end.getError(), is(notNullValue()));
            assertThat(end.getError().getCondition(), is(equalTo(SessionError.WINDOW_VIOLATION)));
        }
    }
}
