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
package org.apache.qpid.tests.protocol.v1_0.extensions.qpid.message;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.qpid.tests.utils.BrokerAdmin.KIND_BROKER_J;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Disposition;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.LinkError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.protocol.v1_0.Interaction;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;
import org.apache.qpid.tests.utils.BrokerSpecific;
import org.apache.qpid.tests.utils.ConfigItem;

@BrokerSpecific(kind = KIND_BROKER_J)
@ConfigItem(name = "qpid.tests.mms.messagestore.persistence", value = "false", jvm = true)
@ConfigItem(name = "connection.maxTransfersPerDelivery", value = "3")
public class TransferLimitTest extends BrokerAdminUsingTestBase
{
    @BeforeEach
    public void setUp()
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
    }

    @Test
    public void transferLimitExceededClosesLink() throws Exception
    {
        try (final FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .begin().consumeResponse(Begin.class)
                       .attachRole(Role.SENDER)
                       .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                       .attach().consumeResponse(Attach.class)
                       .consumeResponse(Flow.class);

            final Binary deliveryTag = new Binary("testTransfer".getBytes(UTF_8));

            // transfer 1: initial transfer (transferCount=1)
            interaction.transferDeliveryId(UnsignedInteger.ZERO)
                       .transferDeliveryTag(deliveryTag)
                       .transferMore(true)
                       .transferPayloadData("chunk1")
                       .transfer()
                       .sync();

            // transfer 2: continuation (transferCount=2)
            interaction.transferMore(true)
                       .transferPayload(null)
                       .transfer()
                       .sync();

            // transfer 3: continuation (transferCount=3, at limit)
            interaction.transferMore(true)
                       .transferPayload(null)
                       .transfer()
                       .sync();

            // transfer 4: continuation (transferCount=4 > maxTransfersPerDelivery=3)
            interaction.transferMore(true)
                       .transferPayload(null)
                       .transfer();

            final Detach detach = interaction.consume(Detach.class, Flow.class);

            assertThat(detach.getError(), is(notNullValue()));
            assertThat(detach.getError().getCondition(), is(equalTo(LinkError.TRANSFER_LIMIT_EXCEEDED)));
            assertThat(detach.getClosed(), is(equalTo(true)));

            interaction.doCloseConnection();
        }
    }

    @Test
    public void transfersAtExactLimitSucceed() throws Exception
    {
        try (final FrameTransport frameTransport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = frameTransport.newInteraction()
                    .negotiateOpen()
                    .begin()
                    .consumeResponse(Begin.class)
                    .attachRole(Role.SENDER)
                    .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                    .attach().consumeResponse(Attach.class)
                    .consumeResponse(Flow.class);

            final UnsignedInteger deliveryId = UnsignedInteger.ZERO;
            final Binary deliveryTag = new Binary(new byte[0]);

            // transfer 1
            interaction.transferDeliveryId(deliveryId)
                    .transferDeliveryTag(deliveryTag)
                    .transferMore(true)
                    .transferPayloadData("transfer1")
                    .transfer()
                    .sync();

            // transfer 2
            interaction.transferMore(true)
                    .transferPayload(null)
                    .transfer()
                    .sync();

            // transfer 3
            interaction.transferMore(false)
                    .transferPayload(null)
                    .transfer();

            final Disposition disposition = interaction.consume(Disposition.class, Flow.class);

            assertThat(disposition.getFirst(), is(equalTo(deliveryId)));
            assertThat(disposition.getLast(), is(anyOf(nullValue(), equalTo(deliveryId))));
            assertThat(disposition.getSettled(), is(equalTo(true)));

            interaction.doCloseConnection();
        }
    }
}
