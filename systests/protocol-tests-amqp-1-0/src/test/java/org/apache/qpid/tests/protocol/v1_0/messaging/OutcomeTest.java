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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.net.InetSocketAddress;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Modified;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Open;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.protocol.v1_0.Interaction;
import org.apache.qpid.tests.protocol.v1_0.SpecificationTest;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;

public class OutcomeTest extends BrokerAdminUsingTestBase
{
    private InetSocketAddress _brokerAddress;

    @Before
    public void setUp()
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
        _brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);
    }


    @Test
    @SpecificationTest(section = "3.4.5", description = "If the undeliverable-here is set, then any messages released"
                                                        + " MUST NOT be redelivered to the modifying link endpoint.")
    public void modifiedOutcomeWithUndeliverableHere() throws Exception
    {
        getBrokerAdmin().putMessageOnQueue(BrokerAdmin.TEST_QUEUE_NAME, "message1");
        getBrokerAdmin().putMessageOnQueue(BrokerAdmin.TEST_QUEUE_NAME, "message2");

        try (FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction()
                                                     .negotiateProtocol().consumeResponse()
                                                     .open().consumeResponse(Open.class)
                                                     .begin().consumeResponse(Begin.class)
                                                     .attachRole(Role.RECEIVER)
                                                     .attachSourceAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                                     .attach().consumeResponse(Attach.class)
                                                     .flowIncomingWindow(UnsignedInteger.ONE)
                                                     .flowLinkCredit(UnsignedInteger.ONE)
                                                     .flowHandleFromLinkHandle()
                                                     .flow()
                                                     .receiveDelivery()

                                                     .decodeLatestDelivery();

            Object firstDeliveryPayload = interaction.getDecodedLatestDelivery();
            assertThat(firstDeliveryPayload, is(equalTo("message1")));

            Modified modifiedOutcome = new Modified();
            modifiedOutcome.setUndeliverableHere(Boolean.TRUE);
            interaction.dispositionSettled(true)
                       .dispositionRole(Role.RECEIVER)
                       .dispositionFirstFromLatestDelivery()
                       .dispositionState(modifiedOutcome)
                       .disposition()
                       .flowIncomingWindow(UnsignedInteger.valueOf(2))
                       .flowLinkCredit(UnsignedInteger.valueOf(2))
                       .flowNextIncomingIdFromLatestDelivery()
                       .flow()
                       .receiveDelivery()
                       .decodeLatestDelivery();
            ;

            Object secondDeliveryPayload = interaction.getDecodedLatestDelivery();
            assertThat(secondDeliveryPayload, is(equalTo("message2")));

            // verify that no unexpected performative is received by closing
            transport.doCloseConnection();
        }
    }
}
