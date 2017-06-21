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

package org.apache.qpid.tests.protocol.v1_0.transaction;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.net.InetSocketAddress;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Accepted;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Rejected;
import org.apache.qpid.server.protocol.v1_0.type.transaction.Coordinator;
import org.apache.qpid.server.protocol.v1_0.type.transaction.Declare;
import org.apache.qpid.server.protocol.v1_0.type.transaction.Declared;
import org.apache.qpid.server.protocol.v1_0.type.transaction.Discharge;
import org.apache.qpid.server.protocol.v1_0.type.transaction.TransactionErrors;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Disposition;
import org.apache.qpid.server.protocol.v1_0.type.transport.Error;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.Open;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.tests.protocol.v1_0.BrokerAdmin;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.protocol.v1_0.Interaction;
import org.apache.qpid.tests.protocol.v1_0.ProtocolTestBase;
import org.apache.qpid.tests.protocol.v1_0.SpecificationTest;

public class DischargeTest extends ProtocolTestBase
{
    private InetSocketAddress _brokerAddress;

    @Before
    public void setUp()
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
        _brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.ANONYMOUS_AMQP);
    }

    @Test
    @SpecificationTest(section = "4.3",
            description = "If the coordinator is unable to complete the discharge, the coordinator MUST convey the error to the controller "
                          + "as a transaction-error. If the source for the link to the coordinator supports the rejected outcome, then the "
                          + "message MUST be rejected with this outcome carrying the transaction-error.")
    public void dischargeUnknownTransactionIdWhenSourceSupportsRejectedOutcome() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            final Disposition disposition = interaction.negotiateProtocol().consumeResponse()
                                                       .open().consumeResponse(Open.class)
                                                       .begin().consumeResponse(Begin.class)
                                                       .attachRole(Role.SENDER)
                                                       .attachSourceOutcomes(Rejected.REJECTED_SYMBOL)
                                                       .attachTarget(new Coordinator())
                                                       .attach().consumeResponse(Attach.class)
                                                       .consumeResponse(Flow.class)
                                                       .transferPayloadData(new Declare())
                                                       .transfer().consumeResponse()
                                                       .getLatestResponse(Disposition.class);

            assertThat(disposition.getSettled(), is(equalTo(true)));
            assertThat(disposition.getState(), is(instanceOf(Declared.class)));
            assertThat(((Declared) disposition.getState()).getTxnId(), is(notNullValue()));

            interaction.consumeResponse(Flow.class);

            final Discharge discharge = new Discharge();
            discharge.setTxnId(new Binary("nonExistingTransaction".getBytes(UTF_8)));
            final Disposition dischargeDisposition = interaction.transferDeliveryId(UnsignedInteger.ONE)
                                                                .transferDeliveryTag(new Binary("discharge".getBytes(UTF_8)))
                                                                .transferPayloadData(discharge)
                                                                .transfer().consumeResponse()
                                                                .getLatestResponse(Disposition.class);
            assertThat(dischargeDisposition.getState(), is(instanceOf(Rejected.class)));
            final Error error = ((Rejected) dischargeDisposition.getState()).getError();
            assertThat(error, is(notNullValue()));
            assertThat(error.getCondition(), is(equalTo(TransactionErrors.UNKNOWN_ID)));
        }
    }

    @Test
    @SpecificationTest(section = "4.3",
            description = "If the coordinator is unable to complete the discharge, the coordinator MUST convey the error to the controller "
                          + "as a transaction-error. [...] If the source does not support "
                          + "the rejected outcome, the transactional resource MUST detach the link to the coordinator, with the detach "
                          + "performative carrying the transaction-error.")
    public void dischargeUnknownTransactionIdWhenSourceDoesNotSupportRejectedOutcome() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(_brokerAddress).connect())
        {
            final Interaction interaction = transport.newInteraction();
            final Disposition disposition = interaction.negotiateProtocol().consumeResponse()
                                                       .open().consumeResponse(Open.class)
                                                       .begin().consumeResponse(Begin.class)
                                                       .attachRole(Role.SENDER)
                                                       .attachSourceOutcomes(Accepted.ACCEPTED_SYMBOL)
                                                       .attachTarget(new Coordinator())
                                                       .attach().consumeResponse(Attach.class)
                                                       .consumeResponse(Flow.class)
                                                       .transferPayloadData(new Declare())
                                                       .transfer().consumeResponse()
                                                       .getLatestResponse(Disposition.class);


            assertThat(disposition.getSettled(), is(equalTo(true)));
            assertThat(disposition.getState(), is(instanceOf(Declared.class)));
            assertThat(((Declared) disposition.getState()).getTxnId(), is(notNullValue()));

            interaction.consumeResponse(Flow.class);

            final Discharge discharge = new Discharge();
            discharge.setTxnId(new Binary("nonExistingTransaction".getBytes(UTF_8)));
            final Detach detachResponse = interaction.transferDeliveryId(UnsignedInteger.ONE)
                                                                .transferDeliveryTag(new Binary("discharge".getBytes(UTF_8)))
                                                                .transferPayloadData(discharge)
                                                                .transfer().consumeResponse()
                                                                .getLatestResponse(Detach.class);
            Error error = detachResponse.getError();
            assertThat(error, is(notNullValue()));
            assertThat(error.getCondition(), is(equalTo(TransactionErrors.UNKNOWN_ID)));
        }
    }
}
