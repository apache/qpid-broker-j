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
package org.apache.qpid.tests.protocol.v1_0.extensions.qpid.transactiontimeout;

import static org.apache.qpid.tests.utils.BrokerAdmin.KIND_BROKER_J;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Accepted;
import org.apache.qpid.server.protocol.v1_0.type.transaction.TransactionError;
import org.apache.qpid.server.protocol.v1_0.type.transaction.TransactionalState;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Close;
import org.apache.qpid.server.protocol.v1_0.type.transport.Disposition;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.ReceiverSettleMode;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.tests.protocol.Response;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.protocol.v1_0.Interaction;
import org.apache.qpid.tests.protocol.v1_0.Utils;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;
import org.apache.qpid.tests.utils.BrokerSpecific;
import org.apache.qpid.tests.utils.ConfigItem;

@BrokerSpecific(kind = KIND_BROKER_J)
@ConfigItem(name = "virtualhost.storeTransactionOpenTimeoutClose", value = "1000")
public class TransactionTimeoutTest extends BrokerAdminUsingTestBase
{
    @Before
    public void setUp()
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
    }

    @Test
    public void transactionalPostingTimeout() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final UnsignedInteger linkHandle = UnsignedInteger.ONE;

            final Interaction interaction = transport.newInteraction();
            Disposition responseDisposition = interaction.negotiateOpen()
                                                         .begin()
                                                         .consumeResponse(Begin.class)

                                                         .txnAttachCoordinatorLink(UnsignedInteger.ZERO)
                                                         .txnDeclare()

                                                         .attachRole(Role.SENDER)
                                                         .attachTargetAddress(BrokerAdmin.TEST_QUEUE_NAME)
                                                         .attachHandle(linkHandle)
                                                         .attach().consumeResponse(Attach.class)
                                                         .consumeResponse(Flow.class)

                                                         .transferHandle(linkHandle)
                                                         .transferPayloadData(getTestName())
                                                         .transferTransactionalStateFromCurrentTransaction()
                                                         .transfer()
                                                         .consumeResponse(Disposition.class)
                                                         .getLatestResponse(Disposition.class);

            assertThat(responseDisposition.getRole(), is(Role.RECEIVER));
            assertThat(responseDisposition.getSettled(), is(Boolean.TRUE));
            assertThat(responseDisposition.getState(), is(instanceOf(TransactionalState.class)));
            assertThat(((TransactionalState) responseDisposition.getState()).getOutcome(), is(instanceOf(Accepted.class)));

            Close responseClose = interaction.consumeResponse().getLatestResponse(Close.class);
            assertThat(responseClose.getError(), is(notNullValue()));
            assertThat(responseClose.getError().getCondition(), equalTo(TransactionError.TRANSACTION_TIMEOUT));
        }
    }

    @Test
    public void transactionalRetirementTimeout() throws Exception
    {
        Utils.putMessageOnQueue(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME, getTestName());
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            interaction.negotiateOpen()
                       .begin()
                       .consumeResponse(Begin.class)

                       .txnAttachCoordinatorLink(UnsignedInteger.ZERO)
                       .txnDeclare()

                       .attachRole(Role.RECEIVER)
                       .attachHandle(UnsignedInteger.ONE)
                       .attachSourceAddress(BrokerAdmin.TEST_QUEUE_NAME)
                       .attachRcvSettleMode(ReceiverSettleMode.FIRST)
                       .attach()
                       .consumeResponse(Attach.class)

                       .flowIncomingWindow(UnsignedInteger.MAX_VALUE)
                       .flowNextIncomingId(UnsignedInteger.ZERO)
                       .flowOutgoingWindow(UnsignedInteger.ZERO)
                       .flowNextOutgoingId(UnsignedInteger.ZERO)
                       .flowLinkCredit(UnsignedInteger.MAX_VALUE)
                       .flowHandleFromLinkHandle()
                       .flow()

                       .receiveDelivery()
                       .decodeLatestDelivery();

            Object data = interaction.getDecodedLatestDelivery();
            assertThat(data, is(equalTo(getTestName())));

            interaction.dispositionSettled(true)
                       .dispositionRole(Role.RECEIVER)
                       .dispositionTransactionalStateFromCurrentTransaction(new Accepted())
                       .disposition()
                       .sync();

            Response<?> response = interaction.consumeResponse(Close.class, Flow.class).getLatestResponse();
            Close responseClose;
            if (response.getBody() instanceof Close)
            {
                responseClose = (Close) response.getBody();
            }
            else
            {
                responseClose = interaction.consumeResponse().getLatestResponse(Close.class);
            }
            assertThat(responseClose.getError(), is(notNullValue()));
            assertThat(responseClose.getError().getCondition(), equalTo(TransactionError.TRANSACTION_TIMEOUT));
        }
        assertThat(Utils.receiveMessage(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME), is(equalTo(getTestName())));
    }
}
