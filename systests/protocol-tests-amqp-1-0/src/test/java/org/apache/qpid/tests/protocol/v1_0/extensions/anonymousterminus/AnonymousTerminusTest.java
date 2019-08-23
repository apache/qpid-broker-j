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
package org.apache.qpid.tests.protocol.v1_0.extensions.anonymousterminus;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItemInArray;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;
import static org.junit.Assume.assumeThat;

import java.nio.charset.StandardCharsets;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.DeliveryState;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Accepted;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Properties;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Rejected;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Target;
import org.apache.qpid.server.protocol.v1_0.type.transaction.TransactionError;
import org.apache.qpid.server.protocol.v1_0.type.transaction.TransactionalState;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Disposition;
import org.apache.qpid.server.protocol.v1_0.type.transport.Error;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.Open;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.server.util.StringUtil;
import org.apache.qpid.tests.protocol.Response;
import org.apache.qpid.tests.protocol.SpecificationTest;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.protocol.v1_0.Interaction;
import org.apache.qpid.tests.protocol.v1_0.MessageEncoder;
import org.apache.qpid.tests.protocol.v1_0.Utils;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;

public class AnonymousTerminusTest extends BrokerAdminUsingTestBase
{
    private static final Symbol ANONYMOUS_RELAY = Symbol.valueOf("ANONYMOUS-RELAY");
    private static final Symbol DELIVERY_TAG = Symbol.valueOf("delivery-tag");

    private Binary _deliveryTag;

    @Before
    public void setUp()
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
        _deliveryTag = new Binary("testTag".getBytes(StandardCharsets.UTF_8));
    }

    @SpecificationTest(section = "Using the Anonymous Terminus for Message Routing. 2.2. Sending A Message",
            description = "Messages sent over links into a routing node will be"
                          + " forwarded to the node referenced in the to field of properties of the message"
                          + " just as if a direct link has been established to that node.")
    @Test
    public void transferPreSettledToKnownDestination() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = openInteractionWithAnonymousRelayCapability(transport);

            interaction.begin()
                       .consumeResponse(Begin.class)

                       .attachRole(Role.SENDER)
                       .attach().consumeResponse(Attach.class)
                       .consumeResponse(Flow.class)
                       .assertLatestResponse(Flow.class, this::assumeSufficientCredits)
                       .transferDeliveryId()
                       .transferPayload(generateMessagePayloadToDestination(BrokerAdmin.TEST_QUEUE_NAME))
                       .transferSettled(Boolean.TRUE)
                       .transferDeliveryTag(_deliveryTag)
                       .transfer()
                       .detachEndCloseUnconditionally();

            assertThat(Utils.receiveMessage(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME),
                       is(equalTo(getTestName())));
        }
    }

    @SpecificationTest(section = "Using the Anonymous Terminus for Message Routing. 2.2.2 Routing Errors",
            description = "It is possible that a message sent to a routing node has an address in the to field"
                          + " of properties which, if used in the address field of target of an attach,"
                          + " would result in an unsuccessful link establishment (for example,"
                          + " if the address cannot be resolved to a node). In this case the routing node"
                          + " MUST communicate the error back to the sender of the message."
                          + " [...] the message has already been settled by the sender,"
                          + " then the routing node MUST detach the link with an error."
                          + " [...] the info field of error MUST contain an entry with symbolic key delivery-tag"
                          + " and binary value of the delivery-tag of the message which caused the failure.")
    @Test
    public void transferPreSettledToUnknownDestination() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = openInteractionWithAnonymousRelayCapability(transport);

            interaction.begin()
                       .consumeResponse(Begin.class)

                       .attachRole(Role.SENDER)
                       .attach().consumeResponse(Attach.class)
                       .consumeResponse(Flow.class)
                       .assertLatestResponse(Flow.class, this::assumeSufficientCredits)
                       .transferDeliveryId()
                       .transferPayload(generateMessagePayloadToDestination(getNonExistingDestinationName()))
                       .transferSettled(Boolean.TRUE)
                       .transferDeliveryTag(_deliveryTag)
                       .transfer();

            final Detach detach = interaction.consume(Detach.class, Flow.class);
            Error error = detach.getError();
            assertThat(error, is(notNullValue()));
            assertThat(error.getCondition(), is(equalTo(AmqpError.NOT_FOUND)));
            assertThat(error.getInfo(), is(notNullValue()));
            assertThat(error.getInfo().get(DELIVERY_TAG), is(equalTo(_deliveryTag)));
        }
    }

    @SpecificationTest(section = "Using the Anonymous Terminus for Message Routing. 2.2.2 Routing Errors",
            description = "It is possible that a message sent to a routing node has an address in the to field"
                          + " of properties which, if used in the address field of target of an attach,"
                          + " would result in an unsuccessful link establishment (for example,"
                          + " if the address cannot be resolved to a node). In this case the routing node"
                          + " MUST communicate the error back to the sender of the message."
                          + " If the source of the link supports the rejected outcome,"
                          + " and the message has not already been settled by the sender, then the routing node"
                          + " MUST reject the message."
                          + " [...] the info field of error MUST contain an entry with symbolic key delivery-tag"
                          + " and binary value of the delivery-tag of the message which caused the failure.")
    @Test
    public void transferUnsettledToUnknownDestinationWhenRejectedOutcomeSupportedBySource() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = openInteractionWithAnonymousRelayCapability(transport);

            interaction.begin()
                       .consumeResponse(Begin.class)

                       .attachRole(Role.SENDER)
                       .attachSourceOutcomes(Accepted.ACCEPTED_SYMBOL, Rejected.REJECTED_SYMBOL)
                       .attach().consumeResponse(Attach.class)
                       .consumeResponse(Flow.class)
                       .assertLatestResponse(Flow.class, this::assumeSufficientCredits)
                       .transferDeliveryId()
                       .transferPayload(generateMessagePayloadToDestination(getNonExistingDestinationName()))
                       .transferDeliveryTag(_deliveryTag)
                       .transfer();

            final Disposition disposition = interaction.consume(Disposition.class, Flow.class);

            assertThat(disposition.getSettled(), is(true));

            DeliveryState dispositionState = disposition.getState();
            assertThat(dispositionState, is(instanceOf(Rejected.class)));

            Rejected rejected = (Rejected)dispositionState;
            Error error = rejected.getError();
            assertThat(error, is(notNullValue()));
            assertThat(error.getCondition(), is(oneOf(AmqpError.NOT_FOUND, AmqpError.NOT_ALLOWED)));
            assertThat(error.getInfo(), is(notNullValue()));
            assertThat(error.getInfo().get(DELIVERY_TAG), is(equalTo(_deliveryTag)));
        }
    }

    @SpecificationTest(section = "Using the Anonymous Terminus for Message Routing. 2.2.2 Routing Errors",
            description = "It is possible that a message sent to a routing node has an address in the to field"
                          + " of properties which, if used in the address field of target of an attach,"
                          + " would result in an unsuccessful link establishment (for example,"
                          + " if the address cannot be resolved to a node). In this case the routing node"
                          + " MUST communicate the error back to the sender of the message."
                          + " [...]"
                          + " If the source of the link does not support the rejected outcome,"
                          + " [...] then the routing node MUST detach the link with an error."
                          + " [...] the info field of error MUST contain an entry with symbolic key delivery-tag"
                          + " and binary value of the delivery-tag of the message which caused the failure.")
    @Test
    public void transferUnsettledToUnknownDestinationWhenRejectedOutcomeNotSupportedBySource() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = openInteractionWithAnonymousRelayCapability(transport);

            interaction.begin()
                       .consumeResponse(Begin.class)

                       .attachRole(Role.SENDER)
                       .attachSourceOutcomes(Accepted.ACCEPTED_SYMBOL)
                       .attach().consumeResponse(Attach.class)
                       .consumeResponse(Flow.class)
                       .assertLatestResponse(Flow.class, this::assumeSufficientCredits)
                       .transferDeliveryId()
                       .transferPayload(generateMessagePayloadToDestination(getNonExistingDestinationName()))
                       .transferDeliveryTag(_deliveryTag)
                       .transfer();

            final Detach detach = interaction.consume(Detach.class, Flow.class);
            Error error = detach.getError();
            assertThat(error, is(notNullValue()));
            assertThat(error.getCondition(), is(equalTo(AmqpError.NOT_FOUND)));
            assertThat(error.getInfo(), is(notNullValue()));
            assertThat(error.getInfo().get(DELIVERY_TAG), is(equalTo(_deliveryTag)));
        }
    }

    @SpecificationTest(section = "Using the Anonymous Terminus for Message Routing. 2.2. Sending A Message",
            description = "Messages sent over links into a routing node will be"
                          + " forwarded to the node referenced in the to field of properties of the message"
                          + " just as if a direct link has been established to that node.")
    @Test
    public void transferPreSettledInTransactionToKnownDestination() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = openInteractionWithAnonymousRelayCapability(transport);
            final UnsignedInteger linkHandle = UnsignedInteger.ONE;
            interaction.begin()
                       .consumeResponse(Begin.class)

                       .txnAttachCoordinatorLink(UnsignedInteger.ZERO, this::coordinatorAttachExpected)
                       .txnDeclare()

                       .attachRole(Role.SENDER)
                       .attachHandle(linkHandle)
                       .attach().consumeResponse(Attach.class)
                       .consumeResponse(Flow.class)
                       .assertLatestResponse(Flow.class, this::assumeSufficientCredits)
                       .transferDeliveryId()
                       .transferHandle(linkHandle)
                       .transferPayload(generateMessagePayloadToDestination(BrokerAdmin.TEST_QUEUE_NAME))
                       .transferDeliveryTag(_deliveryTag)
                       .transferTransactionalStateFromCurrentTransaction()
                       .transferSettled(Boolean.TRUE)
                       .transfer().txnDischarge(false)
                       .detachEndCloseUnconditionally();

            assertThat(interaction.getCoordinatorLatestDeliveryState(), is(instanceOf(Accepted.class)));

            Object receivedMessage = Utils.receiveMessage(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME);
            assertThat(receivedMessage, is(equalTo(getTestName())));
        }
    }

    @Test
    public void transferUnsettledInTransactionToKnownDestination() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = openInteractionWithAnonymousRelayCapability(transport);
            final UnsignedInteger linkHandle = UnsignedInteger.ONE;
            interaction.begin()
                       .consumeResponse(Begin.class)

                       .txnAttachCoordinatorLink(UnsignedInteger.ZERO, this::coordinatorAttachExpected)
                       .txnDeclare()

                       .attachRole(Role.SENDER)
                       .attachHandle(linkHandle)
                       .attach().consumeResponse(Attach.class)
                       .consumeResponse(Flow.class)
                       .assertLatestResponse(Flow.class, this::assumeSufficientCredits)
                       .transferDeliveryId()
                       .transferHandle(linkHandle)
                       .transferPayload(generateMessagePayloadToDestination(BrokerAdmin.TEST_QUEUE_NAME))
                       .transferDeliveryTag(_deliveryTag)
                       .transferTransactionalStateFromCurrentTransaction()
                       .transferSettled(Boolean.FALSE)
                       .transfer();

            final Disposition disposition = interaction.consume(Disposition.class, Flow.class);

            assertThat(disposition.getSettled(), is(true));

            DeliveryState dispositionState = disposition.getState();
            assertThat(dispositionState, is(instanceOf(TransactionalState.class)));

            final TransactionalState receivedTxnState = (TransactionalState) dispositionState;
            assertThat(receivedTxnState.getOutcome(), is(instanceOf(Accepted.class)));

            interaction.txnDischarge(false);

            assertThat(interaction.getCoordinatorLatestDeliveryState(), is(instanceOf(Accepted.class)));

            Object receivedMessage = Utils.receiveMessage(getBrokerAdmin(), BrokerAdmin.TEST_QUEUE_NAME);
            assertThat(receivedMessage, is(equalTo(getTestName())));
        }
    }

    @Test
    public void transferUnsettledInTransactionToUnknownDestinationWhenRejectedOutcomeSupportedBySource() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = openInteractionWithAnonymousRelayCapability(transport);
            final UnsignedInteger linkHandle = UnsignedInteger.ONE;
            interaction.begin()
                       .consumeResponse(Begin.class)

                       .txnAttachCoordinatorLink(UnsignedInteger.ZERO, this::coordinatorAttachExpected)
                       .txnDeclare()

                       .attachRole(Role.SENDER)
                       .attachSourceOutcomes(Accepted.ACCEPTED_SYMBOL, Rejected.REJECTED_SYMBOL)
                       .attachHandle(linkHandle)
                       .attach().consumeResponse(Attach.class)
                       .consumeResponse(Flow.class)
                       .assertLatestResponse(Flow.class, this::assumeSufficientCredits)
                       .transferDeliveryId()
                       .transferHandle(linkHandle)
                       .transferPayload(generateMessagePayloadToDestination(getNonExistingDestinationName()))
                       .transferDeliveryTag(_deliveryTag)
                       .transferTransactionalStateFromCurrentTransaction()
                       .transferSettled(Boolean.FALSE)
                       .transfer();

            final Disposition disposition = interaction.consume(Disposition.class, Flow.class);

            assertThat(disposition.getSettled(), is(true));

            DeliveryState dispositionState = disposition.getState();
            assertThat(dispositionState, is(instanceOf(TransactionalState.class)));

            final TransactionalState receivedTxnState = (TransactionalState) dispositionState;
            assertThat(receivedTxnState.getOutcome(), is(instanceOf(Rejected.class)));

            final Error rejectedError = ((Rejected) receivedTxnState.getOutcome()).getError();
            assertThat(rejectedError.getCondition(), is(equalTo(AmqpError.NOT_FOUND)));
            assertThat(rejectedError.getInfo(), is(notNullValue()));
            assertThat(rejectedError.getInfo().get(DELIVERY_TAG), is(equalTo(_deliveryTag)));

            interaction.txnDischarge(false);

            assertThat(interaction.getCoordinatorLatestDeliveryState(), is(instanceOf(Accepted.class)));
        }
    }

    @Test
    public void transferUnsettledInTransactionToUnknownDestinationWhenRejectedOutcomeNotSupportedBySource() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = openInteractionWithAnonymousRelayCapability(transport);
            final UnsignedInteger linkHandle = UnsignedInteger.ONE;
            interaction.begin()
                       .consumeResponse(Begin.class)

                       .txnAttachCoordinatorLink(UnsignedInteger.ZERO, this::coordinatorAttachExpected)
                       .txnDeclare()

                       .attachRole(Role.SENDER)
                       .attachSourceOutcomes(Accepted.ACCEPTED_SYMBOL)
                       .attachHandle(linkHandle)
                       .attach().consumeResponse(Attach.class)
                       .consumeResponse(Flow.class)
                       .assertLatestResponse(Flow.class, this::assumeSufficientCredits)
                       .transferDeliveryId()
                       .transferHandle(linkHandle)
                       .transferPayload(generateMessagePayloadToDestination(getNonExistingDestinationName()))
                       .transferDeliveryId(UnsignedInteger.valueOf(1))
                       .transferDeliveryTag(_deliveryTag)
                       .transferTransactionalStateFromCurrentTransaction()
                       .transferSettled(Boolean.FALSE)
                       .transfer();

            final Detach senderLinkDetach = interaction.consume(Detach.class, Flow.class);
            Error senderLinkDetachError = senderLinkDetach.getError();
            assertThat(senderLinkDetachError, is(notNullValue()));
            assertThat(senderLinkDetachError.getCondition(), is(equalTo(AmqpError.NOT_FOUND)));
            assertThat(senderLinkDetachError.getInfo(), is(notNullValue()));
            assertThat(senderLinkDetachError.getInfo().get(DELIVERY_TAG), is(equalTo(_deliveryTag)));

            interaction.txnDischarge(false);

            DeliveryState txnDischargeDeliveryState = interaction.getCoordinatorLatestDeliveryState();
            assertThat(txnDischargeDeliveryState, is(instanceOf(Rejected.class)));
            Rejected rejected = (Rejected) txnDischargeDeliveryState;
            Error error = rejected.getError();

            assertThat(error, is(notNullValue()));
            assertThat(error.getCondition(), is(equalTo(TransactionError.TRANSACTION_ROLLBACK)));
        }
    }

    @SpecificationTest(section = "Using the Anonymous Terminus for Message Routing. 2.2.2 Routing Errors",
            description = "It is possible that a message sent to a routing node has an address in the to field"
                          + " of properties which, if used in the address field of target of an attach,"
                          + " would result in an unsuccessful link establishment (for example,"
                          + " if the address cannot be resolved to a node). In this case the routing node"
                          + " MUST communicate the error back to the sender of the message."
                          + " [...]"
                          + " <Not in spec yet>"
                          + " AMQP-140"
                          + " If a message cannot be routed to the destination implied in the \"to:\" field,"
                          + " and the source does not allow for the rejected outcome"
                          + " [...] when messages are being sent within a transaction and have been sent pre-settled."
                          + " In this case the behaviour defined for transactions (of essentially marking"
                          + " the transaction as rollback only) should take precedence. "
                            + ""
                          + " AMQP spec 4.3 Discharging a Transaction"
                          + " If the coordinator is unable to complete the discharge, the coordinator MUST convey"
                          + " the error to the controller as a transaction-error. If the source for the link to"
                          + " the coordinator supports the rejected outcome, then the message MUST be rejected"
                          + " with this outcome carrying the transaction-error.")
    @Test
    public void transferPreSettledInTransactionToUnknownDestinationWhenRejectOutcomeSupportedByTxController()
            throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final UnsignedInteger linkHandle = UnsignedInteger.ONE;
            final Interaction interaction =
                    openInteractionWithAnonymousRelayCapability(transport);

            interaction.begin()
                       .consumeResponse(Begin.class)

                       // attaching coordinator link with supported outcomes Accepted and Rejected
                       .txnAttachCoordinatorLink(UnsignedInteger.ZERO, this::coordinatorAttachExpected)
                       .txnDeclare()

                       .attachRole(Role.SENDER)
                       .attachHandle(linkHandle)
                       .attachSourceOutcomes(Accepted.ACCEPTED_SYMBOL, Rejected.REJECTED_SYMBOL)
                       .attach().consumeResponse(Attach.class)
                       .consumeResponse(Flow.class)
                       .assertLatestResponse(Flow.class, this::assumeSufficientCredits)
                       .transferDeliveryId()
                       .transferHandle(linkHandle)
                       .transferPayload(generateMessagePayloadToDestination(getNonExistingDestinationName()))
                       .transferDeliveryTag(_deliveryTag)
                       .transferTransactionalStateFromCurrentTransaction()
                       .transferSettled(Boolean.TRUE)
                       .transfer();

            interaction.txnDischarge(false);

            DeliveryState txDischargeDeliveryState = interaction.getCoordinatorLatestDeliveryState();
            assertThat(txDischargeDeliveryState, is(instanceOf(Rejected.class)));

            Rejected rejected = (Rejected) txDischargeDeliveryState;
            Error error = rejected.getError();

            assertThat(error, is(notNullValue()));
            assertThat(error.getCondition(), is(equalTo(TransactionError.TRANSACTION_ROLLBACK)));
        }
    }

    @SpecificationTest(section = "Using the Anonymous Terminus for Message Routing. 2.2.2 Routing Errors",
            description = "It is possible that a message sent to a routing node has an address in the to field"
                          + " of properties which, if used in the address field of target of an attach,"
                          + " would result in an unsuccessful link establishment (for example,"
                          + " if the address cannot be resolved to a node). In this case the routing node"
                          + " MUST communicate the error back to the sender of the message."
                          + " [...]"
                          + " <Not in spec yet>"
                          + " AMQP-140"
                          + " If a message cannot be routed to the destination implied in the \"to:\" field,"
                          + " and the source does not allow for the rejected outcome"
                          + " [...] when messages are being sent within a transaction and have been sent pre-settled."
                          + " In this case the behaviour defined for transactions (of essentially marking"
                          + " the transaction as rollback only) should take precedence. "
                          + ""
                          + " AMQP spec 4.3 Discharging a Transaction"
                          + " If the coordinator is unable to complete the discharge, the coordinator MUST convey"
                          + " the error to the controller as a transaction-error."
                          + " [...]"
                          + " If the source does not support the rejected outcome, the transactional resource MUST"
                          + " detach the link to the coordinator, with the detach performative carrying"
                          + " the transaction-error")
    @Test
    public void transferPreSettledInTransactionToUnknownDestinationWhenRejectOutcomeNotSupportedByTxController()
            throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final UnsignedInteger linkHandle = UnsignedInteger.ONE;
            final Interaction interaction =
                    openInteractionWithAnonymousRelayCapability(transport);

            interaction.begin()
                       .consumeResponse(Begin.class)

                       .txnAttachCoordinatorLink(UnsignedInteger.ZERO, this::coordinatorAttachExpected, Accepted.ACCEPTED_SYMBOL)
                       .txnDeclare()

                       .attachRole(Role.SENDER)
                       .attachHandle(linkHandle)
                       .attachTarget(new Target())
                       .attachName("link-" + linkHandle)
                       .attachSourceOutcomes(Accepted.ACCEPTED_SYMBOL, Rejected.REJECTED_SYMBOL)
                       .attach().consumeResponse(Attach.class)
                       .consumeResponse(Flow.class)
                       .assertLatestResponse(Flow.class, this::assumeSufficientCredits)
                       .transferDeliveryId()
                       .transferHandle(linkHandle)
                       .transferPayload(generateMessagePayloadToDestination(getNonExistingDestinationName()))
                       .transferDeliveryTag(_deliveryTag)
                       .transferTransactionalStateFromCurrentTransaction()
                       .transferSettled(Boolean.TRUE)
                       .transfer().txnSendDischarge(false);

            final Detach transactionCoordinatorDetach = interaction.consume(Detach.class, Flow.class);
            Error transactionCoordinatorDetachError = transactionCoordinatorDetach.getError();
            assertThat(transactionCoordinatorDetachError, is(notNullValue()));
            assertThat(transactionCoordinatorDetachError.getCondition(), is(equalTo(TransactionError.TRANSACTION_ROLLBACK)));
        }
    }

    private String getNonExistingDestinationName()
    {
        return String.format("%sNonExisting%s", getTestName(), new StringUtil().randomAlphaNumericString(10));
    }

    private Interaction openInteractionWithAnonymousRelayCapability(final FrameTransport transport) throws Exception
    {
        final Interaction interaction = transport.newInteraction();
        interaction.openDesiredCapabilities(ANONYMOUS_RELAY).negotiateOpen();

        Open open = interaction.getLatestResponse(Open.class);
        assumeThat(open.getOfferedCapabilities(), hasItemInArray((ANONYMOUS_RELAY)));
        return interaction;
    }

    private QpidByteBuffer generateMessagePayloadToDestination(final String destinationName)
    {
        MessageEncoder messageEncoder = new MessageEncoder();
        final Properties properties = new Properties();
        properties.setTo(destinationName);
        messageEncoder.setProperties(properties);
        messageEncoder.addData(getTestName());
        return messageEncoder.getPayload();
    }

    private void assumeSufficientCredits(final Flow flow)
    {
        assumeThat(flow.getLinkCredit(), is(greaterThan(UnsignedInteger.ZERO)));
    }

    private void coordinatorAttachExpected(final Response<?> response)
    {
        assertThat(response, is(notNullValue()));
        assumeThat(response.getBody(), anyOf(instanceOf(Attach.class), instanceOf(Flow.class)));
    }
}
