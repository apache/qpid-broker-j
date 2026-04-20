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

package org.apache.qpid.tests.protocol.v1_0.security;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.protocol.v1_0.constants.Symbols;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Accepted;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Rejected;
import org.apache.qpid.server.protocol.v1_0.type.security.SaslCode;
import org.apache.qpid.server.protocol.v1_0.type.security.SaslMechanisms;
import org.apache.qpid.server.protocol.v1_0.type.security.SaslOutcome;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Close;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Disposition;
import org.apache.qpid.server.protocol.v1_0.type.transport.End;
import org.apache.qpid.server.protocol.v1_0.type.transport.Error;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.Open;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.server.protocol.v1_0.type.transport.Transfer;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.protocol.v1_0.Interaction;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;
import org.apache.qpid.tests.utils.ConfigItem;

/**
 * Guard test to detect Subject leakage/escalation to SYSTEM across asynchronous execution boundaries.
 * <br>
 * The ACL in config-protocol-tests-1-0-system-privilege-guard.json is intentionally crafted so that:
 * - SYSTEM is allowed to perform ALL operations on QUEUE (bypass detector)
 * - guest is explicitly denied to perform ANY operations on QUEUE
 * - everything else is allowed to keep broker usable for tests
 * <br>
 * If guest ever succeeds at an operation on QUEUE, it is a strong indicator that authorise() observed SYSTEM,
 * i.e. Subject propagation is broken.
 */
@ConfigItem(
        name = "qpid.initialConfigurationLocation",
        value = "classpath:config-protocol-tests-1-0-system-privilege-guard.json",
        jvm = true
)
public class SystemPrivilegeEscalationGuardTest extends BrokerAdminUsingTestBase
{
    private static final String ADMIN_USERNAME = "admin";
    private static final String ADMIN_PASSWORD = "admin";
    private static final String GUEST_USERNAME = "guest";
    private static final String GUEST_PASSWORD = "guest";

    private static final String QUEUE1 = "queue1";

    private static final byte[] SASL_AMQP_HEADER_BYTES = "AMQP\3\1\0\0".getBytes(StandardCharsets.UTF_8);
    private static final byte[] AMQP_HEADER_BYTES = "AMQP\0\1\0\0".getBytes(StandardCharsets.UTF_8);

    private static final Symbol PLAIN = Symbol.getSymbol("PLAIN");
    private static final Set<Symbol> ACCESS_DENIED_CONDITIONS = Set.of(AmqpError.UNAUTHORIZED_ACCESS.getValue(), AmqpError.NOT_ALLOWED.getValue());

    // JMS mapping capability used to request creation of a temporary queue
    private static final Symbol TEMPORARY_QUEUE = Symbol.valueOf("temporary-queue");

    @BeforeEach
    public void setUp()
    {
        getBrokerAdmin().createQueue(QUEUE1);
        getBrokerAdmin().putMessageOnQueue(QUEUE1, "message");
    }

    @Test
    public void guestCannotReceiveFromQueueEvenIfSystemRuleWouldAllow() throws Exception
    {
        adminCanSeeQueue(QUEUE1, 1);

        try (final FrameTransport transport = new FrameTransport(getBrokerAdmin(), BrokerAdmin.PortType.AMQP).connect())
        {
            final Interaction interaction = negotiateOpenAs(transport.newInteraction(), GUEST_USERNAME, GUEST_PASSWORD)
                    .begin().consumeResponse(Begin.class);

            interaction.attachName("guestReceiver")
                    .attachHandle(UnsignedInteger.ONE)
                    .attachRole(Role.RECEIVER)
                    .attachSourceAddress(QUEUE1)
                    .attach();

            final Object attachOutcome = awaitTerminal(interaction,
                    25,
                    Attach.class,
                    Detach.class,
                    Close.class,
                    End.class);

            if (attachOutcome instanceof Attach)
            {
                // Link was established; attempt to actually receive a delivery.
                interaction.flowIncomingWindow(UnsignedInteger.ONE)
                        .flowNextIncomingIdFromPeerLatestSessionBeginAndDeliveryCount()
                        .flowOutgoingWindow(UnsignedInteger.ZERO)
                        .flowNextOutgoingId(UnsignedInteger.ZERO)
                        .flowLinkCredit(UnsignedInteger.ONE)
                        .flowHandleFromLinkHandle()
                        .flow();

                final Object receiveOutcome = awaitTerminal(interaction,
                        50,
                        Transfer.class,
                        Detach.class,
                        Close.class,
                        End.class);

                if (receiveOutcome instanceof Transfer)
                {
                    fail("Guest unexpectedly received a delivery from queue '%s'. This indicates possible privilege escalation."
                            .formatted(QUEUE1));
                }

                assertDeniedOutcome(receiveOutcome,
                        "Guest must not be able to receive from queue '%s'".formatted(QUEUE1));
            }
            else
            {
                assertDeniedOutcome(attachOutcome,
                        "Guest must not be able to attach a receiving link to queue '%s'".formatted(QUEUE1));
            }
        }

        adminCanSeeQueue(QUEUE1, 1);
    }

    @Test
    public void guestCannotSendToQueueEvenIfSystemRuleWouldAllow() throws Exception
    {
        adminCanSeeQueue(QUEUE1, 1);

        try (final FrameTransport transport = new FrameTransport(getBrokerAdmin(), BrokerAdmin.PortType.AMQP).connect())
        {
            final Interaction interaction = negotiateOpenAs(transport.newInteraction(), GUEST_USERNAME, GUEST_PASSWORD)
                    .begin().consumeResponse(Begin.class);

            final UnsignedInteger linkHandle = UnsignedInteger.ONE;

            interaction.attachName("guestSender")
                    .attachHandle(linkHandle)
                    .attachRole(Role.SENDER)
                    .attachTargetAddress(QUEUE1)
                    .attachSourceOutcomes(Symbols.AMQP_ACCEPTED, Symbols.AMQP_REJECTED)
                    .attach();

            final Object attachOutcome = awaitTerminal(interaction,
                    25,
                    Attach.class,
                    Detach.class,
                    Close.class,
                    End.class);

            if (attachOutcome instanceof Attach)
            {
                // Wait for credit (Flow) or a link/session/connection failure.
                final Object creditOrFailure = awaitTerminal(interaction,
                        25,
                        Flow.class,
                        Detach.class,
                        Close.class,
                        End.class);

                if (creditOrFailure instanceof Flow)
                {
                    interaction.transferHandle(linkHandle)
                            .transferDeliveryId()
                            .transferPayloadData("guestMessage")
                            .transfer();

                    final Object sendOutcome = awaitTerminal(interaction,
                            50,
                            Disposition.class,
                            Detach.class,
                            Close.class,
                            End.class);

                    if (sendOutcome instanceof Disposition disposition)
                    {
                        if (disposition.getState() instanceof Accepted)
                        {
                            fail("Guest unexpectedly sent a message to queue '%s'. This indicates possible privilege escalation."
                                    .formatted(QUEUE1));
                        }
                        assertDeniedOutcome(sendOutcome,
                                "Guest must not be able to send to queue '%s'".formatted(QUEUE1));
                    }
                    else
                    {
                        assertDeniedOutcome(sendOutcome,
                                "Guest must not be able to send to queue '%s'".formatted(QUEUE1));
                    }
                }
                else
                {
                    assertDeniedOutcome(creditOrFailure,
                            "Guest must not be able to send to queue '%s'".formatted(QUEUE1));
                }
            }
            else
            {
                assertDeniedOutcome(attachOutcome,
                        "Guest must not be able to attach a sending link to queue '%s'".formatted(QUEUE1));
            }
        }

        // Verify that no additional message was enqueued.
        adminCanSeeQueue(QUEUE1, 1);
    }

    @Test
    public void adminCanReceiveFromQueue() throws Exception
    {
        adminCanSeeQueue(QUEUE1, 1);

        try (final FrameTransport transport = new FrameTransport(getBrokerAdmin(), BrokerAdmin.PortType.AMQP).connect())
        {
            final Interaction interaction = negotiateOpenAs(transport.newInteraction(), ADMIN_USERNAME, ADMIN_PASSWORD)
                    .begin().consumeResponse(Begin.class);

            interaction.attachName("adminReceiver")
                    .attachHandle(UnsignedInteger.ONE)
                    .attachRole(Role.RECEIVER)
                    .attachSourceAddress(QUEUE1)
                    .attach().consumeResponse(Attach.class)
                    .flowIncomingWindow(UnsignedInteger.ONE)
                    .flowNextIncomingIdFromPeerLatestSessionBeginAndDeliveryCount()
                    .flowOutgoingWindow(UnsignedInteger.ZERO)
                    .flowNextOutgoingId(UnsignedInteger.ZERO)
                    .flowLinkCredit(UnsignedInteger.ONE)
                    .flowHandleFromLinkHandle()
                    .flow()
                    .receiveDelivery()
                    .decodeLatestDelivery();

            assertEquals("message", interaction.getDecodedLatestDelivery(),
                    "Unexpected message payload received by admin");

            interaction.dispositionSettled(true)
                    .dispositionRole(Role.RECEIVER)
                    .dispositionFirstFromLatestDelivery()
                    .dispositionState(new Accepted())
                    .disposition()
                    .detachEndCloseUnconditionally();
        }

        adminCanSeeQueue(QUEUE1, 0);
    }

    private void adminCanSeeQueue(final String queueName, final int expectedMessagesCount)
            throws InterruptedException
    {
        final long deadline = System.currentTimeMillis() + 5_000L;
        int queueDepthMessages;
        do
        {
            queueDepthMessages = getBrokerAdmin().getQueueDepthMessages(queueName);
            if (queueDepthMessages == expectedMessagesCount)
            {
                return;
            }
            Thread.sleep(50L);
        }
        while (System.currentTimeMillis() < deadline);

        assertEquals(expectedMessagesCount, queueDepthMessages, "Expected queue '%s' to contain %d message(s)"
                .formatted(queueName, expectedMessagesCount));
    }

    private Interaction negotiateOpenAs(final Interaction interaction,
                                        final String username,
                                        final String password) throws Exception
    {
        // SASL handshake
        final byte[] saslHeaderResponse = interaction.protocolHeader(SASL_AMQP_HEADER_BYTES)
                .negotiateProtocol()
                .consumeResponse()
                .getLatestResponse(byte[].class);
        assertArrayEquals(SASL_AMQP_HEADER_BYTES, saslHeaderResponse, "Unexpected SASL protocol header response");

        final SaslMechanisms mechanisms = interaction.consumeResponse(SaslMechanisms.class)
                .getLatestResponse(SaslMechanisms.class);
        assertTrue(Arrays.asList(mechanisms.getSaslServerMechanisms()).contains(PLAIN),
                "Broker does not advertise SASL PLAIN");

        final Binary initialResponse =
                new Binary(String.format("\0%s\0%s", username, password).getBytes(US_ASCII));

        final SaslOutcome saslOutcome = interaction.saslMechanism(PLAIN)
                .saslInitialResponse(initialResponse)
                .saslInit()
                .consumeResponse(SaslOutcome.class)
                .getLatestResponse(SaslOutcome.class);

        assertEquals(SaslCode.OK, saslOutcome.getCode(), "SASL authentication failed");

        // Switch to AMQP
        final byte[] amqpHeaderResponse = interaction.protocolHeader(AMQP_HEADER_BYTES)
                .negotiateProtocol()
                .consumeResponse()
                .getLatestResponse(byte[].class);
        assertArrayEquals(AMQP_HEADER_BYTES, amqpHeaderResponse, "Unexpected AMQP protocol header response");

        interaction.open().consumeResponse(Open.class);
        return interaction;
    }

    private Object awaitTerminal(final Interaction interaction,
                                 final int maxFrames,
                                 final Class<?>... terminalTypes) throws Exception
    {
        for (int i = 0; i < maxFrames; i++)
        {
            interaction.consumeResponse();
            final Object body = interaction.getLatestResponse().getBody();
            if (body == null)
            {
                continue;
            }
            for (final Class<?> terminalType : terminalTypes)
            {
                if (terminalType.isAssignableFrom(body.getClass()))
                {
                    return body;
                }
            }
        }
        fail("Timed out waiting for one of %s".formatted(Arrays.toString(terminalTypes)));
        return null;
    }

    private void assertDeniedOutcome(final Object outcome, final String assertionMessage)
    {
        if (outcome instanceof Detach detach)
        {
            assertDeniedError(detach.getError(), assertionMessage);
        }
        else if (outcome instanceof Close close)
        {
            assertDeniedError(close.getError(), assertionMessage);
        }
        else if (outcome instanceof End end)
        {
            assertDeniedError(end.getError(), assertionMessage);
        }
        else if (outcome instanceof Disposition disposition
                && disposition.getState() instanceof Rejected rejected)
        {
            assertDeniedError(rejected.getError(), assertionMessage);
        }
        else
        {
            fail("Unexpected outcome '%s'. %s".formatted(outcome, assertionMessage));
        }
    }

    private void assertDeniedError(final Error error, final String assertionMessage)
    {
        assertNotNull(error, assertionMessage + " (expected an AMQP error)");
        assertNotNull(error.getCondition(), assertionMessage + " (missing AMQP error condition)");
        assertTrue(ACCESS_DENIED_CONDITIONS.contains(error.getCondition().getValue()),
                assertionMessage + " (expected access denied, got condition " + error.getCondition() + ")");
    }
}
