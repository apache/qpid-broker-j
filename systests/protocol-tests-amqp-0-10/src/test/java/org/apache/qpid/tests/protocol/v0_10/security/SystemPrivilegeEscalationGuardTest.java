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

package org.apache.qpid.tests.protocol.v0_10.security;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.protocol.v0_10.transport.ConnectionOpenOk;
import org.apache.qpid.server.protocol.v0_10.transport.ConnectionSecure;
import org.apache.qpid.server.protocol.v0_10.transport.ConnectionStart;
import org.apache.qpid.server.protocol.v0_10.transport.ConnectionTune;
import org.apache.qpid.server.protocol.v0_10.transport.ExecutionErrorCode;
import org.apache.qpid.server.protocol.v0_10.transport.ExecutionException;
import org.apache.qpid.server.protocol.v0_10.transport.ExecutionResult;
import org.apache.qpid.server.protocol.v0_10.transport.MessageCreditUnit;
import org.apache.qpid.server.protocol.v0_10.transport.MessageTransfer;
import org.apache.qpid.server.protocol.v0_10.transport.QueueQueryResult;
import org.apache.qpid.server.protocol.v0_10.transport.SessionCommandPoint;
import org.apache.qpid.server.protocol.v0_10.transport.SessionCompleted;
import org.apache.qpid.server.protocol.v0_10.transport.SessionConfirmed;
import org.apache.qpid.server.protocol.v0_10.transport.SessionFlush;
import org.apache.qpid.tests.protocol.v0_10.ConnectionInteraction;
import org.apache.qpid.tests.protocol.v0_10.FrameTransport;
import org.apache.qpid.tests.protocol.v0_10.Interaction;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;
import org.apache.qpid.tests.utils.ConfigItem;

/**
 * Guard test to detect Subject leakage/escalation to SYSTEM across asynchronous execution boundaries.
 * <br>
 * The ACL in config-protocol-tests-0-10-system-privilege-guard.json is intentionally crafted so that:
 * - SYSTEM is allowed to perform ALL operations on QUEUE (bypass detector)
 * - guest is explicitly denied to perform ANY operations on QUEUE
 * - everything else is allowed to keep broker usable for tests
 * <br>
 * If guest ever succeeds at an operation on QUEUE, it is a strong indicator that authorise() observed SYSTEM,
 * i.e. Subject propagation is broken.
 */
@ConfigItem(
        name = "qpid.initialConfigurationLocation",
        value = "classpath:config-protocol-tests-0-10-system-privilege-guard.json",
        jvm = true
)
public class SystemPrivilegeEscalationGuardTest extends BrokerAdminUsingTestBase
{
    private static final String ADMIN_USERNAME = "admin";
    private static final String ADMIN_PASSWORD = "admin";
    private static final String GUEST_USERNAME = "guest";
    private static final String GUEST_PASSWORD = "guest";

    private static final String QUEUE1 = "queue1";
    private static final String QUEUE2 = "queue2";

    private static final byte[] SESSION_NAME = "guard-session".getBytes(UTF_8);
    private static final String SUBSCRIBER = "guard-subscriber";

    private static final Set<ExecutionErrorCode> ACCESS_DENIED_CODES =
            EnumSet.of(ExecutionErrorCode.UNAUTHORIZED_ACCESS, ExecutionErrorCode.NOT_ALLOWED);

    @BeforeEach
    public void setUp()
    {
        getBrokerAdmin().createQueue(QUEUE1);
        getBrokerAdmin().putMessageOnQueue(QUEUE1, "message");
    }

    @Test
    public void guestCannotConsumeQueueEvenIfSystemRuleWouldAllow() throws Exception
    {
        // Admin can see queue with 1 message in it
        adminCanSeeQueue(QUEUE1, 1);

        try (final FrameTransport transport = new FrameTransport(getBrokerAdmin(), BrokerAdmin.PortType.AMQP).connect())
        {
            final Interaction interaction = transport.newInteraction();
            negotiateOpenAs(interaction, GUEST_USERNAME, GUEST_PASSWORD)
                    .channelId(1)
                    .attachSession(SESSION_NAME)
                    .message()
                    .subscribeDestination(SUBSCRIBER)
                    .subscribeQueue(QUEUE1)
                    .subscribeId(0)
                    .subscribe()
                    .message()
                    .flowId(1)
                    .flowDestination(SUBSCRIBER)
                    .flowUnit(MessageCreditUnit.MESSAGE)
                    .flowValue(1)
                    .flow()
                    .message()
                    .flowId(2)
                    .flowDestination(SUBSCRIBER)
                    .flowUnit(MessageCreditUnit.BYTE)
                    .flowValue(-1)
                    .flow()
                    .session()
                    .flushCompleted()
                    .flush();

            assertDenied(interaction, 0, "Guest must not be able to consume from queue '%s'".formatted(QUEUE1));
        }

        // Verify queue still contains 1 message
        adminCanSeeQueue(QUEUE1, 1);
    }

    @Test
    public void guestCannotDeleteQueueEvenIfSystemRuleWouldAllow() throws Exception
    {
        // Admin can see queue with 1 message in it
        adminCanSeeQueue(QUEUE1, 1);

        try (final FrameTransport transport = new FrameTransport(getBrokerAdmin(), BrokerAdmin.PortType.AMQP).connect())
        {
            final Interaction interaction = transport.newInteraction();
            negotiateOpenAs(interaction, GUEST_USERNAME, GUEST_PASSWORD)
                    .channelId(1)
                    .attachSession(SESSION_NAME)
                    .queue()
                    .deleteQueue(QUEUE1)
                    .deleteId(0)
                    .delete()
                    .session()
                    .flushCompleted()
                    .flush();

            assertDenied(interaction, 0, "Guest must not be able to delete queue '%s'".formatted(QUEUE1));
        }

        // Verify queue still exists
        adminCanSeeQueue(QUEUE1, 1);
    }

    @Test
    public void guestCannotPurgeQueueEvenIfSystemRuleWouldAllow() throws Exception
    {
        // Admin can see queue with 1 message in it
        adminCanSeeQueue(QUEUE1, 1);

        try (final FrameTransport transport = new FrameTransport(getBrokerAdmin(), BrokerAdmin.PortType.AMQP).connect())
        {
            final Interaction interaction = transport.newInteraction();
            negotiateOpenAs(interaction, GUEST_USERNAME, GUEST_PASSWORD)
                    .channelId(1)
                    .attachSession(SESSION_NAME)
                    .queue()
                    .purgeQueue(QUEUE1)
                    .purgeId(0)
                    .purge()
                    .session()
                    .flushCompleted()
                    .flush();

            assertDenied(interaction, 0, "Guest must not be able to purge queue '%s'".formatted(QUEUE1));
        }

        // Verify queue still has 1 message in it
        adminCanSeeQueue(QUEUE1, 1);
    }

    @Test
    public void guestCannotDeclareQueueEvenIfSystemRuleWouldAllow() throws Exception
    {
        assertQueueDoesNotExist(QUEUE2);

        try (final FrameTransport transport = new FrameTransport(getBrokerAdmin(), BrokerAdmin.PortType.AMQP).connect())
        {
            final Interaction interaction = transport.newInteraction();
            negotiateOpenAs(interaction, GUEST_USERNAME, GUEST_PASSWORD)
                    .channelId(1)
                    .attachSession(SESSION_NAME)
                    .queue()
                    .declareQueue(QUEUE2)
                    .declareId(0)
                    .declare()
                    .session()
                    .flushCompleted()
                    .flush();

            assertDenied(interaction, 0, "Guest must not be able to declare queue '%s'".formatted(QUEUE2));
        }

        // Ensure existing queue is unaffected
        adminCanSeeQueue(QUEUE1, 1);
        assertQueueDoesNotExist(QUEUE2);
    }

    @Test
    public void adminCanDeleteQueue() throws Exception
    {
        // Sanity: queue is present and contains one message
        adminCanSeeQueue(QUEUE1, 1);

        try (final FrameTransport transport = new FrameTransport(getBrokerAdmin(), BrokerAdmin.PortType.AMQP).connect())
        {
            final Interaction interaction = transport.newInteraction();
            negotiateOpenAs(interaction, ADMIN_USERNAME, ADMIN_PASSWORD)
                    .channelId(1)
                    .attachSession(SESSION_NAME)
                    .queue()
                    .deleteQueue(QUEUE1)
                    .deleteId(0)
                    .delete()
                    .session()
                    .flushCompleted()
                    .flush();

            final SessionCompleted completed = awaitSessionCompleted(interaction);
            assertNotNull(completed.getCommands(), "Expected session completed to include command acknowledgements");
            assertTrue(completed.getCommands().includes(0), "Expected delete command id 0 to be completed");
        }

        assertQueueDoesNotExist(QUEUE1);
    }

    private Interaction negotiateOpenAs(final Interaction interaction, final String username, final String password)
            throws Exception
    {
        // After protocol header, broker sends a ProtocolHeader event (mapped to HeaderResponse), then ConnectionStart.
        final ConnectionStart start = interaction.negotiateProtocol()
                .consumeResponse()
                .consumeResponse(ConnectionStart.class)
                .getLatestResponse(ConnectionStart.class);

        final List<Object> mechanisms = start.getMechanisms() == null ? List.of() : start.getMechanisms();
        final boolean plainSupported = mechanisms.stream()
                .map(String::valueOf)
                .anyMatch(m -> m.equalsIgnoreCase(ConnectionInteraction.SASL_MECHANISM_PLAIN));
        assertTrue(plainSupported, "Broker must advertise PLAIN mechanism for this test");

        final byte[] initialResponse = String.format("\0%s\0%s", username, password).getBytes(UTF_8);

        final ConnectionTune tune = interaction.connection()
                .startOkMechanism(ConnectionInteraction.SASL_MECHANISM_PLAIN)
                .startOk()
                .consumeResponse(ConnectionSecure.class)
                .connection()
                .secureOk(initialResponse)
                .consumeResponse(ConnectionTune.class)
                .getLatestResponse(ConnectionTune.class);

        interaction.connection()
                .tuneOkChannelMax(tune.getChannelMax())
                .tuneOkMaxFrameSize(tune.getMaxFrameSize())
                .tuneOkHeartbeat(tune.getHeartbeatMax())
                .tuneOk();

        return interaction.connection().open().consumeResponse(ConnectionOpenOk.class);
    }

    private void adminCanSeeQueue(final String queueName, final int expectedMessagesCount) throws Exception
    {
        try (final FrameTransport transport = new FrameTransport(getBrokerAdmin(), BrokerAdmin.PortType.AMQP).connect())
        {
            final Interaction interaction = transport.newInteraction();
            ExecutionResult result = negotiateOpenAs(interaction, ADMIN_USERNAME, ADMIN_PASSWORD)
                    .channelId(1)
                    .attachSession(SESSION_NAME)
                    .queue()
                    .queryQueue(queueName)
                    .queryId(0)
                    .query()
                    .session()
                    .flushCompleted()
                    .flush()
                    .consumeResponse(SessionCommandPoint.class)
                    .consumeResponse()
                    .getLatestResponse(ExecutionResult.class);

            final QueueQueryResult queryResult = (QueueQueryResult) result.getValue();
            assertEquals(expectedMessagesCount, queryResult.getMessageCount(),
                    "Expected queue '%s' to contain %d message(s)".formatted(queueName, expectedMessagesCount));
        }
    }

    private void assertQueueDoesNotExist(final String queueName) throws Exception
    {
        try (final FrameTransport transport = new FrameTransport(getBrokerAdmin(), BrokerAdmin.PortType.AMQP).connect())
        {
            final Interaction interaction = transport.newInteraction();

            negotiateOpenAs(interaction, ADMIN_USERNAME, ADMIN_PASSWORD)
                    .channelId(1)
                    .attachSession(SESSION_NAME)
                    .queue()
                    .queryQueue(queueName)
                    .queryId(0)
                    .query()
                    .session()
                    .flushCompleted()
                    .flush()
                    .consumeResponse(SessionCommandPoint.class)
                    .consumeResponse(ExecutionException.class, ExecutionResult.class);

            final ExecutionResult executionResult = (ExecutionResult) interaction.getLatestResponse().getBody();
            final QueueQueryResult queryResult = (QueueQueryResult) executionResult.getValue();
            assertNull(queryResult.getQueue());
        }
    }

    private void assertDenied(final Interaction interaction, final int commandId, final String assertionMessage)
            throws Exception
    {
        final Object terminal = awaitTerminalOutcome(interaction, commandId);

        if (terminal instanceof ExecutionException ex)
        {
            assertTrue(ACCESS_DENIED_CODES.contains(ex.getErrorCode()),
                    assertionMessage + " (expected access denied, got " + ex.getErrorCode() + ")");
            assertEquals(commandId, ex.getCommandId(),
                    assertionMessage + " (unexpected command id in exception)");
            return;
        }

        fail(assertionMessage + ": expected access denied, got " + terminal);
    }

    private SessionCompleted awaitSessionCompleted(final Interaction interaction) throws Exception
    {
        Object terminal = awaitTerminalOutcome(interaction, -1);
        if (terminal instanceof SessionCompleted completed)
        {
            return completed;
        }
        throw new IllegalStateException("Expected SessionCompleted, got " + terminal);
    }

    private Object awaitTerminalOutcome(final Interaction interaction, final int expectedCommandId) throws Exception
    {
        // Consume a bounded number of responses; any missing response will timeout in the transport layer.
        for (int i = 0; i < 25; i++)
        {
            interaction.consumeResponse();
            final Object body = interaction.getLatestResponse() == null ? null : interaction.getLatestResponse().getBody();
            if (body == null)
            {
                continue;
            }

            // Responses that are not terminal for our purposes.
            if (body instanceof SessionCommandPoint || body instanceof SessionConfirmed || body instanceof SessionFlush)
            {
                continue;
            }

            if (body instanceof MessageTransfer transfer)
            {
                // Receipt of a message is a definitive sign of consume success.
                return transfer;
            }

            if (body instanceof ExecutionResult result)
            {
                return result;
            }

            if (body instanceof ExecutionException ex)
            {
                return ex;
            }

            if (body instanceof SessionCompleted completed)
            {
                // If a specific command id was requested, keep consuming until it is accounted for.
                if (expectedCommandId >= 0
                        && completed.getCommands() != null
                        && !completed.getCommands().includes(expectedCommandId))
                {
                    continue;
                }
                return completed;
            }

            // Any other response type is unexpected and should fail the test.
            return body;
        }

        return "<no terminal response received>";
    }
}
