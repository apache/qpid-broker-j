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

package org.apache.qpid.tests.protocol.v0_8.security;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.apache.qpid.server.security.auth.manager.AbstractScramAuthenticationManager.PLAIN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.protocol.v0_8.transport.BasicConsumeOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicGetOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelCloseBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelOpenOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionCloseBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionOpenOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionSecureBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionStartBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionTuneBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueDeclareOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueDeleteOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueuePurgeOkBody;
import org.apache.qpid.tests.protocol.v0_8.FrameTransport;
import org.apache.qpid.tests.protocol.v0_8.Interaction;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;
import org.apache.qpid.tests.utils.ConfigItem;

/**
 * Guard test to detect Subject leakage/escalation to SYSTEM across asynchronous execution boundaries.
 * <br>
 * The ACL in config-protocol-tests-0-8-system-privilege-guard.json is intentionally crafted so that:
 * - SYSTEM is allowed to perform ALL operations on QUEUE (bypass detector)
 * - guest is explicitly denied to perform ANY operations on QUEUE
 * - everything else is allowed to keep broker usable for tests
 * <br>
 * If guest ever succeeds at an operation on QUEUE, it is a strong indicator that authorise() observed SYSTEM,
 * i.e. Subject propagation is broken.
 */
@ConfigItem(
        name = "qpid.initialConfigurationLocation",
        value = "classpath:config-protocol-tests-0-8-system-privilege-guard.json",
        jvm = true
)
public class SystemPrivilegeEscalationGuardTest extends BrokerAdminUsingTestBase
{
    private static final String ADMIN_USERNAME = "admin";
    private static final String ADMIN_PASSWORD = "admin";
    private static final String GUEST_USERNAME = "guest";
    private static final String GUEST_PASSWORD = "guest";

    private static final String QUEUE1 = "queue1";

    private static final List<Integer> ACCESS_DENIED_CODES = List.of(ErrorCodes.ACCESS_REFUSED, ErrorCodes.NOT_ALLOWED);

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

        // Guest tries to consume message from queue (Basic.Get is a direct queue access)
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin(), BrokerAdmin.PortType.AMQP).connect())
        {
            final Interaction interaction = negotiateOpenAs(transport.newInteraction(), GUEST_USERNAME, GUEST_PASSWORD)
                    .channel().open().consumeResponse(ChannelOpenOkBody.class)
                    .basic().getQueueName(QUEUE1).get()
                    .consumeResponse(ConnectionCloseBody.class, ChannelCloseBody.class, BasicGetOkBody.class);

            final Object body = interaction.getLatestResponse().getBody();
            if (body instanceof BasicGetOkBody)
            {
                fail("Guest unexpectedly consumed a message from queue '%s'. This indicates possible privilege escalation."
                        .formatted(QUEUE1));
            }
            assertDeniedAndClose(interaction, "Guest must not be able to consume from queue '%s'".formatted(QUEUE1));
        }

        // Verify queue still contains 1 message
        adminCanSeeQueue(QUEUE1, 1);
    }

    @Test
    public void guestCannotDeleteQueueEvenIfSystemRuleWouldAllow() throws Exception
    {
        // Admin can see queue with 1 message in it
        adminCanSeeQueue(QUEUE1, 1);

        // Guest connection must use the authenticated AMQP port (not anonymous)
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin(), BrokerAdmin.PortType.AMQP).connect())
        {
            final Interaction interaction = negotiateOpenAs(transport.newInteraction(), GUEST_USERNAME, GUEST_PASSWORD)
                    .channel().open().consumeResponse(ChannelOpenOkBody.class)
                    .queue().deleteName(QUEUE1).delete()
                    .consumeResponse(ConnectionCloseBody.class, ChannelCloseBody.class, QueueDeleteOkBody.class);

            final Object body = interaction.getLatestResponse().getBody();
            if (body instanceof QueueDeleteOkBody)
            {
                fail("Guest unexpectedly deleted queue '%s'. This indicates possible privilege escalation."
                        .formatted(QUEUE1));
            }
            assertDeniedAndClose(interaction, "Guest must not be able to delete queue '%s'".formatted(QUEUE1));
        }

        // Verify queue still exists
        adminCanSeeQueue(QUEUE1, 1);
    }

    @Test
    public void guestCannotPurgeQueueEvenIfSystemRuleWouldAllow() throws Exception
    {
        // Admin can see queue with 1 message in it
        adminCanSeeQueue(QUEUE1, 1);

        // Guest tries to purge queue
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin(), BrokerAdmin.PortType.AMQP).connect())
        {
            final Interaction interaction = negotiateOpenAs(transport.newInteraction(), GUEST_USERNAME, GUEST_PASSWORD)
                    .channel().open().consumeResponse(ChannelOpenOkBody.class)
                    .queue().purgeName(QUEUE1).purge()
                    .consumeResponse(ConnectionCloseBody.class, ChannelCloseBody.class, QueuePurgeOkBody.class);

            final Object body = interaction.getLatestResponse().getBody();
            if (body instanceof QueuePurgeOkBody)
            {
                fail("Guest unexpectedly purged queue '%s'. This indicates possible privilege escalation."
                        .formatted(QUEUE1));
            }
            assertDeniedAndClose(interaction, "Guest must not be able to purge queue '%s'".formatted(QUEUE1));
        }

        // Verify queue still has 1 message in it
        adminCanSeeQueue(QUEUE1, 1);
    }

    @Test
    public void guestCannotConsumeQueueUsingBasicConsumeEvenIfSystemRuleWouldAllow() throws Exception
    {
        // Admin can see queue with 1 message in it
        adminCanSeeQueue(QUEUE1, 1);

        // Guest tries to create a consumer on the queue (Basic.Consume)
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin(), BrokerAdmin.PortType.AMQP).connect())
        {
            final Interaction interaction = negotiateOpenAs(transport.newInteraction(), GUEST_USERNAME, GUEST_PASSWORD)
                    .channel().open().consumeResponse(ChannelOpenOkBody.class)
                    .basic()
                    .consumeQueue(QUEUE1)
                    .consumeConsumerTag("ctag-queue1")
                    .consumeNoAck(true)
                    .consume()
                    .consumeResponse(ConnectionCloseBody.class, ChannelCloseBody.class, BasicConsumeOkBody.class);

            final Object body = interaction.getLatestResponse().getBody();
            if (body instanceof BasicConsumeOkBody)
            {
                fail("Guest unexpectedly created a consumer on queue '%s'. This indicates possible privilege escalation."
                        .formatted(QUEUE1));
            }

            assertDeniedAndClose(interaction, "Guest must not be able to consume from queue '%s'".formatted(QUEUE1));
        }

        // Verify queue still contains 1 message (no consumption happened)
        adminCanSeeQueue(QUEUE1, 1);
    }

    @Test
    public void guestCannotDeclareServerNamedQueueEvenIfSystemRuleWouldAllow() throws Exception
    {
        // Guest tries to declare a server-named queue (empty queue name)
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin(), BrokerAdmin.PortType.AMQP).connect())
        {
            final Interaction interaction = negotiateOpenAs(transport.newInteraction(), GUEST_USERNAME, GUEST_PASSWORD)
                    .channel().open().consumeResponse(ChannelOpenOkBody.class)
                    .queue()
                    // Explicitly set passive=false to ensure this is a creation attempt, not a lookup.
                    .declarePassive(false)
                    .declareName("")
                    .declare()
                    .consumeResponse(ConnectionCloseBody.class, ChannelCloseBody.class, QueueDeclareOkBody.class);

            final Object body = interaction.getLatestResponse().getBody();
            if (body instanceof QueueDeclareOkBody ok)
            {
                final String createdQueue = ok.getQueue() == null ? "<null>" : ok.getQueue().toString();
                fail("Guest unexpectedly declared server-named queue '%s'. This indicates possible privilege escalation."
                        .formatted(createdQueue));
            }

            assertDeniedAndClose(interaction, "Guest must not be able to declare a server-named queue");
        }

        // Ensure existing queue is unaffected
        adminCanSeeQueue(QUEUE1, 1);
    }

    @Test
    public void adminCanDeleteQueue() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin(), BrokerAdmin.PortType.AMQP).connect())
        {
            QueueDeleteOkBody ok = negotiateOpenAs(transport.newInteraction(), ADMIN_USERNAME, ADMIN_PASSWORD)
                    .channel().open().consumeResponse(ChannelOpenOkBody.class)
                    .queue().deleteName(QUEUE1).delete()
                    .consumeResponse(QueueDeleteOkBody.class)
                    .getLatestResponse(QueueDeleteOkBody.class);

            assertEquals(1L, ok.getMessageCount(), "Sanity: queue contained one message before delete");
        }

        // After delete, passive declare must fail with NOT_FOUND (channel exception)
        assertQueueDoesNotExist(QUEUE1);
    }

    private Interaction negotiateOpenAs(final Interaction interaction, final String username, final String password) throws Exception
    {
        final ConnectionStartBody start = interaction.negotiateProtocol()
                .consumeResponse(ConnectionStartBody.class)
                .getLatestResponse(ConnectionStartBody.class);

        final String mechanisms = start.getMechanisms() == null ? "" : new String(start.getMechanisms(), US_ASCII);
        assertTrue(List.of(mechanisms.split(" ")).contains(PLAIN),
                "Broker must advertise PLAIN mechanism for this test");

        interaction.connection()
                .startOkMechanism(PLAIN)
                .startOk()
                .consumeResponse(ConnectionSecureBody.class);

        // For PLAIN, server challenge is typically empty/null; client sends initial response in secure-ok.
        final byte[] initialResponse = String.format("\0%s\0%s", username, password).getBytes(US_ASCII);

        final ConnectionTuneBody tune = interaction.connection()
                .secureOk(initialResponse)
                .consumeResponse(ConnectionTuneBody.class)
                .getLatestResponse(ConnectionTuneBody.class);

        return interaction.connection()
                .tuneOkChannelMax(tune.getChannelMax())
                .tuneOkFrameMax(tune.getFrameMax())
                .tuneOkHeartbeat(tune.getHeartbeat())
                .tuneOk()
                .connection().open()
                .consumeResponse(ConnectionOpenOkBody.class);
    }

    private void adminCanSeeQueue(final String queueName, final int expectedMessagesCount) throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin(), BrokerAdmin.PortType.AMQP).connect())
        {
            final Interaction interaction = transport.newInteraction();
            negotiateOpenAs(interaction, ADMIN_USERNAME, ADMIN_PASSWORD);

            QueueDeclareOkBody ok = interaction.channel().open().consumeResponse(ChannelOpenOkBody.class)
                    .queue().declarePassive(true).declareName(queueName).declare()
                    .consumeResponse(QueueDeclareOkBody.class)
                    .getLatestResponse(QueueDeclareOkBody.class);

            assertEquals(expectedMessagesCount, ok.getMessageCount(),
                    "Expected queue '%s' to contain %d message(s)".formatted(queueName, expectedMessagesCount));
        }
    }

    private void assertQueueDoesNotExist(final String queueName) throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin(), BrokerAdmin.PortType.AMQP).connect())
        {
            final Interaction interaction = transport.newInteraction();
            negotiateOpenAs(interaction, ADMIN_USERNAME, ADMIN_PASSWORD);

            interaction.channel().open().consumeResponse(ChannelOpenOkBody.class)
                    .queue().declarePassive(true).declareName(queueName).declare()
                    .consumeResponse(ChannelCloseBody.class, QueueDeclareOkBody.class);

            final Object body = interaction.getLatestResponse().getBody();
            if (body instanceof QueueDeclareOkBody)
            {
                fail("Queue '%s' unexpectedly exists".formatted(queueName));
            }

            final ChannelCloseBody close = (ChannelCloseBody) body;
            assertEquals(ErrorCodes.NOT_FOUND, close.getReplyCode(),
                    "Expected NOT_FOUND for missing queue '%s'".formatted(queueName));

            // Reply text is not part of a stable API contract, but it should at least mention the queue name.
            final String replyText = close.getReplyText() == null ? "" : close.getReplyText().toString();
            assertTrue(replyText.contains(queueName),
                    "Expected reply text to mention queue '%s' but got '%s'".formatted(queueName, replyText));

            interaction.channel().closeOk();
        }
    }

    private void assertDeniedAndClose(final Interaction interaction, final String assertionMessage) throws Exception
    {
        final Object body = interaction.getLatestResponse().getBody();
        if (body instanceof ConnectionCloseBody close)
        {
            assertTrue(ACCESS_DENIED_CODES.contains(close.getReplyCode()),
                    assertionMessage + " (expected access denied, got reply code " + close.getReplyCode() + ")");
            interaction.connection().closeOk();
        }
        else if (body instanceof ChannelCloseBody close)
        {
            assertTrue(ACCESS_DENIED_CODES.contains(close.getReplyCode()),
                    assertionMessage + " (expected access denied, got reply code " + close.getReplyCode() + ")");
            interaction.channel().closeOk();
        }
        else
        {
            fail(assertionMessage + ": expected Channel.Close or Connection.Close, got " + body);
        }
    }
}
