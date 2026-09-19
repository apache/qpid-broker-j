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
 */
package org.apache.qpid.systests.end_to_end_conversion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.protocol.v1_0.constants.Symbols;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Accepted;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Disposition;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.ReceiverSettleMode;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.systests.end_to_end_conversion.client.ClientInstruction;
import org.apache.qpid.systests.end_to_end_conversion.client.ClientResult;
import org.apache.qpid.systests.end_to_end_conversion.client.MessageDescription;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.protocol.v1_0.MessageEncoder;

public class Amqp10ToAmqp010ConversionTest extends EndToEndConversionTestBase
{
    private static final long TEST_TIMEOUT = 30_000L;
    private static final String QUEUE_JNDI_NAME = "queue";

    private String _queueName;
    private Map<String, String> _destinations;

    @BeforeEach
    public void setUp(final TestInfo testInfo)
    {
        _queueName = testInfo.getTestMethod().orElseThrow().getName();
        getBrokerAdmin().createQueue(_queueName);
        _destinations = Map.of("queue." + QUEUE_JNDI_NAME, _queueName);
    }

    @Test
    public void unrepresentablePropertyDoesNotPoisonTimestampDelivery() throws Exception
    {
        assumeTrue(getPublisherProtocolVersion() == Protocol.AMQP_1_0 &&
                getSubscriberProtocolVersion() == Protocol.AMQP_0_10,
                "This test requires an AMQP 1.0 publisher and an AMQP 0-10 subscriber");

        publish(Map.of("oversized", "x".repeat(0x10000)), "unrepresentable");

        final long timestamp = 1_700_000_000_123L;
        publish(Map.of("timestamp", new Date(timestamp)), "deliverable");

        final MessageDescription expectedMessage = new MessageDescription();
        expectedMessage.setProperty("timestamp", timestamp);
        final List<ClientInstruction> subscriberInstructions = new ClientInstructionBuilder()
                .configureDestinations(_destinations)
                .receiveMessage(QUEUE_JNDI_NAME, expectedMessage)
                .build();

        final ClientResult subscriberResult = runSubscriber(subscriberInstructions)
                .get(TEST_TIMEOUT, TimeUnit.MILLISECONDS);
        assertEquals(1, subscriberResult.getClientMessages().size(), "Unexpected received message count");
    }

    private void publish(final Map<String, Object> applicationProperties, final String content) throws Exception
    {
        final MessageEncoder messageEncoder = new MessageEncoder();
        messageEncoder.setApplicationProperties(applicationProperties);
        messageEncoder.addData(content);

        try (final FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Disposition disposition = transport.newInteraction()
                    .negotiateOpen()
                    .begin().consumeResponse(Begin.class)
                    .attachRole(Role.SENDER)
                    .attachTargetAddress(_queueName)
                    .attachSourceOutcomes(Symbols.AMQP_ACCEPTED, Symbols.AMQP_REJECTED)
                    .attach().consumeResponse(Attach.class)
                    .consumeResponse(Flow.class)
                    .transferDeliveryId()
                    .transferPayload(messageEncoder.getPayload())
                    .transferRcvSettleMode(ReceiverSettleMode.FIRST)
                    .transfer()
                    .consumeResponse()
                    .getLatestResponse(Disposition.class);

            assertTrue(Boolean.TRUE.equals(disposition.getSettled()), "Transfer was not settled");
            assertInstanceOf(Accepted.class, disposition.getState(), "Transfer was not accepted");
        }
    }
}
