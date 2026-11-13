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

package org.apache.qpid.tests.websocket;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Random;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.transport.AbstractAMQPConnection;

class WebSocketMessagingTest extends WebSocketTestBase
{
    @ParameterizedTest
    @EnumSource(value = Transport.class, names = {"WS", "WSS"})
    public void authenticatedLargeTransactedDelivery(final Transport transport) throws Exception
    {
        final AmqpPort<?> port = createPort(transport);
        createQueue("messages");
        final byte[] payload = new byte[3 * 1024 * 1024];
        new Random(127).nextBytes(payload);
        try (final Connection connection = connect(port, "messaging"))
        {
            final Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            final Queue queue = session.createQueue("messages");
            final MessageProducer producer = session.createProducer(queue);
            final BytesMessage outgoing = session.createBytesMessage();
            outgoing.writeBytes(payload);
            producer.send(outgoing);
            session.commit();
            final MessageConsumer consumer = session.createConsumer(queue);
            final BytesMessage incoming = (BytesMessage) consumer.receive(TIMEOUT_MILLIS);
            assertNotNull(incoming);
            assertEquals(payload.length, incoming.getBodyLength());
            final byte[] received = new byte[payload.length];
            assertEquals(received.length, incoming.readBytes(received));
            assertArrayEquals(payload, received);
            session.rollback();
            final BytesMessage redelivered = (BytesMessage) consumer.receive(TIMEOUT_MILLIS);
            assertNotNull(redelivered);
            assertTrue(redelivered.getJMSRedelivered());
            session.commit();
            final AbstractAMQPConnection<?, ?> server = brokerConnection("messaging");
            assertEquals("guest", server.getPrincipal());
            assertEquals(transport, server.getTransport());
            assertFalse(server.isClosing());
        }
        await(() -> port.getConnectionCount() == 0 && _virtualHost.getConnectionCount() == 0,
                "Clean client close did not release the broker connection");
    }

    @ParameterizedTest
    @EnumSource(value = Transport.class, names = {"WS", "WSS"})
    public void authenticationFailureReleasesConnectionAndPortRemainsUsable(final Transport transport)
            throws Exception
    {
        final AmqpPort<?> port = createPort(transport);
        assertThrows(JMSException.class, () ->
        {
            try (final Connection connection = connectionFactory(port, "rejected").createConnection("guest", "wrong"))
            {
                connection.start();
            }
        });
        await(() -> port.getConnectionCount() == 0, "Failed authentication leaked a transport");
        try (final Connection connection = connect(port, "accepted"))
        {
            assertEquals("guest", brokerConnection("accepted").getPrincipal());
        }
    }
}
