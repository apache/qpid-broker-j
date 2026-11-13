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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.transport.AbstractAMQPConnection;

class WebSocketInteroperabilityTest extends WebSocketTestBase
{
    @ParameterizedTest
    @CsvSource({"TCP,WS", "WS,TCP", "TCP,WSS", "WSS,TCP"})
    public void messagesCrossTransports(final Transport producerTransport,
                                        final Transport consumerTransport)
            throws Exception
    {
        final AmqpPort<?> producerPort = createPort(producerTransport);
        final AmqpPort<?> consumerPort = createPort(consumerTransport);
        final org.apache.qpid.server.model.Queue<?> brokerQueue = createQueue("interoperability");
        final String body = "AMQP 1.0 from " + producerTransport + " to " + consumerTransport;

        try (final Connection producerConnection = connect(producerPort, "producer");
             final Connection consumerConnection = connect(consumerPort, "consumer"))
        {
            final AbstractAMQPConnection<?, ?> producerModel = brokerConnection("producer");
            final AbstractAMQPConnection<?, ?> consumerModel = brokerConnection("consumer");
            assertEquals(producerTransport, producerModel.getTransport());
            assertEquals(consumerTransport, consumerModel.getTransport());
            assertEquals(Protocol.AMQP_1_0, producerModel.getProtocol());
            assertEquals(Protocol.AMQP_1_0, consumerModel.getProtocol());
            assertEquals(VIRTUAL_HOST, producerModel.getAddressSpaceName());
            assertEquals(VIRTUAL_HOST, consumerModel.getAddressSpaceName());
            assertEquals(2, _virtualHost.getConnectionCount());

            final Session producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final Queue destination = producerSession.createQueue(brokerQueue.getName());
            final MessageProducer producer = producerSession.createProducer(destination);
            final TextMessage outgoing = producerSession.createTextMessage(body);
            outgoing.setJMSCorrelationID("request-1");
            outgoing.setJMSType("transport-interoperability");
            outgoing.setStringProperty("application", "websocket-systests");
            outgoing.setIntProperty("sequence", 42);
            outgoing.setBooleanProperty("verified", true);
            producer.send(outgoing);
            await(() -> brokerQueue.getQueueDepthMessages() == 1, "Producer did not enqueue the message");

            final Session consumerSession = consumerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            final MessageConsumer consumer = consumerSession
                    .createConsumer(consumerSession.createQueue(brokerQueue.getName()));
            final TextMessage incoming = (TextMessage) consumer.receive(TIMEOUT_MILLIS);
            assertNotNull(incoming);
            assertEquals(body, incoming.getText());
            assertNotNull(outgoing.getJMSMessageID());
            assertEquals(outgoing.getJMSMessageID(), incoming.getJMSMessageID());
            assertEquals("request-1", incoming.getJMSCorrelationID());
            assertEquals("transport-interoperability", incoming.getJMSType());
            assertEquals("websocket-systests", incoming.getStringProperty("application"));
            assertEquals(42, incoming.getIntProperty("sequence"));
            assertTrue(incoming.getBooleanProperty("verified"));
            assertEquals(1, brokerQueue.getQueueDepthMessages(),
                    "The message must remain on the broker until the consumer acknowledges it");

            incoming.acknowledge();
            await(() -> brokerQueue.getQueueDepthMessages() == 0,
                    "Consumer acknowledgement did not remove the message from the broker");
        }
        await(() -> producerPort.getConnectionCount() == 0 && consumerPort.getConnectionCount() == 0 &&
                    _virtualHost.getConnectionCount() == 0,
                "Closing the clients did not release both transports and managed connections");
        assertEquals(0, brokerQueue.getQueueDepthMessages(), "The acknowledged message was requeued on close");
    }
}
