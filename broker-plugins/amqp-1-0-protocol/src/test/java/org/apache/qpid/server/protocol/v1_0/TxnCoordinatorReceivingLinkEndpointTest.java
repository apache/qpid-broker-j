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
package org.apache.qpid.server.protocol.v1_0;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.model.Session;
import org.apache.qpid.server.protocol.v1_0.constants.Symbols;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.codec.AMQPDescribedTypeRegistry;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Accepted;
import org.apache.qpid.server.protocol.v1_0.type.messaging.AmqpValue;
import org.apache.qpid.server.protocol.v1_0.type.messaging.DeliveryAnnotations;
import org.apache.qpid.server.protocol.v1_0.type.messaging.DeliveryAnnotationsSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.EncodingRetainingSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Header;
import org.apache.qpid.server.protocol.v1_0.type.messaging.HeaderSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Rejected;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
import org.apache.qpid.server.protocol.v1_0.type.transaction.Coordinator;
import org.apache.qpid.server.protocol.v1_0.type.transaction.Declare;
import org.apache.qpid.server.protocol.v1_0.type.transaction.Declared;
import org.apache.qpid.server.protocol.v1_0.type.transaction.Discharge;
import org.apache.qpid.server.protocol.v1_0.type.transport.Error;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;

class TxnCoordinatorReceivingLinkEndpointTest
{
    private final AMQPConnection_1_0<?> _connection = mock(AMQPConnection_1_0.class);
    private final Session_1_0 _session = mock(Session_1_0.class);
    private final ServerTransaction serverTransaction = mock(ServerTransaction.class);
    private final IdentifiedTransaction identifiedTransaction = mock(IdentifiedTransaction.class);

    private final AMQPDescribedTypeRegistry _amqpDescribedTypeRegistry = AMQPDescribedTypeRegistry.newInstance()
            .registerTransportLayer()
            .registerMessagingLayer()
            .registerTransactionLayer()
            .registerSecurityLayer()
            .registerExtensionSoleconnLayer();

    @BeforeEach
    void beforeEach()
    {
        when(identifiedTransaction.getId()).thenReturn(1);
        when(identifiedTransaction.getServerTransaction()).thenReturn(serverTransaction);

        when(_connection.getDescribedTypeRegistry()).thenReturn(_amqpDescribedTypeRegistry);
        when(_connection.createIdentifiedTransaction()).thenReturn(identifiedTransaction);

        doReturn(_connection).when(_session).getConnection();
        when(_session.getContextValue(Long.class, Session.TRANSACTION_TIMEOUT_NOTIFICATION_REPEAT_PERIOD))
                .thenReturn(30_000L);
    }

    @Test
    void receiveDeliveryDischargeOutcomeAccepted()
    {
        final Binary declareDeliveryTag = new Binary(new byte[] { (byte) 0 });
        final QpidByteBuffer declareQpidByteBuffer = declareMessage();
        final Delivery declareDelivery = mock(Delivery.class);
        when(declareDelivery.getDeliveryTag()).thenReturn(declareDeliveryTag);
        when(declareDelivery.getPayload()).thenReturn(declareQpidByteBuffer);

        final Binary dischargeDeliveryTag = new Binary(new byte[] { (byte) 1 });
        final QpidByteBuffer dischargeQpidByteBuffer = dischargeMessage();
        final Delivery dischargeDelivery = mock(Delivery.class);
        when(dischargeDelivery.getDeliveryTag()).thenReturn(dischargeDeliveryTag);
        when(dischargeDelivery.getPayload()).thenReturn(dischargeQpidByteBuffer);

        final Source source = mock(Source.class);
        when(source.getOutcomes()).thenReturn(null);

        final Link_1_0<Source, Coordinator> link = mock(Link_1_0.class);
        when(link.getSource()).thenReturn(source);

        final TxnCoordinatorReceivingLinkEndpoint txnCoordinatorReceivingLinkEndpoint =
                spy(new TxnCoordinatorReceivingLinkEndpoint(_session, link));
        txnCoordinatorReceivingLinkEndpoint.start();

        final Error declareError = txnCoordinatorReceivingLinkEndpoint.receiveDelivery(declareDelivery);
        final Error dischargeError = txnCoordinatorReceivingLinkEndpoint.receiveDelivery(dischargeDelivery);

        verify(txnCoordinatorReceivingLinkEndpoint, times(1))
                .updateDispositions(anySet(), any(Declared.class), anyBoolean());
        verify(txnCoordinatorReceivingLinkEndpoint, times(1))
                .updateDispositions(anySet(), any(Accepted.class), anyBoolean());
        assertNull(declareError);
        assertNull(dischargeError);
    }

    @Test
    void receiveDeliveryDischargeOutcomeNull()
    {
        final QpidByteBuffer qpidByteBuffer = dischargeMessage();
        final Delivery delivery = mock(Delivery.class);
        when(delivery.getPayload()).thenReturn(qpidByteBuffer);

        final Source source = mock(Source.class);
        when(source.getOutcomes()).thenReturn(null);

        final Link_1_0<Source, Coordinator> link = mock(Link_1_0.class);
        when(link.getSource()).thenReturn(source);

        final TxnCoordinatorReceivingLinkEndpoint txnCoordinatorReceivingLinkEndpoint =
                spy(new TxnCoordinatorReceivingLinkEndpoint(_session, link));
        txnCoordinatorReceivingLinkEndpoint.start();

        final Error error = txnCoordinatorReceivingLinkEndpoint.receiveDelivery(delivery);

        verify(txnCoordinatorReceivingLinkEndpoint, times(0))
                .updateDispositions(anySet(), any(), anyBoolean());
        assertEquals("unknown-id", error.getCondition().toString());
    }

    @Test
    void receiveDeliveryDischargeOutcomeRejected()
    {
        final QpidByteBuffer qpidByteBuffer = dischargeMessage();
        final Delivery delivery = mock(Delivery.class);
        when(delivery.getPayload()).thenReturn(qpidByteBuffer);
        when(delivery.getDeliveryTag()).thenReturn(new Binary("1".getBytes(StandardCharsets.UTF_8)));

        final Source source = mock(Source.class);
        when(source.getOutcomes()).thenReturn(new Symbol[] { Symbols.AMQP_REJECTED });

        final Link_1_0<Source, Coordinator> link = mock(Link_1_0.class);
        when(link.getSource()).thenReturn(source);

        final TxnCoordinatorReceivingLinkEndpoint txnCoordinatorReceivingLinkEndpoint =
                spy(new TxnCoordinatorReceivingLinkEndpoint(_session, link));
        txnCoordinatorReceivingLinkEndpoint.start();

        final Error error = txnCoordinatorReceivingLinkEndpoint.receiveDelivery(delivery);

        verify(txnCoordinatorReceivingLinkEndpoint, times(1))
                .updateDispositions(anySet(), any(Rejected.class), anyBoolean());
        verify(link, times(1)).getSource();
        verify(source, times(1)).getOutcomes();

        assertNull(error);
    }

    @Test
    void amqpValueSectionNotFound()
    {
        final QpidByteBuffer qpidByteBuffer = emptyMessage();
        final Delivery delivery = mock(Delivery.class);
        when(delivery.getPayload()).thenReturn(qpidByteBuffer);

        final Source source = mock(Source.class);
        when(source.getOutcomes()).thenReturn(new Symbol[] { Symbols.AMQP_REJECTED });

        final Link_1_0<Source, Coordinator> link = mock(Link_1_0.class);
        when(link.getSource()).thenReturn(source);

        final TxnCoordinatorReceivingLinkEndpoint txnCoordinatorReceivingLinkEndpoint =
                new TxnCoordinatorReceivingLinkEndpoint(_session, link);
        txnCoordinatorReceivingLinkEndpoint.start();

        final ConnectionScopedRuntimeException exception = assertThrows(ConnectionScopedRuntimeException.class,
                () -> txnCoordinatorReceivingLinkEndpoint.receiveDelivery(delivery));

        assertEquals("Received no AmqpValue section", exception.getMessage());
    }

    @Test
    void invalidMessage()
    {
        final QpidByteBuffer qpidByteBuffer = QpidByteBuffer.allocateDirect(1000);
        final Delivery delivery = mock(Delivery.class);
        when(delivery.getPayload()).thenReturn(qpidByteBuffer);

        final Source source = mock(Source.class);
        when(source.getOutcomes()).thenReturn(new Symbol[] { Symbols.AMQP_REJECTED });

        final Link_1_0<Source, Coordinator> link = mock(Link_1_0.class);
        when(link.getSource()).thenReturn(source);

        final TxnCoordinatorReceivingLinkEndpoint txnCoordinatorReceivingLinkEndpoint =
                new TxnCoordinatorReceivingLinkEndpoint(_session, link);
        txnCoordinatorReceivingLinkEndpoint.start();

        final Error error = txnCoordinatorReceivingLinkEndpoint.receiveDelivery(delivery);

        assertEquals("decode-error", error.getCondition().toString());
    }

    @Test
    void unknownCommand()
    {
        final QpidByteBuffer qpidByteBuffer = coordinatorMessage();
        final Delivery delivery = mock(Delivery.class);
        when(delivery.getPayload()).thenReturn(qpidByteBuffer);

        final Source source = mock(Source.class);
        when(source.getOutcomes()).thenReturn(new Symbol[] { Symbols.AMQP_REJECTED });

        final Link_1_0<Source, Coordinator> link = mock(Link_1_0.class);
        when(link.getSource()).thenReturn(source);

        final TxnCoordinatorReceivingLinkEndpoint txnCoordinatorReceivingLinkEndpoint =
                new TxnCoordinatorReceivingLinkEndpoint(_session, link);
        txnCoordinatorReceivingLinkEndpoint.start();

        final ConnectionScopedRuntimeException exception = assertThrows(ConnectionScopedRuntimeException.class,
                () -> txnCoordinatorReceivingLinkEndpoint.receiveDelivery(delivery));

        assertEquals("Received unknown command 'Coordinator'", exception.getMessage());
    }

    private HeaderSection header()
    {
        final Header header = new Header();
        header.setTtl(UnsignedInteger.valueOf(10000L));
        return header.createEncodingRetainingSection();
    }

    private DeliveryAnnotationsSection deliveryAnnotations()
    {
        final Map<Symbol, Object> annotationMap = Map.of(Symbol.valueOf("foo"), "bar");
        final DeliveryAnnotations annotations = new DeliveryAnnotations(annotationMap);
        return annotations.createEncodingRetainingSection();
    }

    private QpidByteBuffer declareMessage()
    {
        final List<QpidByteBuffer> payloads = new ArrayList<>();
        try
        {
            add(payloads, header());
            add(payloads, deliveryAnnotations());
            add(payloads, new AmqpValue(new Declare()).createEncodingRetainingSection());
            return QpidByteBuffer.concatenate(payloads);
        }
        finally
        {
            payloads.forEach(QpidByteBuffer::dispose);
        }
    }

    private QpidByteBuffer dischargeMessage()
    {
        final List<QpidByteBuffer> payloads = new ArrayList<>();
        try
        {
            add(payloads, header());
            add(payloads, deliveryAnnotations());
            final Discharge discharge = new Discharge();
            discharge.setTxnId(new Binary(new byte[] { (byte) 1 }));
            add(payloads, new AmqpValue(discharge).createEncodingRetainingSection());
            return QpidByteBuffer.concatenate(payloads);
        }
        finally
        {
            payloads.forEach(QpidByteBuffer::dispose);
        }
    }

    private QpidByteBuffer emptyMessage()
    {
        final List<QpidByteBuffer> payloads = new ArrayList<>();
        try
        {
            add(payloads, header());
            add(payloads, deliveryAnnotations());
            return QpidByteBuffer.concatenate(payloads);
        }
        finally
        {
            payloads.forEach(QpidByteBuffer::dispose);
        }
    }

    private QpidByteBuffer coordinatorMessage()
    {
        final List<QpidByteBuffer> payloads = new ArrayList<>();
        try
        {
            add(payloads, header());
            add(payloads, deliveryAnnotations());
            add(payloads, new AmqpValue(new Coordinator()).createEncodingRetainingSection());
            return QpidByteBuffer.concatenate(payloads);
        }
        finally
        {
            payloads.forEach(QpidByteBuffer::dispose);
        }
    }

    private void add(final List<QpidByteBuffer> payloads, final EncodingRetainingSection<?> section)
    {
        try
        {
            payloads.add(section.getEncodedForm());
        }
        finally
        {
            section.dispose();
        }
    }
}
