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

package org.apache.qpid.server.protocol.v1_0;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.type.BaseSource;
import org.apache.qpid.server.protocol.v1_0.type.BaseTarget;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.transport.Transfer;
import org.apache.qpid.test.utils.UnitTestBase;

class DeliveryTest extends UnitTestBase
{
    @Test
    void singleTransferPayloadIsReturnedAndSizeIsCounted()
    {
        final byte[] bytes = new byte[]{1, 2, 3, 4};
        final Transfer t = new Transfer();
        t.setDeliveryId(UnsignedInteger.valueOf(1));
        t.setDeliveryTag(new Binary(new byte[]{0x01}));

        try (final QpidByteBuffer buf = QpidByteBuffer.wrap(bytes))
        {
            t.setPayload(buf);
        }

        final LinkEndpoint<? extends BaseSource, ? extends BaseTarget> endpoint = mock(LinkEndpoint.class);
        final Delivery delivery = new Delivery(t, endpoint);

        assertEquals(4L, delivery.getTotalPayloadSize());
        assertNull(t.getPayload(), "Accepted transfer payload should be disposed after delivery ingestion");

        try (final QpidByteBuffer payload = delivery.getPayload())
        {
            final byte[] actual = new byte[payload.remaining()];
            payload.copyTo(actual);
            assertArrayEquals(bytes, actual);
        }
    }

    @Test
    void singleTransferWithNullPayloadReturnsEmptyBuffer()
    {
        final Transfer t = new Transfer();
        t.setDeliveryId(UnsignedInteger.valueOf(1));
        t.setDeliveryTag(new Binary(new byte[]{0x01}));

        final LinkEndpoint<? extends BaseSource, ? extends BaseTarget> endpoint = mock(LinkEndpoint.class);
        final Delivery delivery = new Delivery(t, endpoint);

        assertEquals(0L, delivery.getTotalPayloadSize());
        assertNull(t.getPayload(), "Accepted transfer should be disposed after delivery ingestion");

        try (final QpidByteBuffer payload = delivery.getPayload())
        {
            assertEquals(0, payload.remaining());
        }
    }

    @Test
    void multipleTransfersAreConcatenatedAndSizeIsSum()
    {
        final LinkEndpoint<? extends BaseSource, ? extends BaseTarget> endpoint = mock(LinkEndpoint.class);

        final Transfer t1 = new Transfer();
        t1.setDeliveryId(UnsignedInteger.valueOf(1));
        t1.setDeliveryTag(new Binary(new byte[]{0x01}));
        t1.setMore(true);
        try (final QpidByteBuffer buf = QpidByteBuffer.wrap(new byte[]{1, 2}))
        {
            t1.setPayload(buf);
        }

        final Delivery delivery = new Delivery(t1, endpoint);
        assertNull(t1.getPayload(), "Initial transfer payload should be disposed after delivery ingestion");

        final Transfer t2 = new Transfer();
        t2.setMore(false);
        try (final QpidByteBuffer buf = QpidByteBuffer.wrap(new byte[]{3, 4, 5}))
        {
            t2.setPayload(buf);
        }

        delivery.addTransfer(t2);
        assertNull(t2.getPayload(), "Subsequent transfer payload should be disposed after delivery ingestion");

        assertEquals(5L, delivery.getTotalPayloadSize());

        try (final QpidByteBuffer payload = delivery.getPayload())
        {
            final byte[] actual = new byte[payload.remaining()];
            payload.copyTo(actual);
            assertArrayEquals(new byte[]{1, 2, 3, 4, 5}, actual);
        }
    }

    @Test
    void transferCountIsTracked()
    {
        final LinkEndpoint<? extends BaseSource, ? extends BaseTarget> endpoint = mock(LinkEndpoint.class);

        final Transfer transfer1 = new Transfer();
        transfer1.setDeliveryId(UnsignedInteger.valueOf(1));
        transfer1.setDeliveryTag(new Binary(new byte[]{0x01}));
        transfer1.setMore(true);

        final Delivery delivery = new Delivery(transfer1, endpoint);
        assertEquals(1, delivery.getTransferCount());

        for (int i = 0; i < 10; i++)
        {
            final Transfer transfer = new Transfer();
            transfer.setMore(true);
            delivery.addTransfer(transfer);
            assertNull(transfer.getPayload(), "Accepted transfer should be disposed after delivery ingestion");
        }

        assertEquals(11, delivery.getTransferCount());
    }

    @Test
    void transferCountTracksEmptyPayloadTransfers()
    {
        final LinkEndpoint<? extends BaseSource, ? extends BaseTarget> endpoint = mock(LinkEndpoint.class);

        final Transfer transfer1 = new Transfer();
        transfer1.setDeliveryId(UnsignedInteger.valueOf(1));
        transfer1.setDeliveryTag(new Binary(new byte[]{0x01}));
        transfer1.setMore(true);

        final Delivery delivery = new Delivery(transfer1, endpoint);

        // Add many continuation transfers with no payload
        for (int i = 0; i < 100; i++)
        {
            final Transfer transfer = new Transfer();
            transfer.setMore(true);
            // No payload set
            delivery.addTransfer(transfer);
            assertNull(transfer.getPayload(), "Accepted transfer should be disposed after delivery ingestion");
        }

        assertEquals(101, delivery.getTransferCount());
        // Payload size should be 0 since no payload was ever set
        assertEquals(0L, delivery.getTotalPayloadSize());
        try (final QpidByteBuffer payload = delivery.getPayload())
        {
            assertEquals(0, payload.remaining());
        }
    }

    @Test
    void discardClearsRetainedPayloadFragments()
    {
        final LinkEndpoint<? extends BaseSource, ? extends BaseTarget> endpoint = mock(LinkEndpoint.class);

        final Transfer transfer1 = new Transfer();
        transfer1.setDeliveryId(UnsignedInteger.valueOf(1));
        transfer1.setDeliveryTag(new Binary(new byte[]{0x01}));
        transfer1.setMore(true);
        try (final QpidByteBuffer buf = QpidByteBuffer.wrap(new byte[]{1, 2}))
        {
            transfer1.setPayload(buf);
        }

        final Delivery delivery = new Delivery(transfer1, endpoint);

        final Transfer transfer2 = new Transfer();
        transfer2.setMore(true);
        try (final QpidByteBuffer buf = QpidByteBuffer.wrap(new byte[]{3, 4}))
        {
            transfer2.setPayload(buf);
        }

        delivery.addTransfer(transfer2);
        delivery.discard();

        try (final QpidByteBuffer payload = delivery.getPayload())
        {
            assertEquals(0, payload.remaining());
        }
    }

    @Test
    void abortedTransferDiscardsRetainedPayloadAndIsDisposed()
    {
        final LinkEndpoint<? extends BaseSource, ? extends BaseTarget> endpoint = mock(LinkEndpoint.class);

        final Transfer transfer1 = new Transfer();
        transfer1.setDeliveryId(UnsignedInteger.valueOf(1));
        transfer1.setDeliveryTag(new Binary(new byte[]{0x01}));
        transfer1.setMore(true);
        try (final QpidByteBuffer buf = QpidByteBuffer.wrap(new byte[]{1, 2}))
        {
            transfer1.setPayload(buf);
        }

        final Delivery delivery = new Delivery(transfer1, endpoint);

        final Transfer abortedTransfer = new Transfer();
        abortedTransfer.setAborted(true);
        try (final QpidByteBuffer buf = QpidByteBuffer.wrap(new byte[]{3, 4}))
        {
            abortedTransfer.setPayload(buf);
        }

        delivery.addTransfer(abortedTransfer);

        assertTrue(delivery.isAborted());
        assertNull(abortedTransfer.getPayload(), "Aborted transfer payload should be disposed after delivery ingestion");
        try (final QpidByteBuffer payload = delivery.getPayload())
        {
            assertEquals(0, payload.remaining());
        }
    }
}
