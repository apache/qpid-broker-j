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
package org.apache.qpid.server.protocol.v1_0.type.messaging;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.codec.ValueHandler;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionDecoderImpl;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.codec.AMQPDescribedTypeRegistry;
import org.apache.qpid.server.protocol.v1_0.type.messaging.codec.EncodingRetaining;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.test.utils.UnitTestBase;

@Isolated("Changes the global QpidByteBuffer pool")
class AbstractSectionTest extends UnitTestBase
{
    private static final AMQPDescribedTypeRegistry TYPE_REGISTRY = AMQPDescribedTypeRegistry
            .newInstance()
            .registerTransportLayer()
            .registerMessagingLayer()
            .registerTransactionLayer()
            .registerSecurityLayer();

    private int _buffersInUse;

    @BeforeEach
    void beforeEach()
    {
        QpidByteBuffer.deinitialisePool();
        _buffersInUse = QpidByteBuffer.getNumberOfBuffersInUse();
        QpidByteBuffer.initialisePool(256, 16, 0.5);
    }

    @AfterEach
    void afterEach()
    {
        QpidByteBuffer.deinitialisePool();
        assertEquals(_buffersInUse, QpidByteBuffer.getNumberOfBuffersInUse());
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 255, 256, 257, 4096})
    void clearReleasesDirectMemoryWithoutRequiringDisposal(final int payloadSize)
    {
        final byte[] bytes = new byte[payloadSize + 8];
        try (final QpidByteBuffer encoding = QpidByteBuffer.wrap(bytes))
        {
            encoding.put((byte) 0).put((byte) 0x53).put((byte) 0x75).put((byte) 0xb0).putInt(payloadSize);
        }
        final DataSection section;
        try (final QpidByteBuffer input = QpidByteBuffer.allocateDirect(bytes.length))
        {
            input.put(bytes).flip();
            section = new DataSection(input);
        }
        try
        {
            section.clearEncodedForm();
            section.getValue();
            section.reallocate();
            section.clearEncodedForm();
            try (final QpidByteBuffer encoding = section.getEncodedForm())
            {
                assertArrayEquals(bytes, readBytes(encoding));
            }
            QpidByteBuffer.deinitialisePool();
            assertEquals(_buffersInUse, QpidByteBuffer.getNumberOfBuffersInUse(),
                    "A detached section must not require an owner to dispose pooled memory");
        }
        finally
        {
            section.dispose();
        }
    }

    @ParameterizedTest
    @CsvSource({"false, true", "true, false", "false, false", "true, true"})
    void clearDetachesFragmentsWithoutRequiringDisposal(final boolean firstDirect, final boolean secondDirect)
    {
        final byte[] bytes = HexFormat.of().parseHex("00800000000000000073d00000000400000000");
        final PropertiesSection section;
        try (final QpidByteBuffer first = QpidByteBuffer.allocate(firstDirect, 9);
             final QpidByteBuffer second = QpidByteBuffer.allocate(secondDirect, bytes.length - 9))
        {
            first.put(bytes, 0, 9).flip();
            second.put(bytes, 9, bytes.length - 9).flip();
            try (final QpidByteBuffer input = QpidByteBuffer.concatenate(first, second))
            {
                section = new PropertiesSection(input);
            }
        }
        try
        {
            if (firstDirect || secondDirect)
            {
                assertTrue(QpidByteBuffer.getNumberOfBuffersInUse() > _buffersInUse);
            }
            section.clearEncodedForm();
            QpidByteBuffer.deinitialisePool();
            assertEquals(_buffersInUse, QpidByteBuffer.getNumberOfBuffersInUse(),
                    "Detachment must release every pooled fragment before section disposal");

            QpidByteBuffer.initialisePool(256, 16, 0.5);
            section.getValue();
            section.reallocate();
            section.clearEncodedForm();
            assertEquals(bytes.length, section.getEncodedSize());
            final EncodingRetaining copy = section.copy();
            try (final QpidByteBuffer encoding = section.getEncodedForm();
                 final QpidByteBuffer destination = QpidByteBuffer.allocate(bytes.length))
            {
                assertArrayEquals(bytes, readBytes(encoding));
                copy.writeTo(destination);
                destination.flip();
                assertArrayEquals(bytes, readBytes(destination));
                QpidByteBuffer.deinitialisePool();
                assertEquals(_buffersInUse, QpidByteBuffer.getNumberOfBuffersInUse(),
                        "Reads and repeated detachment must not recreate pooled ownership");
            }
            finally
            {
                copy.dispose();
            }
        }
        finally
        {
            section.dispose();
        }
    }

    @Test
    void outstandingCopySurvivesClearAndDisposal() throws AmqpErrorException
    {
        final byte[] bytes = HexFormat.of().parseHex("00800000000000000073d00000000400000000");
        final EncodingRetainingSection<?> section = decode(bytes);
        final EncodingRetaining beforeClear = section.copy();
        try
        {
            section.clearEncodedForm();
            final EncodingRetaining afterClear = section.copy();
            try
            {
                section.dispose();
                QpidByteBuffer.deinitialisePool();
                assertTrue(QpidByteBuffer.getNumberOfBuffersInUse() > _buffersInUse,
                           "A copy issued before detachment still owns its pooled reference");
                try (final QpidByteBuffer before = beforeClear.getEncodedForm();
                     final QpidByteBuffer after = afterClear.getEncodedForm())
                {
                    assertArrayEquals(bytes, readBytes(before));
                    assertArrayEquals(bytes, readBytes(after));
                }
                beforeClear.dispose();
                assertEquals(_buffersInUse, QpidByteBuffer.getNumberOfBuffersInUse(),
                             "Only the copy issued before detachment should retain pooled storage");
            }
            finally
            {
                afterClear.dispose();
            }
        }
        finally
        {
            beforeClear.dispose();
            section.dispose();
        }
    }

    @Test
    void clearPreservesLazyDecodeLimit()
    {
        final byte[] bytes = HexFormat.of().parseHex("005377c00501c0020140");
        try (final QpidByteBuffer input = QpidByteBuffer.allocateDirect(bytes.length))
        {
            input.put(bytes).flip();
            final AmqpValueSection section = new AmqpValueSection(input, 1);
            try
            {
                section.clearEncodedForm();
                assertThrows(ConnectionScopedRuntimeException.class, section::getValue);
            }
            finally
            {
                section.dispose();
            }
        }
    }

    @Test
    void clearPreservesZeroWidthArrayLimit()
    {
        final byte[] bytes = HexFormat.of().parseHex("005377f0000000050000000541");
        try (final QpidByteBuffer input = QpidByteBuffer.allocateDirect(bytes.length))
        {
            input.put(bytes).flip();
            final ValueHandler handler = new ValueHandler(TYPE_REGISTRY, 64, 2);
            final AmqpValueSection section = new AmqpValueSection(input, handler);
            try
            {
                section.clearEncodedForm();
                assertThrows(ConnectionScopedRuntimeException.class, section::getValue);
            }
            finally
            {
                section.dispose();
            }
        }
    }

    @Test
    void concurrentCopiesAndClearsPreserveBytes() throws Exception
    {
        final byte[] bytes = HexFormat.of()
                .parseHex("00800000000000000074d10000001000000002b1000000016bb10000000176");
        final EncodingRetainingSection<?> section = decode(bytes);
        final CountDownLatch workersStopped = new CountDownLatch(2);
        final ThreadFactory threadFactory = Executors.defaultThreadFactory();
        final ExecutorService executor = Executors.newFixedThreadPool(2,
                QpidByteBuffer.createQpidByteBufferTrackingThreadFactory(task -> threadFactory.newThread(() ->
                {
                    try
                    {
                        task.run();
                    }
                    finally
                    {
                        workersStopped.countDown();
                    }
                })));
        final CountDownLatch start = new CountDownLatch(1);
        try
        {
            final Future<?> clears = executor.submit(() ->
            {
                start.await();
                for (int i = 0; i < 1000; i++)
                {
                    section.clearEncodedForm();
                    section.reallocate();
                }
                return null;
            });
            final Future<?> copies = executor.submit(() ->
            {
                start.await();
                for (int i = 0; i < 1000; i++)
                {
                    final EncodingRetaining copy = section.copy();
                    try (final QpidByteBuffer encoding = copy.getEncodedForm())
                    {
                        assertArrayEquals(bytes, readBytes(encoding));
                    }
                    finally
                    {
                        copy.dispose();
                    }
                }
                return null;
            });
            start.countDown();
            clears.get(10, TimeUnit.SECONDS);
            copies.get(10, TimeUnit.SECONDS);
        }
        finally
        {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
            assertTrue(workersStopped.await(10, TimeUnit.SECONDS));
            section.dispose();
        }
    }

    @ParameterizedTest
    @MethodSource("sectionEncodings")
    void clearPreservesExactEncoding(final String hex) throws AmqpErrorException
    {
        final byte[] bytes = HexFormat.of().parseHex(hex);
        final EncodingRetainingSection<?> section = decode(bytes);
        try
        {
            section.clearEncodedForm();
            assertEquals(bytes.length, section.getEncodedSize());
            try (final QpidByteBuffer encoded = section.getEncodedForm())
            {
                assertFalse(encoded.isDirect());
                assertArrayEquals(bytes, readBytes(encoded));
            }

            final EncodingRetaining copy = section.copy();
            try
            {
                section.clearEncodedForm();
                section.reallocate();
                try (final QpidByteBuffer destination = QpidByteBuffer.allocate(bytes.length))
                {
                    copy.writeTo(destination);
                    destination.flip();
                    assertArrayEquals(bytes, readBytes(destination));
                }
            }
            finally
            {
                copy.dispose();
            }
        }
        finally
        {
            section.dispose();
        }
    }

    private EncodingRetainingSection<?> decode(final byte[] bytes) throws AmqpErrorException
    {
        try (final QpidByteBuffer input = QpidByteBuffer.allocateDirect(bytes.length))
        {
            input.put(bytes).flip();
            final List<EncodingRetainingSection<?>> sections =
                    new SectionDecoderImpl(TYPE_REGISTRY.getSectionDecoderRegistry()).parseAll(input);
            assertEquals(1, sections.size());
            return sections.get(0);
        }
    }

    private byte[] readBytes(final QpidByteBuffer buffer)
    {
        final byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    private static Stream<String> sectionEncodings()
    {
        return Stream.of("00800000000000000070d00000000400000000",
                         "00800000000000000071d10000001000000002b3000000016bb10000000176",
                         "00800000000000000072d10000001000000002b3000000016bb10000000176",
                         "00800000000000000073d00000000400000000",
                         "00800000000000000074d10000001000000002b1000000016bb10000000176",
                         "00800000000000000075b00000000176",
                         "00800000000000000076d00000000400000000",
                         "00800000000000000077827ff0000000000001",
                         "00800000000000000078d10000001000000002b3000000016bb10000000176",
                         "00a31f616d71703a6170706c69636174696f6e2d70726f706572746965733a6d6170d10000000400000000");
    }
}
