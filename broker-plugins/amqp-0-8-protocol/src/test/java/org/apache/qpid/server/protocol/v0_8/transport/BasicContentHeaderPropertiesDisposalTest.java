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
package org.apache.qpid.server.protocol.v0_8.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.protocol.ProtocolVersion;
import org.apache.qpid.server.protocol.v0_8.AMQFrameDecodingException;
import org.apache.qpid.server.protocol.v0_8.AMQType;
import org.apache.qpid.test.utils.UnitTestBase;

class BasicContentHeaderPropertiesDisposalTest extends UnitTestBase
{
    private static final int BUFFER_SIZE = 64;
    private static final int CHANNEL_ID = 1;

    @BeforeEach
    void setUp()
    {
        QpidByteBuffer.deinitialisePool();
        QpidByteBuffer.initialisePool(BUFFER_SIZE, 20, 1.0);
    }

    @AfterEach
    void tearDown()
    {
        QpidByteBuffer.deinitialisePool();
    }

    @Test
    void constructorDisposesEncodedFormWhenHeaderTableDecodeFails()
    {
        final long baseline = QpidByteBuffer.getAllocatedDirectMemorySize();

        final QpidByteBuffer in = QpidByteBuffer.allocateDirect(BUFFER_SIZE);
        in.putInt(0x20000000);   // headers field-table length
        in.put((byte) 0);
        in.flip();
        final int size = in.remaining();

        assertThrows(IllegalArgumentException.class, () -> new BasicContentHeaderProperties(in,
                BasicContentHeaderProperties.HEADERS_MASK, size));

        in.dispose();
        assertEquals(baseline, QpidByteBuffer.getAllocatedDirectMemorySize(), "Malformed content-header headers " +
                "leaked a pooled direct buffer");
    }

    @Test
    void constructorDisposesHeadersWhenLaterPropertyDecodeFails()
    {
        final long baseline = QpidByteBuffer.getAllocatedDirectMemorySize();

        final QpidByteBuffer in = QpidByteBuffer.allocateDirect(BUFFER_SIZE);
        in.putInt(3);                                  // headers field-table length
        in.putUnsignedByte((short) 1);
        in.put((byte) 'x');
        in.put(AMQType.VOID.identifier());
        in.putUnsignedByte((short) 1);                 // expiration string length
        in.put((byte) 'x');                            // invalid numeric digit
        in.flip();
        final int size = in.remaining();

        assertThrows(AMQFrameDecodingException.class, () -> new BasicContentHeaderProperties(in,
                BasicContentHeaderProperties.HEADERS_MASK | BasicContentHeaderProperties.EXPIRATION_MASK, size));

        in.dispose();
        assertEquals(baseline, QpidByteBuffer.getAllocatedDirectMemorySize(),
                "A later property failure leaked decoded content-header headers");
    }

    @Test
    void nestedMalformedHeaderIsRejectedBeforeDispatch()
    {
        final long baseline = QpidByteBuffer.getAllocatedDirectMemorySize();
        final MethodProcessor<ChannelMethodProcessor> methodProcessor = mockMethodProcessor();
        final byte[] fieldTable = malformedNestedFieldTable();
        final QpidByteBuffer in = QpidByteBuffer.allocateDirect(BUFFER_SIZE);
        in.putUnsignedShort(ContentHeaderBody.CLASS_ID);
        in.putUnsignedShort(0);
        in.putLong(0);
        in.putUnsignedShort(BasicContentHeaderProperties.HEADERS_MASK);
        in.putUnsignedInt(fieldTable.length);
        in.put(fieldTable);
        in.flip();
        final int size = in.remaining();

        final AMQFrameDecodingException exception = assertThrows(AMQFrameDecodingException.class, () ->
                ContentHeaderBody.process(in, methodProcessor, CHANNEL_ID, size));

        assertEquals(ErrorCodes.FRAME_ERROR, exception.getErrorCode());
        verify(methodProcessor, never()).getChannelMethodProcessor(anyInt());
        in.dispose();
        assertEquals(baseline, QpidByteBuffer.getAllocatedDirectMemorySize(),
                "Malformed nested content headers retained pooled direct memory");
    }

    @Test
    void configuredNestingDepthRejectsContentHeaderBeforeDispatch()
    {
        final long baseline = QpidByteBuffer.getAllocatedDirectMemorySize();
        final MethodProcessor<ChannelMethodProcessor> methodProcessor = mockMethodProcessor();
        final byte[] fieldTable = nestedFieldTable(2);
        final QpidByteBuffer in = QpidByteBuffer.allocateDirect(BUFFER_SIZE);
        try
        {
            in.putUnsignedShort(ContentHeaderBody.CLASS_ID);
            in.putUnsignedShort(0);
            in.putLong(0);
            in.putUnsignedShort(BasicContentHeaderProperties.HEADERS_MASK);
            in.putUnsignedInt(fieldTable.length);
            in.put(fieldTable);
            in.flip();
            final int size = in.remaining();

            final AMQFrameDecodingException exception = assertThrows(AMQFrameDecodingException.class, () ->
                    ContentHeaderBody.process(in, methodProcessor, CHANNEL_ID, size, 1));

            assertEquals(ErrorCodes.RESOURCE_ERROR, exception.getErrorCode());
            verify(methodProcessor, never()).getChannelMethodProcessor(anyInt());
        }
        finally
        {
            in.dispose();
        }
        assertEquals(baseline, QpidByteBuffer.getAllocatedDirectMemorySize(),
                "Content headers rejected by configured nesting depth retained pooled direct memory");
    }

    @Test
    void propertyPaddingIsRejectedBeforeDispatch()
    {
        assertContentHeaderRejectedBeforeDispatch(0, 0, 0, new byte[] { (byte) 0xFF }, null);
    }

    @ParameterizedTest
    @ValueSource(ints = { 0x0001, 0x0002 })
    void unsupportedPropertyFlagsAreRejectedBeforeDispatch(final int propertyFlags)
    {
        assertContentHeaderRejectedBeforeDispatch(0, 0, propertyFlags, new byte[0], null);
    }

    @Test
    void unsignedBodySizeOutsideSignedRangeIsRejectedWithoutDecodingProperties() throws AMQFrameDecodingException
    {
        final long baseline = QpidByteBuffer.getAllocatedDirectMemorySize();
        final MethodProcessor<ChannelMethodProcessor> methodProcessor = mockMethodProcessor();
        final QpidByteBuffer in = QpidByteBuffer.allocateDirect(BUFFER_SIZE);
        try
        {
            in.putUnsignedShort(ContentHeaderBody.CLASS_ID);
            in.putUnsignedShort(0);
            in.putLong(Long.MIN_VALUE);
            in.putUnsignedShort(0);
            in.put((byte) 0xFF);
            in.flip();

            ContentHeaderBody.process(in, methodProcessor, CHANNEL_ID, in.remaining());

            assertFalse(in.hasRemaining());
            verify(methodProcessor).receiveOversizedMessageHeader(CHANNEL_ID, Long.MIN_VALUE);
            verify(methodProcessor, never()).getChannelMethodProcessor(anyInt());
            verify(methodProcessor, never()).getProtocolVersion();
        }
        finally
        {
            in.dispose();
        }
        assertEquals(baseline, QpidByteBuffer.getAllocatedDirectMemorySize(),
                "Oversized content header retained pooled direct memory");
    }

    @Test
    void amqp08StructuredContentIsRejectedAsNotImplementedBeforeDispatch()
    {
        final AMQFrameDecodingException exception =
                assertContentHeaderRejectedBeforeDispatch(1, 0, 0, new byte[0], ProtocolVersion.v0_8);

        assertEquals(ErrorCodes.NOT_IMPLEMENTED, exception.getErrorCode());
    }

    @Test
    void amqp09ReservedContentWeightIsRejectedAsFrameErrorBeforeDispatch()
    {
        assertReservedContentWeightRejected(ProtocolVersion.v0_9);
    }

    @Test
    void amqp091NonZeroContentWeightIsRejectedAsFrameErrorBeforeDispatch()
    {
        assertReservedContentWeightRejected(ProtocolVersion.v0_91);
    }

    @Test
    void zeroContentWeightDispatchesWithoutProtocolVersionLookup() throws AMQFrameDecodingException
    {
        final MethodProcessor<ChannelMethodProcessor> methodProcessor = mockMethodProcessor();
        final ChannelMethodProcessor channelMethodProcessor = mock(ChannelMethodProcessor.class);
        when(methodProcessor.getChannelMethodProcessor(CHANNEL_ID)).thenReturn(channelMethodProcessor);

        final QpidByteBuffer in = QpidByteBuffer.allocateDirect(BUFFER_SIZE);
        in.putUnsignedShort(ContentHeaderBody.CLASS_ID);
        in.putUnsignedShort(0);
        in.putLong(0);
        in.putUnsignedShort(0);
        in.flip();

        final ArgumentCaptor<BasicContentHeaderProperties> propertiesCaptor =
                ArgumentCaptor.forClass(BasicContentHeaderProperties.class);
        try
        {
            ContentHeaderBody.process(in, methodProcessor, CHANNEL_ID, in.remaining());

            verify(methodProcessor, never()).getProtocolVersion();
            verify(channelMethodProcessor).receiveMessageHeader(propertiesCaptor.capture(), eq(0L));
        }
        finally
        {
            if (!propertiesCaptor.getAllValues().isEmpty())
            {
                propertiesCaptor.getValue().dispose();
            }
            in.dispose();
        }
    }

    @Test
    void channelLookupFailureDisposesContentHeaderProperties()
    {
        final MethodProcessor<ChannelMethodProcessor> methodProcessor = mockMethodProcessor();
        when(methodProcessor.getChannelMethodProcessor(CHANNEL_ID))
                .thenThrow(new IllegalStateException("Connection is not open"));

        assertProcessorFailureDoesNotRetainContentHeaderProperties(methodProcessor);
    }

    @Test
    void channelDispatchFailurePreservesTransferredContentHeaderProperties()
    {
        final long baseline = QpidByteBuffer.getAllocatedDirectMemorySize();
        final MethodProcessor<ChannelMethodProcessor> methodProcessor = mockMethodProcessor();
        final ChannelMethodProcessor channelMethodProcessor = mock(ChannelMethodProcessor.class);
        when(methodProcessor.getChannelMethodProcessor(CHANNEL_ID)).thenReturn(channelMethodProcessor);
        final AtomicReference<BasicContentHeaderProperties> retainedProperties = new AtomicReference<>();
        final IllegalStateException failure = new IllegalStateException("Dispatch failed after retaining properties");
        doAnswer(invocation ->
        {
            retainedProperties.set(invocation.getArgument(0));
            throw failure;
        })
                .when(channelMethodProcessor)
                .receiveMessageHeader(any(BasicContentHeaderProperties.class), anyLong());

        final QpidByteBuffer in = QpidByteBuffer.allocateDirect(BUFFER_SIZE);
        try
        {
            in.putUnsignedShort(ContentHeaderBody.CLASS_ID);
            in.putUnsignedShort(0);
            in.putLong(0);
            in.putUnsignedShort(BasicContentHeaderProperties.HEADERS_MASK);
            in.putUnsignedInt(7);
            in.putUnsignedByte((short) 1);
            in.put((byte) 'v');
            in.put(AMQType.INT.identifier());
            in.putInt(42);
            in.flip();

            final IllegalStateException thrown = assertThrows(IllegalStateException.class, () ->
                    ContentHeaderBody.process(in, methodProcessor, CHANNEL_ID, in.remaining()));
            assertSame(failure, thrown);
        }
        finally
        {
            in.dispose();
        }

        final BasicContentHeaderProperties properties = retainedProperties.get();
        assertNotNull(properties, "Content-header properties were not retained by the receiver");
        try
        {
            assertEquals(42, properties.getHeader("v"), "Transferred field-table buffer was released by the parser");
        }
        finally
        {
            properties.dispose();
        }
        assertEquals(baseline, QpidByteBuffer.getAllocatedDirectMemorySize(),
                "Transferred content-header properties retained pooled direct memory after disposal");
    }

    @Test
    void channelDispatchFailureDoesNotLeakPropertiesDisposedByReceiver()
    {
        final MethodProcessor<ChannelMethodProcessor> methodProcessor = mockMethodProcessor();
        final ChannelMethodProcessor channelMethodProcessor = mock(ChannelMethodProcessor.class);
        when(methodProcessor.getChannelMethodProcessor(CHANNEL_ID)).thenReturn(channelMethodProcessor);
        doAnswer(invocation ->
        {
            final BasicContentHeaderProperties properties = invocation.getArgument(0);
            properties.dispose();
            throw new IllegalStateException("Dispatch rejected properties");
        })
                .when(channelMethodProcessor)
                .receiveMessageHeader(any(BasicContentHeaderProperties.class), anyLong());

        assertProcessorFailureDoesNotRetainContentHeaderProperties(methodProcessor);
    }

    private void assertProcessorFailureDoesNotRetainContentHeaderProperties(
            final MethodProcessor<ChannelMethodProcessor> methodProcessor)
    {
        final long baseline = QpidByteBuffer.getAllocatedDirectMemorySize();
        final QpidByteBuffer in = QpidByteBuffer.allocateDirect(BUFFER_SIZE);
        try
        {
            in.putUnsignedShort(ContentHeaderBody.CLASS_ID);
            in.putUnsignedShort(0);
            in.putLong(0);
            in.putUnsignedShort(0);
            in.flip();

            assertThrows(IllegalStateException.class, () ->
                    ContentHeaderBody.process(in, methodProcessor, CHANNEL_ID, in.remaining()));
        }
        finally
        {
            in.dispose();
        }
        assertEquals(baseline, QpidByteBuffer.getAllocatedDirectMemorySize(),
                "Processor failure retained content-header properties");
    }

    private void assertReservedContentWeightRejected(final ProtocolVersion protocolVersion)
    {
        final AMQFrameDecodingException exception =
                assertContentHeaderRejectedBeforeDispatch(1, 0, 0, new byte[0], protocolVersion);

        assertEquals(ErrorCodes.FRAME_ERROR, exception.getErrorCode());
    }

    private AMQFrameDecodingException assertContentHeaderRejectedBeforeDispatch(final int weight,
                                                                                final long bodySize,
                                                                                final int propertyFlags,
                                                                                final byte[] propertyList,
                                                                                final ProtocolVersion protocolVersion)
    {
        final long baseline = QpidByteBuffer.getAllocatedDirectMemorySize();
        final MethodProcessor<ChannelMethodProcessor> methodProcessor = mockMethodProcessor();
        if (protocolVersion != null)
        {
            when(methodProcessor.getProtocolVersion()).thenReturn(protocolVersion);
        }
        final QpidByteBuffer in = QpidByteBuffer.allocateDirect(BUFFER_SIZE);
        final AMQFrameDecodingException exception;
        try
        {
            in.putUnsignedShort(ContentHeaderBody.CLASS_ID);
            in.putUnsignedShort(weight);
            in.putLong(bodySize);
            in.putUnsignedShort(propertyFlags);
            in.put(propertyList);
            in.flip();
            final int size = in.remaining();

            exception = assertThrows(AMQFrameDecodingException.class, () ->
                    ContentHeaderBody.process(in, methodProcessor, CHANNEL_ID, size));

            verify(methodProcessor, never()).getChannelMethodProcessor(anyInt());
        }
        finally
        {
            in.dispose();
        }
        assertEquals(baseline, QpidByteBuffer.getAllocatedDirectMemorySize(),
                "Rejected content-header properties retained pooled direct memory");
        return exception;
    }

    private static MethodProcessor<ChannelMethodProcessor> mockMethodProcessor()
    {
        return mock(MethodProcessor.class);
    }

    private static byte[] malformedNestedFieldTable()
    {
        final byte[] malformedNestedTable = ByteBuffer.allocate(7)
                .put((byte) 1)
                .put((byte) 's')
                .put(AMQType.LONG_STRING.identifier())
                .putInt(0x80000000)
                .array();
        return ByteBuffer.allocate(7 + malformedNestedTable.length)
                .put((byte) 1)
                .put((byte) 'n')
                .put(AMQType.FIELD_TABLE.identifier())
                .putInt(malformedNestedTable.length)
                .put(malformedNestedTable)
                .array();
    }

    private static byte[] nestedFieldTable(final int depth)
    {
        final ByteBuffer buffer = ByteBuffer.allocate(7 * depth);
        for (int remainingDepth = depth; remainingDepth > 1; remainingDepth--)
        {
            buffer.put((byte) 1);
            buffer.put((byte) 'n');
            buffer.put(AMQType.FIELD_TABLE.identifier());
            buffer.putInt(7 * (remainingDepth - 1));
        }
        buffer.put((byte) 1);
        buffer.put((byte) 'v');
        buffer.put(AMQType.INT.identifier());
        buffer.putInt(42);
        return buffer.array();
    }
}
