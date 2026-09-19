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
package org.apache.qpid.server.protocol.v0_8;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.mockito.ArgumentCaptor;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.protocol.ProtocolVersion;
import org.apache.qpid.server.protocol.v0_8.transport.BasicConsumeBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionStartOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeDeclareBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueBindBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueDeclareBody;
import org.apache.qpid.server.protocol.v0_8.transport.ServerChannelMethodProcessor;
import org.apache.qpid.server.protocol.v0_8.transport.ServerMethodProcessor;
import org.apache.qpid.test.utils.UnitTestBase;

class FieldTableNestingDecoderTest extends UnitTestBase
{
    private static final byte FRAME_END = (byte) 0xCE;
    private static final byte METHOD_FRAME = 1;
    private static final int CHANNEL_ID = 1;
    private static final int CONTENT_HEADERS_PROPERTY_FLAG = 1 << 13;
    private static final int CONFIGURED_MAXIMUM = 4;
    private static final int CONFIGURED_MAXIMUM_ABOVE_DEFAULT = AMQPConnection_0_8.DEFAULT_CODEC_MAX_NESTED_OBJECTS + 10;

    @ParameterizedTest(name = "{0} accepts {1} arguments at the nesting limit")
    @MethodSource("protocolVersionsAndMethods")
    void methodArgumentsAtLimitAreAccepted(final ProtocolVersion protocolVersion,
                                           final FieldTableMethod method)
            throws Exception
    {
        final ServerChannelMethodProcessor channelMethodProcessor = mock(ServerChannelMethodProcessor.class);
        when(channelMethodProcessor.ignoreAllButCloseOk()).thenReturn(true);
        final ServerMethodProcessor<ServerChannelMethodProcessor> methodProcessor =
                createMethodProcessor(protocolVersion, channelMethodProcessor);
        final ServerDecoder decoder = new ServerDecoder(methodProcessor, CONFIGURED_MAXIMUM);
        decoder.setExpectProtocolInitiation(false);

        try (final QpidByteBuffer frame = createMethodFrame(method, buildNestedTable(CONFIGURED_MAXIMUM)))
        {
            decoder.decodeBuffer(frame);
        }

        verify(channelMethodProcessor).ignoreAllButCloseOk();
    }

    @ParameterizedTest(name = "{0} preserves a configured limit above the default after dispatch")
    @MethodSource("protocolVersions")
    void configuredLimitAboveDefaultIsPreservedAfterDispatch(final ProtocolVersion protocolVersion)
            throws Exception
    {
        final ServerChannelMethodProcessor channelMethodProcessor = mock(ServerChannelMethodProcessor.class);
        final ServerMethodProcessor<ServerChannelMethodProcessor> methodProcessor =
                createMethodProcessor(protocolVersion, channelMethodProcessor);
        final ServerDecoder decoder = new ServerDecoder(methodProcessor, CONFIGURED_MAXIMUM_ABOVE_DEFAULT);
        decoder.setExpectProtocolInitiation(false);

        try (final QpidByteBuffer frame = createMethodFrame(FieldTableMethod.QUEUE_DECLARE,
                buildNestedTable(CONFIGURED_MAXIMUM_ABOVE_DEFAULT)))
        {
            decoder.decodeBuffer(frame);
        }

        final ArgumentCaptor<FieldTable> argumentsCaptor = ArgumentCaptor.forClass(FieldTable.class);
        verify(channelMethodProcessor).receiveQueueDeclare(any(), eq(false), eq(false), eq(false), eq(false),
                eq(false), argumentsCaptor.capture());
        final FieldTable arguments = argumentsCaptor.getValue();
        try
        {
            final FieldTable nested = (FieldTable) arguments.get("n");
            try
            {
                assertDoesNotThrow(() -> FieldTable.convertToMap(nested));
            }
            finally
            {
                nested.dispose();
            }
        }
        finally
        {
            arguments.dispose();
        }
    }

    @ParameterizedTest(name = "{0} rejects over-limit {1} arguments")
    @MethodSource("protocolVersionsAndMethods")
    void methodArgumentsOverLimitAreRejectedBeforeDispatch(final ProtocolVersion protocolVersion,
                                                           final FieldTableMethod method)
    {
        final ServerChannelMethodProcessor channelMethodProcessor = mock(ServerChannelMethodProcessor.class);
        final ServerMethodProcessor<ServerChannelMethodProcessor> methodProcessor =
                createMethodProcessor(protocolVersion, channelMethodProcessor);
        final ServerDecoder decoder = new ServerDecoder(methodProcessor, CONFIGURED_MAXIMUM);
        decoder.setExpectProtocolInitiation(false);
        final AMQFrameDecodingException exception;

        try (final QpidByteBuffer frame = createMethodFrame(method, buildNestedTable(CONFIGURED_MAXIMUM + 1)))
        {
            exception = assertThrows(AMQFrameDecodingException.class, () -> decoder.decodeBuffer(frame));
        }

        final int classAndMethod = method.getClassAndMethod();
        assertEquals(ErrorCodes.RESOURCE_ERROR, exception.getErrorCode());
        assertEquals(classAndMethod >> 16, exception.getClassId());
        assertEquals(classAndMethod & 0xFFFF, exception.getMethodId());
        verify(channelMethodProcessor, never()).ignoreAllButCloseOk();
    }

    @ParameterizedTest(name = "{0} accepts content headers at the nesting limit")
    @MethodSource("protocolVersions")
    void contentHeadersAtLimitAreAccepted(final ProtocolVersion protocolVersion) throws Exception
    {
        final ServerChannelMethodProcessor channelMethodProcessor = mock(ServerChannelMethodProcessor.class);
        when(channelMethodProcessor.ignoreAllButCloseOk()).thenReturn(true);
        final ServerMethodProcessor<ServerChannelMethodProcessor> methodProcessor =
                createMethodProcessor(protocolVersion, channelMethodProcessor);
        final ServerDecoder decoder = new ServerDecoder(methodProcessor, CONFIGURED_MAXIMUM);
        decoder.setExpectProtocolInitiation(false);

        try (final QpidByteBuffer frame = createContentHeaderFrame(buildNestedTable(CONFIGURED_MAXIMUM)))
        {
            decoder.decodeBuffer(frame);
        }

        verify(channelMethodProcessor).ignoreAllButCloseOk();
    }

    @ParameterizedTest(name = "{0} rejects over-limit content headers")
    @MethodSource("protocolVersions")
    void contentHeadersOverLimitAreRejectedBeforeDispatch(final ProtocolVersion protocolVersion)
    {
        final ServerChannelMethodProcessor channelMethodProcessor = mock(ServerChannelMethodProcessor.class);
        final ServerMethodProcessor<ServerChannelMethodProcessor> methodProcessor =
                createMethodProcessor(protocolVersion, channelMethodProcessor);
        final ServerDecoder decoder = new ServerDecoder(methodProcessor, CONFIGURED_MAXIMUM);
        decoder.setExpectProtocolInitiation(false);
        final AMQFrameDecodingException exception;

        try (final QpidByteBuffer frame = createContentHeaderFrame(buildNestedTable(CONFIGURED_MAXIMUM + 1)))
        {
            exception = assertThrows(AMQFrameDecodingException.class, () -> decoder.decodeBuffer(frame));
        }

        assertEquals(ErrorCodes.RESOURCE_ERROR, exception.getErrorCode());
        assertEquals(0, exception.getClassId());
        assertEquals(0, exception.getMethodId());
        verify(channelMethodProcessor, never()).ignoreAllButCloseOk();
    }

    @Test
    void brokerDecoderUsesProtocolSpecificNestingContext()
    {
        final AMQPConnection_0_8Impl connection = mock(AMQPConnection_0_8Impl.class);
        when(connection.getContextValue(Integer.class, AMQPConnection_0_8.CODEC_MAX_NESTED_OBJECTS))
                .thenReturn(CONFIGURED_MAXIMUM);
        final BrokerDecoder decoder = new BrokerDecoder(connection);
        decoder.setExpectProtocolInitiation(false);
        final AMQFrameDecodingException exception;

        try (final QpidByteBuffer frame = createConnectionStartOkFrame(buildNestedTable(CONFIGURED_MAXIMUM + 1)))
        {
            exception = assertThrows(AMQFrameDecodingException.class, () -> decoder.decodeBuffer(frame));
        }

        assertEquals(ErrorCodes.RESOURCE_ERROR, exception.getErrorCode());
        assertEquals(ConnectionStartOkBody.CLASS_ID, exception.getClassId());
        assertEquals(ConnectionStartOkBody.METHOD_ID, exception.getMethodId());
        verify(connection).getContextValue(Integer.class, AMQPConnection_0_8.CODEC_MAX_NESTED_OBJECTS);
        verify(connection, never()).receiveConnectionStartOk(any(), any(), any(), any());
    }

    @Test
    void negativeProtocolSpecificNestingContextIsRejected()
    {
        final AMQPConnection_0_8Impl connection = mock(AMQPConnection_0_8Impl.class);
        when(connection.getContextValue(Integer.class, AMQPConnection_0_8.CODEC_MAX_NESTED_OBJECTS)).thenReturn(-1);

        assertThrows(IllegalArgumentException.class, () -> new BrokerDecoder(connection));
    }

    private static Stream<Arguments> protocolVersionsAndMethods()
    {
        return protocolVersions().flatMap(protocolVersion -> Stream.of(FieldTableMethod.values())
                .map(method -> Arguments.of(protocolVersion, method)));
    }

    private static Stream<ProtocolVersion> protocolVersions()
    {
        return Stream.of(ProtocolVersion.v0_8, ProtocolVersion.v0_9, ProtocolVersion.v0_91);
    }

    private static ServerMethodProcessor<ServerChannelMethodProcessor> createMethodProcessor(
            final ProtocolVersion protocolVersion,
            final ServerChannelMethodProcessor channelMethodProcessor)
    {
        final ServerMethodProcessor<ServerChannelMethodProcessor> methodProcessor =
                mock(ServerMethodProcessor.class);
        when(methodProcessor.getProtocolVersion()).thenReturn(protocolVersion);
        when(methodProcessor.getChannelMethodProcessor(CHANNEL_ID)).thenReturn(channelMethodProcessor);
        return methodProcessor;
    }

    private static QpidByteBuffer createMethodFrame(final FieldTableMethod method, final byte[] fieldTable)
    {
        final int argumentsSize = switch (method)
        {
            case QUEUE_DECLARE -> Short.BYTES + 1 + 1;
            case QUEUE_BIND -> Short.BYTES + 1 + 1 + 1 + 1;
            case EXCHANGE_DECLARE, BASIC_CONSUME -> Short.BYTES + 1 + 1 + 1;
        };

        final int bodySize = Integer.BYTES + argumentsSize + Integer.BYTES + fieldTable.length;
        final QpidByteBuffer frame = QpidByteBuffer.allocate(AMQDecoder.FRAME_HEADER_SIZE + bodySize + 1);
        writeFrameHeader(frame, METHOD_FRAME, CHANNEL_ID, bodySize);
        frame.putInt(method.getClassAndMethod());
        frame.putUnsignedShort(0);
        frame.put((byte) 0);
        if (method != FieldTableMethod.QUEUE_DECLARE)
        {
            frame.put((byte) 0);
        }
        if (method == FieldTableMethod.QUEUE_BIND)
        {
            frame.put((byte) 0);
        }
        frame.put((byte) 0);
        writeFieldTable(frame, fieldTable);
        frame.put(FRAME_END);
        frame.flip();
        return frame;
    }

    private static QpidByteBuffer createContentHeaderFrame(final byte[] fieldTable)
    {
        final int bodySize = 2 * Short.BYTES + Long.BYTES + Short.BYTES + Integer.BYTES + fieldTable.length;
        final QpidByteBuffer frame = QpidByteBuffer.allocate(AMQDecoder.FRAME_HEADER_SIZE + bodySize + 1);
        writeFrameHeader(frame, ContentHeaderBody.TYPE, CHANNEL_ID, bodySize);
        frame.putUnsignedShort(ContentHeaderBody.CLASS_ID);
        frame.putUnsignedShort(0);
        frame.putLong(0);
        frame.putUnsignedShort(CONTENT_HEADERS_PROPERTY_FLAG);
        writeFieldTable(frame, fieldTable);
        frame.put(FRAME_END);
        frame.flip();
        return frame;
    }

    private static QpidByteBuffer createConnectionStartOkFrame(final byte[] fieldTable)
    {
        final int bodySize = Integer.BYTES + Integer.BYTES + fieldTable.length + 1 + Integer.BYTES + 1;
        final QpidByteBuffer frame = QpidByteBuffer.allocate(AMQDecoder.FRAME_HEADER_SIZE + bodySize + 1);
        writeFrameHeader(frame, METHOD_FRAME, 0, bodySize);
        frame.putInt((ConnectionStartOkBody.CLASS_ID << 16) | ConnectionStartOkBody.METHOD_ID);
        writeFieldTable(frame, fieldTable);
        frame.put((byte) 0);
        frame.putUnsignedInt(0);
        frame.put((byte) 0);
        frame.put(FRAME_END);
        frame.flip();
        return frame;
    }

    private static void writeFrameHeader(final QpidByteBuffer buffer,
                                         final byte type,
                                         final int channel,
                                         final long bodySize)
    {
        buffer.put(type);
        buffer.putUnsignedShort(channel);
        buffer.putUnsignedInt(bodySize);
    }

    private static void writeFieldTable(final QpidByteBuffer buffer, final byte[] fieldTable)
    {
        buffer.putUnsignedInt(fieldTable.length);
        buffer.put(fieldTable);
    }

    private static byte[] buildNestedTable(final int depth)
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

    private enum FieldTableMethod
    {
        QUEUE_DECLARE,
        QUEUE_BIND,
        EXCHANGE_DECLARE,
        BASIC_CONSUME;

        int getClassAndMethod()
        {
            return switch (this)
            {
                case QUEUE_DECLARE -> (QueueDeclareBody.CLASS_ID << 16) | QueueDeclareBody.METHOD_ID;
                case QUEUE_BIND -> (QueueBindBody.CLASS_ID << 16) | QueueBindBody.METHOD_ID;
                case EXCHANGE_DECLARE -> (ExchangeDeclareBody.CLASS_ID << 16) | ExchangeDeclareBody.METHOD_ID;
                case BASIC_CONSUME -> (BasicConsumeBody.CLASS_ID << 16) | BasicConsumeBody.METHOD_ID;
            };
        }
    }
}
