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

package org.apache.qpid.server.protocol.v0_10;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_10.transport.ProtocolError;
import org.apache.qpid.server.protocol.v0_10.transport.SegmentType;
import org.apache.qpid.test.utils.UnitTestBase;

class ServerInputHandlerTest extends UnitTestBase
{
    private static final byte FIRST_FRAGMENT = ServerFrame.FIRST_SEG | ServerFrame.LAST_SEG | ServerFrame.FIRST_FRAME;

    private ServerAssembler _assembler;
    private ServerInputHandler _handler;

    @BeforeEach
    void setUp()
    {
        _assembler = mock(ServerAssembler.class);
        _handler = new ServerInputHandler(_assembler);

        try (final QpidByteBuffer protocolHeader = QpidByteBuffer.wrap(new byte[]{'A', 'M', 'Q', 'P', 1, 1, 0, 10}))
        {
            _handler.received(protocolHeader);
        }
        reset(_assembler);
    }

    @Test
    void malformedFrameDropsFramesParsedEarlierFromSameBuffer()
    {
        final ByteBuffer inputBytes = ByteBuffer.allocate(2 * ServerFrame.HEADER_SIZE + 1);
        inputBytes.put(FIRST_FRAGMENT);
        inputBytes.put((byte) SegmentType.COMMAND.getValue());
        inputBytes.putShort((short) (ServerFrame.HEADER_SIZE + 1));
        inputBytes.put((byte) 0);
        inputBytes.put((byte) 1);
        inputBytes.putShort((short) 1);
        inputBytes.putInt(0);
        inputBytes.put((byte) 0);

        inputBytes.put((byte) (ServerFrame.FIRST_SEG | ServerFrame.LAST_SEG | ServerFrame.FIRST_FRAME |
                ServerFrame.LAST_FRAME));
        inputBytes.put((byte) 4);

        try (final QpidByteBuffer input = QpidByteBuffer.wrap(inputBytes.array()))
        {
            _handler.received(input);
        }

        verify(_assembler).error(isA(ProtocolError.class));
        verify(_assembler, never()).received(anyList());
    }

    @ParameterizedTest
    @MethodSource("parsingFailures")
    void testParsingFailureDisposesQueuedFrames(final Throwable failure)
    {
        final int frameSize = ServerFrame.HEADER_SIZE + 1;
        final ByteBuffer encoded = ByteBuffer.allocate(2 * frameSize);
        for (int i = 0; i < 2; i++)
        {
            encoded.put(FIRST_FRAGMENT);
            encoded.put((byte) SegmentType.COMMAND.getValue());
            encoded.putShort((short) frameSize);
            encoded.put((byte) 0);
            encoded.put((byte) 1);
            encoded.putShort((short) (i + 1));
            encoded.putInt(0);
            encoded.put((byte) 0);
        }

        final List<QpidByteBuffer> bodies = new ArrayList<>();
        try (final QpidByteBuffer backing = QpidByteBuffer.wrap(encoded.array()))
        {
            final QpidByteBuffer input = mock(QpidByteBuffer.class, delegatesTo(backing));
            doAnswer(invocation ->
            {
                if (backing.position() == frameSize)
                {
                    throw failure;
                }
                return backing.get();
            }).when(input).get();
            doAnswer(invocation ->
            {
                final QpidByteBuffer body = mock(QpidByteBuffer.class, delegatesTo(backing.slice()));
                bodies.add(body);
                return body;
            }).when(input).slice();

            try
            {
                assertSame(failure, assertThrows(failure.getClass(), () -> _handler.received(input)));
                assertEquals(1, bodies.size());
                verify(bodies.get(0)).dispose();
                verify(_assembler, never()).received(anyList());
            }
            finally
            {
                bodies.forEach(QpidByteBuffer::dispose);
            }
        }
    }

    private static Stream<Throwable> parsingFailures()
    {
        return Stream.of(new ArithmeticException("Injected parsing failure"),
                new RuntimeException("Injected parsing failure"), new InternalError("Injected JVM failure"));
    }
}
