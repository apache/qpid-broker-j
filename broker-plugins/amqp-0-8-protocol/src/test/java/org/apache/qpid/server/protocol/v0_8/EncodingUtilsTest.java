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
 *
 */

package org.apache.qpid.server.protocol.v0_8;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.test.utils.UnitTestBase;

class EncodingUtilsTest extends UnitTestBase
{
    private static final int BUFFER_SIZE = 10;
    private static final int POOL_SIZE = 20;
    private static final double SPARSITY_FRACTION = 1.0;

    private QpidByteBuffer _buffer;

    @BeforeEach
    void setUp() throws Exception
    {
        QpidByteBuffer.deinitialisePool();
        QpidByteBuffer.initialisePool(BUFFER_SIZE, POOL_SIZE, SPARSITY_FRACTION);
        _buffer = QpidByteBuffer.allocateDirect(BUFFER_SIZE);
    }

    @AfterEach
    void tearDown()
    {
        _buffer.dispose();
        QpidByteBuffer.deinitialisePool();
    }

    @Test
    void readLongAsShortStringWhenDigitsAreSpecified() throws Exception
    {
        _buffer.putUnsignedByte((short)3);
        _buffer.put((byte)'9');
        _buffer.put((byte)'2');
        _buffer.put((byte)'0');
        _buffer.flip();
        assertEquals(920L, EncodingUtils.readLongAsShortString(_buffer), "Unexpected result");
    }

    @Test
    void readLongAsShortStringWhenNonDigitCharacterIsSpecified()
    {
        _buffer.putUnsignedByte((short)2);
        _buffer.put((byte)'1');
        _buffer.put((byte)'a');
        _buffer.flip();
        assertThrows(AMQFrameDecodingException.class, () -> EncodingUtils.readLongAsShortString(_buffer),
                "Exception is expected");
    }

    @Test
    void readLongStringReadsValidValue()
    {
        _buffer.putInt(2);
        _buffer.put((byte) 'h');
        _buffer.put((byte) 'i');
        _buffer.flip();
        assertEquals("hi", EncodingUtils.readLongString(_buffer), "Unexpected result");
    }

    @Test
    void readLongStringRejectsUnsignedLengthLargerThanRemaining()
    {
        _buffer.putInt(0x80000000);
        _buffer.put((byte) 'x');
        _buffer.flip();
        assertThrows(IllegalArgumentException.class, () -> EncodingUtils.readLongString(_buffer));
    }

    @Test
    void readLongStringRejectsOversizedLength()
    {
        _buffer.putInt(0x20000000);
        _buffer.put((byte) 'x');
        _buffer.flip();
        assertThrows(IllegalArgumentException.class, () -> EncodingUtils.readLongString(_buffer));
    }

    @Test
    void readLongstrRejectsUnsignedLengthLargerThanRemaining()
    {
        _buffer.putInt(0x80000000);
        _buffer.put((byte) 1);
        _buffer.flip();
        assertThrows(IllegalArgumentException.class, () -> EncodingUtils.readLongstr(_buffer));
    }

    @Test
    void readBytesRejectsOversizedLength()
    {
        _buffer.putInt(0x20000000);
        _buffer.put((byte) 1);
        _buffer.flip();
        assertThrows(IllegalArgumentException.class, () -> EncodingUtils.readBytes(_buffer));
    }

    @Test
    void fieldArrayRejectsUnsignedLengthLargerThanRemaining()
    {
        _buffer.putInt(0x80000000);
        _buffer.put((byte) 1);
        _buffer.flip();

        assertThrows(IllegalArgumentException.class, () -> FieldArray.readFromBuffer(_buffer));
    }

    @Test
    void readFieldTableRejectsOversizedLengthBeforeCreatingView()
    {
        final QpidByteBuffer input = mock(QpidByteBuffer.class);
        when(input.getUnsignedInt()).thenReturn(1L);
        when(input.remaining()).thenReturn(0);

        assertThrows(IllegalArgumentException.class, () -> EncodingUtils.readFieldTable(input));

        verify(input, never()).view(anyInt(), anyInt());
        verify(input, never()).position(anyInt());
    }

    @Test
    void fieldTableWithOversizedLongStringValueIsRejected()
    {
        _buffer.putUnsignedByte((short) 1);              // key length
        _buffer.put((byte) 's');                         // key "s"
        _buffer.put(AMQType.LONG_STRING.identifier());   // value type 'S'
        _buffer.putInt(0x80000000);                      // long-string length
        _buffer.flip();
        final int length = _buffer.remaining();
        final FieldTable fieldTable = FieldTableFactory.createFieldTable(_buffer, length);
        try
        {
            assertThrows(IllegalArgumentException.class, fieldTable::validate);
        }
        finally
        {
            fieldTable.dispose();
        }
    }

    @Test
    void decodedFieldTableDetachesNestedTableFromPooledBuffer()
    {
        final long baseline = QpidByteBuffer.getAllocatedDirectMemorySize();
        QpidByteBuffer input = QpidByteBuffer.allocateDirect(60);
        FieldTable encodedTable = null;
        FieldTable decodedTable = null;
        FieldTable nestedTable = null;
        try
        {
            input.putInt(14);                                  // outer field-table length
            input.putUnsignedByte((short) 1);
            input.put((byte) 'n');
            input.put(AMQType.FIELD_TABLE.identifier());
            input.putInt(7);                                   // nested field-table length
            input.putUnsignedByte((short) 1);
            input.put((byte) 'x');
            input.put(AMQType.INT.identifier());
            input.putInt(42);
            input.flip();

            encodedTable = EncodingUtils.readFieldTable(input);
            decodedTable = FieldTable.convertToDecodedFieldTable(encodedTable);
            nestedTable = (FieldTable) decodedTable.get("n");

            encodedTable.dispose();
            encodedTable = null;
            input.dispose();
            input = null;

            assertEquals(baseline, QpidByteBuffer.getAllocatedDirectMemorySize(),
                    "Decoded nested field table retained a pooled direct buffer");
            assertEquals(42, nestedTable.get("x"));
        }
        finally
        {
            if (nestedTable != null)
            {
                nestedTable.dispose();
            }
            if (decodedTable != null)
            {
                decodedTable.dispose();
            }
            if (encodedTable != null)
            {
                encodedTable.dispose();
            }
            if (input != null)
            {
                input.dispose();
            }
        }
    }

    @Test
    void malformedFieldArrayAfterNestedTableDoesNotLeakPooledBuffer()
    {
        final long baseline = QpidByteBuffer.getAllocatedDirectMemorySize();
        QpidByteBuffer input = QpidByteBuffer.allocateDirect(60);
        try
        {
            input.putInt(20);                                  // outer field-table length
            input.putUnsignedByte((short) 1);
            input.put((byte) 'a');
            input.put(AMQType.FIELD_ARRAY.identifier());
            input.putInt(13);                                  // field-array payload length
            input.put(AMQType.FIELD_TABLE.identifier());
            input.putInt(3);                                   // nested field-table length
            input.putUnsignedByte((short) 1);
            input.put((byte) 'x');
            input.put(AMQType.VOID.identifier());
            input.put(AMQType.LONG_STRING.identifier());
            input.putInt(0x80000000);
            input.flip();

            final QpidByteBuffer tableToDecode = input;
            assertThrows(IllegalArgumentException.class, () -> EncodingUtils.readFieldTable(tableToDecode));
            input.dispose();
            input = null;

            assertEquals(baseline, QpidByteBuffer.getAllocatedDirectMemorySize(),
                    "Malformed field array leaked a nested field-table view");
        }
        finally
        {
            if (input != null)
            {
                input.dispose();
            }
        }
    }
}
