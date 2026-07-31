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

package org.apache.qpid.server.protocol.v1_0.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;

class CountValidationListTest extends CountValidationTest
{
    @Test
    void list32NegativeCount()
    {
        // 0xD0 = list32, size=10, count=-1 (0xFFFFFFFF)
        final byte[] bytes =
        {
                (byte) 0xD0,
                0x00, 0x00, 0x00, 0x0A, // size = 10
                (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF // count = -1
        };
        assertDecodeError(bytes);
    }

    @Test
    void list32ExcessiveCount()
    {
        // 0xD0 = list32, size=4, count=200_000_000 (0x0BEBC200)
        final byte[] bytes =
        {
                (byte) 0xD0,
                0x00, 0x00, 0x00, 0x04, // size = 4
                0x0B, (byte) 0xEB, (byte) 0xC2, 0x00 // count = 200_000_000
        };
        assertDecodeError(bytes);
    }

    @Test
    void list32CountExceedsSize()
    {
        // count=100 but size=10 — each element needs at least 1 byte
        final byte[] bytes =
        {
                (byte) 0xD0,
                0x00, 0x00, 0x00, 0x0A, // size = 10
                0x00, 0x00, 0x00, 0x64 // count = 100
        };
        assertDecodeError(bytes);
    }

    @Test
    void list32NegativeSize()
    {
        // 0xD0 = list32, size=-1 (0xFFFFFFFF), count=1
        final byte[] bytes =
        {
                (byte) 0xD0,
                (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, // size = -1
                0x00, 0x00, 0x00, 0x01 // count = 1
        };
        assertDecodeError(bytes);
    }

    @Test
    void list32ValidEmptyList() throws AmqpErrorException
    {
        // 0xD0 = list32, size=4, count=0 — empty list is valid
        final byte[] bytes =
        {
                (byte) 0xD0,
                0x00, 0x00, 0x00, 0x04, // size = 4 (the count field itself)
                0x00, 0x00, 0x00, 0x00 // count = 0
        };
        final QpidByteBuffer qbb = QpidByteBuffer.wrap(bytes);
        final Object result = _valueHandler.parse(qbb);
        assertNotNull(result);
        assertEquals(0, ((List<?>) result).size());
    }

    @Test
    void list8ValidSingleElement() throws AmqpErrorException
    {
        // 0xC0 = list8, size=2, count=1, one null element (0x40)
        final byte[] bytes =
        {
                (byte) 0xC0,
                0x02, // size = 2
                0x01, // count = 1
                0x40 // null element
        };
        final QpidByteBuffer qbb = QpidByteBuffer.wrap(bytes);
        final Object result = _valueHandler.parse(qbb);
        assertNotNull(result);
        assertEquals(1, ((List<?>) result).size());
    }

    @Test
    void list8CountExceedsSize() throws AmqpErrorException
    {
        // 0xC0 = list8, size=2, count=1, one null element (0x40)
        final byte[] bytes =
        {
                (byte) 0xC0,
                0x02, // size = 2
                0x05, // count = 5
                0x40 // null element (insufficient for count)
        };
        assertDecodeError(bytes);
    }
}
