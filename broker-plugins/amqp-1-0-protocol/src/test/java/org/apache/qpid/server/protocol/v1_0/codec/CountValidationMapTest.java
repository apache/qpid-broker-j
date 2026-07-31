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

import java.util.Map;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;

class CountValidationMapTest extends CountValidationTest
{
    @Test
    void map32NegativeCount()
    {
        // 0xD1 = map32, size=10, count=-1
        final byte[] bytes =
        {
                (byte) 0xD1,
                0x00, 0x00, 0x00, 0x0A,
                (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF
        };
        assertDecodeError(bytes);
    }

    @Test
    void map32ExcessiveCount()
    {
        // 0xD1 = map32, size=4, count=200_000_000
        final byte[] bytes =
        {
                (byte) 0xD1,
                0x00, 0x00, 0x00, 0x04,
                0x0B, (byte) 0xEB, (byte) 0xC2, 0x00
        };
        assertDecodeError(bytes);
    }

    @Test
    void map32CountExceedsSize()
    {
        // count=100 but size=10
        final byte[] bytes =
        {
                (byte) 0xD1,
                0x00, 0x00, 0x00, 0x0A,
                0x00, 0x00, 0x00, 0x64
        };
        assertDecodeError(bytes);
    }

    @Test
    void map32NegativeSize()
    {
        final byte[] bytes =
        {
                (byte) 0xD1,
                (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                0x00, 0x00, 0x00, 0x02
        };
        assertDecodeError(bytes);
    }

    @Test
    void map8ValidSinglePair() throws AmqpErrorException
    {
        // 0xC1 = map8, size=6, count=2 (1 key-value pair), key=null, value=null
        final byte[] bytes =
        {
                (byte) 0xC1,
                0x03, // size = 3
                0x02, // count = 2 (key + value)
                0x40, // null key
                0x40 // null value
        };
        final QpidByteBuffer qbb = QpidByteBuffer.wrap(bytes);
        final Object result = _valueHandler.parse(qbb);
        assertNotNull(result);
        assertEquals(1, ((Map<?, ?>) result).size());
    }

    @Test
    void map8CountExceedsSize() throws AmqpErrorException
    {
        // 0xC1 = map8, size=3, count=8 - count > size
        final byte[] bytes =
        {
                (byte) 0xC1,
                0x03,
                0x08,
                0x40,
                0x40
        };
        assertDecodeError(bytes);
    }
}
