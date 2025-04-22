/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */

package org.apache.qpid.server.protocol.v0_8;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import com.github.benmanes.caffeine.cache.Cache;

import com.github.benmanes.caffeine.cache.Caffeine;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.test.utils.UnitTestBase;

class AMQShortStringTest extends UnitTestBase
{
    private static final AMQShortString GOODBYE = AMQShortString.createAMQShortString("Goodbye");

    @Test
    void testEquals()
    {
        assertEquals(GOODBYE, AMQShortString.createAMQShortString("Goodbye"));
        assertEquals(AMQShortString.createAMQShortString("A"), AMQShortString.createAMQShortString("A"));
        assertFalse(AMQShortString.createAMQShortString("A").equals(AMQShortString.createAMQShortString("a")));
    }

    /**
     * Test method for
     * {@link AMQShortString#AMQShortString(byte[])}.
     */
    @Test
    void createAMQShortStringByteArray()
    {
        final byte[] bytes = "test".getBytes(StandardCharsets.UTF_8);
        final AMQShortString string = AMQShortString.createAMQShortString(bytes);
        assertEquals(4, (long) string.length(), "constructed amq short string length differs from expected");
        assertEquals("test", string.toString(), "constructed amq short string differs from expected");
    }

    /**
     * Tests short string construction from string with length less than 255.
     */
    @Test
    void createAMQShortStringString()
    {
        final AMQShortString string = AMQShortString.createAMQShortString("test");
        assertEquals(4, (long) string.length(), "constructed amq short string length differs from expected");
        assertEquals("test", string.toString(), "constructed amq short string differs from expected");
    }

    /**
     * Test method for
     * {@link AMQShortString#AMQShortString(byte[])}.
     * <p>
     * Tests an attempt to create an AMQP short string from byte array with length over 255.
     */
    @Test
    void createAMQShortStringByteArrayOver255()
    {
        final String test = buildString(256);
        final byte[] bytes = test.getBytes(StandardCharsets.UTF_8);
        final IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> AMQShortString.createAMQShortString(bytes),
                "It should not be possible to create AMQShortString with length over 255");
        assertEquals("Cannot create AMQShortString with number of octets over 255!", thrown.getMessage(),
                "Exception message differs from expected");
    }

    /**
     * Tests an attempt to create an AMQP short string from string with length over 255
     */
    @Test
    void createAMQShortStringStringOver255()
    {
        final String test = buildString(256);
        final IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> AMQShortString.createAMQShortString(test),
                "It should not be possible to create AMQShortString with length over 255");
        assertEquals("Cannot create AMQShortString with number of octets over 255!", thrown.getMessage(),
                "Exception message differs from expected");
    }

    @Test
    void valueOf()
    {
        final String string = buildString(255);
        final AMQShortString shortString = AMQShortString.valueOf(string, true, true);
        assertEquals(string, shortString.toString(), "Unexpected string from valueOf");
    }

    @Test
    void valueOfTruncated()
    {
        final String string = buildString(256);
        final AMQShortString shortString = AMQShortString.valueOf(string, true, true);
        assertEquals(string.substring(0, AMQShortString.MAX_LENGTH - 3) + "...", shortString.toString(),
                "Unexpected truncated string from valueOf");
    }

    @Test
    void valueOfNulAsEmptyString()
    {
        final AMQShortString shortString = AMQShortString.valueOf(null, true, true);
        assertEquals(AMQShortString.EMPTY_STRING, shortString, "Unexpected empty string from valueOf");
    }

    @Test
    void valueOfNullAsNull()
    {
        final AMQShortString shortString = AMQShortString.valueOf(null, true, false);
        assertNull(shortString, "Unexpected null string from valueOf");
    }

    @Test
    void caching()
    {
        final Cache<ByteBuffer, AMQShortString> original = AMQShortString.getShortStringCache();
        final Cache<ByteBuffer, AMQShortString> cache = Caffeine.newBuilder().maximumSize(1).build();
        AMQShortString.setShortStringCache(cache);
        try
        {
            final AMQShortString string = AMQShortString.createAMQShortString("hello");
            final QpidByteBuffer qpidByteBuffer = QpidByteBuffer.allocate(2 * (string.length() + 1));
            string.writeToBuffer(qpidByteBuffer);
            string.writeToBuffer(qpidByteBuffer);

            qpidByteBuffer.flip();

            final AMQShortString str1 = AMQShortString.readAMQShortString(qpidByteBuffer);
            final AMQShortString str2 = AMQShortString.readAMQShortString(qpidByteBuffer);

            assertEquals(str1, str2);
            assertSame(str1, str2);
        }
        finally
        {
            cache.cleanUp();
            AMQShortString.setShortStringCache(original);
        }
    }

    /**
     * A helper method to generate a string with given length containing given
     * character
     *
     * @param length target string length
     * @return string
     */
    private String buildString(int length)
    {
        return String.valueOf('a').repeat(Math.max(0, length));
    }
}
