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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.test.utils.UnitTestBase;

public class AMQShortStringTest extends UnitTestBase
{
    private static final AMQShortString GOODBYE = AMQShortString.createAMQShortString("Goodbye");

    @Test
    public void testEquals()
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
    public void testCreateAMQShortStringByteArray()
    {
        byte[] bytes = "test".getBytes(StandardCharsets.UTF_8);
        AMQShortString string = AMQShortString.createAMQShortString(bytes);
        assertEquals(4, (long) string.length(), "constructed amq short string length differs from expected");

        assertEquals("test", string.toString(), "constructed amq short string differs from expected");
    }

    /**
     * Tests short string construction from string with length less than 255.
     */
    @Test
    public void testCreateAMQShortStringString()
    {
        AMQShortString string = AMQShortString.createAMQShortString("test");
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
    public void testCreateAMQShortStringByteArrayOver255()
    {
        String test = buildString('a', 256);
        byte[] bytes = test.getBytes(StandardCharsets.UTF_8);
        try
        {
            AMQShortString.createAMQShortString(bytes);
            fail("It should not be possible to create AMQShortString with length over 255");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Cannot create AMQShortString with number of octets over 255!", e.getMessage(),
                    "Exception message differs from expected");

        }
    }

    /**
     * Tests an attempt to create an AMQP short string from string with length over 255
     */
    @Test
    public void testCreateAMQShortStringStringOver255()
    {
        String test = buildString('a', 256);
        try
        {
            AMQShortString.createAMQShortString(test);
            fail("It should not be possible to create AMQShortString with length over 255");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Cannot create AMQShortString with number of octets over 255!", e.getMessage(),
                    "Exception message differs from expected");
        }
    }

    @Test
    public void testValueOf()
    {
        String string = buildString('a', 255);
        AMQShortString shortString = AMQShortString.valueOf(string, true, true);
        assertEquals(string, shortString.toString(), "Unexpected string from valueOf");
    }

    @Test
    public void testValueOfTruncated()
    {
        String string = buildString('a', 256);
        AMQShortString shortString = AMQShortString.valueOf(string, true, true);
        assertEquals(string.substring(0, AMQShortString.MAX_LENGTH - 3) + "...", shortString.toString(),
                "Unexpected truncated string from valueOf");
    }

    @Test
    public void testValueOfNulAsEmptyString()
    {
        AMQShortString shortString = AMQShortString.valueOf(null, true, true);
        assertEquals(AMQShortString.EMPTY_STRING, shortString, "Unexpected empty string from valueOf");
    }

    @Test
    public void testValueOfNullAsNull()
    {
        AMQShortString shortString = AMQShortString.valueOf(null, true, false);
        assertEquals(null, shortString, "Unexpected null string from valueOf");
    }

    @Test
    public void testCaching()
    {
        Cache<ByteBuffer, AMQShortString> original = AMQShortString.getShortStringCache();
        Cache<ByteBuffer, AMQShortString> cache = CacheBuilder.newBuilder().maximumSize(1).build();
        AMQShortString.setShortStringCache(cache);
        try
        {
            AMQShortString string = AMQShortString.createAMQShortString("hello");
            QpidByteBuffer qpidByteBuffer = QpidByteBuffer.allocate(2 * (string.length() + 1));
            string.writeToBuffer(qpidByteBuffer);
            string.writeToBuffer(qpidByteBuffer);

            qpidByteBuffer.flip();

            AMQShortString str1 = AMQShortString.readAMQShortString(qpidByteBuffer);
            AMQShortString str2 = AMQShortString.readAMQShortString(qpidByteBuffer);

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
     * @param ch
     *            char to build string with
     * @param length
     *            target string length
     * @return string
     */
    private String buildString(char ch, int length)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++)
        {
            sb.append(ch);
        }
        return sb.toString();
    }

}
