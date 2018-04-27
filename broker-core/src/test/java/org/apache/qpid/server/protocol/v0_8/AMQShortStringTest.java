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

import java.nio.charset.StandardCharsets;

import com.google.common.cache.CacheBuilder;

import org.apache.qpid.test.utils.QpidTestCase;

public class AMQShortStringTest extends QpidTestCase
{

    private static final AMQShortString GOODBYE = AMQShortString.createAMQShortString("Goodbye");

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
    public void testCreateAMQShortStringByteArray()
    {
        byte[] bytes = "test".getBytes(StandardCharsets.UTF_8);
        AMQShortString string = AMQShortString.createAMQShortString(bytes);
        assertEquals("constructed amq short string length differs from expected",
                            (long) 4,
                            (long) string.length());

        assertTrue("constructed amq short string differs from expected", string.toString().equals("test"));
    }

    /**
     * Tests short string construction from string with length less than 255.
     */
    public void testCreateAMQShortStringString()
    {
        AMQShortString string = AMQShortString.createAMQShortString("test");
        assertEquals("constructed amq short string length differs from expected", (long) 4, (long) string.length
                ());

        assertTrue("constructed amq short string differs from expected", string.toString().equals("test"));
    }

    /**
     * Test method for
     * {@link AMQShortString#AMQShortString(byte[])}.
     * <p>
     * Tests an attempt to create an AMQP short string from byte array with length over 255.
     */
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
            assertEquals("Exception message differs from expected",
                    "Cannot create AMQShortString with number of octets over 255!", e.getMessage());
        }
    }

    /**
     * Tests an attempt to create an AMQP short string from string with length over 255
     */
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
            assertEquals("Exception message differs from expected",
                    "Cannot create AMQShortString with number of octets over 255!", e.getMessage());
        }
    }

    public void testValueOf()
    {
        String string = buildString('a', 255);
        AMQShortString shortString = AMQShortString.valueOf(string, true, true);
        assertEquals("Unexpected string from valueOf", string, shortString.toString());
    }

    public void testValueOfTruncated()
    {
        String string = buildString('a', 256);
        AMQShortString shortString = AMQShortString.valueOf(string, true, true);
        assertEquals("Unexpected truncated string from valueOf", string.substring(0, AMQShortString.MAX_LENGTH -3) + "...",
                     shortString.toString());
    }

    public void testValueOfNulAsEmptyString()
    {
        AMQShortString shortString = AMQShortString.valueOf(null, true, true);
        assertEquals("Unexpected empty string from valueOf", AMQShortString.EMPTY_STRING, shortString);
    }

    public void testValueOfNullAsNull()
    {
        AMQShortString shortString = AMQShortString.valueOf(null, true, false);
        assertEquals("Unexpected null string from valueOf", null, shortString);
    }

    public void testInterning()
    {
        AMQShortString.setCache(CacheBuilder.newBuilder().maximumSize(1).build());

        try
        {
            AMQShortString str1 = AMQShortString.createAMQShortString("hello");
            str1.intern();
            AMQShortString str2 = AMQShortString.createAMQShortString("hello");
            AMQShortString str3 = AMQShortString.createAMQShortString("hello".getBytes(StandardCharsets.UTF_8));

            assertEquals(str1, str2);
            assertEquals(str1, str3);
            assertSame(str1, str2);
            assertSame(str1, str3);
        }
        finally
        {
            AMQShortString.setCache(null);
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
