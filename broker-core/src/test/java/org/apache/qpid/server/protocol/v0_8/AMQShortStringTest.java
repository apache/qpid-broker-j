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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.charset.StandardCharsets;

import org.junit.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class AMQShortStringTest extends UnitTestBase
{

    public static final AMQShortString HELLO = new AMQShortString("Hello");
    public static final AMQShortString HELL = new AMQShortString("Hell");
    public static final AMQShortString GOODBYE = new AMQShortString("Goodbye");
    public static final AMQShortString GOOD = new AMQShortString("Good");
    public static final AMQShortString BYE = new AMQShortString("BYE");


    @Test
    public void testEquals()
    {
        assertEquals(GOODBYE, new AMQShortString("Goodbye"));
        assertEquals(new AMQShortString("A"), new AMQShortString("A"));
        assertFalse(new AMQShortString("A").equals(new AMQShortString("a")));
    }

    /**
     * Test method for
     * {@link AMQShortString#AMQShortString(byte[])}.
     */
    @Test
    public void testCreateAMQShortStringByteArray()
    {
        byte[] bytes = "test".getBytes(StandardCharsets.UTF_8);
        AMQShortString string = new AMQShortString(bytes);
        assertEquals("constructed amq short string length differs from expected",
                            (long) 4,
                            (long) string.length());


        assertTrue("constructed amq short string differs from expected", string.toString().equals("test"));
    }

    /**
     * Test method for
     * {@link AMQShortString#AMQShortString(java.lang.String)}
     * <p>
     * Tests short string construction from string with length less than 255.
     */
    @Test
    public void testCreateAMQShortStringString()
    {
        AMQShortString string = new AMQShortString("test");
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
    @Test
    public void testCreateAMQShortStringByteArrayOver255()
    {
        String test = buildString('a', 256);
        byte[] bytes = test.getBytes(StandardCharsets.UTF_8);
        try
        {
            new AMQShortString(bytes);
            fail("It should not be possible to create AMQShortString with length over 255");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Exception message differs from expected",
                                "Cannot create AMQShortString with number of octets over 255!",
                                e.getMessage());

        }
    }

    /**
     * Test method for
     * {@link AMQShortString#AMQShortString(java.lang.String)}
     * <p>
     * Tests an attempt to create an AMQP short string from string with length over 255
     */
    @Test
    public void testCreateAMQShortStringStringOver255()
    {
        String test = buildString('a', 256);
        try
        {
            new AMQShortString(test);
            fail("It should not be possible to create AMQShortString with length over 255");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Exception message differs from expected",
                                "Cannot create AMQShortString with number of octets over 255!",
                                e.getMessage());
        }
    }

    @Test
    public void testValueOf()
    {
        String string = buildString('a', 255);
        AMQShortString shortString = AMQShortString.valueOf(string, true, true);
        assertEquals("Unexpected string from valueOf", string, shortString.toString());
    }

    @Test
    public void testValueOfTruncated()
    {
        String string = buildString('a', 256);
        AMQShortString shortString = AMQShortString.valueOf(string, true, true);
        assertEquals("Unexpected truncated string from valueOf",
                            string.substring(0, AMQShortString.MAX_LENGTH - 3) + "...",
                            shortString.toString());
    }

    @Test
    public void testValueOfNulAsEmptyString()
    {
        AMQShortString shortString = AMQShortString.valueOf(null, true, true);
        assertEquals("Unexpected empty string from valueOf", AMQShortString.EMPTY_STRING, shortString);
    }

    @Test
    public void testValueOfNullAsNull()
    {
        AMQShortString shortString = AMQShortString.valueOf(null, true, false);
        assertEquals("Unexpected null string from valueOf", null, shortString);
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
