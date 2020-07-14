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
package org.apache.qpid.server.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class StringUtilTest extends UnitTestBase
{
    private StringUtil _util;

    @Before
    public void setUp() throws Exception
    {
        _util = new StringUtil();
    }

    @Test
    public void testRandomAlphaNumericStringInt()
    {
        String password = _util.randomAlphaNumericString(10);
        assertEquals("Unexpected password string length", (long) 10, (long) password.length());
        assertCharacters(password);
    }

    private void assertCharacters(String password)
    {
        String numbers = "0123456789";
        String letters = "abcdefghijklmnopqrstuvwxwy";
        String others = "_-";
        String expectedCharacters = (numbers + letters + letters.toUpperCase() + others);
        char[] chars = password.toCharArray();
        for (int i = 0; i < chars.length; i++)
        {
            char ch = chars[i];
            assertTrue("Unexpected character " + ch, expectedCharacters.indexOf(ch) != -1);
        }
    }

    @Test
    public void testCreateUniqueJavaName()
    {
        assertEquals("MyName_973de1b4e26b629d4817c8255090e58e", _util.createUniqueJavaName("MyName"));
        assertEquals("ContaisIllegalJavaCharacters_a68b2484f2eb790558d6527e56c595fa",
                            _util.createUniqueJavaName("Contais+Illegal-Java*Characters"));
        assertEquals("StartsWithIllegalInitial_93031eec569608c60c6a98ac9e84a0a7",
                            _util.createUniqueJavaName("9StartsWithIllegalInitial"));
        assertEquals("97b247ba19ff869340d3797cc73ca065", _util.createUniqueJavaName("1++++----"));
        assertEquals("d41d8cd98f00b204e9800998ecf8427e", _util.createUniqueJavaName(""));
    }

    @Test
    public void testTrimToEmpty() {
        assertEquals("Abc", StringUtil.trimToEmpty("Abc  "));
        assertEquals("", StringUtil.trimToEmpty(" "));
        assertEquals("", StringUtil.trimToEmpty(null));
    }

    @Test
    public void testIsEmpty() {
        assertTrue(StringUtil.isEmpty("  "));
        assertTrue(StringUtil.isEmpty(null));
        assertFalse(StringUtil.isEmpty("A "));
        assertFalse(StringUtil.isEmpty("B"));
    }
}
