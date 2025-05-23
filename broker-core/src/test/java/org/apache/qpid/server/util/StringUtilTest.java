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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class StringUtilTest extends UnitTestBase
{
    private StringUtil _util;

    @BeforeEach
    public void setUp() throws Exception
    {
        _util = new StringUtil();
    }

    @Test
    public void testRandomAlphaNumericStringInt()
    {
        final String password = _util.randomAlphaNumericString(10);
        assertEquals(10, (long) password.length(), "Unexpected password string length");
        assertCharacters(password);
    }

    private void assertCharacters(final String password)
    {
        final String numbers = "0123456789";
        final String letters = "abcdefghijklmnopqrstuvwxwy";
        final String others = "_-";
        final String expectedCharacters = (numbers + letters + letters.toUpperCase() + others);
        final char[] chars = password.toCharArray();
        for (final char ch : chars)
        {
            assertTrue(expectedCharacters.indexOf(ch) != -1, "Unexpected character " + ch);
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
    public void escapeHtml4()
    {
        assertNull(StringUtil.escapeHtml4(null));
        assertEquals("", StringUtil.escapeHtml4(""));
        assertEquals("test", StringUtil.escapeHtml4("test"));
        assertEquals("&lt;test&gt;", StringUtil.escapeHtml4("<test>"));
        assertEquals("&quot;test&quot;", StringUtil.escapeHtml4("\"test\""));
        assertEquals("&amp;", StringUtil.escapeHtml4("&"));
        assertEquals("&amp; &amp; &lt; &quot; &gt; &amp; &amp;", StringUtil.escapeHtml4("& & < \" > & &"));
        assertEquals("&gt;&gt;&gt;", StringUtil.escapeHtml4(">>>"));
        assertEquals("&lt;&lt;&lt;", StringUtil.escapeHtml4("<<<"));
        assertEquals("&#x27;&#x27;&#x27;", StringUtil.escapeHtml4("'''"));
    }

    @Test
    public void join()
    {
        assertEquals("", StringUtil.join((List) null, "", ""));
        assertEquals("", StringUtil.join(List.of(), "", ""));
        assertEquals("1,2,3", StringUtil.join(List.of("1", "2", "3"), ",", ""));
        assertEquals("1,nill,3", StringUtil.join(Stream.of("1", null, "3").collect(Collectors.toList()), ",", "nill"));
    }

    @Test
    public void joinMap()
    {
        assertEquals("", StringUtil.join((Map) null, "", ""));
        assertEquals("", StringUtil.join(Map.of(), "", ""));
        final Map<String, Integer> map = new LinkedHashMap<>();
        map.put("1", 1);
        map.put("2", 2);
        map.put("3", 3);
        assertEquals("1=1,2=2,3=3", StringUtil.join(map, ",", "="));
    }
}
