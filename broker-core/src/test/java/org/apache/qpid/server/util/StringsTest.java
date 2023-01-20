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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class StringsTest extends UnitTestBase
{
    @Test
    public void testSubstitutionResolver()
    {
        final Strings.MapResolver mapResolver = new Strings.MapResolver(Map.of("test", "C:\\TEMP\\\"Hello World\""));
        final LinkedHashMap<String, String> map = new LinkedHashMap<>();
        map.put("\\", "\\\\");
        map.put("\"", "\\\"");
        final Strings.Resolver jsonResolver = Strings.createSubstitutionResolver("json:", map);
        assertEquals("{ \"path\" : \"C:\\\\TEMP\\\\\\\"Hello World\\\"\\foo\" }",
                Strings.expand("{ \"path\" : \"${json:test}\\foo\" }", Strings.chain(jsonResolver, mapResolver)));
    }

    @Test
    public void testNestedSubstitutionResolver()
    {
        final Map<String,String> context = Map.of("test", "C:\\TEMP\\\"Hello World\"",
                "nestedTest", "${test}");
        final Strings.MapResolver mapResolver = new Strings.MapResolver(context);
        final LinkedHashMap<String, String> map = new LinkedHashMap<>();
        map.put("\\", "\\\\");
        map.put("\"", "\\\"");
        final Strings.Resolver jsonResolver = Strings.createSubstitutionResolver("json:", map);
        assertEquals("{ \"path\" : \"C:\\\\TEMP\\\\\\\"Hello World\\\"\\foo\" }",
                Strings.expand("{ \"path\" : \"${json:nestedTest}\\foo\" }", Strings.chain(jsonResolver, mapResolver)));
    }

    @Test
    public void hexDumpSingleByte()
    {
        // Known good created with echo -n A | od -Ax -tx1 -v
        final String expected = String.format("0000000    41%n" + "0000001%n");
        final String actual = Strings.hexDump(ByteBuffer.wrap("A".getBytes()));
        assertThat(actual, is(equalTo(expected)));
    }

    @Test
    public void hexDumpManyBytes()
    {
        // Known good created with echo -n 12345678123456789 | od -Ax -tx1 -v
        final String expected = String.format("0000000    31  32  33  34  35  36  37  38  31  32  33  34  35  36  37  38%n" +
                "0000010    39%n" + "0000011%n");
        final String actual = Strings.hexDump(ByteBuffer.wrap("12345678123456789".getBytes()));
        assertThat(actual, is(equalTo(expected)));
    }

    @Test
    public void toCharSequence()
    {
        CharSequence expected = "null";
        CharSequence actual = Strings.toCharSequence(null).toString();
        assertThat(expected, is(equalTo(actual)));

        expected = "";
        actual = Strings.toCharSequence("").toString();
        assertThat(expected, is(equalTo(actual)));

        final Object object = new Object();
        expected = object.toString();
        actual = Strings.toCharSequence(object).toString();
        assertThat(expected, is(equalTo(actual)));
    }

    @Test
    public void decodeCharArray()
    {
        assertThat(null, is(equalTo(Strings.decodeCharArray(null, null))));
        assertThat(new byte[]{}, is(equalTo(Strings.decodeCharArray(new char[]{}, null))));
        assertThat(new byte[]{}, is(equalTo(Strings.decodeCharArray("".toCharArray(), null))));
        final char[] base64 = Base64.getEncoder().encodeToString("test".getBytes(StandardCharsets.UTF_8)).toCharArray();
        assertThat(new byte[]{116, 101, 115, 116}, is(equalTo(Strings.decodeCharArray(base64, null))));
    }

    @Test
    public void split()
    {
        assertThat(List.of(), is(equalTo(Strings.split(null))));
        assertThat(List.of(), is(equalTo(Strings.split(""))));
        assertThat(Collections.singletonList("a"), is(equalTo(Strings.split("a"))));
        assertThat(Collections.singletonList("a "), is(equalTo(Strings.split("a "))));
        assertThat(Arrays.asList("a", "b"), is(equalTo(Strings.split("a,b"))));
    }

    @Test
    public void join()
    {
        NullPointerException thrown = assertThrows(NullPointerException.class,
                () -> Strings.join(",", (Object[]) null));
        assertThat("Items must be not null", is(equalTo(thrown.getMessage())));

        thrown = assertThrows(NullPointerException.class,
                () -> Strings.join(",", (Iterable<?>) null));
        assertThat("Items must be not null", is(equalTo(thrown.getMessage())));

        thrown = assertThrows(NullPointerException.class,
                () -> Strings.join(null, (Iterable<?>) null));
        assertThat("Separator must be not null", is(equalTo(thrown.getMessage())));

        assertThat("", is(equalTo(Strings.join(",", new Object[]{}))));
        assertThat("", is(equalTo(Strings.join(",", new ArrayList<>()))));

        assertThat("a,b,c", is(equalTo(Strings.join(",", new Object[]{"a", "b", "c"}))));
        assertThat("a,b,c", is(equalTo(Strings.join(",", Arrays.asList("a", "b", "c")))));

        assertThat("a,null,1", is(equalTo(Strings.join(",", new Object[]{"a", null, 1}))));
        assertThat("a,null,1", is(equalTo(Strings.join(",", Arrays.asList("a", null, 1)))));
    }
}
