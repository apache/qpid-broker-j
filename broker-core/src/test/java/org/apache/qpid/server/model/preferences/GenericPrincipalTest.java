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
 */

package org.apache.qpid.server.model.preferences;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class GenericPrincipalTest extends UnitTestBase
{
    private static final String UTF8 = StandardCharsets.UTF_8.name();
    private static final String USERNAME = "testuser";
    private static final String ORIGIN_TYPE = "authType";
    private static final String ORIGIN_NAME = "authName";

    @Test
    public void testParseSimple()
    {
        final GenericPrincipal p = new GenericPrincipal(String.format("%s@%s('%s')", USERNAME, ORIGIN_TYPE, ORIGIN_NAME));
        assertEquals(USERNAME, p.getName(), "unexpected principal name");
        assertEquals(ORIGIN_TYPE, p.getOriginType(), "unexpected origin type");
        assertEquals(ORIGIN_NAME, p.getOriginName(), "unexpected origin name");
    }

    @Test
    public void testNoOriginInfo()
    {
        assertThrows(IllegalArgumentException.class,
                () -> new GenericPrincipal("_usernameWithoutOriginInfo"),
                "GenericPrincipal should reject names without origin info");
    }

    @Test
    public void testParseWithDash()
    {
        final String username = "user-name";
        final String originType = "origin-type";
        final String originName = "origin-name";
        final GenericPrincipal p = new GenericPrincipal(String.format("%s@%s('%s')", username, originType, originName));
        assertEquals(username, p.getName(), "unexpected principal name");
        assertEquals(originType, p.getOriginType(), "unexpected origin type");
        assertEquals(originName, p.getOriginName(), "unexpected origin name");
    }

    @Test
    public void testRejectQuotes()
    {
        final String usernameWithQuote = "_username'withQuote";
        final String originTypeWithQuote = "authType'withQuote";
        final String originNameWithQuote = "authName'withQuote";
        assertThrows(IllegalArgumentException.class,
                () -> new GenericPrincipal(String.format("%s@%s('%s')", usernameWithQuote, ORIGIN_TYPE, ORIGIN_NAME)),
                "GenericPrincipal should reject _username with quotes");
        assertThrows(IllegalArgumentException.class,
                () -> new GenericPrincipal(String.format("%s@%s('%s')", USERNAME, originTypeWithQuote, ORIGIN_NAME)),
                "GenericPrincipal should reject origin type with quotes");
        assertThrows(IllegalArgumentException.class,
                () -> new GenericPrincipal(String.format("%s@%s('%s')", USERNAME, ORIGIN_TYPE, originNameWithQuote)),
                "GenericPrincipal should reject origin name with quotes");
    }

    @Test
    public void testRejectParenthesis()
    {
        final String usernameWithParenthesis = "username(withParenthesis";
        final String originTypeWithParenthesis = "authType(withParenthesis";
        final String originNameWithParenthesis = "authName(withParenthesis";
        assertThrows(IllegalArgumentException.class,
                () -> new GenericPrincipal(String.format("%s@%s('%s')", usernameWithParenthesis, ORIGIN_TYPE, ORIGIN_NAME)),
                "GenericPrincipal should reject _username with parenthesis");
        assertThrows(IllegalArgumentException.class,
                () -> new GenericPrincipal(String.format("%s@%s('%s')", USERNAME, originTypeWithParenthesis, ORIGIN_NAME)),
                "GenericPrincipal should reject origin type with parenthesis");
        assertThrows(IllegalArgumentException.class,
                () -> new GenericPrincipal(String.format("%s@%s('%s')", USERNAME, ORIGIN_TYPE, originNameWithParenthesis)),
                "GenericPrincipal should reject origin name with parenthesis");
    }

    @Test
    public void testRejectAtSign()
    {
        final String _usernameWithAtSign = "_username@withAtSign";
        final String originTypeWithAtSign = "authType@withAtSign";
        final String originNameWithAtSign = "authName@withAtSign";
        assertThrows(IllegalArgumentException.class,
                () -> new GenericPrincipal(String.format("%s@%s('%s')", _usernameWithAtSign, ORIGIN_TYPE, ORIGIN_NAME)),
                "GenericPrincipal should reject _usernames with @ sign");
        assertThrows(IllegalArgumentException.class,
                () -> new GenericPrincipal(String.format("%s@%s('%s')", USERNAME, originTypeWithAtSign, ORIGIN_NAME)),
                "GenericPrincipal should reject origin type with @ sign");
        assertThrows(IllegalArgumentException.class,
                () -> new GenericPrincipal(String.format("%s@%s('%s')", USERNAME, ORIGIN_TYPE, originNameWithAtSign)),
                "GenericPrincipal should reject origin name with @ sign");
    }

    @Test
    public void testUrlEncoded() throws Exception
    {
        final String username = "testuser@withFunky%";
        final String originName = "authName('also')with%funky@Characters'";
        final GenericPrincipal p = new GenericPrincipal(String.format("%s@%s('%s')", URLEncoder.encode(username, UTF8),
                ORIGIN_TYPE, URLEncoder.encode(originName, UTF8)));
        assertEquals(username, p.getName(), "unexpected principal name");
        assertEquals(ORIGIN_TYPE, p.getOriginType(), "unexpected origin type");
        assertEquals(originName, p.getOriginName(), "unexpected origin name");
    }
}
