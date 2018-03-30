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
package org.apache.qpid.server.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.Collections;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import org.apache.qpid.server.security.auth.UsernamePrincipal;
import org.apache.qpid.test.utils.UnitTestBase;

public class ConfiguredObjectJacksonModuleTest extends UnitTestBase
{
    private static final String UTF8 = StandardCharsets.UTF_8.name();

    @Test
    public void testPrincipalSerialisation() throws Exception
    {
        final String username = "testuser@withFunky%";
        final String originType = "authType";
        final String originName = "authName('also')with%funky@Characters'";
        final String expectedSerialisation = String.format("\"%s@%s('%s')\"",
                                                           URLEncoder.encode(username, UTF8),
                                                           originType,
                                                           URLEncoder.encode(originName, UTF8));
        AuthenticationProvider<?> authProvider = mock(AuthenticationProvider.class);
        when(authProvider.getType()).thenReturn(originType);
        when(authProvider.getName()).thenReturn(originName);
        Principal p = new UsernamePrincipal(username, authProvider);
        ObjectMapper mapper = ConfiguredObjectJacksonModule.newObjectMapper(false);
        String json = mapper.writeValueAsString(p);
        assertEquals("unexpected principal serialisation", expectedSerialisation, json);
    }

    @Test
    public void testManageableAttributeType() throws IOException
    {
        ManagedAttributeValue testType = new TestManagedAttributeValue();

        ObjectMapper encoder = ConfiguredObjectJacksonModule.newObjectMapper(false);
        String encodedValue = encoder.writeValueAsString(testType);
        ObjectMapper decoder = new ObjectMapper();
        Map decodedMap = decoder.readValue(encodedValue, Map.class);
        assertEquals((long) 3, (long) decodedMap.size());
        assertTrue(decodedMap.containsKey("name"));
        assertTrue(decodedMap.containsKey("map"));
        assertTrue(decodedMap.containsKey("type"));
        assertEquals("foo", decodedMap.get("name"));
        assertEquals(Collections.singletonMap("key", "value"), decodedMap.get("map"));
        assertEquals(Collections.singletonMap("nested", true), decodedMap.get("type"));
    }

    @ManagedAttributeValueType(isAbstract = true)
    private static class TestManagedAttributeValue implements ManagedAttributeValue
    {
        public String getName()
        {
            return "foo";
        }

        public Map<String,String> getMap()
        {
            return Collections.singletonMap("key", "value");
        }

        public NestedManagedAttributeValue getType()
        {
            return new NestedManagedAttributeValue();
        }


    }

    @ManagedAttributeValueType(isAbstract = true)
    private static class NestedManagedAttributeValue implements ManagedAttributeValue
    {
        public boolean isNested()
        {
            return true;
        }
    }
}
