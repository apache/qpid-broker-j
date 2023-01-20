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

import static org.apache.qpid.server.model.preferences.PreferenceTestHelper.createPreferenceAttributes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.security.auth.TestPrincipalUtils;
import org.apache.qpid.test.utils.UnitTestBase;

public class PreferenceFactoryTest extends UnitTestBase
{
    private static final String TEST_USERNAME = "testUser";
    private static final String TEST_PRINCIPAL_SERIALIZATION =
            TestPrincipalUtils.getTestPrincipalSerialization(TEST_USERNAME);
    private ConfiguredObject<?> _testObject;

    @BeforeEach
    public void setUp() throws Exception
    {
        _testObject = mock(ConfiguredObject.class);
    }

    @Test
    public void testCreatePreferenceFromAttributes()
    {
        final Map<String, Object> prefValueMap = Map.of("myprefkey", "myprefvalue");
        final UUID preferenceId = randomUUID();
        final Preference p = PreferenceFactory.fromAttributes(_testObject, 
                createPreferenceAttributes(null, preferenceId, "X-PREF1", "myprefname",
                        "myprefdescription", TEST_PRINCIPAL_SERIALIZATION, Set.of(), prefValueMap));

        assertNotNull(p, "Creation failed");
        assertEquals(preferenceId, p.getId(), "Unexpected preference preferenceId");
        assertEquals("myprefname", p.getName(), "Unexpected preference name");
        assertEquals("myprefdescription", p.getDescription(), "Unexpected preference description");
        assertEquals(Set.of(), p.getVisibilityList(), "Unexpected preference visibility list");
        assertEquals(TEST_USERNAME, p.getOwner().getName(), "Unexpected preference owner");
        final PreferenceValue preferenceValue = p.getValue();
        assertNotNull(preferenceValue, "Preference value is null");
        assertEquals(prefValueMap, preferenceValue.getAttributes(), "Unexpected preference value");
    }
}
