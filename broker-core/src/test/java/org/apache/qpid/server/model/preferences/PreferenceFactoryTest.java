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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.security.auth.TestPrincipalUtils;
import org.apache.qpid.test.utils.UnitTestBase;

public class PreferenceFactoryTest extends UnitTestBase
{
    private static final String TEST_USERNAME = "testUser";
    private static final String TEST_PRINCIPAL_SERIALIZATION =
            TestPrincipalUtils.getTestPrincipalSerialization(TEST_USERNAME);
    private ConfiguredObject<?> _testObject;

    @Before
    public void setUp() throws Exception
    {
        _testObject = mock(ConfiguredObject.class);
    }

    @Test
    public void testCreatePreferenceFromAttributes()
    {
        final Map<String, Object> prefValueMap = Collections.<String, Object>singletonMap("myprefkey", "myprefvalue");
        final UUID preferenceId = UUID.randomUUID();
        Preference p = PreferenceFactory.fromAttributes(_testObject, createPreferenceAttributes(null,
                                                                                                preferenceId,
                                                                                                "X-PREF1",
                                                                                                "myprefname",
                                                                                                "myprefdescription",
                                                                                                TEST_PRINCIPAL_SERIALIZATION,
                                                                                                Collections.<String>emptySet(),
                                                                                                prefValueMap));

        assertNotNull("Creation failed", p);
        assertEquals("Unexpected preference preferenceId", preferenceId, p.getId());
        assertEquals("Unexpected preference name", "myprefname", p.getName());
        assertEquals("Unexpected preference description", "myprefdescription", p.getDescription());
        assertEquals("Unexpected preference visibility list", Collections.emptySet(), p.getVisibilityList());
        assertEquals("Unexpected preference owner", TEST_USERNAME, p.getOwner().getName());
        final PreferenceValue preferenceValue = p.getValue();
        assertNotNull("Preference value is null", preferenceValue);
        assertEquals("Unexpected preference value", prefValueMap, preferenceValue.getAttributes());
    }
}
