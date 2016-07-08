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
import static org.mockito.Mockito.mock;

import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import javax.security.auth.Subject;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;
import org.apache.qpid.server.security.auth.TestPrincipalUtils;
import org.apache.qpid.test.utils.QpidTestCase;

public class PreferenceFactoryTest extends QpidTestCase
{
    public static final String TEST_USERNAME = "testUser";
    private ConfiguredObject<?> _testObject;
    private Subject _testSubject;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        _testObject = mock(ConfiguredObject.class);
        _testSubject = TestPrincipalUtils.createTestSubject(TEST_USERNAME);
    }

    public void testCreatePreference()
    {
        final Map<String, Object> prefValueMap = Collections.<String, Object>singletonMap("myprefkey", "myprefvalue");
        Preference p = Subject.doAs(_testSubject, new PrivilegedAction<Preference>()
        {
            @Override
            public Preference run()
            {
                return PreferenceFactory.create(_testObject, createPreferenceAttributes(null,
                                                                                        null,
                                                                                        "X-PREF1",
                                                                                        "myprefname",
                                                                                        "myprefdescription",
                                                                                        null,
                                                                                        Collections.<String>emptySet(),
                                                                                        prefValueMap));
            }
        });

        assertNotNull("Creation failed", p);
        assertEquals("Unexpected preference name", "myprefname", p.getName());
        assertEquals("Unexpected preference description", "myprefdescription", p.getDescription());
        assertEquals("Unexpected preference visibility list", Collections.emptySet(), p.getVisibilityList());
        assertNotNull("Preference creation date must not be null", p.getLastUpdatedDate());
        final PreferenceValue preferenceValue = p.getValue();
        assertNotNull("Preference value is null", preferenceValue);
        assertEquals("Unexpected preference value", prefValueMap, preferenceValue.getAttributes());
    }

    public void testRecoverPreference()
    {
        final Map<String, Object> prefValueMap = Collections.<String, Object>singletonMap("myprefkey", "myprefvalue");
        Preference p = PreferenceFactory.recover(_testObject, createPreferenceAttributes(null,
                                                                                         null,
                                                                                         "X-PREF1",
                                                                                         "myprefname",
                                                                                         "myprefdescription",
                                                                                         TEST_USERNAME,
                                                                                         Collections.<String>emptySet(),
                                                                                         prefValueMap));
        assertNotNull("Creation failed", p);
        assertEquals("Unexpected preference name", "myprefname", p.getName());
        assertEquals("Unexpected preference description", "myprefdescription", p.getDescription());
        assertEquals("Unexpected preference visibility list", Collections.emptySet(), p.getVisibilityList());
        assertNotNull("Preference creation date must not be null", p.getLastUpdatedDate());
        final PreferenceValue preferenceValue = p.getValue();
        assertNotNull("Preference value is null", preferenceValue);
        assertEquals("Unexpected preference value", prefValueMap, preferenceValue.getAttributes());
    }

    public void testPreferenceNameIsMandatory()
    {
        final Map<String, Object> prefValueMap = Collections.emptyMap();
        try
        {
            PreferenceFactory.recover(_testObject, createPreferenceAttributes(null,
                                                                              null,
                                                                              "X-PREF1",
                                                                              null,
                                                                              "myprefdescription",
                                                                              TEST_USERNAME,
                                                                              Collections.<String>emptySet(),
                                                                              prefValueMap));
            fail("Preference name must not be null");
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }
        try
        {
            PreferenceFactory.recover(_testObject, createPreferenceAttributes(null,
                                                                              null,
                                                                              "X-PREF1",
                                                                              "",
                                                                              "myprefdescription",
                                                                              TEST_USERNAME,
                                                                              Collections.<String>emptySet(),
                                                                              prefValueMap));
            fail("Preference name must not be empty");
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }
    }

    public void testPreferenceHasUuid()
    {
        Preference p1 = PreferenceFactory.recover(_testObject, createPreferenceAttributes(null,
                                                                                          null,
                                                                                          "X-TESTPREF",
                                                                                          "testProp1",
                                                                                          "",
                                                                                          TEST_USERNAME,
                                                                                          null,
                                                                                          Collections.<String, Object>emptyMap()));
        Preference p2 = PreferenceFactory.recover(_testObject, createPreferenceAttributes(null,
                                                                                          null,
                                                                                          "X-TESTPREF",
                                                                                          "testProp2",
                                                                                          "",
                                                                                          TEST_USERNAME,
                                                                                          Collections.<String>emptySet(),
                                                                                          Collections.<String, Object>emptyMap()));
        UUID id1 = p1.getId();
        UUID id2 = p2.getId();
        assertNotNull("preference id must not be null", id1);
        assertNotNull("preference id must not be null", id2);
        assertTrue("preference ids must be unique", !id1.equals(id2));
    }

    public void testPreferenceOwner()
    {
        Preference p = PreferenceFactory.recover(_testObject, createPreferenceAttributes(null,
                                                                                         null,
                                                                                         "X-TESTPREF",
                                                                                         "testProp1",
                                                                                         null,
                                                                                         TEST_USERNAME,
                                                                                         null,
                                                                                         Collections.<String, Object>emptyMap()));
        final Principal testPrincipal = _testSubject.getPrincipals(AuthenticatedPrincipal.class).iterator().next();
        assertEquals("Unexpected preference owner", testPrincipal.getName(), p.getOwner().getName());
    }

    public void testPreferenceOwnerIsMandatoryOnRecovery()
    {
        try
        {
            PreferenceFactory.recover(_testObject, createPreferenceAttributes(null,
                                                                              null,
                                                                              "X-TESTPREF",
                                                                              "testProp1",
                                                                              null,
                                                                              null,
                                                                              null,
                                                                              Collections.<String, Object>emptyMap()));
            fail("Recovery should fail if owner is missed");
        }
        catch (IllegalArgumentException e)
        {
            //pass
        }
    }

    public void testAssociatedObject()
    {
        Preference p = PreferenceFactory.recover(_testObject, createPreferenceAttributes(null,
                                                                                         null,
                                                                                         "X-TESTPREF",
                                                                                         "testProp1",
                                                                                         null,
                                                                                         TEST_USERNAME,
                                                                                         null,
                                                                                         Collections.<String, Object>emptyMap()));
        assertEquals("Unexpected associated object", _testObject, p.getAssociatedObject());
    }

    public void testType()
    {
        final String type = "X-TESTPREF";
        Preference p = PreferenceFactory.recover(_testObject, createPreferenceAttributes(null,
                                                                                         null,
                                                                                         type,
                                                                                         "testProp1",
                                                                                         null,
                                                                                         TEST_USERNAME,
                                                                                         null,
                                                                                         Collections.<String, Object>emptyMap()));
        assertEquals("Unexpected type", type, p.getType());
    }

    public void testLastUpdatedDateOnRecovery()
    {
        Map<String, Object> attributes = createPreferenceAttributes(null,
                                                                    null,
                                                                    "X-TESTPREF",
                                                                    "testProp1",
                                                                    null,
                                                                    TEST_USERNAME,
                                                                    null,
                                                                    Collections.<String, Object>emptyMap());
        attributes.put(Preference.LAST_UPDATED_DATE_ATTRIBUTE, 1);
        Preference p = PreferenceFactory.recover(_testObject, attributes);

        assertEquals("Unexpected last updated date", 1, p.getLastUpdatedDate().getTime());
    }

    public void testLastUpdatedDateOnCreate() throws InterruptedException
    {
        long beforeTime = System.currentTimeMillis();
        Thread.sleep(1);
        Preference p = Subject.doAs(_testSubject, new PrivilegedAction<Preference>()
        {
            @Override
            public Preference run()
            {
                return PreferenceFactory.create(_testObject, createPreferenceAttributes(null,
                                                                                        null,
                                                                                        "X-PREF1",
                                                                                        "myprefname",
                                                                                        "myprefdescription",
                                                                                        null,
                                                                                        Collections.<String>emptySet(),
                                                                                        Collections.<String, Object>emptyMap()));
            }
        });
        Thread.sleep(1);
        long afterTime = System.currentTimeMillis();
        long updateTime = p.getLastUpdatedDate().getTime();

        assertTrue(String.format("Update time is not greater then the time before creation: %d vs %d",
                                 updateTime,
                                 beforeTime), updateTime > beforeTime);
        assertTrue(String.format("Update time is not less then the time after creation: %d vs %d",
                                 updateTime,
                                 afterTime), updateTime < afterTime);
    }
}
