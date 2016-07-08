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
 *
 */

package org.apache.qpid.server.model.testmodels.singleton;

import static org.mockito.Mockito.mock;

import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.security.auth.Subject;

import com.google.common.collect.Sets;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.preferences.Preference;
import org.apache.qpid.server.model.preferences.PreferenceFactory;
import org.apache.qpid.server.model.preferences.PreferenceTestHelper;
import org.apache.qpid.server.model.preferences.UserPreferencesImpl;
import org.apache.qpid.server.security.auth.TestPrincipalUtils;
import org.apache.qpid.server.store.preferences.PreferenceStore;
import org.apache.qpid.test.utils.QpidTestCase;

public class PreferencesTest extends QpidTestCase
{
    public static final String TEST_USERNAME = "testUser";
    public static final String TEST_USERNAME2 = "testUser2";
    private final Model _model = TestModel.getInstance();
    private ConfiguredObject<?> _testObject;
    private Subject _testSubject;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        final String objectName = getTestName();
        _testObject = _model.getObjectFactory()
                            .create(TestSingleton.class,
                                    Collections.<String, Object>singletonMap(ConfiguredObject.NAME, objectName));
        _testObject.setUserPreferences(new UserPreferencesImpl(
                mock(PreferenceStore.class), Collections.<Preference>emptySet()
        ));
        _testSubject = TestPrincipalUtils.createTestSubject(TEST_USERNAME);
    }

    public void testSimpleRoundTrip()
    {
        final Preference p = PreferenceFactory.recover(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                "X-TestPropType",
                "testProp1",
                null,
                TEST_USERNAME,
                null,
                Collections.<String, Object>emptyMap()));
        Subject.doAs(_testSubject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                Set<Preference> preferences = Collections.singleton(p);
                _testObject.getUserPreferences().updateOrAppend(preferences);
                assertEquals("roundtrip failed", preferences, _testObject.getUserPreferences().getPreferences());
                return null;
            }
        });
    }

    public void testOnlyAllowUpdateOwnedPreferences()
    {
        final Preference p = PreferenceFactory.recover(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                "X-testType",
                "prop1",
                null,
                TEST_USERNAME,
                null,
                Collections.<String, Object>emptyMap()));

        Subject.doAs(TestPrincipalUtils.createTestSubject(TEST_USERNAME2), new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                Set<Preference> preferences = Collections.singleton(p);
                try
                {
                    _testObject.getUserPreferences().updateOrAppend(preferences);
                    fail("Saving of preferences owned by somebody else should not be allowed");
                }
                catch (SecurityException e)
                {
                    // pass
                }
                return null;
            }
        });
    }

    public void testGetOnlyOwnedPreferences()
    {
        final Preference testUserPreference =
                PreferenceFactory.recover(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                        null,
                        null,
                        "X-testType",
                        "prop1",
                        null,
                        TEST_USERNAME,
                        null,
                        Collections.<String, Object>emptyMap()));

        updateOrAppendAs(_testSubject, testUserPreference);

        final Preference testUser2Preference =
                PreferenceFactory.recover(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                        null,
                        null,
                        "X-testType",
                        "prop2",
                        null,
                        TEST_USERNAME2,
                        null,
                        Collections.<String, Object>emptyMap()));

        Subject.doAs(TestPrincipalUtils.createTestSubject(TEST_USERNAME2), new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                Set<Preference> preferences = Collections.singleton(testUser2Preference);
                _testObject.getUserPreferences().updateOrAppend(Collections.singleton(testUser2Preference));
                Set<Preference> p2s = _testObject.getUserPreferences().getPreferences();
                assertEquals("Unexpected preferences for subject 2", preferences, p2s);
                return null;
            }
        });

        assertSinglePreference(_testSubject, testUserPreference);
    }

    public void testUpdate()
    {
        final Preference p1 = PreferenceFactory.recover(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                "X-testType",
                "propName",
                null,
                TEST_USERNAME,
                null,
                Collections.<String, Object>emptyMap()));

        final Preference p2 = PreferenceFactory.recover(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                p1.getId(),
                "X-testType",
                "newPropName",
                "newDescription",
                TEST_USERNAME, null,
                Collections.<String, Object>emptyMap()));

        Subject.doAs(_testSubject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                _testObject.getUserPreferences().updateOrAppend(Collections.singleton(p1));
                _testObject.getUserPreferences().updateOrAppend(Collections.singleton(p2));

                Set<Preference> preferences = _testObject.getUserPreferences().getPreferences();
                assertEquals("Unexpected number of preferences", 1, preferences.size());
                Preference newPreference = preferences.iterator().next();
                assertEquals("Unexpected preference id", p2.getId(), newPreference.getId());
                assertEquals("Unexpected preference type", p2.getType(), newPreference.getType());
                assertEquals("Unexpected preference name", p2.getName(), newPreference.getName());
                assertEquals("Unexpected preference description", p2.getDescription(), newPreference.getDescription());
                return null;
            }
        });
    }

    public void testProhibitTypeChange()
    {
        final Preference p1 = PreferenceFactory.recover(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                "X-testType",
                "propName",
                null,
                TEST_USERNAME,
                null,
                Collections.<String, Object>emptyMap()));
        final Preference p2 = PreferenceFactory.recover(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                p1.getId(),
                "X-differentTestType",
                "propName",
                null,
                TEST_USERNAME,
                null,
                Collections.<String, Object>emptyMap()));
        Subject.doAs(_testSubject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                _testObject.getUserPreferences().updateOrAppend(Collections.singleton(p1));
                try
                {
                    _testObject.getUserPreferences().updateOrAppend(Collections.singleton(p2));
                    fail("Type change should not be allowed");
                }
                catch (IllegalArgumentException e)
                {
                    // pass
                }
                return null;
            }
        });
    }

    public void testProhibitDuplicateNamesOfSameType()
    {
        final Preference p1 = PreferenceFactory.recover(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                "X-testType",
                "propName",
                null,
                TEST_USERNAME,
                null,
                Collections.<String, Object>emptyMap()));
        final Preference p2 = PreferenceFactory.recover(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                "X-testType",
                "propName",
                null,
                TEST_USERNAME,
                null,
                Collections.<String, Object>emptyMap()));

        Subject.doAs(_testSubject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                _testObject.getUserPreferences().updateOrAppend(Collections.singleton(p1));

                try
                {
                    _testObject.getUserPreferences().updateOrAppend(Collections.singleton(p2));
                    fail("Property with same name and same type should not be allowed");
                }
                catch (IllegalArgumentException e)
                {
                    // pass
                }
                return null;
            }
        });
    }

    public void testProhibitDuplicateNamesOfSameTypeInSameUpdate()
    {
        final Preference p1 = PreferenceFactory.recover(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                "X-testType",
                "propName",
                null,
                TEST_USERNAME,
                null,
                Collections.<String, Object>emptyMap()));
        final Preference p2 = PreferenceFactory.recover(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                "X-testType",
                "propName",
                null,
                TEST_USERNAME,
                null,
                Collections.<String, Object>emptyMap()));
        Subject.doAs(_testSubject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                Set<Preference> preferences = new HashSet<>();
                preferences.add(p1);
                preferences.add(p2);
                try
                {
                    _testObject.getUserPreferences().updateOrAppend(preferences);
                    fail("Property with same name and same type should not be allowed");
                }
                catch (IllegalArgumentException e)
                {
                    // pass
                }
                return null;
            }
        });
    }

    public void testReplace()
    {
        final String preferenceType = "X-testType";
        Subject testSubject2 = TestPrincipalUtils.createTestSubject(TEST_USERNAME2);

        final Preference unaffectedPreference =
                PreferenceFactory.recover(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                        null,
                        null,
                        preferenceType,
                        "propName",
                        null,
                        TEST_USERNAME2,
                        null,
                        Collections.<String, Object>emptyMap()));
        updateOrAppendAs(testSubject2, unaffectedPreference);

        final Preference p1 = PreferenceFactory.recover(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                preferenceType,
                "propName",
                null,
                TEST_USERNAME,
                null,
                Collections.<String, Object>emptyMap()));
        final Preference p2 = PreferenceFactory.recover(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                preferenceType,
                "newPropName",
                null,
                TEST_USERNAME,
                null,
                Collections.<String, Object>emptyMap()));

        Subject.doAs(_testSubject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                _testObject.getUserPreferences().updateOrAppend(Collections.singleton(p1));
                _testObject.getUserPreferences().replace(Collections.singleton(p2));

                Collection<Preference> retrievedPreferences = _testObject.getUserPreferences().getPreferences();
                assertEquals("Unexpected number of preferences", 1, retrievedPreferences.size());
                assertEquals("Unexpected preference", p2, retrievedPreferences.iterator().next());

                return null;
            }
        });

        assertSinglePreference(testSubject2, unaffectedPreference);
    }

    public void testDelete()
    {
        final String preferenceType = "X-testType";
        Subject testSubject2 = TestPrincipalUtils.createTestSubject(TEST_USERNAME2);
        final Preference unaffectedPreference =
                PreferenceFactory.recover(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                        null,
                        null,
                        preferenceType,
                        "propName",
                        null,
                        TEST_USERNAME2,
                        null,
                        Collections.<String, Object>emptyMap()));
        updateOrAppendAs(testSubject2, unaffectedPreference);

        final Preference p1 = PreferenceFactory.recover(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                preferenceType,
                "propName",
                null,
                TEST_USERNAME,
                null,
                Collections.<String, Object>emptyMap()));

        Subject.doAs(_testSubject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                _testObject.getUserPreferences().updateOrAppend(Collections.singleton(p1));
                _testObject.getUserPreferences().replace(Collections.<Preference>emptySet());

                Collection<Preference> retrievedPreferences = _testObject.getUserPreferences().getPreferences();
                assertEquals("Unexpected number of preferences", 0, retrievedPreferences.size());

                return null;
            }
        });

        assertSinglePreference(testSubject2, unaffectedPreference);
    }

    public void testDeleteByType()
    {
        final String preferenceType = "X-testType";
        final String unaffectedPreferenceType = "X-unaffectedType";
        Subject testSubject2 = TestPrincipalUtils.createTestSubject(TEST_USERNAME2);

        final Preference unaffectedPreference =
                PreferenceFactory.recover(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                        null,
                        null,
                        preferenceType,
                        "propName",
                        null,
                        TEST_USERNAME2,
                        null,
                        Collections.<String, Object>emptyMap()));
        updateOrAppendAs(testSubject2, unaffectedPreference);

        final Preference p1 = PreferenceFactory.recover(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                preferenceType,
                "propName",
                null,
                TEST_USERNAME,
                null,
                Collections.<String, Object>emptyMap()));
        final Preference p2 = PreferenceFactory.recover(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                unaffectedPreferenceType,
                "propName",
                null,
                TEST_USERNAME,
                null,
                Collections.<String, Object>emptyMap()));

        Subject.doAs(_testSubject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                Set<Preference> preferences = new HashSet<>();
                preferences.add(p1);
                preferences.add(p2);
                _testObject.getUserPreferences().updateOrAppend(preferences);

                _testObject.getUserPreferences().replaceByType(preferenceType, Collections.<Preference>emptySet());

                Collection<Preference> retrievedPreferences = _testObject.getUserPreferences().getPreferences();
                assertEquals("Unexpected number of preferences", 1, retrievedPreferences.size());
                assertTrue("Unexpected preference", retrievedPreferences.contains(p2));
                return null;
            }
        });

        assertSinglePreference(testSubject2, unaffectedPreference);
    }

    public void testDeleteByTypeAndName()
    {
        final String preferenceType = "X-testType";
        Subject testSubject2 = TestPrincipalUtils.createTestSubject(TEST_USERNAME2);

        final Preference unaffectedPreference =
                PreferenceFactory.recover(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                        null,
                        null,
                        preferenceType,
                        "propName",
                        null,
                        TEST_USERNAME2,
                        null,
                        Collections.<String, Object>emptyMap()));

        updateOrAppendAs(testSubject2, unaffectedPreference);

        final Preference p1 = PreferenceFactory.recover(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                preferenceType,
                "propName",
                null,
                TEST_USERNAME,
                null,
                Collections.<String, Object>emptyMap()));

        final Preference p2 = PreferenceFactory.recover(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                preferenceType,
                "unaffectedPropName",
                null,
                TEST_USERNAME,
                null,
                Collections.<String, Object>emptyMap()));

        Subject.doAs(_testSubject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                Set<Preference> preferences = new HashSet<>();
                preferences.add(p1);
                preferences.add(p2);
                _testObject.getUserPreferences().updateOrAppend(preferences);

                _testObject.getUserPreferences().replaceByTypeAndName(preferenceType, "propName", null);

                Collection<Preference> retrievedPreferences = _testObject.getUserPreferences().getPreferences();
                assertEquals("Unexpected number of preferences", 1, retrievedPreferences.size());
                assertTrue("Unexpected preference", retrievedPreferences.contains(p2));

                return null;
            }
        });

        assertSinglePreference(testSubject2, unaffectedPreference);
    }

    public void testReplaceByType()
    {
        final String replaceType = "X-replaceType";
        final String unaffectedType = "X-unaffectedType";
        Subject testSubject2 = TestPrincipalUtils.createTestSubject(TEST_USERNAME2);
        final Preference unaffectedPreference =
                PreferenceFactory.recover(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                        null,
                        null,
                        replaceType,
                        "propName",
                        null,
                        TEST_USERNAME2,
                        null,
                        Collections.<String, Object>emptyMap()));

        updateOrAppendAs(testSubject2, unaffectedPreference);

        final Preference p1 = PreferenceFactory.recover(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                replaceType,
                "propName",
                null,
                TEST_USERNAME,
                null,
                Collections.<String, Object>emptyMap()));
        final Preference p2 = PreferenceFactory.recover(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                unaffectedType,
                "propName",
                null,
                TEST_USERNAME,
                null,
                Collections.<String, Object>emptyMap()));
        final Preference p3 = PreferenceFactory.recover(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                replaceType,
                "newPropName",
                null,
                TEST_USERNAME,
                null,
                Collections.<String, Object>emptyMap()));
        Subject.doAs(_testSubject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                Set<Preference> preferences = new HashSet<>();
                preferences.add(p1);
                preferences.add(p2);
                _testObject.getUserPreferences().updateOrAppend(preferences);

                preferences = Collections.singleton(p3);
                _testObject.getUserPreferences().replaceByType(replaceType, preferences);

                Set<Preference> retrievedPreferences = _testObject.getUserPreferences().getPreferences();
                assertEquals("Unexpected number of preferences", 2, retrievedPreferences.size());
                assertTrue("Preference of different type was replaced", retrievedPreferences.contains(p2));
                assertTrue("Preference was not replaced", retrievedPreferences.contains(p3));
                return null;
            }
        });

        assertSinglePreference(testSubject2, unaffectedPreference);
    }

    public void testReplaceByTypeAndName()
    {
        final String replaceType = "X-replaceType";
        final String unaffectedType = "X-unaffectedType";
        Subject testSubject2 = TestPrincipalUtils.createTestSubject(TEST_USERNAME2);
        final Preference unaffectedPreference =
                PreferenceFactory.recover(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                        null,
                        null,
                        replaceType,
                        "propName",
                        null,
                        TEST_USERNAME2,
                        null,
                        Collections.<String, Object>emptyMap()));

        updateOrAppendAs(testSubject2, unaffectedPreference);

        final Preference p1 = PreferenceFactory.recover(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                replaceType,
                "propName",
                null,
                TEST_USERNAME,
                null,
                Collections.<String, Object>emptyMap()));
        final Preference p1b = PreferenceFactory.recover(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                replaceType,
                "unaffectedPropName",
                null,
                TEST_USERNAME,
                null,
                Collections.<String, Object>emptyMap()));

        final Preference p2 = PreferenceFactory.recover(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                unaffectedType,
                "propName",
                null,
                TEST_USERNAME,
                null,
                Collections.<String, Object>emptyMap()));

        final Preference p3 = PreferenceFactory.recover(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                replaceType,
                "propName",
                "new description",
                TEST_USERNAME,
                null,
                Collections.<String, Object>emptyMap()));

        Subject.doAs(_testSubject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                Set<Preference> preferences = new HashSet<>();
                preferences.add(p1);
                preferences.add(p1b);
                preferences.add(p2);
                _testObject.getUserPreferences().updateOrAppend(preferences);

                _testObject.getUserPreferences().replaceByTypeAndName(replaceType, "propName", p3);

                Set<Preference> retrievedPreferences = _testObject.getUserPreferences().getPreferences();
                assertEquals("Unexpected number of preferences", 3, retrievedPreferences.size());
                assertTrue("Preference of different name was replaced", retrievedPreferences.contains(p1b));
                assertTrue("Preference of different type was replaced", retrievedPreferences.contains(p2));
                assertTrue("Preference was not replaced", retrievedPreferences.contains(p3));
                return null;
            }
        });

        assertSinglePreference(testSubject2, unaffectedPreference);
    }

    public void testGetVisiblePreferences()
    {
        final Principal testPrincipal = _testSubject.getPrincipals().iterator().next();

        Subject peerSubject = TestPrincipalUtils.createTestSubject("peer");
        final Preference sharedPreference = PreferenceFactory.recover(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                "X-testType",
                "propName1",
                "shared with colleague testUser",
                "peer",
                Collections.singleton(testPrincipal.toString()),
                Collections.<String, Object>emptyMap()));

        Subject.doAs(peerSubject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                _testObject.getUserPreferences().updateOrAppend(Sets.newHashSet(sharedPreference));
                return null;
            }
        });

        Subject anotherSubject = TestPrincipalUtils.createTestSubject("anotherUser");
        final Preference notSharedPreference = PreferenceFactory.recover(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                "X-testType",
                "propName2",
                null,
                "anotherUser",
                null,
                Collections.<String, Object>emptyMap()));

        Subject.doAs(anotherSubject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                _testObject.getUserPreferences().updateOrAppend(Sets.newHashSet(notSharedPreference));
                return null;
            }
        });

        final Preference p = PreferenceFactory.recover(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                "X-testType",
                "propName",
                null,
                TEST_USERNAME,
                null,
                Collections.<String, Object>emptyMap()));

        Subject.doAs(_testSubject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {

                _testObject.getUserPreferences().updateOrAppend(Sets.newHashSet(p));

                Set<Preference> retrievedPreferences = _testObject.getUserPreferences().getVisiblePreferences();
                assertEquals("Unexpected number of preferences", 2, retrievedPreferences.size());
                assertTrue("Preference of my peer did not exist in visible set",
                           retrievedPreferences.contains(sharedPreference));
                assertTrue("My preference did not exist in visible set", retrievedPreferences.contains(p));
                assertFalse("Preference of the other user unexpectedly exists in visible set",
                            retrievedPreferences.contains(notSharedPreference));
                return null;
            }
        });
    }

    public void testGetVisiblePreferencesSharedByGroup()
    {
        final String testGroupName = "testGroup";
        Subject testSubjectWithGroup = TestPrincipalUtils.createTestSubject(TEST_USERNAME, testGroupName);

        final Preference sharedPreference =
                PreferenceFactory.recover(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                        null,
                        null,
                        "X-testType",
                        "propName1",
                        null,
                        "peer",
                        Collections.singleton(testGroupName),
                        Collections.<String, Object>emptyMap()));

        Subject peerSubject = TestPrincipalUtils.createTestSubject("peer");
        Subject.doAs(peerSubject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                _testObject.getUserPreferences().updateOrAppend(Sets.newHashSet(sharedPreference));
                return null;
            }
        });

        final Preference testUserPreference =
                PreferenceFactory.recover(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                        null,
                        null,
                        "X-testType",
                        "propName",
                        null,
                        TEST_USERNAME,
                        null,
                        Collections.<String, Object>emptyMap()));

        Subject.doAs(testSubjectWithGroup, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                _testObject.getUserPreferences().updateOrAppend(Sets.newHashSet(testUserPreference));

                Set<Preference> retrievedPreferences = _testObject.getUserPreferences().getVisiblePreferences();
                assertEquals("Unexpected number of preferences", 2, retrievedPreferences.size());
                assertTrue("Preference of my peer did not exist in visible set",
                           retrievedPreferences.contains(sharedPreference));
                assertTrue("My preference did not exist in visible set",
                           retrievedPreferences.contains(testUserPreference));
                return null;
            }
        });
    }

    public void testLastUpdatedDate() throws Exception
    {
        Date before = new Date();
        Thread.sleep(1);
        final Preference p1 = PreferenceFactory.recover(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                "X-testType",
                "propName1",
                null,
                TEST_USERNAME,
                null,
                Collections.<String, Object>emptyMap()));
        Thread.sleep(1);
        Date after = new Date();
        Date lastUpdatedDate = p1.getLastUpdatedDate();
        assertTrue(String.format("Creation date is too early. Expected : after %s  Found : %s",
                                 before,
                                 lastUpdatedDate),
                   before.before(lastUpdatedDate));
        assertTrue(String.format("Creation date is too late. Expected : after %s  Found : %s", after, lastUpdatedDate),
                   after.after(lastUpdatedDate));
    }

    public void testLastUpdatedDateIsImmutable() throws Exception
    {
        final Preference p1 = PreferenceFactory.recover(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                "X-testType",
                "propName1",
                null,
                TEST_USERNAME,
                null,
                Collections.<String, Object>emptyMap()));
        Date lastUpdatedDate = p1.getLastUpdatedDate();
        lastUpdatedDate.setTime(0);
        Date lastUpdatedDate2 = p1.getLastUpdatedDate();
        assertTrue("Creation date is not immutable.", lastUpdatedDate2.getTime() != 0);
    }

    public void testGetAttributes() throws Exception
    {
        final Map<String, Object> prefValueMap = Collections.<String, Object>singletonMap("myprefkey", "myprefvalue");
        final UUID uuid = UUID.randomUUID();
        final String type = "X-PREF1";
        final String name = "myprefname";
        final String description = "myprefdescription";
        final Set<String> visibilitySet = Collections.emptySet();
        Preference p = PreferenceFactory.recover(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                uuid,
                type,
                name,
                description,
                TEST_USERNAME,
                visibilitySet,
                prefValueMap));
        assertNotNull("Creation failed", p);
        Date lastUpdatedDate = p.getLastUpdatedDate();

        Map<String, Object> expectedAttributes = new HashMap<>();
        expectedAttributes.put("id", uuid);
        expectedAttributes.put("type", type);
        expectedAttributes.put("name", name);
        expectedAttributes.put("description", description);
        expectedAttributes.put("owner", TEST_USERNAME);
        expectedAttributes.put("associatedObject", _testObject.getId());
        expectedAttributes.put("visibilityList", visibilitySet);
        expectedAttributes.put("lastUpdatedDate", lastUpdatedDate);
        expectedAttributes.put("value", prefValueMap);
        assertEquals("Unexpected preference attributes", expectedAttributes, p.getAttributes());
    }

    private void updateOrAppendAs(final Subject testSubject, final Preference testUserPreference)
    {
        Subject.doAs(testSubject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                _testObject.getUserPreferences().updateOrAppend(Collections.singleton(testUserPreference));
                return null;
            }
        });
    }

    private void assertSinglePreference(final Subject subject, final Preference preference)
    {
        Subject.doAs(subject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                Collection<Preference> retrievedPreferences = _testObject.getUserPreferences().getPreferences();
                assertEquals("Unexpected number of preferences", 1, retrievedPreferences.size());
                assertEquals("Unexpected preference", preference, retrievedPreferences.iterator().next());
                return null;
            }
        });
    }
}
