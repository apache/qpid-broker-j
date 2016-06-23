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

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.preferences.Preference;
import org.apache.qpid.server.model.preferences.PreferenceValue;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;
import org.apache.qpid.server.security.auth.TestPrincipalUtils;
import org.apache.qpid.server.security.group.GroupPrincipal;
import org.apache.qpid.test.utils.QpidTestCase;

public class PreferencesTest extends QpidTestCase
{
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
        _testSubject = TestPrincipalUtils.createTestSubject("testUser");
    }

    public void testCreatePreference()
    {
        final Map<String, Object> prefValueMap = Collections.<String, Object>singletonMap("myprefkey", "myprefvalue");
        Preference p = _testObject.getUserPreferences()
                                  .createPreference(null,
                                                    "X-PREF1",
                                                    "myprefname",
                                                    "myprefdescription",
                                                    Collections.<Principal>emptySet(), prefValueMap);
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
            _testObject.getUserPreferences().createPreference(null,
                                                              "X-PREF1",
                                                              null,
                                                              "myprefdescription",
                                                              Collections.<Principal>emptySet(), prefValueMap);
            fail("Preference name must not be null");
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }
        try
        {
            _testObject.getUserPreferences().createPreference(null,
                                                              "X-PREF1",
                                                              "",
                                                              "myprefdescription",
                                                              Collections.<Principal>emptySet(), prefValueMap);
            fail("Preference name must not be empty");
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }
    }

    public void testPreferenceHasUuid()
    {
        Preference p1 = _testObject.getUserPreferences().createPreference(null,
                                                                          "X-TESTPREF",
                                                                          "testProp1",
                                                                          "",
                                                                          null, Collections.<String, Object>emptyMap());
        Preference p2 = _testObject.getUserPreferences().createPreference(null,
                                                                          "X-TESTPREF",
                                                                          "testProp2",
                                                                          "",
                                                                          Collections.<Principal>emptySet(),
                                                                          Collections.<String, Object>emptyMap());
        UUID id1 = p1.getId();
        UUID id2 = p2.getId();
        assertNotNull("preference id must not be null", id1);
        assertNotNull("preference id must not be null", id2);
        assertTrue("preference ids must be unique", !id1.equals(id2));
    }

    public void testPreferenceOwner()
    {
        Preference p = Subject.doAs(_testSubject, new PrivilegedAction<Preference>()
        {
            @Override
            public Preference run()
            {
                return _testObject.getUserPreferences().createPreference(null,
                                                                         "X-TESTPREF",
                                                                         "testProp1",
                                                                         null,
                                                                         null, Collections.<String, Object>emptyMap());
            }
        });
        final Principal testPrincipal = _testSubject.getPrincipals(AuthenticatedPrincipal.class).iterator().next();
        assertEquals("Unexpected preference owner", testPrincipal, p.getOwner());
    }

    public void testAssociatedObject()
    {
        Preference p = _testObject.getUserPreferences().createPreference(null,
                                                                         "X-TESTPREF",
                                                                         "testProp1",
                                                                         null,
                                                                         null, Collections.<String, Object>emptyMap());
        assertEquals("Unexpected associated object", _testObject, p.getAssociatedObject());
    }

    public void testType()
    {
        final String type = "X-TESTPREF";
        Preference p = _testObject.getUserPreferences().createPreference(null,
                                                                         type,
                                                                         "testProp1",
                                                                         null,
                                                                         null, Collections.<String, Object>emptyMap());
        assertEquals("Unexpected type", type, p.getType());
    }

    public void testSimpleRoundTrip()
    {
        Subject.doAs(_testSubject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                Preference p = _testObject.getUserPreferences().createPreference(null,
                                                                                 "X-TestPropType",
                                                                                 "testProp1",
                                                                                 null,
                                                                                 null,
                                                                                 Collections.<String, Object>emptyMap());
                Set<Preference> preferences = Collections.singleton(p);
                _testObject.getUserPreferences().updateOrAppend(preferences);
                assertEquals("roundtrip failed", preferences, _testObject.getUserPreferences().getPreferences());
                return null;
            }
        });
    }

    public void testOnlyAllowUpdateOwnedPreferences()
    {
        final Preference p = Subject.doAs(_testSubject, new PrivilegedAction<Preference>()
        {
            @Override
            public Preference run()
            {
                return _testObject.getUserPreferences().createPreference(null,
                                                                         "X-testType",
                                                                         "prop1",
                                                                         null,
                                                                         null,
                                                                         Collections.<String, Object>emptyMap());
            }
        });

        Subject testSubject2 = TestPrincipalUtils.createTestSubject("testUser2");
        Subject.doAs(testSubject2, new PrivilegedAction<Void>()
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
        final Set<Preference> p1s = Subject.doAs(_testSubject, new PrivilegedAction<Set<Preference>>()
        {
            @Override
            public Set<Preference> run()
            {
                Preference p = _testObject.getUserPreferences().createPreference(null,
                                                                                 "X-testType",
                                                                                 "prop1",
                                                                                 null,
                                                                                 null,
                                                                                 Collections.<String, Object>emptyMap());
                Set<Preference> preferences = Collections.singleton(p);
                _testObject.getUserPreferences().updateOrAppend(preferences);
                return _testObject.getUserPreferences().getPreferences();
            }
        });

        Subject testSubject2 = TestPrincipalUtils.createTestSubject("testUser2");
        Subject.doAs(testSubject2, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                Preference p = _testObject.getUserPreferences().createPreference(null,
                                                                                 "X-testType",
                                                                                 "prop2",
                                                                                 null,
                                                                                 null,
                                                                                 Collections.<String, Object>emptyMap());
                Set<Preference> preferences = Collections.singleton(p);
                _testObject.getUserPreferences().updateOrAppend(preferences);
                Set<Preference> p2s = _testObject.getUserPreferences().getPreferences();
                assertEquals("Unexpected preferences for subject 2", preferences, p2s);
                return null;
            }
        });

        Subject.doAs(_testSubject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                Set<Preference> preferences = _testObject.getUserPreferences().getPreferences();
                assertEquals("Unexpected preferences for subject 1", p1s, preferences);
                return null;
            }
        });
    }

    public void testUpdate()
    {
        Subject.doAs(_testSubject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                Set<Preference> preferences;
                Preference p1 = _testObject.getUserPreferences().createPreference(null,
                                                                                  "X-testType",
                                                                                  "propName",
                                                                                  null,
                                                                                  null,
                                                                                  Collections.<String, Object>emptyMap());
                preferences = Collections.singleton(p1);
                _testObject.getUserPreferences().updateOrAppend(preferences);
                Preference p2 = _testObject.getUserPreferences().createPreference(p1.getId(),
                                                                                  "X-testType",
                                                                                  "newPropName",
                                                                                  "newDescription",
                                                                                  null,
                                                                                  Collections.<String, Object>emptyMap());
                preferences = Collections.singleton(p2);
                _testObject.getUserPreferences().updateOrAppend(preferences);

                preferences = _testObject.getUserPreferences().getPreferences();
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
        Subject.doAs(_testSubject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                Set<Preference> preferences;
                Preference p1 = _testObject.getUserPreferences().createPreference(null,
                                                                                  "X-testType",
                                                                                  "propName",
                                                                                  null,
                                                                                  null,
                                                                                  Collections.<String, Object>emptyMap());
                preferences = Collections.singleton(p1);
                _testObject.getUserPreferences().updateOrAppend(preferences);
                Preference p2 = _testObject.getUserPreferences().createPreference(p1.getId(),
                                                                                  "X-differentTestType",
                                                                                  "propName",
                                                                                  null,
                                                                                  null,
                                                                                  Collections.<String, Object>emptyMap());
                preferences = Collections.singleton(p2);
                try
                {
                    _testObject.getUserPreferences().updateOrAppend(preferences);
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
        Subject.doAs(_testSubject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                Set<Preference> preferences;
                Preference p1 = _testObject.getUserPreferences().createPreference(null,
                                                                                  "X-testType",
                                                                                  "propName",
                                                                                  null,
                                                                                  null,
                                                                                  Collections.<String, Object>emptyMap());
                preferences = Collections.singleton(p1);
                _testObject.getUserPreferences().updateOrAppend(preferences);
                Preference p2 = _testObject.getUserPreferences().createPreference(null,
                                                                                  "X-testType",
                                                                                  "propName",
                                                                                  null,
                                                                                  null,
                                                                                  Collections.<String, Object>emptyMap());
                preferences = Collections.singleton(p2);
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

    public void testProhibitDuplicateNamesOfSameTypeInSameUpdate()
    {
        Subject.doAs(_testSubject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                Set<Preference> preferences = new HashSet<>();
                Preference p1 = _testObject.getUserPreferences().createPreference(null,
                                                                                  "X-testType",
                                                                                  "propName",
                                                                                  null,
                                                                                  null,
                                                                                  Collections.<String, Object>emptyMap());
                preferences.add(p1);
                Preference p2 = _testObject.getUserPreferences().createPreference(null,
                                                                                  "X-testType",
                                                                                  "propName",
                                                                                  null,
                                                                                  null,
                                                                                  Collections.<String, Object>emptyMap());
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
        Subject testSubject2 = TestPrincipalUtils.createTestSubject("testUser2");
        final Preference unaffectedPreference = Subject.doAs(testSubject2, new PrivilegedAction<Preference>()
        {
            @Override
            public Preference run()
            {
                Set<Preference> preferences;
                Preference p1 = _testObject.getUserPreferences().createPreference(null,
                                                                                  preferenceType,
                                                                                  "propName",
                                                                                  null,
                                                                                  null,
                                                                                  Collections.<String, Object>emptyMap());
                preferences = Collections.singleton(p1);
                _testObject.getUserPreferences().updateOrAppend(preferences);
                return p1;
            }
        });

        Subject.doAs(_testSubject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                Set<Preference> preferences;
                Preference p1 = _testObject.getUserPreferences().createPreference(null,
                                                                                  preferenceType,
                                                                                  "propName",
                                                                                  null,
                                                                                  null,
                                                                                  Collections.<String, Object>emptyMap());
                preferences = Collections.singleton(p1);
                _testObject.getUserPreferences().updateOrAppend(preferences);

                Preference p2 = _testObject.getUserPreferences().createPreference(null,
                                                                                  preferenceType,
                                                                                  "newPropName",
                                                                                  null,
                                                                                  null,
                                                                                  Collections.<String, Object>emptyMap());
                preferences = Collections.singleton(p2);
                _testObject.getUserPreferences().replace(preferences);

                Collection<Preference> retrievedPreferences = _testObject.getUserPreferences().getPreferences();
                assertEquals("Unexpected number of preferences", 1, retrievedPreferences.size());
                assertEquals("Unexpected preference", p2, retrievedPreferences.iterator().next());

                return null;
            }
        });

        Subject.doAs(testSubject2, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                Collection<Preference> retrievedPreferences = _testObject.getUserPreferences().getPreferences();
                assertEquals("Unexpected number of preferences", 1, retrievedPreferences.size());
                assertEquals("Unexpected preference", unaffectedPreference, retrievedPreferences.iterator().next());
                return null;
            }
        });
    }

    public void testDelete()
    {
        final String preferenceType = "X-testType";
        Subject testSubject2 = TestPrincipalUtils.createTestSubject("testUser2");
        final Preference unaffectedPreference = Subject.doAs(testSubject2, new PrivilegedAction<Preference>()
        {
            @Override
            public Preference run()
            {
                Set<Preference> preferences;
                Preference p1 = _testObject.getUserPreferences().createPreference(null,
                                                                                  preferenceType,
                                                                                  "propName",
                                                                                  null,
                                                                                  null,
                                                                                  Collections.<String, Object>emptyMap());
                preferences = Collections.singleton(p1);
                _testObject.getUserPreferences().updateOrAppend(preferences);
                return p1;
            }
        });

        Subject.doAs(_testSubject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                Set<Preference> preferences;
                Preference p1 = _testObject.getUserPreferences().createPreference(null,
                                                                                  preferenceType,
                                                                                  "propName",
                                                                                  null,
                                                                                  null,
                                                                                  Collections.<String, Object>emptyMap());
                preferences = Collections.singleton(p1);
                _testObject.getUserPreferences().updateOrAppend(preferences);

                _testObject.getUserPreferences().replace(Collections.<Preference>emptySet());

                Collection<Preference> retrievedPreferences = _testObject.getUserPreferences().getPreferences();
                assertEquals("Unexpected number of preferences", 0, retrievedPreferences.size());

                return null;
            }
        });

        Subject.doAs(testSubject2, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                Collection<Preference> retrievedPreferences = _testObject.getUserPreferences().getPreferences();
                assertEquals("Unexpected number of preferences", 1, retrievedPreferences.size());
                assertEquals("Unexpected preference", unaffectedPreference, retrievedPreferences.iterator().next());
                return null;
            }
        });
    }

    public void testDeleteByType()
    {
        final String preferenceType = "X-testType";
        final String unaffectedPreferenceType = "X-unaffectedType";
        Subject testSubject2 = TestPrincipalUtils.createTestSubject("testUser2");
        final Preference unaffectedPreference = Subject.doAs(testSubject2, new PrivilegedAction<Preference>()
        {
            @Override
            public Preference run()
            {
                Set<Preference> preferences;
                Preference p1 = _testObject.getUserPreferences().createPreference(null,
                                                                                  preferenceType,
                                                                                  "propName",
                                                                                  null,
                                                                                  null,
                                                                                  Collections.<String, Object>emptyMap());
                preferences = Collections.singleton(p1);
                _testObject.getUserPreferences().updateOrAppend(preferences);
                return p1;
            }
        });

        Subject.doAs(_testSubject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                Set<Preference> preferences = new HashSet<>();
                Preference p1 = _testObject.getUserPreferences().createPreference(null,
                                                                                  preferenceType,
                                                                                  "propName",
                                                                                  null,
                                                                                  null,
                                                                                  Collections.<String, Object>emptyMap());
                preferences.add(p1);
                Preference p2 = _testObject.getUserPreferences().createPreference(null,
                                                                                  unaffectedPreferenceType,
                                                                                  "propName",
                                                                                  null,
                                                                                  null,
                                                                                  Collections.<String, Object>emptyMap());
                preferences.add(p2);
                _testObject.getUserPreferences().updateOrAppend(preferences);

                _testObject.getUserPreferences().replaceByType(preferenceType, Collections.<Preference>emptySet());

                Collection<Preference> retrievedPreferences = _testObject.getUserPreferences().getPreferences();
                assertEquals("Unexpected number of preferences", 1, retrievedPreferences.size());
                assertTrue("Unexpected preference", retrievedPreferences.contains(p2));
                return null;
            }
        });

        Subject.doAs(testSubject2, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                Collection<Preference> retrievedPreferences = _testObject.getUserPreferences().getPreferences();
                assertEquals("Unexpected number of preferences", 1, retrievedPreferences.size());
                assertEquals("Unexpected preference", unaffectedPreference, retrievedPreferences.iterator().next());
                return null;
            }
        });
    }

    public void testDeleteByTypeAndName()
    {
        final String preferenceType = "X-testType";
        Subject testSubject2 = TestPrincipalUtils.createTestSubject("testUser2");
        final Preference unaffectedPreference = Subject.doAs(testSubject2, new PrivilegedAction<Preference>()
        {
            @Override
            public Preference run()
            {
                Set<Preference> preferences;
                Preference p1 = _testObject.getUserPreferences().createPreference(null,
                                                                                  preferenceType,
                                                                                  "propName",
                                                                                  null,
                                                                                  null,
                                                                                  Collections.<String, Object>emptyMap());
                preferences = Collections.singleton(p1);
                _testObject.getUserPreferences().updateOrAppend(preferences);
                return p1;
            }
        });

        Subject.doAs(_testSubject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                Set<Preference> preferences = new HashSet<>();
                Preference p1 = _testObject.getUserPreferences().createPreference(null,
                                                                                  preferenceType,
                                                                                  "propName",
                                                                                  null,
                                                                                  null,
                                                                                  Collections.<String, Object>emptyMap());
                preferences.add(p1);
                Preference p2 = _testObject.getUserPreferences().createPreference(null,
                                                                                  preferenceType,
                                                                                  "unaffectedPropName",
                                                                                  null,
                                                                                  null,
                                                                                  Collections.<String, Object>emptyMap());
                preferences.add(p2);
                _testObject.getUserPreferences().updateOrAppend(preferences);

                _testObject.getUserPreferences().replaceByTypeAndName(preferenceType, "propName", null);

                Collection<Preference> retrievedPreferences = _testObject.getUserPreferences().getPreferences();
                assertEquals("Unexpected number of preferences", 1, retrievedPreferences.size());
                assertTrue("Unexpected preference", retrievedPreferences.contains(p2));

                return null;
            }
        });

        Subject.doAs(testSubject2, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                Collection<Preference> retrievedPreferences = _testObject.getUserPreferences().getPreferences();
                assertEquals("Unexpected number of preferences", 1, retrievedPreferences.size());
                assertEquals("Unexpected preference", unaffectedPreference, retrievedPreferences.iterator().next());
                return null;
            }
        });
    }

    public void testReplaceByType()
    {
        final String replaceType = "X-replaceType";
        final String unaffectedType = "X-unaffectedType";
        Subject testSubject2 = TestPrincipalUtils.createTestSubject("testUser2");
        final Preference unaffectedPreference = Subject.doAs(testSubject2, new PrivilegedAction<Preference>()
        {
            @Override
            public Preference run()
            {
                Set<Preference> preferences;
                Preference p1 = _testObject.getUserPreferences().createPreference(null,
                                                                                  replaceType,
                                                                                  "propName",
                                                                                  null,
                                                                                  null,
                                                                                  Collections.<String, Object>emptyMap());
                preferences = Collections.singleton(p1);
                _testObject.getUserPreferences().updateOrAppend(preferences);
                return p1;
            }
        });

        Subject.doAs(_testSubject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                Set<Preference> preferences = new HashSet<>();
                Preference p1 = _testObject.getUserPreferences().createPreference(null,
                                                                                  replaceType,
                                                                                  "propName",
                                                                                  null,
                                                                                  null,
                                                                                  Collections.<String, Object>emptyMap());
                preferences.add(p1);
                Preference p2 = _testObject.getUserPreferences().createPreference(null,
                                                                                  unaffectedType,
                                                                                  "propName",
                                                                                  null,
                                                                                  null,
                                                                                  Collections.<String, Object>emptyMap());
                preferences.add(p2);
                _testObject.getUserPreferences().updateOrAppend(preferences);

                Preference p3 = _testObject.getUserPreferences().createPreference(null,
                                                                                  replaceType,
                                                                                  "newPropName",
                                                                                  null,
                                                                                  null,
                                                                                  Collections.<String, Object>emptyMap());
                preferences = Collections.singleton(p3);
                _testObject.getUserPreferences().replaceByType(replaceType, preferences);

                Set<Preference> retrievedPreferences = _testObject.getUserPreferences().getPreferences();
                assertEquals("Unexpected number of preferences", 2, retrievedPreferences.size());
                assertTrue("Preference of different type was replaced", retrievedPreferences.contains(p2));
                assertTrue("Preference was not replaced", retrievedPreferences.contains(p3));
                return null;
            }
        });

        Subject.doAs(testSubject2, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                Collection<Preference> retrievedPreferences = _testObject.getUserPreferences().getPreferences();
                assertEquals("Unexpected number of preferences", 1, retrievedPreferences.size());
                assertEquals("Preference of different user was replaced",
                             unaffectedPreference,
                             retrievedPreferences.iterator().next());
                return null;
            }
        });
    }

    public void testReplaceByTypeAndName()
    {
        final String replaceType = "X-replaceType";
        final String unaffectedType = "X-unaffectedType";
        Subject testSubject2 = TestPrincipalUtils.createTestSubject("testUser2");
        final Preference unaffectedPreference = Subject.doAs(testSubject2, new PrivilegedAction<Preference>()
        {
            @Override
            public Preference run()
            {
                Set<Preference> preferences;
                Preference p1 = _testObject.getUserPreferences().createPreference(null,
                                                                                  replaceType,
                                                                                  "propName",
                                                                                  null,
                                                                                  null,
                                                                                  Collections.<String, Object>emptyMap());
                preferences = Collections.singleton(p1);
                _testObject.getUserPreferences().updateOrAppend(preferences);
                return p1;
            }
        });

        Subject.doAs(_testSubject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                Set<Preference> preferences = new HashSet<>();
                Preference p1 = _testObject.getUserPreferences().createPreference(null,
                                                                                  replaceType,
                                                                                  "propName",
                                                                                  null,
                                                                                  null,
                                                                                  Collections.<String, Object>emptyMap());
                preferences.add(p1);
                Preference p1b = _testObject.getUserPreferences().createPreference(null,
                                                                                   replaceType,
                                                                                   "unaffectedPropName",
                                                                                   null,
                                                                                   null,
                                                                                   Collections.<String, Object>emptyMap());
                preferences.add(p1b);
                Preference p2 = _testObject.getUserPreferences().createPreference(null,
                                                                                  unaffectedType,
                                                                                  "propName",
                                                                                  null,
                                                                                  null,
                                                                                  Collections.<String, Object>emptyMap());
                preferences.add(p2);
                _testObject.getUserPreferences().updateOrAppend(preferences);

                Preference p3 = _testObject.getUserPreferences().createPreference(null,
                                                                                  replaceType,
                                                                                  "propName",
                                                                                  "new description",
                                                                                  null,
                                                                                  Collections.<String, Object>emptyMap());
                _testObject.getUserPreferences().replaceByTypeAndName(replaceType, "propName", p3);

                Set<Preference> retrievedPreferences = _testObject.getUserPreferences().getPreferences();
                assertEquals("Unexpected number of preferences", 3, retrievedPreferences.size());
                assertTrue("Preference of different name was replaced", retrievedPreferences.contains(p1b));
                assertTrue("Preference of different type was replaced", retrievedPreferences.contains(p2));
                assertTrue("Preference was not replaced", retrievedPreferences.contains(p3));
                return null;
            }
        });

        Subject.doAs(testSubject2, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                Collection<Preference> retrievedPreferences = _testObject.getUserPreferences().getPreferences();
                assertEquals("Unexpected number of preferences", 1, retrievedPreferences.size());
                assertEquals("Preference of different user was replaced",
                             unaffectedPreference,
                             retrievedPreferences.iterator().next());
                return null;
            }
        });
    }

    public void testGetVisiblePreferences()
    {
        final Principal testPrincipal = _testSubject.getPrincipals().iterator().next();
        Subject testSubject2 = TestPrincipalUtils.createTestSubject("testUser2");
        final Preference p1 = Subject.doAs(testSubject2, new PrivilegedAction<Preference>()
        {
            @Override
            public Preference run()
            {
                Set<Preference> preferences;
                Preference p1 = _testObject.getUserPreferences().createPreference(null,
                                                                                  "X-testType",
                                                                                  "propName1",
                                                                                  null,
                                                                                  Collections.singleton(testPrincipal),
                                                                                  Collections.<String, Object>emptyMap());
                preferences = Collections.singleton(p1);
                _testObject.getUserPreferences().updateOrAppend(preferences);
                return p1;
            }
        });

        Subject.doAs(_testSubject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                Set<Preference> preferences = new HashSet<>();
                Preference p2 = _testObject.getUserPreferences().createPreference(null,
                                                                                  "X-testType",
                                                                                  "propName",
                                                                                  null,
                                                                                  null,
                                                                                  Collections.<String, Object>emptyMap());
                preferences.add(p2);
                _testObject.getUserPreferences().updateOrAppend(preferences);

                Set<Preference> retrievedPreferences = _testObject.getUserPreferences().getVisiblePreferences();
                assertEquals("Unexpected number of preferences", 1, retrievedPreferences.size());
                assertTrue("Preference of different type was replaced", retrievedPreferences.contains(p1));
                return null;
            }
        });
    }

    public void testGetVisiblePreferencesSharedByGroup()
    {
        Subject testSubjectWithGroup = TestPrincipalUtils.createTestSubject("testUser", "testGroup");

        Principal tempGroupPrincipal = null;
        for (Principal principal : testSubjectWithGroup.getPrincipals())
        {
            if (principal instanceof GroupPrincipal)
            {
                tempGroupPrincipal = principal;
                break;
            }
        }
        final Principal groupPrincipal = tempGroupPrincipal;

        Subject testSubject2 = TestPrincipalUtils.createTestSubject("testUser2");
        final Preference p1 = Subject.doAs(testSubject2, new PrivilegedAction<Preference>()
        {
            @Override
            public Preference run()
            {
                Set<Preference> preferences;
                Preference p1 = _testObject.getUserPreferences().createPreference(null,
                                                                                  "X-testType",
                                                                                  "propName1",
                                                                                  null,
                                                                                  Collections.singleton(groupPrincipal),
                                                                                  Collections.<String, Object>emptyMap());
                preferences = Collections.singleton(p1);
                _testObject.getUserPreferences().updateOrAppend(preferences);
                return p1;
            }
        });

        Subject.doAs(testSubjectWithGroup, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                Set<Preference> preferences = new HashSet<>();
                Preference p2 = _testObject.getUserPreferences().createPreference(null,
                                                                                  "X-testType",
                                                                                  "propName",
                                                                                  null,
                                                                                  null,
                                                                                  Collections.<String, Object>emptyMap());
                preferences.add(p2);
                _testObject.getUserPreferences().updateOrAppend(preferences);

                Set<Preference> retrievedPreferences = _testObject.getUserPreferences().getVisiblePreferences();
                assertEquals("Unexpected number of preferences", 1, retrievedPreferences.size());
                assertTrue("Preference of different type was replaced", retrievedPreferences.contains(p1));
                return null;
            }
        });
    }

    public void testLastUpdatedDate() throws Exception
    {
        Date before = new Date();
        Thread.sleep(1);
        final Preference p1 = Subject.doAs(_testSubject, new PrivilegedAction<Preference>()
        {
            @Override
            public Preference run()
            {
                return _testObject.getUserPreferences().createPreference(null,
                                                                         "X-testType",
                                                                         "propName1",
                                                                         null,
                                                                         null,
                                                                         Collections.<String, Object>emptyMap());
            }
        });
        Thread.sleep(1);
        Date after = new Date();
        Date lastUpdatedDate = p1.getLastUpdatedDate();
        assertTrue(String.format("Creation date is too early. Expected : after %s  Found : %s", before, lastUpdatedDate),
                   before.before(lastUpdatedDate));
        assertTrue(String.format("Creation date is too late. Expected : after %s  Found : %s", after, lastUpdatedDate),
                   after.after(lastUpdatedDate));
    }

    public void testLastUpdatedDateIsImmutable() throws Exception
    {
        final Preference p1 = Subject.doAs(_testSubject, new PrivilegedAction<Preference>()
        {
            @Override
            public Preference run()
            {
                return _testObject.getUserPreferences().createPreference(null,
                                                                         "X-testType",
                                                                         "propName1",
                                                                         null,
                                                                         null,
                                                                         Collections.<String, Object>emptyMap());
            }
        });
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
        final Set<Principal> visibilitySet = Collections.<Principal>emptySet();
        Preference p = _testObject.getUserPreferences()
                                  .createPreference(uuid,
                                                    type,
                                                    name,
                                                    description,
                                                    visibilitySet,
                                                    prefValueMap);
        assertNotNull("Creation failed", p);
        Date lastUpdatedDate = p.getLastUpdatedDate();

        Map<String, Object> expectedAttributes = new HashMap<>();
        expectedAttributes.put("id", uuid);
        expectedAttributes.put("type", type);
        expectedAttributes.put("name", name);
        expectedAttributes.put("description", description);
        expectedAttributes.put("owner", "");
        expectedAttributes.put("associatedObject", _testObject.getId());
        expectedAttributes.put("visibilityList", visibilitySet);
        expectedAttributes.put("lastUpdatedDate", lastUpdatedDate);
        expectedAttributes.put("value", prefValueMap);
        assertEquals("Unexpected preference attributes", expectedAttributes, p.getAttributes());
    }
}
