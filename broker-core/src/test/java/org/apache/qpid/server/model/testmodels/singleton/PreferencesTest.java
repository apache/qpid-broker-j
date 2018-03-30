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

import static org.apache.qpid.server.model.preferences.PreferenceTestHelper.awaitPreferenceFuture;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.security.auth.Subject;

import com.google.common.util.concurrent.ListenableFuture;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.preferences.Preference;
import org.apache.qpid.server.model.preferences.PreferenceFactory;
import org.apache.qpid.server.model.preferences.PreferenceTestHelper;
import org.apache.qpid.server.model.preferences.UserPreferencesImpl;
import org.apache.qpid.server.security.auth.TestPrincipalUtils;
import org.apache.qpid.server.store.preferences.PreferenceStore;
import org.apache.qpid.test.utils.UnitTestBase;

public class PreferencesTest extends UnitTestBase
{
    public static final String TEST_USERNAME = "testUser";
    private static final String TEST_PRINCIPAL_SERIALIZATION = TestPrincipalUtils.getTestPrincipalSerialization(TEST_USERNAME);
    public static final String TEST_USERNAME2 = "testUser2";
    private static final String TEST_PRINCIPAL2_SERIALIZATION = TestPrincipalUtils.getTestPrincipalSerialization(TEST_USERNAME2);
    private final Model _model = TestModel.getInstance();
    private ConfiguredObject<?> _testObject;
    private Subject _testSubject;
    private TaskExecutor _preferenceTaskExecutor;

    @Before
    public void setUp() throws Exception
    {
        final String objectName = getTestName();
        _testObject = _model.getObjectFactory()
                            .create(TestSingleton.class,
                                    Collections.<String, Object>singletonMap(ConfiguredObject.NAME, objectName), null);

        _preferenceTaskExecutor = new CurrentThreadTaskExecutor();
        _preferenceTaskExecutor.start();
        PreferenceStore preferenceStore = mock(PreferenceStore.class);
        _testObject.setUserPreferences(new UserPreferencesImpl(
                _preferenceTaskExecutor, _testObject, preferenceStore, Collections.<Preference>emptySet()
        ));
        _testSubject = TestPrincipalUtils.createTestSubject(TEST_USERNAME);
    }

    @After
    public void tearDown() throws Exception
    {
        _preferenceTaskExecutor.stop();
    }

    @Test
    public void testSimpleRoundTrip()
    {
        final Preference p = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                "X-TestPropType",
                "testProp1",
                null,
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Collections.<String, Object>emptyMap()));

        updateOrAppendAs(_testSubject, p);
        assertPreferences(_testSubject, p);
    }

    @Test
    public void testOverrideContradictingOwner()
    {
        Subject testSubject2 = TestPrincipalUtils.createTestSubject(TEST_USERNAME2);
        final Preference p = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                "X-testType",
                "prop1",
                null,
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Collections.<String, Object>emptyMap()));

        updateOrAppendAs(testSubject2, p);
        assertPreferences(testSubject2, p);

        assertPreferences(_testSubject);
    }

    @Test
    public void testGetOnlyOwnedPreferences()
    {
        final Preference testUserPreference =
                PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                        null,
                        null,
                        "X-testType",
                        "prop1",
                        null,
                        TEST_PRINCIPAL_SERIALIZATION,
                        null,
                        Collections.<String, Object>emptyMap()));

        updateOrAppendAs(_testSubject, testUserPreference);

        final Preference testUser2Preference =
                PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                        null,
                        null,
                        "X-testType",
                        "prop2",
                        null,
                        TEST_PRINCIPAL2_SERIALIZATION,
                        null,
                        Collections.<String, Object>emptyMap()));

        Subject testSubject2 = TestPrincipalUtils.createTestSubject(TEST_USERNAME2);
        updateOrAppendAs(testSubject2, testUser2Preference);

        assertPreferences(testSubject2, testUser2Preference);
        assertPreferences(_testSubject, testUserPreference);
    }

    @Test
    public void testUpdate()
    {
        final Preference p1 = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                "X-testType",
                "propName",
                null,
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Collections.<String, Object>emptyMap()));

        final Preference p2 = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                p1.getId(),
                "X-testType",
                "newPropName",
                "newDescription",
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Collections.<String, Object>emptyMap()));

        updateOrAppendAs(_testSubject, p1);
        updateOrAppendAs(_testSubject, p2);

        assertPreferences(_testSubject, p2);
    }

    @Test
    public void testProhibitTypeChange()
    {
        final Preference p1 = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                "X-testType",
                "propName",
                null,
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Collections.<String, Object>emptyMap()));

        updateOrAppendAs(_testSubject, p1);

        final Preference p2 = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                p1.getId(),
                "X-differentTestType",
                "propName",
                null,
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Collections.<String, Object>emptyMap()));

        try
        {
            updateOrAppendAs(_testSubject, p2);
            fail("Type change should not be allowed");
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }
    }

    @Test
    public void testProhibitDuplicateNamesOfSameType()
    {
        final Preference p1 = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                "X-testType",
                "propName",
                null,
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Collections.<String, Object>emptyMap()));

        updateOrAppendAs(_testSubject, p1);

        final Preference p2 = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                "X-testType",
                "propName",
                null,
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Collections.<String, Object>emptyMap()));

        try
        {
            updateOrAppendAs(_testSubject, p2);
            fail("Property with same name and same type should not be allowed");
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }
    }

    @Test
    public void testProhibitDuplicateNamesOfSameTypeInSameUpdate()
    {
        final Preference p1 = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                "X-testType",
                "propName",
                null,
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Collections.<String, Object>emptyMap()));
        final Preference p2 = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                "X-testType",
                "propName",
                null,
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Collections.<String, Object>emptyMap()));

        try
        {
            updateOrAppendAs(_testSubject, p1, p2);
            fail("Property with same name and same type should not be allowed");
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }
    }

    @Test
    public void testProhibitPreferenceStealing() throws Exception
    {
        final String testGroupName = "testGroup";
        Subject user1Subject = TestPrincipalUtils.createTestSubject(TEST_USERNAME, testGroupName);

        Map<String, Object> preferenceAttributes = PreferenceTestHelper.createPreferenceAttributes(
                _testObject.getId(),
                null,
                "X-PREF",
                "prefname",
                null,
                TEST_PRINCIPAL_SERIALIZATION,
                Collections.singleton(TestPrincipalUtils.getTestPrincipalSerialization(testGroupName)),
                Collections.<String,Object>emptyMap());
        final Preference originalPreference = PreferenceFactory.fromAttributes(_testObject, preferenceAttributes);
        updateOrAppendAs(user1Subject, originalPreference);


        Subject user2Subject = TestPrincipalUtils.createTestSubject(TEST_USERNAME2, testGroupName);
        Subject.doAs(user2Subject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                final ListenableFuture<Set<Preference>> visiblePreferencesFuture = _testObject.getUserPreferences().getVisiblePreferences();
                final Set<Preference> visiblePreferences = PreferenceTestHelper.awaitPreferenceFuture(visiblePreferencesFuture);

                assertEquals("Unexpected number of visible preferences", (long) 1, (long) visiblePreferences
                        .size());

                final Preference target = visiblePreferences.iterator().next();
                Map<String, Object> replacementAttributes = new HashMap(target.getAttributes());
                replacementAttributes.put(Preference.OWNER_ATTRIBUTE, TEST_PRINCIPAL2_SERIALIZATION);

                try
                {
                    awaitPreferenceFuture(_testObject.getUserPreferences().updateOrAppend(Arrays.asList(PreferenceFactory.fromAttributes(_testObject, replacementAttributes))));
                    fail("The stealing of a preference must be prohibited");
                }
                catch (IllegalArgumentException e)
                {
                    // pass
                }

                return null;
            }
        });

        assertPreferences(user1Subject, originalPreference);
    }

    @Test
    public void testProhibitDuplicateId() throws Exception
    {
        final String prefType = "X-testType";
        final String prefName = "prop1";
        final Preference testUserPreference =
                PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                        null,
                        null,
                        prefType,
                        prefName,
                        null,
                        TEST_PRINCIPAL_SERIALIZATION,
                        null,
                        Collections.<String, Object>emptyMap()));

        updateOrAppendAs(_testSubject, testUserPreference);

        Subject user2Subject = TestPrincipalUtils.createTestSubject(TEST_USERNAME2);
        final Preference testUserPreference2 =
                PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                        null,
                        testUserPreference.getId(),
                        prefType,
                        prefName,
                        "new preference",
                        TEST_PRINCIPAL2_SERIALIZATION,
                        null,
                        Collections.<String, Object>emptyMap()));
        try
        {
            updateOrAppendAs(user2Subject, testUserPreference2);
            fail("duplicate id should be prohibited");
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }

        try
        {
            Subject.doAs(user2Subject, new PrivilegedAction<Void>()
            {
                @Override
                public Void run()
                {
                    awaitPreferenceFuture(_testObject.getUserPreferences().replace(Arrays.asList(testUserPreference2)));
                    return null;
                }
            });
            fail("duplicate id should be prohibited");
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }

        try
        {
            Subject.doAs(user2Subject, new PrivilegedAction<Void>()
            {
                @Override
                public Void run()
                {
                    awaitPreferenceFuture(_testObject.getUserPreferences().replaceByType(testUserPreference.getType(), Arrays.asList(testUserPreference2)));
                    return null;
                }
            });
            fail("duplicate id should be prohibited");
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }


        try
        {
            Subject.doAs(user2Subject, new PrivilegedAction<Void>()
            {
                @Override
                public Void run()
                {
                    awaitPreferenceFuture(_testObject.getUserPreferences().replaceByTypeAndName(testUserPreference.getType(), testUserPreference.getName(), testUserPreference2));
                    return null;
                }
            });
            fail("duplicate id should be prohibited");
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }
    }

    @Test
    public void testReplace()
    {
        final String preferenceType = "X-testType";
        Subject testSubject2 = TestPrincipalUtils.createTestSubject(TEST_USERNAME2);

        final Preference unaffectedPreference =
                PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                        null,
                        null,
                        preferenceType,
                        "propName",
                        null,
                        TEST_PRINCIPAL2_SERIALIZATION,
                        null,
                        Collections.<String, Object>emptyMap()));
        updateOrAppendAs(testSubject2, unaffectedPreference);

        final Preference p1 = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                preferenceType,
                "propName",
                null,
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Collections.<String, Object>emptyMap()));

        updateOrAppendAs(_testSubject, p1);

        final Preference p2 = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                preferenceType,
                "newPropName",
                null,
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Collections.<String, Object>emptyMap()));

        Subject.doAs(_testSubject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                awaitPreferenceFuture(_testObject.getUserPreferences().replace(Collections.singleton(p2)));
                return null;
            }
        });

        assertPreferences(_testSubject, p2);
        assertPreferences(testSubject2, unaffectedPreference);
    }

    @Test
    public void testDeleteAll() throws Exception
    {
        final Preference p1 = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                "X-type-1",
                "propName",
                null,
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Collections.<String, Object>emptyMap()));
        final Preference p2 = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                "X-type-2",
                "propName",
                null,
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Collections.<String, Object>emptyMap()));
        updateOrAppendAs(_testSubject, p1, p2);

        Subject.doAs(_testSubject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                awaitPreferenceFuture(_testObject.getUserPreferences().delete(null, null, null));
                Set<Preference> result = awaitPreferenceFuture(_testObject.getUserPreferences().getPreferences());
                assertEquals("Unexpected number of preferences", (long) 0, (long) result.size());
                return null;
            }
        });
    }

    @Test
    public void testDeleteByType() throws Exception
    {
        final String deleteType = "X-type-1";
        final Preference deletePreference =
                PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                        null,
                        null,
                        deleteType,
                        "propName",
                        null,
                        TEST_PRINCIPAL_SERIALIZATION,
                        null,
                        Collections.<String, Object>emptyMap()));
        String unaffectedType = "X-type-2";
        final Preference unaffectedPreference =
                PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                        null,
                        null,
                        unaffectedType,
                        "propName",
                        null,
                        TEST_PRINCIPAL_SERIALIZATION,
                        null,
                        Collections.<String, Object>emptyMap()));
        updateOrAppendAs(_testSubject, deletePreference, unaffectedPreference);

        Subject.doAs(_testSubject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                awaitPreferenceFuture(_testObject.getUserPreferences().delete(deleteType, null, null));
                return null;
            }
        });
        assertPreferences(_testSubject, unaffectedPreference);
    }

    @Test
    public void testDeleteByTypeAndName() throws Exception
    {
        final String deleteType = "X-type-1";
        final String deletePropertyName = "propName";
        final Preference deletePreference =
                PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                        null,
                        null,
                        deleteType,
                        deletePropertyName,
                        null,
                        TEST_PRINCIPAL_SERIALIZATION,
                        null,
                        Collections.<String, Object>emptyMap()));
        final Preference unaffectedPreference1 =
                PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                        null,
                        null,
                        deleteType,
                        "propName2",
                        null,
                        TEST_PRINCIPAL_SERIALIZATION,
                        null,
                        Collections.<String, Object>emptyMap()));
        String unaffectedType = "X-type-2";
        final Preference unaffectedPreference2 =
                PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                        null,
                        null,
                        unaffectedType,
                        deletePropertyName,
                        null,
                        TEST_PRINCIPAL_SERIALIZATION,
                        null,
                        Collections.<String, Object>emptyMap()));
        updateOrAppendAs(_testSubject, deletePreference, unaffectedPreference1, unaffectedPreference2);

        Subject.doAs(_testSubject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                awaitPreferenceFuture(_testObject.getUserPreferences().delete(deleteType, deletePropertyName, null));
                return null;
            }
        });
        assertPreferences(_testSubject, unaffectedPreference1, unaffectedPreference2);
    }

    @Test
    public void testDeleteById() throws Exception
    {
        final String deleteType = "X-type-1";
        final Preference deletePreference =
                PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                        null,
                        null,
                        deleteType,
                        "propName",
                        null,
                        TEST_PRINCIPAL_SERIALIZATION,
                        null,
                        Collections.<String, Object>emptyMap()));
        final Preference unaffectedPreference1 =
                PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                        null,
                        null,
                        deleteType,
                        "propName2",
                        null,
                        TEST_PRINCIPAL_SERIALIZATION,
                        null,
                        Collections.<String, Object>emptyMap()));
        String unaffectedType = "X-type-2";
        final Preference unaffectedPreference2 =
                PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                        null,
                        null,
                        unaffectedType,
                        "propName",
                        null,
                        TEST_PRINCIPAL_SERIALIZATION,
                        null,
                        Collections.<String, Object>emptyMap()));
        updateOrAppendAs(_testSubject, deletePreference, unaffectedPreference1, unaffectedPreference2);

        Subject.doAs(_testSubject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                awaitPreferenceFuture(_testObject.getUserPreferences().delete(null, null, deletePreference.getId()));
                return null;
            }
        });
        assertPreferences(_testSubject, unaffectedPreference1, unaffectedPreference2);
    }

    @Test
    public void testDeleteByTypeAndId() throws Exception
    {
        final String deleteType = "X-type-1";
        final Preference deletePreference =
                PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                        null,
                        null,
                        deleteType,
                        "propName",
                        null,
                        TEST_PRINCIPAL_SERIALIZATION,
                        null,
                        Collections.<String, Object>emptyMap()));
        final Preference unaffectedPreference1 =
                PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                        null,
                        null,
                        deleteType,
                        "propName2",
                        null,
                        TEST_PRINCIPAL_SERIALIZATION,
                        null,
                        Collections.<String, Object>emptyMap()));
        String unaffectedType = "X-type-2";
        final Preference unaffectedPreference2 =
                PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                        null,
                        null,
                        unaffectedType,
                        "propName",
                        null,
                        TEST_PRINCIPAL_SERIALIZATION,
                        null,
                        Collections.<String, Object>emptyMap()));
        updateOrAppendAs(_testSubject, deletePreference, unaffectedPreference1, unaffectedPreference2);

        Subject.doAs(_testSubject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                awaitPreferenceFuture(_testObject.getUserPreferences().delete(deleteType, null, deletePreference.getId()));
                return null;
            }
        });
        assertPreferences(_testSubject, unaffectedPreference1, unaffectedPreference2);
    }

    @Test
    public void testDeleteByTypeAndNameAndId() throws Exception
    {
        final String deleteType = "X-type-1";
        final String deletePropertyName = "propName";
        final Preference deletePreference =
                PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                        null,
                        null,
                        deleteType,
                        deletePropertyName,
                        null,
                        TEST_PRINCIPAL_SERIALIZATION,
                        null,
                        Collections.<String, Object>emptyMap()));
        final Preference unaffectedPreference1 =
                PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                        null,
                        null,
                        deleteType,
                        "propName2",
                        null,
                        TEST_PRINCIPAL_SERIALIZATION,
                        null,
                        Collections.<String, Object>emptyMap()));
        String unaffectedType = "X-type-2";
        final Preference unaffectedPreference2 =
                PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                        null,
                        null,
                        unaffectedType,
                        deletePropertyName,
                        null,
                        TEST_PRINCIPAL_SERIALIZATION,
                        null,
                        Collections.<String, Object>emptyMap()));
        updateOrAppendAs(_testSubject, deletePreference, unaffectedPreference1, unaffectedPreference2);

        Subject.doAs(_testSubject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                awaitPreferenceFuture(_testObject.getUserPreferences().delete(deleteType, deletePropertyName, deletePreference.getId()));
                return null;
            }
        });
        assertPreferences(_testSubject, unaffectedPreference1, unaffectedPreference2);
    }

    @Test
    public void testDeleteByNameWithoutType() throws Exception
    {
        Subject.doAs(_testSubject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                try
                {
                    awaitPreferenceFuture(_testObject.getUserPreferences().delete(null, "test", null));
                    fail("delete by name without type should not be allowed");
                }
                catch (IllegalArgumentException e)
                {
                    // pass
                }
                return null;
            }
        });
    }

    @Test
    public void testDeleteViaReplace()
    {
        final String preferenceType = "X-testType";
        Subject testSubject2 = TestPrincipalUtils.createTestSubject(TEST_USERNAME2);
        final Preference unaffectedPreference =
                PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                        null,
                        null,
                        preferenceType,
                        "propName",
                        null,
                        TEST_PRINCIPAL2_SERIALIZATION,
                        null,
                        Collections.<String, Object>emptyMap()));
        updateOrAppendAs(testSubject2, unaffectedPreference);

        final Preference p1 = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                preferenceType,
                "propName",
                null,
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Collections.<String, Object>emptyMap()));
        updateOrAppendAs(_testSubject, p1);

        Subject.doAs(_testSubject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                awaitPreferenceFuture(_testObject.getUserPreferences().replace(Collections.<Preference>emptySet()));
                return null;
            }
        });

        assertPreferences(_testSubject);
        assertPreferences(testSubject2, unaffectedPreference);
    }

    @Test
    public void testDeleteViaReplaceByType()
    {
        final String preferenceType = "X-testType";
        final String unaffectedPreferenceType = "X-unaffectedType";
        Subject testSubject2 = TestPrincipalUtils.createTestSubject(TEST_USERNAME2);

        final Preference unaffectedPreference =
                PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                        null,
                        null,
                        preferenceType,
                        "propName",
                        null,
                        TEST_PRINCIPAL2_SERIALIZATION,
                        null,
                        Collections.<String, Object>emptyMap()));
        updateOrAppendAs(testSubject2, unaffectedPreference);

        final Preference p1 = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                preferenceType,
                "propName",
                null,
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Collections.<String, Object>emptyMap()));
        final Preference p2 = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                unaffectedPreferenceType,
                "propName",
                null,
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Collections.<String, Object>emptyMap()));

        updateOrAppendAs(_testSubject, p1, p2);
        Subject.doAs(_testSubject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                awaitPreferenceFuture(_testObject.getUserPreferences().replaceByType(preferenceType, Collections.<Preference>emptySet()));
                return null;
            }
        });

        assertPreferences(_testSubject, p2);
        assertPreferences(testSubject2, unaffectedPreference);
    }

    @Test
    public void testDeleteViaReplaceByTypeAndName()
    {
        final String preferenceType = "X-testType";
        Subject testSubject2 = TestPrincipalUtils.createTestSubject(TEST_USERNAME2);

        final Preference unaffectedPreference =
                PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                        null,
                        null,
                        preferenceType,
                        "propName",
                        null,
                        TEST_PRINCIPAL2_SERIALIZATION,
                        null,
                        Collections.<String, Object>emptyMap()));

        updateOrAppendAs(testSubject2, unaffectedPreference);

        final Preference p1 = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                preferenceType,
                "propName",
                null,
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Collections.<String, Object>emptyMap()));

        final Preference p2 = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                preferenceType,
                "unaffectedPropName",
                null,
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Collections.<String, Object>emptyMap()));
        updateOrAppendAs(_testSubject, p1, p2);

        Subject.doAs(_testSubject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                awaitPreferenceFuture(_testObject.getUserPreferences().replaceByTypeAndName(preferenceType, "propName", null));
                return null;
            }
        });

        assertPreferences(_testSubject, p2);
        assertPreferences(testSubject2, unaffectedPreference);
    }

    @Test
    public void testReplaceByType()
    {
        final String replaceType = "X-replaceType";
        final String unaffectedType = "X-unaffectedType";
        Subject testSubject2 = TestPrincipalUtils.createTestSubject(TEST_USERNAME2);
        final Preference unaffectedPreference =
                PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                        null,
                        null,
                        replaceType,
                        "propName",
                        null,
                        TEST_PRINCIPAL2_SERIALIZATION,
                        null,
                        Collections.<String, Object>emptyMap()));

        updateOrAppendAs(testSubject2, unaffectedPreference);

        final Preference p1 = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                replaceType,
                "propName",
                null,
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Collections.<String, Object>emptyMap()));
        final Preference p2 = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                unaffectedType,
                "propName",
                null,
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Collections.<String, Object>emptyMap()));
        final Preference p3 = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                replaceType,
                "newPropName",
                null,
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Collections.<String, Object>emptyMap()));
        updateOrAppendAs(_testSubject, p1, p2);

        Subject.doAs(_testSubject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                awaitPreferenceFuture(_testObject.getUserPreferences().replaceByType(replaceType, Collections.singleton(p3)));
                return null;
            }
        });

        assertPreferences(_testSubject, p2, p3);
        assertPreferences(testSubject2, unaffectedPreference);
    }

    @Test
    public void testReplaceByTypeAndName()
    {
        final String replaceType = "X-replaceType";
        final String unaffectedType = "X-unaffectedType";
        Subject testSubject2 = TestPrincipalUtils.createTestSubject(TEST_USERNAME2);
        final Preference unaffectedPreference =
                PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                        null,
                        null,
                        replaceType,
                        "propName",
                        null,
                        TEST_PRINCIPAL2_SERIALIZATION,
                        null,
                        Collections.<String, Object>emptyMap()));

        updateOrAppendAs(testSubject2, unaffectedPreference);

        final Preference p1 = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                replaceType,
                "propName",
                null,
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Collections.<String, Object>emptyMap()));
        final Preference p1b = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                replaceType,
                "unaffectedPropName",
                null,
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Collections.<String, Object>emptyMap()));

        final Preference p2 = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                unaffectedType,
                "propName",
                null,
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Collections.<String, Object>emptyMap()));

        final Preference p3 = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                replaceType,
                "propName",
                "new description",
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Collections.<String, Object>emptyMap()));
        updateOrAppendAs(_testSubject, p1, p1b, p2);

        Subject.doAs(_testSubject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                awaitPreferenceFuture(_testObject.getUserPreferences().replaceByTypeAndName(replaceType, "propName", p3));
                return null;
            }
        });

        assertPreferences(_testSubject, p1b, p2, p3);
        assertPreferences(testSubject2, unaffectedPreference);
    }

    @Test
    public void testGetVisiblePreferences()
    {
        final String testGroupName = "testGroup";

        Subject sharer = TestPrincipalUtils.createTestSubject(TEST_USERNAME2, testGroupName);
        Subject nonSharer = TestPrincipalUtils.createTestSubject(TEST_USERNAME, testGroupName);

        final Preference sharedPreference = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                "X-testType",
                "propName1",
                "shared with group",
                TEST_PRINCIPAL2_SERIALIZATION,
                Collections.singleton(TestPrincipalUtils.getTestPrincipalSerialization(testGroupName)),
                Collections.<String, Object>emptyMap()));

        final Preference notSharedPreference = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                "X-testType",
                "propName2",
                null,
                TEST_PRINCIPAL2_SERIALIZATION,
                null,
                Collections.<String, Object>emptyMap()));

        updateOrAppendAs(sharer, sharedPreference, notSharedPreference);

        final Preference nonSharersPrivatePref = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                "X-testType",
                "propName",
                null,
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Collections.<String, Object>emptyMap()));

        updateOrAppendAs(nonSharer, nonSharersPrivatePref);

        Subject.doAs(nonSharer, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                Set<Preference> retrievedPreferences =
                        awaitPreferenceFuture(_testObject.getUserPreferences().getVisiblePreferences());
                assertEquals("Unexpected number of preferences", (long) 2, (long) retrievedPreferences.size());

                Set<UUID> visibleIds = new HashSet<>(retrievedPreferences.size());
                for (Preference preference : retrievedPreferences)
                {
                    visibleIds.add(preference.getId());
                }
                assertTrue("Owned preference not visible", visibleIds.contains(nonSharersPrivatePref.getId()));
                assertTrue("Shared preference not visible", visibleIds.contains(sharedPreference.getId()));

                return null;
            }
        });
    }

    @Test
    public void testGetVisiblePreferencesSharedByGroup()
    {
        final String testGroupName = "testGroup";
        Subject testSubjectWithGroup = TestPrincipalUtils.createTestSubject(TEST_USERNAME, testGroupName);

        final Preference sharedPreference =
                PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                        null,
                        null,
                        "X-testType",
                        "propName1",
                        null,
                        TEST_PRINCIPAL2_SERIALIZATION,
                        Collections.singleton(TestPrincipalUtils.getTestPrincipalSerialization(testGroupName)),
                        Collections.<String, Object>emptyMap()));

        Subject peerSubject = TestPrincipalUtils.createTestSubject(TEST_USERNAME2, testGroupName);
        updateOrAppendAs(peerSubject, sharedPreference);

        final Preference testUserPreference =
                PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                        null,
                        null,
                        "X-testType",
                        "propName",
                        null,
                        TEST_PRINCIPAL_SERIALIZATION,
                        null,
                        Collections.<String, Object>emptyMap()));
        updateOrAppendAs(testSubjectWithGroup, testUserPreference);

        Subject.doAs(testSubjectWithGroup, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                Set<Preference> retrievedPreferences =
                        awaitPreferenceFuture(_testObject.getUserPreferences().getVisiblePreferences());
                assertEquals("Unexpected number of preferences", (long) 2, (long) retrievedPreferences.size());
                assertTrue("Preference of my peer did not exist in visible set",
                                  retrievedPreferences.contains(sharedPreference));
                assertTrue("My preference did not exist in visible set",
                                  retrievedPreferences.contains(testUserPreference));
                return null;
            }
        });
    }

    @Test
    public void testLastUpdatedDate() throws Exception
    {
        Date beforeUpdate = new Date();
        Thread.sleep(1);
        final Preference p1 = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                "X-testType",
                "propName1",
                null,
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Collections.<String, Object>emptyMap()));
        Thread.sleep(1);
        Date afterUpdate = new Date();
        Date lastUpdatedDate = p1.getLastUpdatedDate();
        assertTrue(String.format("LastUpdated date is too early. Expected : after %s  Found : %s",
                                        beforeUpdate,
                                        lastUpdatedDate), beforeUpdate.before(lastUpdatedDate));
        assertTrue(String.format("LastUpdated date is too late. Expected : before %s  Found : %s", afterUpdate, lastUpdatedDate),
                          afterUpdate.after(lastUpdatedDate));
    }


    @Test
    public void testCreatedDate() throws Exception
    {
        Date beforeCreation = new Date();
        Thread.sleep(1);
        final Preference p1 = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                "X-testType",
                "propName1",
                null,
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Collections.<String, Object>emptyMap()));
        Thread.sleep(1);
        Date afterCreation = new Date();
        Date createdDate = p1.getCreatedDate();
        assertTrue(String.format("Creation date is too early. Expected : after %s  Found : %s",
                                        beforeCreation,
                                        createdDate), beforeCreation.before(createdDate));
        assertTrue(String.format("Creation date is too late. Expected : before %s  Found : %s", afterCreation, createdDate),
                          afterCreation.after(createdDate));
    }

    @Test
    public void testLastUpdatedDateIsImmutable() throws Exception
    {
        final Preference p1 = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                "X-testType",
                "propName1",
                null,
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Collections.<String, Object>emptyMap()));
        Date lastUpdatedDate = p1.getLastUpdatedDate();
        lastUpdatedDate.setTime(0);
        Date lastUpdatedDate2 = p1.getLastUpdatedDate();
        assertTrue("Creation date is not immutable.", lastUpdatedDate2.getTime() != 0);
    }

    private void updateOrAppendAs(final Subject testSubject, final Preference... testUserPreference)
    {
        Subject.doAs(testSubject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                awaitPreferenceFuture(_testObject.getUserPreferences().updateOrAppend(Arrays.asList(testUserPreference)));
                return null;
            }
        });
    }

    private void assertPreferences(final Subject subject, final Preference... expectedPreferences)
    {
        Subject.doAs(subject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                Collection<Preference> retrievedPreferences =
                        awaitPreferenceFuture(_testObject.getUserPreferences().getPreferences());
                assertEquals("Unexpected number of preferences",
                                    (long) expectedPreferences.length,
                                    (long) retrievedPreferences.size());
                Map<UUID, Preference> retrievedPreferencesMap = new HashMap<>(retrievedPreferences.size());
                for (Preference retrievedPreference : retrievedPreferences)
                {
                    retrievedPreferencesMap.put(retrievedPreference.getId(), retrievedPreference);
                }
                for (Preference expectedPreference : expectedPreferences)
                {
                    Preference retrievedPreference = retrievedPreferencesMap.get(expectedPreference.getId());
                    assertNotNull("Expected id '" + expectedPreference.getId() + "' not found",
                                         retrievedPreference);

                    assertEquals("Unexpected name", expectedPreference.getName(), retrievedPreference.getName());
                    assertEquals("Unexpected type", expectedPreference.getType(), retrievedPreference.getType());
                    assertEquals("Unexpected description",
                                        expectedPreference.getDescription(),
                                        retrievedPreference.getDescription());
                }
                return null;
            }
        });
    }
}
