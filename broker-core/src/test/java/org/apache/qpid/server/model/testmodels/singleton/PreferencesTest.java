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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.security.auth.Subject;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

    @BeforeEach
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception
    {
        final String objectName = getTestName();
        _testObject = _model.getObjectFactory()
                .create(TestSingleton.class, Map.of(ConfiguredObject.NAME, objectName), null);

        _preferenceTaskExecutor = new CurrentThreadTaskExecutor();
        _preferenceTaskExecutor.start();
        PreferenceStore preferenceStore = mock(PreferenceStore.class);
        _testObject.setUserPreferences(new UserPreferencesImpl(_preferenceTaskExecutor, _testObject, preferenceStore,
                Set.of()));
        _testSubject = TestPrincipalUtils.createTestSubject(TEST_USERNAME);
    }

    @AfterEach
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
                Map.of()));

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
                Map.of()));

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
                        Map.of()));

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
                        Map.of()));

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
                Map.of()));

        final Preference p2 = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                p1.getId(),
                "X-testType",
                "newPropName",
                "newDescription",
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Map.of()));

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
                Map.of()));

        updateOrAppendAs(_testSubject, p1);

        final Preference p2 = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                p1.getId(),
                "X-differentTestType",
                "propName",
                null,
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Map.of()));

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
                Map.of()));

        updateOrAppendAs(_testSubject, p1);

        final Preference p2 = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                "X-testType",
                "propName",
                null,
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Map.of()));

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
                Map.of()));
        final Preference p2 = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                "X-testType",
                "propName",
                null,
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Map.of()));

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
    public void testProhibitPreferenceStealing()
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
                Set.of(TestPrincipalUtils.getTestPrincipalSerialization(testGroupName)),
                Map.of());
        final Preference originalPreference = PreferenceFactory.fromAttributes(_testObject, preferenceAttributes);
        updateOrAppendAs(user1Subject, originalPreference);


        Subject user2Subject = TestPrincipalUtils.createTestSubject(TEST_USERNAME2, testGroupName);
        Subject.doAs(user2Subject, (PrivilegedAction<Void>) () ->
        {
            final CompletableFuture<Set<Preference>>
                    visiblePreferencesFuture = _testObject.getUserPreferences().getVisiblePreferences();
            final Set<Preference> visiblePreferences = awaitPreferenceFuture(visiblePreferencesFuture);

            assertEquals(1, (long) visiblePreferences
                    .size(), "Unexpected number of visible preferences");

            final Map<String, Object> replacementAttributes =
                    Map.of(Preference.OWNER_ATTRIBUTE, TEST_PRINCIPAL2_SERIALIZATION);

            try
            {
                awaitPreferenceFuture(_testObject.getUserPreferences().updateOrAppend(List.of(PreferenceFactory.fromAttributes(
                        _testObject,
                        replacementAttributes))));
                fail("The stealing of a preference must be prohibited");
            }
            catch (IllegalArgumentException e)
            {
                // pass
            }

            return null;
        });

        assertPreferences(user1Subject, originalPreference);
    }

    @Test
    public void testProhibitDuplicateId()
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
                        Map.of()));

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
                        Map.of()));
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
            Subject.doAs(user2Subject, (PrivilegedAction<Void>) () ->
            {
                awaitPreferenceFuture(_testObject.getUserPreferences().replace(List.of(testUserPreference2)));
                return null;
            });
            fail("duplicate id should be prohibited");
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }

        try
        {
            Subject.doAs(user2Subject, (PrivilegedAction<Void>) () ->
            {
                awaitPreferenceFuture(_testObject.getUserPreferences().replaceByType(testUserPreference.getType(),
                                                                                     List.of(testUserPreference2)));
                return null;
            });
            fail("duplicate id should be prohibited");
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }


        try
        {
            Subject.doAs(user2Subject, (PrivilegedAction<Void>) () ->
            {
                awaitPreferenceFuture(_testObject.getUserPreferences().replaceByTypeAndName(testUserPreference.getType(), testUserPreference.getName(), testUserPreference2));
                return null;
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
                        Map.of()));
        updateOrAppendAs(testSubject2, unaffectedPreference);

        final Preference p1 = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                preferenceType,
                "propName",
                null,
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Map.of()));

        updateOrAppendAs(_testSubject, p1);

        final Preference p2 = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                preferenceType,
                "newPropName",
                null,
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Map.of()));

        Subject.doAs(_testSubject, (PrivilegedAction<Void>) () ->
        {
            awaitPreferenceFuture(_testObject.getUserPreferences().replace(Set.of(p2)));
            return null;
        });

        assertPreferences(_testSubject, p2);
        assertPreferences(testSubject2, unaffectedPreference);
    }

    @Test
    public void testDeleteAll()
    {
        final Preference p1 = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                "X-type-1",
                "propName",
                null,
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Map.of()));
        final Preference p2 = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                "X-type-2",
                "propName",
                null,
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Map.of()));
        updateOrAppendAs(_testSubject, p1, p2);

        Subject.doAs(_testSubject, (PrivilegedAction<Void>) () ->
        {
            awaitPreferenceFuture(_testObject.getUserPreferences().delete(null, null, null));
            Set<Preference> result = awaitPreferenceFuture(_testObject.getUserPreferences().getPreferences());
            assertEquals(0, (long) result.size(), "Unexpected number of preferences");
            return null;
        });
    }

    @Test
    public void testDeleteByType()
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
                        Map.of()));
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
                        Map.of()));
        updateOrAppendAs(_testSubject, deletePreference, unaffectedPreference);

        Subject.doAs(_testSubject, (PrivilegedAction<Void>) () ->
        {
            awaitPreferenceFuture(_testObject.getUserPreferences().delete(deleteType, null, null));
            return null;
        });
        assertPreferences(_testSubject, unaffectedPreference);
    }

    @Test
    public void testDeleteByTypeAndName()
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
                        Map.of()));
        final Preference unaffectedPreference1 =
                PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                        null,
                        null,
                        deleteType,
                        "propName2",
                        null,
                        TEST_PRINCIPAL_SERIALIZATION,
                        null,
                        Map.of()));
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
                        Map.of()));
        updateOrAppendAs(_testSubject, deletePreference, unaffectedPreference1, unaffectedPreference2);

        Subject.doAs(_testSubject, (PrivilegedAction<Void>) () ->
        {
            awaitPreferenceFuture(_testObject.getUserPreferences().delete(deleteType, deletePropertyName, null));
            return null;
        });
        assertPreferences(_testSubject, unaffectedPreference1, unaffectedPreference2);
    }

    @Test
    public void testDeleteById()
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
                        Map.of()));
        final Preference unaffectedPreference1 =
                PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                        null,
                        null,
                        deleteType,
                        "propName2",
                        null,
                        TEST_PRINCIPAL_SERIALIZATION,
                        null,
                        Map.of()));
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
                        Map.of()));
        updateOrAppendAs(_testSubject, deletePreference, unaffectedPreference1, unaffectedPreference2);

        Subject.doAs(_testSubject, (PrivilegedAction<Void>) () ->
        {
            awaitPreferenceFuture(_testObject.getUserPreferences().delete(null, null, deletePreference.getId()));
            return null;
        });
        assertPreferences(_testSubject, unaffectedPreference1, unaffectedPreference2);
    }

    @Test
    public void testDeleteByTypeAndId()
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
                        Map.of()));
        final Preference unaffectedPreference1 =
                PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                        null,
                        null,
                        deleteType,
                        "propName2",
                        null,
                        TEST_PRINCIPAL_SERIALIZATION,
                        null,
                        Map.of()));
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
                        Map.of()));
        updateOrAppendAs(_testSubject, deletePreference, unaffectedPreference1, unaffectedPreference2);

        Subject.doAs(_testSubject, (PrivilegedAction<Void>) () ->
        {
            awaitPreferenceFuture(_testObject.getUserPreferences().delete(deleteType, null, deletePreference.getId()));
            return null;
        });
        assertPreferences(_testSubject, unaffectedPreference1, unaffectedPreference2);
    }

    @Test
    public void testDeleteByTypeAndNameAndId()
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
                        Map.of()));
        final Preference unaffectedPreference1 =
                PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                        null,
                        null,
                        deleteType,
                        "propName2",
                        null,
                        TEST_PRINCIPAL_SERIALIZATION,
                        null,
                        Map.of()));
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
                        Map.of()));
        updateOrAppendAs(_testSubject, deletePreference, unaffectedPreference1, unaffectedPreference2);

        Subject.doAs(_testSubject, (PrivilegedAction<Void>) () ->
        {
            awaitPreferenceFuture(_testObject.getUserPreferences().delete(deleteType, deletePropertyName, deletePreference.getId()));
            return null;
        });
        assertPreferences(_testSubject, unaffectedPreference1, unaffectedPreference2);
    }

    @Test
    public void testDeleteByNameWithoutType()
    {
        Subject.doAs(_testSubject, (PrivilegedAction<Void>) () ->
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
                        Map.of()));
        updateOrAppendAs(testSubject2, unaffectedPreference);

        final Preference p1 = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                preferenceType,
                "propName",
                null,
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Map.of()));
        updateOrAppendAs(_testSubject, p1);

        Subject.doAs(_testSubject, (PrivilegedAction<Void>) () ->
        {
            awaitPreferenceFuture(_testObject.getUserPreferences().replace(Set.of()));
            return null;
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
                        Map.of()));
        updateOrAppendAs(testSubject2, unaffectedPreference);

        final Preference p1 = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                preferenceType,
                "propName",
                null,
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Map.of()));
        final Preference p2 = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                unaffectedPreferenceType,
                "propName",
                null,
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Map.of()));

        updateOrAppendAs(_testSubject, p1, p2);
        Subject.doAs(_testSubject, (PrivilegedAction<Void>) () ->
        {
            awaitPreferenceFuture(_testObject.getUserPreferences().replaceByType(preferenceType, Set.of()));
            return null;
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
                        Map.of()));

        updateOrAppendAs(testSubject2, unaffectedPreference);

        final Preference p1 = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                preferenceType,
                "propName",
                null,
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Map.of()));

        final Preference p2 = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                preferenceType,
                "unaffectedPropName",
                null,
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Map.of()));
        updateOrAppendAs(_testSubject, p1, p2);

        Subject.doAs(_testSubject, (PrivilegedAction<Void>) () ->
        {
            awaitPreferenceFuture(_testObject.getUserPreferences().replaceByTypeAndName(preferenceType, "propName", null));
            return null;
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
                        Map.of()));

        updateOrAppendAs(testSubject2, unaffectedPreference);

        final Preference p1 = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                replaceType,
                "propName",
                null,
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Map.of()));
        final Preference p2 = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                unaffectedType,
                "propName",
                null,
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Map.of()));
        final Preference p3 = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                replaceType,
                "newPropName",
                null,
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Map.of()));
        updateOrAppendAs(_testSubject, p1, p2);

        Subject.doAs(_testSubject, (PrivilegedAction<Void>) () ->
        {
            awaitPreferenceFuture(_testObject.getUserPreferences().replaceByType(replaceType, Set.of(p3)));
            return null;
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
                        Map.of()));

        updateOrAppendAs(testSubject2, unaffectedPreference);

        final Preference p1 = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                replaceType,
                "propName",
                null,
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Map.of()));
        final Preference p1b = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                replaceType,
                "unaffectedPropName",
                null,
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Map.of()));

        final Preference p2 = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                unaffectedType,
                "propName",
                null,
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Map.of()));

        final Preference p3 = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                replaceType,
                "propName",
                "new description",
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Map.of()));
        updateOrAppendAs(_testSubject, p1, p1b, p2);

        Subject.doAs(_testSubject, (PrivilegedAction<Void>) () ->
        {
            awaitPreferenceFuture(_testObject.getUserPreferences().replaceByTypeAndName(replaceType, "propName", p3));
            return null;
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
                Set.of(TestPrincipalUtils.getTestPrincipalSerialization(testGroupName)),
                Map.of()));

        final Preference notSharedPreference = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                "X-testType",
                "propName2",
                null,
                TEST_PRINCIPAL2_SERIALIZATION,
                null,
                Map.of()));

        updateOrAppendAs(sharer, sharedPreference, notSharedPreference);

        final Preference nonSharersPrivatePref = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                "X-testType",
                "propName",
                null,
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Map.of()));

        updateOrAppendAs(nonSharer, nonSharersPrivatePref);

        Subject.doAs(nonSharer, (PrivilegedAction<Void>) () ->
        {
            Set<Preference> retrievedPreferences =
                    awaitPreferenceFuture(_testObject.getUserPreferences().getVisiblePreferences());
            assertEquals(2, (long) retrievedPreferences.size(), "Unexpected number of preferences");

            Set<UUID> visibleIds = new HashSet<>(retrievedPreferences.size());
            for (Preference preference : retrievedPreferences)
            {
                visibleIds.add(preference.getId());
            }
            assertTrue(visibleIds.contains(nonSharersPrivatePref.getId()), "Owned preference not visible");
            assertTrue(visibleIds.contains(sharedPreference.getId()), "Shared preference not visible");

            return null;
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
                        Set.of(TestPrincipalUtils.getTestPrincipalSerialization(testGroupName)),
                        Map.of()));

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
                        Map.of()));
        updateOrAppendAs(testSubjectWithGroup, testUserPreference);

        Subject.doAs(testSubjectWithGroup, (PrivilegedAction<Void>) () ->
        {
            Set<Preference> retrievedPreferences =
                    awaitPreferenceFuture(_testObject.getUserPreferences().getVisiblePreferences());
            assertEquals(2, (long) retrievedPreferences.size(), "Unexpected number of preferences");
            assertTrue(retrievedPreferences.contains(sharedPreference),
                       "Preference of my peer did not exist in visible set");
            assertTrue(retrievedPreferences.contains(testUserPreference), "My preference did not exist in visible set");
            return null;
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
                Map.of()));
        Thread.sleep(1);
        Date afterUpdate = new Date();
        Date lastUpdatedDate = p1.getLastUpdatedDate();
        assertTrue(beforeUpdate.before(lastUpdatedDate),
                String.format("LastUpdated date is too early. Expected : after %s  Found : %s", beforeUpdate, lastUpdatedDate));
        assertTrue(afterUpdate.after(lastUpdatedDate),
                String.format("LastUpdated date is too late. Expected : before %s  Found : %s", afterUpdate, lastUpdatedDate));
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
                Map.of()));
        Thread.sleep(1);
        Date afterCreation = new Date();
        Date createdDate = p1.getCreatedDate();
        assertTrue(beforeCreation.before(createdDate),
                String.format("Creation date is too early. Expected : after %s  Found : %s", beforeCreation, createdDate));
        assertTrue(afterCreation.after(createdDate),
                String.format("Creation date is too late. Expected : before %s  Found : %s", afterCreation, createdDate));
    }

    @Test
    public void testLastUpdatedDateIsImmutable()
    {
        final Preference p1 = PreferenceFactory.fromAttributes(_testObject, PreferenceTestHelper.createPreferenceAttributes(
                null,
                null,
                "X-testType",
                "propName1",
                null,
                TEST_PRINCIPAL_SERIALIZATION,
                null,
                Map.of()));
        Date lastUpdatedDate = p1.getLastUpdatedDate();
        lastUpdatedDate.setTime(0);
        Date lastUpdatedDate2 = p1.getLastUpdatedDate();
        assertTrue(lastUpdatedDate2.getTime() != 0, "Creation date is not immutable.");
    }

    private void updateOrAppendAs(final Subject testSubject, final Preference... testUserPreference)
    {
        Subject.doAs(testSubject, (PrivilegedAction<Void>) () ->
        {
            awaitPreferenceFuture(_testObject.getUserPreferences().updateOrAppend(Arrays.asList(testUserPreference)));
            return null;
        });
    }

    private void assertPreferences(final Subject subject, final Preference... expectedPreferences)
    {
        Subject.doAs(subject, (PrivilegedAction<Void>) () ->
        {
            final Collection<Preference> retrievedPreferences =
                    awaitPreferenceFuture(_testObject.getUserPreferences().getPreferences());
            assertEquals(expectedPreferences.length, (long) retrievedPreferences.size(), "Unexpected number of preferences");
            final Map<UUID, Preference> retrievedPreferencesMap = retrievedPreferences.stream()
                    .collect(Collectors.toMap(Preference::getId, Function.identity()));

            for (final Preference expectedPreference : expectedPreferences)
            {
                final Preference retrievedPreference = retrievedPreferencesMap.get(expectedPreference.getId());
                assertNotNull(retrievedPreference, "Expected id '" + expectedPreference.getId() + "' not found");

                assertEquals(expectedPreference.getName(), retrievedPreference.getName(), "Unexpected name");
                assertEquals(expectedPreference.getType(), retrievedPreference.getType(), "Unexpected type");
                assertEquals(expectedPreference.getDescription(), retrievedPreference.getDescription(), "Unexpected description");
            }
            return null;
        });
    }
}
