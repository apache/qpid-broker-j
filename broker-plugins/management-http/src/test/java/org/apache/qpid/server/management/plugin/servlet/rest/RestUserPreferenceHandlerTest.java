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
package org.apache.qpid.server.management.plugin.servlet.rest;

import static org.apache.qpid.server.management.plugin.HttpManagementConfiguration.DEFAULT_PREFERENCE_OPERATION_TIMEOUT;
import static org.apache.qpid.server.model.preferences.PreferenceTestHelper.awaitPreferenceFuture;
import static org.apache.qpid.server.model.preferences.PreferenceTestHelper.createPreferenceAttributes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.security.auth.Subject;

import com.google.common.collect.Lists;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.preferences.GenericPrincipal;
import org.apache.qpid.server.model.preferences.Preference;
import org.apache.qpid.server.model.preferences.PreferenceFactory;
import org.apache.qpid.server.model.preferences.UserPreferences;
import org.apache.qpid.server.model.preferences.UserPreferencesImpl;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;
import org.apache.qpid.server.security.auth.TestPrincipalUtils;
import org.apache.qpid.server.security.group.GroupPrincipal;
import org.apache.qpid.server.store.preferences.PreferenceStore;
import org.apache.qpid.test.utils.UnitTestBase;

public class RestUserPreferenceHandlerTest extends UnitTestBase
{
    private static final String MYGROUP = "mygroup";
    private static final String MYGROUP_SERIALIZATION = TestPrincipalUtils.getTestPrincipalSerialization(MYGROUP);
    private static final String MYUSER = "myuser";
    private static final String MYUSER_SERIALIZATION = TestPrincipalUtils.getTestPrincipalSerialization(MYUSER);

    private final RestUserPreferenceHandler _handler = new RestUserPreferenceHandler(DEFAULT_PREFERENCE_OPERATION_TIMEOUT);
    private ConfiguredObject<?> _configuredObject;
    private UserPreferences _userPreferences;
    private Subject _subject;
    private Principal _userPrincipal;
    private GroupPrincipal _groupPrincipal;
    private TaskExecutor _preferenceTaskExecutor;

    @BeforeEach
    public void setUp() throws Exception
    {
        _configuredObject = mock(ConfiguredObject.class);
        PreferenceStore preferenceStore = mock(PreferenceStore.class);
        _preferenceTaskExecutor = new CurrentThreadTaskExecutor();
        _preferenceTaskExecutor.start();
        _userPreferences = new UserPreferencesImpl(_preferenceTaskExecutor,
                                                   _configuredObject,
                                                   preferenceStore,
                                                   Collections.emptyList());
        _subject = TestPrincipalUtils.createTestSubject(MYUSER, MYGROUP);
        _groupPrincipal = _subject.getPrincipals(GroupPrincipal.class).iterator().next();
        _userPrincipal = _subject.getPrincipals(AuthenticatedPrincipal.class).iterator().next();
        when(_configuredObject.getUserPreferences()).thenReturn(_userPreferences);
    }

    @AfterEach
    public void tearDown()
    {
        _preferenceTaskExecutor.stop();
    }

    @Test
    public void testPutWithVisibilityList_ValidGroup()
    {

        final RequestInfo requestInfo = RequestInfo.createPreferencesRequestInfo(Collections.<String>emptyList(),
                                                                                 Arrays.asList("X-testtype",
                                                                                               "myprefname")
                                                                                );

        final Map<String, Object> pref = new HashMap<>();
        pref.put(Preference.VALUE_ATTRIBUTE, Collections.emptyMap());
        pref.put(Preference.VISIBILITY_LIST_ATTRIBUTE, Collections.singletonList(MYGROUP_SERIALIZATION));

        Subject.doAs(_subject, (PrivilegedAction<Void>) () ->
        {
            _handler.handlePUT(_configuredObject, requestInfo, pref);

            Set<Preference> preferences = awaitPreferenceFuture(_userPreferences.getPreferences());
            assertEquals(1, (long) preferences.size(), "Unexpected number of preferences");
            Preference prefModel = preferences.iterator().next();
            final Set<Principal> visibilityList = prefModel.getVisibilityList();
            assertEquals(1, (long) visibilityList.size(), "Unexpected number of principals in visibility list");
            Principal principal = visibilityList.iterator().next();
            assertEquals(MYGROUP, principal.getName(), "Unexpected member of visibility list");
            return null;
        });
    }

    @Test
    public void testPutWithVisibilityList_InvalidGroup()
    {

        final RequestInfo requestInfo = RequestInfo.createPreferencesRequestInfo(
                Collections.emptyList(), Arrays.asList("X-testtype", "myprefname"));

        final Map<String, Object> pref = new HashMap<>();
        pref.put(Preference.VALUE_ATTRIBUTE, Collections.emptyMap());
        pref.put(Preference.VISIBILITY_LIST_ATTRIBUTE, Collections.singletonList("Invalid Group"));

        Subject.doAs(_subject, (PrivilegedAction<Void>) () ->
        {
            try
            {
                _handler.handlePUT(_configuredObject, requestInfo, pref);
                fail("Expected exception not thrown");
            }
            catch (IllegalArgumentException e)
            {
                // pass
            }
            return null;
        });
    }

    @Test
    public void testPutByTypeAndName()
    {
        final String prefName = "myprefname";
        final RequestInfo requestInfo = RequestInfo.createPreferencesRequestInfo(
                Collections.emptyList(), Arrays.asList("X-testtype", prefName));

        final Map<String, Object> pref = new HashMap<>();
        pref.put(Preference.VALUE_ATTRIBUTE, Collections.emptyMap());

        Subject.doAs(_subject, (PrivilegedAction<Void>) () ->
        {
            _handler.handlePUT(_configuredObject, requestInfo, pref);

            Set<Preference> preferences = awaitPreferenceFuture(_userPreferences.getPreferences());
            assertEquals(1, (long) preferences.size(), "Unexpected number of preferences");
            Preference prefModel = preferences.iterator().next();
            assertEquals(prefName, prefModel.getName(), "Unexpected preference name");
            return null;
        });
    }

    @Test
    public void testReplaceViaPutByTypeAndName()
    {
        final String prefName = "myprefname";
        final RequestInfo requestInfo = RequestInfo.createPreferencesRequestInfo(
                Collections.emptyList(), Arrays.asList("X-testtype", prefName));

        final Map<String, Object> pref = new HashMap<>();
        pref.put(Preference.VALUE_ATTRIBUTE, Collections.emptyMap());

        final Preference createdPreference = Subject.doAs(_subject, (PrivilegedAction<Preference>) () ->
        {
            _handler.handlePUT(_configuredObject, requestInfo, pref);

            Set<Preference> preferences = awaitPreferenceFuture
                    (_userPreferences.getPreferences());
            assertEquals(1, (long) preferences.size(), "Unexpected number of preferences");
            Preference prefModel = preferences.iterator().next();
            assertEquals(prefName, prefModel.getName(), "Unexpected preference name");
            return prefModel;
        });

        final Map<String, Object> replacementPref = new HashMap<>();
        replacementPref.put(Preference.ID_ATTRIBUTE, createdPreference.getId().toString());
        replacementPref.put(Preference.VALUE_ATTRIBUTE, Collections.emptyMap());
        final String changedDescription = "Replace that maintains id";
        replacementPref.put(Preference.DESCRIPTION_ATTRIBUTE, changedDescription);

        Subject.doAs(_subject, (PrivilegedAction<Void>) () ->
        {
            _handler.handlePUT(_configuredObject, requestInfo, replacementPref);

            Set<Preference> preferences = awaitPreferenceFuture(_userPreferences.getPreferences());
            assertEquals(1, (long) preferences.size(), "Unexpected number of preferences after update");
            Preference updatedPref = preferences.iterator().next();
            assertEquals(createdPreference.getId(), updatedPref.getId(), "Unexpected preference id");
            assertEquals(prefName, updatedPref.getName(), "Unexpected preference name");
            assertEquals(changedDescription, updatedPref.getDescription(), "Unexpected preference description");
            return null;
        });

        replacementPref.remove(Preference.ID_ATTRIBUTE);
        final String changedDescription2 = "Replace that omits id";
        replacementPref.put(Preference.DESCRIPTION_ATTRIBUTE, changedDescription2);

        Subject.doAs(_subject, (PrivilegedAction<Void>) () ->
        {
            _handler.handlePUT(_configuredObject, requestInfo, replacementPref);

            Set<Preference> preferences = awaitPreferenceFuture(_userPreferences.getPreferences());
            assertEquals(1, (long) preferences.size(), "Unexpected number of preferences after update");
            Preference updatedPref = preferences.iterator().next();
            assertNotEquals(createdPreference.getId(), updatedPref.getId(), "Replace without id should create new id");

            assertEquals(prefName, updatedPref.getName(), "Unexpected preference name");
            assertEquals(changedDescription2, updatedPref.getDescription(), "Unexpected preference description");
            return null;
        });
    }

    @Test
    public void testReplaceViaPutByType()
    {
        final String prefName = "myprefname";
        final RequestInfo requestInfo = RequestInfo.createPreferencesRequestInfo(
                Collections.emptyList(), Collections.singletonList("X-testtype"));

        final Map<String, Object> pref = new HashMap<>();
        pref.put(Preference.NAME_ATTRIBUTE, prefName);
        pref.put(Preference.VALUE_ATTRIBUTE, Collections.emptyMap());

        Subject.doAs(_subject, (PrivilegedAction<Void>) () ->
        {
            _handler.handlePUT(_configuredObject, requestInfo, Lists.newArrayList(pref));

            Set<Preference> preferences = awaitPreferenceFuture(_userPreferences.getPreferences());
            assertEquals(1, (long) preferences.size(), "Unexpected number of preferences");
            Preference prefModel = preferences.iterator().next();
            assertEquals(prefName, prefModel.getName(), "Unexpected preference name");
            return null;
        });

        final String replacementPref1Name = "myprefreplacement1";
        final String replacementPref2Name = "myprefreplacement2";

        final Map<String, Object> replacementPref1 = new HashMap<>();
        replacementPref1.put(Preference.NAME_ATTRIBUTE, replacementPref1Name);
        replacementPref1.put(Preference.VALUE_ATTRIBUTE, Collections.emptyMap());

        final Map<String, Object> replacementPref2 = new HashMap<>();
        replacementPref2.put(Preference.NAME_ATTRIBUTE, replacementPref2Name);
        replacementPref2.put(Preference.VALUE_ATTRIBUTE, Collections.emptyMap());

        Subject.doAs(_subject, (PrivilegedAction<Void>) () ->
        {
            _handler.handlePUT(_configuredObject, requestInfo, Lists.newArrayList(replacementPref1, replacementPref2));

            Set<Preference> preferences = awaitPreferenceFuture(_userPreferences.getPreferences());
            assertEquals(2, (long) preferences.size(), "Unexpected number of preferences after update");
            Set<String> prefNames = new HashSet<>(preferences.size());
            for (Preference pref1 : preferences)
            {
                prefNames.add(pref1.getName());
            }
            assertTrue(prefNames.contains(replacementPref1Name),
                    "Replacement preference " + replacementPref1Name + " not found.");

            assertTrue(prefNames.contains(replacementPref2Name),
                    "Replacement preference " + replacementPref2Name + " not found.");
            return null;
        });
    }

    @Test
    public void testReplaceAllViaPut()
    {
        final String pref1Name = "mypref1name";
        final String pref1Type = "X-testtype1";
        final String pref2Name = "mypref2name";
        final String pref2Type = "X-testtype2";

        final RequestInfo requestInfo = RequestInfo.createPreferencesRequestInfo(
                Collections.emptyList(), Collections.emptyList());

        final Map<String, Object> pref1 = new HashMap<>();
        pref1.put(Preference.NAME_ATTRIBUTE, pref1Name);
        pref1.put(Preference.VALUE_ATTRIBUTE, Collections.emptyMap());
        pref1.put(Preference.TYPE_ATTRIBUTE, pref1Type);

        final Map<String, Object> pref2 = new HashMap<>();
        pref2.put(Preference.NAME_ATTRIBUTE, pref2Name);
        pref2.put(Preference.VALUE_ATTRIBUTE, Collections.emptyMap());
        pref2.put(Preference.TYPE_ATTRIBUTE, pref2Type);

        final Map<String, List<Map<String, Object>>> payload = new HashMap<>();
        payload.put(pref1Type, Collections.singletonList(pref1));
        payload.put(pref2Type, Collections.singletonList(pref2));

        Subject.doAs(_subject, (PrivilegedAction<Void>) () ->
        {
            _handler.handlePUT(_configuredObject, requestInfo, payload);

            Set<Preference> preferences = awaitPreferenceFuture(_userPreferences.getPreferences());
            assertEquals(2, (long) preferences.size(), "Unexpected number of preferences");
            return null;
        });

        final String replacementPref1Name = "myprefreplacement1";

        final Map<String, Object> replacementPref1 = new HashMap<>();
        replacementPref1.put(Preference.NAME_ATTRIBUTE, replacementPref1Name);
        replacementPref1.put(Preference.VALUE_ATTRIBUTE, Collections.emptyMap());
        replacementPref1.put(Preference.TYPE_ATTRIBUTE, pref1Type);

        payload.clear();
        payload.put(pref1Type, Collections.singletonList(replacementPref1));

        Subject.doAs(_subject, (PrivilegedAction<Void>) () ->
        {
            _handler.handlePUT(_configuredObject, requestInfo, payload);

            Set<Preference> preferences = awaitPreferenceFuture(_userPreferences.getPreferences());
            assertEquals(1, (long) preferences.size(), "Unexpected number of preferences after update");
            Preference prefModel = preferences.iterator().next();

            assertEquals(replacementPref1Name, prefModel.getName(), "Unexpected preference name");
            return null;
        });
    }

    @Test
    public void testPostToTypeWithVisibilityList_ValidGroup()
    {
        final RequestInfo typeRequestInfo = RequestInfo.createPreferencesRequestInfo(
                Collections.emptyList(), Collections.singletonList("X-testtype"));

        final Map<String, Object> pref = new HashMap<>();
        pref.put(Preference.NAME_ATTRIBUTE, "testPref");
        pref.put(Preference.VALUE_ATTRIBUTE, Collections.emptyMap());
        pref.put(Preference.VISIBILITY_LIST_ATTRIBUTE, Collections.singletonList(MYGROUP_SERIALIZATION));

        Subject.doAs(_subject, (PrivilegedAction<Void>) () ->
        {
            _handler.handlePOST(_configuredObject, typeRequestInfo, Collections.singletonList(pref));

            Set<Preference> preferences = awaitPreferenceFuture(_userPreferences.getPreferences());
            assertEquals(1, (long) preferences.size(), "Unexpected number of preferences");
            Preference prefModel = preferences.iterator().next();
            final Set<Principal> visibilityList = prefModel.getVisibilityList();
            assertEquals(1, (long) visibilityList.size(), "Unexpected number of principals in visibility list");
            Principal principal = visibilityList.iterator().next();
            assertEquals(MYGROUP, principal.getName(), "Unexpected member of visibility list");
            return null;
        });
    }

    @Test
    public void testPostToRootWithVisibilityList_ValidGroup()
    {
        final RequestInfo rootRequestInfo = RequestInfo.createPreferencesRequestInfo(
                Collections.emptyList(), Collections.emptyList());

        final Map<String, Object> pref = new HashMap<>();
        pref.put(Preference.NAME_ATTRIBUTE, "testPref");
        pref.put(Preference.VALUE_ATTRIBUTE, Collections.emptyMap());
        pref.put(Preference.VISIBILITY_LIST_ATTRIBUTE, Collections.singletonList(MYGROUP_SERIALIZATION));

        Subject.doAs(_subject, (PrivilegedAction<Void>) () ->
        {
            final Map<String, List<Map<String, Object>>> payload =
                    Collections.singletonMap("X-testtype2", Collections.singletonList(pref));
            _handler.handlePOST(_configuredObject, rootRequestInfo, payload);

            Set<Preference> preferences = awaitPreferenceFuture(_userPreferences.getPreferences());
            assertEquals(1, (long) preferences.size(), "Unexpected number of preferences");
            Preference prefModel = preferences.iterator().next();
            final Set<Principal> visibilityList = prefModel.getVisibilityList();
            assertEquals(1, (long) visibilityList.size(), "Unexpected number of principals in visibility list");
            Principal principal = visibilityList.iterator().next();
            assertEquals(MYGROUP, principal.getName(), "Unexpected member of visibility list");

            return null;
        });
    }

    @Test
    public void testPostToTypeWithVisibilityList_InvalidGroup()
    {
        final RequestInfo requestInfo = RequestInfo.createPreferencesRequestInfo(
                Collections.emptyList(), Collections.singletonList("X-testtype"));

        final Map<String, Object> pref = new HashMap<>();
        pref.put(Preference.NAME_ATTRIBUTE, "testPref");
        pref.put(Preference.VALUE_ATTRIBUTE, Collections.emptyMap());
        pref.put(Preference.VISIBILITY_LIST_ATTRIBUTE, Collections.singletonList("Invalid Group"));

        Subject.doAs(_subject, (PrivilegedAction<Void>) () ->
        {
            try
            {
                _handler.handlePOST(_configuredObject, requestInfo, Collections.singletonList(pref));
                fail("Expected exception not thrown");
            }
            catch (IllegalArgumentException e)
            {
                // pass
            }
            return null;
        });
    }

    @Test
    public void testPostToRootWithVisibilityList_InvalidGroup()
    {
        final RequestInfo requestInfo = RequestInfo.createPreferencesRequestInfo(
                Collections.emptyList(), Collections.emptyList());

        final Map<String, Object> pref = new HashMap<>();
        pref.put(Preference.NAME_ATTRIBUTE, "testPref");
        pref.put(Preference.VALUE_ATTRIBUTE, Collections.emptyMap());
        pref.put(Preference.VISIBILITY_LIST_ATTRIBUTE, Collections.singletonList("Invalid Group"));

        Subject.doAs(_subject, (PrivilegedAction<Void>) () ->
        {
            try
            {
                final Map<String, List<Map<String, Object>>> payload =
                        Collections.singletonMap("X-testType", Collections.singletonList(pref));
                _handler.handlePOST(_configuredObject, requestInfo, payload);
                fail("Expected exception not thrown");
            }
            catch (IllegalArgumentException e)
            {
                // pass
            }
            return null;
        });
    }

    @Test
    public void testGetHasCorrectVisibilityList()
    {
        final RequestInfo rootRequestInfo = RequestInfo.createPreferencesRequestInfo(
                Collections.emptyList(), Collections.emptyList());
        final String type = "X-testtype";

        Subject.doAs(_subject, (PrivilegedAction<Void>) () ->
        {
            Map<String, Object> prefAttributes = createPreferenceAttributes(
                    null,
                    null,
                    type,
                    "testpref",
                    null,
                    MYUSER_SERIALIZATION,
                    Collections.singleton(MYGROUP_SERIALIZATION),
                    Collections.emptyMap());
            Preference preference = PreferenceFactory.fromAttributes(_configuredObject, prefAttributes);
            awaitPreferenceFuture(_userPreferences.updateOrAppend(Collections.singleton(preference)));

            Map<String, List<Map<String, Object>>> typeToPreferenceListMap =
                    (Map<String, List<Map<String, Object>>>) _handler.handleGET(_userPreferences,
                                                                                rootRequestInfo);
            assertEquals(1, (long) typeToPreferenceListMap.size(), "Unexpected preference map size");
            assertEquals(type, typeToPreferenceListMap.keySet().iterator().next(), "Unexpected type in preference map");
            List<Map<String, Object>> preferences = typeToPreferenceListMap.get(type);
            assertEquals(1, (long) preferences.size(), "Unexpected number of preferences");
            Set<Principal> visibilityList = (Set<Principal>) preferences.get(0).get("visibilityList");
            assertEquals(1, (long) visibilityList.size(), "Unexpected number of principals in visibility list");
            assertTrue(GenericPrincipal.principalsEqual(_groupPrincipal, visibilityList.iterator().next()),
                    "Unexpected principal in visibility list");
            return null;
        });
    }

    @Test
    public void testGetById()
    {
        Subject.doAs(_subject, (PrivilegedAction<Void>) () ->
        {
            final String type = "X-testtype";
            Map<String, Object> pref1Attributes = createPreferenceAttributes(
                    null,
                    null,
                    type,
                    "testpref",
                    null,
                    MYUSER_SERIALIZATION,
                    null,
                    Collections.emptyMap());
            Preference p1 = PreferenceFactory.fromAttributes(_configuredObject, pref1Attributes);
            Map<String, Object> pref2Attributes = createPreferenceAttributes(
                    null,
                    null,
                    type,
                    "testpref2",
                    null,
                    MYUSER_SERIALIZATION,
                    null,
                    Collections.emptyMap());
            Preference p2 = PreferenceFactory.fromAttributes(_configuredObject, pref2Attributes);
            awaitPreferenceFuture(_userPreferences.updateOrAppend(Arrays.asList(p1, p2)));
            UUID id = p1.getId();

            final RequestInfo rootRequestInfo = RequestInfo.createPreferencesRequestInfo(
                    Collections.emptyList(), Collections.emptyList(),
                    Collections.singletonMap("id", Collections.singletonList(id.toString())));

            Map<String, List<Map<String, Object>>> typeToPreferenceListMap =
                    (Map<String, List<Map<String, Object>>>) _handler.handleGET(_userPreferences, rootRequestInfo);
            assertEquals(1, (long) typeToPreferenceListMap.size(), "Unexpected p1 map size");
            assertEquals(type, typeToPreferenceListMap.keySet().iterator().next(), "Unexpected type in p1 map");
            List<Map<String, Object>> preferences = typeToPreferenceListMap.get(type);
            assertEquals(1, (long) preferences.size(), "Unexpected number of preferences");
            assertEquals(id, preferences.get(0).get(Preference.ID_ATTRIBUTE), "Unexpected id");
            return null;
        });
    }

    @Test
    public void testDeleteById()
    {
        Subject.doAs(_subject, (PrivilegedAction<Void>) () ->
        {
            final String type = "X-testtype";
            Map<String, Object> pref1Attributes = createPreferenceAttributes(
                    null,
                    null,
                    type,
                    "testpref",
                    null,
                    MYUSER_SERIALIZATION,
                    null,
                    Collections.emptyMap());
            Preference p1 = PreferenceFactory.fromAttributes(_configuredObject, pref1Attributes);
            Map<String, Object> pref2Attributes = createPreferenceAttributes(
                    null,
                    null,
                    type,
                    "testpref2",
                    null,
                    MYUSER_SERIALIZATION,
                    null,
                    Collections.emptyMap());
            Preference p2 = PreferenceFactory.fromAttributes(_configuredObject, pref2Attributes);
            awaitPreferenceFuture(_userPreferences.updateOrAppend(Arrays.asList(p1, p2)));
            UUID id = p1.getId();

            final RequestInfo rootRequestInfo = RequestInfo.createPreferencesRequestInfo(
                    Collections.emptyList(), Collections.emptyList(),
                    Collections.singletonMap("id", Collections.singletonList(id.toString())));

            _handler.handleDELETE(_userPreferences, rootRequestInfo);

            final Set<Preference> retrievedPreferences = awaitPreferenceFuture(_userPreferences.getPreferences());
            assertEquals(1, (long) retrievedPreferences.size(), "Unexpected number of preferences");
            assertTrue(retrievedPreferences.contains(p2), "Unexpected type in p1 map");
            return null;
        });
    }

    @Test
    public void testDeleteByTypeAndName()
    {
        final String preferenceType = "X-testtype";
        final String preferenceName = "myprefname";
        final RequestInfo requestInfo = RequestInfo.createPreferencesRequestInfo(
                Collections.emptyList(), Arrays.asList(preferenceType, preferenceName));

        final Map<String, Object> pref = new HashMap<>();
        pref.put(Preference.VALUE_ATTRIBUTE, Collections.emptyMap());

        doTestDelete(preferenceType, preferenceName, requestInfo);
    }

    @Test
    public void testDeleteByType()
    {
        final String preferenceType = "X-testtype";
        final String preferenceName = "myprefname";
        final RequestInfo requestInfo = RequestInfo.createPreferencesRequestInfo(
                Collections.emptyList(), Collections.singletonList(preferenceType));

        final Map<String, Object> pref = new HashMap<>();
        pref.put(Preference.VALUE_ATTRIBUTE, Collections.emptyMap());

        doTestDelete(preferenceType, preferenceName, requestInfo);
    }

    @Test
    public void testDeleteByRoot()
    {
        final String preferenceType = "X-testtype";
        final String preferenceName = "myprefname";
        final RequestInfo requestInfo = RequestInfo.createPreferencesRequestInfo(
                Collections.emptyList(), Collections.emptyList());

        final Map<String, Object> pref = new HashMap<>();
        pref.put(Preference.VALUE_ATTRIBUTE, Collections.emptyMap());

        doTestDelete(preferenceType, preferenceName, requestInfo);
    }

    @Test
    public void testGetVisiblePreferencesByRoot()
    {
        final String prefName = "testpref";
        final String prefType = "X-testtype";
        final RequestInfo rootRequestInfo = RequestInfo.createVisiblePreferencesRequestInfo(
                Collections.emptyList(), Collections.emptyList(), Collections.emptyMap());

        Subject.doAs(_subject, (PrivilegedAction<Void>) () ->
        {
            final Set<Preference> preferences = new HashSet<>();
            Map<String, Object> pref1Attributes = createPreferenceAttributes(
                    null,
                    null,
                    prefType,
                    prefName,
                    null,
                    MYUSER_SERIALIZATION,
                    Collections.singleton(MYGROUP_SERIALIZATION),
                    Collections.emptyMap());
            Preference p1 = PreferenceFactory.fromAttributes(_configuredObject, pref1Attributes);
            preferences.add(p1);
            Map<String, Object> pref2Attributes = createPreferenceAttributes(
                    null,
                    null,
                    prefType,
                    "testPref2",
                    null,
                    MYUSER_SERIALIZATION,
                    Collections.emptySet(),
                    Collections.emptyMap());
            Preference p2 = PreferenceFactory.fromAttributes(_configuredObject, pref2Attributes);
            preferences.add(p2);
            awaitPreferenceFuture(_userPreferences.updateOrAppend(preferences));
            return null;
        });

        Subject testSubject2 = TestPrincipalUtils.createTestSubject("testUser2", MYGROUP);
        Subject.doAs(testSubject2, (PrivilegedAction<Void>) () ->
        {
            Map<String, List<Map<String, Object>>> typeToPreferenceListMap =
                    (Map<String, List<Map<String, Object>>>) _handler.handleGET(_userPreferences, rootRequestInfo);
            assertEquals(1, (long) typeToPreferenceListMap.size(), "Unexpected preference map size");
            assertEquals(prefType, typeToPreferenceListMap.keySet().iterator().next(),
                    "Unexpected prefType in preference map");
            List<Map<String, Object>> preferences = typeToPreferenceListMap.get(prefType);
            assertEquals(1, (long) preferences.size(), "Unexpected number of preferences");
            assertEquals(prefName, preferences.get(0).get(Preference.NAME_ATTRIBUTE),
                    "Unexpected name of preferences");
            Set<Principal> visibilityList = (Set<Principal>) preferences.get(0).get(Preference.VISIBILITY_LIST_ATTRIBUTE);
            assertEquals(1, (long) visibilityList.size(),
                    "Unexpected number of principals in visibility list");
            assertTrue(GenericPrincipal.principalsEqual(_groupPrincipal, visibilityList.iterator().next()),
                    "Unexpected principal in visibility list");
            assertTrue(GenericPrincipal.principalsEqual(_userPrincipal, (Principal) preferences.get(0)
                    .get(Preference.OWNER_ATTRIBUTE)),
                    "Unexpected owner");
            return null;
        });
    }

    @Test
    public void testGetVisiblePreferencesByType()
    {
        final String prefName = "testpref";
        final String prefType = "X-testtype";
        final RequestInfo rootRequestInfo = RequestInfo.createVisiblePreferencesRequestInfo(
                Collections.emptyList(), Collections.singletonList(prefType), Collections.emptyMap());

        Subject.doAs(_subject, (PrivilegedAction<Void>) () ->
        {
            final Set<Preference> preferences = new HashSet<>();
            Map<String, Object> pref1Attributes = createPreferenceAttributes(
                    null,
                    null,
                    prefType,
                    prefName,
                    null,
                    MYUSER_SERIALIZATION,
                    Collections.singleton(MYGROUP_SERIALIZATION),
                    Collections.emptyMap());
            Preference p1 = PreferenceFactory.fromAttributes(_configuredObject, pref1Attributes);
            preferences.add(p1);
            Map<String, Object> pref2Attributes = createPreferenceAttributes(
                    null,
                    null,
                    prefType,
                    "testPref2",
                    null,
                    MYUSER_SERIALIZATION,
                    Collections.emptySet(),
                    Collections.emptyMap());
            Preference p2 = PreferenceFactory.fromAttributes(_configuredObject, pref2Attributes);
            preferences.add(p2);
            awaitPreferenceFuture(_userPreferences.updateOrAppend(preferences));
            return null;
        });

        Subject testSubject2 = TestPrincipalUtils.createTestSubject("testUser2", MYGROUP);
        Subject.doAs(testSubject2, (PrivilegedAction<Void>) () ->
        {
            List<Map<String, Object>> preferences =
                    (List<Map<String, Object>>) _handler.handleGET(_userPreferences, rootRequestInfo);
            assertEquals(1, (long) preferences.size(), "Unexpected number of preferences");
            assertEquals(prefName, preferences.get(0).get(Preference.NAME_ATTRIBUTE),
                    "Unexpected name of preferences");
            Set<Principal> visibilityList = (Set<Principal>) preferences.get(0).get(Preference.VISIBILITY_LIST_ATTRIBUTE);
            assertEquals(1, (long) visibilityList.size(),
                    "Unexpected number of principals in visibility list");
            assertTrue(GenericPrincipal.principalsEqual(_groupPrincipal, visibilityList.iterator().next()),
                    "Unexpected principal in visibility list");
            assertTrue(GenericPrincipal.principalsEqual(_userPrincipal, (Principal) preferences.get(0)
                    .get(Preference.OWNER_ATTRIBUTE)),
                    "Unexpected owner");
            return null;
        });
    }

    @Test
    public void testGetVisiblePreferencesByTypeAndName()
    {
        final String prefName = "testpref";
        final String prefType = "X-testtype";
        final RequestInfo rootRequestInfo = RequestInfo.createVisiblePreferencesRequestInfo(
                Collections.emptyList(), Arrays.asList(prefType, prefName), Collections.emptyMap());

        Subject.doAs(_subject, (PrivilegedAction<Void>) () ->
        {
            final Set<Preference> preferences = new HashSet<>();
            Map<String, Object> pref1Attributes = createPreferenceAttributes(
                    null,
                    null,
                    prefType,
                    prefName,
                    null,
                    MYUSER_SERIALIZATION,
                    Collections.singleton(MYGROUP_SERIALIZATION),
                    Collections.emptyMap());
            Preference p1 = PreferenceFactory.fromAttributes(_configuredObject, pref1Attributes);
            preferences.add(p1);
            Map<String, Object> pref2Attributes = createPreferenceAttributes(
                    null,
                    null,
                    prefType,
                    "testPref2",
                    null,
                    MYUSER_SERIALIZATION,
                    Collections.emptySet(),
                    Collections.emptyMap());
            Preference p2 = PreferenceFactory.fromAttributes(_configuredObject, pref2Attributes);
            preferences.add(p2);
            awaitPreferenceFuture(_userPreferences.updateOrAppend(preferences));
            return null;
        });

        Subject testSubject2 = TestPrincipalUtils.createTestSubject("testUser2", MYGROUP);
        Subject.doAs(testSubject2, (PrivilegedAction<Void>) () ->
        {
            Map<String, Object> preference =
                    (Map<String, Object>) _handler.handleGET(_userPreferences, rootRequestInfo);
            assertEquals(prefName, preference.get(Preference.NAME_ATTRIBUTE), "Unexpected name of preferences");
            Set<Principal> visibilityList = (Set<Principal>) preference.get(Preference.VISIBILITY_LIST_ATTRIBUTE);
            assertEquals(1, (long) visibilityList.size(), "Unexpected number of principals in visibility list");
            assertTrue(GenericPrincipal.principalsEqual(_groupPrincipal, visibilityList.iterator().next()),
                    "Unexpected principal in visibility list");
            assertTrue(GenericPrincipal.principalsEqual(_userPrincipal, (Principal) preference.get(Preference.OWNER_ATTRIBUTE)),
                    "Unexpected owner");
            return null;
        });
    }

    private void doTestDelete(final String preferenceType, final String preferenceName, final RequestInfo requestInfo)
    {
        Subject.doAs(_subject, (PrivilegedAction<Void>) () ->
        {
            Map<String, Object> preferenceAttributes = createPreferenceAttributes(
                    null,
                    null,
                    preferenceType,
                    preferenceName,
                    null,
                    MYUSER_SERIALIZATION,
                    null,
                    Collections.emptyMap());
            Preference preference = PreferenceFactory.fromAttributes(_configuredObject, preferenceAttributes);
            awaitPreferenceFuture(_userPreferences.updateOrAppend(Collections.singleton(preference)));
            Set<Preference> retrievedPreferences = awaitPreferenceFuture(_userPreferences.getPreferences());
            assertEquals(1, (long) retrievedPreferences.size(), "adding pref failed");

            _handler.handleDELETE(_userPreferences, requestInfo);

            retrievedPreferences = awaitPreferenceFuture(_userPreferences.getPreferences());
            assertEquals(0, (long) retrievedPreferences.size(), "Deletion of preference failed");

            // this should be a noop
            _handler.handleDELETE(_userPreferences, requestInfo);
            return null;
        });
    }
}
