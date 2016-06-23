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

import static org.apache.qpid.server.management.plugin.servlet.rest.RestUserPreferenceHandler.ActionTaken;
import static org.mockito.Mockito.mock;

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

import com.google.common.collect.Sets;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.preferences.Preference;
import org.apache.qpid.server.model.preferences.UserPreferences;
import org.apache.qpid.server.model.preferences.UserPreferencesImpl;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;
import org.apache.qpid.server.security.auth.TestPrincipalUtils;
import org.apache.qpid.server.security.group.GroupPrincipal;
import org.apache.qpid.test.utils.QpidTestCase;

public class RestUserPreferenceHandlerTest extends QpidTestCase
{

    private static final String MYGROUP = "mygroup";
    private static final String MYUSER = "myuser";

    private RestUserPreferenceHandler _handler = new RestUserPreferenceHandler();
    private ConfiguredObject<?> _configuredObject;
    private UserPreferences _userPreferences;
    private Subject _subject;
    private GroupPrincipal _groupPrincipal;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        _configuredObject = mock(ConfiguredObject.class);
        _userPreferences = new UserPreferencesImpl(_configuredObject,
                                                   new HashMap<UUID, Preference>(),
                                                   new HashMap<String, List<Preference>>());
        _groupPrincipal = new GroupPrincipal(MYGROUP);
        _subject = new Subject(true,
                               Sets.newHashSet(new AuthenticatedPrincipal(MYUSER), _groupPrincipal),
                               Collections.emptySet(),
                               Collections.emptySet());
    }

    public void testPutWithVisibilityList_ValidGroup() throws Exception
    {

        final RequestInfo requestInfo = RequestInfo.createPreferencesRequestInfo(Collections.<String>emptyList(),
                                                                                 Arrays.asList("X-testtype",
                                                                                               "myprefname"));

        final Map<String, Object> pref = new HashMap<>();
        pref.put(Preference.VALUE_ATTRIBUTE, Collections.emptyMap());
        pref.put(Preference.VISIBILITY_LIST_ATTRIBUTE, Collections.singletonList(MYGROUP));

        Subject.doAs(_subject, new PrivilegedAction<Void>()
                     {
                         @Override
                         public Void run()
                         {
                             final ActionTaken action =
                                     _handler.handlePUT(_userPreferences, requestInfo, pref);
                             assertEquals(ActionTaken.CREATED, action);

                             assertEquals("Unexpected number of preferences", 1, _userPreferences.getPreferences().size());
                             Preference prefModel = _userPreferences.getPreferences().iterator().next();
                             final Set<Principal> visibilityList = prefModel.getVisibilityList();
                             assertEquals("Unexpected number of principals in visibility list", 1, visibilityList.size());
                             Principal principal = visibilityList.iterator().next();
                             assertEquals("Unexpected member of visibility list", MYGROUP, principal.getName());
                             return null;
                         }
                     }
                    );
    }

    public void testPutWithVisibilityList_InvalidGroup() throws Exception
    {

        final RequestInfo requestInfo = RequestInfo.createPreferencesRequestInfo(Collections.<String>emptyList(),
                                                                                 Arrays.asList("X-testtype",
                                                                                               "myprefname"));

        final Map<String, Object> pref = new HashMap<>();
        pref.put(Preference.VALUE_ATTRIBUTE, Collections.emptyMap());
        pref.put(Preference.VISIBILITY_LIST_ATTRIBUTE, Collections.singletonList("Invalid Group"));

        Subject.doAs(_subject, new PrivilegedAction<Void>()
                     {
                         @Override
                         public Void run()
                         {
                             try
                             {
                                 _handler.handlePUT(_userPreferences, requestInfo, pref);
                                 fail("Expected exception not thrown");
                             }
                             catch (IllegalArgumentException e)
                             {
                                 // pass
                             }
                             return null;
                         }
                     }
                    );
    }

    public void testPostToTypeWithVisibilityList_ValidGroup() throws Exception
    {
        final RequestInfo typeRequestInfo = RequestInfo.createPreferencesRequestInfo(Collections.<String>emptyList(),
                                                                                     Arrays.asList("X-testtype"));

        final Map<String, Object> pref = new HashMap<>();
        pref.put(Preference.NAME_ATTRIBUTE, "testPref");
        pref.put(Preference.VALUE_ATTRIBUTE, Collections.emptyMap());
        pref.put(Preference.VISIBILITY_LIST_ATTRIBUTE, Collections.singletonList(MYGROUP));

        Subject.doAs(_subject, new PrivilegedAction<Void>()
                     {
                         @Override
                         public Void run()
                         {
                             _handler.handlePOST(_userPreferences, typeRequestInfo, Collections.singletonList(pref));

                             assertEquals("Unexpected number of preferences", 1, _userPreferences.getPreferences().size());
                             Preference prefModel = _userPreferences.getPreferences().iterator().next();
                             final Set<Principal> visibilityList = prefModel.getVisibilityList();
                             assertEquals("Unexpected number of principals in visibility list", 1, visibilityList.size());
                             Principal principal = visibilityList.iterator().next();
                             assertEquals("Unexpected member of visibility list", MYGROUP, principal.getName());
                             return null;
                         }
                     }
                    );
    }

    public void testPostToRootWithVisibilityList_ValidGroup() throws Exception
    {
        final RequestInfo rootRequestInfo = RequestInfo.createPreferencesRequestInfo(Collections.<String>emptyList(),
                                                                                     Collections.<String>emptyList());
        final Map<String, Object> pref = new HashMap<>();
        pref.put(Preference.NAME_ATTRIBUTE, "testPref");
        pref.put(Preference.VALUE_ATTRIBUTE, Collections.emptyMap());
        pref.put(Preference.VISIBILITY_LIST_ATTRIBUTE, Collections.singletonList(MYGROUP));

        Subject.doAs(_subject, new PrivilegedAction<Void>()
                     {
                         @Override
                         public Void run()
                         {
                             final Map<String, List<Map<String, Object>>> payload =
                                     Collections.singletonMap("X-testtype2", Collections.singletonList(pref));
                             _handler.handlePOST(_userPreferences, rootRequestInfo, payload);

                             assertEquals("Unexpected number of preferences", 1, _userPreferences.getPreferences().size());
                             Preference prefModel = _userPreferences.getPreferences().iterator().next();
                             final Set<Principal> visibilityList = prefModel.getVisibilityList();
                             assertEquals("Unexpected number of principals in visibility list", 1, visibilityList.size());
                             Principal principal = visibilityList.iterator().next();
                             assertEquals("Unexpected member of visibility list", MYGROUP, principal.getName());

                             return null;
                         }
                     }
                    );
    }

    public void testPostToTypeWithVisibilityList_InvalidGroup() throws Exception
    {
        final RequestInfo requestInfo = RequestInfo.createPreferencesRequestInfo(Collections.<String>emptyList(),
                                                                                 Arrays.asList("X-testtype"));

        final Map<String, Object> pref = new HashMap<>();
        pref.put(Preference.NAME_ATTRIBUTE, "testPref");
        pref.put(Preference.VALUE_ATTRIBUTE, Collections.emptyMap());
        pref.put(Preference.VISIBILITY_LIST_ATTRIBUTE, Collections.singletonList("Invalid Group"));

        Subject.doAs(_subject, new PrivilegedAction<Void>()
                     {
                         @Override

                         public Void run()
                         {
                             try
                             {
                                 _handler.handlePOST(_userPreferences, requestInfo, Collections.singletonList(pref));
                                 fail("Expected exception not thrown");
                             }
                             catch (IllegalArgumentException e)
                             {
                                 // pass
                             }
                             return null;
                         }
                     }
                    );
    }

    public void testPostToRootWithVisibilityList_InvalidGroup() throws Exception
    {
        final RequestInfo requestInfo = RequestInfo.createPreferencesRequestInfo(Collections.<String>emptyList(),
                                                                                 Collections.<String>emptyList());

        final Map<String, Object> pref = new HashMap<>();
        pref.put(Preference.NAME_ATTRIBUTE, "testPref");
        pref.put(Preference.VALUE_ATTRIBUTE, Collections.emptyMap());
        pref.put(Preference.VISIBILITY_LIST_ATTRIBUTE, Collections.singletonList("Invalid Group"));

        Subject.doAs(_subject, new PrivilegedAction<Void>()
                     {
                         @Override
                         public Void run()
                         {
                             try
                             {
                                 final Map<String, List<Map<String, Object>>> payload =
                                         Collections.singletonMap("X-testType", Collections.singletonList(pref));
                                 _handler.handlePOST(_userPreferences, requestInfo, payload);
                                 fail("Expected exception not thrown");
                             }
                             catch (IllegalArgumentException e)
                             {
                                 // pass
                             }
                             return null;
                         }
                     }
                    );
    }

    public void testGetHasCorrectVisibilityList() throws Exception
    {
        final RequestInfo rootRequestInfo = RequestInfo.createPreferencesRequestInfo(Collections.<String>emptyList(),
                                                                                     Collections.<String>emptyList());
        final String type = "X-testtype";

        Subject.doAs(_subject, new PrivilegedAction<Void>()
                     {
                         @Override
                         public Void run()
                         {
                             Preference preference = _userPreferences.createPreference(null,
                                                                                       type,
                                                                                       "testpref",
                                                                                       null,
                                                                                       Collections.<Principal>singleton(
                                                                                               _groupPrincipal),
                                                                                       Collections.<String, Object>emptyMap());
                             _userPreferences.updateOrAppend(Collections.singleton(preference));

                             Map<String, List<Map<String, Object>>> typeToPreferenceListMap =
                                     (Map<String, List<Map<String, Object>>>) _handler.handleGET(_userPreferences, rootRequestInfo);
                             assertEquals("Unexpected preference map size", 1, typeToPreferenceListMap.size());
                             assertEquals("Unexpected type in preference map",
                                          type,
                                          typeToPreferenceListMap.keySet().iterator().next());
                             List<Map<String, Object>> preferences = typeToPreferenceListMap.get(type);
                             assertEquals("Unexpected number of preferences", 1, preferences.size());
                             Set<String> visibilityList = (Set<String>) preferences.get(0).get("visibilityList");
                             assertEquals("Unexpected number of principals in visibility list", 1, visibilityList.size());
                             assertEquals("Unexpected principal in visibility list", MYGROUP, visibilityList.iterator().next());
                             return null;
                         }
                     }
                    );
    }

    public void testGetById() throws Exception
    {
        Subject.doAs(_subject, new PrivilegedAction<Void>()
                     {
                         @Override
                         public Void run()
                         {
                             final String type = "X-testtype";
                             Preference p1 = _userPreferences.createPreference(null,
                                                                               type,
                                                                               "testpref",
                                                                               null,
                                                                               null,
                                                                               Collections.<String, Object>emptyMap());
                             Preference p2 = _userPreferences.createPreference(null,
                                                                               type,
                                                                               "testpref2",
                                                                               null,
                                                                               null,
                                                                               Collections.<String, Object>emptyMap());
                             _userPreferences.updateOrAppend(Arrays.asList(p1, p2));
                             UUID id = p1.getId();

                             final RequestInfo rootRequestInfo = RequestInfo.createPreferencesRequestInfo(Collections.<String>emptyList(),
                                                                                                          Collections.<String>emptyList(),
                                                                                                          Collections.singletonMap("id", Collections.singletonList(id.toString())));

                             Map<String, List<Map<String, Object>>> typeToPreferenceListMap =
                                     (Map<String, List<Map<String, Object>>>) _handler.handleGET(_userPreferences, rootRequestInfo);
                             assertEquals("Unexpected p1 map size", 1, typeToPreferenceListMap.size());
                             assertEquals("Unexpected type in p1 map",
                                          type,
                                          typeToPreferenceListMap.keySet().iterator().next());
                             List<Map<String, Object>> preferences = typeToPreferenceListMap.get(type);
                             assertEquals("Unexpected number of preferences", 1, preferences.size());
                             assertEquals("Unexpected id", id, preferences.get(0).get(Preference.ID_ATTRIBUTE));
                             return null;
                         }
                     }
                    );
    }

    public void testDeleteById() throws Exception
    {
        Subject.doAs(_subject, new PrivilegedAction<Void>()
                     {
                         @Override
                         public Void run()
                         {
                             final String type = "X-testtype";
                             Preference p1 = _userPreferences.createPreference(null,
                                                                               type,
                                                                               "testpref",
                                                                               null,
                                                                               null,
                                                                               Collections.<String, Object>emptyMap());
                             Preference p2 = _userPreferences.createPreference(null,
                                                                               type,
                                                                               "testpref2",
                                                                               null,
                                                                               null,
                                                                               Collections.<String, Object>emptyMap());
                             _userPreferences.updateOrAppend(Arrays.asList(p1, p2));
                             UUID id = p1.getId();

                             final RequestInfo rootRequestInfo = RequestInfo.createPreferencesRequestInfo(Collections.<String>emptyList(),
                                                                                                          Collections.<String>emptyList(),
                                                                                                          Collections.singletonMap("id", Collections.singletonList(id.toString())));

                             _handler.handleDELETE(_userPreferences, rootRequestInfo);

                             final Set<Preference> retrievedPreferences = _userPreferences.getPreferences();
                             assertEquals("Unexpected number of preferences", 1, retrievedPreferences.size());
                             assertTrue("Unexpected type in p1 map", retrievedPreferences.contains(p2));
                             return null;
                         }
                     }
                    );
    }

    public void testDeleteByTypeAndName() throws Exception
    {
        final String preferenceType = "X-testtype";
        final String preferenceName = "myprefname";
        final RequestInfo requestInfo = RequestInfo.createPreferencesRequestInfo(Collections.<String>emptyList(),
                                                                                 Arrays.asList(preferenceType,
                                                                                               preferenceName));

        final Map<String, Object> pref = new HashMap<>();
        pref.put(Preference.VALUE_ATTRIBUTE, Collections.emptyMap());

        doTestDelete(preferenceType, preferenceName, requestInfo);
    }

    public void testDeleteByType() throws Exception
    {
        final String preferenceType = "X-testtype";
        final String preferenceName = "myprefname";
        final RequestInfo requestInfo = RequestInfo.createPreferencesRequestInfo(Collections.<String>emptyList(),
                                                                                 Arrays.asList(preferenceType));

        final Map<String, Object> pref = new HashMap<>();
        pref.put(Preference.VALUE_ATTRIBUTE, Collections.emptyMap());

        doTestDelete(preferenceType, preferenceName, requestInfo);
    }

    public void testDeleteByRoot() throws Exception
    {
        final String preferenceType = "X-testtype";
        final String preferenceName = "myprefname";
        final RequestInfo requestInfo = RequestInfo.createPreferencesRequestInfo(Collections.<String>emptyList(),
                                                                                 Collections.<String>emptyList());

        final Map<String, Object> pref = new HashMap<>();
        pref.put(Preference.VALUE_ATTRIBUTE, Collections.emptyMap());

        doTestDelete(preferenceType, preferenceName, requestInfo);
    }

    public void testGetVisiblePreferencesByRoot() throws Exception
    {
        final String prefName = "testpref";
        final String prefType = "X-testtype";
        final RequestInfo rootRequestInfo = RequestInfo.createVisiblePreferencesRequestInfo(Collections.<String>emptyList(),
                                                                                            Collections.<String>emptyList(),
                                                                                            Collections.<String, List<String>>emptyMap());

        Subject.doAs(_subject, new PrivilegedAction<Void>()
                     {
                         @Override
                         public Void run()
                         {
                             final Set<Preference> preferences = new HashSet<>();
                             Preference p1 = _userPreferences.createPreference(null,
                                                                               prefType,
                                                                               prefName,
                                                                               null,
                                                                               Collections.<Principal>singleton(
                                                                                       _groupPrincipal),
                                                                               Collections.<String, Object>emptyMap());
                             preferences.add(p1);
                             Preference p2 = _userPreferences.createPreference(null,
                                                                               prefType,
                                                                               "testPref2",
                                                                               null,
                                                                               Collections.<Principal>emptySet(),
                                                                               Collections.<String, Object>emptyMap());
                             preferences.add(p2);
                             _userPreferences.updateOrAppend(preferences);
                             return null;
                         }
                     }
                    );

        Subject testSubject2 = TestPrincipalUtils.createTestSubject("testUser2", MYGROUP);
        Subject.doAs(testSubject2, new PrivilegedAction<Void>()
                     {
                         @Override
                         public Void run()
                         {
                             Map<String, List<Map<String, Object>>> typeToPreferenceListMap =
                                     (Map<String, List<Map<String, Object>>>) _handler.handleGET(_userPreferences, rootRequestInfo);
                             assertEquals("Unexpected preference map size", 1, typeToPreferenceListMap.size());
                             assertEquals("Unexpected prefType in preference map",
                                          prefType,
                                          typeToPreferenceListMap.keySet().iterator().next());
                             List<Map<String, Object>> preferences = typeToPreferenceListMap.get(prefType);
                             assertEquals("Unexpected number of preferences", 1, preferences.size());
                             assertEquals("Unexpected name of preferences", prefName, preferences.get(0).get(Preference.NAME_ATTRIBUTE));
                             Set<String> visibilityList = (Set<String>) preferences.get(0).get(Preference.VISIBILITY_LIST_ATTRIBUTE);
                             assertEquals("Unexpected number of principals in visibility list", 1, visibilityList.size());
                             assertEquals("Unexpected principal in visibility list", MYGROUP, visibilityList.iterator().next());
                             assertEquals("Unexpected owner", MYUSER, preferences.get(0).get(Preference.OWNER_ATTRIBUTE));
                             return null;
                         }
                     }
                    );
    }

    public void testGetVisiblePreferencesByType() throws Exception
    {
        final String prefName = "testpref";
        final String prefType = "X-testtype";
        final RequestInfo rootRequestInfo = RequestInfo.createVisiblePreferencesRequestInfo(Collections.<String>emptyList(),
                                                                                            Arrays.asList(prefType),
                                                                                            Collections.<String, List<String>>emptyMap());

        Subject.doAs(_subject, new PrivilegedAction<Void>()
                     {
                         @Override
                         public Void run()
                         {
                             final Set<Preference> preferences = new HashSet<>();
                             Preference p1 = _userPreferences.createPreference(null,
                                                                               prefType,
                                                                               prefName,
                                                                               null,
                                                                               Collections.<Principal>singleton(
                                                                                       _groupPrincipal),
                                                                               Collections.<String, Object>emptyMap());
                             preferences.add(p1);
                             Preference p2 = _userPreferences.createPreference(null,
                                                                               prefType,
                                                                               "testPref2",
                                                                               null,
                                                                               Collections.<Principal>emptySet(),
                                                                               Collections.<String, Object>emptyMap());
                             preferences.add(p2);
                             _userPreferences.updateOrAppend(preferences);
                             return null;
                         }
                     }
                    );

        Subject testSubject2 = TestPrincipalUtils.createTestSubject("testUser2", MYGROUP);
        Subject.doAs(testSubject2, new PrivilegedAction<Void>()
                     {
                         @Override
                         public Void run()
                         {
                             List<Map<String, Object>> preferences =
                                     (List<Map<String, Object>>) _handler.handleGET(_userPreferences, rootRequestInfo);
                             assertEquals("Unexpected number of preferences", 1, preferences.size());
                             assertEquals("Unexpected name of preferences", prefName, preferences.get(0).get(Preference.NAME_ATTRIBUTE));
                             Set<String> visibilityList = (Set<String>) preferences.get(0).get(Preference.VISIBILITY_LIST_ATTRIBUTE);
                             assertEquals("Unexpected number of principals in visibility list", 1, visibilityList.size());
                             assertEquals("Unexpected principal in visibility list", MYGROUP, visibilityList.iterator().next());
                             assertEquals("Unexpected owner", MYUSER, preferences.get(0).get(Preference.OWNER_ATTRIBUTE));
                             return null;
                         }
                     }
                    );
    }

    public void testGetVisiblePreferencesByTypeAndName() throws Exception
    {
        final String prefName = "testpref";
        final String prefType = "X-testtype";
        final RequestInfo rootRequestInfo = RequestInfo.createVisiblePreferencesRequestInfo(Collections.<String>emptyList(),
                                                                                            Arrays.asList(prefType, prefName),
                                                                                            Collections.<String, List<String>>emptyMap());

        Subject.doAs(_subject, new PrivilegedAction<Void>()
                     {
                         @Override
                         public Void run()
                         {
                             final Set<Preference> preferences = new HashSet<>();
                             Preference p1 = _userPreferences.createPreference(null,
                                                                               prefType,
                                                                               prefName,
                                                                               null,
                                                                               Collections.<Principal>singleton(
                                                                                       _groupPrincipal),
                                                                               Collections.<String, Object>emptyMap());
                             preferences.add(p1);
                             Preference p2 = _userPreferences.createPreference(null,
                                                                               prefType,
                                                                               "testPref2",
                                                                               null,
                                                                               Collections.<Principal>emptySet(),
                                                                               Collections.<String, Object>emptyMap());
                             preferences.add(p2);
                             _userPreferences.updateOrAppend(preferences);
                             return null;
                         }
                     }
                    );

        Subject testSubject2 = TestPrincipalUtils.createTestSubject("testUser2", MYGROUP);
        Subject.doAs(testSubject2, new PrivilegedAction<Void>()
                     {
                         @Override
                         public Void run()
                         {
                             Map<String, Object> preference =
                                     (Map<String, Object>) _handler.handleGET(_userPreferences, rootRequestInfo);
                             assertEquals("Unexpected name of preferences", prefName, preference.get(Preference.NAME_ATTRIBUTE));
                             Set<String> visibilityList = (Set<String>) preference.get(Preference.VISIBILITY_LIST_ATTRIBUTE);
                             assertEquals("Unexpected number of principals in visibility list", 1, visibilityList.size());
                             assertEquals("Unexpected principal in visibility list", MYGROUP, visibilityList.iterator().next());
                             assertEquals("Unexpected owner", MYUSER, preference.get(Preference.OWNER_ATTRIBUTE));
                             return null;
                         }
                     }
                    );
    }

    private void doTestDelete(final String preferenceType, final String preferenceName, final RequestInfo requestInfo)
    {
        Subject.doAs(_subject, new PrivilegedAction<Void>()
                     {
                         @Override
                         public Void run()
                         {
                             Preference preference = _userPreferences.createPreference(null,
                                                                                       preferenceType,
                                                                                       preferenceName,
                                                                                       null,
                                                                                       null,
                                                                                       Collections.<String, Object>emptyMap());
                             _userPreferences.updateOrAppend(Collections.singleton(preference));
                             Set<Preference> retrievedPreferences = _userPreferences.getPreferences();
                             assertEquals("adding pref failed", 1, retrievedPreferences.size());

                             _handler.handleDELETE(_userPreferences, requestInfo);

                             retrievedPreferences = _userPreferences.getPreferences();
                             assertEquals("Deletion of preference failed", 0, retrievedPreferences.size());

                             // this should be a noop
                             _handler.handleDELETE(_userPreferences, requestInfo);
                             return null;
                         }
                     }
                    );
    }
}
