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

import static org.apache.qpid.server.model.preferences.PreferenceTestHelper.awaitPreferenceFuture;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;
import java.util.UUID;

import javax.security.auth.Subject;

import com.google.common.collect.Sets;
import org.hamcrest.Description;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.GroupProvider;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;
import org.apache.qpid.server.security.auth.UsernamePrincipal;
import org.apache.qpid.server.security.group.GroupPrincipal;
import org.apache.qpid.server.store.preferences.PreferenceRecord;
import org.apache.qpid.server.store.preferences.PreferenceStore;
import org.apache.qpid.test.utils.UnitTestBase;

public class UserPreferencesTest extends UnitTestBase
{

    private static final String MYGROUP = "mygroup";
    private static final String MYUSER = "myuser";

    private ConfiguredObject<?> _configuredObject;
    private UserPreferences _userPreferences;
    private Subject _subject;
    private GroupPrincipal _groupPrincipal;
    private PreferenceStore _preferenceStore;
    private UUID _testId;
    private AuthenticatedPrincipal _owner;
    private TaskExecutor _preferenceTaskExecutor;

    @Before
    public void setUp() throws Exception
    {
        _configuredObject = mock(ConfiguredObject.class);
        _preferenceStore = mock(PreferenceStore.class);
        _preferenceTaskExecutor = new CurrentThreadTaskExecutor();
        _preferenceTaskExecutor.start();
        _userPreferences = new UserPreferencesImpl(_preferenceTaskExecutor,
                                                   _configuredObject,
                                                   _preferenceStore,
                                                   Collections.<Preference>emptyList());
        _groupPrincipal = new GroupPrincipal(MYGROUP, (GroupProvider) null);
        _owner = new AuthenticatedPrincipal(new UsernamePrincipal(MYUSER, null));
        _subject = new Subject(true,
                               Sets.newHashSet(_owner, _groupPrincipal),
                               Collections.emptySet(),
                               Collections.emptySet());
        _testId = UUID.randomUUID();
    }

    @After
    public void tearDown() throws Exception
    {
        _preferenceTaskExecutor.stop();
    }

    @Test
    public void testUpdateOrAppend() throws Exception
    {
        final Preference preference = createPreference(_testId,
                                                       "test",
                                                       "X-query",
                                                       Collections.<String, Object>singletonMap("select", "id,name"));

        Subject.doAs(_subject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                awaitPreferenceFuture(_userPreferences.updateOrAppend(Collections.singleton(preference)));
                return null;
            }
        });

        verify(_preferenceStore).updateOrCreate(argThat(new PreferenceRecordMatcher(preference)));
    }


    @Test
    public void testReplace() throws Exception
    {
        final Preference preference = createPreference(_testId,
                                                       "test",
                                                       "X-query",
                                                       Collections.<String, Object>singletonMap("select", "id,name"));

        Subject.doAs(_subject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                awaitPreferenceFuture(_userPreferences.replace(Collections.singleton(preference)));
                return null;
            }
        });

        verify(_preferenceStore).replace(argThat(new UUIDCollectionMatcher(Collections.<UUID>emptyList())),
                                         argThat(new PreferenceRecordMatcher(preference)));
    }


    @Test
    public void testReplaceByType() throws Exception
    {
        final UUID queryUUID = UUID.randomUUID();
        final Preference queryPreference =
                createPreference(queryUUID, "test", "X-query", Collections.<String, Object>emptyMap());

        final UUID dashboardUUID = UUID.randomUUID();
        final Preference dashboardPreference =
                createPreference(dashboardUUID, "test", "X-dashboard", Collections.<String, Object>emptyMap());

        final Preference newQueryPreference =
                createPreference(_testId, "newTest", "X-query", Collections.<String, Object>emptyMap());

        Subject.doAs(_subject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                awaitPreferenceFuture(_userPreferences.updateOrAppend(Arrays.asList(queryPreference, dashboardPreference)));
                awaitPreferenceFuture(_userPreferences.replaceByType("X-query", Collections.singletonList(newQueryPreference)));
                return null;
            }
        });

        verify(_preferenceStore).replace(argThat(new UUIDCollectionMatcher(Collections.singleton(queryUUID))),
                                         argThat(new PreferenceRecordMatcher(newQueryPreference)));
    }

    @Test
    public void testReplaceByTypeAndName() throws Exception
    {
        final UUID query1UUID = UUID.randomUUID();
        final Preference queryPreference1 =
                createPreference(query1UUID, "test", "X-query", Collections.<String, Object>emptyMap());
        final UUID query2UUID = UUID.randomUUID();
        final Preference queryPreference2 =
                createPreference(query2UUID, "test2", "X-query", Collections.<String, Object>emptyMap());

        final UUID dashboardUUID = UUID.randomUUID();
        final Preference dashboardPreference =
                createPreference(dashboardUUID, "test", "X-dashboard", Collections.<String, Object>emptyMap());

        final Preference newQueryPreference =
                createPreference(_testId, "test", "X-query", Collections.<String, Object>emptyMap());

        Subject.doAs(_subject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                awaitPreferenceFuture(_userPreferences.updateOrAppend(Arrays.asList(queryPreference1, queryPreference2, dashboardPreference)));
                awaitPreferenceFuture(_userPreferences.replaceByTypeAndName("X-query", "test", newQueryPreference));
                return null;
            }
        });

        verify(_preferenceStore).replace(argThat(new UUIDCollectionMatcher(Collections.singleton(query1UUID))),
                                         argThat(new PreferenceRecordMatcher(newQueryPreference)));
    }

    private Preference createPreference(final UUID queryUUID,
                                        final String name,
                                        final String type,
                                        final Map<String, Object> preferenceValueAttributes)
    {
        final Preference queryPreference = mock(Preference.class);
        HashMap<String, Object> preferenceAttributes = new HashMap<>();
        preferenceAttributes.put(Preference.ID_ATTRIBUTE, queryUUID);
        preferenceAttributes.put(Preference.NAME_ATTRIBUTE, name);
        preferenceAttributes.put(Preference.TYPE_ATTRIBUTE, type);
        preferenceAttributes.put(Preference.VALUE_ATTRIBUTE, preferenceValueAttributes);
        preferenceAttributes.put(Preference.ASSOCIATED_OBJECT_ATTRIBUTE, _configuredObject.getId());
        when(queryPreference.getId()).thenReturn(queryUUID);
        when(queryPreference.getName()).thenReturn(name);
        when(queryPreference.getType()).thenReturn(type);
        when(queryPreference.getOwner()).thenReturn(_owner);
        when(queryPreference.getAssociatedObject()).thenReturn((ConfiguredObject)_configuredObject);
        when(queryPreference.getAttributes()).thenReturn(preferenceAttributes);
        return queryPreference;
    }

    private class UUIDCollectionMatcher implements ArgumentMatcher<Collection<UUID>>
    {
        private Collection<UUID> _expected;
        private String _failureDescription;

        private UUIDCollectionMatcher(final Collection<UUID> expected)
        {
            _expected = expected;
        }

        @Override
        public boolean matches(final Collection<UUID> o)
        {
            _failureDescription = "Items do not match: expected " + _expected + " actual: " + o;
            return new TreeSet<>(_expected).equals(new TreeSet<>(o));
        }
    }

    private class PreferenceRecordMatcher implements ArgumentMatcher<Collection<PreferenceRecord>>
    {
        private final Preference _preference;

        public PreferenceRecordMatcher(final Preference preference)
        {
            _preference = preference;
        }

        @Override
        public boolean matches(final Collection<PreferenceRecord> preferenceRecords)
        {
            if (preferenceRecords.size() != 1)
            {
                return false;
            }

            PreferenceRecord record = preferenceRecords.iterator().next();
            if (!record.getId().equals(_preference.getId()))
            {
                return false;
            }

            Map<String, Object> recordAttributes = record.getAttributes();
            if (recordAttributes == null)
            {
                return false;
            }

            for (Map.Entry entry : _preference.getAttributes().entrySet())
            {
                if (!Objects.equals(entry.getValue(), recordAttributes.get(entry.getKey())))
                {
                    return false;
                }
            }

            return true;
        }

    }
}
