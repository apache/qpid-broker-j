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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import javax.security.auth.Subject;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;

import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.model.ConfiguredObject;
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
    private PreferenceStore _preferenceStore;
    private UUID _testId;
    private AuthenticatedPrincipal _owner;
    private TaskExecutor _preferenceTaskExecutor;

    @BeforeEach
    public void setUp() throws Exception
    {
        _configuredObject = mock(ConfiguredObject.class);
        _preferenceStore = mock(PreferenceStore.class);
        _preferenceTaskExecutor = new CurrentThreadTaskExecutor();
        _preferenceTaskExecutor.start();
        _userPreferences = new UserPreferencesImpl(_preferenceTaskExecutor, _configuredObject, _preferenceStore, List.of());
        final GroupPrincipal groupPrincipal = new GroupPrincipal(MYGROUP, null);
        _owner = new AuthenticatedPrincipal(new UsernamePrincipal(MYUSER, null));
        _subject = new Subject(true, Set.of(_owner, groupPrincipal), Set.of(), Set.of());
        _testId = randomUUID();
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        _preferenceTaskExecutor.stop();
    }

    @Test
    public void testUpdateOrAppend()
    {
        final Preference preference = createPreference(_testId, "test", "X-query", Map.of("select", "id,name"));
        Subject.doAs(_subject, (PrivilegedAction<Void>) () ->
        {
            awaitPreferenceFuture(_userPreferences.updateOrAppend(Set.of(preference)));
            return null;
        });
        verify(_preferenceStore).updateOrCreate(argThat(new PreferenceRecordMatcher(preference)));
    }

    @Test
    public void testReplace()
    {
        final Preference preference = createPreference(_testId, "test", "X-query", Map.of("select", "id,name"));
        Subject.doAs(_subject, (PrivilegedAction<Void>) () ->
        {
            awaitPreferenceFuture(_userPreferences.replace(Set.of(preference)));
            return null;
        });

        verify(_preferenceStore).replace(argThat(new UUIDCollectionMatcher(List.of())),
                                         argThat(new PreferenceRecordMatcher(preference)));
    }

    @Test
    public void testReplaceByType()
    {
        final UUID queryUUID = randomUUID();
        final Preference queryPreference = createPreference(queryUUID, "test", "X-query", Map.of());
        final UUID dashboardUUID = randomUUID();
        final Preference dashboardPreference = createPreference(dashboardUUID, "test", "X-dashboard", Map.of());
        final Preference newQueryPreference = createPreference(_testId, "newTest", "X-query", Map.of());
        Subject.doAs(_subject, (PrivilegedAction<Void>) () ->
        {
            awaitPreferenceFuture(_userPreferences.updateOrAppend(Arrays.asList(queryPreference, dashboardPreference)));
            awaitPreferenceFuture(_userPreferences.replaceByType("X-query", Collections.singletonList(newQueryPreference)));
            return null;
        });
        verify(_preferenceStore).replace(argThat(new UUIDCollectionMatcher(Set.of(queryUUID))),
                                         argThat(new PreferenceRecordMatcher(newQueryPreference)));
    }

    @Test
    public void testReplaceByTypeAndName()
    {
        final UUID query1UUID = randomUUID();
        final Preference queryPreference1 = createPreference(query1UUID, "test", "X-query", Map.of());
        final UUID query2UUID = randomUUID();
        final Preference queryPreference2 = createPreference(query2UUID, "test2", "X-query", Map.of());
        final UUID dashboardUUID = randomUUID();
        final Preference dashboardPreference = createPreference(dashboardUUID, "test", "X-dashboard", Map.of());
        final Preference newQueryPreference = createPreference(_testId, "test", "X-query", Map.of());
        Subject.doAs(_subject, (PrivilegedAction<Void>) () ->
        {
            awaitPreferenceFuture(_userPreferences.updateOrAppend(Arrays.asList(queryPreference1, queryPreference2, dashboardPreference)));
            awaitPreferenceFuture(_userPreferences.replaceByTypeAndName("X-query", "test", newQueryPreference));
            return null;
        });
        verify(_preferenceStore).replace(argThat(new UUIDCollectionMatcher(Set.of(query1UUID))),
                                         argThat(new PreferenceRecordMatcher(newQueryPreference)));
    }

    @SuppressWarnings("rawtypes")
    private Preference createPreference(final UUID queryUUID,
                                        final String name,
                                        final String type,
                                        final Map<String, Object> preferenceValueAttributes)
    {
        final Preference queryPreference = mock(Preference.class);
        final Map<String, Object> preferenceAttributes = new HashMap<>();
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

    private static class UUIDCollectionMatcher implements ArgumentMatcher<Collection<UUID>>
    {
        private final Collection<UUID> _expected;

        private UUIDCollectionMatcher(final Collection<UUID> expected)
        {
            _expected = expected;
        }

        @Override
        public boolean matches(final Collection<UUID> o)
        {
            return new TreeSet<>(_expected).equals(new TreeSet<>(o));
        }
    }

    @SuppressWarnings("rawtypes")
    private static class PreferenceRecordMatcher implements ArgumentMatcher<Collection<PreferenceRecord>>
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

            final PreferenceRecord record = preferenceRecords.iterator().next();
            if (!record.getId().equals(_preference.getId()))
            {
                return false;
            }

            final Map<String, Object> recordAttributes = record.getAttributes();
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
