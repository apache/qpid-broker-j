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

import static org.mockito.Matchers.argThat;
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
import java.util.TreeSet;
import java.util.UUID;

import javax.security.auth.Subject;

import com.google.common.collect.Sets;
import org.hamcrest.Description;
import org.mockito.ArgumentMatcher;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;
import org.apache.qpid.server.security.group.GroupPrincipal;
import org.apache.qpid.server.store.preferences.PreferenceRecord;
import org.apache.qpid.server.store.preferences.PreferenceStore;
import org.apache.qpid.test.utils.QpidTestCase;

public class UserPreferencesTest extends QpidTestCase
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

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        _configuredObject = mock(ConfiguredObject.class);
        _preferenceStore = mock(PreferenceStore.class);
        _userPreferences = new UserPreferencesImpl(_configuredObject,
                                                   new HashMap<UUID, Preference>(),
                                                   new HashMap<String, List<Preference>>(), _preferenceStore);
        _groupPrincipal = new GroupPrincipal(MYGROUP);
        _owner = new AuthenticatedPrincipal(MYUSER);
        _subject = new Subject(true,
                               Sets.newHashSet(_owner, _groupPrincipal),
                               Collections.emptySet(),
                               Collections.emptySet());
        _testId = UUID.randomUUID();
    }


    public void testUpdateOrAppend() throws Exception
    {
        final Preference preference = createPreference(_testId,
                                                       "test",
                                                       "query",
                                                       Collections.<String, Object>singletonMap("select", "id,name"));

        Subject.doAs(_subject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                _userPreferences.updateOrAppend(Collections.singleton(preference));
                return null;
            }
        });

        verify(_preferenceStore).updateOrCreate(argThat(new PreferenceRecordMatcher(preference)));
    }


    public void testReplace() throws Exception
    {
        final Preference preference = createPreference(_testId,
                                                       "test",
                                                       "query",
                                                       Collections.<String, Object>singletonMap("select", "id,name"));

        Subject.doAs(_subject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                _userPreferences.replace(Collections.singleton(preference));
                return null;
            }
        });

        verify(_preferenceStore).replace(argThat(new UUIDCollectionMatcher(Collections.<UUID>emptyList())),
                                         argThat(new PreferenceRecordMatcher(preference)));
    }


    public void testReplaceByType() throws Exception
    {
        final UUID queryUUID = UUID.randomUUID();
        final Preference queryPreference =
                createPreference(queryUUID, "test", "query", Collections.<String, Object>emptyMap());

        final UUID dashboardUUID = UUID.randomUUID();
        final Preference dashboardPreference =
                createPreference(dashboardUUID, "test", "dashboard", Collections.<String, Object>emptyMap());

        final Preference newQueryPreference =
                createPreference(_testId, "newTest", "query", Collections.<String, Object>emptyMap());

        Subject.doAs(_subject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                _userPreferences.updateOrAppend(Arrays.asList(queryPreference, dashboardPreference));
                _userPreferences.replaceByType("query", Collections.singletonList(newQueryPreference));
                return null;
            }
        });

        verify(_preferenceStore).replace(argThat(new UUIDCollectionMatcher(Collections.singleton(queryUUID))),
                                         argThat(new PreferenceRecordMatcher(newQueryPreference)));
    }

    public void testReplaceByTypeAndName() throws Exception
    {
        final UUID query1UUID = UUID.randomUUID();
        final Preference queryPreference1 =
                createPreference(query1UUID, "test", "query", Collections.<String, Object>emptyMap());
        final UUID query2UUID = UUID.randomUUID();
        final Preference queryPreference2 =
                createPreference(query2UUID, "test2", "query", Collections.<String, Object>emptyMap());

        final UUID dashboardUUID = UUID.randomUUID();
        final Preference dashboardPreference =
                createPreference(dashboardUUID, "test", "dashboard", Collections.<String, Object>emptyMap());

        final Preference newQueryPreference =
                createPreference(_testId, "test", "query", Collections.<String, Object>emptyMap());

        Subject.doAs(_subject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                _userPreferences.updateOrAppend(Arrays.asList(queryPreference1, queryPreference2, dashboardPreference));
                _userPreferences.replaceByTypeAndName("query", "test", newQueryPreference);
                return null;
            }
        });

        verify(_preferenceStore).replace(argThat(new UUIDCollectionMatcher(Collections.singleton(query1UUID))),
                                         argThat(new PreferenceRecordMatcher(newQueryPreference)));
    }

    private Preference createPreference(final UUID queryUUID,
                                        final String name,
                                        final String type,
                                        final Map<String, Object> attributes)
    {
        final Preference queryPreference = mock(Preference.class);
        when(queryPreference.getId()).thenReturn(queryUUID);
        when(queryPreference.getName()).thenReturn(name);
        when(queryPreference.getType()).thenReturn(type);
        when(queryPreference.getOwner()).thenReturn(_owner);
        when(queryPreference.getAttributes()).thenReturn(attributes);
        return queryPreference;
    }

    private class UUIDCollectionMatcher extends ArgumentMatcher<Collection<UUID>>
    {
        private Collection<UUID> _expected;
        private String _failureDescription;

        private UUIDCollectionMatcher(final Collection<UUID> expected)
        {
            _expected = expected;
        }

        @Override
        public boolean matches(final Object o)
        {
            if (!(o instanceof Collection))
            {
                _failureDescription = "Not a collection";
                return false;
            }

            _failureDescription = "Items do not match: expected " + _expected + " actual: " + o;
            return new TreeSet<>(_expected).equals(new TreeSet<>((Collection<UUID>) o));
        }

        @Override
        public void describeTo(Description description)
        {
            if (_failureDescription != null)
            {
                description.appendText(_failureDescription);
            }
        }
    }

    private class PreferenceRecordMatcher extends ArgumentMatcher<Collection<PreferenceRecord>>
    {
        private final Preference _preference;
        private String _failureDescription;

        public PreferenceRecordMatcher(final Preference preference)
        {
            _preference = preference;
        }

        @Override
        public boolean matches(final Object records)
        {
            _failureDescription = "Unexpected arguments type";
            Collection<PreferenceRecord> preferenceRecords = (Collection<PreferenceRecord>) records;
            if (preferenceRecords.size() != 1)
            {
                return false;
            }

            _failureDescription = "Unexpected preference id";
            PreferenceRecord record = preferenceRecords.iterator().next();
            if (!record.getId().equals(_preference.getId()))
            {
                return false;
            }

            _failureDescription = "Attributes is null";
            Map<String, Object> recordAttributes = record.getAttributes();
            if (recordAttributes == null)
            {
                return false;
            }
            ;

            _failureDescription = "Expected attributes are not found: " + recordAttributes;
            for (Map.Entry entry : _preference.getAttributes().entrySet())
            {
                if (!entry.getValue().equals(recordAttributes.get(entry.getKey())))
                {
                    return false;
                }
            }

            return true;
        }

        @Override
        public void describeTo(Description description)
        {
            if (_failureDescription != null)
            {
                description.appendText(_failureDescription);
            }
        }
    }
}
