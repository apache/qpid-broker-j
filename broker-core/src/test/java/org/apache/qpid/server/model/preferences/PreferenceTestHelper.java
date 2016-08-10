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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;

import org.apache.qpid.server.store.preferences.PreferenceRecord;
import org.apache.qpid.server.util.FutureHelper;

public class PreferenceTestHelper
{
    public static Map<String, Object> createPreferenceAttributes(UUID associatedObjectId,
                                                                 UUID id,
                                                                 String type,
                                                                 String name,
                                                                 String description,
                                                                 String owner,
                                                                 Set<String> visibilitySet,
                                                                 Map<String, Object> preferenceValueAttributes)
    {
        Map<String, Object> preferenceAttributes = new HashMap<>();
        preferenceAttributes.put(Preference.ASSOCIATED_OBJECT_ATTRIBUTE,
                                 associatedObjectId == null ? null : associatedObjectId.toString());
        preferenceAttributes.put(Preference.ID_ATTRIBUTE, id != null ? id : UUID.randomUUID());
        preferenceAttributes.put(Preference.TYPE_ATTRIBUTE, type);
        preferenceAttributes.put(Preference.NAME_ATTRIBUTE, name);
        preferenceAttributes.put(Preference.DESCRIPTION_ATTRIBUTE, description);
        preferenceAttributes.put(Preference.OWNER_ATTRIBUTE, owner);
        preferenceAttributes.put(Preference.VISIBILITY_LIST_ATTRIBUTE, visibilitySet);
        preferenceAttributes.put(Preference.VALUE_ATTRIBUTE, preferenceValueAttributes);
        return preferenceAttributes;
    }

    public static void assertRecords(final Collection<PreferenceRecord> expected,
                                     final Collection<PreferenceRecord> actual)
    {
        assertEquals("Unexpected number of records", expected.size(), actual.size());

        for (PreferenceRecord expectedRecord : expected)
        {
            PreferenceRecord actualRecord = null;
            for (PreferenceRecord record : actual)
            {
                if (record.getId().equals(expectedRecord.getId()))
                {
                    actualRecord = record;
                    break;
                }
            }
            assertNotNull(String.format("No actual record found for expected record '%s'", expectedRecord.getId()),
                          actualRecord);
            assertEquals(String.format("Expected attributes are different from actual: %s vs %s",
                                       expectedRecord.getAttributes().toString(),
                                       actualRecord.getAttributes().toString()),
                         new HashMap<>(expectedRecord.getAttributes()),
                         new HashMap<>(actualRecord.getAttributes()));
        }
    }

    public static <T> T awaitPreferenceFuture(final Future<T> future)
    {
        return FutureHelper.<T, RuntimeException>await(future);
    }

}
