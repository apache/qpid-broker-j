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

package org.apache.qpid.server.store.preferences;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.plugin.PluggableService;

@SuppressWarnings("unused")
@PluggableService
public class NoopPreferenceStoreFactoryService implements PreferenceStoreFactoryService
{

    public static final String TYPE = "Noop";

    @Override
    public PreferenceStore createInstance(final ConfiguredObject<?> parent,
                                          final Map<String, Object> preferenceStoreAttributes)
    {
        return new PreferenceStore()
        {
            @Override
            public Collection<PreferenceRecord> openAndLoad(final PreferenceStoreUpdater updater)
            {
                return Collections.emptyList();
            }

            @Override
            public void close()
            {

            }

            @Override
            public void updateOrCreate(final Collection<PreferenceRecord> preferenceRecords)
            {

            }

            @Override
            public void replace(final Collection<UUID> preferenceRecordsToRemove,
                                final Collection<PreferenceRecord> preferenceRecordsToAdd)
            {

            }

            @Override
            public void onDelete()
            {

            }
        };
    }

    @Override
    public String getType()
    {
        return TYPE;
    }
}
