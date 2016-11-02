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

import java.util.Map;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.plugin.PluggableService;

@SuppressWarnings("unused")
@PluggableService
public class JsonFilePreferenceStoreFactoryService implements PreferenceStoreFactoryService
{
    private static final String TYPE = "JSON";
    private static final String PATH = "path";

    @Override
    public PreferenceStore createInstance(final ConfiguredObject<?> parent,
                                          final Map<String, Object> preferenceStoreAttributes)
    {
        final Object path = preferenceStoreAttributes.get(PATH);
        if (path == null || !(path instanceof String))
        {
            throw new IllegalConfigurationException("JsonFilePreferenceStore requires path");
        }
        final String posixFilePermissions = parent.getContextValue(String.class, SystemConfig.POSIX_FILE_PERMISSIONS);
        return new JsonFilePreferenceStore((String) path, posixFilePermissions);
    }

    @Override
    public String getType()
    {
        return TYPE;
    }
}
