/*
 *
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
package org.apache.qpid.server.store.jdbc;

import org.apache.qpid.server.model.ManagedAttribute;
import org.apache.qpid.server.model.ManagedContextDefault;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.store.preferences.PreferenceStoreAttributes;
import org.apache.qpid.server.store.preferences.PreferenceStoreProvider;

public interface JDBCSystemConfig<X extends JDBCSystemConfig<X>> extends SystemConfig<X>,
                                                                         JDBCSettings,
                                                                         PreferenceStoreProvider
{
    @Override
    @ManagedAttribute(mandatory=true, defaultValue = "${systemConfig.connectionUrl}")
    String getConnectionUrl();

    @Override
    @ManagedAttribute(defaultValue = "${systemConfig.connectionPoolType}")
    String getConnectionPoolType();

    @Override
    @ManagedAttribute(defaultValue = "${systemConfig.username}")
    String getUsername();

    @Override
    @ManagedAttribute(secure=true, defaultValue = "${systemConfig.password}")
    String getPassword();

    @Override
    @ManagedAttribute( description = "Configuration for the preference store, e.g. type, path, etc.",
            defaultValue = "{\"type\": \"Provided\"}")
    PreferenceStoreAttributes getPreferenceStoreAttributes();

    @ManagedContextDefault(name = "systemConfig.tableNamePrefix")
    String DEFAULT_SYSTEM_CONFIG_TABLE_NAME_PREFIX = "";

    @Override
    @ManagedAttribute(secure = true,
            description = "Optional database table prefix so multiple SystemConfigs can share the same database",
            defaultValue = "${systemConfig.tableNamePrefix}",
            validValuePattern = "[a-zA-Z_0-9]*")
    String getTableNamePrefix();

}
