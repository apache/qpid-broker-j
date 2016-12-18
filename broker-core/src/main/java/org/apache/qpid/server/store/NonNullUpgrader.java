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
package org.apache.qpid.server.store;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

abstract class NonNullUpgrader implements DurableConfigurationStoreUpgrader
{
    private final Map<UUID, ConfiguredObjectRecord> _updates = new HashMap<>();
    private final Map<UUID, ConfiguredObjectRecord> _deletes = new HashMap<>();

    Map<UUID, ConfiguredObjectRecord> getUpdateMap()
    {
        return _updates;
    }

    Map<UUID, ConfiguredObjectRecord> getDeleteMap()
    {
        return _deletes;
    }

    @Override
    public final Map<UUID, ConfiguredObjectRecord> getUpdatedRecords()
    {
        final Map<UUID, ConfiguredObjectRecord> updates = new HashMap<>(_updates);
        updates.keySet().removeAll(getDeletedRecords().keySet());
        return updates;
    }

    @Override
    public final Map<UUID, ConfiguredObjectRecord> getDeletedRecords()
    {
        return new HashMap<>(_deletes);
    }
}
