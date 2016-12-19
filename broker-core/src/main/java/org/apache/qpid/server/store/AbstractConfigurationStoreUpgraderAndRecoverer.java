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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.util.Action;

public class AbstractConfigurationStoreUpgraderAndRecoverer
{
    private final Map<String, StoreUpgraderPhase> _upgraders = new HashMap<>();

    List<ConfiguredObjectRecord> upgrade(final DurableConfigurationStore store,
                                         final List<ConfiguredObjectRecord> records,
                                         final String rootCategory,
                                         final String modelVersionAttributeName)
    {
        GenericStoreUpgrader upgrader = new GenericStoreUpgrader(rootCategory,
                                                                 modelVersionAttributeName, store, _upgraders);
        upgrader.upgrade(records);
        return upgrader.getRecords();
    }

    void register(StoreUpgraderPhase upgrader)
    {
        _upgraders.put(upgrader.getFromVersion(), upgrader);
    }

    void applyRecursively(final ConfiguredObject<?> object, final RecursiveAction<ConfiguredObject<?>> action)
    {
        applyRecursively(object, action, new HashSet<ConfiguredObject<?>>());
    }

    void applyRecursively(final ConfiguredObject<?> object,
                                  final RecursiveAction<ConfiguredObject<?>> action,
                                  final HashSet<ConfiguredObject<?>> visited)
    {
        if(!visited.contains(object))
        {
            visited.add(object);
            action.performAction(object);
            if (action.applyToChildren(object))
            {
                for (Class<? extends ConfiguredObject> childClass : object.getModel().getChildTypes(object.getCategoryClass()))
                {
                    Collection<? extends ConfiguredObject> children = object.getChildren(childClass);
                    if (children != null)
                    {
                        for (ConfiguredObject<?> child : children)
                        {
                            applyRecursively(child, action, visited);
                        }
                    }
                }
            }
        }
    }



    interface RecursiveAction<C> extends Action<C>
    {
        boolean applyToChildren(C object);
    }


}
