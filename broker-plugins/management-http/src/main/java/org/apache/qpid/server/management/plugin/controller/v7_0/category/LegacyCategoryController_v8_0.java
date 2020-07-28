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
 *
 */

package org.apache.qpid.server.management.plugin.controller.v7_0.category;

import static org.apache.qpid.server.store.BrokerStoreUpgraderAndRecoverer.MODEL9_MAPPING_FOR_RENAME_TO_ALLOW_DENY_CONTEXT_VARIABLES;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.qpid.server.management.plugin.controller.GenericLegacyConfiguredObject;
import org.apache.qpid.server.management.plugin.controller.LegacyConfiguredObject;
import org.apache.qpid.server.management.plugin.controller.LegacyManagementController;
import org.apache.qpid.server.management.plugin.controller.TypeController;
import org.apache.qpid.server.model.ConfiguredObject;

public class LegacyCategoryController_v8_0 extends LegacyCategoryController
{
    private static final Map<String, String> NEW_TO_OLD = MODEL9_MAPPING_FOR_RENAME_TO_ALLOW_DENY_CONTEXT_VARIABLES
            .entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));

    LegacyCategoryController_v8_0(final LegacyManagementController legacyManagementController,
                                  final String type,
                                  final String parentCategory,
                                  final String defaultType,
                                  final Set<TypeController> typeControllersByCategory)
    {
        super(legacyManagementController, type, parentCategory, defaultType, typeControllersByCategory);
    }


    @Override
    protected LegacyConfiguredObject convertNextVersionLegacyConfiguredObject(final LegacyConfiguredObject object)
    {
        final LegacyConfiguredObject_v8_0 converted = new LegacyConfiguredObject_v8_0(getManagementController(),
                                                                                      object,
                                                                                      getCategory());
        if (LegacyCategoryControllerFactory.CATEGORY_VIRTUAL_HOST.equals(getCategory())
            || LegacyCategoryControllerFactory.CATEGORY_BROKER.equals(getCategory()))
        {
            return new ContainerDecorator(converted);
        }
        return converted;
    }

    @Override
    protected Map<String, Object> convertAttributesToNextVersion(final ConfiguredObject<?> root,
                                                                 final List<String> path,
                                                                 final Map<String, Object> attributes)
    {
        Map<String, Object> nextVersionAttributes;
        if (attributes.containsKey("context"))
        {
            nextVersionAttributes = convertContext(attributes);
        }
        else
        {
            nextVersionAttributes = attributes;
        }
        return super.convertAttributesToNextVersion(root, path, nextVersionAttributes);
    }

    private Map<String, Object> convertContext(final Map<String, Object> attributes)
    {
        final Object context = attributes.get("context");
        if (context instanceof Map)
        {
            Map<String, Object> nextVersionAttributes = new HashMap<>(attributes);
            @SuppressWarnings("unchecked") final Map<String, String> oldContext = (Map<String, String>) context;
            final Map<String, String> newContext = new HashMap<>(oldContext);
            MODEL9_MAPPING_FOR_RENAME_TO_ALLOW_DENY_CONTEXT_VARIABLES.forEach((oldName, newName) -> {
                if (newContext.containsKey(oldName))
                {
                    final String value = newContext.remove(oldName);
                    newContext.put(newName, value);
                }
            });
            nextVersionAttributes.put("context", newContext);
            return nextVersionAttributes;
        }
        return attributes;
    }

    static class LegacyConfiguredObject_v8_0 extends GenericLegacyConfiguredObject
    {


        LegacyConfiguredObject_v8_0(final LegacyManagementController managementController,
                                    final LegacyConfiguredObject nextVersionLegacyConfiguredObject,
                                    final String category)
        {
            super(managementController, nextVersionLegacyConfiguredObject, category);
        }

        @Override
        public Object getAttribute(final String name)
        {
            Object value = super.getAttribute(name);
            if ("context".equals(name))
            {
                return convertContext(value);
            }
            return value;
        }


        @Override
        public Object getActualAttribute(final String name)
        {
            Object value = super.getActualAttribute(name);
            if ("context".equals(name))
            {
                return convertContext(value);
            }
            return value;
        }

        @Override
        public String getContextValue(final String contextKey)
        {
            String name =
                    MODEL9_MAPPING_FOR_RENAME_TO_ALLOW_DENY_CONTEXT_VARIABLES.getOrDefault(contextKey, contextKey);
            return super.getContextValue(name);
        }

        private Object convertContext(final Object value)
        {
            if (value instanceof Map)
            {
                Map<String, String> contextMap = (Map<String, String>) value;
                return contextMap.entrySet().stream().collect(Collectors.toMap(
                        e -> renameVariable(e.getKey()),
                        Map.Entry::getValue));
            }
            return null;
        }

        private String renameVariable(final String name)
        {
            return NEW_TO_OLD.getOrDefault(name, name);
        }
    }
}
