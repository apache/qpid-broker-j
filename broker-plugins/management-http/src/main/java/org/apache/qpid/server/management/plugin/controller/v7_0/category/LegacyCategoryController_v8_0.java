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

import static org.apache.qpid.server.store.UpgraderHelper.MODEL9_MAPPING_FOR_RENAME_TO_ALLOW_DENY_CONTEXT_VARIABLES;

import java.util.Collection;
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
import org.apache.qpid.server.store.UpgraderHelper;

public class LegacyCategoryController_v8_0 extends LegacyCategoryController
{
    private static final Map<String, String> NEW_TO_OLD =
            UpgraderHelper.reverse(MODEL9_MAPPING_FOR_RENAME_TO_ALLOW_DENY_CONTEXT_VARIABLES);

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
            nextVersionAttributes = convertContextToNextVersion(attributes);
        }
        else
        {
            nextVersionAttributes = attributes;
        }
        return super.convertAttributesToNextVersion(root, path, nextVersionAttributes);
    }

    private Map<String, Object> convertContextToNextVersion(final Map<String, Object> attributes)
    {
        final Object context = attributes.get("context");
        if (context instanceof Map)
        {
            @SuppressWarnings("unchecked") final Map<String, String> oldContext = (Map<String, String>) context;
            final Map<String, String> newContext = UpgraderHelper.renameContextVariables(oldContext,
                                                                                         MODEL9_MAPPING_FOR_RENAME_TO_ALLOW_DENY_CONTEXT_VARIABLES);
            final Map<String, Object> nextVersionAttributes = new HashMap<>(attributes);
            nextVersionAttributes.put("context", newContext);
            return nextVersionAttributes;
        }
        return attributes;
    }

    static class LegacyConfiguredObject_v8_0 extends GenericLegacyConfiguredObject
    {
        private static final Map<String, String> ALLOW_DENY_TO_WHITE_BLACK_MAPPING = new HashMap<>();

        static
        {
            ALLOW_DENY_TO_WHITE_BLACK_MAPPING.put("tlsProtocolAllowList", "tlsProtocolWhiteList");
            ALLOW_DENY_TO_WHITE_BLACK_MAPPING.put("tlsProtocolDenyList", "tlsProtocolBlackList");
            ALLOW_DENY_TO_WHITE_BLACK_MAPPING.put("tlsCipherSuiteAllowList", "tlsCipherSuiteWhiteList");
            ALLOW_DENY_TO_WHITE_BLACK_MAPPING.put("tlsCipherSuiteDenyList", "tlsCipherSuiteBlackList");
        }

        private static final Map<String, String> WHITE_BLACK_TO_ALLOW_DENY_MAPPING =
                UpgraderHelper.reverse(ALLOW_DENY_TO_WHITE_BLACK_MAPPING);

        LegacyConfiguredObject_v8_0(final LegacyManagementController managementController,
                                    final LegacyConfiguredObject nextVersionLegacyConfiguredObject,
                                    final String category)
        {
            super(managementController, nextVersionLegacyConfiguredObject, category);
        }

        @Override
        public Object getAttribute(final String name)
        {
            Object value;
            if ("context".equals(name))
            {
                return convertContextToModelVersion(super.getAttribute(name));
            }
            else if (isPortOrAuthenticationPovider() && WHITE_BLACK_TO_ALLOW_DENY_MAPPING.containsKey(name))
            {
                value = super.getAttribute(WHITE_BLACK_TO_ALLOW_DENY_MAPPING.getOrDefault(name, name));
            }
            else
            {
                value = super.getAttribute(name);
            }
            return value;
        }

        private boolean isPortOrAuthenticationPovider()
        {
            return "Port".equals(getCategory()) || "AuthenticationProvider".equals(getCategory());
        }

        @Override
        public Collection<String> getAttributeNames()
        {
            final Collection<String> attributeNames = super.getAttributeNames();
            if (isPortOrAuthenticationPovider())
            {
                return attributeNames.stream()
                                     .map(i -> ALLOW_DENY_TO_WHITE_BLACK_MAPPING.getOrDefault(i, i))
                                     .collect(Collectors.toList());
            }
            return attributeNames;
        }

        @Override
        public Object getActualAttribute(final String name)
        {
            Object value = super.getActualAttribute(name);
            if ("context".equals(name))
            {
                return convertContextToModelVersion(value);
            }
            return value;
        }

        @Override
        public String getContextValue(final String contextKey)
        {
            final String nextVersionName =
                    MODEL9_MAPPING_FOR_RENAME_TO_ALLOW_DENY_CONTEXT_VARIABLES.getOrDefault(contextKey, contextKey);
            return super.getContextValue(nextVersionName);
        }

        private Object convertContextToModelVersion(final Object value)
        {
            if (value instanceof Map)
            {
                return UpgraderHelper.renameContextVariables((Map<String, String>) value, NEW_TO_OLD);
            }
            return null;
        }
    }
}
