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
package org.apache.qpid.server.management.plugin.controller;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.qpid.server.model.Named;

class LegacyConfiguredObjectToMapConverter
{
    private static final String STATISTICS_MAP_KEY = "statistics";
    private static final String NAME = "name";
    private static final String CONTEXT = "context";

    private final LegacyManagementController _managementMetadata;

    LegacyConfiguredObjectToMapConverter(final LegacyManagementController managementMetadata)
    {
        _managementMetadata = managementMetadata;
    }

    Map<String, Object> convertManageableToMap(final LegacyConfiguredObject legacyConfiguredObjectObject,
                                               final int depth,
                                               final boolean actuals,
                                               final int oversizeThreshold,
                                               final boolean excludeInheritedContext)
    {
        final Map<String, Object> object = new LinkedHashMap<>();

        incorporateAttributesIntoMap(legacyConfiguredObjectObject,
                                     object,
                                     actuals,
                                     excludeInheritedContext,
                                     oversizeThreshold);
        incorporateStatisticsIntoMap(legacyConfiguredObjectObject, object);

        if (depth > 0)
        {
            incorporateChildrenIntoMap(legacyConfiguredObjectObject,
                                       object,
                                       depth,
                                       actuals,
                                       oversizeThreshold,
                                       excludeInheritedContext);
        }
        return object;
    }

    private void incorporateAttributesIntoMap(
            final LegacyConfiguredObject confObject,
            final Map<String, Object> object,
            final boolean useActualValues,
            final boolean excludeInheritedContext,
            final int oversizeThreshold)
    {

        for (String name : confObject.getAttributeNames())
        {
            Object value = useActualValues ? confObject.getActualAttribute(name) : confObject.getAttribute(name);
            if (value instanceof LegacyConfiguredObject)
            {
                object.put(name, ((LegacyConfiguredObject) value).getAttribute(NAME));
            }
            else if (CONTEXT.equals(name))
            {
                Map<String, Object> contextValues = collectContext(confObject,
                                                                   excludeInheritedContext,
                                                                   useActualValues);

                if (!contextValues.isEmpty())
                {
                    object.put(CONTEXT, contextValues);
                }
            }
            else if (value instanceof Collection)
            {
                List<Object> converted = new ArrayList<>();
                for (Object member : (Collection) value)
                {
                    if (member instanceof LegacyConfiguredObject)
                    {
                        converted.add(((LegacyConfiguredObject) member).getAttribute(NAME));
                    }
                    else
                    {
                        converted.add(member);
                    }
                }
                object.put(name, converted);
            }
            else if (value instanceof Named)
            {
                object.put(name, ((Named) value).getName());
            }
            else if (value != null)
            {
                if (confObject.isSecureAttribute(name))
                {
                    value = confObject.getAttribute(name);
                }

                if (confObject.isOversizedAttribute(name) && !useActualValues)
                {
                    String valueString = String.valueOf(value);
                    if (valueString.length() > oversizeThreshold)
                    {
                        object.put(name, String.valueOf(value).substring(0, oversizeThreshold - 4) + "...");
                    }
                    else
                    {
                        object.put(name, value);
                    }
                }
                else
                {
                    object.put(name, value);
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> collectContext(final LegacyConfiguredObject configuredObject,
                                               final boolean excludeInheritedContext,
                                               final boolean useActualValues)
    {
        final Map<String, Object> actualContext = new HashMap<>();
        if (excludeInheritedContext)
        {
            final Object value = configuredObject.getActualAttribute(CONTEXT);
            if (value instanceof Map)
            {
                actualContext.putAll((Map<String, String>) value);
            }
        }
        else
        {
            actualContext.putAll(System.getenv());
            actualContext.putAll((Map) System.getProperties());
            collectInheritedActualContext(configuredObject, actualContext);
        }

        if (useActualValues)
        {
            return actualContext;
        }
        else
        {
            final Map<String, Object> effectiveContext = new HashMap<>();
            for (String contextKey : actualContext.keySet())
            {
                effectiveContext.put(contextKey, configuredObject.getContextValue(contextKey));
            }
            return effectiveContext;
        }
    }

    @SuppressWarnings("unchecked")
    private void collectInheritedActualContext(final LegacyConfiguredObject confObject,
                                               final Map<String, Object> contextValues)
    {
        final Collection<String> parents = _managementMetadata.getParentTypes(confObject.getCategory());
        if (parents != null && !parents.isEmpty())
        {
            final LegacyConfiguredObject parent = confObject.getParent(parents.iterator().next());
            if (parent != null)
            {
                collectInheritedActualContext(parent, contextValues);
            }
        }
        final Object value = confObject.getActualAttribute(CONTEXT);
        if (value instanceof Map)
        {
            contextValues.putAll((Map<String, Object>) value);
        }
    }

    private void incorporateStatisticsIntoMap(final LegacyConfiguredObject confObject, final Map<String, Object> object)
    {
        final Map<String, Object> statMap = new TreeMap<>(confObject.getStatistics());
        if (!statMap.isEmpty())
        {
            object.put(STATISTICS_MAP_KEY, statMap);
        }
    }

    private void incorporateChildrenIntoMap(
            final LegacyConfiguredObject confObject,
            final Map<String, Object> object,
            final int depth,
            final boolean actuals,
            final int oversizeThreshold,
            final boolean excludeInheritedContext)
    {
        Collection<String> childTypes = _managementMetadata.getChildrenCategories(confObject.getCategory());
        if (childTypes != null && !childTypes.isEmpty())
        {
            List<String> types = new ArrayList<>(childTypes);
            Collections.sort(types);

            for (String childType : types)
            {
                Collection<LegacyConfiguredObject> children = confObject.getChildren(childType);
                if (children != null && !children.isEmpty())
                {
                    List<LegacyConfiguredObject> sortedChildren = new ArrayList<>(children);
                    sortedChildren.sort(Comparator.comparing(o -> ((String) o.getAttribute(NAME))));

                    List<Map<String, Object>> childObjects = sortedChildren.stream()
                                                                           .sorted(Comparator.comparing(o -> ((String) o.getAttribute(NAME))))
                                                                           .map(child -> convertManageableToMap(child,
                                                                                                                depth
                                                                                                                - 1,
                                                                                                                actuals,
                                                                                                                oversizeThreshold,
                                                                                                                excludeInheritedContext))
                                                                           .collect(Collectors.toList());
                    String childTypeSingular = childType.toLowerCase();
                    object.put(childTypeSingular + (childTypeSingular.endsWith("s") ? "es" : "s"), childObjects);
                }
            }
        }
    }
}
