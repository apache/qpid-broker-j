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
package org.apache.qpid.server.management.plugin.servlet.rest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObjectAttribute;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.Named;

public class ConfiguredObjectToMapConverter
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfiguredObjectToMapConverter.class);
    /** Name of the key used for the statistics map */
    public static final String STATISTICS_MAP_KEY = "statistics";

    private static Set<String> CONFIG_EXCLUDED_ATTRIBUTES =
            new HashSet<>(Arrays.asList(ConfiguredObject.ID,
                                        ConfiguredObject.DURABLE,
                                        ConfiguredObject.CREATED_BY,
                                        ConfiguredObject.CREATED_TIME,
                                        ConfiguredObject.LAST_UPDATED_BY,
                                        ConfiguredObject.LAST_UPDATED_TIME));

    public Map<String, Object> convertObjectToMap(final ConfiguredObject<?> confObject,
                                                  Class<? extends ConfiguredObject> clazz,
                                                  ConverterOptions converterOptions
                                                 )
    {
        Map<String, Object> object = new LinkedHashMap<>();

        incorporateAttributesIntoMap(confObject, object, converterOptions);
        incorporateStatisticsIntoMap(confObject, object);

        if(converterOptions.getDepth() > 0)
        {
            incorporateChildrenIntoMap(confObject, clazz, object, converterOptions);
        }
        return object;
    }


    private void incorporateAttributesIntoMap(
            final ConfiguredObject<?> confObject,
            Map<String, Object> object,
            ConverterOptions converterOptions)
    {

        for(String name : confObject.getAttributeNames())
        {
            Object value =
                    converterOptions.isUseActualValues()
                            ? confObject.getActualAttributes().get(name)
                            : confObject.getAttribute(name);
            if (value instanceof ConfiguredObject)
            {
                object.put(name, ((ConfiguredObject) value).getName());
            }
            else if (ConfiguredObject.CONTEXT.equals(name))
            {
                Map<String, Object> contextValues = collectContext(confObject,
                                                                   converterOptions.isExcludeInheritedContext(),
                                                                   converterOptions.isUseActualValues());

                if (!contextValues.isEmpty())
                {
                    object.put(ConfiguredObject.CONTEXT, contextValues);
                }
            }
            else if (value instanceof Collection)
            {
                List<Object> converted = new ArrayList<>();
                for (Object member : (Collection) value)
                {
                    if (member instanceof ConfiguredObject)
                    {
                        converted.add(((ConfiguredObject) member).getName());
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
                ConfiguredObjectAttribute<?, ?> attribute = confObject.getModel()
                        .getTypeRegistry()
                        .getAttributeTypes(confObject.getClass())
                        .get(name);

                if (attribute.isSecureValue(value))
                {
                    // do not expose actual secure attribute value
                    // getAttribute() returns encoded value
                    value = confObject.getAttribute(name);
                }

                if (attribute.isOversized()
                    && !converterOptions.isUseActualValues())
                {
                    String valueString = String.valueOf(value);
                    if (valueString.length() > converterOptions.getOversizeThreshold())
                    {

                        String replacementValue = "".equals(attribute.getOversizedAltText())
                                ? String.valueOf(value).substring(0, converterOptions.getOversizeThreshold() - 4)
                                  + "..."
                                : attribute.getOversizedAltText();

                        object.put(name, replacementValue);
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

    private Map<String, Object> collectContext(ConfiguredObject<?> configuredObject,
                                               boolean excludeInheritedContext,
                                               boolean useActualValues)
    {
        Map<String, Object> actualContext = new HashMap<>();
        if (excludeInheritedContext)
        {
            Object value = configuredObject.getActualAttributes().get(ConfiguredObject.CONTEXT);
            if (value instanceof Map)
            {
                actualContext.putAll((Map<String, String>) value);
            }
        }
        else
        {
            actualContext.putAll(configuredObject.getModel().getTypeRegistry().getDefaultContext());
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
            Map<String, Object> effectiveContext = new HashMap<>();
            for (String contextKey : actualContext.keySet())
            {
                effectiveContext.put(contextKey, configuredObject.getContextValue(String.class, contextKey));
            }
            return effectiveContext;
        }
    }

    private void collectInheritedActualContext(ConfiguredObject<?> confObject, Map<String, Object> contextValues)
    {
        ConfiguredObject parent = confObject.getParent();
        if(parent != null)
        {
            collectInheritedActualContext(parent, contextValues);
        }
        Object value = confObject.getActualAttributes().get(ConfiguredObject.CONTEXT);
        if (value instanceof Map)
        {
            contextValues.putAll((Map<String, Object>)value);
        }
    }

    private void incorporateStatisticsIntoMap(
            final ConfiguredObject<?> confObject, Map<String, Object> object)
    {

        Map<String, Object> statMap = new TreeMap<String,Object>(confObject.getStatistics());

        if(!statMap.isEmpty())
        {
            object.put(STATISTICS_MAP_KEY, statMap);
        }

    }

    private void incorporateChildrenIntoMap(
            final ConfiguredObject confObject,
            Class<? extends ConfiguredObject> clazz,
            Map<String, Object> object,
            ConverterOptions converterOptions)
    {
        List<Class<? extends ConfiguredObject>> childTypes = new ArrayList<>(confObject.getModel().getChildTypes(clazz));

        Collections.sort(childTypes, new Comparator<Class<? extends ConfiguredObject>>()
        {
            @Override
            public int compare(final Class<? extends ConfiguredObject> o1, final Class<? extends ConfiguredObject> o2)
            {
                return o1.getSimpleName().compareTo(o2.getSimpleName());
            }
        });

        ConverterOptions childConverterOptions = new ConverterOptions(converterOptions, converterOptions.getDepth() - 1);
        for(Class<? extends ConfiguredObject> childClass : childTypes)
        {
            Collection children = confObject.getChildren(childClass);
            if (children != null)
            {
                List<? extends ConfiguredObject> sortedChildren = new ArrayList<ConfiguredObject>(children);
                if (Comparable.class.isAssignableFrom(childClass))
                {
                    Collections.sort((List) sortedChildren);
                }
                else
                {
                    Collections.sort(sortedChildren, new Comparator<ConfiguredObject>()
                    {
                        @Override
                        public int compare(final ConfiguredObject o1, final ConfiguredObject o2)
                        {
                            return o1.getName().compareTo(o2.getName());
                        }
                    });
                }
                List<Map<String, Object>> childObjects = new ArrayList<>();


                for (ConfiguredObject child : sortedChildren)
                {
                    childObjects.add(convertObjectToMap(child,
                                                        childClass,
                                                        childConverterOptions));

                }

                if (!childObjects.isEmpty())
                {
                    String childTypeSingular = childClass.getSimpleName().toLowerCase();
                    object.put(childTypeSingular + (childTypeSingular.endsWith("s") ? "es" : "s"), childObjects);
                }
            }

        }
    }


    public static final class ConverterOptions
    {
        private final int _depth;
        private final boolean _useActualValues;
        private final int _oversizeThreshold;
        private final boolean _secureTransport;
        private final boolean _excludeInheritedContext;

        public ConverterOptions(ConverterOptions options, int depth)
        {
            this(depth,
                 options.isUseActualValues(),
                 options.getOversizeThreshold(),
                 options.isSecureTransport(),
                 options.isExcludeInheritedContext());
        }

        public ConverterOptions(final int depth,
                                final boolean useActualValues,
                                final int oversizeThreshold,
                                final boolean secureTransport,
                                final boolean excludeInheritedContext)
        {
            _depth = depth;
            _useActualValues = useActualValues;
            _oversizeThreshold = oversizeThreshold;
            _secureTransport = secureTransport;
            _excludeInheritedContext = excludeInheritedContext;
        }

        public int getDepth()
        {
            return _depth;
        }

        public boolean isUseActualValues()
        {
            return _useActualValues;
        }

        public int getOversizeThreshold()
        {
            return _oversizeThreshold;
        }

        public boolean isSecureTransport()
        {
            return _secureTransport;
        }

        public boolean isExcludeInheritedContext()
        {
            return _excludeInheritedContext;
        }
    }
}
