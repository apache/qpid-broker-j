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
package org.apache.qpid.server.model;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ConfigurationExtractor
{
    private static final Set<String> EXCLUDED_ATTRIBUTES = new HashSet<>(Arrays.asList(ConfiguredObject.ID,
                                                                                       ConfiguredObject.LAST_UPDATED_BY,
                                                                                       ConfiguredObject.LAST_UPDATED_TIME,
                                                                                       ConfiguredObject.CREATED_BY,
                                                                                       ConfiguredObject.CREATED_TIME));

    public Map<String,Object> extractConfig(ConfiguredObject<?> object, final boolean includeSecure)
    {
        Map<String, Object> results = new LinkedHashMap<>();


        results.putAll(extractAttributeValues(object, includeSecure));

        results.putAll(extractNonPrimaryParentsAsAttributes(object));

        results.putAll(extractChildren(object, includeSecure));

        return results;
    }


    private Map<String,Object> extractAttributeValues(final ConfiguredObject<?> object,
                                                      final boolean includeSecure)
    {
        final Model model = object.getModel();
        final ConfiguredObjectTypeRegistry typeRegistry = model.getTypeRegistry();

        Map<String, Object> results = new LinkedHashMap<>();
        Map<String, ConfiguredObjectAttribute<?, ?>> attributeDefinitions = new HashMap<>();
        for (ConfiguredObjectAttribute<?, ?> attributeDefinition : typeRegistry.getAttributes(object.getClass()))
        {
            attributeDefinitions.put(attributeDefinition.getName(), attributeDefinition);
        }

        for (Map.Entry<String, Object> attr : object.getActualAttributes().entrySet())
        {
            if (!EXCLUDED_ATTRIBUTES.contains(attr.getKey()))
            {
                final ConfiguredObjectAttribute attributeDefinition = attributeDefinitions.get(attr.getKey());
                if (attributeDefinition.isSecureValue(attributeDefinition.getValue(object)))
                {
                    results.put(attr.getKey(),
                                extractSecureValue(object, includeSecure, attr, attributeDefinition));
                }
                else if (ConfiguredObject.class.isAssignableFrom(attributeDefinition.getType()))
                {
                    results.put(attr.getKey(), extractConfiguredObjectValue(object, attr, attributeDefinition));
                }
                else if (Collection.class.isAssignableFrom(attributeDefinition.getType())
                         && (attr.getValue() instanceof Collection)
                         && hasConfiguredObjectTypeArguments(attributeDefinition, 1))
                {
                    results.put(attr.getKey(),
                                extractConfiguredObjectCollectionValue(object, attr, attributeDefinition));
                }
                else if (Map.class.isAssignableFrom(attributeDefinition.getType())
                         && (attr.getValue() instanceof Map)
                         && hasConfiguredObjectTypeArguments(attributeDefinition, 2))
                {
                    results.put(attr.getKey(),
                                extractConfiguredObjectMapValue(object, attr, attributeDefinition));
                }
                else
                {
                    results.put(attr.getKey(), attr.getValue());
                }
            }
        }
        return results;
    }


    private Map extractConfiguredObjectMapValue(final ConfiguredObject<?> object,
                                                final Map.Entry<String, Object> attr,
                                                final ConfiguredObjectAttribute attributeDefinition)
    {
        Map mapResults = new LinkedHashMap<>();
        Map values = (Map) attributeDefinition.getValue(object);

        Iterator<Map.Entry> valuesIter = values.entrySet().iterator();
        for (Map.Entry attrValue : ((Map<?,?>) attr.getValue()).entrySet())
        {
            Object key;
            Object value;
            Map.Entry obj = valuesIter.next();
            if(obj.getKey() instanceof ConfiguredObject)
            {
                Object attrKeyVal = attrValue.getKey();
                if(!(attrKeyVal instanceof String) || ((ConfiguredObject)obj.getKey()).getId().toString().equals(attrKeyVal))
                {
                    key = ((ConfiguredObject)obj.getKey()).getName();
                }
                else
                {
                    key = attrValue.getKey();
                }
            }
            else
            {
                key = attrValue.getKey();
            }

            if(obj.getValue() instanceof ConfiguredObject)
            {
                Object attrValueVal = attrValue.getValue();
                if(!(attrValueVal instanceof String) || ((ConfiguredObject)obj.getValue()).getId().toString().equals(attrValueVal))
                {
                    value = ((ConfiguredObject)obj.getValue()).getName();
                }
                else
                {
                    value = attrValue.getValue();
                }
            }
            else
            {
                value = attrValue.getValue();
            }


            mapResults.put(key, value);
        }
        return mapResults;
    }

    private List<Object> extractConfiguredObjectCollectionValue(final ConfiguredObject<?> object,
                                                                final Map.Entry<String, Object> attr,
                                                                final ConfiguredObjectAttribute attributeDefinition)
    {
        List<Object> listResults = new ArrayList<>();
        Collection<? extends ConfiguredObject> values =
                (Collection<? extends ConfiguredObject>) attributeDefinition.getValue(object);

        Iterator<? extends ConfiguredObject> valuesIter = values.iterator();
        for (Object attrValue : (Collection) attr.getValue())
        {
            ConfiguredObject obj = valuesIter.next();
            if (!(attrValue instanceof String) || obj.getId().toString().equals(attrValue))
            {
                listResults.add(obj.getName());
            }
            else
            {
                listResults.add(attrValue);
            }
        }
        return listResults;
    }

    private Object extractConfiguredObjectValue(final ConfiguredObject<?> object,
                                                final Map.Entry<String, Object> attr,
                                                final ConfiguredObjectAttribute attributeDefinition)
    {
        final Object value;
        ConfiguredObject<?> obj = (ConfiguredObject<?>) attributeDefinition.getValue(object);
        if (!(attr.getValue() instanceof String) || obj.getId().toString().equals(attr.getValue()))
        {
            value = obj.getName();
        }
        else
        {
            value = attr.getValue();
        }
        return value;
    }

    private Object extractSecureValue(final ConfiguredObject<?> object,
                                      final boolean includeSecure,
                                      final Map.Entry<String, Object> attr,
                                      final ConfiguredObjectAttribute attributeDefinition)
    {
        final Object value;
        if(includeSecure)
        {
            if(attributeDefinition.isSecure() && object.hasEncrypter())
            {
                value = attributeDefinition.getValue(object);
            }
            else
            {
                value = attr.getValue();
            }
        }
        else
        {
            value = AbstractConfiguredObject.SECURED_STRING_VALUE;
        }
        return value;
    }

    private boolean hasConfiguredObjectTypeArguments(ConfiguredObjectAttribute attributeDefinition, int paramCount)
    {
        if(attributeDefinition.getGenericType() instanceof ParameterizedType
           && ((ParameterizedType) attributeDefinition.getGenericType()).getActualTypeArguments().length == paramCount)
        {
            for(int i = 0 ;  i < paramCount; i++)
            {
                if(isConfiguredObjectTypeArgument(attributeDefinition, i))
                {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean isConfiguredObjectTypeArgument(ConfiguredObjectAttribute attributeDefinition, int paramIndex)
    {
        return ConfiguredObject.class.isAssignableFrom(getTypeParameterClass(attributeDefinition, paramIndex));
    }

    private Class getTypeParameterClass(ConfiguredObjectAttribute attributeDefinition, int paramIndex)
    {
        final Type argType = ((ParameterizedType) attributeDefinition
                .getGenericType()).getActualTypeArguments()[paramIndex];

        return argType instanceof Class ? (Class) argType : (Class) ((ParameterizedType)argType).getRawType();
    }

    private Map<String, Object> extractNonPrimaryParentsAsAttributes(final ConfiguredObject<?> object)
    {
        final Model model = object.getModel();
        Map<String, Object> results = new LinkedHashMap<>();

        Collection<Class<? extends ConfiguredObject>> parentTypes = model.getParentTypes(object.getCategoryClass());

        if(parentTypes.size() > 1)
        {
            Iterator<Class<? extends ConfiguredObject>>
                    parentClassIter = parentTypes.iterator();

            for(int i = 1; i < parentTypes.size(); i++)
            {
                Class<? extends ConfiguredObject> parentClass = parentClassIter.next();
                ConfiguredObject parent = object.getParent(parentClass);
                if(parent != null)
                {
                    results.put(parentClass.getSimpleName().toLowerCase(), parent.getName());
                }
            }
        }

        return results;
    }


    private Map<String, Object> extractChildren(final ConfiguredObject<?> object,
                                                final boolean includeSecure)
    {
        final Model model = object.getModel();

        Map<String, Object> results = new LinkedHashMap<>();

        if(!(object.getCategoryClass().getAnnotation(ManagedObject.class).managesChildren()
             || object.getTypeClass().getAnnotation(ManagedObject.class).managesChildren()))
        {

            for (Class<? extends ConfiguredObject> childClass : model
                    .getChildTypes(object.getCategoryClass()))
            {
                ArrayList<Class<? extends ConfiguredObject>> parentClasses =
                        new ArrayList<>(model.getParentTypes(childClass));
                if (parentClasses.get(parentClasses.size() - 1).equals(object.getCategoryClass()))
                {
                    List<Map<String, Object>> children = new ArrayList<>();
                    for (ConfiguredObject child : object.getChildren(childClass))
                    {
                        if (child.isDurable())
                        {
                            children.add(extractConfig(child, includeSecure));
                        }
                    }
                    if (!children.isEmpty())
                    {
                        String singularName = childClass.getSimpleName().toLowerCase();
                        String attrName = singularName + (singularName.endsWith("s") ? "es" : "s");
                        results.put(attrName, children);
                    }
                }
            }
        }

        return results;
    }


}
