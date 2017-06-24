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
package org.apache.qpid.server.management.amqp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObjectAttribute;
import org.apache.qpid.server.model.ConfiguredObjectCustomSerialization;

class ManagementOutputConverter
{
    private static final List<String> ID_AND_TYPE = Arrays.asList(ConfiguredObject.ID, ConfiguredObject.TYPE);
    private final ManagementNode _managementNode;

    ManagementOutputConverter(final ManagementNode managementNode)
    {
        _managementNode = managementNode;
    }

    Map<?, ?> convertToOutput(final ConfiguredObject<?> object,
                              final boolean actuals)
    {
        Map<String, Object> attributes = new LinkedHashMap<>();
        attributes.put(ManagementNode.IDENTITY_ATTRIBUTE, object.getId());
        attributes.put(ManagementNode.OBJECT_PATH, _managementNode.generatePath(object));
        attributes.put(ManagementNode.TYPE_ATTRIBUTE, _managementNode.getAmqpName(object.getTypeClass()));
        attributes.put(ManagementNode.QPID_TYPE, object.getType());

        if(object != _managementNode.getManagedObject() && !_managementNode.isSyntheticChildClass(object.getCategoryClass()))
        {
            Class<? extends ConfiguredObject> parentType = object.getModel().getParentType(object.getCategoryClass());

            if (parentType != _managementNode.getManagedObject().getCategoryClass())
            {
                attributes.put(parentType.getSimpleName().toLowerCase(), object.getParent());
            }

        }

        for(String name : object.getAttributeNames())
        {
            if(!ID_AND_TYPE.contains(name))
            {
                ConfiguredObjectAttribute<?, ?> attribute = object.getModel()
                                                                  .getTypeRegistry()
                                                                  .getAttributeTypes(object.getClass())
                                                                  .get(name);

                Object value = actuals
                        ? object.getActualAttributes().get(name)
                        : object.getAttribute(name);

                if (attribute.isSecureValue(value))
                {
                    value = object.getAttribute(name);
                }

                if (value != null)
                {
                    attributes.put(name, value);
                }
            }
        }

        return convertMapToOutput(attributes);
    }

    Object convertObjectToOutput(final Object value)
    {
        if(value == null)
        {
            return null;
        }
        else if(value instanceof String
                || value instanceof Short
                || value instanceof Integer
                || value instanceof Long
                || value instanceof Byte
                || value instanceof Boolean
                || value instanceof Character
                || value instanceof Float
                || value instanceof Double
                || value instanceof byte[])
        {
            return value;
        }
        else if(value instanceof Map)
        {
            return convertMapToOutput((Map<?,?>)value);
        }
        else if(value instanceof Collection)
        {
            return convertCollectionToOutput((Collection<?>)value);
        }
        else if(value instanceof ConfiguredObject)
        {
            return ((ConfiguredObject)value).getName();
        }
        else
        {
            for(ConfiguredObjectCustomSerialization.Converter converter : ConfiguredObjectCustomSerialization.getConverters(
                    false))
            {
                if(converter.getConversionClass().isAssignableFrom(value.getClass()))
                {
                    return convertObjectToOutput(converter.convert(value));
                }
            }

            return value.toString();
        }
    }

    private Map<Object, Object> convertMapToOutput(final Map<?, ?> attributes)
    {
        Map<Object,Object> result = new LinkedHashMap<>();
        for(Map.Entry<?,?> entry : attributes.entrySet())
        {
            result.put(convertObjectToOutput(entry.getKey()), convertObjectToOutput(entry.getValue()));
        }
        return result;
    }

    private Collection<?> convertCollectionToOutput(final Collection<?> value)
    {

        List<Object> result = new ArrayList<>();
        for(Object entry : value)
        {
            result.add(convertObjectToOutput(entry));
        }
        return result;
    }
}
