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
package org.apache.qpid.disttest.client.utils;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class BeanUtils
{
    private static final Map<Class, Class> PRIMITIVE_TYPES = Collections.synchronizedMap(new HashMap<Class, Class>());

    static
    {
        PRIMITIVE_TYPES.put(Boolean.TYPE, Boolean.class);
        PRIMITIVE_TYPES.put(Byte.TYPE, Byte.class);
        PRIMITIVE_TYPES.put(Character.TYPE, Character.class);
        PRIMITIVE_TYPES.put(Short.TYPE, Short.class);
        PRIMITIVE_TYPES.put(Integer.TYPE, Integer.class);
        PRIMITIVE_TYPES.put(Long.TYPE, Long.class);
        PRIMITIVE_TYPES.put(Float.TYPE, Float.class);
        PRIMITIVE_TYPES.put(Double.TYPE, Double.class);
    }

    public static void setProperty(Object bean, String propertyName, Object propertyValue) throws IllegalAccessException, InvocationTargetException, IntrospectionException, NoSuchMethodException
    {
        PropertyDescriptor propertyDescriptor = getPropertyDescriptor(bean, propertyName);
        Method writeMethod = propertyDescriptor.getWriteMethod();
        Class expectedType = writeMethod.getParameterTypes()[0];
        if (expectedType.isPrimitive())
        {
            expectedType = PRIMITIVE_TYPES.get(expectedType);
        }
        if (propertyValue != null && !expectedType.isAssignableFrom(propertyValue.getClass()))
        {
            propertyValue = convertPropertyValue(propertyValue, expectedType);
        }
        try
        {
            propertyDescriptor.getWriteMethod().invoke(bean, propertyValue);
        }
        catch (IllegalArgumentException e)
        {
            System.err.println("expectedType.getName(): " + expectedType.getName());
            System.err.println("propertyValue.getClass(): " + (propertyValue == null ? "null" : propertyValue.getClass().getName()));

            throw e;
        }
    }

    private static Object convertPropertyValue(Object propertyValue, Class expectedType) throws IllegalAccessException, InvocationTargetException
    {
        if (propertyValue.getClass() == String.class)
        {
            try
            {
                Constructor constructor = expectedType.getConstructor(String.class);
                propertyValue = constructor.newInstance(propertyValue);
            }
            catch (InstantiationException | NoSuchMethodException e)
            {
                // Ignore exceptions
            }
        }
        else if (propertyValue instanceof Number && Number.class.isAssignableFrom(expectedType))
        {
            if (expectedType == Long.class)
            {
                propertyValue = ((Number)propertyValue).longValue();
            }
            else if (expectedType == Integer.class)
            {
                propertyValue = ((Number)propertyValue).intValue();
            }
            else if (expectedType == Short.class)
            {
                propertyValue = ((Number)propertyValue).shortValue();
            }
            else if (expectedType == Byte.class)
            {
                propertyValue = ((Number)propertyValue).byteValue();
            }
            else if (expectedType == Double.class)
            {
                propertyValue = ((Number)propertyValue).doubleValue();
            }
            else if (expectedType == Float.class)
            {
                propertyValue = ((Number)propertyValue).floatValue();
            }
        }
        return propertyValue;
    }

    public static Object getProperty(Object bean, String propertyName) throws IntrospectionException, IllegalAccessException, InvocationTargetException, NoSuchMethodException
    {
        PropertyDescriptor propertyDescriptor = getPropertyDescriptor(bean, propertyName);
        return propertyDescriptor.getReadMethod().invoke(bean);
    }

    public static void copyProperties(Object bean, Map<String, String> properties) throws IllegalAccessException, InvocationTargetException, IntrospectionException
    {
        for (Map.Entry<String, String> entry : properties.entrySet())
        {
            try
            {
                setProperty(bean, entry.getKey(), entry.getValue());
            }
            catch (NoSuchMethodException e)
            {
                // Ignore
            }
        }
    }

    public static PropertyDescriptor getPropertyDescriptor(Object bean, String propertyName) throws IntrospectionException, NoSuchMethodException
    {
        for (PropertyDescriptor propertyDescriptor : getPropertyDescriptors(bean))
        {
            if (propertyDescriptor.getName().equals(propertyName))
            {
                return propertyDescriptor;
            }
        }
        throw new NoSuchMethodException(propertyName);
    }

    public static PropertyDescriptor[] getPropertyDescriptors(Object bean) throws IntrospectionException
    {
        BeanInfo beanInfo = Introspector.getBeanInfo(bean.getClass());
        return beanInfo.getPropertyDescriptors();
    }
}
