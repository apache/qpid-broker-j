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
package org.apache.qpid.server.jmx.mbeans;

import junit.framework.TestCase;

import org.apache.qpid.server.jmx.DefaultManagedObject;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;

public class MBeanTestUtils
{

    public static void assertMBeanAttribute(DefaultManagedObject managedObject, String jmxAttributeName, Object expectedValue) throws Exception
    {
        Object actualValue = getProperty(managedObject, jmxAttributeName);
        TestCase.assertEquals("Attribute " + jmxAttributeName + " has unexpected value", expectedValue, actualValue);
    }

    public static void setMBeanAttribute(DefaultManagedObject managedObject, String jmxAttributeName, Object newValue) throws Exception
    {
        setProperty(managedObject, jmxAttributeName, newValue);
    }

    private static void setProperty(Object bean, String propertyName, Object propertyValue) throws IllegalAccessException, InvocationTargetException, IntrospectionException, NoSuchMethodException
    {
        PropertyDescriptor propertyDescriptor = getPropertyDescriptor(bean, propertyName);
        propertyDescriptor.getWriteMethod().invoke(bean, propertyValue);
    }

    private static Object getProperty(Object bean, String propertyName) throws IntrospectionException, IllegalAccessException, InvocationTargetException, NoSuchMethodException
    {
        PropertyDescriptor propertyDescriptor = getPropertyDescriptor(bean, propertyName);
        return propertyDescriptor.getReadMethod().invoke(bean);
    }

    private static PropertyDescriptor getPropertyDescriptor(Object bean, String propertyName) throws IntrospectionException, NoSuchMethodException
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

    private static PropertyDescriptor[] getPropertyDescriptors(Object bean) throws IntrospectionException
    {
        BeanInfo beanInfo = Introspector.getBeanInfo(bean.getClass());
        return beanInfo.getPropertyDescriptors();
    }

}
