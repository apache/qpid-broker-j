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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;

import org.apache.qpid.server.util.ServerScopedRuntimeException;

abstract class ConfiguredObjectMethodAttributeOrStatistic<C extends ConfiguredObject, T>
        implements ConfiguredObjectAttributeOrStatistic<C,T>
{

    private final String _name;
    private final Class<T> _type;
    private final Method _getter;

    ConfiguredObjectMethodAttributeOrStatistic(final Method getter)
    {

        _getter = getter;
        _type = (Class<T>) AttributeValueConverter.getTypeFromMethod(getter);
        _name = AttributeValueConverter.getNameFromMethod(getter, getType());
    }

    @Override
    public String getName()
    {
        return _name;
    }

    @Override
    public Class<T> getType()
    {
        return _type;
    }

    @Override
    public Type getGenericType()
    {
        return getGetter().getGenericReturnType();
    }

    @Override
    public T getValue(C configuredObject)
    {
        try
        {
            return (T) getGetter().invoke(configuredObject);
        }
        catch (IllegalAccessException e)
        {
            // This should never happen as it would imply a getter which is not public
            throw new ServerScopedRuntimeException("Unable to get value for '"+getName()
                                                   +"' from configured object of category "
                                                   + configuredObject.getCategoryClass().getSimpleName(), e);
        }
        catch (InvocationTargetException e)
        {
            Throwable targetException = e.getTargetException();
            if(targetException instanceof RuntimeException)
            {
                throw (RuntimeException)targetException;
            }
            else if(targetException instanceof Error)
            {
                throw (Error)targetException;
            }
            else
            {
                // This should never happen as it would imply a getter which is declaring a checked exception
                throw new ServerScopedRuntimeException("Unable to get value for '"+getName()
                                                       +"' from configured object of category "
                                                       + configuredObject.getCategoryClass().getSimpleName(), e);
            }
        }

    }

    public Method getGetter()
    {
        return _getter;
    }

}
