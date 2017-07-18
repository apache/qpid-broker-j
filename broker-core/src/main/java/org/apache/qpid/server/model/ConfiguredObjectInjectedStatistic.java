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
import java.lang.reflect.Modifier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.util.ServerScopedRuntimeException;

final public class ConfiguredObjectInjectedStatistic<C extends ConfiguredObject, T extends Number>
        extends ConfiguredObjectInjectedAttributeOrStatistic<C, T> implements ConfiguredObjectStatistic<C, T>
{

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfiguredObjectInjectedStatistic.class);

    private final String _description;
    private final Method _method;
    private final StatisticUnit _units;
    private final StatisticType _type;
    private final String _label;
    private final Object[] _staticParams;

    public ConfiguredObjectInjectedStatistic(final String name,
                                             final Method method,
                                             final Object[] staticParams,
                                             final String description,
                                             final TypeValidator typeValidator,
                                             final StatisticUnit units,
                                             final StatisticType type,
                                             final String label)
    {
        super(name,
              (Class<T>) AttributeValueConverter.getTypeFromMethod(method), method.getGenericReturnType(), typeValidator);
        _units = units;
        _type = type;
        _label = label;
        _staticParams = staticParams == null ? new Object[0] : staticParams;
        if(!(method.getParameterTypes().length == 1 + _staticParams.length
             && ConfiguredObject.class.isAssignableFrom(method.getParameterTypes()[0])
             && Modifier.isStatic(method.getModifiers())
             && Number.class.isAssignableFrom(AttributeValueConverter.getTypeFromMethod(method))))
        {
            throw new IllegalArgumentException("Injected statistic method must be static, have first argument which inherits from ConfiguredObject, and return a Number");
        }
        final Class<?>[] methodParamTypes = method.getParameterTypes();
        for(int i = 0; i < _staticParams.length; i++)
        {
            if(methodParamTypes[i+1].isPrimitive() && _staticParams[i] == null)
            {
                throw new IllegalArgumentException("Static parameter has null value, but the " + methodParamTypes[i+1].getSimpleName() + " type is a primitive");
            }
            if(!AttributeValueConverter.convertPrimitiveToBoxed(methodParamTypes[i+1]).isAssignableFrom(_staticParams[i].getClass()))
            {
                throw new IllegalArgumentException("Static parameter cannot be assigned value as it is of incompatible type");
            }
        }

        _method = method;
        method.setAccessible(true);
        _description = description;

    }

    @Override
    public String getDescription()
    {
        return _description;
    }

    @Override
    public StatisticUnit getUnits()
    {
        return _units;
    }

    @Override
    public StatisticType getStatisticType()
    {
        return _type;
    }

    @Override
    public String getLabel()
    {
        return _label;
    }

    @Override
    public T getValue(final C configuredObject)
    {
        try
        {
            Object[] params = new Object[1+_staticParams.length];
            params[0] = configuredObject;
            for(int i = 0; i < _staticParams.length; i++)
            {
                params[i+1] = _staticParams[i];
            }
            return (T) _method.invoke(null, params);
        }
        catch (IllegalAccessException e)
        {
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
}
