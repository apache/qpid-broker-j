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
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.util.ServerScopedRuntimeException;

public class ConfiguredDerivedInjectedAttribute<C extends ConfiguredObject, T>
        extends ConfiguredObjectInjectedAttributeOrStatistic<C, T> implements ConfiguredObjectInjectedAttribute<C, T>
{

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfiguredDerivedInjectedAttribute.class);

    private final Pattern _secureValuePattern;
    private final boolean _secure;
    private final boolean _persisted;
    private final boolean _oversized;
    private final String _oversizedAltText;
    private final String _description;
    private final Method _method;
    private final Object[] _staticParams;

    public ConfiguredDerivedInjectedAttribute(final String name,
                                              final Method method,
                                              final Object[] staticParams,
                                              final boolean secure,
                                              final boolean persisted,
                                              final String secureValueFilter,
                                              final boolean oversized,
                                              final String oversizedAltText,
                                              final String description,
                                              final TypeValidator typeValidator)
    {
        super(name, (Class<T>) AttributeValueConverter.getTypeFromMethod(method),
              method.getGenericReturnType(), typeValidator);

        _staticParams = staticParams == null ? new Object[0] : staticParams;

        if(!(method.getParameterTypes().length == 1 + _staticParams.length
             && ConfiguredObject.class.isAssignableFrom(method.getParameterTypes()[0])
             && Modifier.isStatic(method.getModifiers())))
        {
            throw new IllegalArgumentException("Injected derived attribute method must be static, and have an initial argument which inherits from ConfiguredObject");
        }
        _method = method;
        method.setAccessible(true);

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


        _secure = secure;
        _persisted = persisted;
        _oversized = oversized;
        _oversizedAltText = oversizedAltText;
        _description = description;

        if (secureValueFilter == null || "".equals(secureValueFilter))
        {
            _secureValuePattern = null;
        }
        else
        {
            _secureValuePattern = Pattern.compile(secureValueFilter);
        }
    }

    @Override
    public boolean isAutomated()
    {
        return false;
    }

    @Override
    public boolean isDerived()
    {
        return true;
    }

    @Override
    public boolean isSecure()
    {
        return _secure;
    }

    @Override
    public boolean isPersisted()
    {
        return _persisted;
    }

    @Override
    public boolean isOversized()
    {
        return _oversized;
    }

    @Override
    public boolean updateAttributeDespiteUnchangedValue()
    {
        return false;
    }

    @Override
    public String getOversizedAltText()
    {
        return _oversizedAltText;
    }

    @Override
    public String getDescription()
    {
        return _description;
    }

    @Override
    public Pattern getSecureValueFilter()
    {
        return _secureValuePattern;
    }

    @Override
    public boolean isSecureValue(final Object value)
    {
        Pattern filter;
        return isSecure() &&
               ((filter = getSecureValueFilter()) == null || filter.matcher(String.valueOf(value)).matches());
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
