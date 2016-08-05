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
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.qpid.server.util.ServerScopedRuntimeException;

public class ConfiguredObjectInjectedOperation<C extends ConfiguredObject> implements ConfiguredObjectOperation<C>, InjectedAttributeStatisticOrOperation<C>
{
    private final Method _operation;
    private final List<OperationParameter> _params;
    private final Set<String> _validNames;
    private final TypeValidator _validator;
    private final String _name;
    private final String _description;
    private final boolean _nonModifying;
    private final boolean _secure;
    private final Object[] _staticParams;

    public ConfiguredObjectInjectedOperation(final String name,
                                             final String description,
                                             final boolean nonModifying,
                                             final boolean secure,
                                             final OperationParameter[] parameters,
                                             final Method operation,
                                             final Object[] staticParams,
                                             final TypeValidator validator)
    {
        _operation = operation;
        _name = name;
        _description = description;
        _nonModifying = nonModifying;
        _secure = secure;
        _validator = validator;
        _staticParams = staticParams == null ? new Object[0] : staticParams;

        _params = parameters == null ? Collections.<OperationParameter>emptyList() : Arrays.asList(parameters);

        Set<String> validNames = new LinkedHashSet<>();
        for(OperationParameter parameter : _params)
        {
            validNames.add(parameter.getName());
        }

        _validNames = Collections.unmodifiableSet(validNames);

        final Class<?>[] opParameterTypes = operation.getParameterTypes();

        if(!(Modifier.isStatic(operation.getModifiers())
             && Modifier.isPublic(operation.getModifiers())
             && opParameterTypes.length == _params.size() + _staticParams.length + 1
             && ConfiguredObject.class.isAssignableFrom(opParameterTypes[0])))
        {
            throw new IllegalArgumentException("Passed method must be public and static.  The first parameter must derive from ConfiguredObject, and the rest of the parameters must match the passed in specifications");
        }

        for(int i = 0; i < _staticParams.length; i++)
        {
            if(opParameterTypes[i+1].isPrimitive() && _staticParams[i] == null)
            {
                throw new IllegalArgumentException("Static parameter has null value, but the " + opParameterTypes[i+1].getSimpleName() + " type is a primitive");
            }
            if(!AttributeValueConverter.convertPrimitiveToBoxed(opParameterTypes[i+1]).isAssignableFrom(_staticParams[i].getClass()))
            {
                throw new IllegalArgumentException("Static parameter cannot be assigned value as it is of incompatible type");
            }
        }

        int paramId = 1+_staticParams.length;
        for(OperationParameter parameter : _params)
        {
            if(!opParameterTypes[paramId].isAssignableFrom(parameter.getType()))
            {
                throw new IllegalArgumentException("Type for parameter " + parameter.getName() + " does not match");
            }
            paramId++;
        }

    }

    @Override
    public String getName()
    {
        return _name;
    }

    @Override
    public List<OperationParameter> getParameters()
    {
        return _params;
    }

    @Override
    public Object perform(C subject, Map<String, Object> parameters)
    {
        if(!_validator.appliesToType((Class<? extends ConfiguredObject<?>>) subject.getClass()))
        {
            throw new IllegalArgumentException("Operation "
                                               + _operation.getName()
                                               + " cannot be used on an object of type "
                                               + subject.getClass().getSimpleName());
        }
        else
        {

            Set<String> providedNames = new HashSet<>(parameters.keySet());
            providedNames.removeAll(_validNames);
            if (!providedNames.isEmpty())
            {
                throw new IllegalArgumentException("Parameters " + providedNames + " are not accepted by " + getName());
            }
            Object[] paramValues = new Object[1+_staticParams.length+_params.size()];
            paramValues[0] = subject;


            for(int i = 0; i < _staticParams.length; i++)
            {
                paramValues[i+1] = _staticParams[i];
            }

            for (int i = 0; i < _params.size(); i++)
            {
                OperationParameter param = _params.get(i);
                Object providedVal;
                if (parameters.containsKey(param.getName()))
                {
                    providedVal = parameters.get(param.getName());
                }
                else if (!"".equals(param.getDefaultValue()))
                {
                    providedVal = param.getDefaultValue();
                }
                else
                {
                    providedVal = null;
                }
                final AttributeValueConverter<?> converter =
                        AttributeValueConverter.getConverter(AttributeValueConverter.convertPrimitiveToBoxed(param.getType()),
                                                             param.getGenericType());
                try
                {
                    final Object convertedVal = converter.convert(providedVal, subject);
                    paramValues[i+1+_staticParams.length] = convertedVal;
                }
                catch (IllegalArgumentException e)
                {
                    throw new IllegalArgumentException(e.getMessage()
                                                       + " for parameter '"
                                                       + param.getName()
                                                       + "' in "
                                                       + _operation.getDeclaringClass().getSimpleName()
                                                       + "."
                                                       + _operation.getName()
                                                       + "(...) operation", e.getCause());
                }
            }
            try
            {
                return _operation.invoke(null, paramValues);
            }
            catch (IllegalAccessException e)
            {
                throw new ServerScopedRuntimeException(e);
            }
            catch (InvocationTargetException e)
            {
                if (e.getCause() instanceof RuntimeException)
                {
                    throw (RuntimeException) e.getCause();
                }
                else if (e.getCause() instanceof Error)
                {
                    throw (Error) e.getCause();
                }
                else
                {
                    throw new ServerScopedRuntimeException(e);
                }
            }
        }
    }

    @Override
    public boolean hasSameParameters(final ConfiguredObjectOperation<?> other)
    {
        final List<OperationParameter> otherParams = other.getParameters();
        if(_params.size() == otherParams.size())
        {
            for(int i = 0; i < _params.size(); i++)
            {
                if(!_params.get(i).isCompatible(otherParams.get(i)))
                {
                    return false;
                }
            }
            return true;
        }
        else
        {
            return false;
        }
    }

    @Override
    public Class<?> getReturnType()
    {
        return _operation.getReturnType();
    }

    @Override
    public String getDescription()
    {
        return _description;
    }

    @Override
    public boolean isNonModifying()
    {
        return _nonModifying;
    }

    @Override
    public boolean isSecure()
    {
        return _secure;
    }


    @Override
    public Type getGenericReturnType()
    {
        return _operation.getGenericReturnType();
    }

    @Override
    public boolean appliesToConfiguredObjectType(final Class<? extends ConfiguredObject<?>> type)
    {
        return _validator.appliesToType(type);
    }
}
