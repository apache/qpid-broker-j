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

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.qpid.server.util.ServerScopedRuntimeException;

public class ConfiguredObjectMethodOperation<C extends ConfiguredObject<?>> implements ConfiguredObjectOperation<C>
{
    private final Method _operation;
    private final OperationParameterFromAnnotation[] _params;
    private final Set<String> _validNames;
    private final String _objectType;
    private final ConfiguredObjectTypeRegistry _typeRegistry;

    public ConfiguredObjectMethodOperation(Class<C> clazz,
                                           final Method operation,
                                           final ConfiguredObjectTypeRegistry typeRegistry)
    {
        _objectType = clazz.getSimpleName();
        _operation = operation;
        _typeRegistry = typeRegistry;
        final Annotation[][] allParameterAnnotations = _operation.getParameterAnnotations();
        _params = new OperationParameterFromAnnotation[allParameterAnnotations.length];
        Set<String> validNames = new LinkedHashSet<>();
        for(int i = 0; i < allParameterAnnotations.length; i++)
        {
            final Annotation[] parameterAnnotations = allParameterAnnotations[i];
            for(Annotation annotation : parameterAnnotations)
            {
                if(annotation instanceof Param)
                {

                    _params[i] = new OperationParameterFromAnnotation((Param) annotation, _operation.getParameterTypes()[i], _operation.getGenericParameterTypes()[i]);
                    validNames.add(_params[i].getName());
                }
            }
            if(_params[i] == null)
            {
                throw new IllegalArgumentException("Parameter doesn't have a @Param annotation");
            }
        }
        _validNames = Collections.unmodifiableSet(validNames);
    }

    @Override
    public String getName()
    {
        return _operation.getName();
    }

    @Override
    public List<OperationParameter> getParameters()
    {
        return Collections.unmodifiableList(Arrays.<OperationParameter>asList(_params));
    }

    @Override
    public Object perform(C subject, Map<String, Object> parameters)
    {
        final Map<String, ConfiguredObjectOperation<?>> operationsOnSubject =
                _typeRegistry.getOperations(subject.getClass());

        if(operationsOnSubject == null || operationsOnSubject.get(_operation.getName()) == null)
        {
            throw new IllegalArgumentException("No operation " + _operation.getName() + " on " + subject.getClass().getSimpleName());
        }
        else if(!hasSameParameters(operationsOnSubject.get(_operation.getName())))
        {
            throw new IllegalArgumentException("Operation "
                                               + _operation.getName()
                                               + " on "
                                               + _objectType
                                               + " cannot be used on an object of type "
                                               + subject.getClass().getSimpleName());
        }
        else if(operationsOnSubject.get(_operation.getName()) != this)
        {
            return ((ConfiguredObjectOperation<C>)operationsOnSubject.get(_operation.getName())).perform(subject, parameters);
        }
        else
        {
            Set<String> providedNames = new HashSet<>(parameters.keySet());
            providedNames.removeAll(_validNames);
            if (!providedNames.isEmpty())
            {
                throw new IllegalArgumentException("Parameters " + providedNames + " are not accepted by " + getName());
            }
            Object[] paramValues = new Object[_params.length];
            for (int i = 0; i < _params.length; i++)
            {
                paramValues[i] = getParameterValue(subject, parameters, _params[i]);
            }
            try
            {
                return _operation.invoke(subject, paramValues);
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

    protected Object getParameterValue(final C subject,
                                       final Map<String, Object> parameters,
                                       final OperationParameter param)
    {
        final Object convertedVal;
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
        if (providedVal == null && param.isMandatory())
        {
            throw new IllegalArgumentException(String.format("Parameter '%s' of operation %s in %s requires a non-null value",
                                                             param.getName(),
                                                             _operation.getName(),
                                                             _objectType));
        }

        final AttributeValueConverter<?> converter =
                AttributeValueConverter.getConverter(AttributeValueConverter.convertPrimitiveToBoxed(param.getType()),
                                                     param.getGenericType());
        try
        {
            convertedVal = converter.convert(providedVal, subject);

        }
        catch (IllegalArgumentException e)
        {
            throw new IllegalArgumentException(e.getMessage()
                                               + " for parameter '"
                                               + param.getName()
                                               + "' in "
                                               + _objectType
                                               + "."
                                               + _operation.getName()
                                               + "(...) operation", e.getCause());
        }
        return convertedVal;
    }

    @Override
    public boolean hasSameParameters(final ConfiguredObjectOperation<?> other)
    {
        final List<OperationParameter> otherParams = other.getParameters();
        if(_params.length == otherParams.size())
        {
            for(int i = 0; i < _params.length; i++)
            {
                if(!_params[i].isCompatible(otherParams.get(i)))
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
        return _operation.getAnnotation(ManagedOperation.class).description();
    }

    @Override
    public boolean isNonModifying()
    {
        return _operation.getAnnotation(ManagedOperation.class).nonModifying();
    }

    @Override
    public boolean isAssociateAsIfChildren()
    {
        return _operation.getAnnotation(ManagedOperation.class).associateAsIfChildren();
    }

    @Override
    public boolean isSecure(final C subject, final Map<String, Object> arguments)
    {
        return _operation.getAnnotation(ManagedOperation.class).secure() || requiresSecure(subject, arguments);
    }

    private boolean requiresSecure(C subject, final Map<String, Object> arguments)
    {
        String secureParam = _operation.getAnnotation(ManagedOperation.class).paramRequiringSecure();
        for(OperationParameterFromAnnotation param : _params)
        {
            if(secureParam.equals(param.getName()))
            {
                Object value = getParameterValue(subject, arguments, param);
                if(value instanceof Boolean)
                {
                    return (Boolean)value;
                }
                else
                {
                    return value != null;
                }
            }
        }
        return false;
    }


    @Override
    public Type getGenericReturnType()
    {
        return _operation.getGenericReturnType();
    }
}
