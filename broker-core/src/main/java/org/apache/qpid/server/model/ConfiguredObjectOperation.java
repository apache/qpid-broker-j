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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.qpid.server.util.ServerScopedRuntimeException;

public class ConfiguredObjectOperation<C extends ConfiguredObject>
{
    private final Method _operation;
    private final OperationParameter[] _params;
    private final Set<String> _validNames;

    public ConfiguredObjectOperation(Class<C> clazz,
                                     final Method operation)
    {
        _operation = operation;
        final Annotation[][] allParameterAnnotations = _operation.getParameterAnnotations();
        _params = new OperationParameter[allParameterAnnotations.length];
        Set<String> validNames = new LinkedHashSet<>();
        for(int i = 0; i < allParameterAnnotations.length; i++)
        {
            final Annotation[] parameterAnnotations = allParameterAnnotations[i];
            for(Annotation annotation : parameterAnnotations)
            {
                if(annotation instanceof Param)
                {

                    _params[i] = new OperationParameter((Param) annotation, _operation.getParameterTypes()[i], _operation.getGenericParameterTypes()[i]);
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

    public String getName()
    {
        return _operation.getName();
    }

    public List<OperationParameter> getParameters()
    {
        return Collections.unmodifiableList(Arrays.asList(_params));
    }

    public Object perform(C subject, Map<String, Object> parameters)
    {
        Set<String> providedNames = new HashSet<>(parameters.keySet());
        providedNames.removeAll(_validNames);
        if(!providedNames.isEmpty())
        {
            throw new IllegalArgumentException("Parameters " + providedNames + " are not accepted by " + getName());
        }
        Object[] paramValues = new Object[_params.length];
        for(int i = 0; i <_params.length; i++)
        {
            OperationParameter param = _params[i];
            Object providedVal;
            if(parameters.containsKey(param.getName()))
            {
                providedVal = parameters.get(param.getName());
            }
            else if(!"".equals(param.getDefaultValue()))
            {
                providedVal = param.getDefaultValue();
            }
            else
            {
                providedVal = null;
            }
            final AttributeValueConverter<?> converter =
                    AttributeValueConverter.getConverter(param.getType(),
                                                         param.getGenericType());
            final Object convertedVal = converter.convert(providedVal, subject);
            paramValues[i] = convertedVal;
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
            if(e.getCause() instanceof RuntimeException)
            {
                throw (RuntimeException) e.getCause();
            }
            else if(e.getCause() instanceof Error)
            {
                throw (Error) e.getCause();
            }
            else
            {
                throw new ServerScopedRuntimeException(e);
            }
        }
    }

    public boolean hasSameParameters(final ConfiguredObjectOperation<?> other)
    {
        if(_params.length == other._params.length)
        {
            for(int i = 0; i < _params.length; i++)
            {
                if(!_params[i].isCompatible(other._params[i]))
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

    public Class<?> getReturnType()
    {
        return _operation.getReturnType();
    }

    public String getDescription()
    {
        return _operation.getAnnotation(ManagedOperation.class).description();
    }
}
