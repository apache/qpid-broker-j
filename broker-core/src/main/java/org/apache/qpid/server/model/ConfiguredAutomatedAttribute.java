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
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfiguredAutomatedAttribute<C extends ConfiguredObject, T>  extends ConfiguredObjectMethodAttribute<C,T>
        implements ConfiguredSettableAttribute<C, T>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfiguredAutomatedAttribute.class);

    private final ManagedAttribute _annotation;
    private final Method _validValuesMethod;
    private final Pattern _secureValuePattern;
    private final AttributeValueConverter<T> _converter;

    ConfiguredAutomatedAttribute(final Class<C> clazz,
                                 final Method getter,
                                 final ManagedAttribute annotation)
    {
        super(clazz, getter);
        _converter = AttributeValueConverter.getConverter(getType(), getter.getGenericReturnType());

        _annotation = annotation;
        Method validValuesMethod = null;

        if(_annotation.validValues().length == 1)
        {
            String validValue = _annotation.validValues()[0];

            validValuesMethod = getValidValuesMethod(validValue, clazz);
        }
        _validValuesMethod = validValuesMethod;

        String secureValueFilter = _annotation.secureValueFilter();
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
    public final AttributeValueConverter<T> getConverter()
    {
        return _converter;
    }


    private Method getValidValuesMethod(final String validValue, final Class<C> clazz)
    {
        if(validValue.matches("([\\w][\\w\\d_]+\\.)+[\\w][\\w\\d_\\$]*#[\\w\\d_]+\\s*\\(\\s*\\)"))
        {
            String function = validValue;
            try
            {
                String className = function.split("#")[0].trim();
                String methodName = function.split("#")[1].split("\\(")[0].trim();
                Class<?> validValueCalculatingClass = Class.forName(className);
                Method method = validValueCalculatingClass.getMethod(methodName);
                if (Modifier.isStatic(method.getModifiers()) && Modifier.isPublic(method.getModifiers()))
                {
                    if (Collection.class.isAssignableFrom(method.getReturnType()))
                    {
                        if (method.getGenericReturnType() instanceof ParameterizedType)
                        {
                            Type parameterizedType =
                                    ((ParameterizedType) method.getGenericReturnType()).getActualTypeArguments()[0];
                            if (parameterizedType == String.class)
                            {
                                return method;
                            }
                        }
                    }
                }

            }
            catch (ClassNotFoundException | NoSuchMethodException e)
            {
                LOGGER.warn("The validValues of the " + getName() + " attribute in class " + clazz.getSimpleName()
                            + " has value '" + validValue + "' which looks like it should be a method,"
                            + " but no such method could be used.", e );
            }
        }
        return null;
    }

    @Override
    public boolean isAutomated()
    {
        return true;
    }

    @Override
    public boolean isDerived()
    {
        return false;
    }

    @Override
    public String defaultValue()
    {
        return _annotation.defaultValue();
    }

    @Override
    public boolean isSecure()
    {
        return _annotation.secure();
    }

    @Override
    public boolean isMandatory()
    {
        return _annotation.mandatory();
    }

    @Override
    public boolean isImmutable()
    {
        return _annotation.immutable();
    }

    @Override
    public boolean isPersisted()
    {
        return _annotation.persist();
    }

    @Override
    public boolean isOversized()
    {
        return _annotation.oversize();
    }

    @Override
    public boolean updateAttributeDespiteUnchangedValue()
    {
        return _annotation.updateAttributeDespiteUnchangedValue();
    }

    @Override
    public String getOversizedAltText()
    {
        return _annotation.oversizedAltText();
    }

    @Override
    public Initialization getInitialization()
    {
        return _annotation.initialization();
    }

    @Override
    public String getDescription()
    {
        return _annotation.description();
    }

    @Override
    public Pattern getSecureValueFilter()
    {
        return _secureValuePattern;
    }

    @Override
    public Collection<String> validValues()
    {
        if(_validValuesMethod != null)
        {
            try
            {
                return (Collection<String>) _validValuesMethod.invoke(null);
            }
            catch (InvocationTargetException | IllegalAccessException e)
            {
                LOGGER.warn("Could not execute the validValues generation method " + _validValuesMethod.getName(), e);
                return Collections.emptySet();
            }
        }
        else if (_annotation.validValues().length == 0 && getGetter().getReturnType().isEnum())
        {
            final Enum<?>[] constants = (Enum<?>[]) getGetter().getReturnType().getEnumConstants();
            List<String> validValues = new ArrayList<>(constants.length);
            for (Enum<?> constant : constants)
            {
                validValues.add(constant.name());
            }
            return validValues;
        }
        else
        {
            return Arrays.asList(_annotation.validValues());
        }
    }

    /** Returns true iff this attribute has valid values defined */
    @Override
    public boolean hasValidValues()
    {
        return validValues() != null && validValues().size() > 0;
    }

    @Override
    public String validValuePattern()
    {
        return _annotation.validValuePattern();
    }


    @Override
    public T convert(final Object value, C object)
    {
        final AttributeValueConverter<T> converter = getConverter();
        try
        {
            return converter.convert(value, object);
        }
        catch (IllegalArgumentException iae)
        {
            Type returnType = getGetter().getGenericReturnType();
            String simpleName = returnType instanceof Class ? ((Class) returnType).getSimpleName() : returnType.toString();

            throw new IllegalArgumentException("Cannot convert '" + value
                                               + "' into a " + simpleName
                                               + " for attribute " + getName()
                                               + " (" + iae.getMessage() + ")", iae);
        }
    }
}
