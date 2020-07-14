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

import org.apache.qpid.server.model.validator.Resolver;
import org.apache.qpid.server.model.validator.ValidatorResolver;

import java.lang.reflect.Type;
import java.util.regex.Pattern;

public class ConfiguredSettableInjectedAttribute<C extends ConfiguredObject<C>, T>
        extends ConfiguredObjectInjectedAttributeOrStatistic<C,T> implements ConfiguredSettableAttribute<C,T>, ConfiguredObjectInjectedAttribute<C,T>
{
    private final AttributeValueConverter<T> _converter;
    private final Pattern _secureValuePattern;
    private final String _defaultValue;
    private final boolean _secure;
    private final boolean _persisted;
    private final boolean _oversized;
    private final String _oversizedAltText;
    private final String _description;
    private final Initialization _initialization;
    private final Resolver _validatorResolver;

    public ConfiguredSettableInjectedAttribute(final String name,
                                               final Class<T> type,
                                               final Type genericType,
                                               final String defaultValue,
                                               final boolean secure,
                                               final boolean persisted,
                                               final boolean immutable,
                                               final String secureValueFilter,
                                               final boolean oversized,
                                               final String oversizedAltText,
                                               final String description,
                                               final String[] validValues,
                                               final String validValuePattern,
                                               final TypeValidator typeValidator,
                                               final Initialization initialization)
    {
        super(name, type, genericType, typeValidator);
        _converter = AttributeValueConverter.getConverter(type, genericType);

        _defaultValue = defaultValue;
        _secure = secure;
        _persisted = persisted;
        _oversized = oversized;
        _oversizedAltText = oversizedAltText;
        _description = description;
        _initialization = initialization;

        if (secureValueFilter == null || "".equals(secureValueFilter))
        {
            _secureValuePattern = null;
        }
        else
        {
            _secureValuePattern = Pattern.compile(secureValueFilter);
        }
        _validatorResolver = ValidatorResolver.newInstance(validValuePattern, validValues, immutable, type, name);
    }

    @Override
    public final AttributeValueConverter<T> getConverter()
    {
        return _converter;
    }

    @Override
    public boolean isAutomated()
    {
        return false;
    }

    @Override
    public boolean isDerived()
    {
        return false;
    }

    @Override
    public String defaultValue()
    {
        return _defaultValue;
    }

    @Override
    public Initialization getInitialization()
    {
        return _initialization;
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
    public final T getValue(final C configuredObject)
    {
        Object value = configuredObject.getActualAttributes().get(getName());
        if(value == null)
        {
            value = defaultValue();
        }
        return convert(value, configuredObject);
    }

    @Override
    public final T convert(final Object value, final C object)
    {
        final AttributeValueConverter<T> converter = getConverter();
        try
        {
            return converter.convert(value, object);
        }
        catch (IllegalArgumentException iae)
        {
            Type returnType = getGenericType();
            String simpleName = returnType instanceof Class ? ((Class) returnType).getSimpleName() : returnType.toString();

            throw new IllegalArgumentException("Cannot convert '" + value
                                               + "' into a " + simpleName
                                               + " for attribute " + getName()
                                               + " (" + iae.getMessage() + ")", iae);
        }
    }

    @Override
    public Resolver validatorResolver()
    {
        return _validatorResolver;
    }
}
