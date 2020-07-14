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
import org.apache.qpid.server.util.StringUtil;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.regex.Pattern;

public class ConfiguredAutomatedAttribute<C extends ConfiguredObject<C>, T> extends ConfiguredObjectMethodAttribute<C, T>
        implements ConfiguredSettableAttribute<C, T>
{
    private final ManagedAttribute _annotation;
    private final Pattern _secureValuePattern;
    private final AttributeValueConverter<T> _converter;
    private final Resolver _validatorResolver;

    ConfiguredAutomatedAttribute(final Class<C> clazz,
                                 final Method getter,
                                 final ManagedAttribute annotation)
    {
        super(clazz, getter);
        _converter = AttributeValueConverter.getConverter(_type, _getter.getGenericReturnType());

        _annotation = annotation;

        if (StringUtil.isEmpty(_annotation.secureValueFilter()))
        {
            _secureValuePattern = null;
        }
        else
        {
            _secureValuePattern = Pattern.compile(_annotation.secureValueFilter());
        }
        _validatorResolver = ValidatorResolver.newInstance(annotation, clazz, _type, _name);
    }

    @Override
    public final AttributeValueConverter<T> getConverter()
    {
        return _converter;
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

    @Override
    public Resolver validatorResolver()
    {
        return _validatorResolver;
    }
}
