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

import java.lang.reflect.Type;

abstract class ConfiguredObjectInjectedAttributeOrStatistic<C extends ConfiguredObject, T>
        implements ConfiguredObjectAttributeOrStatistic<C,T>, InjectedAttributeOrStatistic<C, T>
{

    private final String _name;
    private final Class<T> _type;
    private final Type _genericType;
    private final AttributeValueConverter<T> _converter;
    private final TypeValidator _typeValidator;

    ConfiguredObjectInjectedAttributeOrStatistic(String name,
                                                 Class<T> type,
                                                 Type genericType,
                                                 final TypeValidator typeValidator)
    {

        _name = name;
        _type = type;
        _genericType = genericType;
        _typeValidator = typeValidator;
        _converter = AttributeValueConverter.getConverter(type, genericType);


    }

    @Override
    public final String getName()
    {
        return _name;
    }

    @Override
    public final Class<T> getType()
    {
        return _type;
    }

    @Override
    public final Type getGenericType()
    {
        return _genericType;
    }

    @Override
    public final AttributeValueConverter<T> getConverter()
    {
        return _converter;
    }

    @Override
    public final boolean appliesToConfiguredObjectType(final Class<? extends ConfiguredObject<?>> type)
    {
        return _typeValidator.appliesToType(type);
    }

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


}
