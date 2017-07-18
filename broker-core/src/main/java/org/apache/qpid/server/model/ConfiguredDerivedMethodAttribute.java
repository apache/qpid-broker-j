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

import java.lang.reflect.Method;
import java.util.regex.Pattern;

public class ConfiguredDerivedMethodAttribute<C extends ConfiguredObject, T>  extends ConfiguredObjectMethodAttribute<C,T>
{
    private final DerivedAttribute _annotation;
    private final Pattern _secureValuePattern;

    ConfiguredDerivedMethodAttribute(final Class<C> clazz,
                                     final Method getter,
                                     final DerivedAttribute annotation)
    {
        super(clazz, getter);
        _annotation = annotation;

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
        return false;
    }

    @Override
    public String getOversizedAltText()
    {
        return "";
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

    public T convertValue(Object input, C configuredObject)
    {
        final AttributeValueConverter<T> converter =
                AttributeValueConverter.getConverter(getType(), getGetter().getGenericReturnType());
        return converter.convert(input, configuredObject);
    }

}
