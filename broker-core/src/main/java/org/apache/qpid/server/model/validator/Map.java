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

package org.apache.qpid.server.model.validator;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.ConfiguredObject;

import java.util.Objects;

public class Map extends AbstractValidator implements ValueValidator
{
    private final ValueValidator _keyValidator;

    private final ValueValidator _valueValidator;

    public static ValueValidator validator(ValueValidator keyValidator, ValueValidator valueValidator)
    {
        return new Map(keyValidator, valueValidator);
    }

    protected Map(ValueValidator keyValidator, ValueValidator valueValidator)
    {
        super();
        this._keyValidator = Objects.requireNonNull(keyValidator, "Key validator is required");
        this._valueValidator = Objects.requireNonNull(valueValidator, "Value validator is required");
    }

    @Override
    public boolean test(Object map)
    {
        if (!(map instanceof java.util.Map))
        {
            return false;
        }
        return ((java.util.Map<?, ?>) map).entrySet()
                .stream()
                .allMatch(this::isValidMapEntry);
    }

    @Override
    public String errorMessage(Object map, final ConfiguredObject<?> object, final String attribute)
    {
        if (!(map instanceof java.util.Map))
        {
            return castErrorMessage(object, attribute);
        }
        return ((java.util.Map<?, ?>) map).entrySet().stream()
                .filter(entry -> !isValidMapEntry(entry))
                .findFirst()
                .map(value -> mapEntryErrorMessage(value, object, attribute))
                .orElse("");
    }

    @Override
    void validateImpl(Object value, ConfiguredObject<?> object, String attribute)
    {
        if (!(value instanceof java.util.Map))
        {
            throw new IllegalConfigurationException(castErrorMessage(object, attribute));
        }
        final String keyAttribute = attribute + ".key";
        final String valueAttribute = attribute + ".value";
        ((java.util.Map<?, ?>) value).entrySet().forEach(entry -> validateMapEntry(entry, object, keyAttribute, valueAttribute));
    }

    protected String castErrorMessage(ConfiguredObject<?> object, String attribute)
    {
        return "A map '" + attribute
                + "' instance of " + object.getClass().getName()
                + " named '" + object.getName() + "' is expected";
    }

    private boolean isValidMapEntry(java.util.Map.Entry<?, ?> entry)
    {
        return _keyValidator.test(entry.getKey()) && _valueValidator.isValid(entry.getValue());
    }

    private void validateMapEntry(java.util.Map.Entry<?, ?> entry, ConfiguredObject<?> object, String keyAttribute, String valueAttribute)
    {
        _keyValidator.validate(entry.getKey(), object, keyAttribute);
        _valueValidator.validate(entry.getValue(), object, valueAttribute);
    }

    private String mapEntryErrorMessage(java.util.Map.Entry<?, ?> entry, ConfiguredObject<?> object, String attribute)
    {
        if (!_keyValidator.isValid(entry.getKey()))
        {
            return _keyValidator.errorMessage(entry.getKey(), object, attribute + ".key");
        }
        return _valueValidator.errorMessage(extractValue(entry.getValue()), object, attribute + ".value");
    }
}
