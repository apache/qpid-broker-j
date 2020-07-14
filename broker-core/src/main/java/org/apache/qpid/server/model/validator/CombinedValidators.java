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

import org.apache.qpid.server.model.ConfiguredObject;

import java.util.Objects;
import java.util.function.Supplier;

public final class CombinedValidators implements ValueValidator
{
    private final ValueValidator _primary;
    private final ValueValidator _secondary;

    public static ValueValidator validator(ValueValidator me, ValueValidator validator)
    {
        return new CombinedValidators(me, validator);
    }

    private CombinedValidators(ValueValidator primary, ValueValidator secondary)
    {
        _primary = Objects.requireNonNull(primary, "Primary validator is required");
        _secondary = Objects.requireNonNull(secondary, "Secondary validator is required");
    }

    @Override
    public boolean test(Object value)
    {
        return _primary.test(value) && _secondary.test(value);
    }

    @Override
    public void validate(Object value, ConfiguredObject<?> object, String attribute)
    {
        if (value instanceof Supplier)
        {
            validate((Supplier<?>) value, object, attribute);
            return;
        }
        _primary.validate(value, object, attribute);
        _secondary.validate(value, object, attribute);
    }

    @Override
    public <T> ValidationResult<T> isValid(Supplier<? extends T> valueSupplier)
    {
        final ValidationResult<T> result = _primary.isValid(valueSupplier);
        if (result.isValid())
        {
            return _secondary.isValid(result);
        }
        return result;
    }

    @Override
    public <T> Supplier<T> validate(Supplier<? extends T> valueSupplier, ConfiguredObject<?> object, String attribute)
    {
        return _secondary.validate(_primary.validate(valueSupplier, object, attribute), object, attribute);
    }

    @Override
    public String errorMessage(Object value, ConfiguredObject<?> object, String attribute)
    {
        if (_primary.test(value))
        {
            return _secondary.errorMessage(value, object, attribute);
        }
        return _primary.errorMessage(value, object, attribute);
    }
}
