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

import java.util.function.Predicate;
import java.util.function.Supplier;

@FunctionalInterface
public interface ValueValidator extends Predicate<Object>
{
    default String errorMessage(Object value, ConfiguredObject<?> object, String attribute)
    {
        return "Attribute '" + attribute
                + "' instance of " + object.getClass().getName()
                + " named '" + object.getName() + "'"
                + " cannot have value '" + value + "'";
    }

    default boolean isValid(Object value)
    {
        if (value instanceof Supplier)
        {
            return isValid((Supplier<?>) value).isValid();
        }
        return test(value);
    }

    default void validate(Object value, ConfiguredObject<?> object, String attribute)
    {
        if (value instanceof Supplier)
        {
            validate((Supplier<?>) value, object, attribute);
            return;
        }
        if (!test(value))
        {
            throw new IllegalConfigurationException(errorMessage(value, object, attribute));
        }
    }

    default <T> ValidationResult<T> isValid(Supplier<? extends T> valueSupplier)
    {
        final T valueToValidate = valueSupplier.get();
        return ValidationResult.newResult(test(valueToValidate), valueToValidate);
    }

    default <T> Supplier<T> validate(Supplier<? extends T> valueSupplier, ConfiguredObject<?> object, String attribute)
    {
        final ValidationResult<T> result = isValid(valueSupplier);
        if (!result.isValid())
        {
            throw new IllegalConfigurationException(errorMessage(result.get(), object, attribute));
        }
        return result;
    }

    default ValueValidator andThen(ValueValidator validator)
    {
        if (validator == null)
        {
            return this;
        }
        return CombinedValidators.validator(this, validator);
    }
}
