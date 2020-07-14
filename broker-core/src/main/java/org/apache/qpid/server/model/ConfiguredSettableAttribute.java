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
import org.apache.qpid.server.util.CollectionUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public interface ConfiguredSettableAttribute<C extends ConfiguredObject<C>, T> extends ConfiguredObjectAttribute<C, T>
{
    String defaultValue();

    default boolean isMandatory()
    {
        return validatorResolver().isMandatory();
    }

    default boolean isImmutable()
    {
        return validatorResolver().isImmutable();
    }

    default Collection<String> validValues()
    {
        final Collection<String> validValues = validatorResolver().validValues();
        if (validValues.isEmpty() && getType().isEnum())
        {
            final Object[] constants = getType().getEnumConstants();

            return Arrays.stream(constants)
                    .map(Enum.class::cast)
                    .map(Enum::name)
                    .collect(Collectors.toList());
        }
        return validValues;
    }

    default String validValuePattern()
    {
        return validatorResolver().validValuePattern();
    }

    default boolean hasValidValues()
    {
        return !CollectionUtils.isEmpty(validValues());
    }

    default String validator()
    {
        return validatorResolver().validator();
    }

    Resolver validatorResolver();

    default void validateValueProvidedBy(final C object)
    {
        final Supplier<?> desiredValueOrDefault = () -> getValue(object);
        validatorResolver()
                .validator(value -> this.convert(value, object))
                .validate(desiredValueOrDefault, object, getName());
    }

    AttributeValueConverter<T> getConverter();

    T convert(Object value, C object);

    Initialization getInitialization();
}
