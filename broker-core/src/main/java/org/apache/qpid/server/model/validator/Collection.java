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

public class Collection extends AbstractValidator implements ValueValidator
{
    private final ValueValidator _validator;

    public static ValueValidator validator(ValueValidator validator)
    {
        return new Collection(validator);
    }

    protected Collection(ValueValidator validator)
    {
        super();
        _validator = Objects.requireNonNull(validator, "Some validator is required to validate a collection");
    }

    @Override
    public boolean test(Object values)
    {
        if (!(values instanceof java.util.Collection))
        {
            return false;
        }

        return ((java.util.Collection<?>) values).stream().allMatch(_validator::isValid);
    }

    @Override
    public String errorMessage(Object values, final ConfiguredObject<?> object, final String attribute)
    {
        if (!(values instanceof java.util.Collection))
        {
            return castErrorMessage(object, attribute);
        }

        return ((java.util.Collection<?>) values).stream()
                .map(this::extractValue)
                .filter(value -> !_validator.test(value))
                .findFirst()
                .map(value -> _validator.errorMessage(value, object, attribute))
                .orElse("");
    }

    @Override
    protected void validateImpl(Object values, ConfiguredObject<?> object, String attribute)
    {
        if (!(values instanceof java.util.Collection))
        {
            throw new IllegalConfigurationException(castErrorMessage(object, attribute));
        }
        ((java.util.Collection<?>) values).forEach(value -> _validator.validate(value, object, attribute));
    }

    private String castErrorMessage(ConfiguredObject<?> object, String attribute)
    {
        return "A collection '" + attribute
                + "' instance of " + object.getClass().getName()
                + " named '" + object.getName() + "' is expected";
    }
}
