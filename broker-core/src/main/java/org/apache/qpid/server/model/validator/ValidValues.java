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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ValidValues implements ValueValidator
{
    private final Collection<?> _validValues;
    private final Function<Object, ?> _converter;

    private List<?> _convertedValues = null;

    public static ValueValidator validator(Collection<?> validValues, Function<Object, ?> converter)
    {
        return new ValidValues(validValues, converter);
    }

    public static ValueValidator validator(Collection<?> validValues)
    {
        return new ValidValues(validValues, null);
    }

    protected ValidValues(Collection<?> validValues, Function<Object, ?> converter)
    {
        this._validValues = Objects.requireNonNull(validValues, "List of valid values is required");
        if (converter != null)
        {
            this._converter = converter;
        }
        else
        {
            this._converter = Function.identity();
        }
    }

    @Override
    public boolean test(Object value)
    {
        return convertedValues().contains(value);
    }

    @Override
    public String errorMessage(Object value, ConfiguredObject<?> object, String attribute)
    {
        return "Attribute '" + attribute
                + "' instance of " + object.getClass().getName()
                + " named '" + object.getName() + "'"
                + " cannot have value '" + value + "'"
                + ". Valid values are: " + _validValues;
    }

    private List<?> convertedValues()
    {
        if (_convertedValues == null)
        {
            _convertedValues = _validValues.stream().map(_converter).distinct().collect(Collectors.toList());
        }
        return Collections.unmodifiableList(_convertedValues);
    }
}
