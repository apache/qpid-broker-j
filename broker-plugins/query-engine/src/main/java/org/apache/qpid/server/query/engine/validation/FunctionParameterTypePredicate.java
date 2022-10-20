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
package org.apache.qpid.server.query.engine.validation;

import java.util.function.Predicate;

import org.apache.qpid.server.query.engine.exception.Errors;
import org.apache.qpid.server.query.engine.exception.QueryValidationException;
import org.apache.qpid.server.query.engine.parsing.converter.DateTimeConverter;

/**
 * Predicate used to validate function parameter types
 *
 * @param <T> Parameter type
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public class FunctionParameterTypePredicate<T> implements Predicate<T>
{
    /**
     * Flag defines whether boolean values are allowed or not
     */
    private final boolean _allowBooleans;

    /**
     * Flag defines whether comparable values are allowed or not
     */
    private final boolean _allowComparables;

    /**
     * Flag defines whether null values are allowed or not
     */
    private final boolean _allowNulls;

    /**
     * Flag defines whether numeric values are allowed or not
     */
    private final boolean _allowNumbers;

    /**
     * Flag defines whether enum values are allowed or not
     */
    private final boolean _allowEnums;

    /**
     * Flag defines whether string values are allowed or not
     */
    private final boolean _allowStrings;

    /**
     * Flag defines whether datetime values are allowed or not
     */
    private final boolean _allowDateTimeTypes;

    /**
     * Constructor sets state flags
     *
     * @param allowBooleans Flag defines whether boolean values are allowed or not
     * @param allowComparables Flag defines whether comparable values are allowed or not
     * @param allowDateTimeTypes Flag defines whether datetime values are allowed or not
     * @param allowEnums Flag defines whether enum values are allowed or not
     * @param allowNulls Flag defines whether null values are allowed or not
     * @param allowNumbers Flag defines whether numeric values are allowed or not
     * @param allowStrings Flag defines whether string values are allowed or not
     */
    public FunctionParameterTypePredicate(
        boolean allowBooleans,
        boolean allowComparables,
        boolean allowDateTimeTypes,
        boolean allowEnums,
        boolean allowNulls,
        boolean allowNumbers,
        boolean allowStrings
    )
    {
        _allowBooleans = allowBooleans;
        _allowComparables = allowComparables;
        _allowDateTimeTypes = allowDateTimeTypes;
        _allowEnums = allowEnums;
        _allowNulls = allowNulls;
        _allowNumbers = allowNumbers;
        _allowStrings = allowStrings;
    }

    /**
     *
     * @param value Function parameter to test
     *
     * @return True when parameter is valid, false otherwise
     */
    @Override
    public boolean test(final T value)
    {
        if (_allowNulls && value == null)
        {
            return true;
        }
        if (!_allowNulls && value == null)
        {
            return false;
        }
        if (_allowBooleans && value.getClass().equals(Boolean.class))
        {
            return true;
        }
        if (!_allowBooleans && value.getClass().equals(Boolean.class))
        {
            return false;
        }
        if ((_allowDateTimeTypes || _allowComparables) && DateTimeConverter.isDateTime(value))
        {
            return true;
        }
        if ((_allowEnums || _allowComparables) && value.getClass().isEnum())
        {
            return true;
        }
        if ((!_allowEnums && !_allowComparables) && value.getClass().isEnum())
        {
            return false;
        }
        if ((_allowNumbers || _allowComparables) && value instanceof Number)
        {
            return true;
        }
        if ((!_allowNumbers && !_allowComparables) && value instanceof Number)
        {
            return false;
        }
        if ((_allowStrings || _allowComparables) && value.getClass().equals(String.class))
        {
            return true;
        }
        if ((!_allowStrings && !_allowComparables) && value.getClass().equals(String.class))
        {
            return false;
        }
        return _allowComparables && value instanceof Comparable;
    }

    /**
     * Creates builder
     *
     * @param <T> Function parameter type
     *
     * @return Builder instance
     */
    public static <T> Builder<T> builder()
    {
        return new Builder<>();
    }

    /**
     * Builder used to create predicate
     *
     * @param <T> Function parameter type
     */
    public static class Builder<T>
    {
        private boolean _allowBooleans;
        private boolean _allowComparables;
        private boolean _allowDateTimeTypes;
        private boolean _allowEnums;
        private boolean _allowNulls;
        private boolean _allowNumbers;
        private boolean _allowStrings;

        public Builder<T> allowBooleans()
        {
            _allowBooleans = true;
            return this;
        }

        public Builder<T> disallowBooleans()
        {
            _allowBooleans = false;
            return this;
        }

        public Builder<T> allowComparables()
        {
            _allowComparables = true;
            return this;
        }

        public Builder<T> allowNulls()
        {
            _allowNulls = true;
            return this;
        }

        public Builder<T> allowNumbers()
        {
            _allowNumbers = true;
            return this;
        }

        public Builder<T> allowEnums()
        {
            _allowEnums = true;
            return this;
        }

        public Builder<T> allowDateTimeTypes()
        {
            _allowDateTimeTypes = true;
            return this;
        }

        public Builder<T> allowStrings()
        {
            _allowStrings = true;
            return this;
        }

        public FunctionParameterTypePredicate<T> build()
        {
            if(!_allowBooleans && !_allowComparables && !_allowDateTimeTypes && !_allowEnums && !_allowNulls
                 && !_allowNumbers && !_allowStrings)
            {
                throw QueryValidationException.of(Errors.VALIDATION.FUNCTION_ARGS_PREDICATE_EMPTY);
            }
            return new FunctionParameterTypePredicate<>(
                _allowBooleans, _allowComparables, _allowDateTimeTypes, _allowEnums, _allowNulls, _allowNumbers, _allowStrings
            );
        }

    }
}
