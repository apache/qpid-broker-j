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

public final class Optional extends Ever
{
    private static final class OptionalThen extends AbstractValidator implements ValueValidator
    {
        private final ValueValidator _validator;

        static ValueValidator validator(ValueValidator another)
        {
            return new OptionalThen(another);
        }

        private OptionalThen(ValueValidator validator)
        {
            super();
            _validator = Objects.requireNonNull(validator);
        }

        @Override
        public boolean test(Object value)
        {
            return value == null || _validator.test(value);
        }

        @Override
        public String errorMessage(Object value, ConfiguredObject<?> object, String attribute)
        {
            if (value != null)
            {
                return _validator.errorMessage(value, object, attribute);
            }
            return "";
        }

        void validateImpl(Object value, ConfiguredObject<?> object, String attribute)
        {
            if (value != null)
            {
                _validator.validate(value, object, attribute);
            }
        }
    }

    private static final Optional VALIDATOR = new Optional();

    public static ValueValidator validator()
    {
        return VALIDATOR;
    }

    private Optional()
    {
        super();
    }

    @Override
    public ValueValidator andThen(ValueValidator validator)
    {
        if (validator == null)
        {
            return this;
        }
        return OptionalThen.validator(validator);
    }
}
