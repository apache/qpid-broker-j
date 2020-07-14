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

import java.util.Objects;
import java.util.function.Supplier;

public abstract class ValidationResult<T> implements Supplier<T>
{
    public static final class Valid<T> extends ValidationResult<T>
    {
        Valid(T value)
        {
            super(value);
        }

        Valid(Supplier<? extends T> value)
        {
            super(value);
        }

        @Override
        public boolean isValid()
        {
            return true;
        }
    }

    public static final class Invalid<T> extends ValidationResult<T>
    {
        Invalid(T value)
        {
            super(value);
        }

        Invalid(Supplier<? extends T> value)
        {
            super(value);
        }

        @Override
        public boolean isValid()
        {
            return false;
        }
    }

    private final Supplier<? extends T> _value;

    public static <T> ValidationResult<T> newResult(boolean valid, T value)
    {
        return valid ? valid(value) : invalid(value);
    }

    public static <T> ValidationResult<T> newResult(boolean valid, Supplier<? extends T> value)
    {
        return valid ? valid(value) : invalid(value);
    }

    public static <T> ValidationResult<T> valid(T value)
    {
        return new Valid<>(value);
    }

    public static <T> ValidationResult<T> valid(Supplier<? extends T> value)
    {
        return new Valid<>(value);
    }

    public static <T> ValidationResult<T> invalid(T value)
    {
        return new Invalid<>(value);
    }

    public static <T> ValidationResult<T> invalid(Supplier<? extends T> value)
    {
        return new Invalid<>(value);
    }

    public abstract boolean isValid();

    ValidationResult(final T value)
    {
        this._value = () -> value;
    }

    ValidationResult(Supplier<? extends T> value)
    {
        if (value instanceof ValidationResult)
        {
            this._value = ((ValidationResult<? extends T>) value)._value;
        }
        else
        {
            this._value = Objects.requireNonNull(value);
        }
    }

    @Override
    public T get()
    {
        return _value.get();
    }
}
