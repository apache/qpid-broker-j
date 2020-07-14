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

import java.util.function.Supplier;

abstract class AbstractValidator implements ValueValidator
{
    abstract void validateImpl(Object value, ConfiguredObject<?> object, String attribute);

    AbstractValidator()
    {
        super();
    }

    @Override
    public void validate(Object values, final ConfiguredObject<?> object, final String attribute)
    {
        if (values instanceof Supplier)
        {
            validateImpl(((Supplier<?>) values).get(), object, attribute);
            return;
        }
        validateImpl(values, object, attribute);
    }

    @Override
    public <T> Supplier<T> validate(Supplier<? extends T> valueSupplier, ConfiguredObject<?> object, String attribute)
    {
        final T value = valueSupplier.get();
        validateImpl(value, object, attribute);
        return ValidationResult.valid(value);
    }

    Object extractValue(Object value)
    {
        if (value instanceof Supplier) {
            return ((Supplier<?>) value).get();
        }
        return value;
    }
}
