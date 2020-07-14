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

public class Ever implements ValueValidator
{
    private static final Ever VALIDATOR = new Ever();

    public static ValueValidator instance()
    {
        return VALIDATOR;
    }

    protected Ever()
    {
        super();
    }

    @Override
    public boolean test(Object value)
    {
        return true;
    }

    @Override
    public boolean isValid(Object value)
    {
        return true;
    }

    @Override
    public void validate(Object value, ConfiguredObject<?> object, String attribute)
    {
        // Do nothing.
    }

    @Override
    public <T> ValidationResult<T> isValid(Supplier<? extends T> valueSupplier)
    {
        return ValidationResult.valid(valueSupplier);
    }

    @Override
    public <T> Supplier<T> validate(Supplier<? extends T> valueSupplier, ConfiguredObject<?> object, String attribute)
    {
        return ValidationResult.valid(valueSupplier);
    }

    @Override
    public String errorMessage(Object value, ConfiguredObject<?> object, String attribute)
    {
        return "";
    }

    @Override
    public ValueValidator andThen(ValueValidator validator)
    {
        if (validator == null)
        {
            return this;
        }
        return validator;
    }
}
