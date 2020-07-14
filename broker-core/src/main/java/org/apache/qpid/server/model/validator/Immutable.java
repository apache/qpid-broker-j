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

public class Immutable implements ValueValidator
{
    private final Supplier<?> _previousValue;

    public static ValueValidator validator(final Object previousValue)
    {
        return new Immutable(() -> previousValue);
    }

    public static ValueValidator validator(Supplier<?> previousValue)
    {
        return new Immutable(previousValue);
    }

    protected Immutable(Supplier<?> previousValue)
    {
        this._previousValue = Objects.requireNonNull(previousValue, "Previous value supplier is required");
    }

    @Override
    public boolean test(Object value)
    {
        return Objects.equals(value, _previousValue.get());
    }

    @Override
    public String errorMessage(Object value, ConfiguredObject<?> object, String attribute)
    {
        return "Attribute '" + attribute + "' cannot be changed";
    }
}
