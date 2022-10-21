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
package org.apache.qpid.server.query.engine.parsing.converter;

import java.util.UUID;

/**
 * Converter used to perform implicit type conversions
 */
public final class ImplicitConverter
{
    /**
     * Shouldn't be instantiated directly
     */
    private ImplicitConverter()
    {

    }

    /**
     * Converts value following implicit conversion rules
     *
     * @param value Object to handle
     *
     * @param <T> Input parameter type
     * @param <R> Return parameter type
     *
     * @return Converted value
     */
    @SuppressWarnings("unchecked")
    public static <T, R> R convert(final T value)
    {
        if (value == null)
        {
            return (R) value;
        }
        if (value.getClass().equals(UUID.class))
        {
            return (R) value.toString();
        }
        if (value.getClass().isEnum())
        {
            return (R) value.toString();
        }
        if (DateTimeConverter.isDateTime(value))
        {
            return (R) DateTimeConverter.toStringMapper().apply(value);
        }
        return (R) value;
    }
}
