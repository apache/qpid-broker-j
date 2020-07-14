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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Objects;
import java.util.concurrent.atomic.DoubleAccumulator;
import java.util.concurrent.atomic.DoubleAdder;

public class AtLeast implements ValueValidator
{
    private final BigInteger _min;

    public static ValueValidator validator(long minimum)
    {
        return new AtLeast(minimum);
    }

    public static ValueValidator validator(BigInteger minimum)
    {
        return new AtLeast(minimum);
    }

    protected AtLeast(long minimum)
    {
        this(BigInteger.valueOf(minimum));
    }

    protected AtLeast(BigInteger min)
    {
        this._min = Objects.requireNonNull(min, "Minimum is required");
    }

    @Override
    public boolean test(Object value)
    {
        if (value instanceof Number)
        {
            return isValidNumber(toBigInteger((Number) value));
        }
        return false;
    }

    @Override
    public String errorMessage(Object value, ConfiguredObject<?> object, String attribute)
    {
        return "Attribute '" + attribute
                + "' instance of " + object.getClass().getName()
                + " named '" + object.getName() + "'"
                + " cannot have value '" + value + "'"
                + " as it has to be at least " + minimum();
    }

    protected boolean isValidNumber(BigInteger bigInt)
    {
        return minimum().compareTo(bigInt) <= 0;
    }

    protected BigInteger minimum()
    {
        return _min;
    }

    private BigInteger toBigInteger(Number value)
    {
        final BigInteger bigInt;
        if (value instanceof Double || value instanceof DoubleAccumulator || value instanceof DoubleAdder)
        {
            bigInt = BigDecimal.valueOf(value.doubleValue()).toBigInteger();
        }
        else if (value instanceof Float)
        {
            bigInt = BigDecimal.valueOf((Float) value).toBigInteger();
        }
        else if (value instanceof BigDecimal)
        {
            bigInt = ((BigDecimal) value).toBigInteger();
        }
        else if (value instanceof BigInteger)
        {
            bigInt = (BigInteger) value;
        }
        else
        {
            bigInt = BigInteger.valueOf(value.longValue());
        }
        return bigInt;
    }
}
