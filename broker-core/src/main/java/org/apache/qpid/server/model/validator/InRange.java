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

import java.math.BigInteger;
import java.util.Objects;

public class InRange extends AtLeast
{
    private final BigInteger _max;

    public static ValueValidator validator(long minimum, long maximum)
    {
        return new InRange(minimum, maximum);
    }

    protected InRange(long min, long max)
    {
        this(BigInteger.valueOf(min), BigInteger.valueOf(max));
    }

    protected InRange(BigInteger min, BigInteger max)
    {
        super(min);
        this._max = Objects.requireNonNull(max, "Maximum is required");
    }

    @Override
    public boolean isValidNumber(BigInteger bigInt)
    {
        return super.isValidNumber(bigInt) && maximum().compareTo(bigInt) > 0;
    }

    @Override
    public String errorMessage(Object value, ConfiguredObject<?> object, String attribute)
    {
        return "Attribute '" + attribute
                + "' instance of " + object.getClass().getName()
                + " named '" + object.getName() + "'"
                + " cannot have value '" + value + "'"
                + " as it is not in range [" + minimum() + ", " + maximum() + ")";
    }

    protected BigInteger maximum()
    {
        return _max;
    }
}
