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

package org.apache.qpid.test.utils.tls;

import java.time.Instant;

class ValidityPeriod
{
    private final Instant _from;
    private final Instant _to;

    ValidityPeriod(final Instant from, final Instant to)
    {
        if (from == null || to == null)
        {
            throw new IllegalArgumentException("Both 'to' and 'from' parameters cannot be null");
        }
        if (to.compareTo(from) < 0)
        {
            throw new IllegalArgumentException("Parameter 'to' cannot be less than 'from' value");
        }
        _from = from;
        _to = to;
    }

    public Instant getFrom()
    {
        return _from;
    }

    public Instant getTo()
    {
        return _to;
    }

    @Override
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        final ValidityPeriod that = (ValidityPeriod) o;

        if (!_from.equals(that._from))
        {
            return false;
        }
        return _to.equals(that._to);
    }

    @Override
    public int hashCode()
    {
        int result = _from.hashCode();
        result = 31 * result + _to.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return "ValidityPeriod{" +
               "_from=" + _from +
               ", _to=" + _to +
               '}';
    }
}
