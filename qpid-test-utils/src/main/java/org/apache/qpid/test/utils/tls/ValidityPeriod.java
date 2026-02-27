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

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

public record ValidityPeriod(Instant from, Instant to)
{
    public ValidityPeriod
    {
        Objects.requireNonNull(from, "from must not be null");
        Objects.requireNonNull(to, "to must not be null");

        if (to.isBefore(from))
        {
            throw new IllegalArgumentException("'to' must not be before 'from'");
        }
    }

    public static ValidityPeriod of(final Instant from, final Instant to)
    {
        return new ValidityPeriod(from, to);
    }

    public static ValidityPeriod fromYesterday(final Duration duration)
    {
        Objects.requireNonNull(duration, "duration must not be null");
        final Instant from = Instant.now().minus(1, ChronoUnit.DAYS);
        return new ValidityPeriod(from, from.plus(duration));
    }
}
