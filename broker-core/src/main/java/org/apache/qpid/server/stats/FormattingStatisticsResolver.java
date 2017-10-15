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

package org.apache.qpid.server.stats;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Map;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.util.Strings.Resolver;

public class FormattingStatisticsResolver implements Resolver
{
    private final Map<String, Object> _statistics;
    static final String BYTEUNIT = "byteunit";
    static final String DURATION = "duration";
    static final String DATETIME = "datetime";

    FormattingStatisticsResolver(final ConfiguredObject<?> object)
    {
        _statistics = object.getStatistics();
    }

    @Override
    public String resolve(String statNameWithFormatSpecifier, final Resolver unused)
    {
        String[] split = statNameWithFormatSpecifier.split(":", 2);
        String statName = split[0];

        Object statisticValue = _statistics.get(statName);

        if (split.length > 1)
        {
            String formatterName = split[1].toLowerCase();
            if (statisticValue instanceof Number)
            {
                final long value = ((Number) statisticValue).longValue();
                switch (formatterName.toLowerCase())
                {
                    case BYTEUNIT:
                        statisticValue = toIEC80000BinaryPrefixedValue(value);
                        break;
                    case DURATION:
                        statisticValue = value < 0 ? "-" : Duration.ofMillis(value);
                        break;
                    case DATETIME:
                        statisticValue = value < 0 ? "-" : Instant.ofEpochMilli(value).toString();
                        break;
                }
            }
            else if (statisticValue instanceof Date)
            {
                switch (formatterName.toLowerCase())
                {
                    case DATETIME:
                        long time = ((Date) statisticValue).getTime();
                        statisticValue = time < 0 ? "-" : Instant.ofEpochMilli(time).toString();
                        break;
                }

            }
        }

        return statisticValue == null ? null : String.valueOf(statisticValue);
    }

    private static String toIEC80000BinaryPrefixedValue(long value)
    {
        if (value > -1023 && value < 1024)
        {
            return value + " B";
        }
        int exp = (int) (Math.log(Math.abs(value)) / Math.log(1024));
        char binaryPrefixInitialChar = "KMGTPEZY".charAt(exp - 1);
        return String.format("%.1f %siB", value / Math.pow(1024, exp), binaryPrefixInitialChar);
    }
}
