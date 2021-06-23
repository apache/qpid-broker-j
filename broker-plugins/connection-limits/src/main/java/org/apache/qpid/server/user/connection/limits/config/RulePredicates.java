/*
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
 */
package org.apache.qpid.server.user.connection.limits.config;

import java.time.Duration;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RulePredicates
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RulePredicates.class);

    public static final String ALL_PORTS = "ALL";
    public static final String ALL_USERS = "ALL";

    private static final String TIME_PERIOD_DELIMITER = "/";

    private static final Duration HOUR = Duration.ofHours(1L);
    private static final Duration MINUTE = Duration.ofMinutes(1L);
    private static final Duration SECOND = Duration.ofSeconds(1L);

    private Integer _connectionCountLimit;
    private Integer _connectionFrequencyLimit;
    private Duration _connectionFrequencyPeriod;
    private boolean _blocked = false;

    private final Set<Property> _properties = EnumSet.noneOf(Property.class);

    private String _port = ALL_PORTS;

    public static boolean isAllPort(String port)
    {
        return RulePredicates.ALL_PORTS.equalsIgnoreCase(port);
    }

    public static boolean isAllUser(String port)
    {
        return RulePredicates.ALL_USERS.equalsIgnoreCase(port);
    }

    public Property parse(String key, String value)
    {
        final Property property = Property.parse(key);

        if (property != null)
        {
            addProperty(property, value);
        }
        return property;
    }

    void addProperty(Property property, String value)
    {
        checkPropertyAlreadyDefined(property);
        switch (property)
        {
            case PORT:
                _port = value.trim();
                break;
            case CONNECTION_LIMIT:
                _connectionCountLimit = validateCounterLimit(Property.CONNECTION_LIMIT, Integer.parseInt(value));
                break;
            case CONNECTION_FREQUENCY_LIMIT:
                addFrequencyLimit(value);
                break;
            default:
        }
        LOGGER.debug("Parsed {} with value {}", property, value);
    }

    private void addFrequencyLimit(String value)
    {
        if (value.contains(TIME_PERIOD_DELIMITER))
        {
            final String[] frequencyLimit = value.split(TIME_PERIOD_DELIMITER, 2);
            _connectionFrequencyLimit = validateCounterLimit(
                    Property.CONNECTION_FREQUENCY_LIMIT, Integer.parseInt(frequencyLimit[0]));
            _connectionFrequencyPeriod = validateTimePeriod(parseTimePeriod(frequencyLimit[1]));
        }
        else
        {
            _connectionFrequencyLimit = validateCounterLimit(
                    Property.CONNECTION_FREQUENCY_LIMIT, Integer.parseInt(value));
            _connectionFrequencyPeriod = null;
        }
    }

    private Duration validateTimePeriod(Duration period)
    {
        if (period != null && period.isNegative())
        {
            throw new IllegalArgumentException(
                    String.format("Frequency period can not be negative %d s", period.getSeconds()));
        }
        return period;
    }

    private Duration parseTimePeriod(String period)
    {
        if ("Second".equalsIgnoreCase(period))
        {
            return SECOND;
        }
        if ("Minute".equalsIgnoreCase(period))
        {
            return MINUTE;
        }
        if ("Hour".equalsIgnoreCase(period))
        {
            return HOUR;
        }
        if (period.matches("\\d+.*"))
        {
            return Duration.parse("PT" + period);
        }
        if (period.matches("[hHmMsS]"))
        {
            return Duration.parse("PT1" + period);
        }
        return Duration.parse(period);
    }

    private Integer validateCounterLimit(Property property, int limit)
    {
        if (limit < 0)
        {
            LOGGER.debug("Negative value of {}, using {} instead", property, Integer.MAX_VALUE);
            return Integer.MAX_VALUE;
        }
        return limit;
    }

    private void checkPropertyAlreadyDefined(Property property)
    {
        if (_properties.contains(property))
        {
            throw new IllegalStateException(String.format("Property '%s' has already been defined", property));
        }
        else
        {
            _properties.add(property);
        }
    }

    public Integer getConnectionCountLimit()
    {
        return _connectionCountLimit;
    }

    public Integer getConnectionFrequencyLimit()
    {
        return _connectionFrequencyLimit;
    }

    public Duration getConnectionFrequencyPeriod()
    {
        return _connectionFrequencyPeriod;
    }

    public boolean isUserBlocked()
    {
        return _blocked;
    }

    public String getPort()
    {
        return _port;
    }

    public void setBlockedUser()
    {
        _blocked = true;
    }

    public boolean isEmpty()
    {
        return !_blocked && _connectionCountLimit == null && _connectionFrequencyLimit == null;
    }

    public enum Property
    {
        PORT,
        CONNECTION_LIMIT,
        CONNECTION_FREQUENCY_LIMIT;

        private static final Map<String, Property> NAME_TO_PROPERTY = new HashMap<>();

        static
        {
            for (final Property property : values())
            {
                NAME_TO_PROPERTY.put(property.name().toLowerCase(Locale.ENGLISH), property);
                NAME_TO_PROPERTY.put(property.name()
                        .replace("_", "").toLowerCase(Locale.ENGLISH), property);
                NAME_TO_PROPERTY.put(property.name()
                        .replace("_", "-").toLowerCase(Locale.ENGLISH), property);
            }
        }

        public static Property parse(String text)
        {
            return NAME_TO_PROPERTY.get(text.toLowerCase(Locale.ENGLISH).trim());
        }

        @Override
        public String toString()
        {
            return name().toLowerCase(Locale.ENGLISH);
        }
    }
}
