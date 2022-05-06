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
package org.apache.qpid.server.query.engine.evaluator.settings;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.ZoneId;

import org.apache.qpid.server.query.engine.evaluator.DateFormat;

/**
 * Contains settings for query execution
 */
// sonar complains about underscores in variable names
@SuppressWarnings({"java:S116", "unused"})
public class QuerySettings
{
    /**
     * Format of date/time representation
     */
    private DateFormat _dateTimeFormat = DateFormat.STRING;

    /**
     * Pattern for date string representation
     */
    private String _datePattern = "uuuu-MM-dd";

    /**
     * Pattern for date/time string representation
     */
    private String _dateTimePattern = "uuuu-MM-dd HH:mm:ss";

    /**
     * Amount of decimal digits after the point
     */
    private int _decimalDigits = 6;

    /**
     * Maximal allowed BigDecimal value.
     * Is needed to prevent heap memory consumption when calculating very large numbers.
     */
    private BigDecimal _maxBigDecimalValue = BigDecimal.valueOf(Double.MAX_VALUE).pow(4);

    /**
     * Maximal amount of queries allowed caching
     */
    private int _maxQueryCacheSize = 1000;

    /**
     * Maximal amount of query tree nodes allowed
     */
    private int _maxQueryDepth = 4096;

    /**
     * Rounding mode used in calculations
     */
    private RoundingMode _roundingMode = RoundingMode.HALF_UP;

    /**
     * ZoneId used in date/time representation
     */
    private ZoneId _zoneId = ZoneId.of("UTC");

    public DateFormat getDateTimeFormat()
    {
        return _dateTimeFormat;
    }

    public String getDatePattern()
    {
        return _datePattern;
    }

    public String getDateTimePattern()
    {
        return _dateTimePattern;
    }

    public int getDecimalDigits()
    {
        return _decimalDigits;
    }

    public RoundingMode getRoundingMode()
    {
        return _roundingMode;
    }

    public ZoneId getZoneId()
    {
        return _zoneId;
    }

    public BigDecimal getMaxBigDecimalValue()
    {
        return _maxBigDecimalValue;
    }

    public int getMaxQueryCacheSize()
    {
        return _maxQueryCacheSize;
    }

    public int getMaxQueryDepth()
    {
        return _maxQueryDepth;
    }

    public void setDateTimeFormat(final DateFormat dateTimeFormat)
    {
        _dateTimeFormat = dateTimeFormat;
    }

    public void setDatePattern(final String datePattern)
    {
        _datePattern = datePattern;
    }

    public void setDateTimePattern(final String dateTimePattern)
    {
        _dateTimePattern = dateTimePattern;
    }

    public void setDecimalDigits(final int decimalDigits)
    {
        _decimalDigits = decimalDigits;
    }

    public void setMaxBigDecimalValue(final BigDecimal maxBigDecimalValue)
    {
        _maxBigDecimalValue = maxBigDecimalValue;
    }

    public void setMaxQueryCacheSize(final int maxQueryCacheSize)
    {
        _maxQueryCacheSize = maxQueryCacheSize;
    }

    public void setMaxQueryDepth(final int maxQueryDepth)
    {
        _maxQueryDepth = maxQueryDepth;
    }

    public void setRoundingMode(final RoundingMode roundingMode)
    {
        _roundingMode = roundingMode;
    }

    public void setZoneId(final ZoneId zoneId)
    {
        _zoneId = zoneId;
    }

    @Override
    public String toString()
    {
        return "QuerySettings{"
               + "_dateTimeFormat=" + _dateTimeFormat
               + ", _datePattern='" + _datePattern + '\''
               + ", _dateTimePattern='" + _dateTimePattern + '\''
               + ", _decimalDigits=" + _decimalDigits
               + ", _maxBigDecimalValue=" + _maxBigDecimalValue
               + ", _maxQueryCacheSize=" + _maxQueryCacheSize
               + ", _maxQueryDepth=" + _maxQueryDepth
               + ", _roundingMode=" + _roundingMode
               + ", _zoneId=" + _zoneId
               + '}';
    }
}
