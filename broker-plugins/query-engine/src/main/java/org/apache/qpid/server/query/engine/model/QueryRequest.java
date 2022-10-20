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
package org.apache.qpid.server.query.engine.model;

import java.math.RoundingMode;

import org.apache.qpid.server.query.engine.evaluator.DateFormat;
import org.apache.qpid.server.query.engine.evaluator.settings.QuerySettings;

/**
 * Model / DTO class for a JSON request
 */
@SuppressWarnings({"java:S116", "unused"})
public class QueryRequest
{
    /**
     * SQL expression
     */
    private String _sql;

    /**
     * Format of datetime values (LONG or STRING)
     */
    private String _dateTimeFormat;

    /**
     * Pattern for formatting datetime values
     */
    private String _dateTimePattern;

    /**
     * Amount of decimal digits
     */
    private Integer _decimalDigits;

    /**
     * Preferred rounding mode (UP, DOWN, CEILING, FLOOR, HALF_UP, HALF_DOWN, HALF_EVEN, UNNECESSARY)
     */
    private String _roundingMode;

    public String getSql()
    {
        return _sql;
    }

    public void setSql(final String sql)
    {
        this._sql = sql;
    }

    public String getDateTimeFormat()
    {
        return _dateTimeFormat;
    }

    public void setDateTimeFormat(final String dateTimeFormat)
    {
        this._dateTimeFormat = dateTimeFormat;
    }

    public String getDateTimePattern()
    {
        return _dateTimePattern;
    }

    public void setDateTimePattern(final String dateTimePattern)
    {
        this._dateTimePattern = dateTimePattern;
    }

    public String getRoundingMode()
    {
        return _roundingMode;
    }

    public void setRoundingMode(final String roundingMode)
    {
        this._roundingMode = roundingMode;
    }

    public Integer getDecimalDigits()
    {
        return _decimalDigits;
    }

    public void setDecimalDigits(final Integer decimalDigits)
    {
        this._decimalDigits = decimalDigits;
    }

    public QuerySettings toQuerySettings()
    {
        final QuerySettings querySettings = new QuerySettings();
        if (_dateTimeFormat != null)
        {
            querySettings.setDateTimeFormat(DateFormat.valueOf(_dateTimeFormat));
        }
        if (_dateTimePattern != null)
        {
            querySettings.setDateTimePattern(_dateTimePattern);
        }
        if (_decimalDigits != null)
        {
            querySettings.setDecimalDigits(_decimalDigits);
        }
        if (_roundingMode != null)
        {
            querySettings.setRoundingMode(RoundingMode.valueOf(_roundingMode));
        }
        return querySettings;
    }
}
