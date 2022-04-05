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
package org.apache.qpid.server.query.engine.utils;

import java.math.RoundingMode;
import java.time.ZoneId;

import org.apache.qpid.server.query.engine.evaluator.DateFormat;
import org.apache.qpid.server.query.engine.evaluator.settings.QuerySettings;

public class QuerySettingsBuilder
{
    private DateFormat _dateTimeFormat;

    private String _datePattern;

    private String _dateTimePattern;

    private Integer _decimalDigits;

    private RoundingMode _roundingMode;

    private ZoneId _zoneId;

    public QuerySettingsBuilder dateTimeFormat(DateFormat dateTimeFormat)
    {
        _dateTimeFormat = dateTimeFormat;
        return this;
    }

    public QuerySettingsBuilder datePattern(String datePattern)
    {
        _datePattern = datePattern;
        return this;
    }

    public QuerySettingsBuilder dateTimePattern(String dateTimePattern)
    {
        _dateTimePattern = dateTimePattern;
        return this;
    }

    public QuerySettingsBuilder decimalDigits(int decimalDigits)
    {
        _decimalDigits = decimalDigits;
        return this;
    }

    public QuerySettingsBuilder roundingMode(RoundingMode roundingMode)
    {
        _roundingMode = roundingMode;
        return this;
    }

    public QuerySettingsBuilder zoneId(ZoneId zoneId)
    {
        _zoneId = zoneId;
        return this;
    }

    public QuerySettings build()
    {
        final QuerySettings querySettings = new QuerySettings();
        if (_dateTimeFormat != null)
        {
            querySettings.setDateTimeFormat(_dateTimeFormat);
        }
        if (_datePattern != null)
        {
            querySettings.setDatePattern(_datePattern);
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
            querySettings.setRoundingMode(_roundingMode);
        }
        if (_zoneId != null)
        {
            querySettings.setZoneId(_zoneId);
        }
        return querySettings;
    }
}
