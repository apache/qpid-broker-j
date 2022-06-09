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

import org.apache.qpid.server.query.engine.evaluator.DateFormat;

/**
 * Contains default settings for query execution
 */
public class DefaultQuerySettings
{
    /**
     * Format of date/time representation
     */
    public static DateFormat DATE_TIME_FORMAT = DateFormat.STRING;

    /**
     * Pattern for date string representation
     */
    public static String DATE_PATTERN = "uuuu-MM-dd";

    /**
     * Pattern for date/time string representation
     */
    public static String DATE_TIME_PATTERN = "uuuu-MM-dd HH:mm:ss";

    /**
     * Amount of decimal digits after the point
     */
    public static int DECIMAL_DIGITS = 6;

    /**
     * Maximal allowed BigDecimal value.
     * Is needed to prevent heap memory consumption when calculating very large numbers.
     */
    public static BigDecimal MAX_BIG_DECIMAL_VALUE = BigDecimal.valueOf(Double.MAX_VALUE).pow(4);

    /**
     * Maximal amount of queries allowed caching
     */
    public static int MAX_QUERY_CACHE_SIZE = 1000;

    /**
     * Maximal amount of query tree nodes allowed
     */
    public static int MAX_QUERY_DEPTH = 4096;

    /**
     * Rounding mode used in calculations
     */
    public static RoundingMode ROUNDING_MODE = RoundingMode.HALF_UP;

    /**
     * ZoneId used in date/time representation
     */
    public static String ZONE_ID = "UTC";
}
