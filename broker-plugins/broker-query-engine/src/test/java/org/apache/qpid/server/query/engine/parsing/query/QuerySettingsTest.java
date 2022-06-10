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
package org.apache.qpid.server.query.engine.parsing.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.query.engine.QueryEngine;
import org.apache.qpid.server.query.engine.TestBroker;
import org.apache.qpid.server.query.engine.evaluator.DateFormat;
import org.apache.qpid.server.query.engine.evaluator.QueryEvaluator;
import org.apache.qpid.server.query.engine.evaluator.settings.DefaultQuerySettings;
import org.apache.qpid.server.query.engine.evaluator.settings.QuerySettings;
import org.apache.qpid.server.query.engine.exception.QueryParsingException;
import org.apache.qpid.server.query.engine.utils.QuerySettingsBuilder;

public class QuerySettingsTest
{
    private final Broker<?> _broker = TestBroker.createBroker();

    @Test()
    public void customizeDateFormat()
    {
        QueryEvaluator queryEvaluator = new QueryEvaluator(_broker);

        QuerySettings querySettings = new QuerySettingsBuilder().dateTimeFormat(DateFormat.LONG).build();

        String query = "select current_timestamp() as result";
        List<Map<String, Object>> result = queryEvaluator.execute(query, querySettings).getResults();
        assertEquals(1, result.size());
        assertTrue(result.get(0).get("result") instanceof Long);

        querySettings = new QuerySettingsBuilder().dateTimeFormat(DateFormat.STRING).build();

        query = "select current_timestamp() as result";
        result = queryEvaluator.execute(query, querySettings).getResults();
        assertEquals(1, result.size());
        assertTrue(result.get(0).get("result") instanceof String);
    }

    @Test()
    public void customizeDatetimePattern()
    {
        DateTimeFormatter formatter = new DateTimeFormatterBuilder().appendPattern("yyyy/MM/dd HH:mm:ss")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 6, true)
            .toFormatter().withZone(ZoneId.systemDefault());

        QueryEvaluator queryEvaluator = new QueryEvaluator(_broker);
        QuerySettings querySettings = new QuerySettingsBuilder().dateTimePattern("yyyy/MM/dd HH:mm:ss").build();

        String query = "select current_timestamp() as result";
        List<Map<String, Object>> result = queryEvaluator.execute(query, querySettings).getResults();
        assertEquals(1, result.size());
        formatter.parse((String)result.get(0).get("result"));
    }

    @Test()
    public void customizeMaxQueryDepth()
    {
        QueryEngine queryEngine = new QueryEngine(_broker);
        queryEngine.setMaxQueryDepth(10);
        QueryEvaluator queryEvaluator = queryEngine.createEvaluator();

        String query = "select 1, 2, 3";
        List<Map<String, Object>> result = queryEvaluator.execute(query).getResults();
        assertEquals(3, result.size());

        try
        {
            query = "select 1, 2, 3, 4, 5, 6, 7, 8, 9, 0";
            queryEvaluator.execute(query);
            fail("Expected exception with message \"Max query depth reached: 10\"");
        }
        catch (QueryParsingException e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Max query depth reached: 10", e.getMessage());
        }

        try
        {
            query = "select (1 + 2) * (2 - 3)";
            queryEvaluator.execute(query);
            fail("Expected exception with message \"Max query depth reached: 10\"");
        }
        catch (QueryParsingException e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Max query depth reached: 10", e.getMessage());
        }
    }

    @Test()
    public void customizeMaxBigDecimalValue()
    {
        QueryEngine queryEngine = new QueryEngine(_broker);
        queryEngine.setMaxBigDecimalValue(BigDecimal.valueOf(100L));
        queryEngine.setMaxQueryDepth(DefaultQuerySettings.MAX_QUERY_DEPTH);
        QueryEvaluator queryEvaluator = queryEngine.createEvaluator();

        String query = "select 2 * 2 as result";
        List<Map<String, Object>> result = queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(4, result.get(0).get("result"));

        try
        {
            query = "select 10 * 10 as result";
            queryEvaluator.execute(query);
            fail("Expected exception with message \"Reached maximal allowed big decimal value: 100\"");
        }
        catch (QueryParsingException e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Reached maximal allowed big decimal value: 100", e.getMessage());
        }

        try
        {
            query = "select -10 * 10 as result";
            queryEvaluator.execute(query);
            fail("Expected exception with message \"Reached maximal allowed big decimal value: -100\"");
        }
        catch (QueryParsingException e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Reached maximal allowed big decimal value: -100", e.getMessage());
        }
    }

    @Test()
    public void customizeDecimalDigits()
    {
        QueryEvaluator queryEvaluator = new QueryEvaluator(_broker);

        QuerySettings querySettings = new QuerySettingsBuilder()
            .decimalDigits(2)
            .roundingMode(RoundingMode.DOWN)
            .build();

        String query = "select 1.999 as result";
        List<Map<String, Object>> result = queryEvaluator.execute(query, querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(1.99, result.get(0).get("result"));

        query = "select 1/3 as result";
        result = queryEvaluator.execute(query, querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(0.33, result.get(0).get("result"));

        querySettings = new QuerySettingsBuilder()
            .decimalDigits(4)
            .roundingMode(RoundingMode.DOWN)
            .build();

        query = "select 1.9999 as result";
        result = queryEvaluator.execute(query, querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(1.9999, result.get(0).get("result"));

        query = "select 1/3 as result";
        result = queryEvaluator.execute(query, querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(0.3333, result.get(0).get("result"));
    }

    @Test()
    public void customizeRoundingMode()
    {
        QueryEvaluator queryEvaluator = new QueryEvaluator(_broker);
        QuerySettings querySettings = new QuerySettingsBuilder().decimalDigits(2).roundingMode(RoundingMode.HALF_UP).build();

        String query = "select 1.977 as result";
        List<Map<String, Object>> result = queryEvaluator.execute(query, querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(1.98, result.get(0).get("result"));

        query = "select 1/3 as result";
        result = queryEvaluator.execute(query, querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(0.33, result.get(0).get("result"));

        query = "select 2/3 as result";
        result = queryEvaluator.execute(query, querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(0.67, result.get(0).get("result"));

        querySettings = new QuerySettingsBuilder().decimalDigits(2).roundingMode(RoundingMode.DOWN).build();

        query = "select 1.977 as result";
        result = queryEvaluator.execute(query, querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(1.97, result.get(0).get("result"));

        query = "select 1/3 as result";
        result = queryEvaluator.execute(query, querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(0.33, result.get(0).get("result"));

        query = "select 2/3 as result";
        result = queryEvaluator.execute(query, querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(0.66, result.get(0).get("result"));
    }

    @Test()
    public void customizeZoneIdViaQuerySettings()
    {
        DateTimeFormatter formatter = new DateTimeFormatterBuilder().appendPattern(DefaultQuerySettings.DATE_TIME_PATTERN)
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 6, true)
            .toFormatter().withZone(ZoneId.of(DefaultQuerySettings.ZONE_ID));

        QueryEvaluator queryEvaluator = new QueryEvaluator(_broker);
        QuerySettings querySettings = new QuerySettingsBuilder().zoneId(ZoneId.of(DefaultQuerySettings.ZONE_ID)).build();

        String query = "select current_timestamp() as result";
        List<Map<String, Object>> result = queryEvaluator.execute(query, querySettings).getResults();
        assertEquals(1, result.size());
        formatter.parse((String)result.get(0).get("result"));
    }

    @Test()
    public void customizeZoneIdViaQueryEngine()
    {
        QueryEngine queryEngine = new QueryEngine(_broker);
        queryEngine.setZoneId(ZoneId.of("GMT+2"));
        queryEngine.setMaxQueryDepth(DefaultQuerySettings.MAX_QUERY_DEPTH);
        QueryEvaluator queryEvaluator = queryEngine.createEvaluator();
        QuerySettings querySettings = new QuerySettings();

        Instant expected = LocalDateTime.now().atZone(ZoneId.of("GMT+2")).toInstant();
        DateTimeFormatter formatter = new DateTimeFormatterBuilder().appendPattern(DefaultQuerySettings.DATE_TIME_PATTERN)
           .appendFraction(ChronoField.NANO_OF_SECOND, 0, 6, true)
           .toFormatter().withZone(ZoneId.of("GMT+2")).withResolverStyle(ResolverStyle.STRICT);

        String query = "select current_timestamp() as result";
        List<Map<String,Object>> result = queryEvaluator.execute(query, querySettings).getResults();
        Instant actual = LocalDateTime.parse((String)result.get(0).get("result"), formatter).atZone(ZoneId.of("GMT+2")).toInstant();

        assertEquals(1, result.size());
        assertTrue(actual.toEpochMilli() - expected.toEpochMilli() < 1000);
    }

    @Test()
    public void customizeBroker()
    {
        try
        {
            new QueryEngine(null);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals("Broker instance not provided for querying", e.getMessage());
        }

        try
        {
            new QueryEvaluator(null);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals("Broker instance not provided for querying", e.getMessage());
        }
    }

    @Test()
    public void customizeMaxQueryCacheSize()
    {

        QueryEngine queryEngine = new QueryEngine(_broker);
        queryEngine.setMaxQueryCacheSize(10);
        queryEngine.setMaxQueryDepth(DefaultQuerySettings.MAX_QUERY_DEPTH);
        queryEngine.initQueryCache();
        QueryEvaluator queryEvaluator = queryEngine.createEvaluator();

        QuerySettings querySettings = new QuerySettings();

        String query = "select current_timestamp() as result";
        queryEvaluator.execute(query, querySettings);
        assertEquals(1, queryEngine.getCacheSize());

        query = "select current_timestamp() as result";
        queryEvaluator.execute(query, querySettings);
        assertEquals(1, queryEngine.getCacheSize());

        for (int i = 0; i < 100; i++)
        {
            query = "select current_timestamp() as result" + i;
            queryEvaluator.execute(query, querySettings);
        }
        assertEquals(10, queryEngine.getCacheSize());
    }
}
