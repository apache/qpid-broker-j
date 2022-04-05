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
package org.apache.qpid.server.query.engine.parsing.expression.function.datetime;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.qpid.server.query.engine.TestBroker;
import org.apache.qpid.server.query.engine.evaluator.QueryEvaluator;
import org.apache.qpid.server.query.engine.evaluator.settings.QuerySettings;
import org.apache.qpid.server.query.engine.exception.QueryParsingException;

public class ExtractExpressionTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    private final QuerySettings _querySettings = new QuerySettings();

    @Test()
    public void extractYear()
    {
        String query = "select extract(YEAR from '2000-01-01 00:00:00') as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(2000, result.get(0).get("result"));

        query = "select extract(YEAR from '3000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(3000, result.get(0).get("result"));

        query = "select extract(YEAR from '1000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(1000, result.get(0).get("result"));
    }

    @Test()
    public void extractMonth()
    {
        String query = "select extract(month from '2000-01-01 00:00:00') as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(1, result.get(0).get("result"));

        query = "select extract(month from '2000-02-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(2, result.get(0).get("result"));

        query = "select extract(month from '2000-03-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(3, result.get(0).get("result"));

        query = "select extract(month from '2000-04-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(4, result.get(0).get("result"));

        query = "select extract(month from '2000-05-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(5, result.get(0).get("result"));

        query = "select extract(month from '2000-06-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(6, result.get(0).get("result"));

        query = "select extract(month from '2000-07-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(7, result.get(0).get("result"));

        query = "select extract(month from '2000-08-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(8, result.get(0).get("result"));

        query = "select extract(month from '2000-09-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(9, result.get(0).get("result"));

        query = "select extract(month from '2000-10-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(10, result.get(0).get("result"));

        query = "select extract(month from '2000-11-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(11, result.get(0).get("result"));

        query = "select extract(month from '2000-12-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(12, result.get(0).get("result"));
    }

    @Test()
    public void extractWeek()
    {
        String query = "select extract(week from '2000-01-01 00:00:00') as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(1, result.get(0).get("result"));

        query = "select extract(week from '2000-01-08 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(2, result.get(0).get("result"));

        query = "select extract(week from '2000-01-15 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(3, result.get(0).get("result"));

        query = "select extract(week from '2000-01-22 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(4, result.get(0).get("result"));

        query = "select extract(week from '2000-01-29 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(5, result.get(0).get("result"));

        query = "select extract(week from '2000-02-05 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(6, result.get(0).get("result"));

        query = "select extract(week from '2000-02-12 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(7, result.get(0).get("result"));

        query = "select extract(week from '2000-02-19 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(8, result.get(0).get("result"));

        query = "select extract(week from '2000-02-26 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(9, result.get(0).get("result"));

        query = "select extract(week from '2000-03-04 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(10, result.get(0).get("result"));
    }

    @Test()
    public void extractDay()
    {
        String query = "select extract(day from '2000-01-01 00:00:00') as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(1, result.get(0).get("result"));

        query = "select extract(day from '2000-02-29 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(29, result.get(0).get("result"));

        query = "select extract(day from '2000-12-31 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(31, result.get(0).get("result"));
    }

    @Test()
    public void extractHour()
    {
        String query = "select extract(hour from '2000-01-01 00:00:00') as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(0, result.get(0).get("result"));

        query = "select extract(hour from '2000-01-01 12:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(12, result.get(0).get("result"));

        query = "select extract(hour from '2000-01-01 23:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(23, result.get(0).get("result"));
    }

    @Test()
    public void extractMinute()
    {
        String query = "select extract(minute from '2000-01-01 00:00:00') as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(0, result.get(0).get("result"));

        query = "select extract(minute from '2000-01-01 12:30:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(30, result.get(0).get("result"));

        query = "select extract(minute from '2000-01-01 23:59:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(59, result.get(0).get("result"));
    }

    @Test()
    public void extractSecond()
    {
        String query = "select extract(second from '2000-01-01 00:01:00') as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(0, result.get(0).get("result"));

        query = "select extract(second from '2000-01-01 12:32:30') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(30, result.get(0).get("result"));

        query = "select extract(second from '2000-01-01 23:57:59') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(59, result.get(0).get("result"));
    }

    @Test()
    public void extractMillisecond()
    {
        String query = "select extract(millisecond from '2000-01-01 00:00:00') as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(0, result.get(0).get("result"));

        query = "select extract(millisecond from '2000-01-01 12:30:30.500') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(500, result.get(0).get("result"));

        query = "select extract(millisecond from '2000-01-01 23:59:59.999') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(999, result.get(0).get("result"));
    }

    @Test()
    public void extractFromCreatedTime()
    {
        int year = LocalDateTime.now().getYear();
        String query = "select * from queue where extract(year from createdTime) = " + year;
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(70, result.size());
    }

    @Test()
    public void noArguments()
    {
        String query = "select extract() as result";
        try
        {
            _queryEvaluator.execute(query, _querySettings);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Function 'EXTRACT' requires 2 parameters", e.getMessage());
        }
    }

    @Test()
    public void threeArguments()
    {
        String query = "select extract(year, from, current_timestamp()) as result";
        try
        {
            _queryEvaluator.execute(query, _querySettings);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Encountered \" \",\" \",\"\" at line 1, column 20. Was expecting: \"FROM\" ...", e.getMessage());
        }
    }
}
