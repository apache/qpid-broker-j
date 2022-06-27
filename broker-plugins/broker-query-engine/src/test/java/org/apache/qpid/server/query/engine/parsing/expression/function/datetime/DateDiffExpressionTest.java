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

import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.qpid.server.query.engine.TestBroker;
import org.apache.qpid.server.query.engine.evaluator.QueryEvaluator;
import org.apache.qpid.server.query.engine.evaluator.settings.QuerySettings;
import org.apache.qpid.server.query.engine.exception.QueryEvaluationException;
import org.apache.qpid.server.query.engine.exception.QueryParsingException;

/**
 * Tests designed to verify the public class {@link DateDiffExpression} functionality
 */
public class DateDiffExpressionTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    private final QuerySettings _querySettings = new QuerySettings();

    @Test()
    public void noArguments()
    {
        String query = "select datediff() as result";
        try
        {
            _queryEvaluator.execute(query, _querySettings);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Function 'DATEDIFF' requires 3 parameters", e.getMessage());
        }
    }

    @Test()
    public void fourArguments()
    {
        String query = "select datediff(YEAR, 1, 2, 3) as result";
        try
        {
            _queryEvaluator.execute(query, _querySettings);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Function 'DATEDIFF' requires 3 parameters", e.getMessage());
        }
    }

    @Test()
    public void invalidDatepart()
    {
        try
        {
            String query = "select datediff(YEARS, 1, '2000-02-28 00:00:00') as result";
            _queryEvaluator.execute(query, _querySettings);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Datepart 'YEARS' not supported", e.getMessage());
        }

        try
        {
            String query = "select datediff(test, 1, '2000-02-28 00:00:00') as result";
            _queryEvaluator.execute(query, _querySettings);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Datepart 'test' not supported", e.getMessage());
        }

        try
        {
            String query = "select datediff(null, 1, '2000-02-28 00:00:00') as result";
            _queryEvaluator.execute(query, _querySettings);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Datepart 'null' not supported", e.getMessage());
        }

        try
        {
            String query = "select datediff(1, 1, '2000-02-28 00:00:00') as result";
            _queryEvaluator.execute(query, _querySettings);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Datepart '1' not supported", e.getMessage());
        }

        try
        {
            String query = "select datediff(0, 1, '2000-02-28 00:00:00') as result";
            _queryEvaluator.execute(query, _querySettings);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Datepart '0' not supported", e.getMessage());
        }

        try
        {
            String query = "select datediff('test', 1, '2000-02-28 00:00:00') as result";
            _queryEvaluator.execute(query, _querySettings);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Datepart '\\'test\\'' not supported", e.getMessage());
        }
    }

    @Test()
    public void invalidDate()
    {
        String query = "select datediff(YEAR, '2000-01-01 00:00:00', '2000-02-30 00:00:00') as result";
        try
        {
            _queryEvaluator.execute(query, _querySettings);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryEvaluationException.class, e.getClass());
            assertEquals("Text '2000-02-30 00:00:00' could not be parsed: Invalid date 'FEBRUARY 30'", e.getMessage());
        }
    }

    @Test()
    public void nullDate()
    {
        String query = "select datediff(YEAR, '2000-01-01 00:00:00', null) as result";
        try
        {
            _queryEvaluator.execute(query, _querySettings);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryEvaluationException.class, e.getClass());
            assertEquals("Parameter of function 'DATEDIFF' invalid (parameter type: null)", e.getMessage());
        }
    }

    @Test()
    public void differenceInYears()
    {
        String query = "select datediff(YEAR, '2000-01-01 00:00:00', '2000-01-01 00:00:00') as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(0, result.get(0).get("result"));

        query = "select datediff(YEAR, '2000-01-01 00:00:00', '2010-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(10, result.get(0).get("result"));

        query = "select datediff(YEAR, '2010-01-01 00:00:00', '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(-10, result.get(0).get("result"));
    }

    @Test()
    public void differenceInMonths()
    {
        String query = "select datediff(MONTH, '2000-01-01 00:00:00', '2000-01-31 00:00:00') as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(0, result.get(0).get("result"));

        query = "select datediff(MONTH, '2000-01-01 00:00:00', '2000-02-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(1, result.get(0).get("result"));

        query = "select datediff(MONTH, '2000-01-01 00:00:00', '2001-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(12, result.get(0).get("result"));
    }

    @Test()
    public void differenceInWeeks()
    {
        String query = "select datediff(WEEK, '2000-01-01 00:00:00', '2000-01-31 00:00:00') as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(4, result.get(0).get("result"));

        query = "select datediff(WEEK, '2000-01-01 00:00:00', '2000-02-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(4, result.get(0).get("result"));

        query = "select datediff(WEEK, '2000-01-01 00:00:00', '2001-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(52, result.get(0).get("result"));
    }

    @Test()
    public void differenceInDays()
    {
        String query = "select datediff(DAY, '2000-01-01 00:00:00', '2000-01-31 00:00:00') as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(30, result.get(0).get("result"));

        query = "select datediff(DAY, '2000-01-01 00:00:00', '2000-02-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(31, result.get(0).get("result"));

        query = "select datediff(DAY, '2000-01-01 00:00:00', '2001-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(366, result.get(0).get("result"));
    }

    @Test()
    public void differenceInHours()
    {
        String query = "select datediff(HOUR, '2000-01-01 00:00:00', '2000-01-31 00:00:00') as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(720, result.get(0).get("result"));

        query = "select datediff(HOUR, '2000-01-01 00:00:00', '2000-02-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(744, result.get(0).get("result"));

        query = "select datediff(HOUR, '2000-01-01 00:00:00', '2001-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(8784, result.get(0).get("result"));
    }

    @Test()
    public void differenceInMinutes()
    {
        String query = "select datediff(MINUTE, '2000-01-01 00:00:00', '2000-01-31 00:00:00') as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(43200, result.get(0).get("result"));

        query = "select datediff(MINUTE, '2000-01-01 00:00:00', '2000-02-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(44640, result.get(0).get("result"));

        query = "select datediff(MINUTE, '2000-01-01 00:00:00', '2001-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(527040, result.get(0).get("result"));

        query = "select datediff(MINUTE, current_timestamp(), current_timestamp()) as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(0, result.get(0).get("result"));
    }

    @Test()
    public void differenceInSeconds()
    {
        String query = "select datediff(SECOND, '2000-01-01 00:00:00', '2000-01-31 00:00:00') as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(2592000, result.get(0).get("result"));

        query = "select datediff(SECOND, '2000-01-01 00:00:00', '2000-02-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(2678400, result.get(0).get("result"));

        query = "select datediff(SECOND, '2000-01-01 00:00:00', '2001-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(31622400, result.get(0).get("result"));
    }

    @Test()
    public void differenceInMilliseconds()
    {
        String query = "select datediff(MILLISECOND, '2000-01-01 00:00:00', '2000-01-31 00:00:00') as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(2592000000L, result.get(0).get("result"));

        query = "select datediff(MILLISECOND, '2000-01-01 00:00:00', '2000-02-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(2678400000L, result.get(0).get("result"));

        query = "select datediff(MILLISECOND, '2000-01-01 00:00:00', '2001-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(31622400000L, result.get(0).get("result"));
    }

    @Test()
    public void differenceBetweenCurrentTimeStampAndCreatedTime()
    {
        String query = "select * from queue where datediff(year, current_timestamp(), createdTime) = 0" ;
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(70, result.size());
    }
}
