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
 * Tests designed to verify the public class {@link DateAddExpression} functionality
 */
public class DateAddExpressionTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    private final QuerySettings _querySettings = new QuerySettings();

    @Test()
    public void noArguments()
    {
        String query = "select dateadd() as result";
        try
        {
            _queryEvaluator.execute(query, _querySettings);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Function 'DATEADD' requires 3 parameters", e.getMessage());
        }
    }

    @Test()
    public void fourArguments()
    {
        String query = "select dateadd(YEAR, 1, 2, 3) as result";
        try
        {
            _queryEvaluator.execute(query, _querySettings);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Function 'DATEADD' requires 3 parameters", e.getMessage());
        }
    }

    @Test()
    public void invalidDatepart()
    {
        try
        {
            String query = "select dateadd(YEARS, 1, '2000-02-28 00:00:00') as result";
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
            String query = "select dateadd(test, 1, '2000-02-28 00:00:00') as result";
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
            String query = "select dateadd(null, 1, '2000-02-28 00:00:00') as result";
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
            String query = "select dateadd(1, 1, '2000-02-28 00:00:00') as result";
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
            String query = "select dateadd(0, 1, '2000-02-28 00:00:00') as result";
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
            String query = "select dateadd('test', 1, '2000-02-28 00:00:00') as result";
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
    public void invalidAmountToAdd()
    {
        try
        {
            String query = "select dateadd(YEAR, '1', '2000-02-30 00:00:00') as result";
            _queryEvaluator.execute(query, _querySettings);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryEvaluationException.class, e.getClass());
            assertEquals("Parameter of function 'DATEADD' invalid (parameter type: String)", e.getMessage());
        }
    }

    @Test()
    public void nullAmountToAdd()
    {
        String query = "select dateadd(YEAR, null, '2000-02-30 00:00:00') as result";
        try
        {
            _queryEvaluator.execute(query, _querySettings);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryEvaluationException.class, e.getClass());
            assertEquals("Parameter of function 'DATEADD' invalid (parameter type: null)", e.getMessage());
        }
    }

    @Test()
    public void invalidDate()
    {
        String query = "select dateadd(YEAR, 1, '2000-02-30 00:00:00') as result";
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
        String query = "select dateadd(YEAR, 1, null) as result";
        try
        {
            _queryEvaluator.execute(query, _querySettings);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryEvaluationException.class, e.getClass());
            assertEquals("Parameter of function 'DATEADD' invalid (parameter type: null)", e.getMessage());
        }
    }

    @Test()
    public void addingLeapYears()
    {
        String query = "select dateadd(YEAR, 1, '2000-02-29 00:00:00') as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2001-02-28 00:00:00", result.get(0).get("result"));

        query = "select dateadd(YEAR, 4, '2000-02-29 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2004-02-29 00:00:00", result.get(0).get("result"));
    }

    @Test()
    public void subtractingLeapYears()
    {
        String query = "select dateadd(YEAR, -1, '2000-02-29 00:00:00') as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-02-28 00:00:00", result.get(0).get("result"));

        query = "select dateadd(YEAR, -4, '2000-02-29 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1996-02-29 00:00:00", result.get(0).get("result"));
    }

    @Test()
    public void addingYears()
    {
        String query = "select dateadd(YEAR, 1, '2000-01-01 00:00:00') as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2001-01-01 00:00:00", result.get(0).get("result"));

        query = "select dateadd(YEAR, 5, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2005-01-01 00:00:00", result.get(0).get("result"));

        query = "select dateadd(YEAR, 10, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2010-01-01 00:00:00", result.get(0).get("result"));

        query = "select dateadd(YEAR, 100, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2100-01-01 00:00:00", result.get(0).get("result"));

        query = "select dateadd(YEAR, 1000, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("3000-01-01 00:00:00", result.get(0).get("result"));
    }

    @Test()
    public void subtractingYears()
    {
        String query = "select dateadd(YEAR, -1, '2000-01-01 00:00:00') as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-01-01 00:00:00", result.get(0).get("result"));

        query = "select dateadd(YEAR, -5, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1995-01-01 00:00:00", result.get(0).get("result"));

        query = "select dateadd(YEAR, -10, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1990-01-01 00:00:00", result.get(0).get("result"));

        query = "select dateadd(YEAR, -100, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1900-01-01 00:00:00", result.get(0).get("result"));

        query = "select dateadd(YEAR, -1000, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1000-01-01 00:00:00", result.get(0).get("result"));
    }

    @Test()
    public void addingMonths()
    {
        String query = "select dateadd(MONTH, 1, '2000-01-01 00:00:00') as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-02-01 00:00:00", result.get(0).get("result"));

        query = "select dateadd(MONTH, 2, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-03-01 00:00:00", result.get(0).get("result"));

        query = "select dateadd(MONTH, 3, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-04-01 00:00:00", result.get(0).get("result"));

        query = "select dateadd(MONTH, 4, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-05-01 00:00:00", result.get(0).get("result"));

        query = "select dateadd(MONTH, 5, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-06-01 00:00:00", result.get(0).get("result"));

        query = "select dateadd(MONTH, 6, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-07-01 00:00:00", result.get(0).get("result"));

        query = "select dateadd(MONTH, 7, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-08-01 00:00:00", result.get(0).get("result"));

        query = "select dateadd(MONTH, 8, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-09-01 00:00:00", result.get(0).get("result"));

        query = "select dateadd(MONTH, 9, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-10-01 00:00:00", result.get(0).get("result"));

        query = "select dateadd(MONTH, 10, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-11-01 00:00:00", result.get(0).get("result"));

        query = "select dateadd(MONTH, 11, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-12-01 00:00:00", result.get(0).get("result"));

        query = "select dateadd(MONTH, 12, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2001-01-01 00:00:00", result.get(0).get("result"));

        query = "select dateadd(MONTH, 13, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2001-02-01 00:00:00", result.get(0).get("result"));
    }

    @Test()
    public void subtractingMonths()
    {
        String query = "select dateadd(MONTH, -1, '2000-01-01 00:00:00') as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-12-01 00:00:00", result.get(0).get("result"));

        query = "select dateadd(MONTH, -2, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-11-01 00:00:00", result.get(0).get("result"));

        query = "select dateadd(MONTH, -3, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-10-01 00:00:00", result.get(0).get("result"));

        query = "select dateadd(MONTH, -4, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-09-01 00:00:00", result.get(0).get("result"));

        query = "select dateadd(MONTH, -5, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-08-01 00:00:00", result.get(0).get("result"));

        query = "select dateadd(MONTH, -6, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-07-01 00:00:00", result.get(0).get("result"));

        query = "select dateadd(MONTH, -7, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-06-01 00:00:00", result.get(0).get("result"));

        query = "select dateadd(MONTH, -8, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-05-01 00:00:00", result.get(0).get("result"));

        query = "select dateadd(MONTH, -9, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-04-01 00:00:00", result.get(0).get("result"));

        query = "select dateadd(MONTH, -10, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-03-01 00:00:00", result.get(0).get("result"));

        query = "select dateadd(MONTH, -11, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-02-01 00:00:00", result.get(0).get("result"));

        query = "select dateadd(MONTH, -12, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-01-01 00:00:00", result.get(0).get("result"));

        query = "select dateadd(MONTH, -13, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1998-12-01 00:00:00", result.get(0).get("result"));
    }

    @Test()
    public void addingWeeks()
    {
        String query = "select dateadd(WEEK, 1, '2000-01-01 00:00:00') as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-01-08 00:00:00", result.get(0).get("result"));

        query = "select dateadd(WEEK, 2, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-01-15 00:00:00", result.get(0).get("result"));

        query = "select dateadd(WEEK, 3, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-01-22 00:00:00", result.get(0).get("result"));

        query = "select dateadd(WEEK, 4, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-01-29 00:00:00", result.get(0).get("result"));

        query = "select dateadd(WEEK, 5, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-02-05 00:00:00", result.get(0).get("result"));

        query = "select dateadd(WEEK, 6, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-02-12 00:00:00", result.get(0).get("result"));

        query = "select dateadd(WEEK, 7, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-02-19 00:00:00", result.get(0).get("result"));

        query = "select dateadd(WEEK, 8, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-02-26 00:00:00", result.get(0).get("result"));

        query = "select dateadd(WEEK, 9, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-03-04 00:00:00", result.get(0).get("result"));

        query = "select dateadd(WEEK, 10, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-03-11 00:00:00", result.get(0).get("result"));
    }

    @Test()
    public void subtractingWeeks()
    {
        String query = "select dateadd(WEEK, -1, '2000-01-01 00:00:00') as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-12-25 00:00:00", result.get(0).get("result"));

        query = "select dateadd(WEEK, -2, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-12-18 00:00:00", result.get(0).get("result"));

        query = "select dateadd(WEEK, -3, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-12-11 00:00:00", result.get(0).get("result"));

        query = "select dateadd(WEEK, -4, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-12-04 00:00:00", result.get(0).get("result"));

        query = "select dateadd(WEEK, -5, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-11-27 00:00:00", result.get(0).get("result"));

        query = "select dateadd(WEEK, -6, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-11-20 00:00:00", result.get(0).get("result"));

        query = "select dateadd(WEEK, -7, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-11-13 00:00:00", result.get(0).get("result"));

        query = "select dateadd(WEEK, -8, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-11-06 00:00:00", result.get(0).get("result"));

        query = "select dateadd(WEEK, -9, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-10-30 00:00:00", result.get(0).get("result"));

        query = "select dateadd(WEEK, -10, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-10-23 00:00:00", result.get(0).get("result"));
    }

    @Test()
    public void addingDays()
    {
        String query = "select dateadd(DAY, 1, '2000-01-01 00:00:00') as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-01-02 00:00:00", result.get(0).get("result"));

        query = "select dateadd(DAY, 10, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-01-11 00:00:00", result.get(0).get("result"));

        query = "select dateadd(DAY, 20, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-01-21 00:00:00", result.get(0).get("result"));

        query = "select dateadd(DAY, 30, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-01-31 00:00:00", result.get(0).get("result"));

        query = "select dateadd(DAY, 40, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-02-10 00:00:00", result.get(0).get("result"));

        query = "select dateadd(DAY, 50, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-02-20 00:00:00", result.get(0).get("result"));

        query = "select dateadd(DAY, 60, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-03-01 00:00:00", result.get(0).get("result"));

        query = "select dateadd(DAY, 366, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2001-01-01 00:00:00", result.get(0).get("result"));
    }

    @Test()
    public void subtractingDays()
    {
        String query = "select dateadd(DAY, -1, '2000-01-01 00:00:00') as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-12-31 00:00:00", result.get(0).get("result"));

        query = "select dateadd(DAY, -10, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-12-22 00:00:00", result.get(0).get("result"));

        query = "select dateadd(DAY, -20, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-12-12 00:00:00", result.get(0).get("result"));

        query = "select dateadd(DAY, -30, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-12-02 00:00:00", result.get(0).get("result"));

        query = "select dateadd(DAY, -40, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-11-22 00:00:00", result.get(0).get("result"));

        query = "select dateadd(DAY, -50, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-11-12 00:00:00", result.get(0).get("result"));

        query = "select dateadd(DAY, -60, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-11-02 00:00:00", result.get(0).get("result"));

        query = "select dateadd(DAY, -366, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1998-12-31 00:00:00", result.get(0).get("result"));
    }

    @Test()
    public void addingHours()
    {
        String query = "select dateadd(HOUR, 1, '2000-01-01 00:00:00') as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-01-01 01:00:00", result.get(0).get("result"));

        query = "select dateadd(HOUR, 24, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-01-02 00:00:00", result.get(0).get("result"));

        query = "select dateadd(HOUR, 36, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-01-02 12:00:00", result.get(0).get("result"));

        query = "select dateadd(HOUR, 48, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-01-03 00:00:00", result.get(0).get("result"));

        query = "select dateadd(HOUR, 240, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-01-11 00:00:00", result.get(0).get("result"));
    }

    @Test()
    public void subtractingHours()
    {
        String query = "select dateadd(HOUR, -1, '2000-01-01 00:00:00') as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-12-31 23:00:00", result.get(0).get("result"));

        query = "select dateadd(HOUR, -24, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-12-31 00:00:00", result.get(0).get("result"));

        query = "select dateadd(HOUR, -36, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-12-30 12:00:00", result.get(0).get("result"));

        query = "select dateadd(HOUR, -48, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-12-30 00:00:00", result.get(0).get("result"));

        query = "select dateadd(HOUR, -240, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-12-22 00:00:00", result.get(0).get("result"));
    }

    @Test()
    public void addingMinutes()
    {
        String query = "select dateadd(MINUTE, 1, '2000-01-01 00:00:00') as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-01-01 00:01:00", result.get(0).get("result"));

        query = "select dateadd(MINUTE, 30, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-01-01 00:30:00", result.get(0).get("result"));

        query = "select dateadd(MINUTE, 60, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-01-01 01:00:00", result.get(0).get("result"));

        query = "select dateadd(MINUTE, 90, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-01-01 01:30:00", result.get(0).get("result"));

        query = "select dateadd(MINUTE, 1440, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-01-02 00:00:00", result.get(0).get("result"));
    }

    @Test()
    public void subtractingMinutes()
    {
        String query = "select dateadd(MINUTE, -1, '2000-01-01 00:00:00') as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-12-31 23:59:00", result.get(0).get("result"));

        query = "select dateadd(MINUTE, -30, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-12-31 23:30:00", result.get(0).get("result"));

        query = "select dateadd(MINUTE, -60, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-12-31 23:00:00", result.get(0).get("result"));

        query = "select dateadd(MINUTE, -90, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-12-31 22:30:00", result.get(0).get("result"));

        query = "select dateadd(MINUTE, -1440, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-12-31 00:00:00", result.get(0).get("result"));
    }

    @Test()
    public void addingSeconds()
    {
        String query = "select dateadd(SECOND, 1, '2000-01-01 00:00:00') as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-01-01 00:00:01", result.get(0).get("result"));

        query = "select dateadd(SECOND, 60, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-01-01 00:01:00", result.get(0).get("result"));

        query = "select dateadd(SECOND, 600, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-01-01 00:10:00", result.get(0).get("result"));

        query = "select dateadd(SECOND, 3600, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-01-01 01:00:00", result.get(0).get("result"));

        query = "select dateadd(SECOND, 86400, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-01-02 00:00:00", result.get(0).get("result"));
    }

    @Test()
    public void subtractingSeconds()
    {
        String query = "select dateadd(SECOND, -1, '2000-01-01 00:00:00') as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-12-31 23:59:59", result.get(0).get("result"));

        query = "select dateadd(SECOND, -60, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-12-31 23:59:00", result.get(0).get("result"));

        query = "select dateadd(SECOND, -600, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-12-31 23:50:00", result.get(0).get("result"));

        query = "select dateadd(SECOND, -3600, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-12-31 23:00:00", result.get(0).get("result"));

        query = "select dateadd(SECOND, -86400, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-12-31 00:00:00", result.get(0).get("result"));
    }

    @Test()
    public void addingMilliseconds()
    {
        String query = "select dateadd(MILLISECOND, 1, '2000-01-01 00:00:00') as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-01-01 00:00:00.001", result.get(0).get("result"));

        query = "select dateadd(MILLISECOND, 500, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-01-01 00:00:00.5", result.get(0).get("result"));

        query = "select dateadd(MILLISECOND, 1000, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-01-01 00:00:01", result.get(0).get("result"));

        query = "select dateadd(MILLISECOND, 60000, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-01-01 00:01:00", result.get(0).get("result"));

        query = "select dateadd(MILLISECOND, 600000, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-01-01 00:10:00", result.get(0).get("result"));

        query = "select dateadd(MILLISECOND, 3600000, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-01-01 01:00:00", result.get(0).get("result"));

        query = "select dateadd(MILLISECOND, 86400000, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-01-02 00:00:00", result.get(0).get("result"));
    }

    @Test()
    public void subtractingMilliseconds()
    {
        String query = "select dateadd(MILLISECOND, -1, '2000-01-01 00:00:00') as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-12-31 23:59:59.999", result.get(0).get("result"));

        query = "select dateadd(MILLISECOND, -500, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-12-31 23:59:59.5", result.get(0).get("result"));

        query = "select dateadd(MILLISECOND, -1000, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-12-31 23:59:59", result.get(0).get("result"));

        query = "select dateadd(MILLISECOND, -60000, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-12-31 23:59:00", result.get(0).get("result"));

        query = "select dateadd(MILLISECOND, -600000, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-12-31 23:50:00", result.get(0).get("result"));

        query = "select dateadd(MILLISECOND, -3600000, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-12-31 23:00:00", result.get(0).get("result"));

        query = "select dateadd(MILLISECOND, -86400000, '2000-01-01 00:00:00') as result";
        result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("1999-12-31 00:00:00", result.get(0).get("result"));
    }

    @Test()
    public void subtractingFromCreatedTime()
    {
        String query = "select * from queue where dateadd(year, -1, createdTime) < current_timestamp()" ;
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(70, result.size());
    }
}
