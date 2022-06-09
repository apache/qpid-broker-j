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
package org.apache.qpid.server.query.engine.parsing.expression.function.string;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.qpid.server.query.engine.TestBroker;
import org.apache.qpid.server.query.engine.evaluator.settings.DefaultQuerySettings;
import org.apache.qpid.server.query.engine.exception.QueryEvaluationException;
import org.apache.qpid.server.query.engine.exception.QueryParsingException;
import org.apache.qpid.server.query.engine.evaluator.QueryEvaluator;

public class SubstringExpressionTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    @Test()
    public void oneArgument()
    {
        try
        {
            String query = "select substring('TEST') as result";
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Function 'SUBSTRING' requires at least 2 parameters", e.getMessage());
        }
    }

    @Test()
    public void twoArguments()
    {
        String query = "select substring('hello', 1) as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("hello", result.get(0).get("result"));

        query = "select substring('hello', 2) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("ello", result.get(0).get("result"));

        query = "select substring('hello', 3) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("llo", result.get(0).get("result"));

        query = "select substring('hello', 4) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("lo", result.get(0).get("result"));

        query = "select substring('hello', 5) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("o", result.get(0).get("result"));

        query = "select substring('hello', 6) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("", result.get(0).get("result"));
    }

    @Test()
    public void startIndexGreaterThanSourceLength()
    {
        String query = "select substring('hello', 6, 2) as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("", result.get(0).get("result"));
    }

    @Test()
    public void startIndexZero()
    {
        String query = "select substring('hello', 0, 2) as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("he", result.get(0).get("result"));
    }

    @Test()
    public void startIndexNegative()
    {
        String query = "select substring('hello', -2, 2) as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("lo", result.get(0).get("result"));

        query = "select substring('hello world', -5, 5) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("world", result.get(0).get("result"));
    }

    @Test()
    public void threeArguments()
    {
        String query = "select substring('hello', 1, 2) as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("he", result.get(0).get("result"));

        query = "select substring('hello', 2, 2) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("el", result.get(0).get("result"));
    }

    @Test()
    public void extractPartOfQueueName()
    {
        String query = "select substring(name, position('.', name) + 1) from queue";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(70, result.size());
    }

    @Test()
    public void thirdArgumentZero()
    {
        String query = "select substring('hello', 1, 0) as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("", result.get(0).get("result"));
    }

    @Test()
    public void thirdArgumentNegative()
    {
        String query = "select substring('hello', 1, -1) as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("", result.get(0).get("result"));
    }

    @Test()
    public void fourArguments()
    {
        String query = "select substring('test', 1, 1, 1) as result";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Function 'SUBSTRING' requires maximum 3 parameters", e.getMessage());
        }
    }

    @Test()
    public void noArguments()
    {
        String query = "select substring() as result";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Function 'SUBSTRING' requires at least 2 parameters", e.getMessage());
        }
    }

    @Test()
    public void secondArgumentZero()
    {
        String query = "select substring('test', 0) as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("test", result.get(0).get("result"));
    }

    @Test()
    public void secondArgumentInvalid()
    {
        String query = "select substring('test', null) as result";

        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryEvaluationException.class, e.getClass());
            assertEquals("Function 'SUBSTRING' requires argument 2 to be an integer", e.getMessage());
        }

        query = "select substring('test', true) as result";

        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryEvaluationException.class, e.getClass());
            assertEquals("Function 'SUBSTRING' requires argument 2 to be an integer", e.getMessage());
        }

        query = "select substring('test', '1') as result";

        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryEvaluationException.class, e.getClass());
            assertEquals("Function 'SUBSTRING' requires argument 2 to be an integer", e.getMessage());
        }
    }

    @Test()
    public void thirdArgumentInvalid()
    {
        String query = "select substring('test', 1, null) as result";

        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryEvaluationException.class, e.getClass());
            assertEquals("Function 'SUBSTRING' requires argument 3 to be an integer", e.getMessage());
        }

        query = "select substring('test', 1, true) as result";

        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryEvaluationException.class, e.getClass());
            assertEquals("Function 'SUBSTRING' requires argument 3 to be an integer", e.getMessage());
        }

        query = "select substring('test', 1, '1') as result";

        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryEvaluationException.class, e.getClass());
            assertEquals("Function 'SUBSTRING' requires argument 3 to be an integer", e.getMessage());
        }
    }

    @Test()
    public void nullArgument()
    {
        String query = "select substring(null, 1) as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertNull(result.get(0).get("result"));

        query = "select substring(null, 1, 2) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertNull(result.get(0).get("result"));
    }

    @Test()
    public void integerArgument()
    {
        String query = "select substring(1, 1) as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("1", result.get(0).get("result"));

        query = "select substring(-1, 1) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("-1", result.get(0).get("result"));
    }

    @Test()
    public void longArgument()
    {
        String query = "select substring(1L, 1) as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("1", result.get(0).get("result"));
    }

    @Test()
    public void doubleArgument()
    {
        String query = "select substring(2/3, 1) as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("0.666667", result.get(0).get("result"));
    }

    @Test()
    public void bigDecimalArgument()
    {
        String query = "select substring(" + BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE) + ", 1) as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("9223372036854775808", result.get(0).get("result"));
    }

    @Test()
    public void dateArgument()
    {
        DateTimeFormatter formatter = new DateTimeFormatterBuilder().appendPattern(DefaultQuerySettings.DATE_TIME_PATTERN)
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 6, true)
            .toFormatter().withZone(ZoneId.of(DefaultQuerySettings.ZONE_ID)).withResolverStyle(ResolverStyle.STRICT);

        String query = "select substring(lastUpdatedTime, 1) as result from queue where name='QUEUE_0'";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        formatter.parse((String)result.get(0).get("result"));
    }

    @Test()
    public void booleanArgument()
    {
        String query = "select substring(true, 1) as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("true", result.get(0).get("result"));

        query = "select substring(true, 2) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("rue", result.get(0).get("result"));
    }

    @Test()
    public void invalidArgumentType()
    {
        String query = "select substring(statistics, 1) from queue";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryEvaluationException.class, e.getClass());
            assertEquals("Parameter of function 'SUBSTRING' invalid (parameter type: HashMap)", e.getMessage());
        }
    }
}
