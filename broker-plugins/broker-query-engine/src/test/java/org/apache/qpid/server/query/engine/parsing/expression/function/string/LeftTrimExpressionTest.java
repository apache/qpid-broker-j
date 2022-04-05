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
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.qpid.server.query.engine.TestBroker;
import org.apache.qpid.server.query.engine.evaluator.EvaluationContext;
import org.apache.qpid.server.query.engine.evaluator.EvaluationContextHolder;
import org.apache.qpid.server.query.engine.evaluator.QueryEvaluator;
import org.apache.qpid.server.query.engine.evaluator.settings.QuerySettings;
import org.apache.qpid.server.query.engine.exception.QueryEvaluationException;
import org.apache.qpid.server.query.engine.exception.QueryParsingException;
import org.apache.qpid.server.query.engine.parsing.converter.DateTimeConverter;

public class LeftTrimExpressionTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    @Test()
    public void oneArgument()
    {
        String query = "select ltrim(' TEST ') as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("TEST ", result.get(0).get("result"));
    }

    @Test()
    public void twoArguments()
    {
        String query = "select ltrim('__TEST__', '_') as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("TEST__", result.get(0).get("result"));

        query = "select ltrim('abcTEST__', 'cba') as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("TEST__", result.get(0).get("result"));
    }

    @Test()
    public void threeArguments()
    {
        String query = "select ltrim('hello', '_', '') as result";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Function 'LTRIM' requires maximum 2 parameters", e.getMessage());
        }
    }

    @Test()
    public void noArguments()
    {
        String query = "select ltrim() as result";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Function 'LTRIM' requires at least 1 parameter", e.getMessage());
        }
    }

    @Test()
    public void nullArgument()
    {
        String query = "select ltrim(null) as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertNull(result.get(0).get("result"));
    }

    @Test()
    public void integerArgument()
    {
        String query = "select ltrim(1) as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("1", result.get(0).get("result"));
    }

    @Test()
    public void longArgument()
    {
        String query = "select ltrim(1L) as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("1", result.get(0).get("result"));
    }

    @Test()
    public void doubleArgument()
    {
        String query = "select ltrim(2/3) as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("0.666667", result.get(0).get("result"));
    }

    @Test()
    public void bigDecimalArgument()
    {
        String query = "select ltrim(" + BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE) + ") as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("9223372036854775808", result.get(0).get("result"));
    }

    @Test()
    public void dateArgument()
    {
        EvaluationContextHolder.getEvaluationContext().put(EvaluationContext.QUERY_SETTINGS, new QuerySettings());
        DateTimeFormatter formatter = DateTimeConverter.getFormatter();

        String query = "select ltrim(lastUpdatedTime) as result from queue where name='QUEUE_0'";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        formatter.parse((String)result.get(0).get("result"));
    }

    @Test()
    public void booleanArgument()
    {
        String query = "select ltrim(true) as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("true", result.get(0).get("result"));
    }

    @Test()
    public void invalidArgumentType()
    {
        String query = "select ltrim(statistics) from queue";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryEvaluationException.class, e.getClass());
            assertEquals("Parameter of function 'LTRIM' invalid (parameter type: HashMap)", e.getMessage());
        }
    }
}
