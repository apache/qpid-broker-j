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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.math.BigDecimal;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.query.engine.TestBroker;
import org.apache.qpid.server.query.engine.evaluator.EvaluationContext;
import org.apache.qpid.server.query.engine.evaluator.EvaluationContextHolder;
import org.apache.qpid.server.query.engine.evaluator.settings.QuerySettings;
import org.apache.qpid.server.query.engine.exception.QueryEvaluationException;
import org.apache.qpid.server.query.engine.exception.QueryParsingException;
import org.apache.qpid.server.query.engine.evaluator.QueryEvaluator;
import org.apache.qpid.server.query.engine.parsing.converter.DateTimeConverter;

/**
 * Tests designed to verify the public class {@link LowerExpression} functionality
 */
public class LowerExpressionTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    @Test()
    public void oneArgument()
    {
        String query = "select lower('TEST') as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("test", result.get(0).get("result"));
    }

    @Test()
    public void twoArguments()
    {
        String query = "select lower('hello', 'world') as result";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Function 'LOWER' requires 1 parameter", e.getMessage());
        }
    }

    @Test()
    public void noArguments()
    {
        String query = "select lower() as result";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Function 'LOWER' requires 1 parameter", e.getMessage());
        }
    }

    @Test()
    public void nullArgument()
    {
        String query = "select lower(null) as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertNull(result.get(0).get("result"));
    }

    @Test()
    public void integerArgument()
    {
        String query = "select lower(1) as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("1", result.get(0).get("result"));
    }

    @Test()
    public void longArgument()
    {
        String query = "select lower(1L) as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("1", result.get(0).get("result"));
    }

    @Test()
    public void doubleArgument()
    {
        String query = "select lower(2/3) as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("0.666667", result.get(0).get("result"));
    }

    @Test()
    public void bigDecimalArgument()
    {
        String query = "select lower(" + BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE) + ") as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("9223372036854775808", result.get(0).get("result"));
    }

    @Test()
    public void dateArgument()
    {
        EvaluationContextHolder.getEvaluationContext().put(EvaluationContext.QUERY_SETTINGS, new QuerySettings());
        DateTimeFormatter formatter = DateTimeConverter.getFormatter();

        String query = "select lower(lastUpdatedTime) as result from queue where name='QUEUE_0'";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        formatter.parse((String)result.get(0).get("result"));
    }

    @Test()
    public void booleanArgument()
    {
        String query = "select lower(true) as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("true", result.get(0).get("result"));
    }

    @Test()
    public void invalidArgumentType()
    {
        String query = "select lower(statistics) from queue";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryEvaluationException.class, e.getClass());
            assertEquals("Parameter of function 'LOWER' invalid (parameter type: HashMap)", e.getMessage());
        }
    }
}
