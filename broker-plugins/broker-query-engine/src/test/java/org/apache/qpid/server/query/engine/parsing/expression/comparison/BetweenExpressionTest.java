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
package org.apache.qpid.server.query.engine.parsing.expression.comparison;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.qpid.server.query.engine.TestBroker;
import org.apache.qpid.server.query.engine.exception.QueryEvaluationException;
import org.apache.qpid.server.query.engine.evaluator.QueryEvaluator;

/**
 * Tests designed to verify the {@link BetweenExpression} functionality
 */
public class BetweenExpressionTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    @Test()
    public void comparingIntegers()
    {
        String query = "select 1 between 0 and 2";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("1 between 0 and 2"));

        query = "select 1 between 1 and 2";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("1 between 1 and 2"));

        query = "select 1 between 2 and 3";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(false, result.get(0).get("1 between 2 and 3"));
    }

    @Test()
    public void comparingLongs()
    {
        String query = "select 2L between 2 and 5L as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 2L not between 3L and 5L as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));
    }

    @Test()
    public void comparingDoubles()
    {
        String query = "select 2/3 between 1/3 and 9/10 as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 1/3 between 1/3 and 9/10 as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 0.0 between -1/2 and +1/2 as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));
    }

    @Test()
    public void comparingBigDecimals()
    {
        String query = "select " + BigDecimal.valueOf(Long.MAX_VALUE).subtract(BigDecimal.ONE)
            + " between " + BigDecimal.valueOf(Long.MAX_VALUE).subtract(BigDecimal.TEN)
            + " and " + BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.TEN)
            + " as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select " + BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE)
            + " between " + BigDecimal.valueOf(Long.MAX_VALUE).subtract(BigDecimal.TEN)
            + " and " + BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.TEN)
            + " as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select " + BigDecimal.valueOf(Long.MIN_VALUE).add(BigDecimal.ONE)
            + " between " + BigDecimal.valueOf(Long.MIN_VALUE).subtract(BigDecimal.TEN)
            + " and " + BigDecimal.valueOf(Long.MIN_VALUE).add(BigDecimal.TEN)
            + " as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select " + BigDecimal.valueOf(Long.MIN_VALUE).subtract(BigDecimal.ONE)
            + " between " + BigDecimal.valueOf(Long.MIN_VALUE).subtract(BigDecimal.TEN)
            + " and " + BigDecimal.valueOf(Long.MIN_VALUE).add(BigDecimal.TEN)
            + " as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));
    }

    @Test()
    public void comparingStrings()
    {
        String query = "select 'aba' between 'aaa' and 'bbb' as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 'test123' between 'test122' and 'test124' as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));
    }

    @Test()
    public void comparingInvalidTypes()
    {
        String query = "select statistics between statistics and statistics as result from queue";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryEvaluationException.class, e.getClass());
            assertEquals("Objects of types 'HashMap' and 'HashMap' can not be compared", e.getMessage());
        }

        query = "select bindings between statistics and bindings as result from exchange";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryEvaluationException.class, e.getClass());
            assertEquals("Objects of types 'UnmodifiableRandomAccessList' and 'HashMap' can not be compared", e.getMessage());
        }
    }

    @Test()
    public void optionalBrackets()
    {
        String query = "select 1 between (0 and 2) as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 1 not between (0 and 2) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(false, result.get(0).get("result"));

        query = "select 1 between (0, 2) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 1 not between (0, 2) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(false, result.get(0).get("result"));

        query = "select 1 between(0, 2) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 1 not between(0, 2) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(false, result.get(0).get("result"));
    }
}
