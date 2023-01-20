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
package org.apache.qpid.server.query.engine.parsing.expression.arithmetic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.query.engine.TestBroker;
import org.apache.qpid.server.query.engine.evaluator.QueryEvaluator;

/**
 * Tests designed to verify the numeric type conversions functionality
 */
public class NumericTypeConversionsTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    @Test()
    public void conversionToNull()
    {
        String query = "select 1 + null as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertNull(result.get(0).get("result"));

        query = "select null + 1 as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertNull(result.get(0).get("result"));

        query = "select null + 'test' as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertNull(result.get(0).get("result"));

        query = "select null - 1 as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertNull(result.get(0).get("result"));
    }

    @Test()
    public void conversionToInteger()
    {
        String query = "select 1 + 1 as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(2, result.get(0).get("result"));

        query = "select 1 + 1L as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(2, result.get(0).get("result"));

        query = "select 1 + 1.0 as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(2, result.get(0).get("result"));

        query = "select 2147483648 - 1 as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(2147483647, result.get(0).get("result"));
    }

    @Test()
    public void conversionToLong()
    {
        String query = "select 2147483647 + 1 as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(2147483648L, result.get(0).get("result"));

        query = "select -2147483648 - 1 as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(-2147483649L, result.get(0).get("result"));
    }

    @Test()
    public void conversionToDouble()
    {
        String query = "select 0.5 + 0.6 as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(1.1, result.get(0).get("result"));

        query = "select " + -Double.MAX_VALUE + " + 1 as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(BigDecimal.valueOf(-Double.MAX_VALUE).add(BigDecimal.ONE), result.get(0).get("result"));

        query = "select " + Double.MAX_VALUE + " - 1 as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(BigDecimal.valueOf(Double.MAX_VALUE).subtract(BigDecimal.ONE), result.get(0).get("result"));
    }

    @Test()
    public void conversionToBigDecimal()
    {
        String query = "select " + Long.MAX_VALUE + " + 1 as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE), result.get(0).get("result"));

        query = "select " + Long.MIN_VALUE + " - 1 as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(BigDecimal.valueOf(Long.MIN_VALUE).subtract(BigDecimal.ONE), result.get(0).get("result"));

        query = "select " + Double.MAX_VALUE + " + 1 as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(BigDecimal.valueOf(Double.MAX_VALUE).add(BigDecimal.ONE), result.get(0).get("result"));

        query = "select " + -Double.MAX_VALUE + " - 1 as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(BigDecimal.valueOf(-Double.MAX_VALUE).subtract(BigDecimal.ONE), result.get(0).get("result"));
    }

    @Test()
    public void conversionToString()
    {
        String query = "select 1 + '' as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("1", result.get(0).get("result"));

        query = "select 'hello' + ' ' + 1 as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("hello 1", result.get(0).get("result"));
    }

}
