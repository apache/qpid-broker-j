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
package org.apache.qpid.server.query.engine.parsing.expression.literal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.qpid.server.query.engine.TestBroker;
import org.apache.qpid.server.query.engine.evaluator.QueryEvaluator;

/**
 * Tests designed to verify the literal expressions functionality
 */
public class LiteralExpressionTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    @Test()
    public void booleanLiteral()
    {
        String query = "select true";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("true"));

        query = "select false";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(false, result.get(0).get("false"));
    }

    @Test()
    public void stringLiteral()
    {
        String query = "select ''";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("", result.get(0).get("''"));

        query = "select 'test'";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("test", result.get(0).get("'test'"));
    }

    @Test()
    public void nullLiteral()
    {
        String query = "select null";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertNull(result.get(0).get("null"));
    }

    @Test()
    public void integers()
    {
        String query = "select 0";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(0, result.get(0).get("0"));

        query = "select " + Integer.MAX_VALUE + " as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(Integer.MAX_VALUE, result.get(0).get("result"));

        query = "select " + Integer.MIN_VALUE + " as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(Integer.MIN_VALUE, result.get(0).get("result"));
    }

    @Test()
    public void longs()
    {
        String query = "select " + (Integer.MAX_VALUE + 1L) + " as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(2147483648L, result.get(0).get("result"));

        query = "select " + (Integer.MIN_VALUE - 1L) + " as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(-2147483649L, result.get(0).get("result"));

        query = "select " + Long.MAX_VALUE + " as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(Long.MAX_VALUE, result.get(0).get("result"));

        query = "select " + Long.MIN_VALUE + " as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(Long.MIN_VALUE, result.get(0).get("result"));
    }

    @Test()
    public void doubles()
    {
        String query = "select 0.1";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(0.1, result.get(0).get("0.1"));

        query = "select " + Double.MAX_VALUE + " as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(Double.MAX_VALUE, result.get(0).get("result"));

        query = "select " + (-Double.MAX_VALUE) + " as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(new BigDecimal(BigDecimal.valueOf(-Double.MAX_VALUE).toPlainString()), result.get(0).get("result"));
    }

    @Test()
    public void bigDecimals()
    {
        String query = "select " + BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE) + " as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE), result.get(0).get("result"));

        query = "select " + BigDecimal.valueOf(Long.MIN_VALUE).subtract(BigDecimal.ONE) + " as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(BigDecimal.valueOf(Long.MIN_VALUE).subtract(BigDecimal.ONE), result.get(0).get("result"));
    }
}
