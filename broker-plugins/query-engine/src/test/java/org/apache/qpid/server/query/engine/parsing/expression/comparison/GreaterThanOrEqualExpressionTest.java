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
 * Tests designed to verify the public class {@link GreaterThanOrEqualExpression} functionality
 */
public class GreaterThanOrEqualExpressionTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    @Test()
    public void comparingIntegers()
    {
        String query = "select 2 >= 1 as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 1 >= 2 as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(false, result.get(0).get("result"));

        query = "select 0 >= -1 as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select -1 >= -2 as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 2 >= -2 as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));
    }

    @Test()
    public void comparingLongs()
    {
        String query = "select 2L >= 1 as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 2L >= 2L as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 1L >= 2 as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(false, result.get(0).get("result"));

        query = "select 0 >= -1L as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select -1L >= -2 as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));
    }

    @Test()
    public void comparingDoubles()
    {
        String query = "select 2/3 >= 1/3 as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 2/3 >= 4/6 as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 1/4 >= 2/4 as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(false, result.get(0).get("result"));

        query = "select 1/2 >= 2/4 as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 0 >= -1/2 as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select -1/3 >= -2/3 as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));
    }

    @Test()
    public void comparingBigDecimals()
    {
        String query = "select " + BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.TEN) + " >= " + BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE) + " as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select " + BigDecimal.valueOf(Long.MIN_VALUE).subtract(BigDecimal.TEN) + " >= " + BigDecimal.valueOf(Long.MIN_VALUE).subtract(BigDecimal.ONE) + " as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(false, result.get(0).get("result"));
    }

    @Test()
    public void comparingBooleans()
    {
        String query = "select true >= false as result";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryEvaluationException.class, e.getClass());
            assertEquals("Objects of types 'Boolean' and 'Boolean' can not be compared", e.getMessage());
        }
    }

    @Test()
    public void comparingStrings()
    {
        String query = "select 'b' >= 'a' as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 'b' >= 'b' as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 'b' >= 'c' as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(false, result.get(0).get("result"));

        query = "select '1' >= '2' as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(false, result.get(0).get("result"));

        query = "select 'test124' >= 'test123' as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 'test124' >= 'test124' as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));
    }

    @Test()
    public void comparingInvalidTypes()
    {
        String query = "select statistics >= statistics as result from queue";
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

        query = "select bindings >= statistics as result from exchange";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryEvaluationException.class, e.getClass());
            assertEquals("Objects of types 'List12' and 'HashMap' can not be compared", e.getMessage());
        }
    }

    @Test()
    public void comparingNulls()
    {
        String query = "select 1 >= null as result from queue";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryEvaluationException.class, e.getClass());
            assertEquals("Objects of types 'Integer' and 'null' can not be compared", e.getMessage());
        }

        query = "select null >= 'test' as result from queue";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryEvaluationException.class, e.getClass());
            assertEquals("Objects of types 'null' and 'String' can not be compared", e.getMessage());
        }
    }
}
