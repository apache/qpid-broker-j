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
import static org.junit.Assert.fail;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.qpid.server.query.engine.TestBroker;
import org.apache.qpid.server.query.engine.evaluator.QueryEvaluator;
import org.apache.qpid.server.query.engine.exception.QueryEvaluationException;
import org.apache.qpid.server.query.engine.exception.QueryParsingException;

public class PositionExpressionTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    @Test()
    public void oneArgument()
    {
        try
        {
            String query = "select position('TEST') as result";
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Function 'POSITION' requires at least 2 parameters", e.getMessage());
        }
    }

    @Test()
    public void twoArguments()
    {
        String query = "select position(4, '123') as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(0, result.get(0).get("result"));

        query = "select position(3, '123') as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(3, result.get(0).get("result"));

        query = "select position('world', 'hello world') as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(7, result.get(0).get("result"));
    }

    @Test()
    public void twoArgumentsIn()
    {
        String query = "select position(4 in '123') as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(0, result.get(0).get("result"));

        query = "select position(3 in '123') as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(3, result.get(0).get("result"));

        query = "select position('world' in 'hello world') as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(7, result.get(0).get("result"));
    }

    @Test()
    public void threeArguments()
    {
        String query = "select position('.', 'X.X.X.X') as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(2, result.get(0).get("result"));

        query = "select position('X', 'X.X.X.X', 1) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(1, result.get(0).get("result"));

        query = "select position('.', 'X.X.X.X', 2) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(2, result.get(0).get("result"));

        query = "select position('.', 'X.X.X.X', 3) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(4, result.get(0).get("result"));

        query = "select position('.', 'X.X.X.X', 4) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(4, result.get(0).get("result"));

        query = "select position('.', 'X.X.X.X', 5) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(6, result.get(0).get("result"));
    }

    @Test()
    public void threeArgumentsIn()
    {
        String query = "select position('.' in 'X.X.X.X') as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(2, result.get(0).get("result"));

        query = "select position('.' in 'X.X.X.X', 2) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(2, result.get(0).get("result"));

        query = "select position('.' in 'X.X.X.X', 3) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(4, result.get(0).get("result"));

        query = "select position('.' in 'X.X.X.X', 4) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(4, result.get(0).get("result"));

        query = "select position('.' in 'X.X.X.X', 5) as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(6, result.get(0).get("result"));
    }

    @Test()
    public void extractValueBetweenTwoStrings()
    {
        String query = "select substring('request_be.XX_XXX.C7', position('.', 'request_be.XX_XXX.C7') + 1, len('request_be.XX_XXX.C7') - position('.', 'request_be.XX_XXX.C7') - (len('request_be.XX_XXX.C7') - position('.', 'request_be.XX_XXX.C7', position('.', 'request_be.XX_XXX.C7') + 1) + 1)) as account";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("XX_XXX", result.get(0).get("account"));
    }

    @Test()
    public void fourArguments()
    {
        String query = "select position('test', 1, 1, 1) as result";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Function 'POSITION' requires maximum 3 parameters", e.getMessage());
        }
    }

    @Test()
    public void noArguments()
    {
        String query = "select position() as result";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Function 'POSITION' requires at least 2 parameters", e.getMessage());
        }
    }

    @Test()
    public void firstArgumentInvalid()
    {
        String query = "select position('', 'test') as result";

        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryEvaluationException.class, e.getClass());
            assertEquals("Function 'POSITION' requires argument 1 to be a non-empty string", e.getMessage());
        }

        query = "select position(null, 'test') as result";

        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryEvaluationException.class, e.getClass());
            assertEquals("Parameter of function 'POSITION' invalid (parameter type: null)", e.getMessage());
        }
    }

    @Test()
    public void secondArgumentInvalid()
    {
        String query = "select position('x', statistics) as result from queue";

        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryEvaluationException.class, e.getClass());
            assertEquals("Parameter of function 'POSITION' invalid (parameter type: HashMap)", e.getMessage());
        }
    }

    @Test()
    public void thirdArgumentInvalid()
    {
        String query = "select position('x', 'test', 'x') as result from queue";

        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryEvaluationException.class, e.getClass());
            assertEquals("Function 'POSITION' requires argument 3 to be an integer greater than 0", e.getMessage());
        }

        query = "select position('x', 'test', -1) as result from queue";

        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryEvaluationException.class, e.getClass());
            assertEquals("Function 'POSITION' requires argument 3 to be an integer greater than 0", e.getMessage());
        }

        query = "select position('x', 'test', null) as result from queue";

        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryEvaluationException.class, e.getClass());
            assertEquals("Function 'POSITION' requires argument 3 to be an integer greater than 0", e.getMessage());
        }
    }
}
