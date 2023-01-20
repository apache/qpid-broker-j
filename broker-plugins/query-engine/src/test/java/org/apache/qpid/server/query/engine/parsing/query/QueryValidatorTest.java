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
package org.apache.qpid.server.query.engine.parsing.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.query.engine.TestBroker;
import org.apache.qpid.server.query.engine.evaluator.QueryEvaluator;
import org.apache.qpid.server.query.engine.exception.QueryParsingException;
import org.apache.qpid.server.query.engine.exception.QueryValidationException;

/**
 * Tests designed to verify the query validation functionality
 */
public class QueryValidatorTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    @Test()
    public void selectWithoutFields()
    {
        String query = "select";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Missing expression", e.getMessage());
        }
    }

    @Test()
    public void selectUnknownProperty()
    {
        String query = "select unknownProperty";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryValidationException.class, e.getClass());
            assertEquals("Keyword 'FROM' not found where expected", e.getMessage());
        }

        query = "select 1+1, current_timestamp(), unknownProperty";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryValidationException.class, e.getClass());
            assertEquals("Keyword 'FROM' not found where expected", e.getMessage());
        }
    }

    @Test()
    public void emptyFrom()
    {
        String query = "select 1 from ";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Missing domain name", e.getMessage());
        }
    }

    @Test()
    public void multipleDomains()
    {
        String query = "select 1 from queue,exchange";
        try
        {
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryValidationException.class, e.getClass());
            assertEquals("Querying from multiple domains not supported", e.getMessage());
        }
    }

    @Test()
    public void join()
    {
        try
        {
            String query = "select 1 from queue q join exchange";
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryValidationException.class, e.getClass());
            assertEquals("Joins are not supported", e.getMessage());
        }

        try
        {
            String query = "select 1 from queue q join exchange e on (q.name = e.name)";
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryValidationException.class, e.getClass());
            assertEquals("Joins are not supported", e.getMessage());
        }
    }

    @Test()
    public void missingGroupByItems()
    {
        try
        {
            String query = "select count(*), overflowPolicy from queue";
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryValidationException.class, e.getClass());
            assertEquals("Not a single-group group function: projections [overflowPolicy] should be included in GROUP BY clause", e.getMessage());
        }

        try
        {
            String query = "select count(*), overflowPolicy, expiryPolicy from queue";
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryValidationException.class, e.getClass());
            assertEquals("Not a single-group group function: projections [overflowPolicy, expiryPolicy] should be included in GROUP BY clause", e.getMessage());
        }

        try
        {
            String query = "select count(*), overflowPolicy, expiryPolicy from queue group by overflowPolicy";
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryValidationException.class, e.getClass());
            assertEquals("Not a single-group group function: projections [expiryPolicy] should be included in GROUP BY clause", e.getMessage());
        }
    }
}
