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
package org.apache.qpid.server.query.engine.evaluator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.query.engine.TestBroker;
import org.apache.qpid.server.query.engine.evaluator.settings.QuerySettings;
import org.apache.qpid.server.query.engine.exception.Errors;
import org.apache.qpid.server.query.engine.exception.QueryValidationException;

import java.util.List;
import java.util.Map;

/**
 * Tests designed to verify the {@link QueryEvaluator} functionality
 */
public class QueryEvaluatorTest
{
    @Test()
    public void createWithNullBroker()
    {
        try
        {
            new QueryEvaluator(null);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.EVALUATION.BROKER_NOT_SUPPLIED, e.getMessage());
        }
    }

    @Test()
    public void createWithNullQuerySettings()
    {
        try
        {
            new QueryEvaluator(null, null, null);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.EVALUATION.DEFAULT_QUERY_SETTINGS_NOT_SUPPLIED, e.getMessage());
        }
    }

    @Test()
    public void executeNullSql()
    {
        try
        {
            QueryEvaluator evaluator = new QueryEvaluator(null, new QuerySettings(), TestBroker.createBroker());
            evaluator.execute(null);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.EVALUATION.QUERY_NOT_SUPPLIED, e.getMessage());
        }

        try
        {
            QueryEvaluator evaluator = new QueryEvaluator(null, new QuerySettings(), TestBroker.createBroker());
            evaluator.execute(null, new QuerySettings());
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.EVALUATION.QUERY_NOT_SUPPLIED, e.getMessage());
        }
    }

    @Test()
    public void executeEmptySql()
    {
        try
        {
            QueryEvaluator evaluator = new QueryEvaluator(null, new QuerySettings(), TestBroker.createBroker());
            evaluator.execute("");
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryValidationException.class, e.getClass());
            assertEquals(Errors.VALIDATION.QUERY_EMPTY, e.getMessage());
        }

        try
        {
            QueryEvaluator evaluator = new QueryEvaluator(null, new QuerySettings(), TestBroker.createBroker());
            evaluator.execute("", new QuerySettings());
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(QueryValidationException.class, e.getClass());
            assertEquals(Errors.VALIDATION.QUERY_EMPTY, e.getMessage());
        }
    }

    @Test()
    public void executeWithNullQuerySettings()
    {
        try
        {
            QueryEvaluator evaluator = new QueryEvaluator(null, new QuerySettings(), TestBroker.createBroker());
            evaluator.execute("select 1 + 1", null);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.EVALUATION.QUERY_SETTINGS_NOT_SUPPLIED, e.getMessage());
        }
    }

    @Test()
    public void evaluateWithNullQuery()
    {
        try
        {
            QueryEvaluator evaluator = new QueryEvaluator(null, new QuerySettings(), TestBroker.createBroker());
            evaluator.evaluate(null);
            fail("Expected exception not thrown");
        }
        catch (Exception e)
        {
            assertEquals(NullPointerException.class, e.getClass());
            assertEquals(Errors.EVALUATION.QUERY_NOT_SUPPLIED, e.getMessage());
        }
    }

    @Test
    public void multiLineQuery()
    {
        final QueryEvaluator evaluator = new QueryEvaluator(null, new QuerySettings(), TestBroker.createBroker());
        final List<String> delimiters = List.of("\n", "\r", "\r\n");
        for (final String delimiter : delimiters)
        {
            final String query = "select * " + delimiter +
                    "from queue " + delimiter +
                    "where name = 'QUEUE_1'";
            final List<Map<String, Object>> result = evaluator.execute(query).getResults();
            assertEquals(1, result.size());
        }
    }
}
