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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.query.engine.TestBroker;
import org.apache.qpid.server.query.engine.evaluator.QueryEvaluator;

/**
 * Tests designed to verify the public class {@link LikeExpression} functionality
 */
public class LikeExpressionTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    @Test()
    public void percentWildcard()
    {
        String query = "select 'test' like 't%' as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 'test' like 'te%' as result";
        _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 'test' like 'tes%' as result";
        _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 'test' like 'test%' as result";
        _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 'test' like '%t' as result";
        _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 'test' like '%st' as result";
        _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 'test' like '%est' as result";
        _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 'test' like '%test' as result";
        _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 'test' like '%test%' as result";
        _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 'test' like '%es%' as result";
        _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 'test' like '%e%' as result";
        _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 'test' like '%s%' as result";
        _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 'test' like '%' as result";
        _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));
    }

    @Test()
    public void notLikeUsingPercentWildcard()
    {
        String query = "select 'test' like 'T%' as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(false, result.get(0).get("result"));

        query = "select 'test' like 'et%' as result";
        _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(false, result.get(0).get("result"));

        query = "select 'test' like '%T' as result";
        _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(false, result.get(0).get("result"));

        query = "select '123test123' like '3%`T`' as result";
        _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(false, result.get(0).get("result"));
    }

    @Test()
    public void questionMarkWildcard()
    {
        String query = "select 'test' like 'tes?' as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 'test' like 'te?t' as result";
        _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 'test' like 't?st' as result";
        _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 'test' like '?est' as result";
        _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 'test' like '?es?' as result";
        _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 'test' like '????' as result";
        _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));
    }

    @Test()
    public void notLikeUsingQuestionMarkWildcard()
    {
        String query = "select 'test' like 'Tes?' as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(false, result.get(0).get("result"));

        query = "select 'tesT' like 'te?t' as result";
        _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(false, result.get(0).get("result"));

        query = "select 'test_123' like 'test_124' as result";
        _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(false, result.get(0).get("result"));
    }

    @Test()
    public void escape()
    {
        String query = "select 'test 100% test' like '% 100#% test' escape '#' as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 'test? really?' like 'test#? %' escape '#' as result";
        _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));
    }

    @Test()
    public void patternMatching()
    {
        String query = "select name from queue where lower(name) like 'queue_1?' ";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(10, result.size());

        query = "select name from queue where lower(name) like 'queue_2?' ";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(10, result.size());

        query = "select name from queue where lower(name) like 'queue_3?' ";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(10, result.size());

        query = "select name from queue where lower(name) like 'queue_%' ";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(70, result.size());

        query = "select count(*) from queue where lower(overflowPolicy) like 'r%' ";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(20, result.get(0).get("count(*)"));

        query = "select distinct overflowPolicy from queue where lower(overflowPolicy) like '%flow%' ";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(2, result.size());
        assertEquals("FLOW_TO_DISK", result.get(0).get("overflowPolicy"));
        assertEquals("PRODUCER_FLOW_CONTROL", result.get(1).get("overflowPolicy"));
    }

    @Test()
    public void optionalBrackets()
    {
        String query = "select 'test 100% test' like ('% 100#% test' escape '#') as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 'test 100% test' not like ('% 100#% test' escape '#') as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(false, result.get(0).get("result"));

        query = "select 'test? really?' like ('test#? %' escape '#') as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 'test? really?' not like ('test#? %' escape '#') as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(false, result.get(0).get("result"));
    }
}
