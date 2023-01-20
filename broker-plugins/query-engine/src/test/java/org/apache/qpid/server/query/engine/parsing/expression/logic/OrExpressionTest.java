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
package org.apache.qpid.server.query.engine.parsing.expression.logic;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.query.engine.TestBroker;
import org.apache.qpid.server.query.engine.evaluator.QueryEvaluator;

/**
 * Tests designed to verify the {@link OrExpression} functionality
 */
public class OrExpressionTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    @Test()
    public void or()
    {
        String query = "select true or true";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("true or true"));

        query = "select true or false";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("true or false"));

        query = "select false or true";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("false or true"));

        query = "select false or false";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(false, result.get(0).get("false or false"));

        query = "select (true or true) or false";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("(true or true) or false"));

        query = "select (true or true) or (false or true)";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("(true or true) or (false or true)"));

        query = "select 2 > 1 or 2 < 3";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("2>1 or 2<3"));

        query = "select (2 >= 1 or 2 <= 3) as expr";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("expr"));
    }
}
