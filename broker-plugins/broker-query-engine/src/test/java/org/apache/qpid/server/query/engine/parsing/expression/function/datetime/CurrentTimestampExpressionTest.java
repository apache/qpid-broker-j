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
package org.apache.qpid.server.query.engine.parsing.expression.function.datetime;

import static org.junit.Assert.assertEquals;

import java.time.ZoneId;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.qpid.server.query.engine.TestBroker;
import org.apache.qpid.server.query.engine.evaluator.EvaluationContext;
import org.apache.qpid.server.query.engine.evaluator.EvaluationContextHolder;
import org.apache.qpid.server.query.engine.evaluator.QueryEvaluator;
import org.apache.qpid.server.query.engine.evaluator.settings.QuerySettings;
import org.apache.qpid.server.query.engine.parsing.converter.DateTimeConverter;
import org.apache.qpid.server.query.engine.utils.QuerySettingsBuilder;

/**
 * Tests designed to verify the public class {@link CurrentTimestampExpression} functionality
 */
public class CurrentTimestampExpressionTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    @Test()
    public void noArguments()
    {
        String query = "select current_timestamp() as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        EvaluationContextHolder.getEvaluationContext().put(EvaluationContext.QUERY_SETTINGS, new QuerySettings());
        DateTimeConverter.getFormatter().parse((String)result.get(0).get("result"));
    }

    @Test()
    public void comparingDatetimes()
    {
        QuerySettings querySettings = new QuerySettingsBuilder().zoneId(ZoneId.of("UTC")).build();

        String query = "select name from virtualhostnode where createdTime < '2001-01-01 12:55:31.000'";
        List<Map<String, Object>> result = _queryEvaluator.execute(query, querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("mock", result.get(0).get("name"));

        query = "select name from virtualhostnode where createdTime = '2001-01-01 12:55:30.000'";
        result = _queryEvaluator.execute(query, querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("mock", result.get(0).get("name"));

        query = "select name from virtualhostnode where createdTime < '2001-01-01 12:55:29.000'";
        result = _queryEvaluator.execute(query, querySettings).getResults();
        assertEquals(0, result.size());

        query = "select name from virtualhostnode where createdTime between '2001-01-01 12:55:29' and '2001-01-01 12:55:31'";
        result = _queryEvaluator.execute(query, querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("mock", result.get(0).get("name"));
    }
}
