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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.query.engine.TestBroker;
import org.apache.qpid.server.query.engine.evaluator.QueryEvaluator;
import org.apache.qpid.server.query.engine.evaluator.settings.QuerySettings;

/**
 * Tests designed to verify the public class {@link DateExpression} functionality
 */
public class DateExpressionTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    private final QuerySettings _querySettings = new QuerySettings();

    @Test()
    public void extractDateFromString()
    {
        String query = "select date('2000-01-01 00:00:00') as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-01-01", result.get(0).get("result"));
    }

    @Test()
    public void extractDateFromField()
    {
        String date = LocalDateTime.now().format(DateTimeFormatter.ofPattern("uuuu-MM-dd"));
        String query = "select date(lastUpdatedTime) as result from queue where name = 'QUEUE_0'";
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(date, result.get(0).get("result"));
    }

    @Test()
    public void comparingDatesUsingEquals()
    {
        String query = "SELECT * FROM certificate WHERE DATE(validFrom) = '2020-01-01'";
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("aaa_mock", result.get(0).get("alias"));
    }

    @Test()
    public void comparingDatesUsingNotEquals()
    {
        String query = "SELECT * FROM certificate WHERE DATE(validFrom) <> ('2020-01-01')";
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(9, result.size());
        assertEquals("bbb_mock", result.get(0).get("alias"));
        assertEquals("ccc_mock", result.get(1).get("alias"));
        assertEquals("ddd_mock", result.get(2).get("alias"));
        assertEquals("eee_mock", result.get(3).get("alias"));
        assertEquals("fff_mock", result.get(4).get("alias"));
        assertEquals("ggg_mock", result.get(5).get("alias"));
        assertEquals("hhh_mock", result.get(6).get("alias"));
        assertEquals("iii_mock", result.get(7).get("alias"));
        assertEquals("jjj_mock", result.get(8).get("alias"));
    }

    @Test()
    public void comparingDatesUsingGreaterThan()
    {
        String query = "SELECT * FROM certificate WHERE DATE(validFrom) > '2020-01-09'";
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("jjj_mock", result.get(0).get("alias"));
    }

    @Test()
    public void comparingDatesUsingGreaterThanOrEqual()
    {
        String query = "SELECT * FROM certificate WHERE DATE(validFrom) >= '2020-01-09'";
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(2, result.size());
        assertEquals("iii_mock", result.get(0).get("alias"));
        assertEquals("jjj_mock", result.get(1).get("alias"));
    }

    @Test()
    public void comparingDatesUsingLessThan()
    {
        String query = "SELECT * FROM certificate WHERE DATE(validFrom) < '2020-01-02'";
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("aaa_mock", result.get(0).get("alias"));
    }

    @Test()
    public void comparingDatesUsingLessThanOrEqual()
    {
        String query = "SELECT * FROM certificate WHERE DATE(validFrom) <= '2020-01-02'";
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(2, result.size());
        assertEquals("aaa_mock", result.get(0).get("alias"));
        assertEquals("bbb_mock", result.get(1).get("alias"));
    }

    @Test()
    public void comparingDatesUsingBetween()
    {
        String query = "SELECT * FROM certificate WHERE DATE(validFrom) BETWEEN ('2020-01-01', '2020-01-03')";
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(3, result.size());
        assertEquals("aaa_mock", result.get(0).get("alias"));
        assertEquals("bbb_mock", result.get(1).get("alias"));
        assertEquals("ccc_mock", result.get(2).get("alias"));
    }

    @Test()
    public void comparingDatesUsingNotBetween()
    {
        String query = "SELECT * FROM certificate WHERE DATE(validFrom) NOT BETWEEN ('2020-01-02', '2020-01-09')";
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(2, result.size());
        assertEquals("aaa_mock", result.get(0).get("alias"));
        assertEquals("jjj_mock", result.get(1).get("alias"));
    }

    @Test()
    public void comparingDatesUsingIn()
    {
        String query = "SELECT * FROM certificate WHERE DATE(validFrom) IN ('2020-01-01', '2020-01-03', '2020-01-05', '2020-01-07', '2020-01-09')";
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(5, result.size());
        assertEquals("aaa_mock", result.get(0).get("alias"));
        assertEquals("ccc_mock", result.get(1).get("alias"));
        assertEquals("eee_mock", result.get(2).get("alias"));
        assertEquals("ggg_mock", result.get(3).get("alias"));
        assertEquals("iii_mock", result.get(4).get("alias"));
    }

    @Test()
    public void comparingDatesUsingNotIn()
    {
        String query = "SELECT * FROM certificate WHERE DATE(validFrom) NOT IN ('2020-01-01', '2020-01-03', '2020-01-05', '2020-01-07', '2020-01-09')";
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(5, result.size());
        assertEquals("bbb_mock", result.get(0).get("alias"));
        assertEquals("ddd_mock", result.get(1).get("alias"));
        assertEquals("fff_mock", result.get(2).get("alias"));
        assertEquals("hhh_mock", result.get(3).get("alias"));
        assertEquals("jjj_mock", result.get(4).get("alias"));
    }
}
