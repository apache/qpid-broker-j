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

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.qpid.server.query.engine.TestBroker;
import org.apache.qpid.server.query.engine.evaluator.QueryEvaluator;

public class AliasTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    @Test()
    public void fields()
    {
        String query = "select id, name, description from queue";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(70, result.size());
        assertEquals("id", result.get(0).keySet().iterator().next());
        assertEquals("name", result.get(0).keySet().stream().skip(1).findFirst().orElse(null));
        assertEquals("description", result.get(0).keySet().stream().skip(2).findFirst().orElse(null));

        query = "select id as ID, name as NAME, description as DSC from queue";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(70, result.size());
        assertEquals("ID", result.get(0).keySet().iterator().next());
        assertEquals("NAME", result.get(0).keySet().stream().skip(1).findFirst().orElse(null));
        assertEquals("DSC", result.get(0).keySet().stream().skip(2).findFirst().orElse(null));

        query = "select id as \"ID\", name as \"NAME\", description as \"DSC\" from queue";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(70, result.size());
        assertEquals("ID", result.get(0).keySet().iterator().next());
        assertEquals("NAME", result.get(0).keySet().stream().skip(1).findFirst().orElse(null));
        assertEquals("DSC", result.get(0).keySet().stream().skip(2).findFirst().orElse(null));

        query = "select id ID, name NAME, description DSC from queue";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(70, result.size());
        assertEquals("ID", result.get(0).keySet().iterator().next());
        assertEquals("NAME", result.get(0).keySet().stream().skip(1).findFirst().orElse(null));
        assertEquals("DSC", result.get(0).keySet().stream().skip(2).findFirst().orElse(null));

        query = "select id \"ID\", name \"NAME\", description \"DSC\" from queue";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(70, result.size());
        assertEquals("ID", result.get(0).keySet().iterator().next());
        assertEquals("NAME", result.get(0).keySet().stream().skip(1).findFirst().orElse(null));
        assertEquals("DSC", result.get(0).keySet().stream().skip(2).findFirst().orElse(null));
    }

    @Test()
    public void abs()
    {
        String query = "select abs(queueDepthMessages) from queue";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals("abs(queueDepthMessages)", result.get(0).keySet().iterator().next());

        query = "select abs(1/3 - 12)";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals("abs(1/3-12)", result.get(0).keySet().iterator().next());
    }

    @Test()
    public void and()
    {
        String query = "select true and false";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals("true and false", result.get(0).keySet().iterator().next());

        query = "select 1 > 0 and 2 < 5";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals("1>0 and 2<5", result.get(0).keySet().iterator().next());
    }

    @Test()
    public void avg()
    {
        String query = "select avg(queueDepthMessages) from queue";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals("avg(queueDepthMessages)", result.get(0).keySet().iterator().next());

        query = "select avg(queueDepthMessages) + 1 from queue";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals("avg(queueDepthMessages)+1", result.get(0).keySet().iterator().next());

        query = "select 1 + avg(queueDepthMessages) + 1 from queue";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals("1+avg(queueDepthMessages)+1", result.get(0).keySet().iterator().next());
    }

    @Test()
    public void between()
    {
        String query = "select 'aba' between 'aaa' and 'bbb'";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("'aba' between 'aaa' and 'bbb'", result.get(0).keySet().iterator().next());

        query = "select 'aba' BETWEEN 'aaa' AND 'bbb'";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("'aba' BETWEEN 'aaa' AND 'bbb'", result.get(0).keySet().iterator().next());

        query = "select 1 between -111 and +111";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("1 between -111 and +111", result.get(0).keySet().iterator().next());
    }

    @Test()
    @SuppressWarnings("unchecked")
    public void caseExpression()
    {
        String query = "select case when 1 > 2 then 1 else 2 end";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals("case when 1>2 then 1 else 2 end", result.get(0).keySet().iterator().next());

        query = "select (case when 1 > 2 then 1 else 2 end)";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals("(case when 1>2 then 1 else 2 end)", result.get(0).keySet().iterator().next());

        query = "select "
            + "case "
            + "when (maximumQueueDepthMessages = -1 and maximumQueueDepthBytes = -1) or queueDepthMessages < maximumQueueDepthMessages * 0.6 then 'good' "
            + "when queueDepthMessages < maximumQueueDepthMessages * 0.9 then 'bad' "
            + "else 'critical' "
            + "end "
            + "from queue";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals("case "
            + "when (maximumQueueDepthMessages=-1 and maximumQueueDepthBytes=-1) or queueDepthMessages<maximumQueueDepthMessages*0.6 then 'good' "
            + "when queueDepthMessages<maximumQueueDepthMessages*0.9 then 'bad' "
            + "else 'critical' "
            + "end", result.get(0).keySet().iterator().next());

        query = "select "
                + "case "
                + "when (maximumQueueDepthMessages = -1 and maximumQueueDepthBytes = -1) or queueDepthMessages < maximumQueueDepthMessages * 0.6 then 'good' "
                + "when queueDepthMessages < maximumQueueDepthMessages * 0.9 then 'bad' "
                + "else 'critical' "
                + "end, "
                + "count(*) "
                + "from queue group by 1";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals("bad", ((Map<String,Object>)result.get(0).get("count(*)")).keySet().stream().skip(0).findFirst().orElse(null));
        assertEquals("critical", ((Map<String,Object>)result.get(0).get("count(*)")).keySet().stream().skip(1).findFirst().orElse(null));
        assertEquals("good", ((Map<String,Object>)result.get(0).get("count(*)")).keySet().stream().skip(2).findFirst().orElse(null));
    }

    @Test()
    public void coalesce()
    {
        String query = "select coalesce(null, 1)";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("coalesce(null, 1)", result.get(0).keySet().iterator().next());

        query = "select coalesce(null, null + 'test', 3)";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("coalesce(null, null+'test', 3)", result.get(0).keySet().iterator().next());

        query = "select count(coalesce(description, 'empty')) from queue having coalesce(description, 'empty') <> 'empty'";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("count(coalesce(description, 'empty'))", result.get(0).keySet().iterator().next());
    }

    @Test()
    public void concat()
    {
        String query = "select concat('hello', ' ',  'world')";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("concat('hello', ' ', 'world')", result.get(0).keySet().iterator().next());
    }

    @Test()
    public void configuredObjectAccessor()
    {
        String query = "select e.name from exchange e";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(14, result.size());
        assertEquals("e.name", result.get(0).keySet().iterator().next());
    }

    @Test()
    public void count()
    {
        String query = "select count(*) from queue";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals("count(*)", result.get(0).keySet().iterator().next());

        query = "select count(distinct overflowPolicy) from queue";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals("count(distinct overflowPolicy)", result.get(0).keySet().iterator().next());
    }

    @Test()
    public void currentTimestamp()
    {
        String query = "select current_timestamp()";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("current_timestamp()", result.get(0).keySet().iterator().next());
    }

    @Test()
    public void in()
    {
        String query = "select 1 in ('1', 'test', 1.0)";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals("1 in ('1','test',1)", result.get(0).keySet().iterator().next());

        query = "select 1 not in ('1', 'test', 1.0)";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals("1 not in ('1','test',1)", result.get(0).keySet().iterator().next());
    }

    @Test()
    public void max()
    {
        String query = "select max(queueDepthMessages) from queue";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals("max(queueDepthMessages)", result.get(0).keySet().iterator().next());
    }

    @Test()
    public void min()
    {
        String query = "select min(queueDepthMessages) from queue";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals("min(queueDepthMessages)", result.get(0).keySet().iterator().next());
    }

    @Test()
    public void subquery()
    {
        String query = "select (select count(*) from queue where queueDepthMessages > 0.6 * maximumQueueDepthMessages) from broker";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals("select count(*) from queue where queueDepthMessages>0.6*maximumQueueDepthMessages", result.get(0).keySet().iterator().next());
    }

    @Test()
    public void sum()
    {
        String query = "select sum(queueDepthMessages) from queue";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals("sum(queueDepthMessages)", result.get(0).keySet().iterator().next());
    }
}
