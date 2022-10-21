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

/**
 * Tests designed to verify the aliases functionality
 */
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
        assertEquals(10, result.size());
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
    public void date()
    {
        String query = "select date(validUntil) from certificate";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(10, result.size());
        assertEquals("date(validUntil)", result.get(0).keySet().iterator().next());
    }

    @Test()
    public void dateadd()
    {
        String query = "select dateadd(day, -30, validUntil) from certificate";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(10, result.size());
        assertEquals("dateadd(day, -30, validUntil)", result.get(0).keySet().iterator().next());
    }

    @Test()
    public void datediff()
    {
        String query = "select datediff(day, current_timestamp(), validUntil) from certificate";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(10, result.size());
        assertEquals("datediff(day, current_timestamp(), validUntil)", result.get(0).keySet().iterator().next());
    }

    @Test()
    public void extract()
    {
        String query = "select extract(year from validUntil) from certificate";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(10, result.size());
        assertEquals("extract(year from validUntil)", result.get(0).keySet().iterator().next());

        query = "select extract(month from validUntil) from certificate";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(10, result.size());
        assertEquals("extract(month from validUntil)", result.get(0).keySet().iterator().next());

        query = "select extract(week from validUntil) from certificate";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(10, result.size());
        assertEquals("extract(week from validUntil)", result.get(0).keySet().iterator().next());

        query = "select extract(day from validUntil) from certificate";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(10, result.size());
        assertEquals("extract(day from validUntil)", result.get(0).keySet().iterator().next());

        query = "select extract(hour from validUntil) from certificate";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(10, result.size());
        assertEquals("extract(hour from validUntil)", result.get(0).keySet().iterator().next());

        query = "select extract(minute from validUntil) from certificate";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(10, result.size());
        assertEquals("extract(minute from validUntil)", result.get(0).keySet().iterator().next());

        query = "select extract(second from validUntil) from certificate";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(10, result.size());
        assertEquals("extract(second from validUntil)", result.get(0).keySet().iterator().next());

        query = "select extract(millisecond from validUntil) from certificate";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(10, result.size());
        assertEquals("extract(millisecond from validUntil)", result.get(0).keySet().iterator().next());
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
    public void len()
    {
        String query = "select len(name) from queue";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals("len(name)", result.get(0).keySet().iterator().next());

        query = "select length(name) from queue";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals("length(name)", result.get(0).keySet().iterator().next());
    }

    @Test()
    public void lower()
    {
        String query = "select lower(name) from queue";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals("lower(name)", result.get(0).keySet().iterator().next());
    }

    @Test()
    public void ltrim()
    {
        String query = "select ltrim(name) from queue";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals("ltrim(name)", result.get(0).keySet().iterator().next());
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
    public void position()
    {
        String query = "select position('_' in name) from queue";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals("position('_' in name)", result.get(0).keySet().iterator().next());

        query = "select position('_' in name, 1) from queue";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals("position('_' in name,1)", result.get(0).keySet().iterator().next());

        query = "select position('_', name) from queue";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals("position('_',name)", result.get(0).keySet().iterator().next());

        query = "select position('_', name, 1) from queue";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals("position('_',name,1)", result.get(0).keySet().iterator().next());
    }

    @Test()
    public void replace()
    {
        String query = "select replace(name, '_', '') from queue";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals("replace(name, '_', '')", result.get(0).keySet().iterator().next());
    }

    @Test()
    public void rtrim()
    {
        String query = "select rtrim(name) from queue";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals("rtrim(name)", result.get(0).keySet().iterator().next());
    }

    @Test()
    public void subquery()
    {
        String query = "select (select count(*) from queue where queueDepthMessages > 0.6 * maximumQueueDepthMessages) from broker";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals("select count(*) from queue where queueDepthMessages>0.6*maximumQueueDepthMessages", result.get(0).keySet().iterator().next());
    }

    @Test()
    public void substring()
    {
        String query = "select substring(name, 2) from queue";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals("substring(name, 2)", result.get(0).keySet().iterator().next());

        query = "select substring(name, 2, 5) from queue";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals("substring(name, 2, 5)", result.get(0).keySet().iterator().next());
    }

    @Test()
    public void sum()
    {
        String query = "select sum(queueDepthMessages) from queue";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals("sum(queueDepthMessages)", result.get(0).keySet().iterator().next());
    }

    @Test()
    public void trim()
    {
        String query = "select trim(name) from queue";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals("trim(name)", result.get(0).keySet().iterator().next());
    }

    @Test()
    public void upper()
    {
        String query = "select upper(name) from queue";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals("upper(name)", result.get(0).keySet().iterator().next());
    }
}
