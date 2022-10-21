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
import static org.junit.Assert.fail;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.qpid.server.query.engine.TestBroker;
import org.apache.qpid.server.query.engine.evaluator.QueryEvaluator;
import org.apache.qpid.server.query.engine.exception.QueryEvaluationException;
import org.apache.qpid.server.query.engine.exception.QueryParsingException;

/**
 * Tests designed to verify the subqueries functionality
 */
public class SubqueryTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    @Test()
    public void nestedSubquery()
    {
        String query = "select id from exchange where name = 'EXCHANGE_0'";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        String exchangeId = (String) result.get(0).get("id");

        query = "select name from queue where name in (select destination from binding where exchange in (select name from exchange where id = '" + exchangeId + "'))";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("QUEUE_1", result.get(0).get("name"));
    }


    @Test()
    public void singeRowSubqueryReturnedMoreThanOneRow()
    {
        try
        {
            String query = "select * from queue where name = (select destination from binding)";
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch(Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Single-row subquery 'select destination from binding' returns more than one row: 10", e.getMessage());
        }
    }

    @Test()
    public void subqueryReturnsEmptySet()
    {
        String query = "select * from queue where name = (select destination from binding where exchange = 'not-existing')";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(0, result.size());

        query = "select * from queue where name > (select destination from binding where exchange = 'not-existing')";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(0, result.size());

        query = "select * from queue where name >= (select destination from binding where exchange = 'not-existing')";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(0, result.size());

        query = "select * from queue where name < (select destination from binding where exchange = 'not-existing')";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(0, result.size());

        query = "select * from queue where name <= (select destination from binding where exchange = 'not-existing')";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(0, result.size());

        query = "select * from queue where name in (select destination from binding where exchange = 'not-existing')";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(0, result.size());
    }

    @Test()
    public void subqueryReturnsMoreThanOneValue()
    {
        try
        {
            String query = "select * from queue where name = (select destination, exchange from binding where destination = 'QUEUE_1' and exchange = 'EXCHANGE_0')";
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch(Exception e)
        {
            assertEquals(QueryParsingException.class, e.getClass());
            assertEquals("Subquery 'select destination, exchange from binding where destination='QUEUE_1' and exchange='EXCHANGE_0'' returns more than one value: [destination, exchange]", e.getMessage());
        }
    }

    @Test()
    public void subqueryReturnsAggregation()
    {
        String query = "select * from queue where queueDepthMessages = (select max(queueDepthMessages) from queue)";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(10, result.size());
        for (int i = 0; i < result.size(); i ++)
        {
            assertEquals("QUEUE_" + (60 + i), result.get(i).get("name"));
        }
    }

    @Test()
    public void fieldNotFoundInSubquery()
    {
        try
        {
            String query = "SELECT * FROM certificate where UPPER(alias) in (select UPPER(NAME) FROM queue)";
            _queryEvaluator.execute(query);
            fail("Expected exception not thrown");
        }
        catch(Exception e)
        {
            assertEquals(QueryEvaluationException.class, e.getClass());
            assertEquals("Domains [certificate, queue] do not contain field 'NAME'", e.getMessage());
        }
    }
}
