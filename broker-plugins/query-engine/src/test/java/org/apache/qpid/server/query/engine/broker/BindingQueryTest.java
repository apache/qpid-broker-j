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
package org.apache.qpid.server.query.engine.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.query.engine.TestBroker;
import org.apache.qpid.server.query.engine.evaluator.QueryEvaluator;

/**
 * Tests designed to verify the bindings retrieval
 */
public class BindingQueryTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    @Test()
    public void selectAllBindings()
    {
        String query = "select * from binding";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(10, result.size());
    }

    @Test()
    public void countBindings()
    {
        String query = "select count(*) from binding";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(10, result.get(0).get("count(*)"));
    }

    @Test()
    @SuppressWarnings("unchecked")
    public void groupByExchange()
    {
        String query = "select count(*), exchange from binding group by exchange";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        for (int i = 0; i < 10; i ++)
        {
            assertEquals(1, ((Map<String, Object>) result.get(0).get("count(*)")).get("EXCHANGE_" + i));
        }
    }

    @Test()
    @SuppressWarnings("unchecked")
    public void groupByDestination()
    {
        String query = "select count(*), destination from binding group by destination";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(10, ((Map<String, Object>) result.get(0).get("count(*)")).get("QUEUE_1"));
    }

    @Test()
    public void extractExchangeIdAndDestinationId()
    {
        String query = "select "
            + "exchange, destination, "
            + "(select id from exchange where name = b.exchange) as exchangeId, "
            + "(select id from queue where name = b.destination) as destinationId "
            + "from binding b";

        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();

        assertEquals(10, result.size());

        for (Map<String, Object> stringObjectMap : result)
        {
            String exchangeQuery = "select id from exchange where name = '" + stringObjectMap.get("exchange") + "'";
            String exchangeId = (String) _queryEvaluator.execute(exchangeQuery).getResults().get(0).get("id");
            assertEquals(stringObjectMap.get("exchangeId"), exchangeId);

            String queueQuery = "select id from queue where name = '" + stringObjectMap.get("destination") + "'";
            String queueId = (String) _queryEvaluator.execute(queueQuery).getResults().get(0).get("id");
            assertEquals(stringObjectMap.get("destinationId"), queueId);
        }
    }
}
