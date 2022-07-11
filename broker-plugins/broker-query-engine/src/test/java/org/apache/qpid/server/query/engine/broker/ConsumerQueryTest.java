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

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.qpid.server.query.engine.TestBroker;
import org.apache.qpid.server.query.engine.evaluator.QueryEvaluator;

/**
 * Tests designed to verify the consumers retrieval
 */
public class ConsumerQueryTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    @Test()
    public void selectAllConsumers()
    {
        String query = "select * from consumer";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(10, result.size());
    }

    @Test()
    public void countConsumers()
    {
        String query = "select count(*) from consumer";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(10, result.get(0).get("count(*)"));
    }

    @Test()
    public void extractConnectionNameAndSessionName()
    {
        String query = "select "
            + "id, name, "
            + "(select id from connection where substring(name, 1, position(']' in name)) = '[' + substring(c.name, 1, position('|' in c.name) - 1) + ']') as connectionId, "
            + "(select name from connection where substring(name, 1, position(']' in name)) = '[' + substring(c.name, 1, position('|' in c.name) - 1) + ']') as connection, "
            + "(select name from session where id = c.session.id) as session, "
            + "session.id as sessionId "
            + "from consumer c "
            + "order by name";

        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();

        assertEquals(10, result.size());

        for (Map<String, Object> stringObjectMap : result)
        {
            String connectionQuery = "select id from connection where name = '" + stringObjectMap.get("connection") + "'";
            String connectionId = (String) _queryEvaluator.execute(connectionQuery).getResults().get(0).get("id");
            assertEquals(stringObjectMap.get("connectionId"), connectionId);

            String sessionQuery = "select name as name from session where id = '" + stringObjectMap.get("sessionId") + "'";
            String sessionName = (String) _queryEvaluator.execute(sessionQuery).getResults().get(0).get("name");
            assertEquals(stringObjectMap.get("session"), sessionName);
        }
    }
}
