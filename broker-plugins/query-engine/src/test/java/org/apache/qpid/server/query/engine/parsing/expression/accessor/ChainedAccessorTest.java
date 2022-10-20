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
package org.apache.qpid.server.query.engine.parsing.expression.accessor;


import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.qpid.server.query.engine.TestBroker;
import org.apache.qpid.server.query.engine.evaluator.QueryEvaluator;

/**
 * Tests designed to verify the {@link ChainedObjectAccessor} functionality
 */
public class ChainedAccessorTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    @Test()
    public void getStatisticsField()
    {
        String query = "select statistics.availableMessages from queue where name = 'QUEUE_0'";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(0, result.get(0).get("statistics.availableMessages"));

        query = "select statistics['availableMessages'] from queue where name = 'QUEUE_0'";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(0, result.get(0).get("statistics['availableMessages']"));
    }

    @Test()
    public void getByDomainAlias()
    {
        String query = "select q.statistics.availableMessages from queue as q where q.name = 'QUEUE_0'";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(0, result.get(0).get("q.statistics.availableMessages"));

        query = "select q.statistics['availableMessages'] from queue as q where q.name = 'QUEUE_0'";
        result =_queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(0, result.get(0).get("q.statistics['availableMessages']"));
    }

}
