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

public class BrokerDigestQueryTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    @Test()
    public void selectBrokerDigest()
    {
        String query = "select "
                       + "name as label, "
                       + "null as objectsState, "
                       + "'UNAVAILABLE' as diskUsageState, "
                       + "'UNAVAILABLE' as memoryUsageState, "
                       + "'UNAVAILABLE' as cpuUsageState, "
                       + "(select count(*) from connection) as connectionCount, "
                       + "(select count(*) from exchange) as exchangesCount, "
                       + "(select count(*) from binding) as bindingsCount, "
                       + "(select count(*) from queue) as queuesCount, "
                       + "0 as goodQueuesCount, "
                       + "0 as badQueuesCount, "
                       + "0 as criticalQueuesCount "
                       + "from broker";

        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
    }
}
