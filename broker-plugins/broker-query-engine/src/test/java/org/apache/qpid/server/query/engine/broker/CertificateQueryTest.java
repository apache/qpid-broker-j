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

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.qpid.server.query.engine.TestBroker;
import org.apache.qpid.server.query.engine.evaluator.QueryEvaluator;
import org.apache.qpid.server.query.engine.evaluator.settings.QuerySettings;
import org.apache.qpid.server.query.engine.utils.QuerySettingsBuilder;

public class CertificateQueryTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    private final QuerySettings _querySettings = new QuerySettingsBuilder().zoneId(ZoneId.of("UTC")).build();

    @Test()
    public void selectAllCertificates()
    {
        String query = "select * from certificate";
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(10, result.size());

        assertEquals("peersTruststore", result.get(0).get("store"));
        assertEquals("aaa_mock", result.get(0).get("alias"));
        assertEquals("CN=aaa_mock", result.get(0).get("issuerName"));
        assertEquals("1", result.get(0).get("serialNumber"));
        assertEquals("0x1", result.get(0).get("hexSerialNumber"));
        assertEquals("SHA512withRSA", result.get(0).get("signatureAlgorithm"));
        assertEquals(new ArrayList<>(), result.get(0).get("subjectAltNames"));
        assertEquals("CN=aaa_mock", result.get(0).get("subjectName"));
        assertEquals("2020-01-01 00:00:00", result.get(0).get("validFrom"));
        assertEquals("2022-12-31 23:59:59", result.get(0).get("validUntil"));
        assertEquals(3, result.get(0).get("version"));
    }
}
