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
package org.apache.qpid.tests.rest;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.model.Broker;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;

public class BrokerRestTest extends BrokerAdminUsingTestBase
{
    private RestTestHelper _helper;

    @Before
    public void setUp()
    {
        _helper = new RestTestHelper(getBrokerAdmin());
    }

    @Test
    public void get() throws Exception
    {
        Map<String, Object> brokerDetails = _helper.getJsonAsMap("broker");
        assertThat("Unexpected value of attribute " + Broker.NAME,  brokerDetails.get(Broker.NAME), is(equalTo("Broker")));
    }
}
