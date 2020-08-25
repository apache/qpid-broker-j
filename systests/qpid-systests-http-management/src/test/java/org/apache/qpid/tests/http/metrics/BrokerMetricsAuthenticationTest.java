/*
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

package org.apache.qpid.tests.http.metrics;

import javax.servlet.http.HttpServletResponse;

import org.junit.Test;

import org.apache.qpid.server.management.plugin.HttpManagement;
import org.apache.qpid.tests.http.HttpRequestConfig;
import org.apache.qpid.tests.http.HttpTestBase;
import org.apache.qpid.tests.http.HttpTestHelper;
import org.apache.qpid.tests.utils.ConfigItem;

@HttpRequestConfig(useVirtualHostAsHost = false)
@ConfigItem(name = HttpManagement.HTTP_MANAGEMENT_ENABLE_CONTENT_AUTHENTICATION, value = "true")
public class BrokerMetricsAuthenticationTest extends HttpTestBase
{
    @Test
    public void testBrokerMetricsForAuthenticatedUser() throws Exception
    {
        getHelper().submitRequest("/metrics", "GET", HttpServletResponse.SC_OK);
    }

    @Test
    public void testBrokerMetricsForUnauthenticatedUser() throws Exception
    {
        final HttpTestHelper helper = new HttpTestHelper(getBrokerAdmin(), null);
        helper.setUserName(null);
        helper.setPassword(null);
        helper.submitRequest("/metrics", "GET", HttpServletResponse.SC_UNAUTHORIZED);
    }

}
