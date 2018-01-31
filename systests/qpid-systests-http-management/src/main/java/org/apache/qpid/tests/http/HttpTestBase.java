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

package org.apache.qpid.tests.http;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;

public abstract class HttpTestBase extends BrokerAdminUsingTestBase
{
    @Rule
    public final TestName _testName = new TestName();
    private HttpTestHelper _helper;

    @Before
    public void setUpTestBase()
    {
        System.setProperty("sun.net.http.allowRestrictedHeaders", "true");

        HttpRequestConfig config = getClass().getAnnotation(HttpRequestConfig.class);
        _helper = new HttpTestHelper(getBrokerAdmin(),
                                     config != null && config.useVirtualHostAsHost() ? getVirtualHost() : null);
    }

    @After
    public void tearDownTestBase()
    {
        System.clearProperty("sun.net.http.allowRestrictedHeaders");
    }

    protected String getVirtualHost()
    {
        return getClass().getSimpleName() + "_" + _testName.getMethodName();
    }

    public HttpTestHelper getHelper()
    {
        return _helper;
    }
}
