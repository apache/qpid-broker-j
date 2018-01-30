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
package org.apache.qpid.tests.http.rest.model;

import static javax.servlet.http.HttpServletResponse.SC_NOT_FOUND;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.tests.http.HttpRequestConfig;
import org.apache.qpid.tests.http.HttpTestBase;

@HttpRequestConfig
public class DeleteTest extends HttpTestBase
{
    private static final String QUEUE1_NAME = "myqueue1";
    private static final String QUEUE2_NAME = "myqueue2";
    private static final String QUEUE1_URL = String.format("queue/%s", QUEUE1_NAME);
    private static final String QUEUE2_URL = String.format("queue/%s", QUEUE2_NAME);

    @Before
    public void setUp()
    {
        getBrokerAdmin().createQueue(QUEUE1_NAME);
        getBrokerAdmin().createQueue(QUEUE2_NAME);
    }

    @Test
    public void delete() throws Exception
    {
        getHelper().submitRequest(QUEUE1_URL, "GET", SC_OK);
        getHelper().submitRequest(QUEUE1_URL, "DELETE", SC_OK);
        getHelper().submitRequest(QUEUE1_URL, "GET", SC_NOT_FOUND);
    }

    @Test
    public void notFound() throws Exception
    {
        final String queueUrl = "queue/unknown";
        getHelper().submitRequest(queueUrl, "DELETE", SC_NOT_FOUND);
    }

    @Test
    public void deleteAll() throws Exception
    {
        getHelper().submitRequest("queue/", "DELETE", SC_OK);
        getHelper().submitRequest(QUEUE1_URL, "GET", SC_NOT_FOUND);
        getHelper().submitRequest(QUEUE2_URL, "GET", SC_NOT_FOUND);
    }

    @Test
    public void deleteFilter() throws Exception
    {
        final Map<String, Object> queue1 = getHelper().getJsonAsMap(QUEUE1_URL);

        getHelper().submitRequest(String.format("queue/?%s=%s", ConfiguredObject.ID, queue1.get(ConfiguredObject.ID)),
                                  "DELETE", SC_OK);
        getHelper().submitRequest(QUEUE1_URL, "GET", SC_NOT_FOUND);
        getHelper().submitRequest(QUEUE2_URL, "GET", SC_OK);
    }
}
