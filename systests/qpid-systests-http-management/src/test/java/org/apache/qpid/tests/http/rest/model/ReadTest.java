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

import static java.util.Collections.singletonMap;
import static javax.servlet.http.HttpServletResponse.SC_NOT_FOUND;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.tests.http.HttpRequestConfig;
import org.apache.qpid.tests.http.HttpTestBase;

/** KWTODO wildcards, actuals */
@HttpRequestConfig
public class ReadTest extends HttpTestBase
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
    public void readObject() throws Exception
    {
        final Map<String, Object> queue = getHelper().getJsonAsMap(QUEUE1_URL);
        assertThat(queue.get(ConfiguredObject.NAME), is(equalTo(QUEUE1_NAME)));
        assertThat(queue.get(ConfiguredObject.ID), is(notNullValue()));
    }

    @Test
    public void notFound() throws Exception
    {
        getHelper().submitRequest("queue/unknown", "GET", SC_NOT_FOUND);
    }

    @Test
    public void readObjectResponseAsList() throws Exception
    {
        final Map<String, Object> queue = getHelper().getJsonAsSingletonList(QUEUE1_URL + "?singletonModelObjectResponseAsList=true");
        assertThat(queue.get(ConfiguredObject.NAME), is(equalTo(QUEUE1_NAME)));
        assertThat(queue.get(ConfiguredObject.ID), is(notNullValue()));
    }

    @Test
    public void readAll() throws Exception
    {
        List<Map<String, Object>> list = getHelper().getJsonAsList("queue");
        assertThat(list.size(), is(equalTo(2)));
        Set<String> queueNames = list.stream()
                                     .map(map -> (String) map.get(ConfiguredObject.NAME))
                                     .collect(Collectors.toSet());
        assertThat(queueNames, containsInAnyOrder(QUEUE1_NAME, QUEUE2_NAME));
    }

    @Test
    public void readFilter() throws Exception
    {
        final Map<String, Object> queue1 = getHelper().getJsonAsMap(QUEUE1_URL);

        List<Map<String, Object>> list = getHelper().getJsonAsList(String.format("queue/?%s=%s",
                                                                                 ConfiguredObject.ID,
                                                                                 queue1.get(ConfiguredObject.ID)));
        assertThat(list.size(), is(equalTo(1)));
        final Map<String, Object> queue = list.get(0);
        assertThat(queue.get(ConfiguredObject.NAME), is(equalTo(QUEUE1_NAME)));
        assertThat(queue.get(ConfiguredObject.ID), is(notNullValue()));
    }

    @Test
    public void filterNotFound() throws Exception
    {
        List<Map<String, Object>> list = getHelper().getJsonAsList(String.format("queue/?%s=%s",
                                                                                 ConfiguredObject.ID,
                                                                                 UUID.randomUUID()));
        assertThat(list.size(), is(equalTo(0)));
    }

    @Test
    public void readHierarchy() throws Exception
    {
        final Map<String, Object> virtualHost = getHelper().getJsonAsMap("virtualhost?depth=2");
        assertThat(virtualHost.get(ConfiguredObject.ID), is(notNullValue()));
        assertThat(virtualHost.get(ConfiguredObject.NAME), is(equalTo(getVirtualHost())));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> queues = (List<Map<String, Object>>) virtualHost.get("queues");
        assertThat(queues.size(), is(equalTo(2)));
    }

    @Test
    public void excludeInheritedContext() throws Exception
    {
        final String hostContextKey = "myvhcontextvar";
        final String hostContextValue = UUID.randomUUID().toString();
        final Map<String, Object> hostUpdateAttrs = singletonMap("context",
                                                                 singletonMap(hostContextKey, hostContextValue));
        getHelper().submitRequest("virtualhost", "POST", hostUpdateAttrs, SC_OK);

        final String queueContextKey = "myqueuecontextvar";
        final String queueContextValue = UUID.randomUUID().toString();
        final Map<String, Object> queueUpdateAttrs = singletonMap("context",
                                                                  singletonMap(queueContextKey, queueContextValue));
        getHelper().submitRequest(QUEUE1_URL, "POST", queueUpdateAttrs, SC_OK);

        final Map<String, Object> queue = getHelper().getJsonAsMap(QUEUE1_URL);
        @SuppressWarnings("unchecked")
        Map<String, Object> context = (Map<String, Object>) queue.get("context");
        assertThat(context.size(), is(equalTo(1)));
        assertThat(context.get(queueContextKey), is(equalTo(queueContextValue)));

        final Map<String, Object> queue2 = getHelper().getJsonAsMap(QUEUE1_URL + "?excludeInheritedContext=false");
        @SuppressWarnings("unchecked")
        Map<String, Object> context2 = (Map<String, Object>) queue2.get("context");
        assertThat(context2.size(), is(greaterThanOrEqualTo(2)));
        assertThat(context2.get(queueContextKey), is(equalTo(queueContextValue)));
        assertThat(context2.get(hostContextKey), is(equalTo(hostContextValue)));
    }
}
