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
package org.apache.qpid.tests.http.rest.model;

import static javax.servlet.http.HttpServletResponse.SC_CREATED;
import static javax.servlet.http.HttpServletResponse.SC_METHOD_NOT_ALLOWED;
import static javax.servlet.http.HttpServletResponse.SC_NOT_FOUND;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import static org.apache.qpid.server.management.plugin.servlet.rest.AbstractServlet.SC_UNPROCESSABLE_ENTITY;
import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import org.junit.Test;

import org.apache.qpid.server.logging.logback.VirtualHostFileLogger;
import org.apache.qpid.server.logging.logback.VirtualHostNameAndLevelLogInclusionRule;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.tests.http.HttpTestBase;
import org.apache.qpid.tests.http.HttpRequestConfig;

@HttpRequestConfig
public class CreateTest extends HttpTestBase
{
    private static final String UTF_8 = StandardCharsets.UTF_8.toString();

    @Test
    public void create() throws Exception
    {
        final String queueUrl = "queue/myqueue";
        getHelper().submitRequest(queueUrl, "PUT", Collections.emptyMap(), SC_CREATED);
        final Map<String, Object> queue = getHelper().getJsonAsMap(queueUrl);

        assertThat(queue.get(ConfiguredObject.NAME), is(equalTo("myqueue")));
    }

    @Test
    public void putToUriLocationHeader() throws Exception
    {
        final String queueUrl = "queue/myqueue";
        Map<String, List<String>> headers = new HashMap<>();
        int responseCode = getHelper().submitRequest(queueUrl, "PUT", Collections.emptyMap(), headers);
        assertThat(responseCode, is(equalTo(SC_CREATED)));
        List<String> location = headers.get("Location");
        assertThat(location.size(), is(equalTo(1)));
        assertThat(location.get(0), endsWith(queueUrl));
    }

    @Test
    public void putToParentUriLocationHeader() throws Exception
    {
        final String parentUrl = "queue";
        final String queueUrl = "queue/myqueue";
        Map<String, List<String>> headers = new HashMap<>();
        int responseCode = getHelper().submitRequest(parentUrl, "PUT", Collections.singletonMap(ConfiguredObject.NAME, "myqueue"), headers);
        assertThat(responseCode, is(equalTo(SC_CREATED)));
        List<String> location = headers.get("Location");
        assertThat(location.size(), is(equalTo(1)));
        assertThat(location.get(0), endsWith(queueUrl));
    }

    @Test
    public void createSubtype() throws Exception
    {
        final String queueUrl = "queue/myqueue";
        final Map<String, Object> attrs = Collections.singletonMap(ConfiguredObject.TYPE, "priority");
        getHelper().submitRequest(queueUrl, "PUT", attrs, SC_CREATED);
        final Map<String, Object> queue = getHelper().getJsonAsMap(queueUrl);

        assertThat(queue.get(ConfiguredObject.NAME), is(equalTo("myqueue")));
        assertThat(queue.get(ConfiguredObject.TYPE), is(equalTo("priority")));
    }

    @Test
    public void createPutToParent() throws Exception
    {
        createToParent("PUT");
    }

    @Test
    public void createPostToParent() throws Exception
    {
        createToParent("POST");
    }

    @Test
    public void unknownSubtype() throws Exception
    {
        final String queueUrl = "queue/myqueue";
        final Map<String, Object> attrs = Collections.singletonMap(ConfiguredObject.TYPE, "unknown");
        getHelper().submitRequest(queueUrl, "PUT", attrs, SC_UNPROCESSABLE_ENTITY);
        getHelper().submitRequest(queueUrl, "GET", SC_NOT_FOUND);
    }

    @Test
    public void unknownCategory() throws Exception
    {
        final String queueUrl = "unknown/myobj";
        final Map<Object, Object> attrs = Collections.singletonMap(ConfiguredObject.TYPE, "unknown");
        getHelper().submitRequest(queueUrl, "PUT", attrs, SC_METHOD_NOT_ALLOWED);
        getHelper().submitRequest(queueUrl, "GET", SC_NOT_FOUND);
    }

    @Test
    public void createChild() throws Exception
    {
        final String parentUrl = "virtualhostlogger/mylogger";
        Map<String, Object> parentAttrs = Collections.singletonMap(ConfiguredObject.TYPE, VirtualHostFileLogger.TYPE);

        getHelper().submitRequest(parentUrl, "PUT", parentAttrs, SC_CREATED);

        final String childUrl = "virtualhostloginclusionrule/mylogger/myrule";
        Map<String, Object> childAttrs = Collections.singletonMap(ConfiguredObject.TYPE, VirtualHostNameAndLevelLogInclusionRule.TYPE);
        getHelper().submitRequest(childUrl, "PUT", childAttrs, SC_CREATED);
    }

    @Test
    public void unknownParent() throws Exception
    {
        final String childUrl = "virtualhostloginclusionrule/unknown/myrule";
        Map<String, Object> childAttrs = Collections.singletonMap(ConfiguredObject.TYPE, VirtualHostNameAndLevelLogInclusionRule.TYPE);
        getHelper().submitRequest(childUrl, "PUT", childAttrs, SC_UNPROCESSABLE_ENTITY);
    }

    @Test
    public void objectsWithSlashes() throws Exception
    {
        String queueName = "testQueue/with/slashes";
        String queueNameEncoded = URLEncoder.encode(queueName, UTF_8);
        String queueNameDoubleEncoded = URLEncoder.encode(queueNameEncoded, UTF_8);
        String queueUrl = "queue/" + queueNameDoubleEncoded;

        Map<String, List<String>> headers = new HashMap<>();
        int responseCode = getHelper().submitRequest(queueUrl, "PUT", Collections.emptyMap(), headers);
        assertThat(responseCode, is(equalTo(SC_CREATED)));
        List<String> location = headers.get("Location");
        assertThat(location.size(), is(equalTo(1)));
        assertThat(location.get(0), endsWith(queueUrl));

        final Map<String, Object> queue = getHelper().getJson(queueUrl,
                                                              new TypeReference<Map<String, Object>>() {}, SC_OK);

        assertThat(queue.get(ConfiguredObject.NAME), is(equalTo(queueName)));
    }

    private void createToParent(final String method) throws Exception
    {
        final String parentUrl = "queue";
        final String queueName = "myqueue";
        final Map<Object, Object> attrs = Collections.singletonMap(ConfiguredObject.NAME, queueName);
        getHelper().submitRequest(parentUrl, method, attrs, SC_CREATED);
        final Map<String, Object> queue = getHelper().getJsonAsMap(String.format("%s/%s", parentUrl, queueName));

        assertThat(queue.get(ConfiguredObject.NAME), is(equalTo(queueName)));
    }
}
