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

package org.apache.qpid.tests.http.v6_1;

import static javax.servlet.http.HttpServletResponse.SC_CREATED;
import static javax.servlet.http.HttpServletResponse.SC_NOT_FOUND;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.filter.AMQPFilterTypes;
import org.apache.qpid.tests.http.HttpRequestConfig;
import org.apache.qpid.tests.http.HttpTestBase;

@HttpRequestConfig
public class BindingTest extends HttpTestBase
{
    private static final String QUEUE_NAME = "myqueue";

    @Before
    public void setUp()
    {
        getBrokerAdmin().createQueue(QUEUE_NAME);
    }

    @Test
    public void bind() throws Exception
    {
        Map<String, String> arguments = Collections.singletonMap(AMQPFilterTypes.JMS_SELECTOR.getValue(), "1<2");
        getHelper().submitRequest("/api/v6.1/binding/amq.direct/myqueue/foo",
                                  "PUT",
                                  Collections.singletonMap("arguments",
                                                           arguments),
                                  SC_CREATED);
        List<Map<String, Object>> bindings = getHelper().getJsonAsList("queue/myqueue/getPublishingLinks");
        assertThat(bindings, is(notNullValue()));

        Map<String, Object> binding = findBindingByName(bindings, "foo");
        assertThat(binding, is(notNullValue()));

        assertThat(binding.get("name"), is(equalTo("foo")));
        assertThat(binding.get("arguments"), is(notNullValue()));

        Object args = binding.get("arguments");
        assertThat(args, is(instanceOf(Map.class)));
        assertThat(args, is(equalTo(arguments)));
    }

    private Map<String, Object> findBindingByName(final List<Map<String, Object>> bindings, final String name)
    {
        return bindings.stream().filter(b -> name.equals(b.get("name"))).findFirst().orElse(null);
    }

    @Test
    public void get() throws Exception
    {
        Map<String, String> arguments = Collections.singletonMap(AMQPFilterTypes.JMS_SELECTOR.getValue(), "1<2");
        Map<String, Object> bindOperationArguments = new HashMap<>();
        bindOperationArguments.put("destination", "myqueue");
        bindOperationArguments.put("bindingKey", "foo");
        bindOperationArguments.put("arguments", arguments);
        getHelper().submitRequest("exchange/amq.direct/bind",
                                  "POST",
                                  bindOperationArguments,
                                  SC_OK);
        List<Map<String, Object>> bindings = getHelper().getJsonAsList("/api/v6.1/binding/amq.direct/myqueue/foo");
        assertThat(bindings, is(notNullValue()));
        assertThat(bindings.size(), is(equalTo(1)));

        Map<String, Object> binding = bindings.get(0);
        assertThat(binding, is(notNullValue()));
        assertThat(binding.get("name"), is(equalTo("foo")));
        assertThat(binding.get("queue"), is(equalTo("myqueue")));
        assertThat(binding.get("arguments"), is(notNullValue()));

        Object args = binding.get("arguments");
        assertThat(args, is(instanceOf(Map.class)));
        assertThat(args, is(equalTo(arguments)));
    }

    @Test
    public void delete() throws Exception
    {
        Map<String, String> arguments = Collections.singletonMap(AMQPFilterTypes.JMS_SELECTOR.getValue(), "1<2");
        Map<String, Object> bindOperationArguments = new HashMap<>();
        bindOperationArguments.put("destination", "myqueue");
        bindOperationArguments.put("bindingKey", "foo");
        bindOperationArguments.put("arguments", arguments);
        getHelper().submitRequest("exchange/amq.direct/bind",
                                  "POST",
                                  bindOperationArguments,
                                  SC_OK);
        getHelper().submitRequest("/api/v6.1/binding/amq.direct/myqueue/foo", "DELETE");
        List<Map<String, Object>> bindings = getHelper().getJsonAsList("queue/myqueue/getPublishingLinks");
        assertThat(bindings, is(notNullValue()));
        Map<String, Object> binding = findBindingByName(bindings, "foo");
        assertThat(binding, is(nullValue()));

    }
}
