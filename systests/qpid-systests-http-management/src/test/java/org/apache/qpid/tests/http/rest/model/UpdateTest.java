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
import static org.apache.qpid.server.management.plugin.servlet.rest.AbstractServlet.SC_UNPROCESSABLE_ENTITY;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.tests.http.HttpRequestConfig;
import org.apache.qpid.tests.http.HttpTestBase;

@HttpRequestConfig
public class UpdateTest extends HttpTestBase
{
    private static final String QUEUE_NAME = "myqueue";
    private static final String QUEUE_URL = String.format("queue/%s", QUEUE_NAME);

    @Before
    public void setUp()
    {
        getBrokerAdmin().createQueue(QUEUE_NAME);
    }

    @Test
    public void update() throws Exception
    {
        final String newDescription = "newDescription";
        Map<String, Object> updatedAttrs = Collections.singletonMap(ConfiguredObject.DESCRIPTION, newDescription);

        getHelper().submitRequest(QUEUE_URL, "PUT", updatedAttrs, SC_OK);
        final Map<String, Object> queue = getHelper().getJsonAsMap(QUEUE_URL);

        assertThat(queue.get(ConfiguredObject.NAME), is(equalTo(QUEUE_NAME)));
        assertThat(queue.get(ConfiguredObject.DESCRIPTION), is(equalTo(newDescription)));
    }

    @Test
    public void typeError() throws Exception
    {
        Map<String, Object> updatedAttrs = Collections.singletonMap(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES, "notNumber");
        getHelper().submitRequest(QUEUE_URL, "PUT", updatedAttrs, SC_UNPROCESSABLE_ENTITY);
    }

    @Test
    public void emptyUpdate() throws Exception
    {
        getHelper().submitRequest(QUEUE_URL, "PUT", Collections.<String, Object>emptyMap(), SC_OK);
    }

    @Test
    public void notFound() throws Exception
    {
        final String queueUrl = "queue/unknown";
        getHelper().submitRequest(queueUrl, "POST", Collections.emptyMap(), SC_NOT_FOUND);
    }

    @Test
    public void immutableAttributeRejected() throws Exception
    {
        final Map<String, Object> before = getHelper().getJsonAsMap(QUEUE_URL);
        String originalId = (String) before.get(ConfiguredObject.ID);
        assertThat(originalId, is(notNullValue()));

        Map<String, Object> updatedAttrs = Collections.singletonMap(ConfiguredObject.ID, UUID.randomUUID());

        getHelper().submitRequest(QUEUE_URL, "POST", updatedAttrs, SC_UNPROCESSABLE_ENTITY);
        final Map<String, Object> queue = getHelper().getJsonAsMap(QUEUE_URL);

        assertThat(queue.get(ConfiguredObject.ID), is(equalTo(before.get(ConfiguredObject.ID))));
    }
}
