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
package org.apache.qpid.server.management.plugin.controller.v6_1.category;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.management.plugin.ManagementException;
import org.apache.qpid.server.management.plugin.ManagementResponse;
import org.apache.qpid.server.management.plugin.ResponseType;
import org.apache.qpid.server.management.plugin.controller.ControllerManagementResponse;
import org.apache.qpid.server.management.plugin.controller.LegacyConfiguredObject;
import org.apache.qpid.server.management.plugin.controller.LegacyManagementController;
import org.apache.qpid.server.model.Binding;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Connection;
import org.apache.qpid.server.model.Session;
import org.apache.qpid.server.transport.AbstractAMQPConnection;
import org.apache.qpid.test.utils.UnitTestBase;

public class ConsumerControllerTest extends UnitTestBase
{
    private LegacyManagementController _managementController;
    private LegacyManagementController _nextVersionManagementController;
    private ConsumerController _controller;
    private ConfiguredObject<?> _root;

    @Before
    public void setUp()
    {
        _managementController = mock(LegacyManagementController.class);
        _nextVersionManagementController = mock(LegacyManagementController.class);
        when(_managementController.getNextVersionManagementController()).thenReturn(_nextVersionManagementController);
        _controller = new ConsumerController(_managementController);
        _root = mock(ConfiguredObject.class);
    }
    @Test
    public void getCategory()
    {
        assertThat(_controller.getCategory(), is(equalTo("Consumer")));
    }

    @Test
    public void getNextVersionCategory()
    {
        assertThat(_controller.getNextVersionCategory(), is(equalTo("Consumer")));
    }

    @Test
    public void getDefaultType()
    {
        assertThat(_controller.getDefaultType(), is(equalTo(null)));
    }

    @Test
    public void getParentCategories()
    {
        assertThat(_controller.getParentCategories(), is(equalTo(new String[]{"Session", "Queue"})));
    }

    @Test
    public void getManagementController()
    {
        assertThat(_controller.getManagementController(), is(equalTo(_managementController)));
    }

    @Test
    public void get()
    {
        final List<String> path = Arrays.asList("my-vhn", "my-vh", "my-session", "my-queue", "my-consumer");
        final Map<String, List<String>> parameters =
                Collections.singletonMap("actuals", Collections.singletonList("true"));

        final List<String> hierarchy = Arrays.asList("virtualhostnode", "virtualhost", "session", "queue", "consumer");
        when(_managementController.getCategoryHierarchy(_root, "Consumer")).thenReturn(hierarchy);

        final LegacyConfiguredObject session = mock(LegacyConfiguredObject.class);
        when(session.getAttribute(LegacyConfiguredObject.NAME)).thenReturn("my-session");
        when(session.getCategory()).thenReturn("Session");

        final LegacyConfiguredObject consumer = mock(LegacyConfiguredObject.class);
        when(consumer.getAttribute(LegacyConfiguredObject.NAME)).thenReturn("my-consumer");
        when(consumer.getAttribute("session")).thenReturn(session);
        final Collection<LegacyConfiguredObject> consumers = Collections.singletonList(consumer);

        final LegacyConfiguredObject queue1 = mock(LegacyConfiguredObject.class);
        when(queue1.getAttribute(LegacyConfiguredObject.NAME)).thenReturn("my-queue1");
        final LegacyConfiguredObject queue2 = mock(LegacyConfiguredObject.class);
        when(queue2.getAttribute(LegacyConfiguredObject.NAME)).thenReturn("my-queue");
        when(queue2.getChildren(ConsumerController.TYPE)).thenReturn(consumers);

        final Collection<LegacyConfiguredObject> queues = Arrays.asList(queue1, queue2);

        doReturn(queues).when(_nextVersionManagementController)
                       .get(eq(_root),
                            eq("Queue"),
                            eq(Arrays.asList("my-vhn", "my-vh")),
                            eq(Collections.emptyMap()));

        final Object result = _controller.get(_root, path, parameters);
        assertThat(result, is(instanceOf(Collection.class)));

        Collection<?> consumerItems = (Collection<?>)result;
        assertThat(consumerItems.size(), is(equalTo(1)));

        final Object object = consumerItems.iterator().next();
        assertThat(object, is(instanceOf(LegacyConfiguredObject.class)));

        final LegacyConfiguredObject consumerObject = (LegacyConfiguredObject) object;
        assertThat(consumerObject.getAttribute(LegacyConfiguredObject.NAME), is(equalTo("my-consumer")));
        assertThat(consumerObject.getCategory(), is(equalTo("Consumer")));
    }

    @Test
    public void delete()
    {
        final List<String> path = Arrays.asList("my-vhn", "my-vh",  "my-queue", "my-consumer");
        final Map<String, List<String>> parameters = Collections.emptyMap();
        try
        {
            _controller.delete(_root, path, parameters);
            fail("Consumer cannot be deleted from REST");
        }
        catch (ManagementException e)
        {
            // pass
        }
    }

    @Test
    public void createOrUpdate()
    {
        final List<String> path = Arrays.asList("my-vhn", "my-vh",  "my-queue", "my-consumer");
        final Map<String, Object> attributes = Collections.singletonMap(LegacyConfiguredObject.NAME, "my-consumer" );
        try
        {
            _controller.createOrUpdate(_root, path, attributes, true);
            fail("Consumer cannot be created from REST");
        }
        catch (ManagementException e)
        {
            // pass
        }
    }

    @Test
    public void invoke()
    {
        final List<String> path = Arrays.asList("my-vhn", "my-vh", "my-session", "my-queue", "my-consumer");
        final List<String> hierarchy = Arrays.asList("virtualhostnode", "virtualhost", "session", "queue", "consumer");
        when(_managementController.getCategoryHierarchy(_root, "Consumer")).thenReturn(hierarchy);

        final LegacyConfiguredObject consumer = mock(LegacyConfiguredObject.class);
        final LegacyConfiguredObject queue = mock(LegacyConfiguredObject.class);
        final LegacyConfiguredObject session = mock(LegacyConfiguredObject.class);
        when(queue.getAttribute(LegacyConfiguredObject.NAME)).thenReturn("my-queue");
        when(consumer.getAttribute(LegacyConfiguredObject.NAME)).thenReturn("my-consumer");
        when(consumer.getAttribute("session")).thenReturn(session);
        when(session.getAttribute(LegacyConfiguredObject.NAME)).thenReturn("my-session");

        final Object stats = mock(Object.class);
        final ManagementResponse statistics = new ControllerManagementResponse(ResponseType.DATA, stats);
        when(consumer.invoke(eq("getStatistics"), eq(Collections.emptyMap()), eq(false))).thenReturn(statistics);
        final Collection<LegacyConfiguredObject> consumers = Collections.singletonList(consumer);
        when(queue.getChildren(ConsumerController.TYPE)).thenReturn(consumers);

        final Collection<LegacyConfiguredObject> queues = Collections.singletonList(queue);

        doReturn(queues).when(_nextVersionManagementController)
                        .get(eq(_root),
                             eq("Queue"),
                             eq(Arrays.asList("my-vhn", "my-vh")),
                             eq(Collections.emptyMap()));

        ManagementResponse response = _controller.invoke(_root, path, "getStatistics", Collections.emptyMap(), false, false);
        assertThat(response, is(notNullValue()));
        assertThat(response.getResponseCode(), is(equalTo(200)));
        assertThat(response.getBody(), is(notNullValue()));
        assertThat(response.getBody(), is(equalTo(stats)));
    }

    @Test
    public void getPreferences()
    {
        final List<String> path = Arrays.asList("my-vhn", "my-vh",  "my-queue", "my-consumer");
        final Map<String, List<String>> parameters = Collections.emptyMap();
        try
        {
            _controller.getPreferences(_root, path, parameters);
            fail("Consumer preferences are unknown");
        }
        catch (ManagementException e)
        {
            // pass
        }
    }

    @Test
    public void setPreferences()
    {
        final List<String> path = Arrays.asList("my-vhn", "my-vh",  "my-queue", "my-consumer");
        final Map<String, List<String>> parameters = Collections.emptyMap();
        try
        {
            _controller.setPreferences(_root,
                                       path,
                                       Collections.singletonMap("Consumer-Preferences",
                                                                Collections.singleton(Collections.singletonMap("value",
                                                                                                               "foo"))),
                                       parameters,
                                       true);
            fail("Consumer preferences are unknown");
        }
        catch (ManagementException e)
        {
            // pass
        }
    }

    @Test
    public void deletePreferences()
    {
        final List<String> path = Arrays.asList("my-vhn", "my-vh",  "my-queue", "my-consumer");
        final Map<String, List<String>> parameters = Collections.emptyMap();
        try
        {
            _controller.deletePreferences(_root,
                                          path,
                                          parameters);
            fail("Consumer preferences are unknown");
        }
        catch (ManagementException e)
        {
            // pass
        }
    }

    @Test
    public void convertFromNextVersion()
    {
        final LegacyConfiguredObject session = mock(LegacyConfiguredObject.class);
        when(session.getAttribute(LegacyConfiguredObject.NAME)).thenReturn("my-session");
        when(session.getCategory()).thenReturn("Session");



        final LegacyConfiguredObject queue1 = mock(LegacyConfiguredObject.class);
        when(queue1.getAttribute(LegacyConfiguredObject.NAME)).thenReturn("my-queue1");
        final LegacyConfiguredObject queue2 = mock(LegacyConfiguredObject.class);
        when(queue2.getAttribute(LegacyConfiguredObject.NAME)).thenReturn("my-queue");



        final LegacyConfiguredObject nextVersionConfiguredObject = mock(LegacyConfiguredObject.class);
        when(nextVersionConfiguredObject.getAttribute("session")).thenReturn(session);
        when(nextVersionConfiguredObject.getAttribute("queue")).thenReturn(queue2);
        when(nextVersionConfiguredObject.getAttribute(LegacyConfiguredObject.NAME)).thenReturn("test-consumer");
        when(nextVersionConfiguredObject.getParent(eq("Queue"))).thenReturn(queue2);
        final Collection<LegacyConfiguredObject> consumers = Collections.singletonList(nextVersionConfiguredObject);
        when(queue2.getChildren(ConsumerController.TYPE)).thenReturn(consumers);

        final LegacyConfiguredObject convertedSession = mock(LegacyConfiguredObject.class);
        when(_managementController.convertFromNextVersion(session)).thenReturn(convertedSession);
        final LegacyConfiguredObject convertedQueue = mock(LegacyConfiguredObject.class);
        when(_managementController.convertFromNextVersion(queue2)).thenReturn(convertedQueue);
        final LegacyConfiguredObject  converted = _controller.convertFromNextVersion(nextVersionConfiguredObject);

        LegacyConfiguredObject sessionParent = converted.getParent("Session");
        LegacyConfiguredObject queueParent = converted.getParent("Queue");

        assertThat(sessionParent, is(equalTo(convertedSession)));
        assertThat(queueParent, is(equalTo(convertedQueue)));

        LegacyConfiguredObject sessionAttribute = (LegacyConfiguredObject)converted.getAttribute("session");
        LegacyConfiguredObject queueAttribute = (LegacyConfiguredObject)converted.getAttribute("queue");

        assertThat(sessionAttribute, is(equalTo(convertedSession)));
        assertThat(queueAttribute, is(equalTo(convertedQueue)));

    }
}