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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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
import org.apache.qpid.test.utils.UnitTestBase;

public class BindingControllerTest extends UnitTestBase
{
    private LegacyManagementController _managementController;
    private LegacyManagementController _nextVersionManagementController;
    private BindingController _controller;
    private ConfiguredObject<?> _root;

    @Before
    public void setUp()
    {
        _managementController = mock(LegacyManagementController.class);
        _nextVersionManagementController = mock(LegacyManagementController.class);
        when(_managementController.getNextVersionManagementController()).thenReturn(_nextVersionManagementController);
        _controller = new BindingController(_managementController);
        _root = mock(ConfiguredObject.class);
    }

    @Test
    public void getCategory()
    {
        assertThat(_controller.getCategory(), is(equalTo("Binding")));
    }

    @Test
    public void getNextVersionCategory()
    {
        assertThat(_controller.getNextVersionCategory(), is(equalTo(null)));
    }

    @Test
    public void getDefaultType()
    {
        assertThat(_controller.getDefaultType(), is(equalTo(null)));
    }

    @Test
    public void getParentCategories()
    {
        assertThat(_controller.getParentCategories(), is(equalTo(new String[]{"Exchange", "Queue"})));
    }

    @Test
    public void getManagementController()
    {
        assertThat(_controller.getManagementController(), is(equalTo(_managementController)));
    }

    @Test
    public void get()
    {
        final List<String> path = Arrays.asList("my-vhn", "my-vh", "my-exchange", "my-queue", "my-binding");
        final Map<String, List<String>> parameters =
                Collections.singletonMap("actuals", Collections.singletonList("true"));

        final List<String> hierarchy = Arrays.asList("virtualhostnode", "virtualhost", "exchange", "queue", "binding");
        when(_managementController.getCategoryHierarchy(_root, "Binding")).thenReturn(hierarchy);

        final LegacyConfiguredObject exchange1 = mock(LegacyConfiguredObject.class);
        when(exchange1.getAttribute(LegacyConfiguredObject.NAME)).thenReturn("foo");
        when(exchange1.getCategory()).thenReturn("Exchange");

        final LegacyConfiguredObject exchange2 = mock(LegacyConfiguredObject.class);
        when(exchange2.getAttribute(LegacyConfiguredObject.NAME)).thenReturn("my-exchange");
        when(exchange2.getCategory()).thenReturn("Exchange");
        final LegacyConfiguredObject vh = mock(LegacyConfiguredObject.class);
        when(exchange2.getParent("VirtualHost")).thenReturn(vh);
        final LegacyConfiguredObject queue = mock(LegacyConfiguredObject.class);
        when(queue.getAttribute(LegacyConfiguredObject.NAME)).thenReturn("my-queue");
        final Collection<LegacyConfiguredObject> queues = Collections.singletonList(queue);
        when(vh.getChildren("Queue")).thenReturn(queues);
        final Binding binding = mock(Binding.class);
        when(binding.getName()).thenReturn("my-binding");
        when(binding.getDestination()).thenReturn("my-queue");
        when(binding.getBindingKey()).thenReturn("my-binding");
        final Collection<Binding> bindings = Collections.singletonList(binding);
        when(exchange2.getAttribute("bindings")).thenReturn(bindings);
        final Collection<LegacyConfiguredObject> exchanges = Arrays.asList(exchange1, exchange2);

        doReturn(exchanges).when(_nextVersionManagementController).get(any(), eq("exchange"), any(), any());

        final Object readResult = _controller.get(_root, path, parameters);
        assertThat(readResult, is(instanceOf(Collection.class)));

        final Collection<?> exchangeBindings = (Collection<?>) readResult;
        assertThat(exchangeBindings.size(), is(equalTo(1)));

        final Object object = exchangeBindings.iterator().next();
        assertThat(object, is(instanceOf(LegacyConfiguredObject.class)));

        final LegacyConfiguredObject bindingObject = (LegacyConfiguredObject) object;
        assertThat(bindingObject.getAttribute(LegacyConfiguredObject.NAME), is(equalTo("my-binding")));
    }

    @Test
    public void createOrUpdate()
    {
        final List<String> path = Arrays.asList("my-vhn", "my-vh", "my-exchange");
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put("name", "my-binding");
        attributes.put("queue", "my-queue");

        final List<String> hierarchy = Arrays.asList("virtualhostnode", "virtualhost", "exchange", "queue", "binding");
        doReturn(hierarchy).when(_managementController).getCategoryHierarchy(_root, "Binding");

        final LegacyConfiguredObject exchange = mock(LegacyConfiguredObject.class);
        when(exchange.getCategory()).thenReturn("Exchange");
        when(exchange.getAttribute(LegacyConfiguredObject.NAME)).thenReturn("my-exchange");

        final ManagementResponse bindingResult = new ControllerManagementResponse(ResponseType.DATA, Boolean.TRUE);
        when(exchange.invoke(eq("bind"), any(), eq(true))).thenReturn(bindingResult);

        final LegacyConfiguredObject queue = mock(LegacyConfiguredObject.class);
        when(queue.getCategory()).thenReturn("Queue");
        when(queue.getAttribute(LegacyConfiguredObject.NAME)).thenReturn("my-queue");

        doReturn(exchange).when(_nextVersionManagementController).get(any(), eq("exchange"), any(), any());
        doReturn(queue).when(_nextVersionManagementController).get(any(), eq("queue"), any(), any());
        when(_managementController.convertFromNextVersion(exchange)).thenReturn(exchange);
        when(_managementController.convertFromNextVersion(queue)).thenReturn(queue);

        final LegacyConfiguredObject binding = _controller.createOrUpdate(_root, path, attributes, true);
        assertThat(binding, is(notNullValue()));

        assertThat(binding.getAttribute(LegacyConfiguredObject.NAME), is(equalTo("my-binding")));

        Object queueObject = binding.getAttribute("queue");
        Object exchangeObject = binding.getAttribute("exchange");

        assertThat(queueObject, is(instanceOf(LegacyConfiguredObject.class)));
        assertThat(exchangeObject, is(instanceOf(LegacyConfiguredObject.class)));

        assertThat(((LegacyConfiguredObject) queueObject).getAttribute(LegacyConfiguredObject.NAME),
                   is(equalTo("my-queue")));
        assertThat(((LegacyConfiguredObject) exchangeObject).getAttribute(LegacyConfiguredObject.NAME),
                   is(equalTo("my-exchange")));
    }

    @Test
    public void delete()
    {
        final List<String> path = Arrays.asList("my-vhn", "my-vh", "my-exchange", "my-queue", "my-binding");

        final List<String> hierarchy = Arrays.asList("virtualhostnode", "virtualhost", "exchange", "queue", "binding");
        doReturn(hierarchy).when(_managementController).getCategoryHierarchy(_root, "Binding");


        final LegacyConfiguredObject exchange = mock(LegacyConfiguredObject.class);
        when(exchange.getCategory()).thenReturn("Exchange");
        when(exchange.getAttribute(LegacyConfiguredObject.NAME)).thenReturn("my-exchange");

        final ManagementResponse unbindingResult = new ControllerManagementResponse(ResponseType.DATA, Boolean.TRUE);
        when(exchange.invoke(eq("unbind"), any(), eq(true))).thenReturn(unbindingResult);

        doReturn(exchange).when(_nextVersionManagementController).get(any(), eq("exchange"), any(), any());

        int result = _controller.delete(_root, path, Collections.emptyMap());

        assertThat(result, is(equalTo(1)));
        verify(exchange).invoke(eq("unbind"), any(), eq(true));
    }

    @Test
    public void invoke()
    {
        final List<String> path = Arrays.asList("my-vhn", "my-vh", "my-exchange", "my-queue", "my-binding");
        final String operationName = "getStatistics";
        final Map<String, Object> parameters = Collections.emptyMap();



        final List<String> hierarchy = Arrays.asList("virtualhostnode", "virtualhost", "exchange", "queue", "binding");
        when(_managementController.getCategoryHierarchy(_root, "Binding")).thenReturn(hierarchy);


        final LegacyConfiguredObject exchange = mock(LegacyConfiguredObject.class);
        when(exchange.getAttribute(LegacyConfiguredObject.NAME)).thenReturn("my-exchange");
        when(exchange.getCategory()).thenReturn("Exchange");
        final LegacyConfiguredObject vh = mock(LegacyConfiguredObject.class);
        when(exchange.getParent("VirtualHost")).thenReturn(vh);
        final LegacyConfiguredObject queue = mock(LegacyConfiguredObject.class);
        when(queue.getAttribute(LegacyConfiguredObject.NAME)).thenReturn("my-queue");
        final Collection<LegacyConfiguredObject> queues = Collections.singletonList(queue);
        when(vh.getChildren("Queue")).thenReturn(queues);
        final Binding binding = mock(Binding.class);
        when(binding.getName()).thenReturn("my-binding");
        when(binding.getDestination()).thenReturn("my-queue");
        when(binding.getBindingKey()).thenReturn("my-binding");
        final Collection<Binding> bindings = Collections.singletonList(binding);
        when(exchange.getAttribute("bindings")).thenReturn(bindings);
        final Collection<LegacyConfiguredObject> exchanges = Collections.singletonList(exchange);

        doReturn(exchanges).when(_nextVersionManagementController).get(any(), eq("exchange"), any(), any());

        final ManagementResponse result = _controller.invoke(_root, path, operationName, parameters, true, true);

        assertThat(result, is(notNullValue()));
        assertThat(result.getResponseCode(), is(equalTo(200)));
        assertThat(result.getBody(), is(notNullValue()));
        assertThat(result.getBody(), is(equalTo(Collections.emptyMap())));
    }

    @Test
    public void getPreferences()
    {
        final List<String> path = Arrays.asList("vhn", "vh", "exchange", "queue", "binding");
        final Map<String, List<String>> parameters = Collections.emptyMap();
        try
        {
            _controller.getPreferences(_root, path, parameters);
            fail("Binding preferences are unknown");
        }
        catch (ManagementException e)
        {
            // pass
        }
    }

    @Test
    public void setPreferences()
    {
        final List<String> path = Arrays.asList("vhn", "vh", "exchange", "queue", "binding");
        final Map<String, List<String>> parameters = Collections.emptyMap();
        try
        {
            _controller.setPreferences(_root,
                                       path,
                                       Collections.singletonMap("Binding-Preferences",
                                                                Collections.singleton(Collections.singletonMap("value",
                                                                                                               "foo"))),
                                       parameters,
                                       true);
            fail("Binding preferences are unknown");
        }
        catch (ManagementException e)
        {
            // pass
        }
    }

    @Test
    public void deletePreferences()
    {
        final List<String> path = Arrays.asList("vhn", "vh", "exchange", "queue", "binding");
        final Map<String, List<String>> parameters = Collections.emptyMap();
        try
        {
            _controller.deletePreferences(_root,
                                          path,
                                          parameters);
            fail("Binding preferences are unknown");
        }
        catch (ManagementException e)
        {
            // pass
        }
    }
}