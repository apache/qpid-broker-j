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
package org.apache.qpid.server.management.plugin.controller.latest;

import static org.apache.qpid.server.management.plugin.HttpManagementConfiguration.PREFERENCE_OPERTAION_TIMEOUT_CONTEXT_NAME;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.management.plugin.HttpManagementConfiguration;
import org.apache.qpid.server.management.plugin.ManagementController;
import org.apache.qpid.server.management.plugin.ManagementRequest;
import org.apache.qpid.server.management.plugin.ManagementResponse;
import org.apache.qpid.server.management.plugin.controller.LegacyConfiguredObject;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.test.utils.UnitTestBase;

public class LatestManagementControllerAdapterTest extends UnitTestBase
{
    private LatestManagementControllerAdapter _adapter;

    @Before
    public void setUp()
    {
        final HttpManagementConfiguration<?> httpManagement = mock(HttpManagementConfiguration.class);
        when(httpManagement.getContextValue(Long.class, PREFERENCE_OPERTAION_TIMEOUT_CONTEXT_NAME)).thenReturn(1000L);
        when(httpManagement.getModel()).thenReturn(BrokerModel.getInstance());
        final ManagementController managementController = new LatestManagementController(httpManagement);
        _adapter = new LatestManagementControllerAdapter(managementController);
    }

    @Test
    public void getVersion()
    {
        assertThat(_adapter.getVersion(), is(equalTo(BrokerModel.MODEL_VERSION)));
    }

    @Test
    public void getCategories()
    {
        assertThat(_adapter.getCategories(), is(equalTo(BrokerModel.getInstance()
                                                                   .getSupportedCategories()
                                                                   .stream()
                                                                   .map(Class::getSimpleName)
                                                                   .collect(Collectors.toSet()))));
    }

    @Test
    public void getCategoryMapping()
    {
        assertThat(_adapter.getCategoryMapping("foo"),
                   is(equalTo(String.format("/api/v%s/%s/", BrokerModel.MODEL_VERSION, "foo"))));
    }

    @Test
    public void getCategory()
    {
        final ConfiguredObject<?> object = mock(ConfiguredObject.class);
        doReturn(Broker.class).when(object).getCategoryClass();
        assertThat(_adapter.getCategory(object), is(equalTo(Broker.class.getSimpleName())));
    }

    @Test
    public void getCategoryHierarchy()
    {
        final Broker<?> object = BrokerTestHelper.createBrokerMock();
        final Collection<String> expected = Arrays.asList("VirtualHostNode", "VirtualHost", "Queue");
        assertThat(_adapter.getCategoryHierarchy(object, "Queue"), is(equalTo(expected)));
    }

    @Test
    public void handleGet() throws Exception
    {
        final String hostName = "test";
        final String queueName = "foo";

        final QueueManagingVirtualHost<?> virtualHost = createTestVirtualHost(hostName);
        virtualHost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, queueName));

        final String nodeName = virtualHost.getParent().getName();
        final ManagementRequest request = mockManagementRequest(virtualHost.getBroker(),
                                                                "GET",
                                                                "queue",
                                                                Arrays.asList(nodeName, hostName),
                                                                Collections.singletonMap("name",
                                                                                         Collections.singletonList("foo")));

        final ManagementResponse response = _adapter.handleGet(request);
        assertThat(response, is(notNullValue()));
        assertThat(response.getResponseCode(), is(equalTo(200)));
        assertThat(response.getBody(), is(notNullValue()));
        assertThat(response.getBody(), is(instanceOf(Collection.class)));

        final Collection data = (Collection) response.getBody();
        assertThat(data.size(), is(equalTo(1)));

        final Object object = data.iterator().next();
        assertThat(object, is(notNullValue()));
        assertThat(object, is(instanceOf(LegacyConfiguredObject.class)));
        assertThat(((LegacyConfiguredObject) object).getAttribute(LegacyConfiguredObject.NAME), is(equalTo("foo")));
    }


    @Test
    public void handlePut() throws Exception
    {
        final String hostName = "test";
        final String queueName = "foo";

        final QueueManagingVirtualHost<?> virtualHost = createTestVirtualHost(hostName);
        final String nodeName = virtualHost.getParent().getName();
        final Broker root = virtualHost.getBroker();

        final ManagementRequest request =
                mockManagementRequest(root, "PUT", "queue", Arrays.asList(nodeName, hostName), Collections.emptyMap());

        when(request.getBody(LinkedHashMap.class)).thenReturn(new LinkedHashMap<String, Object>(Collections.singletonMap(
                "name",
                queueName)));
        when(request.getRequestURL()).thenReturn("test");

        final ManagementResponse response = _adapter.handlePut(request);
        assertThat(response, is(notNullValue()));
        assertThat(response.getResponseCode(), is(equalTo(201)));
        assertThat(response.getBody(), is(notNullValue()));
        assertThat(response.getBody(), is(instanceOf(LegacyConfiguredObject.class)));

        final LegacyConfiguredObject object = (LegacyConfiguredObject) response.getBody();
        assertThat(object.getAttribute(LegacyConfiguredObject.NAME), is(equalTo("foo")));
    }

    @Test
    public void handlePost() throws Exception
    {
        final String hostName = "test";
        final String queueName = "foo";

        final QueueManagingVirtualHost<?> virtualHost = createTestVirtualHost(hostName);
        final String nodeName = virtualHost.getParent().getName();
        final Broker root = virtualHost.getBroker();

        final ManagementRequest request =
                mockManagementRequest(root, "POST", "queue", Arrays.asList(nodeName, hostName), Collections.emptyMap());

        when(request.getBody(LinkedHashMap.class)).thenReturn(new LinkedHashMap<String, Object>(Collections.singletonMap(
                "name",
                queueName)));
        when(request.getRequestURL()).thenReturn("test");

        final ManagementResponse response = _adapter.handlePut(request);
        assertThat(response, is(notNullValue()));
        assertThat(response.getResponseCode(), is(equalTo(201)));
        assertThat(response.getBody(), is(notNullValue()));
        assertThat(response.getBody(), is(instanceOf(LegacyConfiguredObject.class)));

        final LegacyConfiguredObject object = (LegacyConfiguredObject) response.getBody();
        assertThat(object.getAttribute(LegacyConfiguredObject.NAME), is(equalTo("foo")));
    }

    @Test
    public void handleDelete() throws Exception
    {
        final String hostName = "test";
        final String queueName = "foo";

        final QueueManagingVirtualHost<?> virtualHost = createTestVirtualHost(hostName);
        virtualHost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, queueName));
        assertThat(virtualHost.getQueueCount(), is(equalTo(1L)));

        final String nodeName = virtualHost.getParent().getName();
        final ManagementRequest request = mockManagementRequest(virtualHost.getBroker(),
                                                                "DELETE",
                                                                "queue",
                                                                Arrays.asList(nodeName, hostName),
                                                                Collections.singletonMap("name",
                                                                                         Collections.singletonList("foo")));
        final ManagementResponse response = _adapter.handleDelete(request);
        assertThat(response, is(notNullValue()));
        assertThat(response.getResponseCode(), is(equalTo(200)));
        assertThat(virtualHost.getQueueCount(), is(equalTo(0L)));
    }

    @Test
    public void get() throws Exception
    {
        final String hostName = "test";
        final String queueName = "foo";

        final QueueManagingVirtualHost<?> virtualHost = createTestVirtualHost(hostName);
        virtualHost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, queueName));

        final String nodeName = virtualHost.getParent().getName();

        final Object response = _adapter.get(virtualHost.getBroker(), "queue", Arrays.asList(nodeName, hostName),
                                             Collections.singletonMap("name",
                                                                      Collections.singletonList("foo")));
        assertThat(response, is(instanceOf(Collection.class)));

        final Collection data = (Collection) response;
        assertThat(data.size(), is(equalTo(1)));

        final Object object = data.iterator().next();
        assertThat(object, is(notNullValue()));
        assertThat(object, is(instanceOf(LegacyConfiguredObject.class)));
        assertThat(((LegacyConfiguredObject) object).getAttribute(LegacyConfiguredObject.NAME), is(equalTo("foo")));
    }

    @Test
    public void createOrUpdate() throws Exception
    {
        final String hostName = "test";
        final String queueName = "foo";

        final QueueManagingVirtualHost<?> virtualHost = createTestVirtualHost(hostName);
        final String nodeName = virtualHost.getParent().getName();
        final Broker root = virtualHost.getBroker();

        final ManagementRequest request =
                mockManagementRequest(root, "POST", "queue", Arrays.asList(nodeName, hostName), Collections.emptyMap());

        when(request.getBody(LinkedHashMap.class)).thenReturn(new LinkedHashMap<String, Object>(Collections.singletonMap(
                "name",
                queueName)));
        when(request.getRequestURL()).thenReturn("test");

        final Object response = _adapter.createOrUpdate(virtualHost.getBroker(),
                                                        "queue",
                                                        Arrays.asList(nodeName, hostName),
                                                        Collections.singletonMap("name", queueName),
                                                        true);
        assertThat(response, is(instanceOf(LegacyConfiguredObject.class)));

        final LegacyConfiguredObject object = (LegacyConfiguredObject) response;
        assertThat(object.getAttribute(LegacyConfiguredObject.NAME), is(equalTo(queueName)));
    }

    @Test
    public void delete() throws Exception
    {
        final String hostName = "test";
        final String queueName = "foo";

        final QueueManagingVirtualHost<?> virtualHost = createTestVirtualHost(hostName);
        virtualHost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, queueName));
        assertThat(virtualHost.getQueueCount(), is(equalTo(1L)));

        final String nodeName = virtualHost.getParent().getName();

        _adapter.delete(virtualHost.getBroker(),
                        "queue",
                        Arrays.asList(nodeName, hostName, queueName),
                        Collections.emptyMap());

        assertThat(virtualHost.getQueueCount(), is(equalTo(0L)));
    }

    @Test
    public void invoke() throws Exception
    {
        final String hostName = "test";
        final String queueName = "foo";

        final QueueManagingVirtualHost<?> virtualHost = createTestVirtualHost(hostName);
        Queue queue = virtualHost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, queueName));
        assertThat(virtualHost.getQueueCount(), is(equalTo(1L)));

        List<String> path = Arrays.asList(virtualHost.getParent().getName(), hostName);

        Map<String, Object> message = new HashMap<>();
        message.put("address", "foo");
        message.put("persistent", "false");
        message.put("content", "Test Content");
        message.put("mimeType", "text/plain");
        ManagementResponse response = _adapter.invoke(virtualHost.getBroker(),
                                                         "virtualhost",
                                                         path,
                                                         "publishMessage",
                                                         Collections.singletonMap("message", message),
                                                         true,
                                                         true);

        assertThat(response, is(notNullValue()));
        assertThat(response.getResponseCode(), is(equalTo(200)));
        Object body = response.getBody();
        assertThat(body, is(instanceOf(Number.class)));
        assertThat(((Number) body).intValue(), is(equalTo(1)));
        assertThat(queue.getQueueDepthMessages(), is(equalTo(1)));
    }

    @Test
    public void formatConfiguredObject() throws Exception
    {
        final String hostName = "test";
        final QueueManagingVirtualHost<?> virtualHost = createTestVirtualHost(hostName);
        final String queueName = "foo";
        virtualHost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, queueName));
        assertThat(virtualHost.getQueueCount(), is(equalTo(1L)));

        final Object formatted = _adapter.formatConfiguredObject(virtualHost,
                                                                    Collections.singletonMap("depth",
                                                                                             Collections.singletonList(
                                                                                                     "1")),
                                                                    true);
        assertThat(formatted, is(notNullValue()));
        assertThat(formatted, is(instanceOf(Map.class)));

        final Map<?, ?> data = (Map<?, ?>) formatted;
        assertThat(data.get(VirtualHost.NAME), is(equalTo(hostName)));
        final Object queues = data.get("queues");
        assertThat(queues, is(notNullValue()));
        assertThat(queues, is(instanceOf(Collection.class)));

        final Collection<?> queueCollection = (Collection<?>) queues;

        assertThat(queueCollection.size(), is(equalTo(1)));
        final Iterator<?> iterator = queueCollection.iterator();
        final Object queue1 = iterator.next();

        assertThat(queue1, is(instanceOf(Map.class)));

        final Map<?, ?> queueMap1 = (Map<?, ?>) queue1;

        assertThat(queueMap1.get(Queue.NAME), is(equalTo("foo")));
    }


    private QueueManagingVirtualHost<?> createTestVirtualHost(final String hostName) throws Exception
    {
        final QueueManagingVirtualHost<?> virtualHost = BrokerTestHelper.createVirtualHost(hostName, this);
        final Broker root = virtualHost.getBroker();
        final ConfiguredObject<?> virtualHostNode = virtualHost.getParent();
        when(root.getChildren(VirtualHostNode.class)).thenReturn(Collections.singletonList(virtualHostNode));
        when(virtualHostNode.getChildren(VirtualHost.class)).thenReturn(Collections.singletonList(virtualHost));
        when(virtualHostNode.getChildByName(VirtualHost.class, hostName)).thenReturn(virtualHost);
        return virtualHost;
    }

    private ManagementRequest mockManagementRequest(ConfiguredObject<?> root,
                                                    String method,
                                                    String category,
                                                    List<String> path,
                                                    Map<String, List<String>> parameters)
    {
        final ManagementRequest request = mock(ManagementRequest.class);
        when(request.getCategory()).thenReturn(category);
        doReturn(root).when(request).getRoot();
        when(request.getPath()).thenReturn(path);
        when(request.getParameters()).thenReturn(parameters);
        when(request.getMethod()).thenReturn(method);
        return request;
    }
}
