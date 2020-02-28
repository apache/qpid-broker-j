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
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.security.auth.Subject;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.management.plugin.HttpManagementConfiguration;
import org.apache.qpid.server.management.plugin.ManagementException;
import org.apache.qpid.server.management.plugin.ManagementRequest;
import org.apache.qpid.server.management.plugin.ManagementResponse;
import org.apache.qpid.server.management.plugin.RequestType;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.model.preferences.GenericPreferenceValueFactory;
import org.apache.qpid.server.model.preferences.Preference;
import org.apache.qpid.server.model.preferences.PreferenceImpl;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;
import org.apache.qpid.server.security.auth.UsernamePrincipal;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.test.utils.UnitTestBase;

public class LatestManagementControllerTest extends UnitTestBase
{
    private LatestManagementController _controller;

    @Before
    public void setUp()
    {
        final HttpManagementConfiguration<?> httpManagement = mock(HttpManagementConfiguration.class);
        when(httpManagement.getContextValue(Long.class, PREFERENCE_OPERTAION_TIMEOUT_CONTEXT_NAME)).thenReturn(1000L);
        when(httpManagement.getModel()).thenReturn(BrokerModel.getInstance());
        _controller = new LatestManagementController(httpManagement);
    }

    @Test
    public void getVersion()
    {
        assertThat(_controller.getVersion(), is(equalTo(BrokerModel.MODEL_VERSION)));
    }

    @Test
    public void getCategories()
    {
        assertThat(_controller.getCategories(), is(equalTo(BrokerModel.getInstance()
                                                                      .getSupportedCategories()
                                                                      .stream()
                                                                      .map(Class::getSimpleName)
                                                                      .collect(Collectors.toSet()))));
    }

    @Test
    public void getCategoryMapping()
    {
        assertThat(_controller.getCategoryMapping("foo"),
                   is(equalTo(String.format("/api/v%s/%s/", BrokerModel.MODEL_VERSION, "foo"))));
    }

    @Test
    public void getCategory()
    {
        final ConfiguredObject<?> object = mock(ConfiguredObject.class);
        doReturn(Broker.class).when(object).getCategoryClass();
        assertThat(_controller.getCategory(object), is(equalTo(Broker.class.getSimpleName())));
    }

    @Test
    public void getCategoryHierarchyForBrokerRootAndQueueCategory()
    {
        final Broker<?> object = BrokerTestHelper.createBrokerMock();
        final Collection<String> expected = Arrays.asList("VirtualHostNode", "VirtualHost", "Queue");
        assertThat(_controller.getCategoryHierarchy(object, "Queue"), is(equalTo(expected)));
    }

    @Test
    public void getCategoryHierarchyForVirtualHostRootAndExchangeCategory() throws Exception
    {
        final QueueManagingVirtualHost<?> object = BrokerTestHelper.createVirtualHost("test", this);
        final Collection<String> expected = Collections.singletonList("Exchange");
        assertThat(_controller.getCategoryHierarchy(object, "Exchange"), is(equalTo(expected)));
    }


    @Test
    public void getCategoryHierarchyForBrokerRootAndUnknownCategory()
    {
        final Broker<?> object = BrokerTestHelper.createBrokerMock();
        final Collection<String> expected = Collections.emptyList();
        assertThat(_controller.getCategoryHierarchy(object, "Binding"), is(equalTo(expected)));
    }

    @Test
    public void getNextVersionManagementController()
    {
        assertThat(_controller.getNextVersionManagementController(), is(nullValue()));
    }

    @Test
    public void getRequestTypeForGetAndModelObjectWithNotFullPath() throws Exception
    {
        final String hostName = "test";
        final QueueManagingVirtualHost<?> virtualHost = createVirtualHostWithQueue(hostName, "foo", "bar");

        final ManagementRequest request = mock(ManagementRequest.class);
        when(request.getCategory()).thenReturn("queue");
        doReturn(virtualHost.getBroker()).when(request).getRoot();
        when(request.getPath()).thenReturn(Arrays.asList("*", hostName));
        when(request.getParameters()).thenReturn(Collections.emptyMap());
        when(request.getMethod()).thenReturn("GET");

        final RequestType type = _controller.getRequestType(request);

        assertThat(type, is(equalTo(RequestType.MODEL_OBJECT)));
    }

    @Test
    public void getRequestTypeForGetAndModelObjectWithFullPath() throws Exception
    {
        final String hostName = "test";
        final QueueManagingVirtualHost<?> virtualHost = createVirtualHostWithQueue(hostName, "foo", "bar");

        final ManagementRequest request = mock(ManagementRequest.class);
        when(request.getCategory()).thenReturn("queue");
        doReturn(virtualHost.getBroker()).when(request).getRoot();
        final List<String> path = Arrays.asList(virtualHost.getParent().getName(), hostName, "bar");
        when(request.getPath()).thenReturn(path);
        when(request.getParameters()).thenReturn(Collections.emptyMap());
        when(request.getMethod()).thenReturn("GET");

        final RequestType type = _controller.getRequestType(request);

        assertThat(type, is(equalTo(RequestType.MODEL_OBJECT)));
    }

    @Test
    public void getRequestTypeForGetAndUserPreferences() throws Exception
    {
        final String hostName = "test";
        final QueueManagingVirtualHost<?> virtualHost = createVirtualHostWithQueue(hostName, "foo", "bar");

        final ManagementRequest request = mock(ManagementRequest.class);
        when(request.getCategory()).thenReturn("queue");
        doReturn(virtualHost.getBroker()).when(request).getRoot();
        List<String> path = Arrays.asList(virtualHost.getParent().getName(),
                                          hostName,
                                          "bar",
                                          "userpreferences");
        when(request.getPath()).thenReturn(path);
        when(request.getParameters()).thenReturn(Collections.emptyMap());
        when(request.getMethod()).thenReturn("GET");

        final RequestType type = _controller.getRequestType(request);

        assertThat(type, is(equalTo(RequestType.USER_PREFERENCES)));
    }

    @Test
    public void getRequestTypeForGetAndVisiblePreferences() throws Exception
    {
        final String hostName = "test";
        final QueueManagingVirtualHost<?> virtualHost = createVirtualHostWithQueue(hostName, "foo", "bar");

        final ManagementRequest request = mock(ManagementRequest.class);
        when(request.getCategory()).thenReturn("queue");
        doReturn(virtualHost.getBroker()).when(request).getRoot();
        List<String> path = Arrays.asList(virtualHost.getParent().getName(), hostName, "bar", "visiblepreferences");
        when(request.getPath()).thenReturn(path);
        when(request.getParameters()).thenReturn(Collections.emptyMap());
        when(request.getMethod()).thenReturn("GET");

        final RequestType type = _controller.getRequestType(request);

        assertThat(type, is(equalTo(RequestType.VISIBLE_PREFERENCES)));
    }

    @Test
    public void getForBrokerRootAndQueueSingletonPath() throws Exception
    {
        final String hostName = "test";
        final QueueManagingVirtualHost<?> virtualHost = createVirtualHostWithQueue(hostName, "foo", "bar");
        final String nodeName = virtualHost.getParent().getName();
        final List<String> path = Arrays.asList(nodeName, hostName, "foo");

        final Object object = _controller.get(virtualHost.getBroker(), "queue", path, Collections.emptyMap());
        assertThat(object, is(notNullValue()));
        assertThat(object, is(instanceOf(Queue.class)));

        final Queue data = (Queue) object;
        assertThat(data.getName(), is(equalTo("foo")));
    }

    @Test
    public void getForBrokerRootAndQueuePathNoQueueName() throws Exception
    {
        final String hostName = "test";
        final QueueManagingVirtualHost<?> virtualHost = createVirtualHostWithQueue(hostName, "foo", "bar");
        final String nodeName = virtualHost.getParent().getName();
        final List<String> path = Arrays.asList(nodeName, hostName);

        final Object object = _controller.get(virtualHost.getBroker(), "queue", path, Collections.emptyMap());
        assertThat(object, is(notNullValue()));
        assertThat(object, is(instanceOf(Collection.class)));

        final Collection<?> data = (Collection<?>) object;
        final Iterator iterator = data.iterator();
        final Object o = iterator.next();
        final Object o2 = iterator.next();
        assertThat(o, is(notNullValue()));
        assertThat(o, is(instanceOf(Queue.class)));
        assertThat(((Queue) o).getName(), is(equalTo("foo")));

        assertThat(o2, is(notNullValue()));
        assertThat(o2, is(instanceOf(Queue.class)));
        assertThat(((Queue) o2).getName(), is(equalTo("bar")));
    }

    @Test
    public void getForBrokerRootAndQueuePathWithWildCards() throws Exception
    {
        final String hostName = "test";
        final QueueManagingVirtualHost<?> virtualHost = createVirtualHostWithQueue(hostName, "foo", "bar");
        final List<String> path = Arrays.asList("*", hostName);

        final Object object = _controller.get(virtualHost.getBroker(), "queue", path, Collections.emptyMap());
        assertThat(object, is(notNullValue()));
        assertThat(object, is(instanceOf(Collection.class)));

        final Collection<?> data = (Collection<?>) object;
        assertThat(data.size(), is(equalTo(2)));
        final Iterator iterator = data.iterator();
        final Object o = iterator.next();
        final Object o2 = iterator.next();
        assertThat(o, is(notNullValue()));
        assertThat(o, is(instanceOf(Queue.class)));
        assertThat(((Queue) o).getName(), is(equalTo("foo")));

        assertThat(o2, is(notNullValue()));
        assertThat(o2, is(instanceOf(Queue.class)));
        assertThat(((Queue) o2).getName(), is(equalTo("bar")));
    }

    @Test
    public void getForBrokerRootAndQueuePathWithFilter() throws Exception
    {
        final String hostName = "test";
        final QueueManagingVirtualHost<?> virtualHost = createVirtualHostWithQueue(hostName, "foo", "bar", "bar2");
        final List<String> path = Arrays.asList("*", hostName);

        final Object object = _controller.get(virtualHost.getBroker(),
                                              "queue",
                                              path,
                                              Collections.singletonMap(Queue.NAME, Arrays.asList("foo", "bar")));
        assertThat(object, is(notNullValue()));
        assertThat(object, is(instanceOf(Collection.class)));

        final Collection<?> data = (Collection<?>) object;
        assertThat(data.size(), is(equalTo(2)));
        final Iterator iterator = data.iterator();
        final Object o = iterator.next();
        final Object o2 = iterator.next();
        assertThat(o, is(notNullValue()));
        assertThat(o, is(instanceOf(Queue.class)));
        assertThat(((Queue) o).getName(), is(equalTo("foo")));

        assertThat(o2, is(notNullValue()));
        assertThat(o2, is(instanceOf(Queue.class)));
        assertThat(((Queue) o2).getName(), is(equalTo("bar")));
    }

    @Test
    public void createOrUpdateUsingPutAndFullPath() throws Exception
    {
        final String hostName = "test";
        final QueueManagingVirtualHost<?> virtualHost = createVirtualHostWithQueue(hostName);
        final List<String> path = Arrays.asList(virtualHost.getParent().getName(), hostName, "bar");

        final Object object = _controller.createOrUpdate(virtualHost.getBroker(),
                                                         "queue",
                                                         path,
                                                         new HashMap<>(Collections.singletonMap(Queue.NAME, "bar")),
                                                         false);

        assertThat(object, is(notNullValue()));
        assertThat(object, is(instanceOf(Queue.class)));
        assertThat(((Queue) object).getName(), is(equalTo("bar")));
    }

    @Test
    public void createOrUpdateUsingPostAndFullPathForNonExisting() throws Exception
    {
        final String hostName = "test";
        final QueueManagingVirtualHost<?> virtualHost = createVirtualHostWithQueue(hostName);
        final List<String> path = Arrays.asList(virtualHost.getParent().getName(), hostName, "bar");

        try
        {
            _controller.createOrUpdate(virtualHost.getBroker(),
                                       "queue",
                                       path,
                                       new HashMap<>(Collections.singletonMap(Queue.NAME, "bar")),
                                       true);
            fail("Post update should fail for non existing");
        }
        catch (ManagementException e)
        {
            assertThat(e.getStatusCode(), is(equalTo(404)));
        }
    }

    @Test
    public void createOrUpdateUsingPostAndFullPathForExisting() throws Exception
    {
        final String hostName = "test";
        final QueueManagingVirtualHost<?> virtualHost = createVirtualHostWithQueue(hostName, "bar");
        final List<String> path = Arrays.asList(virtualHost.getParent().getName(), hostName, "bar");

        final Object object = _controller.createOrUpdate(virtualHost.getBroker(),
                                                         "queue",
                                                         path,
                                                         new HashMap<>(Collections.singletonMap(Queue.NAME, "bar")),
                                                         true);

        assertThat(object, is(nullValue()));
    }

    @Test
    public void createOrUpdateUsingPostAndParentPath() throws Exception
    {
        final String hostName = "test";
        final QueueManagingVirtualHost<?> virtualHost = createVirtualHostWithQueue(hostName);
        final List<String> path = Arrays.asList(virtualHost.getParent().getName(), hostName);

        final Object object = _controller.createOrUpdate(virtualHost.getBroker(),
                                                         "queue",
                                                         path,
                                                         new HashMap<>(Collections.singletonMap(Queue.NAME, "bar")),
                                                         true);

        assertThat(object, is(notNullValue()));
        assertThat(object, is(instanceOf(Queue.class)));
        assertThat(((Queue) object).getName(), is(equalTo("bar")));
    }

    @Test
    public void deleteUsingFullPath() throws Exception
    {
        final String hostName = "test";
        final QueueManagingVirtualHost<?> virtualHost = createVirtualHostWithQueue(hostName, "foo", "bar");

        List<String> path = Arrays.asList(virtualHost.getParent().getName(), hostName, "bar");

        int count = _controller.delete(virtualHost.getBroker(), "queue", path, Collections.emptyMap());

        assertThat(count, is(equalTo(1)));
    }

    @Test
    public void deleteUsingFilter() throws Exception
    {
        final String hostName = "test";
        final QueueManagingVirtualHost<?> virtualHost = createVirtualHostWithQueue(hostName, "foo", "bar");

        List<String> path = Arrays.asList(virtualHost.getParent().getName(), hostName);

        int count = _controller.delete(virtualHost.getBroker(),
                                       "queue",
                                       path,
                                       Collections.singletonMap(Queue.NAME, Arrays.asList("foo", "bar", "bar2")));

        assertThat(count, is(equalTo(2)));
    }

    @Test
    public void deleteUsingWildcard() throws Exception
    {
        final String hostName = "test";
        final QueueManagingVirtualHost<?> virtualHost = createVirtualHostWithQueue(hostName, "foo", "bar");

        List<String> path = Arrays.asList(virtualHost.getParent().getName(), hostName, "*");

        int count = _controller.delete(virtualHost.getBroker(), "queue", path, Collections.emptyMap());

        assertThat(count, is(equalTo(2)));
    }

    @Test
    public void invoke() throws Exception
    {
        final String hostName = "test";
        final QueueManagingVirtualHost<?> virtualHost = createVirtualHostWithQueue(hostName, "foo", "bar");

        List<String> path = Arrays.asList(virtualHost.getParent().getName(), hostName);

        Map<String, Object> message = new HashMap<>();
        message.put("address", "foo");
        message.put("persistent", "false");
        message.put("content", "Test Content");
        message.put("mimeType", "text/plain");
        ManagementResponse response = _controller.invoke(virtualHost.getBroker(),
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
    }

    @Test
    public void getPreferences() throws Exception
    {
        final String hostName = "default";
        final QueueManagingVirtualHost<?> virtualHost = createVirtualHostWithQueue(hostName);
        final String preferencesType = "X-type-preference";
        final Map<String, Object> preferenceValue = Collections.singletonMap("foo", "bar");
        final Subject testSubject = createTestSubject();
        final String prefernceName = "test";
        createPreferences(testSubject, virtualHost, preferencesType, prefernceName, preferenceValue);

        List<String> path = Arrays.asList(virtualHost.getParent().getName(), hostName, "userpreferences");
        final Object preferences = Subject.doAs(testSubject, (PrivilegedAction<Object>) () ->
                _controller.getPreferences(virtualHost.getBroker(), "virtualhost", path, Collections.emptyMap()));

        assertPreference(preferencesType, prefernceName, preferenceValue, preferences);
    }


    @Test
    public void setPreferences() throws Exception
    {
        final String hostName = "default";
        final QueueManagingVirtualHost<?> virtualHost = createVirtualHostWithQueue(hostName);
        final String preferencesType = "X-type";
        final Map<String, Object> preferenceValue = Collections.singletonMap("foo", "bar");
        final Subject testSubject = createTestSubject();
        final String preferenceName = "pref";
        final UUID id = createPreferences(testSubject, virtualHost, preferencesType, preferenceName, preferenceValue);
        final List<String> path = Arrays.asList(virtualHost.getParent().getName(), hostName, "userpreferences");
        final Map<String, Object> newValue = Collections.singletonMap("foo", "bar2");
        final Map<String, Object> data = new HashMap<>();
        data.put("id", id.toString());
        data.put("name", preferenceName);
        data.put("value", newValue);
        final Map<String, List<Object>> modifiedPreferences = Collections.singletonMap(preferencesType,
                                                                                       Collections.singletonList(data));
        Subject.doAs(testSubject, (PrivilegedAction<Void>) () -> {
            _controller.setPreferences(virtualHost.getBroker(),
                                       "virtualhost",
                                       path,
                                       modifiedPreferences,
                                       Collections.emptyMap(),
                                       true);
            return null;
        });
        final Object preferences = Subject.doAs(testSubject, (PrivilegedAction<Object>) () ->
                _controller.getPreferences(virtualHost.getBroker(), "virtualhost", path, Collections.emptyMap()));

        assertPreference(preferencesType, preferenceName, newValue, preferences);
    }

    @Test
    public void deletePreferences() throws Exception
    {
        final String hostName = "test";
        final QueueManagingVirtualHost<?> virtualHost = createVirtualHostWithQueue(hostName);
        final String preferencesType = "X-type";
        final Map<String, Object> preferenceValue = Collections.singletonMap("foo", "bar");
        final Subject testSubject = createTestSubject();
        final String preferenceName = "pref";
        createPreferences(testSubject, virtualHost, preferencesType, preferenceName, preferenceValue);

        final List<String> path = Arrays.asList(virtualHost.getParent().getName(),
                                                hostName,
                                                "userpreferences",
                                                preferencesType,
                                                preferenceName);

        Subject.doAs(testSubject, (PrivilegedAction<Void>) () -> {
            _controller.deletePreferences(virtualHost.getBroker(),
                                          "virtualhost",
                                          path,
                                          Collections.emptyMap());
            return null;
        });

        final List<String> path2 = Arrays.asList(virtualHost.getParent().getName(), hostName, "userpreferences");

        final Object preferences = Subject.doAs(testSubject, (PrivilegedAction<Object>) () ->
                _controller.getPreferences(virtualHost.getBroker(), "virtualhost", path2, Collections.emptyMap()));
        assertThat(preferences, is(notNullValue()));
        assertThat(preferences, is(instanceOf(Map.class)));

        final Map<?, ?> map = (Map<?, ?>) preferences;
        assertThat(map.size(), is(equalTo(0)));
    }

    @Test
    public void formatConfiguredObjectForSingletonResponse() throws Exception
    {
        final String hostName = "test";
        final QueueManagingVirtualHost<?> virtualHost = createVirtualHostWithQueue(hostName, "foo", "bar");

        final Object formatted = _controller.formatConfiguredObject(virtualHost,
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

        assertThat(queueCollection.size(), is(equalTo(2)));
        final Iterator<?> iterator = queueCollection.iterator();
        final Object queue1 = iterator.next();
        final Object queue2 = iterator.next();

        assertThat(queue1, is(instanceOf(Map.class)));
        assertThat(queue2, is(instanceOf(Map.class)));

        final Map<?, ?> queueMap1 = (Map<?, ?>) queue1;
        final Map<?, ?> queueMap2 = (Map<?, ?>) queue2;

        assertThat(queueMap1.get(Queue.NAME), is(equalTo("bar")));
        assertThat(queueMap2.get(Queue.NAME), is(equalTo("foo")));
    }

    @Test
    public void formatConfiguredObjectForCollectionResponse() throws Exception
    {
        final String hostName = "test";
        final QueueManagingVirtualHost<?> virtualHost = createVirtualHostWithQueue(hostName, "foo", "bar");

        final Object formatted = _controller.formatConfiguredObject(Collections.singletonList(virtualHost),
                                                                    Collections.singletonMap("depth",
                                                                                             Collections.singletonList(
                                                                                                     "1")),
                                                                    true);
        assertThat(formatted, is(notNullValue()));
        assertThat(formatted, is(instanceOf(Collection.class)));

        final Collection<?> formattedCollection = (Collection<?>) formatted;
        assertThat(formattedCollection.size(), is(equalTo(1)));

        Object item = formattedCollection.iterator().next();
        assertThat(item, is(instanceOf(Map.class)));

        final Map<?, ?> data = (Map<?, ?>) item;
        assertThat(data.get(VirtualHost.NAME), is(equalTo(hostName)));
        final Object queues = data.get("queues");
        assertThat(queues, is(notNullValue()));
        assertThat(queues, is(instanceOf(Collection.class)));

        final Collection<?> queueCollection = (Collection<?>) queues;

        assertThat(queueCollection.size(), is(equalTo(2)));
        final Iterator<?> iterator = queueCollection.iterator();
        final Object queue1 = iterator.next();
        final Object queue2 = iterator.next();

        assertThat(queue1, is(instanceOf(Map.class)));
        assertThat(queue2, is(instanceOf(Map.class)));

        final Map<?, ?> queueMap1 = (Map<?, ?>) queue1;
        final Map<?, ?> queueMap2 = (Map<?, ?>) queue2;

        assertThat(queueMap1.get(Queue.NAME), is(equalTo("bar")));
        assertThat(queueMap2.get(Queue.NAME), is(equalTo("foo")));
    }

    @Test
    public void handleGetForBrokerRootAndQueueSingletonPath() throws Exception
    {
        final String hostName = "test";
        final QueueManagingVirtualHost<?> virtualHost = createVirtualHostWithQueue(hostName, "foo", "bar");

        final String nodeName = virtualHost.getParent().getName();
        final ManagementRequest request = mock(ManagementRequest.class);
        when(request.getCategory()).thenReturn("queue");
        doReturn(virtualHost.getBroker()).when(request).getRoot();
        when(request.getPath()).thenReturn(Arrays.asList(nodeName, hostName, "foo"));
        when(request.getMethod()).thenReturn("GET");

        final ManagementResponse response = _controller.handleGet(request);
        assertThat(response, is(notNullValue()));
        assertThat(response.getResponseCode(), is(equalTo(200)));
        assertThat(response.getBody(), is(notNullValue()));
        assertThat(response.getBody(), is(instanceOf(Queue.class)));

        final Queue data = (Queue) response.getBody();
        assertThat(data.getName(), is(equalTo("foo")));
    }


    @Test
    public void handleGetForBrokerRootAndQueuePathWithoutQueueName() throws Exception
    {
        final String hostName = "test";
        final QueueManagingVirtualHost<?> virtualHost = createVirtualHostWithQueue(hostName, "foo", "bar");

        final String nodeName = virtualHost.getParent().getName();
        final ManagementRequest request = mock(ManagementRequest.class);
        when(request.getCategory()).thenReturn("queue");
        doReturn(virtualHost.getBroker()).when(request).getRoot();
        when(request.getPath()).thenReturn(Arrays.asList(nodeName, hostName));
        when(request.getParameters()).thenReturn(Collections.emptyMap());
        when(request.getMethod()).thenReturn("GET");

        final ManagementResponse response = _controller.handleGet(request);
        assertThat(response, is(notNullValue()));
        assertThat(response.getResponseCode(), is(equalTo(200)));
        assertThat(response.getBody(), is(notNullValue()));
        assertThat(response.getBody(), is(instanceOf(Collection.class)));

        final Collection data = (Collection) response.getBody();
        assertThat(data.size(), is(equalTo(2)));

        final Iterator iterator = data.iterator();
        final Object object = iterator.next();
        final Object object2 = iterator.next();
        assertThat(object, is(notNullValue()));
        assertThat(object, is(instanceOf(Queue.class)));
        assertThat(((Queue) object).getName(), is(equalTo("foo")));

        assertThat(object2, is(notNullValue()));
        assertThat(object2, is(instanceOf(Queue.class)));
        assertThat(((Queue) object2).getName(), is(equalTo("bar")));
    }

    @Test
    public void handleGetForBrokerRootAndQueuePathWithFilter() throws Exception
    {
        final String hostName = "test";
        final QueueManagingVirtualHost<?> virtualHost = createVirtualHostWithQueue(hostName, "foo", "bar");

        final String nodeName = virtualHost.getParent().getName();
        final ManagementRequest request = mock(ManagementRequest.class);
        when(request.getCategory()).thenReturn("queue");
        doReturn(virtualHost.getBroker()).when(request).getRoot();
        when(request.getPath()).thenReturn(Arrays.asList(nodeName, hostName));
        when(request.getParameters()).thenReturn(Collections.singletonMap("name", Collections.singletonList("bar")));
        when(request.getMethod()).thenReturn("GET");

        ManagementResponse response = _controller.handleGet(request);
        assertThat(response, is(notNullValue()));
        assertThat(response.getResponseCode(), is(equalTo(200)));
        assertThat(response.getBody(), is(notNullValue()));
        assertThat(response.getBody(), is(instanceOf(Collection.class)));

        Collection data = (Collection) response.getBody();
        assertThat(data.size(), is(equalTo(1)));

        Object object = data.iterator().next();
        assertThat(object, is(notNullValue()));
        assertThat(object, is(instanceOf(Queue.class)));
        assertThat(((Queue) object).getName(), is(equalTo("bar")));
    }

    private QueueManagingVirtualHost<?> createVirtualHostWithQueue(final String hostName, String... queueName)
            throws Exception
    {
        final QueueManagingVirtualHost<?> virtualHost = BrokerTestHelper.createVirtualHost(hostName, this);
        final Broker root = virtualHost.getBroker();
        final ConfiguredObject<?> virtualHostNode = virtualHost.getParent();
        when(root.getChildren(VirtualHostNode.class)).thenReturn(Collections.singletonList(virtualHostNode));
        when(virtualHostNode.getChildren(VirtualHost.class)).thenReturn(Collections.singletonList(virtualHost));
        when(virtualHostNode.getChildByName(VirtualHost.class, hostName)).thenReturn(virtualHost);
        Stream.of(queueName)
              .forEach(n -> virtualHost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, n)));
        return virtualHost;
    }


    private UUID createPreferences(final Subject testSubject,
                                   final QueueManagingVirtualHost<?> virtualHost,
                                   final String preferenceType,
                                   final String preferenceName,
                                   final Map<String, Object> preferenceValue)
            throws Exception
    {
        UUID uuid = UUID.randomUUID();
        final Preference preference = new PreferenceImpl(virtualHost,
                                                         uuid,
                                                         preferenceName,
                                                         preferenceType,
                                                         "Some preference",
                                                         null,
                                                         new Date(),
                                                         new Date(),
                                                         null,
                                                         new GenericPreferenceValueFactory().createInstance(
                                                                 preferenceValue));
        final List<Preference> preferenceList = Collections.singletonList(preference);
        final Future<Void> result = Subject.doAs(testSubject,
                                                 (PrivilegedAction<Future<Void>>) () -> virtualHost.getUserPreferences()
                                                                                                   .updateOrAppend(
                                                                                                           preferenceList));

        result.get(2000L, TimeUnit.MILLISECONDS);
        return uuid;
    }

    private Subject createTestSubject()
    {
        final AuthenticationProvider<?> provider = mock(AuthenticationProvider.class);
        when(provider.getType()).thenReturn("type");
        when(provider.getName()).thenReturn("name");

        return new Subject(false,
                           Collections.singleton(new AuthenticatedPrincipal(new UsernamePrincipal("user", provider))),
                           Collections.emptySet(),
                           Collections.emptySet());
    }

    private void assertPreference(final String expectedType,
                                  final String expectedName,
                                  final Map<String, Object> expectedValue,
                                  final Object preferences)
    {
        assertThat(preferences, is(notNullValue()));
        assertThat(preferences, is(instanceOf(Map.class)));

        final Map<?, ?> data = (Map<?, ?>) preferences;

        final Object pt = data.get(expectedType);
        assertThat(pt, is(notNullValue()));
        assertThat(pt, is(instanceOf(Collection.class)));

        final Collection<?> items = (Collection<?>) pt;
        assertThat(items.size(), is(equalTo(1)));

        final Object item = items.iterator().next();
        assertThat(item, is(notNullValue()));
        assertThat(item, is(instanceOf(Map.class)));

        final Map<?, ?> map = (Map<?, ?>) item;

        final Object value = map.get("value");
        assertThat(value, is(notNullValue()));
        assertThat(value, is(equalTo(expectedValue)));

        final Object name = map.get("name");
        assertThat(name, is(notNullValue()));
        assertThat(name, is(equalTo(expectedName)));
    }
}