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
package org.apache.qpid.server.management.plugin.controller.v6_1;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.management.plugin.ManagementController;
import org.apache.qpid.server.management.plugin.controller.CategoryController;
import org.apache.qpid.server.management.plugin.controller.CategoryControllerFactory;
import org.apache.qpid.server.management.plugin.controller.LegacyConfiguredObject;
import org.apache.qpid.server.management.plugin.controller.TypeController;
import org.apache.qpid.server.management.plugin.controller.TypeControllerFactory;
import org.apache.qpid.server.management.plugin.controller.v6_1.category.BrokerController;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.test.utils.UnitTestBase;

public class LegacyManagementControllerTest extends UnitTestBase
{

    private LegacyManagementController _controller;
    private ManagementController _nextVersionManagementController;

    @Before
    public void setUp()
    {
        _nextVersionManagementController = mock(ManagementController.class);
        _controller = new LegacyManagementController(_nextVersionManagementController);
        _controller.initialize();
    }

    @Test
    public void convertQueryParameters()
    {
        final Map<String, List<String>> parameters = Collections.singletonMap("actuals", Collections.singletonList("true"));
        final Map<String, List<String>> converted = _controller.convertQueryParameters(parameters);
        assertThat(converted, is(notNullValue()));
        assertThat(converted.get("excludeInheritedContext"), is(equalTo(Collections.singletonList("true"))));
    }

    @Test
    public void formatConfiguredObject()
    {
        final String objectName = "test-object";
        final String hostName = "test-vhn";
        final Map<String, List<String>> parameters = Collections.emptyMap();
        final LegacyConfiguredObject object = mock(LegacyConfiguredObject.class);
        final LegacyConfiguredObject vhn = mock(LegacyConfiguredObject.class);
        when(object.getAttributeNames()).thenReturn(Arrays.asList(LegacyConfiguredObject.NAME,
                                                                  LegacyConfiguredObject.TYPE));
        when(object.getAttribute(LegacyConfiguredObject.NAME)).thenReturn(objectName);
        when(object.getAttribute(LegacyConfiguredObject.TYPE)).thenReturn("Broker");
        when(object.getCategory()).thenReturn("Broker");
        when(object.getChildren("VirtualHostNode")).thenReturn(Collections.singletonList(vhn));
        when(vhn.getAttributeNames()).thenReturn(Arrays.asList(LegacyConfiguredObject.NAME,
                                                               LegacyConfiguredObject.TYPE));
        when(vhn.getAttribute(LegacyConfiguredObject.NAME)).thenReturn(hostName);
        when(vhn.getAttribute(LegacyConfiguredObject.TYPE)).thenReturn("VirtualHostNode");
        when(vhn.getCategory()).thenReturn("VirtualHostNode");

        Object data = _controller.formatConfiguredObject(object, parameters, true);

        assertThat(data, is(instanceOf(Map.class)));
        Map<?, ?> formatted = (Map<?, ?>) data;

        assertThat(formatted.get(LegacyConfiguredObject.NAME), is(equalTo(objectName)));
        assertThat(formatted.get(LegacyConfiguredObject.TYPE), is(equalTo("Broker")));

        Object vhns = formatted.get("virtualhostnodes");
        assertThat(vhns, is(instanceOf(Collection.class)));

        Collection<?> nodes = (Collection<?>)vhns;

        assertThat(nodes.size(), is(equalTo(1)));

        Object node = nodes.iterator().next();
        assertThat(node, is(instanceOf(Map.class)));
        Map<?, ?> formattedNode = (Map<?, ?>) node;

        assertThat(formattedNode.get(LegacyConfiguredObject.NAME), is(equalTo(hostName)));
        assertThat(formattedNode.get(LegacyConfiguredObject.TYPE), is(equalTo("VirtualHostNode")));
    }

    @Test
    public void get()
    {
        final List<String> path = Collections.emptyList();
        final LegacyConfiguredObject object = mock(LegacyConfiguredObject.class);
        final Map<String, List<String>> parameters = Collections.emptyMap();

        final ConfiguredObject<?> root = mock(ConfiguredObject.class);
        when(_nextVersionManagementController.get(eq(root), eq(BrokerController.TYPE), eq(path), any())).thenReturn(object);
        final Object result = _controller.get(root, BrokerController.TYPE, path, parameters);
        assertThat(result, is(instanceOf(Collection.class)));
        Collection data = (Collection) result;
        assertThat(data.size(), is(equalTo(1)));
        Object obj = data.iterator().next();
        assertThat(obj, is(instanceOf(LegacyConfiguredObject.class)));

        assertThat(((LegacyConfiguredObject)obj).getCategory(), is(equalTo(BrokerController.TYPE)));
    }

}