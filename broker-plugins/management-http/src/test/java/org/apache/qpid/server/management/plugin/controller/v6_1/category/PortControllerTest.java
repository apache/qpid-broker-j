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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.management.plugin.ManagementController;
import org.apache.qpid.server.management.plugin.controller.LegacyConfiguredObject;
import org.apache.qpid.server.management.plugin.controller.LegacyManagementController;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.test.utils.UnitTestBase;

public class PortControllerTest extends UnitTestBase
{
    private PortController _portController;

    @Before
    public void setUp()
    {
        final LegacyManagementController legacyManagementController = mock(LegacyManagementController.class);
        final ManagementController nextVersionManagementController = mock(ManagementController.class);
        when(legacyManagementController.getNextVersionManagementController()).thenReturn(
                nextVersionManagementController);
        _portController = new PortController(legacyManagementController, Collections.emptySet());
    }

    @Test
    public void convertNextVersionLegacyConfiguredObject()
    {
        final LegacyConfiguredObject nextVersionPort = mock(LegacyConfiguredObject.class);
        when(nextVersionPort.getAttribute(LegacyConfiguredObject.NAME)).thenReturn("test");
        when(nextVersionPort.getAttribute(LegacyConfiguredObject.TYPE)).thenReturn("HTTP");
        Map<String, String> context = new HashMap<>();
        context.put("qpid.port.http.acceptBacklog", "2000");
        when(nextVersionPort.getAttribute(LegacyConfiguredObject.CONTEXT)).thenReturn(context);

        final LegacyConfiguredObject converted =
                _portController.convertNextVersionLegacyConfiguredObject(nextVersionPort);

        assertThat(converted, is(notNullValue()));
        assertThat(converted.getCategory(), is(equalTo(PortController.TYPE)));
        assertThat(converted.getAttribute(LegacyConfiguredObject.NAME), is(equalTo("test")));
        assertThat(converted.getAttribute(LegacyConfiguredObject.TYPE), is(equalTo("HTTP")));

        Object contextObject = converted.getAttribute(LegacyConfiguredObject.CONTEXT);
        assertThat(contextObject, is(instanceOf(Map.class)));

        Map<?,?> convertedContext = (Map<?,?>)contextObject;
        assertThat(convertedContext.get("port.http.maximumQueuedRequests"), is(equalTo("2000")));
    }

    @Test
    public void convertAttributesToNextVersion()
    {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(LegacyConfiguredObject.NAME, "test");
        attributes.put(LegacyConfiguredObject.TYPE, "HTTP");
        Map<String, String> context = new HashMap<>();
        context.put("port.http.maximumQueuedRequests", "2000");
        context.put("port.http.additionalInternalThreads", "10");
        attributes.put(LegacyConfiguredObject.CONTEXT, context);

        final ConfiguredObject<?> root = mock(ConfiguredObject.class);
        final List<String> path = Collections.emptyList();
        final Map<String, Object> converted = _portController.convertAttributesToNextVersion(root, path, attributes);

        assertThat(converted, is(instanceOf(Map.class)));
        assertThat(converted.get(LegacyConfiguredObject.NAME), is(equalTo("test")));
        assertThat(converted.get(LegacyConfiguredObject.TYPE), is(equalTo("HTTP")));

        Object contextObject = converted.get(LegacyConfiguredObject.CONTEXT);
        assertThat(converted, is(instanceOf(Map.class)));

        Map<?,?> convertedContext = (Map<?,?>)contextObject;

        assertThat(convertedContext.get("qpid.port.http.acceptBacklog"), is(equalTo("2000")));
        assertThat(convertedContext.containsKey("port.http.additionalInternalThreads"), is(equalTo(false)));


    }
}