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

public class VirtualHostControllerTest extends UnitTestBase
{
    private VirtualHostController _virtualHostController;

    @Before
    public void setUp()
    {
        final LegacyManagementController legacyManagementController = mock(LegacyManagementController.class);
        final ManagementController nextVersionManagementController = mock(ManagementController.class);
        when(legacyManagementController.getNextVersionManagementController()).thenReturn(
                nextVersionManagementController);
        when(legacyManagementController.getVersion()).thenReturn("6.1");
        _virtualHostController = new VirtualHostController(legacyManagementController, Collections.emptySet());
    }

    @Test
    public void convertNextVersionLegacyConfiguredObject()
    {
        final LegacyConfiguredObject nextVersionVirtualHost = mock(LegacyConfiguredObject.class);
        when(nextVersionVirtualHost.getAttribute(LegacyConfiguredObject.NAME)).thenReturn("test");

        final LegacyConfiguredObject converted =
                _virtualHostController.convertNextVersionLegacyConfiguredObject(nextVersionVirtualHost);

        assertThat(converted, is(notNullValue()));
        assertThat(converted.getCategory(), is(equalTo(VirtualHostController.TYPE)));
        assertThat(converted.getAttribute(LegacyConfiguredObject.NAME), is(equalTo("test")));
        assertThat(converted.getAttribute("modelVersion"), is(equalTo("6.1")));
        assertThat(converted.getAttribute("queue_deadLetterQueueEnabled"), is(equalTo(false)));
    }

    @Test
    public void convertAttributesToNextVersion()
    {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(LegacyConfiguredObject.NAME, "test");
        attributes.put("queue_deadLetterQueueEnabled", true);
        Map<String, String> context = new HashMap<>();
        context.put("queue.deadLetterQueueEnabled", "true");
        context.put("virtualhost.housekeepingCheckPeriod", "30");
        attributes.put(LegacyConfiguredObject.CONTEXT, context);

        final ConfiguredObject<?> root = mock(ConfiguredObject.class);
        final List<String> path = Collections.singletonList("test-vhn");
        Map<String, Object> converted = _virtualHostController.convertAttributesToNextVersion(root, path, attributes);

        assertThat(converted, is(notNullValue()));
        assertThat(converted.get(LegacyConfiguredObject.NAME), is(equalTo("test")));

        assertThat(converted.containsKey("queue_deadLetterQueueEnabled"), is(equalTo(false)));

        Object contextObject = converted.get(LegacyConfiguredObject.CONTEXT);
        assertThat(converted, is(instanceOf(Map.class)));

        Map<?,?> convertedContext = (Map<?,?>)contextObject;

        assertThat(convertedContext.get("virtualhost.housekeepingCheckPeriod"), is(equalTo("30")));
        assertThat(convertedContext.containsKey("queue.deadLetterQueueEnabled"), is(equalTo(false)));
    }
}