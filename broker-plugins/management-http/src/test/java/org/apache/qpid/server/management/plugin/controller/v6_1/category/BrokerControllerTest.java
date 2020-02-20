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
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.management.plugin.controller.LegacyConfiguredObject;
import org.apache.qpid.server.management.plugin.controller.LegacyManagementController;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.test.utils.UnitTestBase;

public class BrokerControllerTest extends UnitTestBase
{
    private LegacyManagementController _legacyVersionManagementController;
    private String MODEL_VERSION = "6.1";

    @Before
    public void setUp()
    {
        _legacyVersionManagementController = mock(LegacyManagementController.class);
        when(_legacyVersionManagementController.getVersion()).thenReturn(MODEL_VERSION);
    }

    @Test
    public void convertNextVersionLegacyConfiguredObject()
    {
        final LegacyConfiguredObject object = mock(LegacyConfiguredObject.class);
        when(object.getAttribute("modelVersion")).thenReturn("foo");
        final Map<String, String> context = new HashMap<>();
        context.put("qpid.port.sessionCountLimit", "512");
        context.put("qpid.port.heartbeatDelay", "10000");
        context.put("qpid.port.closeWhenNoRoute", "true");
        when(object.getAttribute("context")).thenReturn(context);

        final BrokerController controller =
                new BrokerController(_legacyVersionManagementController, Collections.emptySet());
        assertThat(controller.getCategory(), is(equalTo("Broker")));

        final LegacyConfiguredObject converted = controller.convertFromNextVersion(object);
        assertThat(converted.getAttribute("modelVersion"), is(equalTo(MODEL_VERSION)));
        assertThat(converted.getAttribute("connection.sessionCountLimit"), is(equalTo(512)));
        assertThat(converted.getAttribute("connection.heartBeatDelay"), is(equalTo(10000L)));
        assertThat(converted.getAttribute("connection.closeWhenNoRoute"), is(equalTo(true)));
    }


    @Test
    public void convertAttributesToNextVersion()
    {
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put("connection.sessionCountLimit", 512);
        attributes.put("connection.heartBeatDelay", 10000L);
        attributes.put("connection.closeWhenNoRoute", true);
        attributes.put("statisticsReportingResetEnabled", true);
        attributes.put("statisticsReportingEnabled", true);

        final BrokerController controller =
                new BrokerController(_legacyVersionManagementController, Collections.emptySet());
        assertThat(controller.getCategory(), is(equalTo("Broker")));

        final Map<String, Object> converted = controller.convertAttributesToNextVersion(mock(ConfiguredObject.class),
                                                                                        Collections.emptyList(),
                                                                                        attributes);
        assertThat(converted.size(), is(equalTo(2)));
        assertThat(converted.get("statisticsReportingEnabled"), is(equalTo(true)));

        final Map<String, String> expectedContext = new HashMap<>();
        expectedContext.put("qpid.port.sessionCountLimit", "512");
        expectedContext.put("qpid.port.heartbeatDelay", "10000");
        expectedContext.put("qpid.port.closeWhenNoRoute", "true");
        assertThat(converted.get("context"), is(equalTo(expectedContext)));
    }
}