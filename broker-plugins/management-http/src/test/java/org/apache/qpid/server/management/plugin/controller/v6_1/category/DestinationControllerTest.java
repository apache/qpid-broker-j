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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.management.plugin.ManagementController;
import org.apache.qpid.server.management.plugin.controller.LegacyConfiguredObject;
import org.apache.qpid.server.management.plugin.controller.LegacyManagementController;
import org.apache.qpid.server.model.AlternateBinding;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.test.utils.UnitTestBase;

public class DestinationControllerTest extends UnitTestBase
{
    private LegacyManagementController _legacyVersionManagementController;
    private ManagementController _nextVersionManagementController;

    @Before
    public void setUp()
    {
        _legacyVersionManagementController = mock(LegacyManagementController.class);
        _nextVersionManagementController = mock(ManagementController.class);
        when(_legacyVersionManagementController.getNextVersionManagementController()).thenReturn(
                _nextVersionManagementController);
    }

    @Test
    public void convertAttributesToNextVersion()
    {
        final String alternateExchangeName = "alternate";
        final String queueName = "test";
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put("alternateExchange", alternateExchangeName);
        attributes.put(LegacyConfiguredObject.NAME, queueName);

        final DestinationController controller =
                new DestinationController(_legacyVersionManagementController,
                                          "Queue",
                                          new String[]{"VirtualHost"},
                                          null,
                                          Collections.emptySet());
        assertThat(controller.getCategory(), is(equalTo("Queue")));
        assertThat(controller.getParentCategories(), is(equalTo(new String[]{"VirtualHost"})));

        final ConfiguredObject root = mock(ConfiguredObject.class);
        final List<String> exchangePath = Arrays.asList("vhn", "vh");
        final LegacyConfiguredObject exchange = mock(LegacyConfiguredObject.class);
        when(exchange.getAttribute(LegacyConfiguredObject.NAME)).thenReturn(alternateExchangeName);
        when(exchange.getCategory()).thenReturn(ExchangeController.TYPE);
        final Collection<LegacyConfiguredObject> exchanges = Collections.singletonList(exchange);
        when(_nextVersionManagementController.get(eq(root),
                                                  eq(ExchangeController.TYPE),
                                                  eq(exchangePath),
                                                  eq(Collections.emptyMap()))).thenReturn(exchanges);

        final Collection<String> hierarchy = Arrays.asList("virtualhostnode", "virtualhost", "exchange");
        when(_nextVersionManagementController.getCategoryHierarchy(eq(root), eq(ExchangeController.TYPE))).thenReturn(hierarchy);
        final List<String> path = Arrays.asList("vhn", "vh", queueName);
        final Map<String, Object> converted = controller.convertAttributesToNextVersion(root,
                                                                                        path,
                                                                                        attributes);
        assertThat(converted.size(), is(equalTo(2)));
        assertThat(converted.get(LegacyConfiguredObject.NAME), is(equalTo(queueName)));
        final Object alternateBinding = converted.get("alternateBinding");
        assertThat(alternateBinding, is(notNullValue()));
        assertThat(alternateBinding, is(instanceOf(Map.class)));

        final Map<?, ?> alternateDestination = (Map<?, ?>) alternateBinding;
        assertThat(alternateDestination.get("destination"), is(equalTo(alternateExchangeName)));
        assertThat(alternateDestination.get("attributes"), is(equalTo(null)));
    }

    @Test
    public void testLegacyDestination()
    {
        final String alternateExchangeName = "alt";
        final LegacyConfiguredObject nextVersionDestination = mock(LegacyConfiguredObject.class);
        final AlternateBinding alternateDestination = mock(AlternateBinding.class);
        when(alternateDestination.getDestination()).thenReturn(alternateExchangeName);
        when(nextVersionDestination.getAttribute("alternateBinding")).thenReturn(alternateDestination);

        final LegacyConfiguredObject vh = mock(LegacyConfiguredObject.class);
        when(nextVersionDestination.getParent(VirtualHostController.TYPE)).thenReturn(vh);
        final LegacyConfiguredObject alternateExchange = mock(LegacyConfiguredObject.class);
        when(alternateExchange.getCategory()).thenReturn(ExchangeController.TYPE);
        when(alternateExchange.getAttribute(LegacyConfiguredObject.NAME)).thenReturn(alternateExchangeName);
        final Collection<LegacyConfiguredObject> exchanges = Collections.singletonList(alternateExchange);
        when(vh.getChildren(ExchangeController.TYPE)).thenReturn(exchanges);

        final LegacyConfiguredObject converted = mock(LegacyConfiguredObject.class);
        when(_legacyVersionManagementController.convertFromNextVersion(alternateExchange)).thenReturn(converted);

        DestinationController.LegacyDestination destination = new DestinationController.LegacyDestination(_legacyVersionManagementController, nextVersionDestination, "Queue");


        assertThat(destination.getAttribute("alternateExchange"), is(equalTo(converted)));
    }
}