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

import java.text.NumberFormat;
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
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.AlternateBinding;
import org.apache.qpid.server.model.Binding;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.test.utils.UnitTestBase;

public class QueueControllerTest extends UnitTestBase
{
    private LegacyManagementController _legacyVersionManagementController;
    private QueueController _queueController;

    @Before
    public void setUp()
    {
        _legacyVersionManagementController = mock(LegacyManagementController.class);
        final ManagementController nextVersionManagementController = mock(ManagementController.class);
        when(_legacyVersionManagementController.getNextVersionManagementController()).thenReturn(
                nextVersionManagementController);
        _queueController = new QueueController(_legacyVersionManagementController, Collections.emptySet());
    }

    @Test
    public void convertNextVersionLegacyConfiguredObject()
    {
        final String exchangeName = "testExchange";
        final String alternateExchangeName = "altExchange";
        final String queueName = "testQueue";
        final String bindingKey = "testBindingKey";

        final LegacyConfiguredObject nextVersionQueue = mock(LegacyConfiguredObject.class);
        final Binding nextVersionBinding = mock(Binding.class);
        final LegacyConfiguredObject nextVersionVirtualHost = mock(LegacyConfiguredObject.class);
        final LegacyConfiguredObject nextVersionAlternateExchange = mock(LegacyConfiguredObject.class);
        final LegacyConfiguredObject nextVersionExchange = mock(LegacyConfiguredObject.class);

        final AlternateBinding alternateDestination = mock(AlternateBinding.class);
        when(alternateDestination.getDestination()).thenReturn(alternateExchangeName);

        when(nextVersionQueue.getCategory()).thenReturn(QueueController.TYPE);
        when(nextVersionQueue.getParent(VirtualHostController.TYPE)).thenReturn(nextVersionVirtualHost);
        when(nextVersionQueue.getAttribute("alternateBinding")).thenReturn(alternateDestination);
        when(nextVersionQueue.getAttribute(AbstractConfiguredObject.NAME)).thenReturn(queueName);
        when(nextVersionQueue.getAttribute("overflowPolicy")).thenReturn("PRODUCER_FLOW_CONTROL");
        when(nextVersionQueue.getAttribute("maximumQueueDepthBytes")).thenReturn(10000L);
        when(nextVersionQueue.getAttribute("context")).thenReturn(Collections.singletonMap("queue.queueFlowResumeLimit", "70"));
        when(nextVersionQueue.getAttribute("messageGroupType")).thenReturn("SHARED_GROUPS");
        when(nextVersionQueue.getAttribute("messageGroupKeyOverride")).thenReturn("test");

        when(nextVersionBinding.getDestination()).thenReturn(queueName);
        when(nextVersionBinding.getBindingKey()).thenReturn(bindingKey);

        when(nextVersionExchange.getAttribute(AbstractConfiguredObject.NAME)).thenReturn(exchangeName);
        when(nextVersionExchange.getCategory()).thenReturn(ExchangeController.TYPE);
        when(nextVersionExchange.getAttribute("bindings")).thenReturn(Collections.singletonList(nextVersionBinding));

        when(nextVersionAlternateExchange.getCategory()).thenReturn(ExchangeController.TYPE);
        when(nextVersionAlternateExchange.getCategory()).thenReturn(ExchangeController.TYPE);
        when(nextVersionAlternateExchange.getAttribute(LegacyConfiguredObject.NAME)).thenReturn(alternateExchangeName);

        when(nextVersionVirtualHost.getChildren(ExchangeController.TYPE)).thenReturn(Arrays.asList(nextVersionExchange, nextVersionAlternateExchange));
        when(nextVersionVirtualHost.getChildren(QueueController.TYPE)).thenReturn(Collections.singletonList(nextVersionExchange));

        final LegacyConfiguredObject convertedExchange = mock(LegacyConfiguredObject.class);
        final LegacyConfiguredObject convertedAltExchange = mock(LegacyConfiguredObject.class);
        final LegacyConfiguredObject convertedQueue = mock(LegacyConfiguredObject.class);
        when(_legacyVersionManagementController.convertFromNextVersion(nextVersionQueue)).thenReturn(
                convertedQueue);
        when(_legacyVersionManagementController.convertFromNextVersion(nextVersionAlternateExchange)).thenReturn(
                convertedAltExchange);
        when(_legacyVersionManagementController.convertFromNextVersion(nextVersionExchange)).thenReturn(convertedExchange);

        final LegacyConfiguredObject destination = _queueController.convertFromNextVersion(nextVersionQueue);

        assertThat(destination.getAttribute("alternateExchange"), is(equalTo(convertedAltExchange)));
        assertThat(destination.getAttribute("queueFlowControlSizeBytes"), is(equalTo(10000L)));
        assertThat(destination.getAttribute("queueFlowResumeSizeBytes"), is(equalTo(7000L)));
        assertThat(destination.getAttribute("messageGroupSharedGroups"), is(equalTo(true)));
        assertThat(destination.getAttribute("messageGroupKey"), is(equalTo("test")));

        final Collection<LegacyConfiguredObject> children = destination.getChildren(BindingController.TYPE);
        assertThat(children.size(), is(equalTo(1)));

        final LegacyConfiguredObject o = children.iterator().next();
        assertThat(o.getCategory(), is(equalTo(BindingController.TYPE)));
        assertThat(o.getAttribute(AbstractConfiguredObject.NAME), is(equalTo(bindingKey)));
        assertThat(o.getAttribute("queue"), is(equalTo(convertedQueue)));
        assertThat(o.getAttribute("exchange"), is(equalTo(convertedExchange)));
    }

    @Test
    public void convertAttributesToNextVersion()
    {
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put("queueFlowResumeSizeBytes", 7000L);
        attributes.put("queueFlowControlSizeBytes", 10000L);
        attributes.put("messageGroupSharedGroups", true);
        attributes.put("messageGroupKey", "groupKey");
        attributes.put("name", "testQueue");


        final ConfiguredObject<?> root = mock(ConfiguredObject.class);
        final List path = Arrays.asList("my-vhn", "my-vh", "testQueue");
        final Map<String, Object> converted = _queueController.convertAttributesToNextVersion(root, path, attributes);

        assertThat(converted, is(notNullValue()));
        assertThat(converted.get("overflowPolicy"), is(equalTo("PRODUCER_FLOW_CONTROL")));
        assertThat(converted.get("maximumQueueDepthBytes"), is(equalTo(10000L)));
        assertThat(converted.get("messageGroupType"), is(equalTo("SHARED_GROUPS")));
        assertThat(converted.get("messageGroupKeyOverride"), is(equalTo("groupKey")));
        assertThat(converted.get("name"), is(equalTo("testQueue")));

        final Object contextObject = converted.get("context");
        assertThat(contextObject, is(instanceOf(Map.class)));

        final Map<?,?> context =(Map<?,?>)contextObject;
        NumberFormat formatter = NumberFormat.getInstance();
        formatter.setMinimumFractionDigits(2);
        assertThat(context.get("queue.queueFlowResumeLimit"), is(equalTo(formatter.format(70L))));
    }
}
