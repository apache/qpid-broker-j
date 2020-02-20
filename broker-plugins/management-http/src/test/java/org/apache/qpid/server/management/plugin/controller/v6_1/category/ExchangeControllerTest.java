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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.management.plugin.ManagementController;
import org.apache.qpid.server.management.plugin.controller.LegacyConfiguredObject;
import org.apache.qpid.server.management.plugin.controller.LegacyManagementController;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.AlternateBinding;
import org.apache.qpid.server.model.Binding;
import org.apache.qpid.test.utils.UnitTestBase;

public class ExchangeControllerTest extends UnitTestBase
{
    private LegacyManagementController _legacyManagementController;

    @Before
    public void setUp()
    {
        _legacyManagementController = mock(LegacyManagementController.class);
        final ManagementController nextVersionManagementController = mock(ManagementController.class);
        when(_legacyManagementController.getNextVersionManagementController()).thenReturn(
                nextVersionManagementController);
    }

    @Test
    public void convertNextVersionLegacyConfiguredObject()
    {
        final ExchangeController exchangeController =
                new ExchangeController(_legacyManagementController, Collections.emptySet());

        final String exchangeName = "testExchange";
        final String alternateExchangeName = "altExchange";
        final String queueName = "testQueue";
        final String bindingKey = "testBindingKey";

        final LegacyConfiguredObject nextVersionExchange = mock(LegacyConfiguredObject.class);
        final AlternateBinding alternateBinding = mock(AlternateBinding.class);
        final Binding nextVersionBinding = mock(Binding.class);
        final LegacyConfiguredObject nextVersionAlternateExchange = mock(LegacyConfiguredObject.class);
        final LegacyConfiguredObject nextVersionVirtualHost = mock(LegacyConfiguredObject.class);
        final LegacyConfiguredObject queue = mock(LegacyConfiguredObject.class);

        when(alternateBinding.getDestination()).thenReturn(alternateExchangeName);

        when(nextVersionExchange.getCategory()).thenReturn(ExchangeController.TYPE);
        when(nextVersionExchange.getAttribute("alternateBinding")).thenReturn(alternateBinding);
        when(nextVersionExchange.getAttribute(AbstractConfiguredObject.NAME)).thenReturn(exchangeName);
        when(nextVersionExchange.getAttribute("bindings")).thenReturn(Collections.singletonList(nextVersionBinding));
        when(nextVersionExchange.getParent(VirtualHostController.TYPE)).thenReturn(nextVersionVirtualHost);

        when(nextVersionBinding.getDestination()).thenReturn(queueName);
        when(nextVersionBinding.getBindingKey()).thenReturn(bindingKey);

        when(nextVersionAlternateExchange.getCategory()).thenReturn(ExchangeController.TYPE);
        when(nextVersionAlternateExchange.getAttribute(LegacyConfiguredObject.NAME)).thenReturn(alternateExchangeName);

        when(nextVersionVirtualHost.getChildren(ExchangeController.TYPE)).thenReturn(Arrays.asList(nextVersionExchange,
                nextVersionAlternateExchange));
        when(nextVersionVirtualHost.getChildren(QueueController.TYPE)).thenReturn(Collections.singletonList(queue));

        final LegacyConfiguredObject convertedExchange = mock(LegacyConfiguredObject.class);
        final LegacyConfiguredObject convertedAltExchange = mock(LegacyConfiguredObject.class);
        final LegacyConfiguredObject convertedQueue = mock(LegacyConfiguredObject.class);
        when(_legacyManagementController.convertFromNextVersion(nextVersionExchange)).thenReturn(convertedExchange);
        when(_legacyManagementController.convertFromNextVersion(nextVersionAlternateExchange)).thenReturn(convertedAltExchange);
        when(_legacyManagementController.convertFromNextVersion(queue)).thenReturn(convertedQueue);

        final LegacyConfiguredObject destination = exchangeController.convertFromNextVersion(nextVersionExchange);

        assertThat(destination.getAttribute("alternateExchange"), is(equalTo(convertedAltExchange)));

        final Collection<LegacyConfiguredObject> children = destination.getChildren(BindingController.TYPE);
        assertThat(children.size(), is(equalTo(1)));

        final LegacyConfiguredObject o = children.iterator().next();
        assertThat(o.getCategory(), is(equalTo(BindingController.TYPE)));
        assertThat(o.getAttribute(AbstractConfiguredObject.NAME), is(equalTo(bindingKey)));
        assertThat(o.getAttribute("queue"), is(equalTo(convertedQueue)));
        assertThat(o.getAttribute("exchange"), is(equalTo(convertedExchange)));
    }
}