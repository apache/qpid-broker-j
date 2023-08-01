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
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.management.plugin.ManagementController;
import org.apache.qpid.server.management.plugin.controller.LegacyConfiguredObject;
import org.apache.qpid.server.management.plugin.controller.LegacyManagementController;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.test.utils.UnitTestBase;

public class LegacyCategoryControllerTest extends UnitTestBase
{
    private LegacyCategoryController _controller;

    @BeforeEach
    public void setUp()
    {
        final LegacyManagementController legacyManagementController = mock(LegacyManagementController.class);
        final ManagementController nextVersionManagementController = mock(ManagementController.class);
        when(legacyManagementController.getNextVersionManagementController()).thenReturn(
                nextVersionManagementController);
        _controller = new LegacyCategoryController(legacyManagementController,
                                                   VirtualHostNode.TYPE,
                                                   new String[]{BrokerController.TYPE},
                                                   null,
                                                   Set.of());
    }


    @Test
    public void getParentCategories()
    {
        assertThat(_controller.getParentCategories(), is(equalTo(new String[]{BrokerController.TYPE})));
    }

    @Test
    public void convertNextVersionLegacyConfiguredObject()
    {
        final LegacyConfiguredObject node = mock(LegacyConfiguredObject.class);
        when(node.getAttribute(LegacyConfiguredObject.NAME)).thenReturn("test");

        final LegacyConfiguredObject converted =
                _controller.convertNextVersionLegacyConfiguredObject(node);

        assertThat(converted, is(notNullValue()));
        assertThat(converted.getCategory(), is(equalTo(VirtualHostNode.TYPE)));
        assertThat(converted.getAttribute(LegacyConfiguredObject.NAME), is(equalTo("test")));
    }
}