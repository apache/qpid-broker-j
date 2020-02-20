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
package org.apache.qpid.server.management.plugin.controller.v7_0.category;

import static org.apache.qpid.server.management.plugin.controller.v7_0.category.LegacyCategoryControllerFactory.SUPPORTED_CATEGORIES;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.management.plugin.ManagementController;
import org.apache.qpid.server.management.plugin.controller.CategoryController;
import org.apache.qpid.server.management.plugin.controller.LegacyManagementController;
import org.apache.qpid.test.utils.UnitTestBase;

public class LegacyCategoryControllerFactoryTest extends UnitTestBase
{
    private LegacyCategoryControllerFactory _factory;
    private LegacyManagementController _nextVersionManagementController;

    @Before
    public void setUp()
    {
        _nextVersionManagementController = mock(LegacyManagementController.class);
        _factory = new LegacyCategoryControllerFactory();
    }

    @Test
    public void createController()
    {
        SUPPORTED_CATEGORIES.keySet().forEach(category-> {
            final CategoryController controller =
                    _factory.createController(category, _nextVersionManagementController);
            assertThat(controller.getCategory(), is(equalTo(category)));
        });
    }
}
