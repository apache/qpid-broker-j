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

import java.util.Set;

import org.apache.qpid.server.management.plugin.controller.GenericCategoryController;
import org.apache.qpid.server.management.plugin.controller.GenericLegacyConfiguredObject;
import org.apache.qpid.server.management.plugin.controller.LegacyConfiguredObject;
import org.apache.qpid.server.management.plugin.controller.LegacyManagementController;
import org.apache.qpid.server.management.plugin.controller.TypeController;
import org.apache.qpid.server.management.plugin.controller.latest.LatestManagementControllerAdapter;

public class LegacyCategoryController extends GenericCategoryController
{
    private final String _parentCategory;

    LegacyCategoryController(final LegacyManagementController managementController,
                             final String name,
                             final String parentCategory,
                             final String defaultType,
                             final Set<TypeController> typeControllers)
    {
        super(managementController,
              managementController.getNextVersionManagementController(),
              name,
              defaultType,
              typeControllers);
        _parentCategory = parentCategory;
    }

    @Override
    public String[] getParentCategories()
    {
        return new String[]{_parentCategory};
    }

    @Override
    protected LegacyConfiguredObject convertNextVersionLegacyConfiguredObject(final LegacyConfiguredObject object)
    {
        return new GenericLegacyConfiguredObject(getManagementController(),
                                                 object,
                                                 getCategory());
    }
}
