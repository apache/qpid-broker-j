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

import org.apache.qpid.server.management.plugin.controller.CategoryController;
import org.apache.qpid.server.management.plugin.controller.LegacyManagementController;
import org.apache.qpid.server.management.plugin.controller.v7_0.LegacyManagementControllerFactory_v8_0;
import org.apache.qpid.server.plugin.PluggableService;

@PluggableService
public class LegacyCategoryControllerFactory_v8_0 extends LegacyCategoryControllerFactory
{
    @Override
    public String getModelVersion()
    {
        return LegacyManagementControllerFactory_v8_0.MODEL_VERSION;
    }


    @Override
    public CategoryController createController(final String type,
                                               final LegacyManagementController legacyManagementController)
    {
        if (SUPPORTED_CATEGORIES.containsKey(type))
        {
            return new LegacyCategoryController_v8_0(legacyManagementController,
                                                     type,
                                                     SUPPORTED_CATEGORIES.get(type),
                                                     DEFAULT_TYPES.get(type),
                                                     legacyManagementController.getTypeControllersByCategory(type));
        }
        else
        {
            throw new IllegalArgumentException(String.format("Unsupported type '%s'", type));
        }
    }
}
