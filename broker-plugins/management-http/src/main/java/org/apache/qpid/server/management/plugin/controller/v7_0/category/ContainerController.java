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

import org.apache.qpid.server.management.plugin.controller.GenericLegacyConfiguredObject;
import org.apache.qpid.server.management.plugin.controller.LegacyConfiguredObject;
import org.apache.qpid.server.management.plugin.controller.LegacyManagementController;
import org.apache.qpid.server.management.plugin.controller.TypeController;


class ContainerController extends LegacyCategoryController
{
    ContainerController(final LegacyManagementController legacyManagementController,
                        final String type,
                        final String parentType,
                        final String defaultType,
                        final Set<TypeController> typeControllers)
    {
        super(legacyManagementController, type, parentType, defaultType, typeControllers);
    }

    @Override
    protected LegacyConfiguredObject convertNextVersionLegacyConfiguredObject(final LegacyConfiguredObject object)
    {
        return new LegacyContainer(getManagementController(), object, getCategory());
    }

    static class LegacyContainer extends GenericLegacyConfiguredObject
    {
        private static final String MODEL_VERSION = "modelVersion";

        LegacyContainer(final LegacyManagementController managementController,
                        final LegacyConfiguredObject nextVersionLegacyConfiguredObject,
                        final String category)
        {
            super(managementController, nextVersionLegacyConfiguredObject, category);
        }

        @Override
        public Object getAttribute(final String name)
        {
            if (MODEL_VERSION.equals(name))
            {
                return getManagementController().getVersion();
            }
            return super.getAttribute(name);
        }

        @Override
        public Object getActualAttribute(final String name)
        {
            if (MODEL_VERSION.equals(name))
            {
                return getManagementController().getVersion();
            }
            return super.getActualAttribute(name);
        }

        @Override
        public LegacyConfiguredObject getParent(final String category)
        {
            if (LegacyCategoryControllerFactory.CATEGORY_BROKER.equals(getCategory())
                && LegacyCategoryControllerFactory.CATEGORY_SYSTEM_CONFIG.equals(category))
            {
                LegacyConfiguredObject nextVersionParent = getNextVersionLegacyConfiguredObject().getParent(category);
                return new GenericLegacyConfiguredObject(getManagementController(),
                                                         nextVersionParent,
                                                         category);
            }
            return super.getParent(category);
        }
    }
}
