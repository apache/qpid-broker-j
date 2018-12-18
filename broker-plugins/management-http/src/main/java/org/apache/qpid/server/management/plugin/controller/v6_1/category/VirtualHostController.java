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

import static org.apache.qpid.server.management.plugin.ManagementException.createGoneManagementException;
import static org.apache.qpid.server.management.plugin.controller.GenericLegacyConfiguredObject.CONTEXT;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.qpid.server.management.plugin.ManagementResponse;
import org.apache.qpid.server.management.plugin.controller.GenericLegacyConfiguredObject;
import org.apache.qpid.server.management.plugin.controller.LegacyConfiguredObject;
import org.apache.qpid.server.management.plugin.controller.LegacyManagementController;
import org.apache.qpid.server.management.plugin.controller.TypeController;
import org.apache.qpid.server.model.ConfiguredObject;

class VirtualHostController extends LegacyCategoryController
{

    public static final String TYPE = "VirtualHost";

    VirtualHostController(final LegacyManagementController legacyManagementController,
                          final Set<TypeController> typeControllers)
    {
        super(legacyManagementController,
              TYPE,
              new String[]{LegacyCategoryControllerFactory.CATEGORY_VIRTUAL_HOST_NODE},
              "ProvidedStore",
              typeControllers);
    }

    @Override
    public LegacyConfiguredObject convertNextVersionLegacyConfiguredObject(final LegacyConfiguredObject object)
    {
        return new VirtualHostController.LegacyVirtualHost(getManagementController(), object);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object> convertAttributesToNextVersion(final ConfiguredObject<?> root,
                                                              final List<String> path,
                                                              final Map<String, Object> attributes)
    {

        final Map<String, String> context = (Map<String, String>) attributes.get(CONTEXT);

        if (attributes.containsKey(LegacyVirtualHost.QUEUE_DEAD_LETTER_QUEUE_ENABLED)
            || (context != null && context.containsKey("queue.deadLetterQueueEnabled")))
        {
            final Map<String, Object> converted = new LinkedHashMap<>(attributes);
            converted.remove("queue_deadLetterQueueEnabled");
            if (context != null)
            {
                final Map<String, String> convertedContext = new LinkedHashMap<>(context);
                converted.put("context", convertedContext);
                convertedContext.remove("queue.deadLetterQueueEnabled");
            }

            return converted;
        }
        return attributes;
    }

    public static class LegacyVirtualHost extends GenericLegacyConfiguredObject
    {
        static final String QUEUE_DEAD_LETTER_QUEUE_ENABLED = "queue_deadLetterQueueEnabled";
        private static final String MODEL_VERSION = "modelVersion";

        LegacyVirtualHost(final LegacyManagementController managementController,
                          final LegacyConfiguredObject nextVersionLegacyConfiguredObject)
        {
            super(managementController, nextVersionLegacyConfiguredObject, VirtualHostController.TYPE);
        }

        @Override
        public Collection<String> getAttributeNames()
        {

            return super.getAttributeNames();
        }

        @Override
        public Object getAttribute(final String name)
        {
            if (MODEL_VERSION.equals(name))
            {
                return getManagementController().getVersion();
            }
            else if (QUEUE_DEAD_LETTER_QUEUE_ENABLED.equals(name))
            {
                return false;
            }
            return super.getAttribute(name);
        }

        @Override
        public ManagementResponse invoke(final String operation,
                                         final Map<String, Object> parameters,
                                         final boolean isSecure)
        {
            if ("resetStatistics".equalsIgnoreCase(operation))
            {
                throw createGoneManagementException("Method 'resetStatistics' was removed");
            }
            return super.invoke(operation, parameters, isSecure);
        }
    }
}
