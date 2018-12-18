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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.qpid.server.management.plugin.ManagementException;
import org.apache.qpid.server.management.plugin.controller.GenericLegacyConfiguredObject;
import org.apache.qpid.server.management.plugin.controller.LegacyConfiguredObject;
import org.apache.qpid.server.management.plugin.controller.LegacyManagementController;
import org.apache.qpid.server.management.plugin.controller.TypeController;
import org.apache.qpid.server.model.ConfiguredObject;

public class PortController extends LegacyCategoryController
{
    public static final String TYPE = "Port";

    PortController(final LegacyManagementController legacyManagementController,
                   final Set<TypeController> typeControllers)
    {
        super(legacyManagementController,
              TYPE,
              new String[]{BrokerController.TYPE},
              null,
              typeControllers);
    }

    @Override
    protected LegacyConfiguredObject convertNextVersionLegacyConfiguredObject(final LegacyConfiguredObject object)
    {
        return new LegacyPort(getManagementController(), object);
    }

    @Override
    public Map<String, Object> convertAttributesToNextVersion(final ConfiguredObject<?> root,
                                                              final List<String> path,
                                                              final Map<String, Object> attributes)
    {
        Map<String, Object> portAttributes = attributes;
        if ("HTTP".equals(portAttributes.get("type")) && portAttributes.containsKey("context"))
        {
            @SuppressWarnings("unchecked")
            Map<String, String> context = (Map<String, String>) portAttributes.get("context");
            if (context.containsKey("port.http.additionalInternalThreads")
                || context.containsKey("port.http.maximumQueuedRequests"))
            {
                Map<String, Object> updatedAttributes = new LinkedHashMap<>(portAttributes);
                updatedAttributes.put("context", convertContextToNextVersion(context));
                portAttributes = updatedAttributes;
            }
        }
        return portAttributes;
    }

    private Map<String, String> convertContextToNextVersion(final Map<String, String> context)
            throws ManagementException
    {
        if (context.containsKey("port.http.additionalInternalThreads")
            || context.containsKey("port.http.maximumQueuedRequests"))
        {
            Map<String, String> updatedContext = new LinkedHashMap<>(context);
            updatedContext.remove("port.http.additionalInternalThreads");
            String acceptorsBacklog = updatedContext.remove("port.http.maximumQueuedRequests");
            if (acceptorsBacklog != null)
            {
                updatedContext.put("qpid.port.http.acceptBacklog", acceptorsBacklog);
            }
            return updatedContext;
        }
        return context;
    }

    static class LegacyPort extends GenericLegacyConfiguredObject
    {
        LegacyPort(final LegacyManagementController managementController,
                   final LegacyConfiguredObject nextVersionQueue)
        {
            super(managementController, nextVersionQueue, PortController.TYPE);
        }

        @Override
        @SuppressWarnings("unchecked")
        public Object getAttribute(final String name)
        {
            if (CONTEXT.equals(name))
            {
                if ("HTTP".equals(super.getAttribute(TYPE)))
                {
                    Map<String, String> context = (Map<String, String>) super.getAttribute(CONTEXT);
                    return convertContextIfRequired(context);
                }
            }
            return super.getAttribute(name);
        }

        @Override
        @SuppressWarnings("unchecked")
        public Object getActualAttribute(final String name)
        {
            if (CONTEXT.equals(name) && "HTTP".equals(super.getAttribute(TYPE)))
            {
                Map<String, String> context = (Map<String, String>) super.getActualAttribute(CONTEXT);
                return convertContextIfRequired(context);
            }
            return super.getActualAttribute(name);
        }

        private Object convertContextIfRequired(final Map<String, String> context)
        {
            if (context != null && context.containsKey("qpid.port.http.acceptBacklog"))
            {
                Map<String, String> updatedContext = new LinkedHashMap<>(context);
                updatedContext.put("port.http.maximumQueuedRequests", updatedContext.remove("qpid.port.http.acceptBacklog"));
                return updatedContext;
            }
            return context;
        }
    }
}
