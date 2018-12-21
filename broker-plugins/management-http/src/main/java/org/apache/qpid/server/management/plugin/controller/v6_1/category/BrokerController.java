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

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.qpid.server.management.plugin.ManagementResponse;
import org.apache.qpid.server.management.plugin.controller.ConverterHelper;
import org.apache.qpid.server.management.plugin.controller.GenericLegacyConfiguredObject;
import org.apache.qpid.server.management.plugin.controller.LegacyConfiguredObject;
import org.apache.qpid.server.management.plugin.controller.LegacyManagementController;
import org.apache.qpid.server.management.plugin.controller.TypeController;
import org.apache.qpid.server.model.ConfiguredObject;


public class BrokerController extends LegacyCategoryController
{
    public static final String TYPE = "Broker";
    private static final String CONNECTION_SESSION_COUNT_LIMIT = "connection.sessionCountLimit";
    private static final String CONNECTION_HEART_BEAT_DELAY = "connection.heartBeatDelay";
    private static final String CONNECTION_CLOSE_WHEN_NO_ROUTE = "connection.closeWhenNoRoute";

    private static Map<String, String> BROKER_ATTRIBUTES_MOVED_INTO_CONTEXT = new HashMap<>();

    static
    {
        BROKER_ATTRIBUTES_MOVED_INTO_CONTEXT.put(CONNECTION_SESSION_COUNT_LIMIT, "qpid.port.sessionCountLimit");
        BROKER_ATTRIBUTES_MOVED_INTO_CONTEXT.put(CONNECTION_HEART_BEAT_DELAY, "qpid.port.heartbeatDelay");
        BROKER_ATTRIBUTES_MOVED_INTO_CONTEXT.put(CONNECTION_CLOSE_WHEN_NO_ROUTE, "qpid.port.closeWhenNoRoute");
    }

    BrokerController(final LegacyManagementController legacyManagementController,
                     final Set<TypeController> typeControllers)
    {
        super(legacyManagementController,
              TYPE,
              new String[]{LegacyCategoryControllerFactory.CATEGORY_SYSTEM_CONFIG},
              "Broker",
              typeControllers);
    }

    @Override
    public LegacyConfiguredObject convertNextVersionLegacyConfiguredObject(final LegacyConfiguredObject object)
    {
        return new LegacyBroker(getManagementController(), object);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Map<String, Object> convertAttributesToNextVersion(final ConfiguredObject<?> root,
                                                                 final List<String> path,
                                                                 final Map<String, Object> attributes)
    {
        final Map<String, Object> converted = new LinkedHashMap<>(attributes);
        final Map<String, String> context = (Map<String, String>) converted.get(LegacyConfiguredObject.CONTEXT);
        final Map<String, String> newContext = new LinkedHashMap<>();
        if (context != null)
        {
            newContext.putAll(context);
        }
        converted.put(LegacyConfiguredObject.CONTEXT, newContext);
        for (String attributeName : BROKER_ATTRIBUTES_MOVED_INTO_CONTEXT.keySet())
        {
            Object value = converted.remove(attributeName);
            if (value != null)
            {
                newContext.put(BROKER_ATTRIBUTES_MOVED_INTO_CONTEXT.get(attributeName), String.valueOf(value));
            }
        }

        converted.remove("statisticsReportingResetEnabled");

        final Object statisticsReportingPeriod = converted.get("statisticsReportingPeriod");
        if (statisticsReportingPeriod != null
            && !newContext.containsKey("qpid.broker.statisticsReportPattern")
            && (ConverterHelper.toInt(statisticsReportingPeriod) > 0 || ConverterHelper.isContextVariable(
                statisticsReportingPeriod)))
        {
            newContext.put("qpid.broker.statisticsReportPattern",
                           "messagesIn=${messagesIn}, bytesIn=${bytesIn:byteunit}, messagesOut=${messagesOut}, bytesOut=${bytesOut:byteunit}");
        }

        return converted;
    }

    static class LegacyBroker extends GenericLegacyConfiguredObject
    {
        private static final String MODEL_VERSION = "modelVersion";

        LegacyBroker(final LegacyManagementController managementController,
                     final LegacyConfiguredObject nextVersionLegacyConfiguredObject)
        {
            super(managementController, nextVersionLegacyConfiguredObject, BrokerController.TYPE);
        }

        @Override
        public Collection<String> getAttributeNames()
        {
            return Stream.concat(super.getAttributeNames().stream(),
                                 BROKER_ATTRIBUTES_MOVED_INTO_CONTEXT.keySet().stream()).collect(
                    Collectors.toSet());
        }

        @Override
        public Object getAttribute(final String name)
        {
            if (MODEL_VERSION.equals(name))
            {
                return getManagementController().getVersion();
            }
            else if (BROKER_ATTRIBUTES_MOVED_INTO_CONTEXT.containsKey(name))
            {
                Object value = getMovedAttribute(name);
                if (value != null)
                {
                    if (CONNECTION_SESSION_COUNT_LIMIT.equals(name))
                    {
                        return ConverterHelper.toInt(value);
                    }
                    else if (CONNECTION_HEART_BEAT_DELAY.equals(name))
                    {
                        return ConverterHelper.toLong(value);
                    }
                    else if (CONNECTION_CLOSE_WHEN_NO_ROUTE.equals(name))
                    {
                        return ConverterHelper.toBoolean(value);
                    }
                }
                return null;
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
            else if (BROKER_ATTRIBUTES_MOVED_INTO_CONTEXT.containsKey(name))
            {
                return getMovedAttribute(name);
            }
            return super.getActualAttribute(name);
        }

        @SuppressWarnings("unchecked")
        private Object getMovedAttribute(final String name)
        {
            Map<String, String> context = (Map<String, String>) super.getAttribute(LegacyConfiguredObject.CONTEXT);
            if (context != null)
            {
                String contextVariable = BROKER_ATTRIBUTES_MOVED_INTO_CONTEXT.get(name);
                return context.get(contextVariable);
            }
            return null;
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

        @Override
        public boolean isSecureAttribute(final String name)
        {
            return !BROKER_ATTRIBUTES_MOVED_INTO_CONTEXT.containsKey(name) && super.isSecureAttribute(name);
        }

        @Override
        public boolean isOversizedAttribute(final String name)
        {
            return !BROKER_ATTRIBUTES_MOVED_INTO_CONTEXT.containsKey(name) && super.isOversizedAttribute(name);
        }

        @Override
        public LegacyConfiguredObject getParent(final String category)
        {
            if (LegacyCategoryControllerFactory.CATEGORY_SYSTEM_CONFIG.equals(category))
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
