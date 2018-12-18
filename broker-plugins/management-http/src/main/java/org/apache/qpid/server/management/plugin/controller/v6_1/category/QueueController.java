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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.qpid.server.management.plugin.controller.ConverterHelper;
import org.apache.qpid.server.management.plugin.controller.LegacyConfiguredObject;
import org.apache.qpid.server.management.plugin.controller.LegacyManagementController;
import org.apache.qpid.server.management.plugin.controller.TypeController;
import org.apache.qpid.server.model.Binding;
import org.apache.qpid.server.model.ConfiguredObject;

public class QueueController extends DestinationController
{
    public static final String TYPE = "Queue";

    QueueController(final LegacyManagementController legacyManagementController,
                    final Set<TypeController> typeControllers)
    {
        super(legacyManagementController,
              TYPE,
              new String[]{VirtualHostController.TYPE},
              "standard",
              typeControllers);
    }

    @Override
    protected LegacyConfiguredObject convertNextVersionLegacyConfiguredObject(final LegacyConfiguredObject object)
    {
        return new LegacyQueue(getManagementController(), object);
    }

    @Override
    public Map<String, Object> convertAttributesToNextVersion(final ConfiguredObject<?> root,
                                                              final List<String> path,
                                                              final Map<String, Object> attributes)
    {
        Map<String, Object> converted = new LinkedHashMap<>(attributes);
        Object queueFlowControlSizeBytes = converted.remove("queueFlowControlSizeBytes");
        Object queueFlowResumeSizeBytes = converted.remove("queueFlowResumeSizeBytes");
        if (queueFlowControlSizeBytes != null)
        {
            long queueFlowControlSizeBytesValue = ConverterHelper.toLong(queueFlowControlSizeBytes);
            if (queueFlowControlSizeBytesValue > 0)
            {
                if (queueFlowResumeSizeBytes != null)
                {
                    long queueFlowResumeSizeBytesValue = ConverterHelper.toLong(queueFlowResumeSizeBytes);
                    double ratio = ((double) queueFlowResumeSizeBytesValue)
                                   / ((double) queueFlowControlSizeBytesValue);
                    String flowResumeLimit = String.format("%.2f", ratio * 100.0);

                    Object context = converted.get("context");
                    Map<String, String> contextMap;
                    if (context instanceof Map)
                    {
                        contextMap = (Map) context;
                    }
                    else
                    {
                        contextMap = new LinkedHashMap<>();
                        converted.put("context", contextMap);
                    }
                    contextMap.put("queue.queueFlowResumeLimit", flowResumeLimit);
                }
                converted.put("overflowPolicy", "PRODUCER_FLOW_CONTROL");
                converted.put("maximumQueueDepthBytes", queueFlowControlSizeBytes);
            }
        }

        if (converted.containsKey("messageGroupKey"))
        {
            if (converted.containsKey("messageGroupSharedGroups")
                && ConverterHelper.toBoolean(converted.remove("messageGroupSharedGroups")))
            {
                converted.put("messageGroupType", "SHARED_GROUPS");
            }
            else
            {
                converted.put("messageGroupType", "STANDARD");
            }
            Object oldMessageGroupKey = converted.remove("messageGroupKey");
            if (!"JMSXGroupId".equals(oldMessageGroupKey))
            {
                converted.put("messageGroupKeyOverride", oldMessageGroupKey);
            }
        }
        else
        {
            converted.put("messageGroupType", "NONE");
        }

        return super.convertAttributesToNextVersion(root, path, converted);
    }


    static class LegacyQueue extends LegacyDestination
    {
        static final String QUEUE_FLOW_RESUME_SIZE_BYTES = "queueFlowResumeSizeBytes";
        static final String QUEUE_FLOW_CONTROL_SIZE_BYTES = "queueFlowControlSizeBytes";
        static final String MESSAGE_GROUP_SHARED_GROUPS = "messageGroupSharedGroups";
        static final String MESSAGE_GROUP_KEY = "messageGroupKey";

        Set<String> MOVED_ATTRIBUTES = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
                QUEUE_FLOW_RESUME_SIZE_BYTES,
                QUEUE_FLOW_CONTROL_SIZE_BYTES,
                MESSAGE_GROUP_SHARED_GROUPS,
                MESSAGE_GROUP_KEY,
                ALTERNATE_EXCHANGE)));

        LegacyQueue(final LegacyManagementController managementController,
                    final LegacyConfiguredObject nextVersionQueue)
        {
            super(managementController, nextVersionQueue, QueueController.TYPE);
        }

        @Override
        public Collection<String> getAttributeNames()
        {
            return Stream.concat(super.getAttributeNames().stream(),
                                 MOVED_ATTRIBUTES.stream()).collect(Collectors.toSet());
        }

        @Override
        @SuppressWarnings("unchecked")
        public Collection<LegacyConfiguredObject> getChildren(final String category)
        {
            if (BindingController.TYPE.equals(category))
            {
                String queueName = (String) getAttribute(NAME);
                Collection<LegacyConfiguredObject> exchanges =
                        getNextVersionLegacyConfiguredObject().getParent(VirtualHostController.TYPE)
                                                              .getChildren(ExchangeController.TYPE);

                List<LegacyConfiguredObject> bindingObjects = new ArrayList<>();
                for (LegacyConfiguredObject exchange : exchanges)
                {
                    Object bindings = exchange.getAttribute("bindings");
                    if (bindings instanceof Collection)
                    {
                        Collection<?> exchangeBindings = (Collection<?>) bindings;
                        exchangeBindings.stream()
                                        .map(Binding.class::cast)
                                        .filter(i -> i.getDestination().equals(queueName))
                                        .map(i -> new BindingController.LegacyBinding(getManagementController(),
                                                                                      exchange,
                                                                                      getNextVersionLegacyConfiguredObject(),
                                                                                      i.getBindingKey(),
                                                                                      i.getArguments()))
                                        .forEach(bindingObjects::add);
                    }
                }
                return bindingObjects;
            }
            return super.getChildren(category);
        }

        protected Object getAttributeInternal(final String name, boolean isActual)
        {
            if (QUEUE_FLOW_CONTROL_SIZE_BYTES.equals(name))
            {
                final Object overflowPolicy = getAttribute("overflowPolicy", isActual);
                if (overflowPolicy != null)
                {
                    final Object maximumQueueDepthBytes = getAttribute("maximumQueueDepthBytes", isActual);
                    if ("PRODUCER_FLOW_CONTROL".equals(String.valueOf(overflowPolicy))
                        && maximumQueueDepthBytes != null)
                    {
                        return maximumQueueDepthBytes;
                    }
                }
                return null;
            }
            else if (QUEUE_FLOW_RESUME_SIZE_BYTES.equals(name))
            {
                final Object overflowPolicy = getAttribute("overflowPolicy", isActual);
                final Object maximumQueueDepthBytes = getAttribute("maximumQueueDepthBytes", isActual);
                if ("PRODUCER_FLOW_CONTROL".equals(String.valueOf(overflowPolicy)) && maximumQueueDepthBytes != null)
                {
                    final long queueFlowControlSizeBytesValue = ConverterHelper.toLong(maximumQueueDepthBytes);
                    if (queueFlowControlSizeBytesValue > 0)
                    {
                        int queueFlowResumeLimit = 80;

                        @SuppressWarnings("unchecked")
                        Map<String, String> context = (Map<String, String>) super.getAttribute(CONTEXT);
                        if (context != null)
                        {
                            queueFlowResumeLimit = ConverterHelper.toInt(context.get("queue.queueFlowResumeLimit"));
                            if (queueFlowResumeLimit == 0)
                            {
                                queueFlowResumeLimit = 80;
                            }
                        }
                        return Math.round(queueFlowControlSizeBytesValue * ((double) queueFlowResumeLimit) / 100.0);
                    }
                }
                return null;
            }
            else if (MESSAGE_GROUP_SHARED_GROUPS.equals(name))
            {
                Object messageGroupType = getAttribute("messageGroupType", isActual);
                if (messageGroupType != null)
                {
                    String type = String.valueOf(messageGroupType);
                    return "SHARED_GROUPS".equals(type);
                }
                return null;
            }
            else if (MESSAGE_GROUP_KEY.equals(name))
            {
                Object messageGroupKeyOverride = getAttribute("messageGroupKeyOverride", isActual);
                Object messageGroupType = getAttribute("messageGroupType", isActual);
                if (messageGroupType != null)
                {
                    return messageGroupKeyOverride == null ? "JMSXGroupId" : messageGroupKeyOverride;
                }
                return null;
            }

            return super.getAttributeInternal(name, isActual);
        }

        @Override
        public boolean isSecureAttribute(final String name)
        {
             if (MOVED_ATTRIBUTES.contains(name))
             {
                 return false;
             }
             return super.isSecureAttribute(name);
        }

        @Override
        public boolean isOversizedAttribute(final String name)
        {
            if (MOVED_ATTRIBUTES.contains(name))
            {
                return false;
            }
            return super.isOversizedAttribute(name);
        }
    }
}
