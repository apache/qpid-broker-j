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
package org.apache.qpid.server.store;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.configuration.store.StoreConfigurationChangeListener;
import org.apache.qpid.server.filter.FilterSupport;
import org.apache.qpid.server.model.AbstractConfigurationChangeListener;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.Binding;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.UUIDGenerator;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.store.handler.ConfiguredObjectRecordHandler;
import org.apache.qpid.server.util.FixedKeyMapCreator;

public class VirtualHostStoreUpgraderAndRecoverer extends AbstractConfigurationStoreUpgraderAndRecoverer
{
    private final VirtualHostNode<?> _virtualHostNode;

    @SuppressWarnings("serial")
    private static final Map<String, String> DEFAULT_EXCHANGES = Collections.unmodifiableMap(new HashMap<String, String>()
    {{
        put("amq.direct", "direct");
        put("amq.topic", "topic");
        put("amq.fanout", "fanout");
        put("amq.match", "headers");
    }});

    private final Map<String, UUID> _defaultExchangeIds;

    public VirtualHostStoreUpgraderAndRecoverer(VirtualHostNode<?> virtualHostNode)
    {
        super("0.0");
        _virtualHostNode = virtualHostNode;
        register(new Upgrader_0_0_to_0_1());
        register(new Upgrader_0_1_to_0_2());
        register(new Upgrader_0_2_to_0_3());
        register(new Upgrader_0_3_to_0_4());
        register(new Upgrader_0_4_to_2_0());
        register(new Upgrader_2_0_to_3_0());
        register(new Upgrader_3_0_to_6_0());
        register(new Upgrader_6_0_to_6_1());
        register(new Upgrader_6_1_to_7_0());
        register(new Upgrader_7_0_to_7_1());
        register(new Upgrader_7_1_to_8_0());

        Map<String, UUID> defaultExchangeIds = new HashMap<String, UUID>();
        for (String exchangeName : DEFAULT_EXCHANGES.keySet())
        {
            UUID id = UUIDGenerator.generateExchangeUUID(exchangeName, virtualHostNode.getName());
            defaultExchangeIds.put(exchangeName, id);
        }
        _defaultExchangeIds = Collections.unmodifiableMap(defaultExchangeIds);
    }

    /*
     * Removes filters from queue bindings to exchanges other than topic exchanges.  In older versions of the broker
     * such bindings would have been ignored, starting from the point at which the config version changed, these
     * arguments would actually cause selectors to be enforced, thus changing which messages would reach a queue.
     */
    private class Upgrader_0_0_to_0_1  extends StoreUpgraderPhase
    {
        private final Map<UUID, ConfiguredObjectRecord> _records = new HashMap<UUID, ConfiguredObjectRecord>();

        public Upgrader_0_0_to_0_1()
        {
            super("modelVersion", "0.0", "0.1");
        }

        @Override
        public void configuredObject(final ConfiguredObjectRecord record)
        {
            _records.put(record.getId(), record);
        }

        private void removeSelectorArguments(Map<String, Object> binding)
        {
            @SuppressWarnings("unchecked")
            Map<String, Object> arguments = new LinkedHashMap<String, Object>((Map<String,Object>)binding.get("arguments"));

            FilterSupport.removeFilters(arguments);
            binding.put("arguments", arguments);
        }

        private boolean isTopicExchange(ConfiguredObjectRecord entry)
        {
            UUID exchangeId = entry.getParents().get("Exchange");
            if (exchangeId == null)
            {
                return false;
            }

            if(_records.containsKey(exchangeId))
            {
                return "topic".equals(_records.get(exchangeId)
                        .getAttributes()
                        .get(org.apache.qpid.server.model.Exchange.TYPE));
            }
            else
            {
                if (_defaultExchangeIds.get("amq.topic").equals(exchangeId))
                {
                    return true;
                }

                return false;
            }

        }

        private boolean hasSelectorArguments(Map<String, Object> binding)
        {
            @SuppressWarnings("unchecked")
            Map<String, Object> arguments = (Map<String, Object>) binding.get("arguments");
            return (arguments != null) && FilterSupport.argumentsContainFilter(arguments);
        }

        @Override
        public void complete()
        {
            for(Map.Entry<UUID,ConfiguredObjectRecord> entry : _records.entrySet())
            {
                ConfiguredObjectRecord record = entry.getValue();
                String type = record.getType();
                Map<String, Object> attributes = record.getAttributes();
                UUID id = record.getId();
                if ("org.apache.qpid.server.model.VirtualHost".equals(type))
                {
                    upgradeRootRecord(record);
                }
                else if(type.equals(Binding.class.getName()) && hasSelectorArguments(attributes) && !isTopicExchange(record))
                {
                    attributes = new LinkedHashMap<String, Object>(attributes);
                    removeSelectorArguments(attributes);

                    record = new ConfiguredObjectRecordImpl(id, type, attributes, record.getParents());
                    getUpdateMap().put(id, record);
                    entry.setValue(record);

                }
            }
        }

    }

    /*
     * Change the type string from org.apache.qpid.server.model.Foo to Foo (in line with the practice in the broker
     * configuration store).  Also remove bindings which reference nonexistent queues or exchanges.
     */
    private class Upgrader_0_1_to_0_2 extends StoreUpgraderPhase
    {
        public Upgrader_0_1_to_0_2()
        {
            super("modelVersion", "0.1", "0.2");
        }

        @Override
        public void configuredObject(final ConfiguredObjectRecord record)
        {
            String type = record.getType().substring(1 + record.getType().lastIndexOf('.'));
            ConfiguredObjectRecord newRecord = new ConfiguredObjectRecordImpl(record.getId(), type, record.getAttributes(), record.getParents());
            getUpdateMap().put(record.getId(), newRecord);

            if ("VirtualHost".equals(type))
            {
                upgradeRootRecord(newRecord);
            }
        }

        @Override
        public void complete()
        {
            for (Iterator<Map.Entry<UUID, ConfiguredObjectRecord>> iterator = getUpdateMap().entrySet().iterator(); iterator.hasNext();)
            {
                Map.Entry<UUID, ConfiguredObjectRecord> entry = iterator.next();
                final ConfiguredObjectRecord record = entry.getValue();
                final UUID exchangeParent = record.getParents().get(Exchange.class.getSimpleName());
                final UUID queueParent = record.getParents().get(Queue.class.getSimpleName());
                if(isBinding(record.getType()) && (exchangeParent == null || unknownExchange(exchangeParent)
                                                   || queueParent == null || unknownQueue(queueParent)))
                {
                    getDeleteMap().put(entry.getKey(), entry.getValue());
                    iterator.remove();
                }
            }
        }

        private boolean unknownExchange(final UUID exchangeId)
        {
            if (_defaultExchangeIds.containsValue(exchangeId))
            {
                return false;
            }
            ConfiguredObjectRecord localRecord = getUpdateMap().get(exchangeId);
            return !(localRecord != null && localRecord.getType().equals(Exchange.class.getSimpleName()));
        }

        private boolean unknownQueue(final UUID queueId)
        {
            ConfiguredObjectRecord localRecord = getUpdateMap().get(queueId);
            return !(localRecord != null  && localRecord.getType().equals(Queue.class.getSimpleName()));
        }

        private boolean isBinding(final String type)
        {
            return Binding.class.getSimpleName().equals(type);
        }
    }


    /*
     * Convert the storage of queue attributes to remove the separate "ARGUMENT" attribute, and flatten the
     * attributes into the map using the model attribute names rather than the wire attribute names
     */
    private class Upgrader_0_2_to_0_3 extends StoreUpgraderPhase
    {
        private static final String ARGUMENTS = "arguments";

        public Upgrader_0_2_to_0_3()
        {
            super("modelVersion", "0.2", "0.3");
        }

        @SuppressWarnings("unchecked")
        @Override
        public void configuredObject(ConfiguredObjectRecord record)
        {
            if ("VirtualHost".equals(record.getType()))
            {
                upgradeRootRecord(record);
            }
            else if ("Queue".equals(record.getType()))
            {
                Map<String, Object> newAttributes = new LinkedHashMap<String, Object>();
                if (record.getAttributes().get(ARGUMENTS) instanceof Map)
                {
                    newAttributes.putAll(convertWireArgsToModel((Map<String, Object>) record.getAttributes()
                                                                                            .get(ARGUMENTS)));
                }
                newAttributes.putAll(record.getAttributes());

                record = new ConfiguredObjectRecordImpl(record.getId(),
                                                        record.getType(),
                                                        newAttributes,
                                                        record.getParents());
                getUpdateMap().put(record.getId(), record);
            }
        }

        @Override
        public void complete()
        {
        }

        private final Map<String, String> ATTRIBUTE_MAPPINGS = new LinkedHashMap<>();

        {
            ATTRIBUTE_MAPPINGS.put("x-qpid-minimum-alert-repeat-gap", "alertRepeatGap");
            ATTRIBUTE_MAPPINGS.put("x-qpid-maximum-message-age", "alertThresholdMessageAge");
            ATTRIBUTE_MAPPINGS.put("x-qpid-maximum-message-size", "alertThresholdMessageSize");
            ATTRIBUTE_MAPPINGS.put("x-qpid-maximum-message-count", "alertThresholdQueueDepthMessages");
            ATTRIBUTE_MAPPINGS.put("x-qpid-maximum-queue-depth", "alertThresholdQueueDepthBytes");
            ATTRIBUTE_MAPPINGS.put("qpid.alert_count", "alertThresholdQueueDepthMessages");
            ATTRIBUTE_MAPPINGS.put("qpid.alert_size", "alertThresholdQueueDepthBytes");
            ATTRIBUTE_MAPPINGS.put("qpid.alert_repeat_gap", "alertRepeatGap");
            ATTRIBUTE_MAPPINGS.put("x-qpid-maximum-delivery-count", "maximumDeliveryAttempts");
            ATTRIBUTE_MAPPINGS.put("x-qpid-capacity", "queueFlowControlSizeBytes");
            ATTRIBUTE_MAPPINGS.put("x-qpid-flow-resume-capacity", "queueFlowResumeSizeBytes");
            ATTRIBUTE_MAPPINGS.put("qpid.queue_sort_key", "sortKey");
            ATTRIBUTE_MAPPINGS.put("qpid.last_value_queue_key", "lvqKey");
            ATTRIBUTE_MAPPINGS.put("x-qpid-priorities", "priorities");
            ATTRIBUTE_MAPPINGS.put("x-qpid-description", "description");
            ATTRIBUTE_MAPPINGS.put("x-qpid-dlq-enabled", "x-qpid-dlq-enabled");
            ATTRIBUTE_MAPPINGS.put("qpid.group_header_key", "messageGroupKey");
            ATTRIBUTE_MAPPINGS.put("qpid.default-message-group", "messageGroupDefaultGroup");
            ATTRIBUTE_MAPPINGS.put("no-local", "noLocal");
            ATTRIBUTE_MAPPINGS.put("qpid.message_durability", "messageDurability");
        }

        private Map<String, Object> convertWireArgsToModel(Map<String, Object> wireArguments)
        {
            Map<String, Object> modelArguments = new HashMap<>();
            if (wireArguments != null)
            {
                for (Map.Entry<String, String> entry : ATTRIBUTE_MAPPINGS.entrySet())
                {
                    if (wireArguments.containsKey(entry.getKey()))
                    {
                        modelArguments.put(entry.getValue(), wireArguments.get(entry.getKey()));
                    }
                }
                if (wireArguments.containsKey("qpid.last_value_queue")
                    && !wireArguments.containsKey("qpid.last_value_queue_key"))
                {
                    modelArguments.put("lvqKey", "qpid.LVQ_key");
                }
                if (wireArguments.containsKey("qpid.shared_msg_group"))
                {
                    modelArguments.put("messageGroupSharedGroups",
                                       "1".equals(String.valueOf(wireArguments.get("qpid.shared_msg_group"))));
                }
                if (wireArguments.get("x-qpid-dlq-enabled") != null)
                {
                    modelArguments.put("x-qpid-dlq-enabled",
                                       Boolean.parseBoolean(wireArguments.get("x-qpid-dlq-enabled").toString()));
                }
                if (wireArguments.get("no-local") != null)
                {
                    modelArguments.put("noLocal", Boolean.parseBoolean(wireArguments.get("no-local").toString()));
                }
            }
            return modelArguments;
        }
    }

    /*
     * Convert the storage of queue attribute exclusive to change exclusive from a boolean to an enum
     * where exclusive was false it will now be "NONE", and where true it will now be "CONTAINER"
     * ensure OWNER is null unless the exclusivity policy is CONTAINER
     */
    private class Upgrader_0_3_to_0_4 extends StoreUpgraderPhase
    {
        private static final String EXCLUSIVE = "exclusive";

        public Upgrader_0_3_to_0_4()
        {
            super("modelVersion", "0.3", "0.4");
        }


        @Override
        public void configuredObject(ConfiguredObjectRecord record)
        {
            if("VirtualHost".equals(record.getType()))
            {
                upgradeRootRecord(record);
            }
            else if(Queue.class.getSimpleName().equals(record.getType()))
            {
                Map<String, Object> newAttributes = new LinkedHashMap<String, Object>(record.getAttributes());
                if(record.getAttributes().get(EXCLUSIVE) instanceof Boolean)
                {
                    boolean isExclusive = (Boolean) record.getAttributes().get(EXCLUSIVE);
                    newAttributes.put(EXCLUSIVE, isExclusive ? "CONTAINER" : "NONE");
                    if(!isExclusive && record.getAttributes().containsKey("owner"))
                    {
                        newAttributes.remove("owner");
                    }
                }
                else
                {
                    newAttributes.remove("owner");
                }
                if(!record.getAttributes().containsKey("durable"))
                {
                    newAttributes.put("durable", "true");
                }

                record = new ConfiguredObjectRecordImpl(record.getId(),record.getType(),newAttributes, record.getParents());
                getUpdateMap().put(record.getId(), record);
            }
        }

        @Override
        public void complete()
        {
        }

    }

    private class Upgrader_0_4_to_2_0 extends StoreUpgraderPhase
    {
        private static final String ALTERNATE_EXCHANGE = "alternateExchange";
        private static final String DLQ_ENABLED_ARGUMENT = "x-qpid-dlq-enabled";
        private static final String  DEFAULT_DLE_NAME_SUFFIX = "_DLE";

        private Map<String, String> _missingAmqpExchanges = new HashMap<String, String>(DEFAULT_EXCHANGES);
        private ConfiguredObjectRecord _virtualHostRecord;

        private Map<UUID, String> _queuesMissingAlternateExchange = new HashMap<>();
        private Map<String, ConfiguredObjectRecord> _exchanges = new HashMap<>();

        public Upgrader_0_4_to_2_0()
        {
            super("modelVersion", "0.4", "2.0");
        }

        @Override
        public void configuredObject(ConfiguredObjectRecord record)
        {
            if("VirtualHost".equals(record.getType()))
            {
                record = upgradeRootRecord(record);
                Map<String, Object> virtualHostAttributes = new HashMap<String, Object>(record.getAttributes());
                virtualHostAttributes.put("name", _virtualHostNode.getName());
                virtualHostAttributes.put("modelVersion", getToVersion());
                record = new ConfiguredObjectRecordImpl(record.getId(), "VirtualHost", virtualHostAttributes, Collections.<String, UUID>emptyMap());
                _virtualHostRecord = record;
            }
            else if("Exchange".equals(record.getType()))
            {
                Map<String, Object> attributes = record.getAttributes();
                String name = (String)attributes.get("name");
                _missingAmqpExchanges.remove(name);
                _exchanges.put(name, record);
            }
            else if("Queue".equals(record.getType()))
            {
                updateQueueRecordIfNecessary(record);
            }
        }

        @Override
        public void complete()
        {
            for (UUID queueId : _queuesMissingAlternateExchange.keySet())
            {
                ConfiguredObjectRecord record = getUpdateMap().get(queueId);
                if (record != null)
                {
                    String dleExchangeName = _queuesMissingAlternateExchange.get(queueId);
                    ConfiguredObjectRecord alternateExchange = _exchanges.get(dleExchangeName);
                    if (alternateExchange != null)
                    {
                        setAlternateExchangeAttribute(record, alternateExchange);
                    }
                }
            }

            for (Entry<String, String> entry : _missingAmqpExchanges.entrySet())
            {
                String name = entry.getKey();
                String type = entry.getValue();
                UUID id = _defaultExchangeIds.get(name);

                Map<String, Object> attributes = new HashMap<String, Object>();
                attributes.put("name", name);
                attributes.put("type", type);
                attributes.put("lifetimePolicy", "PERMANENT");

                ConfiguredObjectRecord record = new ConfiguredObjectRecordImpl(id, Exchange.class.getSimpleName(), attributes, Collections.singletonMap(_virtualHostRecord.getType(), _virtualHostRecord.getId()));
                getUpdateMap().put(id, record);

            }

        }

        private ConfiguredObjectRecord updateQueueRecordIfNecessary(ConfiguredObjectRecord record)
        {
            Map<String, Object> attributes = record.getAttributes();
            boolean queueDLQEnabled = Boolean.parseBoolean(String.valueOf(attributes.get(DLQ_ENABLED_ARGUMENT)));
            if(queueDLQEnabled && attributes.get(ALTERNATE_EXCHANGE) == null)
            {
                Object queueName =  attributes.get("name");
                if (queueName == null || "".equals(queueName))
                {
                    throw new IllegalConfigurationException("Queue name is not found in queue configuration entry attributes: " + attributes);
                }

                String dleSuffix = System.getProperty("qpid.broker_dead_letter_exchange_suffix", DEFAULT_DLE_NAME_SUFFIX);
                String dleExchangeName = queueName + dleSuffix;

                ConfiguredObjectRecord exchangeRecord = findConfiguredObjectRecordInUpdateMap("Exchange", dleExchangeName);
                if (exchangeRecord == null)
                {
                    // add record to update Map if it is not there
                    if (!getUpdateMap().containsKey(record.getId()))
                    {
                        getUpdateMap().put(record.getId(), record);
                    }
                    _queuesMissingAlternateExchange.put(record.getId(), dleExchangeName);
                }
                else
                {
                    record = setAlternateExchangeAttribute(record, exchangeRecord);
                }
            }
            return record;
        }

        private ConfiguredObjectRecord setAlternateExchangeAttribute(ConfiguredObjectRecord record, ConfiguredObjectRecord alternateExchange)
        {
            Map<String, Object> attributes = new LinkedHashMap<>(record.getAttributes());
            attributes.put(ALTERNATE_EXCHANGE, alternateExchange.getId());
            record = new ConfiguredObjectRecordImpl(record.getId(), record.getType(), attributes, record.getParents());
            getUpdateMap().put(record.getId(), record);
            return record;
        }

        private ConfiguredObjectRecord findConfiguredObjectRecordInUpdateMap(String type, String name)
        {
            for(ConfiguredObjectRecord record: getUpdateMap().values())
            {
                if (type.equals(record.getType()) && name.equals(record.getAttributes().get("name")))
                {
                    return record;
                }
            }
            return null;
        }

    }

    private class Upgrader_2_0_to_3_0 extends StoreUpgraderPhase
    {
        public Upgrader_2_0_to_3_0()
        {
            super("modelVersion", "2.0", "3.0");
        }

        @Override
        public void configuredObject(ConfiguredObjectRecord record)
        {

            if("VirtualHost".equals(record.getType()))
            {
                upgradeRootRecord(record);
            }
        }

        @Override
        public void complete()
        {
        }

    }
    private class Upgrader_3_0_to_6_0 extends StoreUpgraderPhase
    {
        public Upgrader_3_0_to_6_0()
        {
            super("modelVersion", "3.0", "6.0");
        }

        @Override
        public void configuredObject(ConfiguredObjectRecord record)
        {

            if("VirtualHost".equals(record.getType()))
            {
                upgradeRootRecord(record);
            }
        }

        @Override
        public void complete()
        {
        }

    }

    private class Upgrader_6_0_to_6_1 extends StoreUpgraderPhase
    {
        public Upgrader_6_0_to_6_1()
        {
            super("modelVersion", "6.0", "6.1");
        }

        @Override
        public void configuredObject(ConfiguredObjectRecord record)
        {

            if("VirtualHost".equals(record.getType()))
            {
                upgradeRootRecord(record);
            }
        }

        @Override
        public void complete()
        {
        }

    }

    private static final FixedKeyMapCreator BINDING_MAP_CREATOR = new FixedKeyMapCreator("bindingKey", "destination", "arguments");
    private static final FixedKeyMapCreator NO_ARGUMENTS_BINDING_MAP_CREATOR = new FixedKeyMapCreator("bindingKey", "destination");

    private class Upgrader_6_1_to_7_0 extends StoreUpgraderPhase
    {
        private final Map<UUID, List<BindingRecord>> _exchangeBindings = new HashMap<>();
        private final Map<UUID, ConfiguredObjectRecord> _exchanges = new HashMap<>();
        private final Map<UUID, String> _queues = new HashMap<>();
        private final Map<String, List<Map<String,Object>>> _queueBindings = new HashMap<>();
        private Set<UUID> _destinationsWithAlternateExchange = new HashSet<>();


        public Upgrader_6_1_to_7_0()
        {
            super("modelVersion", "6.1", "7.0");
        }

        @Override
        public void configuredObject(ConfiguredObjectRecord record)
        {
            if("VirtualHost".equals(record.getType()))
            {
                record = upgradeRootRecord(record);
                Map<String, Object> attributes = new HashMap<>(record.getAttributes());
                boolean modified = attributes.remove("queue_deadLetterQueueEnabled") != null;
                Object context = attributes.get("context");
                Map<String,Object> contextMap = null;
                if(context instanceof Map)
                {
                    contextMap = new HashMap<>((Map<String,Object>) context);
                    modified |= contextMap.remove("queue.deadLetterQueueEnabled") != null;
                    if (modified)
                    {
                        attributes.put("context", contextMap);
                    }
                }

                int brokerStatisticsReportingPeriod = ((Broker) _virtualHostNode.getParent()).getStatisticsReportingPeriod();
                if (brokerStatisticsReportingPeriod > 0)
                {
                    attributes.put("statisticsReportingPeriod", brokerStatisticsReportingPeriod);
                    if (contextMap == null)
                    {
                        contextMap = new HashMap<>();
                    }

                    contextMap.put("qpid.virtualhost.statisticsReportPattern", "${ancestor:virtualhost:name}: messagesIn=${messagesIn}, bytesIn=${bytesIn:byteunit}, messagesOut=${messagesOut}, bytesOut=${bytesOut:byteunit}");
                    attributes.put("context", contextMap);
                    modified = true;
                }

                if (modified)
                {
                    record = new ConfiguredObjectRecordImpl(record.getId(), record.getType(), attributes, record.getParents());
                    getUpdateMap().put(record.getId(), record);
                }

            }
            else if("Binding".equals(record.getType()))
            {
                BindingRecord binding = new BindingRecord(String.valueOf(record.getAttributes().get("name")),
                                                          record.getParents().get("Queue").toString(),
                                                          record.getAttributes().get("arguments"));
                final UUID exchangeId = record.getParents().get("Exchange");
                List<BindingRecord> existingBindings = _exchangeBindings.get(exchangeId);
                if(existingBindings == null)
                {
                    existingBindings = new ArrayList<>();
                    _exchangeBindings.put(exchangeId, existingBindings);
                }
                existingBindings.add(binding);
                getDeleteMap().put(record.getId(), record);
            }
            else if("Exchange".equals(record.getType()))
            {
                final UUID exchangeId = record.getId();
                _exchanges.put(exchangeId, record);
                if(record.getAttributes().containsKey("bindings"))
                {
                    List<BindingRecord> existingBindings = _exchangeBindings.get(exchangeId);
                    if(existingBindings == null)
                    {
                        existingBindings = new ArrayList<>();
                        _exchangeBindings.put(exchangeId, existingBindings);
                    }

                    List<Map<String,Object>> bindingList =
                            (List<Map<String, Object>>) record.getAttributes().get("bindings");
                    for(Map<String,Object> existingBinding : bindingList)
                    {
                        existingBindings.add(new BindingRecord((String)existingBinding.get("name"),
                                                               String.valueOf(existingBinding.get("queue")),
                                                               existingBinding.get("arguments")));
                    }
                }

                if (record.getAttributes().containsKey("alternateExchange"))
                {
                    _destinationsWithAlternateExchange.add(record.getId());

                    getUpdateMap().put(record.getId(), record);
                }
            }
            else if("Queue".equals(record.getType()))
            {
                Map<String, Object> attributes = new HashMap<>(record.getAttributes());
                Object queueFlowControlSizeBytes = attributes.remove("queueFlowControlSizeBytes");
                Object queueFlowResumeSizeBytes = attributes.remove("queueFlowResumeSizeBytes");
                if (queueFlowControlSizeBytes != null)
                {
                    long queueFlowControlSizeBytesValue = convertAttributeValueToLong("queueFlowControlSizeBytes",
                                                                                      queueFlowControlSizeBytes);
                    if (queueFlowControlSizeBytesValue > 0)
                    {
                        if (queueFlowResumeSizeBytes != null)
                        {
                            long queueFlowResumeSizeBytesValue =
                                    convertAttributeValueToLong("queueFlowResumeSizeBytes", queueFlowResumeSizeBytes);
                            double ratio = ((double) queueFlowResumeSizeBytesValue)
                                           / ((double) queueFlowControlSizeBytesValue);
                            String flowResumeLimit = String.format("%.2f", ratio * 100.0);

                            Object context = attributes.get("context");
                            Map<String, String> contextMap;
                            if (context instanceof Map)
                            {
                                contextMap = (Map) context;
                            }
                            else
                            {
                                contextMap = new HashMap<>();
                                attributes.put("context", contextMap);
                            }
                            contextMap.put("queue.queueFlowResumeLimit", flowResumeLimit);
                        }
                        attributes.put("overflowPolicy", "PRODUCER_FLOW_CONTROL");
                        attributes.put("maximumQueueDepthBytes", queueFlowControlSizeBytes);
                    }
                }

                boolean addToUpdateMap = false;
                if (attributes.containsKey("alternateExchange"))
                {
                    _destinationsWithAlternateExchange.add(record.getId());
                    addToUpdateMap = true;

                }

                if(attributes.containsKey("bindings"))
                {
                    _queueBindings.put(String.valueOf(attributes.get("name")),
                                       (List<Map<String, Object>>) attributes.get("bindings"));
                    attributes.remove("bindings");
                }

                if(attributes.containsKey("messageGroupKey"))
                {
                    if(attributes.containsKey("messageGroupSharedGroups")
                       && convertAttributeValueToBoolean("messageGroupSharedGroups",
                                                         attributes.remove("messageGroupSharedGroups")))
                    {
                        attributes.put("messageGroupType", "SHARED_GROUPS");

                    }
                    else
                    {
                        attributes.put("messageGroupType", "STANDARD");
                    }
                    Object oldMessageGroupKey = attributes.remove("messageGroupKey");
                    if (!"JMSXGroupId".equals(oldMessageGroupKey))
                    {
                        attributes.put("messageGroupKeyOverride", oldMessageGroupKey);
                    }
                }
                else
                {
                    attributes.put("messageGroupType", "NONE");
                }

                _queues.put(record.getId(), (String) attributes.get("name"));

                if (!attributes.equals(new HashMap<>(record.getAttributes())) || addToUpdateMap)
                {
                    getUpdateMap().put(record.getId(),
                                       new ConfiguredObjectRecordImpl(record.getId(),
                                                                      record.getType(),
                                                                      attributes,
                                                                      record.getParents()));
                }
            }
            else if (record.getType().equals("VirtualHostLogger"))
            {
                Map<String,Object> attributes = new HashMap<>();
                attributes.put("name", "statistics-" + record.getAttributes().get("name"));
                attributes.put("level", "INFO");
                attributes.put("loggerName", "qpid.statistics.*");
                attributes.put("type", "NameAndLevel");


                final ConfiguredObjectRecord filterRecord = new ConfiguredObjectRecordImpl(UUID.randomUUID(),
                                                                                           "VirtualHostLogInclusionRule",
                                                                                           attributes,
                                                                                           Collections.singletonMap("VirtualHostLogger",
                                                                                                                    record.getId()));
                getUpdateMap().put(filterRecord.getId(), filterRecord);
            }
        }

        private long convertAttributeValueToLong(final String attributeName,
                                                 final Object attributeValue)
        {
            long value;
            if (attributeValue instanceof Number)
            {
                value = ((Number) attributeValue).longValue();
            }
            else if (attributeValue instanceof String)
            {
                try
                {
                    value = Long.parseLong((String) attributeValue);
                }
                catch (Exception e)
                {
                    throw new IllegalConfigurationException(String.format(
                            "Cannot evaluate '%s': %s",
                            attributeName, attributeValue));
                }
            }
            else
            {
                throw new IllegalConfigurationException(String.format("Cannot evaluate '%s': %s",
                                                                      attributeName,
                                                                      String.valueOf(attributeValue)));
            }
            return value;
        }

        private boolean convertAttributeValueToBoolean(final String attributeName,
                                                       final Object attributeValue)
        {
            boolean value;
            if (attributeValue instanceof Boolean)
            {
                value = (Boolean) attributeValue;
            }
            else if (attributeValue instanceof String)
            {
                String strValue = (String)attributeValue;
                if(strValue.equalsIgnoreCase("true"))
                {
                    value = true;
                }
                else if(strValue.equalsIgnoreCase("false"))
                {
                    value = false;
                }
                else
                {
                    throw new IllegalConfigurationException(String.format(
                            "Cannot evaluate '%s': %s",
                            attributeName, attributeValue));
                }

            }
            else
            {
                throw new IllegalConfigurationException(String.format("Cannot evaluate '%s': %s",
                                                                      attributeName,
                                                                      String.valueOf(attributeValue)));
            }
            return value;
        }


        @Override
        public void complete()
        {
            for(Map.Entry<String, List<Map<String,Object>>> entry : _queueBindings.entrySet())
            {
                for(Map<String, Object> existingBinding : entry.getValue())
                {
                    UUID exchangeId;
                    if(existingBinding.get("exchange") instanceof UUID)
                    {
                        exchangeId = (UUID) existingBinding.get("exchange");
                    }
                    else
                    {
                        exchangeId = getExchangeIdFromNameOrId( existingBinding.get("exchange").toString());
                    }
                    List<BindingRecord> existingBindings = _exchangeBindings.get(exchangeId);
                    if(existingBindings == null)
                    {
                        existingBindings = new ArrayList<>();
                        _exchangeBindings.put(exchangeId, existingBindings);
                    }
                    existingBindings.add(new BindingRecord((String)existingBinding.get("name"),
                                                           entry.getKey(),
                                                           existingBinding.get("arguments")));
                }
            }

            for(Map.Entry<UUID, List<BindingRecord>> entry : _exchangeBindings.entrySet())
            {
                ConfiguredObjectRecord exchangeRecord = _exchanges.get(entry.getKey());
                if(exchangeRecord != null)
                {
                    final List<BindingRecord> bindingRecords = entry.getValue();
                    List<Map<String,Object>> actualBindings = new ArrayList<>(bindingRecords.size());
                    for(BindingRecord bindingRecord : bindingRecords)
                    {
                        if(bindingRecord._arguments == null)
                        {
                            actualBindings.add(NO_ARGUMENTS_BINDING_MAP_CREATOR.createMap(bindingRecord._name,
                                                                                          getQueueFromIdOrName(bindingRecord)));
                        }
                        else
                        {
                            actualBindings.add(BINDING_MAP_CREATOR.createMap(bindingRecord._name,
                                                                             getQueueFromIdOrName(bindingRecord), bindingRecord._arguments));
                        }
                    }
                    Map<String, Object> updatedAttributes = new HashMap<>(exchangeRecord.getAttributes());
                    updatedAttributes.remove("bindings");
                    updatedAttributes.put("durableBindings", actualBindings);
                    exchangeRecord = new ConfiguredObjectRecordImpl(exchangeRecord.getId(), exchangeRecord.getType(), updatedAttributes, exchangeRecord.getParents());
                    getUpdateMap().put(exchangeRecord.getId(), exchangeRecord);
                }

            }

            for (UUID recordId : _destinationsWithAlternateExchange)
            {
                ConfiguredObjectRecord record = getUpdateMap().get(recordId);
                Map<String, Object> attributes = new HashMap<>(record.getAttributes());

                String exchangeNameOrUuid = String.valueOf(attributes.remove("alternateExchange"));

                ConfiguredObjectRecord exchangeRecord = getExchangeFromNameOrUUID(exchangeNameOrUuid);
                if (exchangeRecord != null)
                {
                    attributes.put("alternateBinding",
                                   Collections.singletonMap("destination", exchangeRecord.getAttributes().get("name")));
                }
                else
                {
                    throw new IllegalConfigurationException(String.format(
                            "Cannot upgrade record UUID '%s' as cannot find exchange with name or UUID '%s'",
                            recordId,
                            exchangeNameOrUuid));
                }

                getUpdateMap().put(record.getId(),
                                   new ConfiguredObjectRecordImpl(record.getId(),
                                                                  record.getType(),
                                                                  attributes,
                                                                  record.getParents()));
            }
        }

        private ConfiguredObjectRecord getExchangeFromNameOrUUID(final String exchangeNameOrUuid)
        {
            for(ConfiguredObjectRecord record : _exchanges.values())
            {
                if(exchangeNameOrUuid.equals(record.getAttributes().get("name")))
                {
                    return record;
                }
                else
                {
                    try
                    {
                        UUID uuid = UUID.fromString(exchangeNameOrUuid);
                        if (uuid.equals(record.getId()))
                        {
                            return record;
                        }
                    }
                    catch (IllegalArgumentException e)
                    {
                        // ignore - not a UUID
                    }
                }
            }

            return null;
        }

        private UUID getExchangeIdFromNameOrId(final String exchange)
        {
            for(ConfiguredObjectRecord record : _exchanges.values())
            {
                if(exchange.equals(record.getAttributes().get("name")))
                {
                    return record.getId();
                }
            }
            return UUID.fromString(exchange);
        }

        private String getQueueFromIdOrName(final BindingRecord bindingRecord)
        {
            final String queueIdOrName = bindingRecord._queueIdOrName;
            try
            {
                UUID queueId = UUID.fromString(queueIdOrName);
                String name = _queues.get(queueId);
                return name == null ? queueIdOrName : name;
            }
            catch(IllegalArgumentException e)
            {
                return queueIdOrName;
            }
        }

        private class BindingRecord
        {
            private final String _name;
            private final String _queueIdOrName;
            private final Object _arguments;

            public BindingRecord(final String name, final String queueIdOrName, final Object arguments)
            {
                _name = name;
                _queueIdOrName = queueIdOrName;
                _arguments = arguments;
            }
        }
    }


    private class Upgrader_7_0_to_7_1 extends StoreUpgraderPhase
    {

        public Upgrader_7_0_to_7_1()
        {
            super("modelVersion", "7.0", "7.1");
        }

        @Override
        public void configuredObject(final ConfiguredObjectRecord record)
        {
            if("VirtualHost".equals(record.getType()))
            {
                upgradeRootRecord(record);
            }
        }

        @Override
        public void complete()
        {

        }
    }

    private class Upgrader_7_1_to_8_0 extends StoreUpgraderPhase
    {

        public Upgrader_7_1_to_8_0()
        {
            super("modelVersion", "7.1", "8.0");
        }

        @Override
        public void configuredObject(final ConfiguredObjectRecord record)
        {
            if("VirtualHost".equals(record.getType()))
            {
                upgradeRootRecord(record);
            }
        }

        @Override
        public void complete()
        {

        }
    }

    public boolean upgradeAndRecover(final DurableConfigurationStore durableConfigurationStore,
                                     final ConfiguredObjectRecord... initialRecords)
    {
        final List<ConfiguredObjectRecord> records = new ArrayList<>();
        boolean isNew = durableConfigurationStore.openConfigurationStore(new ConfiguredObjectRecordHandler()
        {
            @Override
            public void handle(final ConfiguredObjectRecord record)
            {
                records.add(record);
            }
        }, initialRecords);

        List<ConfiguredObjectRecord> upgradedRecords = upgrade(durableConfigurationStore,
                                                               records,
                                                               VirtualHost.class.getSimpleName(),
                                                               VirtualHost.MODEL_VERSION);
        recover(_virtualHostNode, durableConfigurationStore, upgradedRecords, isNew);
        return isNew;
    }

    public void reloadAndRecover(final DurableConfigurationStore durableConfigurationStore)
    {
        reloadAndRecoverInternal(_virtualHostNode, durableConfigurationStore);
    }

    public void reloadAndRecoverVirtualHost(final DurableConfigurationStore durableConfigurationStore)
    {
        reloadAndRecoverInternal(_virtualHostNode.getVirtualHost(), durableConfigurationStore);
    }

    private void reloadAndRecoverInternal(final ConfiguredObject<?> recoveryRoot,
                                          final DurableConfigurationStore durableConfigurationStore)
    {
        final List<ConfiguredObjectRecord> records = new ArrayList<>();
        durableConfigurationStore.reload(records::add);
        recover(recoveryRoot, durableConfigurationStore, records, false);
    }

    private void recover(final ConfiguredObject<?> recoveryRoot,
                         final DurableConfigurationStore durableConfigurationStore,
                         final List<ConfiguredObjectRecord> records,
                         final boolean isNew)
    {
        new GenericRecoverer(recoveryRoot).recover(records, isNew);

        final StoreConfigurationChangeListener
                configChangeListener = new StoreConfigurationChangeListener(durableConfigurationStore);
        if(_virtualHostNode.getVirtualHost() != null)
        {
            applyRecursively(_virtualHostNode.getVirtualHost(), new RecursiveAction<ConfiguredObject<?>>()
            {
                @Override
                public boolean applyToChildren(final ConfiguredObject<?> object)
                {
                    return object.isDurable();
                }

                @Override
                public void performAction(final ConfiguredObject<?> object)
                {
                    object.addChangeListener(configChangeListener);
                }
            });
        }

        if (recoveryRoot instanceof VirtualHostNode)
        {
            _virtualHostNode.addChangeListener(new AbstractConfigurationChangeListener()
            {
                @Override
                public void childAdded(final ConfiguredObject<?> object, final ConfiguredObject<?> child)
                {
                    if (child instanceof VirtualHost)
                    {
                        applyRecursively(child, new RecursiveAction<ConfiguredObject<?>>()
                        {
                            @Override
                            public boolean applyToChildren(final ConfiguredObject<?> object)
                            {
                                return object.isDurable();
                            }

                            @Override
                            public void performAction(final ConfiguredObject<?> object)
                            {
                                if (object.isDurable())
                                {
                                    durableConfigurationStore.update(true, object.asObjectRecord());
                                    object.addChangeListener(configChangeListener);
                                }
                            }
                        });
                    }
                }

                @Override
                public void childRemoved(final ConfiguredObject<?> object, final ConfiguredObject<?> child)
                {
                    if (child instanceof VirtualHost)
                    {
                        child.removeChangeListener(configChangeListener);
                    }
                }
            });
            if (isNew)
            {
                if (_virtualHostNode instanceof AbstractConfiguredObject)
                {
                    ((AbstractConfiguredObject) _virtualHostNode).forceUpdateAllSecureAttributes();
                }
            }
        }
    }
}
