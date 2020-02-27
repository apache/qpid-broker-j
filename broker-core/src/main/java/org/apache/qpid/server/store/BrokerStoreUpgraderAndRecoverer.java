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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.configuration.store.StoreConfigurationChangeListener;
import org.apache.qpid.server.logging.LogLevel;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ContainerStoreUpgraderAndRecoverer;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.model.VirtualHostAlias;

public class BrokerStoreUpgraderAndRecoverer extends AbstractConfigurationStoreUpgraderAndRecoverer implements ContainerStoreUpgraderAndRecoverer<Broker>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BrokerStoreUpgraderAndRecoverer.class);

    public static final String VIRTUALHOSTS = "virtualhosts";
    private final SystemConfig<?> _systemConfig;

    // Note: don't use externally defined constants in upgraders in case they change, the values here MUST stay the same
    // no matter what changes are made to the code in the future
    public BrokerStoreUpgraderAndRecoverer(SystemConfig<?> systemConfig)
    {
        super("1.0");
        _systemConfig = systemConfig;

        register(new Upgrader_1_0_to_1_1());
        register(new Upgrader_1_1_to_1_2());
        register(new Upgrader_1_2_to_1_3());
        register(new Upgrader_1_3_to_2_0());
        register(new Upgrader_2_0_to_3_0());

        register(new Upgrader_3_0_to_6_0());
        register(new Upgrader_6_0_to_6_1());
        register(new Upgrader_6_1_to_7_0());
        register(new Upgrader_7_0_to_7_1());
        register(new Upgrader_7_1_to_8_0());
    }

    private static final class Upgrader_1_0_to_1_1 extends StoreUpgraderPhase
    {
        private Upgrader_1_0_to_1_1()
        {
            super("modelVersion", "1.0", "1.1");
        }

        @Override
        public void configuredObject(ConfiguredObjectRecord record)
        {
            if (record.getType().equals("Broker"))
            {
                record = upgradeRootRecord(record);
                createVirtualHostsRecordsFromBrokerRecordForModel_1_x(record, this);
            }
            else if (record.getType().equals("VirtualHost") && record.getAttributes().containsKey("storeType"))
            {
                Map<String, Object> updatedAttributes = new HashMap<String, Object>(record.getAttributes());
                updatedAttributes.put("type", "STANDARD");
                record = new ConfiguredObjectRecordImpl(record.getId(), record.getType(), updatedAttributes, record.getParents());
                getUpdateMap().put(record.getId(), record);
            }

        }

        @Override
        public void complete()
        {
        }

    }

    private static final class Upgrader_1_1_to_1_2 extends StoreUpgraderPhase
    {
        private Upgrader_1_1_to_1_2()
        {
            super("modelVersion", "1.1", "1.2");
        }

        @Override
        public void configuredObject(ConfiguredObjectRecord record)
        {
            if (record.getType().equals("Broker"))
            {
                record = upgradeRootRecord(record);
                createVirtualHostsRecordsFromBrokerRecordForModel_1_x(record, this);
            }
        }

        @Override
        public void complete()
        {
        }

    }

    private static final class Upgrader_1_2_to_1_3 extends StoreUpgraderPhase
    {
        private Upgrader_1_2_to_1_3()
        {
            super("modelVersion", "1.2", "1.3");
        }

        @Override
        public void configuredObject(ConfiguredObjectRecord record)
        {
            if (record.getType().equals("TrustStore") && record.getAttributes().containsKey("type"))
            {
                Map<String, Object> updatedAttributes = new HashMap<String, Object>(record.getAttributes());
                updatedAttributes.put("trustStoreType", updatedAttributes.remove("type"));
                record = new ConfiguredObjectRecordImpl(record.getId(), record.getType(), updatedAttributes, record.getParents());
                getUpdateMap().put(record.getId(), record);

            }
            else if (record.getType().equals("KeyStore") && record.getAttributes().containsKey("type"))
            {
                Map<String, Object> updatedAttributes = new HashMap<String, Object>(record.getAttributes());
                updatedAttributes.put("keyStoreType", updatedAttributes.remove("type"));
                record = new ConfiguredObjectRecordImpl(record.getId(), record.getType(), updatedAttributes, record.getParents());
                getUpdateMap().put(record.getId(), record);

            }
            else if (record.getType().equals("Broker"))
            {
                record = upgradeRootRecord(record);
                createVirtualHostsRecordsFromBrokerRecordForModel_1_x(record, this);
            }
        }

        @Override
        public void complete()
        {
        }

    }

    private static final class Upgrader_1_3_to_2_0 extends StoreUpgraderPhase
    {
        private final VirtualHostEntryUpgrader _virtualHostUpgrader;

        private Upgrader_1_3_to_2_0()
        {
            super("modelVersion", "1.3", "2.0");
            _virtualHostUpgrader = new VirtualHostEntryUpgrader();
        }

        @Override
        public void configuredObject(ConfiguredObjectRecord record)
        {
            if (record.getType().equals("VirtualHost"))
            {
                Map<String, Object> attributes = record.getAttributes();
                if (attributes.containsKey("configPath"))
                {
                    throw new IllegalConfigurationException("Auto-upgrade of virtual host " + attributes.get("name") + " having XML configuration is not supported. Virtual host configuration file is " + attributes.get("configPath"));
                }

                record = _virtualHostUpgrader.upgrade(record);
                getUpdateMap().put(record.getId(), record);
            }
            else if (record.getType().equals("Plugin") && record.getAttributes().containsKey("pluginType"))
            {
                Map<String, Object> updatedAttributes = new HashMap<String, Object>(record.getAttributes());
                updatedAttributes.put("type", updatedAttributes.remove("pluginType"));
                record = new ConfiguredObjectRecordImpl(record.getId(), record.getType(), updatedAttributes, record.getParents());
                getUpdateMap().put(record.getId(), record);

            }
            else if (record.getType().equals("Broker"))
            {
                record = upgradeRootRecord(record);
                createVirtualHostsRecordsFromBrokerRecordForModel_1_x(record, this);
            }

        }

        @Override
        public void complete()
        {
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
            if(record.getType().equals("Port") && isAmqpPort(record.getAttributes()))
            {
                createAliasRecord(record, "nameAlias", "nameAlias");
                createAliasRecord(record, "defaultAlias", "defaultAlias");
                createAliasRecord(record, "hostnameAlias", "hostnameAlias");

            }
            else if(record.getType().equals("User") && "scram".equals(record.getAttributes().get("type")) )
            {
                Map<String, Object> updatedAttributes = new HashMap<String, Object>(record.getAttributes());
                updatedAttributes.put("type", "managed");
                record = new ConfiguredObjectRecordImpl(record.getId(), record.getType(), updatedAttributes, record.getParents());
                getUpdateMap().put(record.getId(), record);
            }
            else if (record.getType().equals("Broker"))
            {
                upgradeRootRecord(record);
            }
            else if("KeyStore".equals(record.getType()))
            {
                upgradeKeyStoreRecordIfTypeTheSame(record, "FileKeyStore");
            }
            else if("TrustStore".equals(record.getType()))
            {
                upgradeKeyStoreRecordIfTypeTheSame(record, "FileTrustStore");
            }
        }

        private ConfiguredObjectRecord upgradeKeyStoreRecordIfTypeTheSame(ConfiguredObjectRecord record, String expectedType)
        {
            Map<String, Object> attributes = new HashMap<>(record.getAttributes());
            // Type may not be present, in which case the default type - which is the type affected - will be being used
            if(!attributes.containsKey("type"))
            {
                attributes.put("type", expectedType);
            }
            if (expectedType.equals(attributes.get("type")))
            {
                Object path = attributes.remove("path");
                attributes.put("storeUrl", path);
                record = new ConfiguredObjectRecordImpl(record.getId(), record.getType(), attributes, record.getParents());
                getUpdateMap().put(record.getId(), record);
            }
            return record;
        }

        private boolean isAmqpPort(final Map<String, Object> attributes)
        {
            Object type = attributes.get(ConfiguredObject.TYPE);
            Object protocols = attributes.get(Port.PROTOCOLS);
            String protocolString = protocols == null ? null : protocols.toString();
            return "AMQP".equals(type)
                   || ((type == null || "".equals(type.toString().trim()))
                       && (protocolString == null
                           || !protocolString.matches(".*\\w.*")
                           || protocolString.contains("AMQP")));

        }

        private void createAliasRecord(ConfiguredObjectRecord parent, String name, String type)
        {
            Map<String,Object> attributes = new HashMap<>();
            attributes.put(VirtualHostAlias.NAME, name);
            attributes.put(VirtualHostAlias.TYPE, type);

            final ConfiguredObjectRecord record = new ConfiguredObjectRecordImpl(UUID.randomUUID(),
                                                                                 "VirtualHostAlias",
                                                                                 attributes,
                                                                                 Collections.singletonMap("Port", parent.getId()));
            getUpdateMap().put(record.getId(), record);
        }

        @Override
        public void complete()
        {
        }

    }
    private class Upgrader_3_0_to_6_0 extends StoreUpgraderPhase
    {
        private String _defaultVirtualHost;
        private final Set<ConfiguredObjectRecord> _knownBdbHaVirtualHostNode = new HashSet<>();
        private final Map<String, ConfiguredObjectRecord> _knownNonBdbHaVirtualHostNode = new HashMap<>();

        public Upgrader_3_0_to_6_0()
        {
            super("modelVersion", "3.0", "6.0");
        }

        @Override
        public void configuredObject(ConfiguredObjectRecord record)
        {
            if (record.getType().equals("Broker"))
            {
                record = upgradeRootRecord(record);

                Map<String, Object> brokerAttributes = new HashMap<>(record.getAttributes());
                _defaultVirtualHost = (String)brokerAttributes.remove("defaultVirtualHost");

                boolean typeDetected = brokerAttributes.remove("type") != null;

                if (_defaultVirtualHost != null || typeDetected)
                {
                    record = new ConfiguredObjectRecordImpl(record.getId(),
                                                            record.getType(),
                                                            brokerAttributes,
                                                            record.getParents());
                    getUpdateMap().put(record.getId(), record);
                }

                addLogger(record, "memory", "Memory");
                addLogger(record, "logfile", "File");
            }
            else if (record.getType().equals("VirtualHostNode"))
            {
                if ("BDB_HA".equals(record.getAttributes().get("type")))
                {
                    _knownBdbHaVirtualHostNode.add(record);
                }
                else
                {
                    String nodeName = (String) record.getAttributes().get("name");
                    _knownNonBdbHaVirtualHostNode.put(nodeName, record);
                }
            }
            else if (record.getType().equals("Port") && "AMQP".equals(record.getAttributes().get("type")))
            {
                Map<String, Object> updatedAttributes = new HashMap<>(record.getAttributes());
                if (updatedAttributes.containsKey("receiveBufferSize") || updatedAttributes.containsKey("sendBufferSize"))
                {
                    updatedAttributes.remove("receiveBufferSize");
                    updatedAttributes.remove("sendBufferSize");
                    record = new ConfiguredObjectRecordImpl(record.getId(), record.getType(), updatedAttributes, record.getParents());
                    getUpdateMap().put(record.getId(), record);
                }
            }
        }

        private void addLogger(final ConfiguredObjectRecord record, String name, String type)
        {
            Map<String,Object> attributes = new HashMap<>();
            attributes.put("name", name);
            attributes.put("type", type);
            final ConfiguredObjectRecord logger = new ConfiguredObjectRecordImpl(UUID.randomUUID(),
                    "BrokerLogger",
                    attributes,
                    Collections.singletonMap("Broker",
                            record.getId()));
            addNameValueFilter("Root", logger, LogLevel.WARN, "ROOT");
            addNameValueFilter("Qpid", logger, LogLevel.INFO, "org.apache.qpid.*");
            addNameValueFilter("Operational", logger, LogLevel.INFO, "qpid.message.*");
            getUpdateMap().put(logger.getId(), logger);
        }

        private void addNameValueFilter(String inclusionRuleName,
                                        final ConfiguredObjectRecord loggerRecord,
                                        final LogLevel level,
                                        final String loggerName)
        {
            Map<String,Object> attributes = new HashMap<>();
            attributes.put("name", inclusionRuleName);
            attributes.put("level", level.name());
            attributes.put("loggerName", loggerName);
            attributes.put("type", "NameAndLevel");


            final ConfiguredObjectRecord filterRecord = new ConfiguredObjectRecordImpl(UUID.randomUUID(),
                                                                                       "BrokerLogInclusionRule",
                                                                                       attributes,
                                                                                       Collections.singletonMap("BrokerLogger",
                                                                                                                loggerRecord.getId()));
            getUpdateMap().put(filterRecord.getId(), filterRecord);
        }

        @Override
        public void complete()
        {
            if (_defaultVirtualHost != null)
            {
                final ConfiguredObjectRecord defaultVirtualHostNode;
                if (_knownNonBdbHaVirtualHostNode.containsKey(_defaultVirtualHost))
                {
                    defaultVirtualHostNode = _knownNonBdbHaVirtualHostNode.get(_defaultVirtualHost);
                }
                else if (_knownBdbHaVirtualHostNode.size() == 1)
                {
                    // We had a default VHN but it didn't match the non-BDBHAVHNs and we have only one BDBHAVHN.
                    // It has to be the target.
                    defaultVirtualHostNode = _knownBdbHaVirtualHostNode.iterator().next();

                }
                else
                {
                    LOGGER.warn("Unable to identify the target virtual host node for old default virtualhost name : '{}'",
                                _defaultVirtualHost);
                    defaultVirtualHostNode = null;
                }

                if (defaultVirtualHostNode != null)
                {
                    final Map<String, Object> updatedAttributes = new HashMap<>(defaultVirtualHostNode.getAttributes());
                    updatedAttributes.put("defaultVirtualHostNode", "true");

                    ConfiguredObjectRecordImpl updateRecord =
                            new ConfiguredObjectRecordImpl(defaultVirtualHostNode.getId(),
                                                           defaultVirtualHostNode.getType(),
                                                           updatedAttributes,
                                                           defaultVirtualHostNode.getParents());
                    getUpdateMap().put(updateRecord.getId(), updateRecord);

                }

            }
        }

    }

    private class Upgrader_6_0_to_6_1 extends StoreUpgraderPhase
    {
        private boolean _hasAcl = false;
        private UUID _rootRecordId;

        public Upgrader_6_0_to_6_1()
        {
            super("modelVersion", "6.0", "6.1");
        }

        @Override
        public void configuredObject(ConfiguredObjectRecord record)
        {
            if (record.getType().equals("Broker"))
            {
                record = upgradeRootRecord(record);
                _rootRecordId = record.getId();
            }
            else if (record.getType().equals("TrustStore"))
            {
                upgradeTrustStore(record);
            }
            else
            {
                Map<String, Object> attributes = record.getAttributes();
                String type = (String)attributes.get("type");
                if (record.getType().equals("Plugin") && "MANAGEMENT-JMX".equals(type))
                {
                    getDeleteMap().put(record.getId(), record);
                }
                else if (record.getType().equals("Port"))
                {
                    Object protocols = attributes.get("protocols");
                    if ((protocols instanceof Collection && (((Collection) protocols).contains("RMI")
                                                             || ((Collection) protocols).contains("JMX_RMI")))
                        || "JMX".equals(type)
                        || "RMI".equals(type))
                    {
                        getDeleteMap().put(record.getId(), record);
                    }
                }
                else if (record.getType().equals("AuthenticationProvider") && attributes.containsKey("preferencesproviders"))
                {
                    // removing of Preferences Provider from AuthenticationProvider attribute for JSON configuration store
                    Map<String, Object> updatedAttributes = new LinkedHashMap<>(attributes);
                    updatedAttributes.remove("preferencesproviders");
                    record = new ConfiguredObjectRecordImpl(record.getId(), record.getType(), updatedAttributes, record.getParents());
                    getUpdateMap().put(record.getId(), record);
                }
                else if (record.getType().equals("PreferencesProvider"))
                {
                    // removing of f Preferences Provider record for non-JSON configuration store
                    getDeleteMap().put(record.getId(), record);
                }
            }
        }

        private void upgradeTrustStore(ConfiguredObjectRecord record)
        {
            Map<String, Object> updatedAttributes = new LinkedHashMap<>(record.getAttributes());
            if (updatedAttributes.containsKey("includedVirtualHostMessageSources")
                || updatedAttributes.containsKey("excludedVirtualHostMessageSources"))
            {
                if (updatedAttributes.containsKey("includedVirtualHostMessageSources"))
                {
                    LOGGER.warn("Detected 'includedVirtualHostMessageSources' attribute during upgrade."
                                + " Starting with version 6.1 this attribute has been replaced with"
                                + " 'includedVirtualHostNodeMessageSources'. The upgrade is automatic but"
                                + " assumes that the VirtualHostNode has the same name as the VirtualHost."
                                + " Assumed name: '{}'", updatedAttributes.get("includedVirtualHostMessageSources"));
                    updatedAttributes.put("includedVirtualHostNodeMessageSources",
                                          updatedAttributes.get("includedVirtualHostMessageSources"));
                    updatedAttributes.remove("includedVirtualHostMessageSources");

                }
                if (updatedAttributes.containsKey("excludedVirtualHostMessageSources"))
                {
                    LOGGER.warn("Detected 'excludedVirtualHostMessageSources' attribute during upgrade."
                                + " Starting with version 6.1 this attribute has been replaced with"
                                + " 'excludedVirtualHostNodeMessageSources'. The upgrade is automatic but"
                                + " assumes that the VirtualHostNode has the same name as the VirtualHost."
                                + " Assumed name: '{}'", updatedAttributes.get("excludedVirtualHostMessageSources"));
                    updatedAttributes.put("excludedVirtualHostNodeMessageSources",
                                          updatedAttributes.get("excludedVirtualHostMessageSources"));
                    updatedAttributes.remove("excludedVirtualHostMessageSources");
                }
                record = new ConfiguredObjectRecordImpl(record.getId(), record.getType(), updatedAttributes, record.getParents());
                getUpdateMap().put(record.getId(), record);
            }
        }

        @Override
        public void complete()
        {
            if(!_hasAcl)
            {
                UUID allowAllACLId = UUID.randomUUID();
                Map<String,Object> attrs = new HashMap<>();
                attrs.put(ConfiguredObject.NAME, "AllowAll");
                attrs.put(ConfiguredObject.TYPE, "AllowAll");
                attrs.put("priority", 9999);
                ConfiguredObjectRecord allowAllAclRecord =
                        new ConfiguredObjectRecordImpl(allowAllACLId, "AccessControlProvider", attrs, Collections.singletonMap("Broker", _rootRecordId));
                getUpdateMap().put(allowAllAclRecord.getId(), allowAllAclRecord);

            }
        }

    }

    private class Upgrader_6_1_to_7_0 extends StoreUpgraderPhase
    {

        private Map<String,String> BROKER_ATTRIBUTES_MOVED_INTO_CONTEXT = new HashMap<>();
        {
            BROKER_ATTRIBUTES_MOVED_INTO_CONTEXT.put("connection.sessionCountLimit", "qpid.port.sessionCountLimit");
            BROKER_ATTRIBUTES_MOVED_INTO_CONTEXT.put("connection.heartBeatDelay", "qpid.port.heartbeatDelay");
            BROKER_ATTRIBUTES_MOVED_INTO_CONTEXT.put("connection.closeWhenNoRoute", "qpid.port.closeWhenNoRoute");
        };

        public Upgrader_6_1_to_7_0()
        {
            super("modelVersion", "6.1", "7.0");
        }

        @Override
        public void configuredObject(ConfiguredObjectRecord record)
        {
            if (record.getType().equals("Broker"))
            {
                boolean rebuildRecord = false;
                Map<String, Object> attributes = new HashMap<>(record.getAttributes());
                Map<String, String> additionalContext = new HashMap<>();
                for (String attributeName : BROKER_ATTRIBUTES_MOVED_INTO_CONTEXT.keySet())
                {
                    Object value = attributes.remove(attributeName);
                    if (value != null)
                    {
                        additionalContext.put(BROKER_ATTRIBUTES_MOVED_INTO_CONTEXT.get(attributeName),
                                              String.valueOf(value));
                    }
                }

                if (attributes.containsKey("statisticsReportingResetEnabled"))
                {
                    attributes.remove("statisticsReportingResetEnabled");
                    rebuildRecord = true;
                }

                if (attributes.containsKey("statisticsReportingPeriod")
                    && Integer.parseInt(String.valueOf(attributes.get("statisticsReportingPeriod"))) > 0)
                {
                    additionalContext.put("qpid.broker.statisticsReportPattern", "messagesIn=${messagesIn}, bytesIn=${bytesIn:byteunit}, messagesOut=${messagesOut}, bytesOut=${bytesOut:byteunit}");

                    rebuildRecord = true;
                }

                if (!additionalContext.isEmpty())
                {
                    Map<String, String> newContext = new HashMap<>();
                    if (attributes.containsKey("context"))
                    {
                        newContext.putAll((Map<String, String>) attributes.get("context"));
                    }
                    newContext.putAll(additionalContext);
                    attributes.put("context", newContext);

                    rebuildRecord = true;
                }

                if (rebuildRecord)
                {
                    record = new ConfiguredObjectRecordImpl(record.getId(),
                                                            record.getType(),
                                                            attributes,
                                                            record.getParents());
                }

                upgradeRootRecord(record);
            }
            else if (record.getType().equals("Port"))
            {
                Map<String, Object> attributes = record.getAttributes();
                Object protocols = attributes.get("protocols");
                String type = (String) attributes.get("type");
                if ((protocols instanceof Collection && ((Collection) protocols).contains("HTTP"))
                    || "HTTP".equals(type))
                {
                    upgradeHttpPortIfRequired(record);
                }
            }
            else if (record.getType().equals("BrokerLogger"))
            {
                Map<String,Object> attributes = new HashMap<>();
                attributes.put("name", "statistics-" + record.getAttributes().get("name"));
                attributes.put("level", "INFO");
                attributes.put("loggerName", "qpid.statistics.*");
                attributes.put("type", "NameAndLevel");


                final ConfiguredObjectRecord filterRecord = new ConfiguredObjectRecordImpl(UUID.randomUUID(),
                                                                                           "BrokerLogInclusionRule",
                                                                                           attributes,
                                                                                           Collections.singletonMap("BrokerLogger",
                                                                                                                    record.getId()));
                getUpdateMap().put(filterRecord.getId(), filterRecord);
            }
        }

        private void upgradeHttpPortIfRequired(final ConfiguredObjectRecord record)
        {
            Map<String, Object> attributes = record.getAttributes();
            if (attributes.containsKey("context"))
            {
                Map<String, String> context = (Map<String, String>) attributes.get("context");
                if (context != null
                    && (context.containsKey("port.http.additionalInternalThreads")
                        || context.containsKey("port.http.maximumQueuedRequests")))
                {
                    Map<String, String> updatedContext = new HashMap<>(context);
                    updatedContext.remove("port.http.additionalInternalThreads");
                    String acceptorsBacklog = updatedContext.remove("port.http.maximumQueuedRequests");
                    if (acceptorsBacklog != null)
                    {
                        updatedContext.put("qpid.port.http.acceptBacklog", acceptorsBacklog);
                    }
                    Map<String, Object> updatedAttributes = new LinkedHashMap<>(attributes);
                    updatedAttributes.put("context", updatedContext);

                    ConfiguredObjectRecord upgradedRecord = new ConfiguredObjectRecordImpl(record.getId(),
                                                                                           record.getType(),
                                                                                           updatedAttributes,
                                                                                           record.getParents());
                    getUpdateMap().put(upgradedRecord.getId(), upgradedRecord);
                }
            }
        }

        @Override
        public void complete()
        {

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
            if("Broker".equals(record.getType()))
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
            if("Broker".equals(record.getType()))
            {
                upgradeRootRecord(record);
            }
        }

        @Override
        public void complete()
        {

        }
    }

    private static class VirtualHostEntryUpgrader
    {
        @SuppressWarnings("serial")
        Map<String, AttributesTransformer> _messageStoreToNodeTransformers = new HashMap<String, AttributesTransformer>()
        {{
                put("DERBY", new AttributesTransformer().
                        addAttributeTransformer("id", copyAttribute()).
                        addAttributeTransformer("name", copyAttribute()).
                        addAttributeTransformer("createdTime", copyAttribute()).
                        addAttributeTransformer("createdBy", copyAttribute()).
                        addAttributeTransformer("storePath", copyAttribute()).
                        addAttributeTransformer("storeUnderfullSize", copyAttribute()).
                        addAttributeTransformer("storeOverfullSize", copyAttribute()));
                put("Memory",  new AttributesTransformer().
                        addAttributeTransformer("id", copyAttribute()).
                        addAttributeTransformer("name", copyAttribute()).
                        addAttributeTransformer("createdTime", copyAttribute()).
                        addAttributeTransformer("createdBy", copyAttribute()));
                put("BDB", new AttributesTransformer().
                        addAttributeTransformer("id", copyAttribute()).
                        addAttributeTransformer("name", copyAttribute()).
                        addAttributeTransformer("createdTime", copyAttribute()).
                        addAttributeTransformer("createdBy", copyAttribute()).
                        addAttributeTransformer("storePath", copyAttribute()).
                        addAttributeTransformer("storeUnderfullSize", copyAttribute()).
                        addAttributeTransformer("storeOverfullSize", copyAttribute()).
                        addAttributeTransformer("bdbEnvironmentConfig", mutateAttributeName("context")));
                put("JDBC", new AttributesTransformer().
                        addAttributeTransformer("id", copyAttribute()).
                        addAttributeTransformer("name", copyAttribute()).
                        addAttributeTransformer("createdTime", copyAttribute()).
                        addAttributeTransformer("createdBy", copyAttribute()).
                        addAttributeTransformer("storePath", mutateAttributeName("connectionURL")).
                        addAttributeTransformer("connectionURL", mutateAttributeName("connectionUrl")).
                        addAttributeTransformer("connectionPool", new AttributeTransformer()
                        {
                            @Override
                            public MutableEntry transform(MutableEntry entry)
                            {
                               Object value = entry.getValue();
                                if ("DEFAULT".equals(value))
                                {
                                    value = "NONE";
                                }
                                return new MutableEntry("connectionPoolType", value);
                            }
                        }).
                        addAttributeTransformer("jdbcBigIntType", addContextVar("qpid.jdbcstore.bigIntType")).
                        addAttributeTransformer("jdbcBytesForBlob", addContextVar("qpid.jdbcstore.useBytesForBlob")).
                        addAttributeTransformer("jdbcBlobType", addContextVar("qpid.jdbcstore.blobType")).
                        addAttributeTransformer("jdbcVarbinaryType", addContextVar("qpid.jdbcstore.varBinaryType")).
                        addAttributeTransformer("partitionCount", addContextVar("qpid.jdbcstore.bonecp.partitionCount")).
                        addAttributeTransformer("maxConnectionsPerPartition", addContextVar("qpid.jdbcstore.bonecp.maxConnectionsPerPartition")).
                        addAttributeTransformer("minConnectionsPerPartition", addContextVar("qpid.jdbcstore.bonecp.minConnectionsPerPartition")));
                put("BDB_HA", new AttributesTransformer().
                        addAttributeTransformer("id", copyAttribute()).
                        addAttributeTransformer("createdTime", copyAttribute()).
                        addAttributeTransformer("createdBy", copyAttribute()).
                        addAttributeTransformer("storePath", copyAttribute()).
                        addAttributeTransformer("storeUnderfullSize", copyAttribute()).
                        addAttributeTransformer("storeOverfullSize", copyAttribute()).
                        addAttributeTransformer("haNodeName", mutateAttributeName("name")).
                        addAttributeTransformer("haGroupName", mutateAttributeName("groupName")).
                        addAttributeTransformer("haHelperAddress", mutateAttributeName("helperAddress")).
                        addAttributeTransformer("haNodeAddress", mutateAttributeName("address")).
                        addAttributeTransformer("haDesignatedPrimary", mutateAttributeName("designatedPrimary")).
                        addAttributeTransformer("haReplicationConfig", mutateAttributeName("context")).
                        addAttributeTransformer("bdbEnvironmentConfig", mutateAttributeName("context")));
            }};

        public ConfiguredObjectRecord upgrade(ConfiguredObjectRecord vhost)
        {
            Map<String, Object> attributes = vhost.getAttributes();
            String type = (String) attributes.get("type");
            AttributesTransformer nodeAttributeTransformer = null;
            if ("STANDARD".equalsIgnoreCase(type))
            {
                if (attributes.containsKey("configStoreType"))
                {
                    throw new IllegalConfigurationException("Auto-upgrade of virtual host " + attributes.get("name")
                            + " with split configuration and message store is not supported."
                            + " Configuration store type is " + attributes.get("configStoreType") + " and message store type is "
                            + attributes.get("storeType"));
                }
                else
                {
                    type = (String) attributes.get("storeType");
                }
            }

            if (type == null)
            {
                throw new IllegalConfigurationException("Cannot auto-upgrade virtual host with attributes: " + attributes);
            }

            type = getVirtualHostNodeType(type);
            nodeAttributeTransformer = _messageStoreToNodeTransformers.get(type);

            if (nodeAttributeTransformer == null)
            {
                throw new IllegalConfigurationException("Don't know how to perform an upgrade from version for virtualhost type " + type);
            }

            Map<String, Object> nodeAttributes = nodeAttributeTransformer.upgrade(attributes);
            nodeAttributes.put("type", type);
            return new ConfiguredObjectRecordImpl(vhost.getId(), "VirtualHostNode", nodeAttributes, vhost.getParents());
        }

        private String getVirtualHostNodeType(String type)
        {
            for (String t : _messageStoreToNodeTransformers.keySet())
            {
                if (type.equalsIgnoreCase(t))
                {
                    return t;
                }
            }
            return null;
        }
    }

    private static class AttributesTransformer
    {
        private final Map<String, List<AttributeTransformer>> _transformers = new HashMap<String, List<AttributeTransformer>>();

        public AttributesTransformer addAttributeTransformer(String string, AttributeTransformer... attributeTransformers)
        {
            _transformers.put(string, Arrays.asList(attributeTransformers));
            return this;
        }

        public Map<String, Object> upgrade(Map<String, Object> attributes)
        {
            Map<String, Object> settings = new HashMap<>();
            for (Map.Entry<String, List<AttributeTransformer>> entry : _transformers.entrySet())
            {
                String attributeName = entry.getKey();
                if (attributes.containsKey(attributeName))
                {
                    Object attributeValue = attributes.get(attributeName);
                    MutableEntry newEntry = new MutableEntry(attributeName, attributeValue);

                    List<AttributeTransformer> transformers = entry.getValue();
                    for (AttributeTransformer attributeTransformer : transformers)
                    {
                        newEntry = attributeTransformer.transform(newEntry);
                        if (newEntry == null)
                        {
                            break;
                        }
                    }
                    if (newEntry != null)
                    {
                        if (settings.get(newEntry.getKey()) instanceof Map && newEntry.getValue() instanceof Map)
                        {
                            final Map newMap = (Map)newEntry.getValue();
                            final Map mergedMap = new HashMap((Map) settings.get(newEntry.getKey()));
                            mergedMap.putAll(newMap);
                            settings.put(newEntry.getKey(), mergedMap);
                        }
                        else
                        {
                            settings.put(newEntry.getKey(), newEntry.getValue());
                        }
                    }
                }
            }
            return settings;
        }
    }

    private static AttributeTransformer copyAttribute()
    {
        return CopyAttribute.INSTANCE;
    }

    private static AttributeTransformer mutateAttributeName(String newName)
    {
        return new MutateAttributeName(newName);
    }

    private static AttributeTransformer addContextVar(String newName)
    {
        return new AddContextVar(newName);
    }

    private static interface AttributeTransformer
    {
        MutableEntry transform(MutableEntry entry);
    }

    private static class CopyAttribute implements AttributeTransformer
    {
        private static final CopyAttribute INSTANCE = new CopyAttribute();

        private CopyAttribute()
        {
        }

        @Override
        public MutableEntry transform(MutableEntry entry)
        {
            return entry;
        }
    }

    private static class AddContextVar implements AttributeTransformer
    {
        private final String _newName;

        public AddContextVar(String newName)
        {
            _newName = newName;
        }

        @Override
        public MutableEntry transform(MutableEntry entry)
        {
            return new MutableEntry("context", Collections.singletonMap(_newName, entry.getValue()));
        }
    }

    private static class MutateAttributeName implements AttributeTransformer
    {
        private final String _newName;

        public MutateAttributeName(String newName)
        {
            _newName = newName;
        }

        @Override
        public MutableEntry transform(MutableEntry entry)
        {
            entry.setKey(_newName);
            return entry;
        }
    }

    private static class MutableEntry
    {
        private String _key;
        private Object _value;

        public MutableEntry(String key, Object value)
        {
            _key = key;
            _value = value;
        }

        public String getKey()
        {
            return _key;
        }

        public void setKey(String key)
        {
            _key = key;
        }

        public Object getValue()
        {
            return _value;
        }
    }

    private static ConfiguredObjectRecord createVirtualHostsRecordsFromBrokerRecordForModel_1_x(ConfiguredObjectRecord brokerRecord, StoreUpgraderPhase upgrader)
    {
        Map<String, Object> attributes = brokerRecord.getAttributes();
        if (attributes.containsKey(VIRTUALHOSTS) && attributes.get(VIRTUALHOSTS) instanceof Collection)
        {
            Collection<?> virtualHosts = (Collection<?>)attributes.get(VIRTUALHOSTS);
            for (Object virtualHost: virtualHosts)
            {
                if (virtualHost instanceof Map)
                {
                    Map<String, Object> virtualHostAttributes = (Map)virtualHost;
                    if (virtualHostAttributes.containsKey("configPath"))
                    {
                        throw new IllegalConfigurationException("Auto-upgrade of virtual host " + attributes.get("name")
                                + " having XML configuration is not supported. Virtual host configuration file is " + attributes.get("configPath"));
                    }

                    virtualHostAttributes = new HashMap<>(virtualHostAttributes);
                    Object nameAttribute = virtualHostAttributes.get("name");
                    Object idAttribute = virtualHostAttributes.remove("id");
                    UUID id;
                    if (idAttribute == null)
                    {
                        id = UUID.randomUUID();
                    }
                    else
                    {
                        if (idAttribute instanceof String)
                        {
                            id = UUID.fromString((String)idAttribute);
                        }
                        else if (idAttribute instanceof UUID)
                        {
                            id = (UUID)idAttribute;
                        }
                        else
                        {
                            throw new IllegalConfigurationException("Illegal ID value '" + idAttribute + "' for virtual host " + nameAttribute);
                        }
                    }

                    ConfiguredObjectRecord nodeRecord = new ConfiguredObjectRecordImpl(id, "VirtualHost", virtualHostAttributes, Collections.singletonMap("Broker", brokerRecord.getId()));

                    upgrader.getUpdateMap().put(nodeRecord.getId(), nodeRecord);
                    upgrader.configuredObject(nodeRecord);
                }
            }
            attributes = new HashMap<>(attributes);
            attributes.remove(VIRTUALHOSTS);
            brokerRecord = new ConfiguredObjectRecordImpl(brokerRecord.getId(), brokerRecord.getType(), attributes, brokerRecord.getParents());
            upgrader.getUpdateMap().put(brokerRecord.getId(), brokerRecord);
        }
        return brokerRecord;
    }

    @Override
    public Broker<?> upgradeAndRecover(List<ConfiguredObjectRecord> records)
    {
        final DurableConfigurationStore store = _systemConfig.getConfigurationStore();

        List<ConfiguredObjectRecord> upgradedRecords = upgrade(store, records);
        new GenericRecoverer(_systemConfig).recover(upgradedRecords, false);

        final StoreConfigurationChangeListener configChangeListener = new StoreConfigurationChangeListener(store);
        applyRecursively(_systemConfig.getContainer(Broker.class), new RecursiveAction<ConfiguredObject<?>>()
        {
            @Override
            public void performAction(final ConfiguredObject<?> object)
            {
                object.addChangeListener(configChangeListener);
            }

            @Override
            public boolean applyToChildren(ConfiguredObject<?> object)
            {
                return !object.managesChildStorage();
            }
        });

        return _systemConfig.getContainer(Broker.class);
    }


    public List<ConfiguredObjectRecord> upgrade(final DurableConfigurationStore dcs,
                                                final List<ConfiguredObjectRecord> records)
    {
        return upgrade(dcs, records, Broker.class.getSimpleName(), Broker.MODEL_VERSION);
    }
}
