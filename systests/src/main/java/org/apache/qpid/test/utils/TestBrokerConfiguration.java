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
package org.apache.qpid.test.utils;

import static org.mockito.Mockito.mock;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.management.plugin.HttpManagement;
import org.apache.qpid.server.model.AbstractSystemConfig;
import org.apache.qpid.server.model.AccessControlProvider;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.GroupProvider;
import org.apache.qpid.server.model.JsonSystemConfigImpl;
import org.apache.qpid.server.model.Plugin;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.model.UUIDGenerator;
import org.apache.qpid.server.model.VirtualHostAlias;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.model.adapter.FileBasedGroupProvider;
import org.apache.qpid.server.model.adapter.FileBasedGroupProviderImpl;
import org.apache.qpid.server.plugin.PluggableFactoryLoader;
import org.apache.qpid.server.plugin.SystemConfigFactory;
import org.apache.qpid.server.security.access.plugins.AclFileAccessControlProvider;
import org.apache.qpid.server.security.access.plugins.AclRule;
import org.apache.qpid.server.security.access.plugins.RuleBasedAccessControlProvider;
import org.apache.qpid.server.store.AbstractMemoryStore;
import org.apache.qpid.server.store.ConfiguredObjectRecord;
import org.apache.qpid.server.store.ConfiguredObjectRecordConverter;
import org.apache.qpid.server.store.ConfiguredObjectRecordImpl;
import org.apache.qpid.server.store.DurableConfigurationStore;
import org.apache.qpid.server.store.handler.ConfiguredObjectRecordHandler;
import org.apache.qpid.server.virtualhostnode.JsonVirtualHostNode;
import org.apache.qpid.util.Strings;

public class TestBrokerConfiguration
{

    public static final String ENTRY_NAME_HTTP_PORT = "http";
    public static final String ENTRY_NAME_AMQP_PORT = "amqp";
    public static final String ENTRY_NAME_VIRTUAL_HOST = "test";
    public static final String ENTRY_NAME_AUTHENTICATION_PROVIDER = "plain";
    public static final String ENTRY_NAME_EXTERNAL_PROVIDER = "external";
    public static final String ENTRY_NAME_SSL_PORT = "sslPort";
    public static final String ENTRY_NAME_HTTP_MANAGEMENT = "MANAGEMENT-HTTP";
    public static final String MANAGEMENT_HTTP_PLUGIN_TYPE = "MANAGEMENT-HTTP";
    public static final String ENTRY_NAME_ANONYMOUS_PROVIDER = "anonymous";
    public static final String ENTRY_NAME_SSL_KEYSTORE = "systestsKeyStore";
    public static final String ENTRY_NAME_SSL_TRUSTSTORE = "systestsTrustStore";
    public static final String ENTRY_NAME_GROUP_FILE = "groupFile";
    public static final String ENTRY_NAME_ACL_FILE = "aclFile";
    public static final String ENTRY_NAME_ACL_RULES = "aclRules";
    private final TaskExecutor _taskExecutor;
    private final String _storeType;

    private AbstractMemoryStore _store;
    private boolean _saved;
    private File _passwdFile;

    private static final Logger LOGGER = LoggerFactory.getLogger(TestBrokerConfiguration.class);

    public TestBrokerConfiguration(String storeType, String initialStoreLocation)
    {
        _taskExecutor = new CurrentThreadTaskExecutor();
        _taskExecutor.start();
        _storeType = storeType;
        Map<String,Object> config = new HashMap<>();
        config.put("storePath", initialStoreLocation);
        final AbstractSystemConfig parentObject = new JsonSystemConfigImpl(_taskExecutor,
                                                                           mock(EventLogger.class),
                                                                           null,
                                                                           config)
        {

            {
                updateModel(BrokerModel.getInstance());
            }
        };

        ConfiguredObjectRecordConverter converter = new ConfiguredObjectRecordConverter(BrokerModel.getInstance());

        Reader reader;
        try
        {
            try
            {
                URL url = new URL(initialStoreLocation);
                try(InputStream urlStream = url.openStream())
                {
                    reader = new InputStreamReader(urlStream);
                }
            }
            catch (MalformedURLException e)
            {
                reader = new FileReader(initialStoreLocation);
            }

            Collection<ConfiguredObjectRecord> records = converter.readFromJson(org.apache.qpid.server.model.Broker.class, parentObject, reader);
            reader.close();

            _store = new AbstractMemoryStore(Broker.class){};

            ConfiguredObjectRecord[] initialRecords = records.toArray(new ConfiguredObjectRecord[records.size()]);
            _store.init(parentObject);

            _store.openConfigurationStore(new ConfiguredObjectRecordHandler()
            {
                @Override
                public void handle(ConfiguredObjectRecord record)
                {
                    Map<String, Object> attributes = record.getAttributes();
                    String rawType = (String)attributes.get("type");
                    if (rawType != null)
                    {
                        String interpolatedType = Strings.expand(rawType, false, Strings.ENV_VARS_RESOLVER, Strings.JAVA_SYS_PROPS_RESOLVER);
                        if (!interpolatedType.equals(rawType))
                        {
                            setObjectAttribute(record, "type", interpolatedType);
                        }
                    }
                }

            }, initialRecords);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Unable to load initial store", e);
        }


    }

    public boolean setBrokerAttribute(String name, Object value)
    {
        ConfiguredObjectRecord entry = findObject(Broker.class, null);
        if (entry == null)
        {
            return false;
        }

        return setObjectAttribute(entry, name, value);
    }

    public boolean setObjectAttribute(final Class<? extends ConfiguredObject> category,
                                      String objectName,
                                      String attributeName,
                                      Object value)
    {
        ConfiguredObjectRecord entry = findObject(category, objectName);
        if (entry == null)
        {
            return false;
        }
        return setObjectAttribute(entry, attributeName, value);
    }

    public boolean setObjectAttributes(final Class<? extends ConfiguredObject> category,
                                       String objectName,
                                       Map<String, Object> attributes)
    {
        ConfiguredObjectRecord entry = findObject(category, objectName);
        if (entry == null)
        {
            return false;
        }
        return setObjectAttributes(entry, attributes);
    }

    public boolean save(File configFile)
    {

        Map<String, Object> attributes = new HashMap<>();
        attributes.put("storePath", configFile.getAbsolutePath());

        SystemConfigFactory configFactory =
                (new PluggableFactoryLoader<>(SystemConfigFactory.class)).get(_storeType);

        attributes.put(SystemConfig.STARTUP_LOGGED_TO_SYSTEM_OUT, false);
        attributes.put(ConfiguredObject.DESIRED_STATE, State.QUIESCED);
        final SystemConfig parentObject = configFactory.newInstance(_taskExecutor,
                                                                    mock(EventLogger.class),
                                                                    null, attributes);

        parentObject.open();
        DurableConfigurationStore configurationStore = parentObject.getConfigurationStore();
        configurationStore.closeConfigurationStore();

        final List<ConfiguredObjectRecord> records = getConfiguredObjectRecords();


        configurationStore.init(parentObject);

        clearStore(configurationStore);

        configurationStore.update(true, records.toArray(new ConfiguredObjectRecord[records.size()]));


        configurationStore.closeConfigurationStore();
        parentObject.close();
        return true;
    }

    public void clearStore(final DurableConfigurationStore configurationStore)
    {
        final List<ConfiguredObjectRecord> recordsToDelete = new ArrayList<>();
        configurationStore.openConfigurationStore(new ConfiguredObjectRecordHandler()
        {

            @Override
            public void handle(final ConfiguredObjectRecord record)
            {
                recordsToDelete.add(record);
            }

        });
        if(!recordsToDelete.isEmpty())
        {
            configurationStore.remove(recordsToDelete.toArray(new ConfiguredObjectRecord[recordsToDelete.size()]));
        }
    }

    public List<ConfiguredObjectRecord> getConfiguredObjectRecords()
    {
        return _store.getConfiguredObjectRecords();

    }

    public UUID[] removeObjectConfiguration(final Class<? extends ConfiguredObject> category,
                                            final String name)
    {
        final ConfiguredObjectRecord entry = findObject(category, name);

        if (entry != null)
        {

            if(category == VirtualHostNode.class)
            {
                final List<ConfiguredObjectRecord> aliasRecords = new ArrayList<>();
                // remove vhost aliases associated with the vhost

                for(ConfiguredObjectRecord record : getConfiguredObjectRecords())
                {
                    if (record.getType().equals(VirtualHostAlias.class.getSimpleName())
                            && name.equals(record.getAttributes().get(ConfiguredObject.NAME)))
                    {
                        aliasRecords.add(record);
                    }
                }

                _store.remove(aliasRecords.toArray(new ConfiguredObjectRecord[aliasRecords.size()]));
            }
            return _store.remove(entry);

        }
        return null;
    }

    public UUID addObjectConfiguration(Class<? extends ConfiguredObject> type, Map<String, Object> attributes)
    {
        UUID id = UUIDGenerator.generateRandomUUID();
        addObjectConfiguration(id, type.getSimpleName(), attributes);
        return id;
    }

    public UUID addObjectConfiguration(final Class<? extends ConfiguredObject> parentCategory, final String parentName,
                                       Class<? extends ConfiguredObject> type, Map<String, Object> attributes)
    {
        UUID id = UUIDGenerator.generateRandomUUID();
        ConfiguredObjectRecord entry =
                new ConfiguredObjectRecordImpl(id, type.getSimpleName(), attributes,
                                               Collections.singletonMap(parentCategory.getSimpleName(), findObject(parentCategory,parentName).getId()));

        _store.update(true, entry);
        return id;
    }

    public UUID addHttpManagementConfiguration()
    {
        setObjectAttribute(AuthenticationProvider.class, TestBrokerConfiguration.ENTRY_NAME_AUTHENTICATION_PROVIDER,
                           "secureOnlyMechanisms", "{}");
        setObjectAttribute(Port.class, TestBrokerConfiguration.ENTRY_NAME_HTTP_PORT, Port.AUTHENTICATION_PROVIDER,
                           TestBrokerConfiguration.ENTRY_NAME_AUTHENTICATION_PROVIDER);

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(Plugin.TYPE, MANAGEMENT_HTTP_PLUGIN_TYPE);
        attributes.put(Plugin.NAME, ENTRY_NAME_HTTP_MANAGEMENT);
        attributes.put(HttpManagement.HTTP_BASIC_AUTHENTICATION_ENABLED, true);
        return addObjectConfiguration(Plugin.class, attributes);
    }

    public UUID addGroupFileConfiguration(String groupFilePath)
    {
        Map<String, Object> attributes = new HashMap<String, Object>();
        attributes.put(GroupProvider.NAME, ENTRY_NAME_GROUP_FILE);
        attributes.put(GroupProvider.TYPE, FileBasedGroupProviderImpl.GROUP_FILE_PROVIDER_TYPE);
        attributes.put(FileBasedGroupProvider.PATH, groupFilePath);

        return addObjectConfiguration(GroupProvider.class, attributes);
    }

    public UUID addAclFileConfiguration(String aclFilePath)
    {
        Map<String, Object> attributes = new HashMap<String, Object>();
        attributes.put(AccessControlProvider.NAME, ENTRY_NAME_ACL_FILE);
        attributes.put(AccessControlProvider.TYPE, AclFileAccessControlProvider.ACL_FILE_PROVIDER_TYPE);
        attributes.put(AclFileAccessControlProvider.PATH, aclFilePath);

        return addObjectConfiguration(AccessControlProvider.class, attributes);
    }

    public UUID addAclRuleConfiguration(AclRule[] aclRules)
    {
        Map<String, Object> attributes = new HashMap<String, Object>();
        attributes.put(AccessControlProvider.NAME, ENTRY_NAME_ACL_RULES);
        attributes.put(AccessControlProvider.TYPE, RuleBasedAccessControlProvider.RULE_BASED_TYPE);
        attributes.put(RuleBasedAccessControlProvider.RULES, aclRules);

        return addObjectConfiguration(AccessControlProvider.class, attributes);
    }


    private boolean setObjectAttributes(ConfiguredObjectRecord entry, Map<String, Object> attributes)
    {
        Map<String, Object> newAttributes = new HashMap<String, Object>(entry.getAttributes());
        newAttributes.putAll(attributes);
        ConfiguredObjectRecord newEntry = new ConfiguredObjectRecordImpl(entry.getId(), entry.getType(), newAttributes,
                                                                         entry.getParents());
        _store.update(false, newEntry);
        return true;
    }

    private ConfiguredObjectRecord findObject(final Class<? extends ConfiguredObject> category, final String objectName)
    {
        Collection<ConfiguredObjectRecord> records = getConfiguredObjectRecords();
        for(ConfiguredObjectRecord record : records)
        {
            if (record.getType().equals(category.getSimpleName())
                && (objectName == null
                    || objectName.equals(record.getAttributes().get(ConfiguredObject.NAME))))
            {
                return record;
            }
        }
        return null;

    }

    private void addObjectConfiguration(UUID id, String type, Map<String, Object> attributes)
    {
        ConfiguredObjectRecord entry = new ConfiguredObjectRecordImpl(id, type, attributes, Collections.singletonMap(Broker.class.getSimpleName(), findObject(Broker.class,null).getId()));

        _store.update(true, entry);
    }

    private boolean setObjectAttribute(ConfiguredObjectRecord entry, String attributeName, Object value)
    {
        Map<String, Object> attributes = new HashMap<String, Object>(entry.getAttributes());
        attributes.put(attributeName, value);
        ConfiguredObjectRecord newEntry = new ConfiguredObjectRecordImpl(entry.getId(), entry.getType(), attributes, entry.getParents());
        _store.update(false, newEntry);
        return true;
    }

    public boolean isSaved()
    {
        return _saved;
    }

    public void setSaved(boolean saved)
    {
        _saved = saved;
    }

    public Map<String,Object> getObjectAttributes(final Class<? extends ConfiguredObject> category, final String name)
    {
        return findObject(category, name).getAttributes();
    }

    public void createVirtualHostNode(final String virtualHostNodeName,
                                      final String storeType,
                                      final String storeDir,
                                      final String blueprint)
    {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(VirtualHostNode.NAME, virtualHostNodeName);
        attributes.put(VirtualHostNode.TYPE, storeType);
        if (storeDir != null)
        {
            attributes.put(JsonVirtualHostNode.STORE_PATH, storeDir);
        }

        if (blueprint != null)
        {
            attributes.put(VirtualHostNode.CONTEXT,
                           Collections.singletonMap(VirtualHostNode.VIRTUALHOST_BLUEPRINT_CONTEXT_VAR, blueprint));
        }

        addObjectConfiguration(VirtualHostNode.class, attributes);
    }

    public void configureTemporaryPasswordFile(String... users) throws IOException
    {
        _passwdFile = createTemporaryPasswordFile(users);

        setObjectAttribute(AuthenticationProvider.class, TestBrokerConfiguration.ENTRY_NAME_AUTHENTICATION_PROVIDER,
                                    "path", _passwdFile.getAbsolutePath());
    }

    public void cleanUp()
    {
        if (_passwdFile != null)
        {
            if (_passwdFile.exists())
            {
                _passwdFile.delete();
            }
        }
    }

    public File createTemporaryPasswordFile(String[] users) throws IOException
    {
        BufferedWriter writer = null;
        try
        {
            File testFile = File.createTempFile(this.getClass().getName(),"tmp");
            testFile.deleteOnExit();

            writer = new BufferedWriter(new FileWriter(testFile));
            for (int i = 0; i < users.length; i++)
            {
                String username = users[i];
                writer.write(username + ":" + username);
                writer.newLine();
            }

            return testFile;

        }
        finally
        {
            if (writer != null)
            {
                writer.close();
            }
        }
    }

}
