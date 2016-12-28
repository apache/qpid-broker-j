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

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObjectJacksonModule;
import org.apache.qpid.server.model.ContainerType;
import org.apache.qpid.server.model.DynamicModel;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.plugin.QpidServiceLoader;
import org.apache.qpid.server.store.handler.ConfiguredObjectRecordHandler;

public class JsonFileConfigStore extends AbstractJsonFileStore implements DurableConfigurationStore
{
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonFileConfigStore.class);

    private static final Comparator<Class<? extends ConfiguredObject>> CATEGORY_CLASS_COMPARATOR =
            new Comparator<Class<? extends ConfiguredObject>>()
            {
                @Override
                public int compare(final Class<? extends ConfiguredObject> left,
                                   final Class<? extends ConfiguredObject> right)
                {
                    return left.getSimpleName().compareTo(right.getSimpleName());
                }
            };
    private static final Comparator<ConfiguredObjectRecord> CONFIGURED_OBJECT_RECORD_COMPARATOR =
            new Comparator<ConfiguredObjectRecord>()
            {
                @Override
                public int compare(final ConfiguredObjectRecord left, final ConfiguredObjectRecord right)
                {
                    String leftName = (String) left.getAttributes().get(ConfiguredObject.NAME);
                    String rightName = (String) right.getAttributes().get(ConfiguredObject.NAME);
                    return leftName.compareTo(rightName);
                }
            };

    private final Map<UUID, ConfiguredObjectRecord> _objectsById = new HashMap<UUID, ConfiguredObjectRecord>();
    private final Map<String, List<UUID>> _idsByType = new HashMap<String, List<UUID>>();
    private volatile Class<? extends ConfiguredObject> _rootClass;
    private final ObjectMapper _objectMapper;
    private volatile Map<String,Class<? extends ConfiguredObject>> _classNameMapping;

    private ConfiguredObject<?> _parent;

    private enum State { CLOSED, CONFIGURED, OPEN };
    private State _state = State.CLOSED;
    private final Object _lock = new Object();

    public JsonFileConfigStore(Class<? extends ConfiguredObject> rootClass)
    {
        super();
        _objectMapper = ConfiguredObjectJacksonModule.newObjectMapper(true).enable(SerializationFeature.INDENT_OUTPUT);
        _rootClass = rootClass;
    }

    @Override
    public void upgradeStoreStructure() throws StoreException
    {
        // No-op for Json
    }

    @Override
    public void init(ConfiguredObject<?> parent)
    {
        assertState(State.CLOSED);
        _parent = parent;
        _classNameMapping = generateClassNameMap(_parent.getModel(), _rootClass);

        FileBasedSettings fileBasedSettings = (FileBasedSettings) _parent;
        setup(parent.getName(),
              fileBasedSettings.getStorePath(),
              parent.getContextValue(String.class, SystemConfig.POSIX_FILE_PERMISSIONS),
              Collections.emptyMap());
        changeState(State.CLOSED, State.CONFIGURED);

    }

    @Override
    public boolean openConfigurationStore(ConfiguredObjectRecordHandler handler,
                                          final ConfiguredObjectRecord... initialRecords)
    {
        changeState(State.CONFIGURED, State.OPEN);
        boolean isNew = load(initialRecords);
        List<ConfiguredObjectRecord> records = new ArrayList<ConfiguredObjectRecord>(_objectsById.values());
        for(ConfiguredObjectRecord record : records)
        {
            handler.handle(record);
        }
        return isNew;
    }

    @Override
    public void reload(ConfiguredObjectRecordHandler handler)
    {
        assertState(State.OPEN);
        _idsByType.clear();
        _objectsById.clear();
        load();
        List<ConfiguredObjectRecord> records = new ArrayList<ConfiguredObjectRecord>(_objectsById.values());
        for(ConfiguredObjectRecord record : records)
        {
            handler.handle(record);
        }
    }


    protected boolean load(final ConfiguredObjectRecord... initialRecords)
    {
        final File configFile = getConfigFile();
        try
        {
            LOGGER.debug("Loading file {}", configFile.getCanonicalPath());

            boolean updated = false;
            Collection<ConfiguredObjectRecord> records = Collections.emptyList();
            ConfiguredObjectRecordConverter configuredObjectRecordConverter =
                    new ConfiguredObjectRecordConverter(_parent.getModel());

            records = configuredObjectRecordConverter.readFromJson(_rootClass, _parent, new FileReader(configFile));

            if(_rootClass == null)
            {
                _rootClass = configuredObjectRecordConverter.getRootClass();
                _classNameMapping = generateClassNameMap(configuredObjectRecordConverter.getModel(), _rootClass);
            }

            if(records.isEmpty())
            {
                LOGGER.debug("File contains no records - using initial configuration");
                records = Arrays.asList(initialRecords);
                updated = true;
                if (_rootClass == null)
                {
                    String containerTypeName = ((DynamicModel) _parent).getDefaultContainerType();
                    ConfiguredObjectRecord rootRecord = null;
                    for(ConfiguredObjectRecord record : records)
                    {
                        if(record.getParents() == null || record.getParents().isEmpty())
                        {
                            rootRecord = record;
                            break;
                        }
                    }
                    if (rootRecord != null && rootRecord.getAttributes().get(ConfiguredObject.TYPE) instanceof String)
                    {
                        containerTypeName = rootRecord.getAttributes().get(ConfiguredObject.TYPE).toString();
                    }

                    QpidServiceLoader loader = new QpidServiceLoader();
                    final ContainerType<?> containerType =
                            loader.getInstancesByType(ContainerType.class).get(containerTypeName);

                    if (containerType != null)
                    {
                        _rootClass = containerType.getCategoryClass();
                        _classNameMapping = generateClassNameMap(containerType.getModel(), containerType.getCategoryClass());
                    }

                }
            }

            for(ConfiguredObjectRecord record : records)
            {
                LOGGER.debug("Loading record (Category: {} \t Name: {} \t ID: {}", record.getType(), record.getAttributes().get("name"), record.getId());
                _objectsById.put(record.getId(), record);
                List<UUID> idsForType = _idsByType.get(record.getType());
                if (idsForType == null)
                {
                    idsForType = new ArrayList<>();
                    _idsByType.put(record.getType(), idsForType);
                }
                if(idsForType.contains(record.getId()))
                {
                    throw new IllegalArgumentException("Duplicate id for record " + record);
                }
                idsForType.add(record.getId());
            }
            if(updated)
            {
                save();
            }
            return updated;
        }
        catch (IOException e)
        {
            throw new StoreException("Cannot construct configuration from the configuration file " + configFile, e);
        }
    }

    @Override
    public synchronized void create(ConfiguredObjectRecord record) throws StoreException
    {
        assertState(State.OPEN);
        if(_objectsById.containsKey(record.getId()))
        {
            throw new StoreException("Object with id " + record.getId() + " already exists");
        }
        else if(!_classNameMapping.containsKey(record.getType()))
        {
            throw new StoreException("Cannot create object of unknown type " + record.getType());
        }
        else if(record.getAttributes() == null || !(record.getAttributes().get(ConfiguredObject.NAME) instanceof String))
        {
            throw new StoreException("The record " + record.getId()
                                     + " of type " + record.getType()
                                     + " does not have an attribute '"
                                     + ConfiguredObject.NAME
                                     + "' of type String");
        }
        else
        {
            record = new ConfiguredObjectRecordImpl(record);
            _objectsById.put(record.getId(), record);
            List<UUID> idsForType = _idsByType.get(record.getType());
            if(idsForType == null)
            {
                idsForType = new ArrayList<UUID>();
                _idsByType.put(record.getType(), idsForType);
            }

            if (_rootClass.getSimpleName().equals(record.getType()) && idsForType.size() > 0)
            {
                throw new IllegalStateException("Only a single root entry of type " + _rootClass.getSimpleName() + " can exist in the store.");
            }
            if(idsForType.contains(record.getId()))
            {
                throw new IllegalArgumentException("Duplicate id for record " + record);
            }

            idsForType.add(record.getId());

            save();
        }
    }

    private UUID getRootId()
    {
        List<UUID> ids = _idsByType.get(_rootClass.getSimpleName());
        if (ids == null)
        {
            return null;
        }
        if (ids.size() == 0)
        {
            return null;
        }
        return ids.get(0);
    }

    private void save()
    {
        UUID rootId = getRootId();
        final Map<String, Object> data;
        if (rootId == null)
        {
            data = Collections.emptyMap();
        }
        else
        {
            data = build(_rootClass, rootId, createChildMap());
        }

        save(data);
    }

    private Map<UUID, Map<String, SortedSet<ConfiguredObjectRecord>>> createChildMap()
    {
        Model model = _parent.getModel();
        Map<UUID, Map<String, SortedSet<ConfiguredObjectRecord>>> map = new HashMap<>();

        for(ConfiguredObjectRecord record : _objectsById.values())
        {
            int parentCount = record.getParents().size();
            if (parentCount == 0)
            {
                continue;
            }
            Class<? extends ConfiguredObject> parentType =
                    model.getParentType(_classNameMapping.get(record.getType()));
            if (parentType != null)
            {

                String parentCategoryName = parentType.getSimpleName();

                UUID parentId = record.getParents().get(parentCategoryName);

                if (parentId != null)
                {
                    Map<String, SortedSet<ConfiguredObjectRecord>> childMap = map.get(parentId);
                    if (childMap == null)
                    {
                        childMap = new TreeMap<>();
                        map.put(parentId, childMap);
                    }
                    SortedSet<ConfiguredObjectRecord> children = childMap.get(record.getType());
                    if (children == null)
                    {
                        children = new TreeSet<>(CONFIGURED_OBJECT_RECORD_COMPARATOR);
                        childMap.put(record.getType(), children);
                    }
                    children.add(record);
                }
            }
        }
        return map;
    }

    private Map<String, Object> build(final Class<? extends ConfiguredObject> type, final UUID id,
                                      Map<UUID, Map<String, SortedSet<ConfiguredObjectRecord>>> childMap)
    {
        ConfiguredObjectRecord record = _objectsById.get(id);
        Map<String,Object> map = new LinkedHashMap<>();

        map.put("id", id);
        map.putAll(record.getAttributes());

        List<Class<? extends ConfiguredObject>> childClasses = new ArrayList<>(_parent.getModel().getChildTypes(type));

        Collections.sort(childClasses, CATEGORY_CLASS_COMPARATOR);

        final Map<String, SortedSet<ConfiguredObjectRecord>> allChildren = childMap.get(id);
        if(allChildren != null && !allChildren.isEmpty())
        {
            for(Map.Entry<String, SortedSet<ConfiguredObjectRecord>> entry : allChildren.entrySet())
            {
                String singularName = entry.getKey().toLowerCase();
                String attrName = singularName + (singularName.endsWith("s") ? "es" : "s");
                final SortedSet<ConfiguredObjectRecord> sortedChildren = entry.getValue();
                List<Map<String,Object>> entities = new ArrayList<>();

                for(ConfiguredObjectRecord childRecord : sortedChildren)
                {
                    entities.add(build(_classNameMapping.get(entry.getKey()), childRecord.getId(), childMap));
                }

                if(!entities.isEmpty())
                {
                    map.put(attrName,entities);
                }
            }
        }

        return map;
    }

    @Override
    public synchronized UUID[] remove(final ConfiguredObjectRecord... objects) throws StoreException
    {
        assertState(State.OPEN);

        if (objects.length == 0)
        {
            return new UUID[0];
        }

        List<UUID> removedIds = new ArrayList<UUID>();
        for(ConfiguredObjectRecord requestedRecord : objects)
        {
            ConfiguredObjectRecord record = _objectsById.remove(requestedRecord.getId());
            if(record != null)
            {
                removedIds.add(record.getId());
                _idsByType.get(record.getType()).remove(record.getId());
            }
        }
        save();
        return removedIds.toArray(new UUID[removedIds.size()]);
    }


    @Override
    public synchronized void update(final boolean createIfNecessary, final ConfiguredObjectRecord... records)
            throws StoreException
    {
        assertState(State.OPEN);

        if (records.length == 0)
        {
            return;
        }

        for(ConfiguredObjectRecord record : records)
        {
            final UUID id = record.getId();
            final String type = record.getType();

            if(record.getAttributes() == null || !(record.getAttributes().get(ConfiguredObject.NAME) instanceof String))
            {
                throw new StoreException("The record " + id + " of type " + type + " does not have an attribute '"
                                         + ConfiguredObject.NAME
                                         + "' of type String");
            }

            if(_objectsById.containsKey(id))
            {
                final ConfiguredObjectRecord existingRecord = _objectsById.get(id);
                if(!type.equals(existingRecord.getType()))
                {
                    throw new StoreException("Cannot change the type of record " + id + " from type "
                                                + existingRecord.getType() + " to type " + type);
                }
            }
            else if(!createIfNecessary)
            {
                throw new StoreException("Cannot update record with id " + id
                                        + " of type " + type + " as it does not exist");
            }
            else if(!_classNameMapping.containsKey(type))
            {
                throw new StoreException("Cannot update record of unknown type " + type);
            }
        }
        for(ConfiguredObjectRecord record : records)
        {
            record = new ConfiguredObjectRecordImpl(record);
            final UUID id = record.getId();
            final String type = record.getType();
            if(_objectsById.put(id, record) == null)
            {
                List<UUID> idsForType = _idsByType.get(type);
                if(idsForType == null)
                {
                    idsForType = new ArrayList<UUID>();
                    _idsByType.put(type, idsForType);
                }
                if(idsForType.contains(record.getId()))
                {
                    throw new IllegalArgumentException("Duplicate id for record " + record);
                }

                idsForType.add(id);
            }
        }

        save();
    }

    @Override
    public void closeConfigurationStore()
    {

        try
        {
            cleanup();
        }
        finally
        {
            _idsByType.clear();
            _objectsById.clear();
            synchronized (_lock)
            {
                _state = State.CLOSED;
            }
        }
    }

    @Override
    public void onDelete(ConfiguredObject<?> parent)
    {
        FileBasedSettings fileBasedSettings = (FileBasedSettings)parent;

        delete(fileBasedSettings.getStorePath());
    }

    private static Map<String,Class<? extends ConfiguredObject>> generateClassNameMap(final Model model,
                                                                                      final Class<? extends ConfiguredObject> clazz)
    {
        Map<String,Class<? extends ConfiguredObject>>map = new HashMap<String, Class<? extends ConfiguredObject>>();
        if(clazz != null)
        {
            map.put(clazz.getSimpleName(), clazz);
            Collection<Class<? extends ConfiguredObject>> childClasses = model.getChildTypes(clazz);
            if (childClasses != null)
            {
                for (Class<? extends ConfiguredObject> childClass : childClasses)
                {
                    map.putAll(generateClassNameMap(model, childClass));
                }
            }
        }
        return map;
    }

    @Override
    protected ObjectMapper getSerialisationObjectMapper()
    {
        return _objectMapper;
    }

    private void assertState(State state)
    {
        synchronized (_lock)
        {
            if(_state != state)
            {
                throw new IllegalStateException("The store must be in state " + state + " to perform this operation, but it is in state " + _state + " instead");
            }
        }
    }

    private void changeState(State oldState, State newState)
    {
        synchronized (_lock)
        {
            assertState(oldState);
            _state = newState;
        }
    }

}
