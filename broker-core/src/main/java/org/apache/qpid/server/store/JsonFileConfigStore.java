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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import org.apache.qpid.server.configuration.BrokerProperties;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObjectJacksonModule;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.store.handler.ConfiguredObjectRecordHandler;

public class JsonFileConfigStore extends AbstractJsonFileStore implements DurableConfigurationStore
{


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
    private final Class<? extends ConfiguredObject> _rootClass;
    private final ObjectMapper _objectMapper;
    private Map<String,Class<? extends ConfiguredObject>> _classNameMapping;

    private ConfiguredObject<?> _parent;

    public JsonFileConfigStore(Class<? extends ConfiguredObject> rootClass)
    {
        super();
        _objectMapper = ConfiguredObjectJacksonModule.newObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
        _rootClass = rootClass;
    }

    @Override
    public void upgradeStoreStructure() throws StoreException
    {
        // No-op for Json
    }

    @Override
    public void openConfigurationStore(ConfiguredObject<?> parent,
                                       final boolean overwrite,
                                       final ConfiguredObjectRecord... initialRecords)
    {
        _parent = parent;
        _classNameMapping = generateClassNameMap(_parent.getModel(), _rootClass);
        FileBasedSettings fileBasedSettings = (FileBasedSettings)_parent;
        setup(parent.getName(), fileBasedSettings.getStorePath(), parent.getContextValue(String.class, BrokerProperties.POSIX_FILE_PERMISSIONS),
              Collections.emptyMap());
        load(overwrite, initialRecords);
    }

    @Override
    public void visitConfiguredObjectRecords(ConfiguredObjectRecordHandler handler)
    {
        handler.begin();
        List<ConfiguredObjectRecord> records = new ArrayList<ConfiguredObjectRecord>(_objectsById.values());
        for(ConfiguredObjectRecord record : records)
        {
            boolean shouldContinue = handler.handle(record);
            if (!shouldContinue)
            {
                break;
            }
        }
        handler.end();
    }

    protected void load(final boolean overwrite, final ConfiguredObjectRecord[] initialRecords)
    {
        final File configFile = getConfigFile();
        try
        {
            boolean updated = false;
            Collection<ConfiguredObjectRecord> records = Collections.emptyList();
            if(!overwrite)
            {
                ConfiguredObjectRecordConverter configuredObjectRecordConverter =
                        new ConfiguredObjectRecordConverter(_parent.getModel());

                records = configuredObjectRecordConverter.readFromJson(_rootClass, _parent, new FileReader(configFile));
            }

            if(records.isEmpty())
            {
                records = Arrays.asList(initialRecords);
                updated = true;
            }

            for(ConfiguredObjectRecord record : records)
            {
                _objectsById.put(record.getId(), record);
                List<UUID> idsForType = _idsByType.get(record.getType());
                if (idsForType == null)
                {
                    idsForType = new ArrayList<>();
                    _idsByType.put(record.getType(), idsForType);
                }
                idsForType.add(record.getId());
            }
            if(updated)
            {
                save();
            }
        }
        catch (IOException e)
        {
            throw new StoreException("Cannot construct configuration from the configuration file " + configFile, e);
        }
    }

    @Override
    public synchronized void create(ConfiguredObjectRecord record) throws StoreException
    {
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
            data = build(_rootClass, rootId);
        }

        save(data);
    }

    private Map<String, Object> build(final Class<? extends ConfiguredObject> type, final UUID id)
    {
        ConfiguredObjectRecord record = _objectsById.get(id);
        Map<String,Object> map = new LinkedHashMap<String, Object>();

        Collection<Class<? extends ConfiguredObject>> parentTypes = _parent.getModel().getParentTypes(type);
        if(parentTypes.size() > 1)
        {
            Iterator<Class<? extends ConfiguredObject>> iter = parentTypes.iterator();
            // skip the first parent, which is given by structure
            iter.next();
            // for all other parents add a fake attribute with name being the parent type in lower case, and the value
            // being the parents id
            while(iter.hasNext())
            {
                String parentType = iter.next().getSimpleName();
                map.put(parentType.toLowerCase(), record.getParents().get(parentType));
            }
        }

        map.put("id", id);
        map.putAll(record.getAttributes());

        List<Class<? extends ConfiguredObject>> childClasses =
                new ArrayList<Class<? extends ConfiguredObject>>(_parent.getModel().getChildTypes(type));

        Collections.sort(childClasses, CATEGORY_CLASS_COMPARATOR);

        for(Class<? extends ConfiguredObject> childClass : childClasses)
        {
            // only add if this is the "first" parent
            if(_parent.getModel().getParentTypes(childClass).iterator().next() == type)
            {
                String singularName = childClass.getSimpleName().toLowerCase();
                String attrName = singularName + (singularName.endsWith("s") ? "es" : "s");
                List<UUID> childIds = _idsByType.get(childClass.getSimpleName());
                if(childIds != null)
                {
                    List<Map<String,Object>> entities = new ArrayList<Map<String, Object>>();
                    List<ConfiguredObjectRecord> sortedChildren = new ArrayList<>();
                    for(UUID childId : childIds)
                    {
                        ConfiguredObjectRecord childRecord = _objectsById.get(childId);

                        final UUID parent = childRecord.getParents().get(type.getSimpleName());
                        String parentId = parent.toString();
                        if(id.toString().equals(parentId))
                        {
                            sortedChildren.add(childRecord);
                        }
                    }

                    Collections.sort(sortedChildren, CONFIGURED_OBJECT_RECORD_COMPARATOR);

                    for(ConfiguredObjectRecord childRecord : sortedChildren)
                    {
                        entities.add(build(childClass, childRecord.getId()));
                    }

                    if(!entities.isEmpty())
                    {
                        map.put(attrName,entities);
                    }
                }
            }
        }

        return map;
    }

    @Override
    public synchronized UUID[] remove(final ConfiguredObjectRecord... objects) throws StoreException
    {
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
        map.put(clazz.getSimpleName(), clazz);
        Collection<Class<? extends ConfiguredObject>> childClasses = model.getChildTypes(clazz);
        if(childClasses != null)
        {
            for(Class<? extends ConfiguredObject> childClass : childClasses)
            {
                map.putAll(generateClassNameMap(model, childClass));
            }
        }
        return map;
    }

    @Override
    protected ObjectMapper getSerialisationObjectMapper()
    {
        return _objectMapper;
    }
}
