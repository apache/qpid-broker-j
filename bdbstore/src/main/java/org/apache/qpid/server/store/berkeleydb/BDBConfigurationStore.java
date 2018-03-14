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
package org.apache.qpid.server.store.berkeleydb;

import static org.apache.qpid.server.store.berkeleydb.BDBConfigurationStore.State.CLOSED;
import static org.apache.qpid.server.store.berkeleydb.BDBConfigurationStore.State.CONFIGURED;
import static org.apache.qpid.server.store.berkeleydb.BDBConfigurationStore.State.OPEN;
import static org.apache.qpid.server.store.berkeleydb.BDBUtils.DEFAULT_DATABASE_CONFIG;
import static org.apache.qpid.server.store.berkeleydb.BDBUtils.abortTransactionSafely;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.store.ConfiguredObjectRecord;
import org.apache.qpid.server.store.DurableConfigurationStore;
import org.apache.qpid.server.store.FileBasedSettings;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.MessageStoreProvider;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.server.store.berkeleydb.entry.HierarchyKey;
import org.apache.qpid.server.store.berkeleydb.tuple.ConfiguredObjectBinding;
import org.apache.qpid.server.store.berkeleydb.tuple.HierarchyKeyBinding;
import org.apache.qpid.server.store.berkeleydb.tuple.UUIDTupleBinding;
import org.apache.qpid.server.store.handler.ConfiguredObjectRecordHandler;
import org.apache.qpid.server.store.preferences.PreferenceRecord;
import org.apache.qpid.server.store.preferences.PreferenceStore;
import org.apache.qpid.server.store.preferences.PreferenceStoreUpdater;
import org.apache.qpid.server.util.FileUtils;

/**
 * Implementation of a DurableConfigurationStore backed by BDB JE
 * that also provides a MessageStore.
 */
public class BDBConfigurationStore implements MessageStoreProvider, DurableConfigurationStore
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BDBConfigurationStore.class);

    public static final int VERSION = 9;
    private static final String CONFIGURED_OBJECTS_DB_NAME = "CONFIGURED_OBJECTS";
    private static final String CONFIGURED_OBJECT_HIERARCHY_DB_NAME = "CONFIGURED_OBJECT_HIERARCHY";

    enum State { CLOSED, CONFIGURED, OPEN }
    private State _state = State.CLOSED;
    private final Object _lock = new Object();

    private final EnvironmentFacadeFactory _environmentFacadeFactory;

    private final ProvidedBDBMessageStore _providedMessageStore = new ProvidedBDBMessageStore();
    private final ProvidedBDBPreferenceStore _providedPreferenceStore = new ProvidedBDBPreferenceStore();

    private EnvironmentFacade _environmentFacade;

    private ConfiguredObject<?> _parent;
    private final Class<? extends ConfiguredObject> _rootClass;

    public BDBConfigurationStore(final Class<? extends ConfiguredObject> rootClass)
    {
        this(new StandardEnvironmentFacadeFactory(), rootClass);
    }

    public BDBConfigurationStore(EnvironmentFacadeFactory environmentFacadeFactory, Class<? extends ConfiguredObject> rootClass)
    {
        _environmentFacadeFactory = environmentFacadeFactory;
        _rootClass = rootClass;
    }

    @Override
    public void init(ConfiguredObject<?> parent)
    {
        synchronized (_lock)
        {
            if(_state == OPEN && _parent == parent)
            {
                _state = CONFIGURED;
            }
            else if(_state == CONFIGURED && _parent == parent)
            {
                _state = CONFIGURED;
            }
            else
            {
                changeState(CLOSED, CONFIGURED);
                _parent = parent;

                if (_environmentFacade == null)
                {
                    _environmentFacade = _environmentFacadeFactory.createEnvironmentFacade(parent);
                }
                else
                {
                    throw new IllegalStateException("The database have been already opened as message store");
                }
            }
        }

    }

    public String getState()
    {
        return _state.toString();
    }



    @Override
    public void upgradeStoreStructure() throws StoreException
    {
        try
        {
            _environmentFacade.upgradeIfNecessary(_parent);
        }
        catch (RuntimeException e)
        {
            throw _environmentFacade.handleDatabaseException("Cannot upgrade store", e);
        }
    }

    @Override
    public boolean openConfigurationStore(ConfiguredObjectRecordHandler handler,
                                          final ConfiguredObjectRecord... initialRecords)
    {
        changeState(CONFIGURED, OPEN);
        try
        {
            Collection<? extends ConfiguredObjectRecord> records = doVisitAllConfiguredObjectRecords();

            boolean empty = records.isEmpty();

            if(empty)
            {
                records = Arrays.asList(initialRecords);

                com.sleepycat.je.Transaction txn = null;
                try
                {
                    txn = _environmentFacade.beginTransaction(null);
                    for(ConfiguredObjectRecord record : records)
                    {
                        update(true, record, txn);
                    }
                    txn.commit();
                    txn = null;
                }
                catch (RuntimeException e)
                {
                    throw _environmentFacade.handleDatabaseException("Error updating configuration details within the store: " + e,e);
                }
                finally
                {
                    if (txn != null)
                    {
                        abortTransactionSafely(txn, _environmentFacade);
                    }
                }


            }

            for (ConfiguredObjectRecord record : records)
            {
                handler.handle(record);
            }
            return empty;
        }
        catch (RuntimeException e)
        {
            throw _environmentFacade.handleDatabaseException("Cannot visit configured object records", e);
        }
    }

    @Override
    public void reload(ConfiguredObjectRecordHandler handler)
    {
        assertState(OPEN);

        try
        {
            Collection<? extends ConfiguredObjectRecord> records = doVisitAllConfiguredObjectRecords();

            for (ConfiguredObjectRecord record : records)
            {
                handler.handle(record);

            }
        }
        catch (RuntimeException e)
        {
            throw _environmentFacade.handleDatabaseException("Cannot visit configured object records", e);
        }
    }


    private Collection<? extends ConfiguredObjectRecord> doVisitAllConfiguredObjectRecords()
    {
        Map<UUID, BDBConfiguredObjectRecord> configuredObjects = new HashMap<>();
        try(Cursor objectsCursor = getConfiguredObjectsDb().openCursor(null, null))
        {
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry value = new DatabaseEntry();


            while (objectsCursor.getNext(key, value, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS)
            {
                UUID id = UUIDTupleBinding.getInstance().entryToObject(key);

                BDBConfiguredObjectRecord configuredObject =
                        (BDBConfiguredObjectRecord) new ConfiguredObjectBinding(id).entryToObject(value);
                configuredObjects.put(configuredObject.getId(), configuredObject);
            }

            // set parents
            try(Cursor hierarchyCursor = getConfiguredObjectHierarchyDb().openCursor(null, null))
            {
                while (hierarchyCursor.getNext(key, value, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS)
                {
                    HierarchyKey hk = HierarchyKeyBinding.getInstance().entryToObject(key);
                    UUID parentId = UUIDTupleBinding.getInstance().entryToObject(value);
                    BDBConfiguredObjectRecord child = configuredObjects.get(hk.getChildId());
                    if(child != null)
                    {
                        ConfiguredObjectRecord parent = configuredObjects.get(parentId);
                        if(parent != null)
                        {
                            child.addParent(hk.getParentType(), parent);
                        }
                    }
                }
            }
        }
        return configuredObjects.values();

    }

    public EnvironmentFacade getEnvironmentFacade()
    {
        return _environmentFacade;
    }

    @Override
    public void closeConfigurationStore() throws StoreException
    {
        synchronized (_lock)
        {
            _state = CLOSED;
        }

        if (!_providedMessageStore.isMessageStoreOpen())
        {
            closeEnvironment();
        }

    }

    private void closeEnvironment()
    {
        if (_environmentFacade != null)
        {
            try
            {
                _environmentFacade.close();
                _environmentFacade = null;
            }
            catch (RuntimeException e)
            {
                throw new StoreException("Exception occurred on message store close", e);
            }
        }
    }

    @Override
    public void create(ConfiguredObjectRecord configuredObject) throws StoreException
    {
        assertState(OPEN);

        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("Create " + configuredObject);
        }

        com.sleepycat.je.Transaction txn = null;
        try
        {
            txn = _environmentFacade.beginTransaction(null);
            storeConfiguredObjectEntry(txn, configuredObject);
            txn.commit();
            txn = null;
        }
        catch (RuntimeException e)
        {
            throw _environmentFacade.handleDatabaseException("Error creating configured object " + configuredObject
                    + " in database: " + e.getMessage(), e);
        }
        finally
        {
            if (txn != null)
            {
                abortTransactionSafely(txn, _environmentFacade);
            }
        }
    }

    @Override
    public UUID[] remove(final ConfiguredObjectRecord... objects) throws StoreException
    {
        assertState(OPEN);

        com.sleepycat.je.Transaction txn = null;
        try
        {
            txn = _environmentFacade.beginTransaction(null);

            Collection<UUID> removed = new ArrayList<>(objects.length);
            for(ConfiguredObjectRecord record : objects)
            {
                if(removeConfiguredObject(txn, record) == OperationStatus.SUCCESS)
                {
                    removed.add(record.getId());
                }
            }

            txn.commit();
            txn = null;
            return removed.toArray(new UUID[removed.size()]);
        }
        catch (RuntimeException e)
        {
            throw _environmentFacade.handleDatabaseException("Error deleting configured objects from database", e);
        }
        finally
        {
            if (txn != null)
            {
                abortTransactionSafely(txn, _environmentFacade);
            }
        }
    }

    @Override
    public void update(boolean createIfNecessary, ConfiguredObjectRecord... records) throws StoreException
    {
        assertState(OPEN);

        com.sleepycat.je.Transaction txn = null;
        try
        {
            txn = _environmentFacade.beginTransaction(null);
            for(ConfiguredObjectRecord record : records)
            {
                update(createIfNecessary, record, txn);
            }
            txn.commit();
            txn = null;
        }
        catch (RuntimeException e)
        {
            throw _environmentFacade.handleDatabaseException("Error updating configuration details within the store: " + e,e);
        }
        finally
        {
            if (txn != null)
            {
                abortTransactionSafely(txn, _environmentFacade);
            }
        }
    }

    private void update(boolean createIfNecessary, ConfiguredObjectRecord record, com.sleepycat.je.Transaction txn) throws StoreException
    {
        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("Updating, creating " + createIfNecessary + " : "  + record);
        }

        DatabaseEntry key = new DatabaseEntry();
        UUIDTupleBinding keyBinding = UUIDTupleBinding.getInstance();
        keyBinding.objectToEntry(record.getId(), key);

        DatabaseEntry value = new DatabaseEntry();
        DatabaseEntry newValue = new DatabaseEntry();
        ConfiguredObjectBinding configuredObjectBinding = ConfiguredObjectBinding.getInstance();

        OperationStatus status = getConfiguredObjectsDb().get(txn, key, value, LockMode.DEFAULT);
        final boolean isNewRecord = status == OperationStatus.NOTFOUND;
        if (status == OperationStatus.SUCCESS || (createIfNecessary && isNewRecord))
        {
            // write the updated entry to the store
            configuredObjectBinding.objectToEntry(record, newValue);
            status = getConfiguredObjectsDb().put(txn, key, newValue);
            if (status != OperationStatus.SUCCESS)
            {
                throw new StoreException("Error updating configuration details within the store: " + status);
            }
            if(isNewRecord)
            {
                writeHierarchyRecords(txn, record);
            }
        }
        else if (status != OperationStatus.NOTFOUND)
        {
            throw new StoreException("Error finding configuration details within the store: " + status);
        }
    }


    private void storeConfiguredObjectEntry(final Transaction txn, ConfiguredObjectRecord configuredObject) throws StoreException
    {
        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("Storing configured object record: " + configuredObject);
        }
        DatabaseEntry key = new DatabaseEntry();
        UUIDTupleBinding uuidBinding = UUIDTupleBinding.getInstance();
        uuidBinding.objectToEntry(configuredObject.getId(), key);
        DatabaseEntry value = new DatabaseEntry();
        ConfiguredObjectBinding queueBinding = ConfiguredObjectBinding.getInstance();

        queueBinding.objectToEntry(configuredObject, value);
        try
        {
            OperationStatus status = getConfiguredObjectsDb().put(txn, key, value);
            if (status != OperationStatus.SUCCESS)
            {
                throw new StoreException("Error writing configured object " + configuredObject + " to database: "
                        + status);
            }
            writeHierarchyRecords(txn, configuredObject);
        }
        catch (RuntimeException e)
        {
            throw _environmentFacade.handleDatabaseException("Error writing configured object " + configuredObject
                    + " to database: " + e.getMessage(), e);
        }
    }

    private void writeHierarchyRecords(final Transaction txn, final ConfiguredObjectRecord configuredObject)
    {
        OperationStatus status;
        HierarchyKeyBinding hierarchyBinding = HierarchyKeyBinding.getInstance();
        DatabaseEntry hierarchyKey = new DatabaseEntry();
        DatabaseEntry hierarchyValue = new DatabaseEntry();

        for(Map.Entry<String, UUID> parent : configuredObject.getParents().entrySet())
        {

            hierarchyBinding.objectToEntry(new HierarchyKey(configuredObject.getId(), parent.getKey()), hierarchyKey);
            UUIDTupleBinding.getInstance().objectToEntry(parent.getValue(), hierarchyValue);
            status = getConfiguredObjectHierarchyDb().put(txn, hierarchyKey, hierarchyValue);
            if (status != OperationStatus.SUCCESS)
            {
                throw new StoreException("Error writing configured object " + configuredObject + " parent record to database: "
                                         + status);
            }
        }
    }

    private OperationStatus removeConfiguredObject(Transaction tx, ConfiguredObjectRecord record) throws StoreException
    {
        UUID id = record.getId();
        Map<String, UUID> parents = record.getParents();

        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("Removing configured object: " + id);
        }
        DatabaseEntry key = new DatabaseEntry();

        UUIDTupleBinding uuidBinding = UUIDTupleBinding.getInstance();
        uuidBinding.objectToEntry(id, key);
        OperationStatus status = getConfiguredObjectsDb().delete(tx, key);
        if(status == OperationStatus.SUCCESS)
        {
            for(String parentType : parents.keySet())
            {
                DatabaseEntry hierarchyKey = new DatabaseEntry();
                HierarchyKeyBinding keyBinding = HierarchyKeyBinding.getInstance();
                keyBinding.objectToEntry(new HierarchyKey(record.getId(), parentType), hierarchyKey);
                getConfiguredObjectHierarchyDb().delete(tx, hierarchyKey);
            }
        }
        return status;
    }

    @Override
    public MessageStore getMessageStore()
    {
        return _providedMessageStore;
    }

    public PreferenceStore getPreferenceStore()
    {
        return _providedPreferenceStore;
    }

    @Override
    public void onDelete(ConfiguredObject<?> parent)
    {
        FileBasedSettings fileBasedSettings = (FileBasedSettings)parent;
        String storePath = fileBasedSettings.getStorePath();

        if (storePath != null)
        {
            if (LOGGER.isDebugEnabled())
            {
                LOGGER.debug("Deleting store " + storePath);
            }

            File configFile = new File(storePath);
            if (!FileUtils.delete(configFile, true))
            {
                LOGGER.info("Failed to delete the store at location " + storePath);
            }
        }
    }

    private Database getConfiguredObjectsDb()
    {
        return _environmentFacade.openDatabase(CONFIGURED_OBJECTS_DB_NAME, DEFAULT_DATABASE_CONFIG);
    }

    private Database getConfiguredObjectHierarchyDb()
    {
        return _environmentFacade.openDatabase(CONFIGURED_OBJECT_HIERARCHY_DB_NAME, DEFAULT_DATABASE_CONFIG);
    }

    class ProvidedBDBMessageStore extends AbstractBDBMessageStore
    {
        @Override
        protected void doOpen(final ConfiguredObject<?> parent)
        {
        }

        @Override
        protected void doClose()
        {
        }

        @Override
        public EnvironmentFacade getEnvironmentFacade()
        {
            return _environmentFacade;
        }

        @Override
        public void onDelete(ConfiguredObject<?> parent)
        {
            if (isMessageStoreOpen())
            {
                throw new IllegalStateException("Cannot delete the store as store is still open");
            }

            deleteMessageStoreDatabases();
        }

        @Override
        public String getStoreLocation()
        {
            return ((FileBasedSettings)(BDBConfigurationStore.this._parent)).getStorePath();
        }

        @Override
        public File getStoreLocationAsFile()
        {
            return new File(getStoreLocation());
        }

        @Override
        protected Logger getLogger()
        {
            return LOGGER;
        }
    }

    private class ProvidedBDBPreferenceStore extends AbstractBDBPreferenceStore
    {
        @Override
        public Collection<PreferenceRecord> openAndLoad(final PreferenceStoreUpdater updater) throws StoreException
        {
            return super.openAndLoad(updater);
        }

        @Override
        protected void doDelete()
        {
            // noop
        }

        @Override
        protected void doClose()
        {
            // noop
        }

        @Override
        protected EnvironmentFacade getEnvironmentFacade()
        {
            return _environmentFacade;
        }

        @Override
        protected Logger getLogger()
        {
            return LOGGER;
        }
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


    public boolean isOpen()
    {
        synchronized (_lock)
        {
            return _state == OPEN;
        }
    }


}
