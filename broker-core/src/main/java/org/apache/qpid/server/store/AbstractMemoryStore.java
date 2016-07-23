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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.store.handler.ConfiguredObjectRecordHandler;

public abstract class AbstractMemoryStore implements DurableConfigurationStore, MessageStoreProvider
{
    private final MessageStore _messageStore = new MemoryMessageStore();
    private final Class<? extends ConfiguredObject> _rootClass;

    enum State { CLOSED, CONFIGURED, OPEN };

    private State _state = State.CLOSED;
    private final Object _lock = new Object();

    private final ConcurrentMap<UUID, ConfiguredObjectRecord> _configuredObjectRecords = new ConcurrentHashMap<UUID, ConfiguredObjectRecord>();

    protected AbstractMemoryStore(final Class<? extends ConfiguredObject> rootClass)
    {
        _rootClass = rootClass;
    }

    @Override
    public void create(ConfiguredObjectRecord record)
    {
        assertState(State.OPEN);
        if (_configuredObjectRecords.putIfAbsent(record.getId(), record) != null)
        {
            throw new StoreException("Record with id " + record.getId() + " is already present");
        }
    }

    @Override
    public void update(boolean createIfNecessary, ConfiguredObjectRecord... records)
    {
        assertState(State.OPEN);
        for (ConfiguredObjectRecord record : records)
        {
            if(createIfNecessary)
            {
                _configuredObjectRecords.put(record.getId(), record);
            }
            else
            {
                ConfiguredObjectRecord previousValue = _configuredObjectRecords.replace(record.getId(), record);
                if (previousValue == null)
                {
                    throw new StoreException("Record with id " + record.getId() + " does not exist");
                }
            }
        }
    }

    @Override
    public UUID[] remove(final ConfiguredObjectRecord... objects)
    {
        assertState(State.OPEN);
        List<UUID> removed = new ArrayList<UUID>();
        for (ConfiguredObjectRecord record : objects)
        {
            if (_configuredObjectRecords.remove(record.getId()) != null)
            {
                removed.add(record.getId());
            }
        }
        return removed.toArray(new UUID[removed.size()]);
    }

    @Override
    public void init(ConfiguredObject<?> parent)
    {
        changeState(State.CLOSED, State.CONFIGURED);
    }

    @Override
    public void upgradeStoreStructure() throws StoreException
    {

    }

    @Override
    public void closeConfigurationStore()
    {
        synchronized (_lock)
        {
            _state = State.CLOSED;
        }
        _configuredObjectRecords.clear();
    }


    @Override
    public boolean openConfigurationStore(ConfiguredObjectRecordHandler handler,
                                          final ConfiguredObjectRecord... initialRecords) throws StoreException
    {
        changeState(State.CONFIGURED, State.OPEN);
        boolean isNew = _configuredObjectRecords.isEmpty();
        if(isNew)
        {
            for(ConfiguredObjectRecord record : initialRecords)
            {
                _configuredObjectRecords.put(record.getId(), record);
            }
        }
        for (ConfiguredObjectRecord record : _configuredObjectRecords.values())
        {
            handler.handle(record);
        }
        return isNew;
    }

    @Override
    public void reload(ConfiguredObjectRecordHandler handler) throws StoreException
    {
        assertState(State.OPEN);
        for (ConfiguredObjectRecord record : _configuredObjectRecords.values())
        {
            handler.handle(record);
        }
    }


    @Override
    public MessageStore getMessageStore()
    {
        return _messageStore;
    }

    @Override
    public void onDelete(ConfiguredObject<?> parent)
    {
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


    public List<ConfiguredObjectRecord> getConfiguredObjectRecords()
    {
        return new ArrayList<>(_configuredObjectRecords.values());
    }

}
