/*
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
 */

package org.apache.qpid.server.protocol.v1_0.store;

import java.util.Collection;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.qpid.server.protocol.v1_0.LinkDefinition;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Target;
import org.apache.qpid.server.store.StoreException;

public abstract class AbstractLinkStore implements LinkStore
{
    private final ReentrantReadWriteLock _useOrCloseRWLock = new ReentrantReadWriteLock(true);
    private volatile StoreState _storeState = StoreState.CLOSED;

    protected abstract Collection<LinkDefinition<Source, Target>> doOpenAndLoad(final LinkStoreUpdater updater);
    protected abstract void doClose();
    protected abstract void doDelete();
    protected abstract void doSaveLink(final LinkDefinition<Source, Target> link);
    protected abstract void doDeleteLink(final LinkDefinition<Source, Target> link);

    @Override
    public final Collection<LinkDefinition<Source, Target>> openAndLoad(final LinkStoreUpdater updater) throws StoreException
    {
        _useOrCloseRWLock.readLock().lock();
        try
        {
            if (_storeState != StoreState.CLOSED)
            {
                throw new StoreException("Store is already opened");
            }

            Collection<LinkDefinition<Source, Target>> linkDefinitions = doOpenAndLoad(updater);
            _storeState = StoreState.OPENED;
            return linkDefinitions;
        }
        finally
        {
            _useOrCloseRWLock.readLock().unlock();
        }
    }

    @Override
    public final void close() throws StoreException
    {
        _useOrCloseRWLock.writeLock().lock();
        try
        {
            doClose();
            _storeState = StoreState.CLOSED;
        }
        finally
        {
            _useOrCloseRWLock.writeLock().unlock();
        }
    }

    @Override
    public final void saveLink(final LinkDefinition<Source, Target> link) throws StoreException
    {
        _useOrCloseRWLock.readLock().lock();
        try
        {
            if (_storeState != StoreState.OPENED)
            {
                throw new StoreException("Store is not opened");
            }

            doSaveLink(link);
        }
        finally
        {
            _useOrCloseRWLock.readLock().unlock();
        }
    }

    @Override
    public final void deleteLink(final LinkDefinition<Source, Target> link) throws StoreException
    {
        _useOrCloseRWLock.readLock().lock();
        try
        {
            if (_storeState != StoreState.OPENED)
            {
                throw new StoreException("Store is not opened");
            }

            doDeleteLink(link);
        }
        finally
        {
            _useOrCloseRWLock.readLock().unlock();
        }
    }

    @Override
    public final void delete()
    {
        _useOrCloseRWLock.writeLock().lock();
        try
        {
            close();
            doDelete();
        }
        finally
        {
            _useOrCloseRWLock.writeLock().unlock();
        }
    }

    enum StoreState
    {
        CLOSED, OPENED
    }
}
