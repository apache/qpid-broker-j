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

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.store.handler.DistributedTransactionHandler;
import org.apache.qpid.server.store.handler.MessageHandler;
import org.apache.qpid.server.store.handler.MessageInstanceHandler;

/**
 * MessageStore defines the interface to a storage area, which can be used to preserve the state of messages.
 *
 */
public interface MessageStore
{
    long getNextMessageId();

    String getStoreLocation();

    File getStoreLocationAsFile();

    void addEventListener(EventListener eventListener, Event... events);

    /**
     * Initializes and opens the message store.
     *  @param parent parent object
     *
     */
    void openMessageStore(ConfiguredObject<?> parent);

    /**
     * Requests that the store performs any upgrade work on the store's structure. If there is no
     * upgrade work to be done, this method should return without doing anything.
     *
     * @throws StoreException signals that a problem was encountered trying to upgrade the store.
     * Implementations, on encountering a problem, should endeavour to leave the store in its
     * original state.
     */
    void upgradeStoreStructure() throws StoreException;

    <T extends StorableMessageMetaData> MessageHandle<T> addMessage(T metaData);

    long getInMemorySize();

    long getBytesEvacuatedFromMemory();

    void resetStatistics();

    /**
     * Is this store capable of persisting the data
     *
     * @return true if this store is capable of persisting data
     */
    boolean isPersistent();

    Transaction newTransaction();

    /**
     * Called to close and cleanup any resources used by the message store.
     */
    void closeMessageStore();

    void onDelete(ConfiguredObject<?> parent);

    void addMessageDeleteListener(MessageDeleteListener listener);

    void removeMessageDeleteListener(MessageDeleteListener listener);

    MessageStoreReader newMessageStoreReader();

    interface MessageStoreReader
    {
        void visitMessages(MessageHandler handler) throws StoreException;

        void visitMessageInstances(MessageInstanceHandler handler) throws StoreException;
        void visitMessageInstances(TransactionLogResource queue, MessageInstanceHandler handler) throws StoreException;

        void visitDistributedTransactions(DistributedTransactionHandler handler) throws StoreException;

        StoredMessage<?> getMessage(long messageId);
        void close();
    }

    interface MessageDeleteListener
    {
        void messageDeleted(StoredMessage<?> m);
    }

}
