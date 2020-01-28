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
package org.apache.qpid.server.store.berkeleydb;

import static org.apache.qpid.server.store.berkeleydb.BDBUtils.DEFAULT_DATABASE_CONFIG;
import static org.apache.qpid.server.store.berkeleydb.BDBUtils.abortTransactionSafely;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.util.concurrent.ListenableFuture;
import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Sequence;
import com.sleepycat.je.SequenceConfig;
import com.sleepycat.je.Transaction;
import org.slf4j.Logger;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.EnqueueableMessage;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.store.Event;
import org.apache.qpid.server.store.EventListener;
import org.apache.qpid.server.store.EventManager;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.store.MessageHandle;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.SizeMonitoringSettings;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.server.store.berkeleydb.entry.PreparedTransaction;
import org.apache.qpid.server.store.berkeleydb.entry.QueueEntryKey;
import org.apache.qpid.server.store.berkeleydb.tuple.MessageMetaDataBinding;
import org.apache.qpid.server.store.berkeleydb.tuple.PreparedTransactionBinding;
import org.apache.qpid.server.store.berkeleydb.tuple.QueueEntryBinding;
import org.apache.qpid.server.store.berkeleydb.tuple.XidBinding;
import org.apache.qpid.server.store.handler.DistributedTransactionHandler;
import org.apache.qpid.server.store.handler.MessageHandler;
import org.apache.qpid.server.store.handler.MessageInstanceHandler;
import org.apache.qpid.server.txn.Xid;
import org.apache.qpid.server.util.CachingUUIDFactory;


public abstract class AbstractBDBMessageStore implements MessageStore
{

    private static final int LOCK_RETRY_ATTEMPTS = 5;

    private static final String MESSAGE_META_DATA_DB_NAME = "MESSAGE_METADATA";
    private static final String MESSAGE_META_DATA_SEQ_DB_NAME = "MESSAGE_METADATA.SEQ";
    private static final String MESSAGE_CONTENT_DB_NAME = "MESSAGE_CONTENT";
    private static final String DELIVERY_DB_NAME = "QUEUE_ENTRIES";

    //TODO: Add upgrader to remove BRIDGES and LINKS
    private static final String BRIDGEDB_NAME = "BRIDGES";
    private static final String LINKDB_NAME = "LINKS";
    private static final String XID_DB_NAME = "XIDS";
    private final AtomicBoolean _messageStoreOpen = new AtomicBoolean();

    private final EventManager _eventManager = new EventManager();

    private final DatabaseEntry MESSAGE_METADATA_SEQ_KEY = new DatabaseEntry("MESSAGE_METADATA_SEQ_KEY".getBytes(
            StandardCharsets.UTF_8));

    private final SequenceConfig MESSAGE_METADATA_SEQ_CONFIG = SequenceConfig.DEFAULT.
            setAllowCreate(true).
            setInitialValue(1).
            setWrap(true).
            setCacheSize(100000);
    private ConfiguredObject<?> _parent;
    private long _persistentSizeLowThreshold;
    private long _persistentSizeHighThreshold;

    private boolean _limitBusted;
    private long _totalStoreSize;
    private final Random _lockConflictRandom = new Random();
    private final AtomicLong _inMemorySize = new AtomicLong();
    private final AtomicLong _bytesEvacuatedFromMemory = new AtomicLong();
    private final Set<StoredBDBMessage<?>> _messages = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Set<MessageDeleteListener> _messageDeleteListeners = Collections.newSetFromMap(new ConcurrentHashMap<>());

    @Override
    public void openMessageStore(final ConfiguredObject<?> parent)
    {
        if (_messageStoreOpen.compareAndSet(false, true))
        {
            _parent = parent;

            final SizeMonitoringSettings sizeMonitorSettings = (SizeMonitoringSettings) parent;
            _persistentSizeHighThreshold = sizeMonitorSettings.getStoreOverfullSize();
            _persistentSizeLowThreshold = sizeMonitorSettings.getStoreUnderfullSize();

            if (_persistentSizeLowThreshold > _persistentSizeHighThreshold || _persistentSizeLowThreshold < 0L)
            {
                _persistentSizeLowThreshold = _persistentSizeHighThreshold;
            }

            doOpen(parent);
        }
    }

    protected abstract void doOpen(final ConfiguredObject<?> parent);

    @Override
    public void closeMessageStore()
    {
        if (_messageStoreOpen.compareAndSet(true, false))
        {
            for (StoredBDBMessage<?> message : _messages)
            {
                message.clear(true);
            }
            _messages.clear();
            _inMemorySize.set(0);
            _bytesEvacuatedFromMemory.set(0);
            doClose();
        }
    }

    protected abstract void doClose();

    @Override
    public void upgradeStoreStructure() throws StoreException
    {
        try
        {
            getEnvironmentFacade().upgradeIfNecessary(getParent());

            // TODO this relies on the fact that the VH will call upgrade just before putting the VH into service.
            _totalStoreSize = getSizeOnDisk();
        }
        catch(RuntimeException e)
        {
            throw getEnvironmentFacade().handleDatabaseException("Cannot upgrade store", e);
        }
    }

    void deleteMessageStoreDatabases()
    {
        try
        {
            for (String db : Arrays.asList(MESSAGE_META_DATA_DB_NAME,
                                          MESSAGE_META_DATA_SEQ_DB_NAME,
                                          MESSAGE_CONTENT_DB_NAME,
                                          DELIVERY_DB_NAME,
                                          XID_DB_NAME))
            {
                try
                {

                    getEnvironmentFacade().deleteDatabase(db);
                }
                catch (DatabaseNotFoundException ignore)
                {
                }

            }
        }
        catch (IllegalStateException e)
        {
            getLogger().warn("Could not delete message store databases: {}", e.getMessage());
        }
        catch (RuntimeException e)
        {
            getEnvironmentFacade().handleDatabaseException("Deletion of message store databases failed", e);
        }
    }

    @Override
    public <T extends StorableMessageMetaData> MessageHandle<T> addMessage(T metaData)
    {

        long newMessageId = getNextMessageId();

        return createStoredBDBMessage(newMessageId, metaData, false);
    }

    private <T extends StorableMessageMetaData> StoredBDBMessage<T> createStoredBDBMessage(final long newMessageId,
                                                                                           final T metaData,
                                                                                           final boolean recovered)
    {
        final StoredBDBMessage<T> message = new StoredBDBMessage<>(newMessageId, metaData, recovered);
        _messages.add(message);
        return message;
    }

    @Override
    public long getNextMessageId()
    {
        long newMessageId;
        try
        {
            // The implementations of sequences mean that there is only a transaction
            // after every n sequence values, where n is the MESSAGE_METADATA_SEQ_CONFIG.getCacheSize()

            Sequence mmdSeq = getEnvironmentFacade().openSequence(getMessageMetaDataSeqDb(),
                                                              MESSAGE_METADATA_SEQ_KEY,
                                                              MESSAGE_METADATA_SEQ_CONFIG);
            newMessageId = mmdSeq.get(null, 1);
        }
        catch (RuntimeException de)
        {
            throw getEnvironmentFacade().handleDatabaseException("Cannot get sequence value for new message", de);
        }
        return newMessageId;
    }

    @Override
    public long getInMemorySize()
    {
        return _inMemorySize.get();
    }

    @Override
    public long getBytesEvacuatedFromMemory()
    {
        return _bytesEvacuatedFromMemory.get();
    }

    @Override
    public boolean isPersistent()
    {
        return true;
    }

    @Override
    public org.apache.qpid.server.store.Transaction newTransaction()
    {
        checkMessageStoreOpen();

        return new BDBTransaction();
    }

    @Override
    public void addEventListener(final EventListener eventListener, final Event... events)
    {
        _eventManager.addEventListener(eventListener, events);
    }

    @Override
    public MessageStoreReader newMessageStoreReader()
    {
        return new BDBMessageStoreReader();
    }

    /**
     * Retrieves message meta-data.
     *
     * @param messageId The message to get the meta-data for.
     *
     * @return The message meta data.
     *
     * @throws org.apache.qpid.server.store.StoreException If the operation fails for any reason, or if the specified message does not exist.
     */
    StorableMessageMetaData getMessageMetaData(long messageId) throws StoreException
    {
        getLogger().debug("public MessageMetaData getMessageMetaData(Long messageId = {}): called", messageId);

        DatabaseEntry key = new DatabaseEntry();
        LongBinding.longToEntry(messageId, key);
        DatabaseEntry value = new DatabaseEntry();
        MessageMetaDataBinding messageBinding = MessageMetaDataBinding.getInstance();

        try
        {
            OperationStatus status = getMessageMetaDataDb().get(null, key, value, LockMode.READ_UNCOMMITTED);
            if (status != OperationStatus.SUCCESS)
            {
                throw new StoreException("Metadata not found for message with id " + messageId);
            }

            StorableMessageMetaData mdd = messageBinding.entryToObject(value);

            return mdd;
        }
        catch (RuntimeException e)
        {
            throw getEnvironmentFacade().handleDatabaseException("Error reading message metadata for message with id "
                                                                 + messageId
                                                                 + ": "
                                                                 + e.getMessage(), e);
        }
    }

    void removeMessage(long messageId, boolean sync) throws StoreException
    {
        boolean complete = false;
        Transaction tx = null;
        int attempts = 0;
        try
        {
            do
            {
                tx = null;
                try
                {
                    tx = getEnvironmentFacade().beginTransaction(null);

                    //remove the message meta data from the store
                    DatabaseEntry key = new DatabaseEntry();
                    LongBinding.longToEntry(messageId, key);

                    getLogger().debug("Removing message id {}", messageId);


                    OperationStatus status = getMessageMetaDataDb().delete(tx, key);
                    if (status == OperationStatus.NOTFOUND)
                    {
                        getLogger().debug("Message id {} not found (attempt to remove failed - probably application initiated rollback)",messageId);
                    }

                    getLogger().debug("Deleted metadata for message {}", messageId);

                    //now remove the content data from the store if there is any.
                    DatabaseEntry contentKeyEntry = new DatabaseEntry();
                    LongBinding.longToEntry(messageId, contentKeyEntry);
                    getMessageContentDb().delete(tx, contentKeyEntry);

                    getLogger().debug("Deleted content for message {}", messageId);

                    getEnvironmentFacade().commit(tx, sync);

                    complete = true;
                    tx = null;
                }
                catch (LockConflictException e)
                {
                    try
                    {
                        if(tx != null)
                        {
                            tx.abort();
                        }
                    }
                    catch(RuntimeException e2)
                    {
                        getLogger().warn("Unable to abort transaction after LockConflictException on removal of message with id {}", messageId,
                                e2);
                        // rethrow the original log conflict exception, the secondary exception should already have
                        // been logged.
                        throw getEnvironmentFacade().handleDatabaseException("Cannot remove message with id "
                                                                             + messageId, e);
                    }

                    sleepOrThrowOnLockConflict(attempts++, "Cannot remove messages", e);
                }
            }
            while(!complete);
        }
        catch (RuntimeException e)
        {
            if (getLogger().isDebugEnabled())
            {
                getLogger().debug("Unexpected BDB exception", e);
            }

            try
            {
                abortTransactionSafely(tx,
                                       getEnvironmentFacade());
            }
            finally
            {
                tx = null;
            }

            throw getEnvironmentFacade().handleDatabaseException("Error removing message with id "
                                                                 + messageId
                                                                 + " from database: "
                                                                 + e.getMessage(), e);
        }
        finally
        {
            try
            {
                abortTransactionSafely(tx,
                                       getEnvironmentFacade());
            }
            finally
            {
                tx = null;
            }
        }
    }

    QpidByteBuffer getAllContent(long messageId) throws StoreException
    {
        DatabaseEntry contentKeyEntry = new DatabaseEntry();
        LongBinding.longToEntry(messageId, contentKeyEntry);
        DatabaseEntry value = new DatabaseEntry();

        getLogger().debug("Message Id: {} Getting content body", messageId);

        try
        {
            OperationStatus status = getMessageContentDb().get(null, contentKeyEntry, value, LockMode.READ_UNCOMMITTED);

            if (status == OperationStatus.SUCCESS)
            {
                byte[] data = value.getData();
                int offset = value.getOffset();
                int length = value.getSize();
                QpidByteBuffer buf = QpidByteBuffer.allocateDirect(length);
                buf.put(data, offset, length);
                buf.flip();
                return buf;
            }
            else
            {
                throw new StoreException("Unable to find message with id " + messageId);
            }

        }
        catch (RuntimeException e)
        {
            throw getEnvironmentFacade().handleDatabaseException("Error getting AMQMessage with id "
                                                                 + messageId
                                                                 + " to database: "
                                                                 + e.getMessage(), e);
        }
    }

    private void visitMessagesInternal(MessageHandler handler, EnvironmentFacade environmentFacade)
    {
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry value = new DatabaseEntry();
        MessageMetaDataBinding valueBinding = MessageMetaDataBinding.getInstance();

        try(Cursor cursor = getMessageMetaDataDb().openCursor(null, null))
        {
            while (cursor.getNext(key, value, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS)
            {
                long messageId = LongBinding.entryToLong(key);
                StorableMessageMetaData metaData = valueBinding.entryToObject(value);
                StoredBDBMessage message = createStoredBDBMessage(messageId, metaData, true);
                if (!handler.handle(message))
                {
                    break;
                }
            }
        }
        catch (RuntimeException e)
        {
            throw environmentFacade.handleDatabaseException("Cannot visit messages", e);
        }
    }

    private void sleepOrThrowOnLockConflict(int attempts, String throwMessage, LockConflictException cause)
    {
        if (attempts < LOCK_RETRY_ATTEMPTS)
        {
            getLogger().info("Lock conflict exception. Retrying (attempt {} of {})", attempts, LOCK_RETRY_ATTEMPTS);
            try
            {
                Thread.sleep(500l + (long)(500l * _lockConflictRandom.nextDouble()));
            }
            catch (InterruptedException ie)
            {
                Thread.currentThread().interrupt();
                throw getEnvironmentFacade().handleDatabaseException(throwMessage, cause);
            }
        }
        else
        {
            // rethrow the lock conflict exception since we could not solve by retrying
            throw getEnvironmentFacade().handleDatabaseException(throwMessage, cause);
        }
    }

    private StoredBDBMessage<?> getMessageInternal(long messageId, EnvironmentFacade environmentFacade)
    {
        try
        {
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry value = new DatabaseEntry();
            MessageMetaDataBinding valueBinding = MessageMetaDataBinding.getInstance();
            LongBinding.longToEntry(messageId, key);
            if(getMessageMetaDataDb().get(null, key, value, LockMode.READ_COMMITTED) == OperationStatus.SUCCESS)
            {
                StorableMessageMetaData metaData = valueBinding.entryToObject(value);
                StoredBDBMessage message = createStoredBDBMessage(messageId, metaData, true);
                return message;
            }
            else
            {
                return null;
            }

        }
        catch (RuntimeException e)
        {
            throw environmentFacade.handleDatabaseException("Cannot visit messages", e);
        }
    }

    /**
     * Stores a chunk of message data.
     *
     * @param tx         The transaction for the operation.
     * @param messageId       The message to store the data for.
     * @param contentBody     The content of the data chunk.
     *
     * @throws org.apache.qpid.server.store.StoreException If the operation fails for any reason, or if the specified message does not exist.
     */
    private void addContent(final Transaction tx, long messageId, QpidByteBuffer contentBody) throws StoreException
    {
        DatabaseEntry key = new DatabaseEntry();
        LongBinding.longToEntry(messageId, key);
        DatabaseEntry value = new DatabaseEntry();

        byte[] data = new byte[contentBody.remaining()];
        contentBody.copyTo(data);
        value.setData(data);
        try
        {
            OperationStatus status = getMessageContentDb().put(tx, key, value);
            if (status != OperationStatus.SUCCESS)
            {
                throw new StoreException("Error adding content for message id " + messageId + ": " + status);
            }

            getLogger().debug("Storing content for message {} in transaction {}", messageId, tx);

        }
        catch (RuntimeException e)
        {
            throw getEnvironmentFacade().handleDatabaseException("Error writing AMQMessage with id "
                                                                 + messageId
                                                                 + " to database: "
                                                                 + e.getMessage(), e);
        }
    }

    /**
     * Stores message meta-data.
     *
     * @param tx         The transaction for the operation.
     * @param messageId       The message to store the data for.
     * @param messageMetaData The message meta data to store.
     *
     * @throws org.apache.qpid.server.store.StoreException If the operation fails for any reason, or if the specified message does not exist.
     */
    private void storeMetaData(final Transaction tx, long messageId,
                               StorableMessageMetaData messageMetaData)
            throws StoreException
    {
        getLogger().debug("storeMetaData called for transaction {}, messageId {}, messageMetaData {} ",
                          tx, messageId, messageMetaData);

        DatabaseEntry key = new DatabaseEntry();
        LongBinding.longToEntry(messageId, key);
        DatabaseEntry value = new DatabaseEntry();

        MessageMetaDataBinding messageBinding = MessageMetaDataBinding.getInstance();
        messageBinding.objectToEntry(messageMetaData, value);
        try
        {
            getMessageMetaDataDb().put(tx, key, value);
            getLogger().debug("Storing message metadata for message id {} in transaction {}", messageId, tx);

        }
        catch (RuntimeException e)
        {
            throw getEnvironmentFacade().handleDatabaseException("Error writing message metadata with id "
                                                                 + messageId
                                                                 + " to database: "
                                                                 + e.getMessage(), e);
        }
    }


    private static final byte[] ENQUEUE_RECORD_VALUE = new byte[] {};
    /**
     * Places a message onto a specified queue, in a given transaction.
     *
     * @param tx   The transaction for the operation.
     * @param queue     The the queue to place the message on.
     * @param messageId The message to enqueue.
     *
     * @throws org.apache.qpid.server.store.StoreException If the operation fails for any reason.
     */
    private void enqueueMessage(final Transaction tx, final TransactionLogResource queue,
                                long messageId) throws StoreException
    {

        DatabaseEntry key = new DatabaseEntry();
        QueueEntryKey queueEntryKey = new QueueEntryKey(queue.getId(), messageId);
        QueueEntryBinding.objectToEntry(queueEntryKey, key);
        DatabaseEntry value = new DatabaseEntry();
        value.setData(ENQUEUE_RECORD_VALUE, 0, ENQUEUE_RECORD_VALUE.length);

        try
        {
            if (getLogger().isDebugEnabled())
            {
                getLogger().debug("Enqueuing message {} on queue {} with id {} in transaction {}",
                                  messageId, queue.getName(), queue.getId(), tx);
            }
            getDeliveryDb().put(tx, key, value);
        }
        catch (RuntimeException e)
        {
            if (getLogger().isDebugEnabled())
            {
                getLogger().debug("Failed to enqueue: {}", e.getMessage(), e);
            }
            throw getEnvironmentFacade().handleDatabaseException("Error writing enqueued message with id "
                                                                 + messageId
                                                                 + " for queue "
                                                                 + queue.getName()
                                                                 + " with id "
                                                                 + queue.getId()
                                                                 + " to database", e);
        }
    }

    /**
     * Extracts a message from a specified queue, in a given transaction.
     *
     * @param tx   The transaction for the operation.
     * @param queueId     The id of the queue to take the message from.
     * @param messageId The message to dequeue.
     *
     * @throws org.apache.qpid.server.store.StoreException If the operation fails for any reason, or if the specified message does not exist.
     */
    private void dequeueMessage(final Transaction tx, final UUID queueId,
                                long messageId) throws StoreException
    {

        DatabaseEntry key = new DatabaseEntry();
        QueueEntryKey queueEntryKey = new QueueEntryKey(queueId, messageId);
        UUID id = queueId;
        QueueEntryBinding.objectToEntry(queueEntryKey, key);

        getLogger().debug("Dequeue message id {} from queue with id {}", messageId, id);

        try
        {

            OperationStatus status = getDeliveryDb().delete(tx, key);
            if (status == OperationStatus.NOTFOUND)
            {
                throw new StoreException("Unable to find message with id " + messageId + " on queue with id "  + id);
            }
            else if (status != OperationStatus.SUCCESS)
            {
                throw new StoreException("Unable to remove message with id " + messageId + " on queue with id " + id);
            }

            getLogger().debug("Removed message {} on queue with id {}", messageId, id);

        }
        catch (RuntimeException e)
        {
            if (getLogger().isDebugEnabled())
            {
                getLogger().debug("Failed to dequeue message {} in transaction {}", messageId, tx, e);
            }

            throw getEnvironmentFacade().handleDatabaseException("Error accessing database while dequeuing message: "
                                                                 + e.getMessage(), e);
        }
    }

    private List<Runnable> recordXid(Transaction txn,
                                     long format,
                                     byte[] globalId,
                                     byte[] branchId,
                                     org.apache.qpid.server.store.Transaction.EnqueueRecord[] enqueues,
                                     org.apache.qpid.server.store.Transaction.DequeueRecord[] dequeues) throws StoreException
    {
        DatabaseEntry key = new DatabaseEntry();
        Xid xid = new Xid(format, globalId, branchId);
        XidBinding keyBinding = XidBinding.getInstance();
        keyBinding.objectToEntry(xid,key);

        DatabaseEntry value = new DatabaseEntry();
        PreparedTransaction preparedTransaction = new PreparedTransaction(enqueues, dequeues);
        PreparedTransactionBinding.objectToEntry(preparedTransaction, value);
        for(org.apache.qpid.server.store.Transaction.EnqueueRecord enqueue : enqueues)
        {
            StoredMessage storedMessage = enqueue.getMessage().getStoredMessage();
            if(storedMessage instanceof StoredBDBMessage)
            {
                ((StoredBDBMessage) storedMessage).store(txn);
            }
        }

        try
        {
            getXidDb().put(txn, key, value);
            return Collections.emptyList();
        }
        catch (RuntimeException e)
        {
            if (getLogger().isDebugEnabled())
            {
                getLogger().debug("Failed to write xid: {}", e.getMessage(), e);
            }
            throw getEnvironmentFacade().handleDatabaseException("Error writing xid to database", e);
        }
    }

    private void removeXid(Transaction txn, long format, byte[] globalId, byte[] branchId)
            throws StoreException
    {
        DatabaseEntry key = new DatabaseEntry();
        Xid xid = new Xid(format, globalId, branchId);
        XidBinding keyBinding = XidBinding.getInstance();

        keyBinding.objectToEntry(xid, key);


        try
        {

            OperationStatus status = getXidDb().delete(txn, key);
            if (status == OperationStatus.NOTFOUND)
            {
                throw new StoreException("Unable to find xid");
            }
            else if (status != OperationStatus.SUCCESS)
            {
                throw new StoreException("Unable to remove xid");
            }

        }
        catch (RuntimeException e)
        {
            if (getLogger().isDebugEnabled())
            {
                getLogger().error("Failed to remove xid in transaction {}", e);
            }

            throw getEnvironmentFacade().handleDatabaseException("Error accessing database while removing xid: "
                                                                 + e.getMessage(), e);
        }
    }

    /**
     * Commits all operations performed within a given transaction.
     *
     * @param tx The transaction to commit all operations for.
     *
     * @throws org.apache.qpid.server.store.StoreException If the operation fails for any reason.
     */
    private void commitTranImpl(final Transaction tx, boolean syncCommit) throws StoreException
    {
        if (tx == null)
        {
            throw new StoreException("Fatal internal error: transactional is null at commitTran");
        }

        getEnvironmentFacade().commit(tx, syncCommit);

        getLogger().debug("commitTranImpl completed {} transaction {}",
                          syncCommit ? "synchronous" : "asynchronous", tx);


    }

    private <X> ListenableFuture<X> commitTranAsyncImpl(final Transaction tx, X val) throws StoreException
    {
        if (tx == null)
        {
            throw new StoreException("Fatal internal error: transactional is null at commitTran");
        }

        ListenableFuture<X> result = getEnvironmentFacade().commitAsync(tx, val);

        getLogger().debug("commitTranAsynImpl completed transaction {}", tx);

        return result;
    }


    /**
     * Abandons all operations performed within a given transaction.
     *
     * @param tx The transaction to abandon.
     *
     * @throws org.apache.qpid.server.store.StoreException If the operation fails for any reason.
     */
    private void abortTran(final Transaction tx) throws StoreException
    {
        getLogger().debug("abortTran called for transaction {}", tx);

        try
        {
            tx.abort();
        }
        catch (RuntimeException e)
        {
            throw getEnvironmentFacade().handleDatabaseException("Error aborting transaction: " + e.getMessage(), e);
        }
    }

    private void storedSizeChangeOccurred(final int delta) throws StoreException
    {
        try
        {
            storedSizeChange(delta);
        }
        catch(RuntimeException e)
        {
            throw getEnvironmentFacade().handleDatabaseException("Stored size change exception", e);
        }
    }

    private void storedSizeChange(final int delta)
    {
        if(getPersistentSizeHighThreshold() > 0)
        {
            synchronized (this)
            {
                // the delta supplied is an approximation of a store size change. we don;t want to check the statistic every
                // time, so we do so only when there's been enough change that it is worth looking again. We do this by
                // assuming the total size will change by less than twice the amount of the message data change.
                long newSize = _totalStoreSize += 2*delta;

                if(!_limitBusted &&  newSize > getPersistentSizeHighThreshold())
                {
                    _totalStoreSize = getSizeOnDisk();

                    if(_totalStoreSize > getPersistentSizeHighThreshold())
                    {
                        _limitBusted = true;
                        _eventManager.notifyEvent(Event.PERSISTENT_MESSAGE_SIZE_OVERFULL);
                    }
                }
                else if(_limitBusted && newSize < getPersistentSizeLowThreshold())
                {
                    long oldSize = _totalStoreSize;
                    _totalStoreSize = getSizeOnDisk();

                    if(oldSize <= _totalStoreSize)
                    {

                        reduceSizeOnDisk();

                        _totalStoreSize = getSizeOnDisk();

                    }

                    if(_totalStoreSize < getPersistentSizeLowThreshold())
                    {
                        _limitBusted = false;
                        _eventManager.notifyEvent(Event.PERSISTENT_MESSAGE_SIZE_UNDERFULL);
                    }


                }
            }
        }
    }

    private void reduceSizeOnDisk()
    {
        getEnvironmentFacade().reduceSizeOnDisk();
    }

    private long getSizeOnDisk()
    {
        return getEnvironmentFacade().getTotalLogSize();
    }

    private Database getMessageContentDb()
    {
        return getEnvironmentFacade().openDatabase(MESSAGE_CONTENT_DB_NAME, DEFAULT_DATABASE_CONFIG);
    }

    private Database getMessageMetaDataDb()
    {
        return getEnvironmentFacade().openDatabase(MESSAGE_META_DATA_DB_NAME, DEFAULT_DATABASE_CONFIG);
    }

    private Database getMessageMetaDataSeqDb()
    {
        return getEnvironmentFacade().openDatabase(MESSAGE_META_DATA_SEQ_DB_NAME, DEFAULT_DATABASE_CONFIG);
    }

    private Database getDeliveryDb()
    {
        return getEnvironmentFacade().openDatabase(DELIVERY_DB_NAME, DEFAULT_DATABASE_CONFIG);
    }

    private Database getXidDb()
    {
        return getEnvironmentFacade().openDatabase(XID_DB_NAME, DEFAULT_DATABASE_CONFIG);
    }

    private void checkMessageStoreOpen()
    {
        if (!_messageStoreOpen.get())
        {
            throw new IllegalStateException("Message store is not open");
        }
    }

    protected boolean isMessageStoreOpen()
    {
        return _messageStoreOpen.get();
    }

    protected final ConfiguredObject<?> getParent()
    {
        return _parent;
    }

    protected abstract EnvironmentFacade getEnvironmentFacade();

    private long getPersistentSizeLowThreshold()
    {
        return _persistentSizeLowThreshold;
    }

    private long getPersistentSizeHighThreshold()
    {
        return _persistentSizeHighThreshold;
    }

    protected abstract Logger getLogger();

    private static class MessageDataRef<T extends StorableMessageMetaData>
    {
        private volatile T _metaData;
        private volatile QpidByteBuffer _data;
        private volatile boolean _isHardRef;

        private MessageDataRef(final T metaData, boolean isHardRef)
        {
            this(metaData, null, isHardRef);
        }

        private MessageDataRef(final T metaData, QpidByteBuffer data, boolean isHardRef)
        {
            _metaData = metaData;
            _data = data;
            _isHardRef = isHardRef;
        }

        public T getMetaData()
        {
            return _metaData;
        }

        public QpidByteBuffer getData()
        {
            return _data;
        }

        public void setData(final QpidByteBuffer data)
        {
            _data = data;
        }

        public boolean isHardRef()
        {
            return _isHardRef;
        }

        public void setSoft()
        {
            _isHardRef = false;
        }

        public void reallocate()
        {
            if(_metaData != null)
            {
                _metaData.reallocate();
            }
            _data = QpidByteBuffer.reallocateIfNecessary(_data);
        }

        public long clear(boolean close)
        {
            long bytesCleared = 0;
            if(_data != null)
            {
                if(_data != null)
                {
                    bytesCleared += _data.remaining();
                    _data.dispose();
                    _data = null;
                }
            }
            if (_metaData != null)
            {
                bytesCleared += _metaData.getStorableSize();
                try
                {
                    if (close)
                    {
                        _metaData.dispose();
                    }
                    else
                    {
                        _metaData.clearEncodedForm();
                    }
                }
                finally
                {
                    _metaData = null;
                }
            }
            return bytesCleared;
        }
    }

    final class StoredBDBMessage<T extends StorableMessageMetaData> implements StoredMessage<T>, MessageHandle<T>
    {

        private final long _messageId;
        private final int _contentSize;
        private final int _metadataSize;
        private MessageDataRef<T> _messageDataRef;

        StoredBDBMessage(long messageId, T metaData, boolean isRecovered)
        {
            _messageId = messageId;

            _messageDataRef = new MessageDataRef<>(metaData, !isRecovered);

            _contentSize = metaData.getContentSize();
            _metadataSize = metaData.getStorableSize();
            _inMemorySize.addAndGet(_metadataSize);
        }

        @Override
        public synchronized T getMetaData()
        {
            if (_messageDataRef == null)
            {
                return null;
            }
            else
            {
                T metaData = _messageDataRef.getMetaData();

                if (metaData == null)
                {
                    checkMessageStoreOpen();
                    metaData = (T) getMessageMetaData(_messageId);
                    _messageDataRef = new MessageDataRef<>(metaData, _messageDataRef.getData(), false);
                    _inMemorySize.addAndGet(getMetadataSize());
                }
                return metaData;
            }
        }

        @Override
        public long getMessageNumber()
        {
            return _messageId;
        }

        @Override
        public synchronized void addContent(QpidByteBuffer src)
        {
            try(QpidByteBuffer data = _messageDataRef.getData())
            {
                if(data == null)
                {
                    _messageDataRef.setData(src.slice());
                }
                else
                {
                    _messageDataRef.setData(QpidByteBuffer.concatenate(Arrays.asList(data, src)));
                }
            }
        }

        @Override
        public StoredMessage<T> allContentAdded()
        {
            _inMemorySize.addAndGet(getContentSize());
            return this;
        }

        /**
         * returns QBB containing the content. The caller must not dispose of them because we keep a reference in _messageDataRef.
         */
        private QpidByteBuffer getContentAsByteBuffer()
        {
            QpidByteBuffer data = _messageDataRef == null ? QpidByteBuffer.emptyQpidByteBuffer() : _messageDataRef.getData();
            if(data == null)
            {
                if(stored())
                {
                    checkMessageStoreOpen();
                    data = AbstractBDBMessageStore.this.getAllContent(_messageId);
                    _messageDataRef.setData(data);
                    _inMemorySize.addAndGet(getContentSize());
                }
                else
                {
                    data = QpidByteBuffer.emptyQpidByteBuffer();
                }
            }
            return data;
        }


        @Override
        public synchronized QpidByteBuffer getContent(int offset, int length)
        {
            QpidByteBuffer contentAsByteBuffer = getContentAsByteBuffer();
            if (length == Integer.MAX_VALUE)
            {
                length = contentAsByteBuffer.remaining();
            }
            return contentAsByteBuffer.view(offset, length);
        }

        @Override
        public int getContentSize()
        {
            return _contentSize;
        }

        @Override
        public int getMetadataSize()
        {
            return _metadataSize;
        }

        synchronized void store(Transaction txn)
        {
            if (!stored())
            {
                AbstractBDBMessageStore.this.storeMetaData(txn, _messageId, _messageDataRef.getMetaData());
                AbstractBDBMessageStore.this.addContent(txn, _messageId,
                                                        _messageDataRef.getData() == null
                                                                ? QpidByteBuffer.emptyQpidByteBuffer()
                                                                : _messageDataRef.getData());
                _messageDataRef.setSoft();
            }
        }

        synchronized void flushToStore()
        {
            if (_messageDataRef != null)
            {
                if (!stored())
                {
                    checkMessageStoreOpen();

                    Transaction txn;
                    try
                    {
                        txn = getEnvironmentFacade().beginTransaction(null);
                    }
                    catch (RuntimeException e)
                    {
                        throw getEnvironmentFacade().handleDatabaseException("failed to begin transaction", e);
                    }
                    store(txn);
                    getEnvironmentFacade().commit(txn, false);

                }
            }
        }

        @Override
        public synchronized void remove()
        {
            checkMessageStoreOpen();
            _messages.remove(this);
            if(stored())
            {
                removeMessage(_messageId, false);
                storedSizeChangeOccurred(-getContentSize());
            }

            final T metaData;
            long bytesCleared = 0;
            if ((metaData =_messageDataRef.getMetaData()) != null)
            {
                bytesCleared += getMetadataSize();
                metaData.dispose();
            }

            try (QpidByteBuffer data = _messageDataRef.getData())
            {
                if (data != null)
                {
                    bytesCleared += getContentSize();
                    _messageDataRef.setData(null);
                }
            }
            _messageDataRef = null;
            _inMemorySize.addAndGet(-bytesCleared);
            if (!_messageDeleteListeners.isEmpty())
            {
                for (final MessageDeleteListener messageDeleteListener : _messageDeleteListeners)
                {
                    messageDeleteListener.messageDeleted(this);
                }
            }
        }

        @Override
        public synchronized boolean isInContentInMemory()
        {
            return _messageDataRef != null && (_messageDataRef.isHardRef() || _messageDataRef.getData() != null);
        }

        @Override
        public synchronized long getInMemorySize()
        {
            long size = 0;
            if (_messageDataRef != null)
            {
                if (_messageDataRef.isHardRef())
                {
                    size += getMetadataSize() + getContentSize();
                }
                else
                {
                    if (_messageDataRef.getMetaData() != null)
                    {
                        size += getMetadataSize();
                    }
                    if (_messageDataRef.getData() != null)
                    {
                        size += getContentSize();
                    }
                }
            }
            return size;
        }

        private boolean stored()
        {
            return _messageDataRef != null && !_messageDataRef.isHardRef();
        }

        @Override
        public synchronized boolean flowToDisk()
        {

            flushToStore();
            if(_messageDataRef != null && !_messageDataRef.isHardRef())
            {
                final long bytesCleared = _messageDataRef.clear(false);
                _inMemorySize.addAndGet(-bytesCleared);
                _bytesEvacuatedFromMemory.addAndGet(bytesCleared);
            }
            return true;
        }

        @Override
        public String toString()
        {
            return this.getClass() + "[messageId=" + _messageId + "]";
        }

        @Override
        public synchronized void reallocate()
        {
            if(_messageDataRef != null)
            {
                _messageDataRef.reallocate();
            }
        }

        public synchronized void clear(boolean close)
        {
            if (_messageDataRef != null)
            {
                _messageDataRef.clear(close);
            }
        }
    }


    private class BDBTransaction implements org.apache.qpid.server.store.Transaction
    {
        private Transaction _txn;
        private int _storeSizeIncrease;
        private final List<Runnable> _preCommitActions = new ArrayList<>();
        private final List<Runnable> _postCommitActions = new ArrayList<>();

        private BDBTransaction() throws StoreException
        {
            try
            {
                _txn = getEnvironmentFacade().beginTransaction(null);
            }
            catch(RuntimeException e)
            {
                throw getEnvironmentFacade().handleDatabaseException("Cannot create store transaction", e);
            }
        }

        @Override
        public MessageEnqueueRecord enqueueMessage(TransactionLogResource queue, EnqueueableMessage message) throws StoreException
        {
            checkMessageStoreOpen();

            if(message.getStoredMessage() instanceof StoredBDBMessage)
            {
                final StoredBDBMessage storedMessage = (StoredBDBMessage) message.getStoredMessage();
                final long contentSize = storedMessage.getContentSize();
                _preCommitActions.add(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        storedMessage.store(_txn);
                        _storeSizeIncrease += contentSize;
                    }
                });

            }

            AbstractBDBMessageStore.this.enqueueMessage(_txn, queue, message.getMessageNumber());
            return new BDBEnqueueRecord(queue.getId(), message.getMessageNumber());
        }

        @Override
        public void dequeueMessage(final MessageEnqueueRecord enqueueRecord)
        {
            checkMessageStoreOpen();

            AbstractBDBMessageStore.this.dequeueMessage(_txn, enqueueRecord.getQueueId(),
                                                        enqueueRecord.getMessageNumber());
        }

        @Override
        public void commitTran() throws StoreException
        {
            checkMessageStoreOpen();
            doPreCommitActions();
            AbstractBDBMessageStore.this.commitTranImpl(_txn, true);
            doPostCommitActions();
            AbstractBDBMessageStore.this.storedSizeChangeOccurred(_storeSizeIncrease);
        }

        private void doPreCommitActions()
        {
            for(Runnable action : _preCommitActions)
            {
                action.run();
            }
            _preCommitActions.clear();
        }

        private void doPostCommitActions()
        {
            // QPID-7447: prevent unnecessary allocation of empty iterator
            if (!_postCommitActions.isEmpty())
            {
                for (Runnable action : _postCommitActions)
                {
                    action.run();
                }
                _postCommitActions.clear();
            }
        }

        @Override
        public <X> ListenableFuture<X> commitTranAsync(final X val) throws StoreException
        {
            checkMessageStoreOpen();
            doPreCommitActions();
            AbstractBDBMessageStore.this.storedSizeChangeOccurred(_storeSizeIncrease);
            ListenableFuture<X> futureResult = AbstractBDBMessageStore.this.commitTranAsyncImpl(_txn, val);
            doPostCommitActions();
            return futureResult;
        }

        @Override
        public void abortTran() throws StoreException
        {
            checkMessageStoreOpen();
            _preCommitActions.clear();
            _postCommitActions.clear();
            AbstractBDBMessageStore.this.abortTran(_txn);
        }

        @Override
        public void removeXid(final StoredXidRecord record)
        {
            checkMessageStoreOpen();

            AbstractBDBMessageStore.this.removeXid(_txn, record.getFormat(), record.getGlobalId(), record.getBranchId());
        }

        @Override
        public StoredXidRecord recordXid(final long format, final byte[] globalId, final byte[] branchId, final EnqueueRecord[] enqueues,
                                         final DequeueRecord[] dequeues) throws StoreException
        {
            checkMessageStoreOpen();

            _postCommitActions.addAll(AbstractBDBMessageStore.this.recordXid(_txn, format, globalId, branchId, enqueues, dequeues));
            return new BDBStoredXidRecord(format, globalId, branchId);
        }

    }

    @Override
    public void addMessageDeleteListener(final MessageDeleteListener listener)
    {
        _messageDeleteListeners.add(listener);
    }

    @Override
    public void removeMessageDeleteListener(final MessageDeleteListener listener)
    {
        _messageDeleteListeners.remove(listener);
    }

    private static class BDBStoredXidRecord implements org.apache.qpid.server.store.Transaction.StoredXidRecord
    {
        private final long _format;
        private final byte[] _globalId;
        private final byte[] _branchId;

        public BDBStoredXidRecord(final long format, final byte[] globalId, final byte[] branchId)
        {
            _format = format;
            _globalId = globalId;
            _branchId = branchId;
        }

        @Override
        public long getFormat()
        {
            return _format;
        }

        @Override
        public byte[] getGlobalId()
        {
            return _globalId;
        }

        @Override
        public byte[] getBranchId()
        {
            return _branchId;
        }

        @Override
        public boolean equals(final Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || getClass() != o.getClass())
            {
                return false;
            }

            final BDBStoredXidRecord that = (BDBStoredXidRecord) o;

            return _format == that._format
                   && Arrays.equals(_globalId, that._globalId)
                   && Arrays.equals(_branchId, that._branchId);

        }

        @Override
        public int hashCode()
        {
            int result = (int) (_format ^ (_format >>> 32));
            result = 31 * result + Arrays.hashCode(_globalId);
            result = 31 * result + Arrays.hashCode(_branchId);
            return result;
        }
    }
    public static class BDBEnqueueRecord implements MessageEnqueueRecord
    {
        private final UUID _queueId;

        private final long _messageNumber;

        public BDBEnqueueRecord(final UUID queueid, final long messageNumber)
        {
            _queueId = queueid;
            _messageNumber = messageNumber;
        }

        @Override
        public long getMessageNumber()
        {
            return _messageNumber;
        }

        @Override
        public UUID getQueueId()
        {
            return _queueId;
        }

    }

    private class BDBMessageStoreReader implements MessageStoreReader
    {
        @Override
        public void visitMessages(final MessageHandler handler) throws StoreException
        {
            checkMessageStoreOpen();
            visitMessagesInternal(handler, getEnvironmentFacade());
        }

        @Override
        public StoredMessage<?> getMessage(final long messageId)
        {
            checkMessageStoreOpen();
            return getMessageInternal(messageId, getEnvironmentFacade());
        }

        @Override
        public void close()
        {

        }

        @Override
        public void visitMessageInstances(final TransactionLogResource queue, final MessageInstanceHandler handler) throws StoreException
        {
            checkMessageStoreOpen();

            final List<QueueEntryKey> entries = new ArrayList<>();
            try(Cursor cursor = getDeliveryDb().openCursor(null, null))
            {
                DatabaseEntry key = new DatabaseEntry();
                DatabaseEntry value = new DatabaseEntry();
                value.setPartial(0, 0, true);

                CachingUUIDFactory uuidFactory = new CachingUUIDFactory();
                QueueEntryBinding.objectToEntry(new QueueEntryKey(queue.getId(), 0L), key);

                if (cursor.getSearchKeyRange(key, value, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS)
                {
                    do
                    {
                        QueueEntryKey entry = QueueEntryBinding.entryToObject(uuidFactory, key);
                        if (entry.getQueueId().equals(queue.getId()))
                        {
                            entries.add(entry);
                        }
                        else
                        {
                            break;
                        }
                    }
                    while (cursor.getNext(key, value, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS);
                }
            }
            catch (RuntimeException e)
            {
                throw getEnvironmentFacade().handleDatabaseException("Cannot visit message instances", e);
            }

            for(QueueEntryKey entry : entries)
            {
                UUID queueId = entry.getQueueId();
                long messageId = entry.getMessageId();
                if (!handler.handle(new BDBEnqueueRecord(queueId, messageId)))
                {
                    break;
                }
            }

        }



        @Override
        public void visitMessageInstances(final MessageInstanceHandler handler) throws StoreException
        {
            checkMessageStoreOpen();

            List<QueueEntryKey> entries = new ArrayList<>();
            try(Cursor cursor = getDeliveryDb().openCursor(null, null))
            {
                DatabaseEntry key = new DatabaseEntry();
                CachingUUIDFactory uuidFactory = new CachingUUIDFactory();

                DatabaseEntry value = new DatabaseEntry();
                value.setPartial(0, 0, true);
                while (cursor.getNext(key, value, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS)
                {
                    QueueEntryKey entry = QueueEntryBinding.entryToObject(uuidFactory, key);
                    entries.add(entry);
                }
            }
            catch (RuntimeException e)
            {
                throw getEnvironmentFacade().handleDatabaseException("Cannot visit message instances", e);
            }

            for(QueueEntryKey entry : entries)
            {
                UUID queueId = entry.getQueueId();
                long messageId = entry.getMessageId();
                if (!handler.handle(new BDBEnqueueRecord(queueId, messageId)))
                {
                    break;
                }
            }

        }

        @Override
        public void visitDistributedTransactions(final DistributedTransactionHandler handler) throws StoreException
        {
            checkMessageStoreOpen();

            try(Cursor cursor = getXidDb().openCursor(null, null))
            {
                CachingUUIDFactory uuidFactory = new CachingUUIDFactory();
                DatabaseEntry key = new DatabaseEntry();
                XidBinding keyBinding = XidBinding.getInstance();
                DatabaseEntry value = new DatabaseEntry();

                while (cursor.getNext(key, value, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS)
                {
                    Xid xid = keyBinding.entryToObject(key);
                    PreparedTransaction preparedTransaction = PreparedTransactionBinding.entryToObject(uuidFactory, value);
                    if (!handler.handle(new BDBStoredXidRecord(xid.getFormat(), xid.getGlobalId(), xid.getBranchId()),
                                        preparedTransaction.getEnqueues(), preparedTransaction.getDequeues()))
                    {
                        break;
                    }
                }
            }
            catch (RuntimeException e)
            {
                throw getEnvironmentFacade().handleDatabaseException("Cannot recover distributed transactions", e);
            }
        }


    }
}
