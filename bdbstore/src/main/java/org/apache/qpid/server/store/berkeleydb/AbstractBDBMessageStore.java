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
import static org.apache.qpid.server.store.berkeleydb.BDBUtils.closeCursorSafely;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import com.google.common.util.concurrent.ListenableFuture;
import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Sequence;
import com.sleepycat.je.SequenceConfig;
import com.sleepycat.je.Transaction;
import org.slf4j.Logger;

import org.apache.qpid.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.EnqueueableMessage;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.store.Event;
import org.apache.qpid.server.store.EventListener;
import org.apache.qpid.server.store.EventManager;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.store.MessageHandle;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.server.store.Xid;
import org.apache.qpid.server.store.berkeleydb.entry.PreparedTransaction;
import org.apache.qpid.server.store.berkeleydb.entry.QueueEntryKey;
import org.apache.qpid.server.store.berkeleydb.tuple.ByteBufferBinding;
import org.apache.qpid.server.store.berkeleydb.tuple.MessageMetaDataBinding;
import org.apache.qpid.server.store.berkeleydb.tuple.PreparedTransactionBinding;
import org.apache.qpid.server.store.berkeleydb.tuple.QueueEntryBinding;
import org.apache.qpid.server.store.berkeleydb.tuple.XidBinding;
import org.apache.qpid.server.store.handler.DistributedTransactionHandler;
import org.apache.qpid.server.store.handler.MessageHandler;
import org.apache.qpid.server.store.handler.MessageInstanceHandler;


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
    private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.allocateDirect(0);

    private final EventManager _eventManager = new EventManager();

    private final DatabaseEntry MESSAGE_METADATA_SEQ_KEY = new DatabaseEntry("MESSAGE_METADATA_SEQ_KEY".getBytes(
            Charset.forName("UTF-8")));

    private final SequenceConfig MESSAGE_METADATA_SEQ_CONFIG = SequenceConfig.DEFAULT.
            setAllowCreate(true).
            setInitialValue(1).
            setWrap(true).
            setCacheSize(100000);

    private boolean _limitBusted;
    private long _totalStoreSize;
    private final Random _lockConflictRandom = new Random();

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

    @Override
    public <T extends StorableMessageMetaData> MessageHandle<T> addMessage(T metaData)
    {

        long newMessageId = getNextMessageId();

        return new StoredBDBMessage<T>(newMessageId, metaData);
    }

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
            getLogger().error("Unexpected BDB exception", e);

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


    /**
     * Fills the provided ByteBuffer with as much content for the specified message as possible, starting
     * from the specified offset in the message.
     *
     * @param messageId The message to get the data for.
     * @param offset    The offset of the data within the message.
     * @param dst       The destination of the content read back
     *
     * @return The number of bytes inserted into the destination
     *
     * @throws org.apache.qpid.server.store.StoreException If the operation fails for any reason, or if the specified message does not exist.
     */
    int getContent(long messageId, int offset, ByteBuffer dst) throws StoreException
    {
        DatabaseEntry contentKeyEntry = new DatabaseEntry();
        LongBinding.longToEntry(messageId, contentKeyEntry);
        DatabaseEntry value = new DatabaseEntry();
        ByteBufferBinding contentTupleBinding = ByteBufferBinding.getInstance();


        getLogger().debug("Message Id: {} Getting content body from offset: {}", messageId, offset);


        try
        {

            int written = 0;
            OperationStatus status = getMessageContentDb().get(null, contentKeyEntry, value, LockMode.READ_UNCOMMITTED);
            if (status == OperationStatus.SUCCESS)
            {
                QpidByteBuffer buffer = contentTupleBinding.entryToObject(value);
                int size = buffer.remaining();
                if (offset > size)
                {
                    throw new RuntimeException("Offset " + offset + " is greater than message size " + size
                                               + " for message id " + messageId + "!");

                }

                written = size - offset;
                if(written > dst.remaining())
                {
                    written = dst.remaining();
                }
                buffer = buffer.view(offset, written);
                buffer.get(dst);
            }
            return written;
        }
        catch (RuntimeException e)
        {
            throw getEnvironmentFacade().handleDatabaseException("Error getting AMQMessage with id "
                                                                 + messageId
                                                                 + " to database: "
                                                                 + e.getMessage(), e);
        }
    }

    Collection<QpidByteBuffer> getAllContent(long messageId) throws StoreException
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
                Collection<QpidByteBuffer> buffers = QpidByteBuffer.allocateDirectCollection(length);
                for(QpidByteBuffer buf : buffers)
                {
                    int bufSize = buf.remaining();
                    buf.put(data, offset, bufSize);
                    buf.flip();
                    offset+=bufSize;
                }
                return buffers;
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
        Cursor cursor = null;
        try
        {
            cursor = getMessageMetaDataDb().openCursor(null, null);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry value = new DatabaseEntry();
            MessageMetaDataBinding valueBinding = MessageMetaDataBinding.getInstance();

            int attempts = 0;
            boolean completed = false;
            do
            {
                try
                {
                    while (cursor.getNext(key, value, LockMode.RMW) == OperationStatus.SUCCESS)
                    {
                        long messageId = LongBinding.entryToLong(key);
                        StorableMessageMetaData metaData = valueBinding.entryToObject(value);
                        StoredBDBMessage message = new StoredBDBMessage(messageId, metaData, true);
                        if (!handler.handle(message))
                        {
                            break;
                        }
                    }
                    completed = true;
                }
                catch (LockConflictException e)
                {
                    sleepOrThrowOnLockConflict(attempts++, "Cannot visit messages", e);
                }
            }
            while (!completed);
        }
        catch (RuntimeException e)
        {
            throw environmentFacade.handleDatabaseException("Cannot visit messages", e);
        }
        finally
        {
            if (cursor != null)
            {
                try
                {
                    cursor.close();
                }
                catch(RuntimeException e)
                {
                    throw environmentFacade.handleDatabaseException("Cannot close cursor", e);
                }
            }
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
                StoredBDBMessage message = new StoredBDBMessage(messageId, metaData, true);
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
    private void addContent(final Transaction tx, long messageId,
                            Collection<QpidByteBuffer> contentBody) throws StoreException
    {
        DatabaseEntry key = new DatabaseEntry();
        LongBinding.longToEntry(messageId, key);
        DatabaseEntry value = new DatabaseEntry();

        int size = 0;

        for(QpidByteBuffer buf : contentBody)
        {
            size += buf.remaining();
        }
        byte[] data = new byte[size];
        ByteBuffer dst = ByteBuffer.wrap(data);
        for(QpidByteBuffer buf : contentBody)
        {
            buf.copyTo(dst);
        }
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
        QueueEntryBinding keyBinding = QueueEntryBinding.getInstance();
        QueueEntryKey dd = new QueueEntryKey(queue.getId(), messageId);
        keyBinding.objectToEntry(dd, key);
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
            getLogger().error("Failed to enqueue: {}", e.getMessage(), e);
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
        QueueEntryBinding keyBinding = QueueEntryBinding.getInstance();
        QueueEntryKey queueEntryKey = new QueueEntryKey(queueId, messageId);
        UUID id = queueId;
        keyBinding.objectToEntry(queueEntryKey, key);

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

            getLogger().error("Failed to dequeue message " + messageId + " in transaction " + tx, e);

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
        PreparedTransactionBinding valueBinding = new PreparedTransactionBinding();
        valueBinding.objectToEntry(preparedTransaction, value);
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
            getLogger().error("Failed to write xid: " + e.getMessage(), e);
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

            getLogger().error("Failed to remove xid in transaction " + txn, e);

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

    protected abstract void checkMessageStoreOpen();

    protected abstract ConfiguredObject<?> getParent();

    protected abstract EnvironmentFacade getEnvironmentFacade();

    protected abstract long getPersistentSizeLowThreshold();

    protected abstract long getPersistentSizeHighThreshold();

    protected abstract Logger getLogger();

    interface MessageDataRef<T extends StorableMessageMetaData>
    {
        T getMetaData();
        Collection<QpidByteBuffer> getData();
        void setData(Collection<QpidByteBuffer> data);
        boolean isHardRef();
    }

    private static final class MessageDataHardRef<T extends StorableMessageMetaData> implements MessageDataRef<T>
    {
        private final T _metaData;
        private volatile Collection<QpidByteBuffer> _data;

        private MessageDataHardRef(final T metaData)
        {
            _metaData = metaData;
        }

        @Override
        public T getMetaData()
        {
            return _metaData;
        }

        @Override
        public Collection<QpidByteBuffer> getData()
        {
            return _data;
        }

        @Override
        public void setData(final Collection<QpidByteBuffer> data)
        {
            _data = data;
        }

        @Override
        public boolean isHardRef()
        {
            return true;
        }
    }

    private static final class MessageDataSoftRef<T extends StorableMessageMetaData> implements MessageDataRef<T>
    {

        private T _metaData;
        private volatile Collection<QpidByteBuffer> _data;

        private MessageDataSoftRef(final T metaData, Collection<QpidByteBuffer> data)
        {
            _metaData = metaData;
            _data = data;
        }

        @Override
        public T getMetaData()
        {
            return _metaData;
        }

        @Override
        public Collection<QpidByteBuffer> getData()
        {
            return _data;
        }

        @Override
        public void setData(final Collection<QpidByteBuffer> data)
        {
            _data = data;
        }

        public void clear()
        {
            if(_metaData != null)
            {
                _metaData.clearEncodedForm();
                _metaData = null;
            }
            if(_data != null)
            {
                for(QpidByteBuffer buf : _data)
                {
                    buf.dispose();
                }
            }
            _data = null;
        }

        @Override
        public boolean isHardRef()
        {
            return false;
        }
    }

    final class StoredBDBMessage<T extends StorableMessageMetaData> implements StoredMessage<T>, MessageHandle<T>
    {

        private final long _messageId;

        private MessageDataRef<T> _messageDataRef;

        StoredBDBMessage(long messageId, T metaData)
        {
            this(messageId, metaData, false);
        }

        StoredBDBMessage(long messageId, T metaData, boolean isRecovered)
        {
            _messageId = messageId;

            if(!isRecovered)
            {
                _messageDataRef = new MessageDataHardRef<>(metaData);
            }
            else
            {
                _messageDataRef = new MessageDataSoftRef<>(metaData, null);
            }
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
                    _messageDataRef = new MessageDataSoftRef<>(metaData, _messageDataRef.getData());
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
            src = src.slice();
            Collection<QpidByteBuffer> data = _messageDataRef.getData();
            if(data == null)
            {
                _messageDataRef.setData(Collections.singleton(src));
            }
            else
            {
                List<QpidByteBuffer> newCollection = new ArrayList<>(data.size()+1);
                newCollection.addAll(data);
                newCollection.add(src);
                _messageDataRef.setData(Collections.unmodifiableCollection(newCollection));
            }

        }

        @Override
        public StoredMessage<T> allContentAdded()
        {
            return this;
        }

        /**
         * returns QBBs containing the content. The caller must not dispose of them because we keep a reference in _messageDataRef.
         */
        private Collection<QpidByteBuffer> getContentAsByteBuffer()
        {
            Collection<QpidByteBuffer> data = _messageDataRef == null ? Collections.<QpidByteBuffer>emptyList() : _messageDataRef.getData();
            if(data == null)
            {
                if(stored())
                {
                    checkMessageStoreOpen();
                    data = AbstractBDBMessageStore.this.getAllContent(_messageId);
                    _messageDataRef.setData(data);
                }
                else
                {
                    data = Collections.emptyList();
                }
            }
            return data;
        }


        @Override
        public synchronized Collection<QpidByteBuffer> getContent(int offset, int length)
        {
            Collection<QpidByteBuffer> bufs = getContentAsByteBuffer();
            Collection<QpidByteBuffer> content = new ArrayList<>(bufs.size());
            int pos = 0;
            for (QpidByteBuffer buf : bufs)
            {
                if(length > 0)
                {
                    int bufRemaining = buf.remaining();
                    if (pos + bufRemaining <= offset)
                    {
                        pos += bufRemaining;
                    }
                    else if (pos >= offset)
                    {
                        buf = buf.duplicate();
                        if (bufRemaining <= length)
                        {
                            length -= bufRemaining;
                        }
                        else
                        {
                            buf.limit(length);
                            length = 0;
                        }
                        content.add(buf);
                        pos += buf.remaining();

                    }
                    else
                    {
                        int offsetInBuf = offset - pos;
                        int limit = length < bufRemaining - offsetInBuf ? length : bufRemaining - offsetInBuf;
                        final QpidByteBuffer bufView = buf.view(offsetInBuf, limit);
                        content.add(bufView);
                        length -= limit;
                        pos+=limit+offsetInBuf;
                    }
                }

            }
            return content;
        }

        synchronized void store(Transaction txn)
        {
            if (!stored())
            {

                AbstractBDBMessageStore.this.storeMetaData(txn, _messageId, _messageDataRef.getMetaData());
                AbstractBDBMessageStore.this.addContent(txn, _messageId,
                                                        _messageDataRef.getData() == null
                                                                ? Collections.<QpidByteBuffer>emptySet()
                                                                : _messageDataRef.getData());


                MessageDataRef<T> hardRef = _messageDataRef;
                MessageDataSoftRef<T> messageDataSoftRef;

                messageDataSoftRef = new MessageDataSoftRef<>(hardRef.getMetaData(), hardRef.getData());

                _messageDataRef = messageDataSoftRef;


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
            Collection<QpidByteBuffer> data = _messageDataRef.getData();

            final T metaData = getMetaData();
            int delta = metaData.getContentSize();
            if(stored())
            {
                removeMessage(_messageId, false);
                storedSizeChangeOccurred(-delta);
            }
            if(data != null)
            {
                _messageDataRef.setData(null);
                for(QpidByteBuffer buf : data)
                {
                    buf.dispose();
                }
            }
            metaData.dispose();
            _messageDataRef = null;
        }

        @Override
        public synchronized boolean isInMemory()
        {
            return _messageDataRef != null && (_messageDataRef.isHardRef() || _messageDataRef.getData() != null);
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
                ((MessageDataSoftRef)_messageDataRef).clear();
            }
            return true;
        }

        @Override
        public String toString()
        {
            return this.getClass() + "[messageId=" + _messageId + "]";
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
                final long contentSize = storedMessage.getMetaData().getContentSize();
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

        public long getMessageNumber()
        {
            return _messageNumber;
        }

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

            Cursor cursor = null;
            List<QueueEntryKey> entries = new ArrayList<QueueEntryKey>();
            try
            {
                cursor = getDeliveryDb().openCursor(null, null);
                DatabaseEntry key = new DatabaseEntry();
                DatabaseEntry value = new DatabaseEntry();
                value.setPartial(0, 0, true);

                QueueEntryBinding keyBinding = QueueEntryBinding.getInstance();
                keyBinding.objectToEntry(new QueueEntryKey(queue.getId(),0l), key);

                boolean searchCompletedSuccessfully = false;
                int attempts = 0;
                boolean completed = false;
                do
                {
                    try
                    {
                        if (!searchCompletedSuccessfully && (searchCompletedSuccessfully = cursor.getSearchKeyRange(key,value, LockMode.DEFAULT) == OperationStatus.SUCCESS))
                        {
                            QueueEntryKey entry = keyBinding.entryToObject(key);
                            if(entry.getQueueId().equals(queue.getId()))
                            {
                                entries.add(entry);
                            }
                        }

                        if (searchCompletedSuccessfully)
                        {
                            while(cursor.getNext(key, value, LockMode.DEFAULT) == OperationStatus.SUCCESS)
                            {
                                QueueEntryKey entry = keyBinding.entryToObject(key);
                                if(entry.getQueueId().equals(queue.getId()))
                                {
                                    entries.add(entry);
                                }
                                else
                                {
                                    break;
                                }
                            }
                        }
                        completed = true;
                    }
                    catch (LockConflictException e)
                    {
                        sleepOrThrowOnLockConflict(attempts++, "Cannot visit messages", e);
                    }
                }
                while (!completed);
            }
            catch (RuntimeException e)
            {
                throw getEnvironmentFacade().handleDatabaseException("Cannot visit message instances", e);
            }
            finally
            {
                closeCursorSafely(cursor, getEnvironmentFacade());
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

            Cursor cursor = null;
            List<QueueEntryKey> entries = new ArrayList<QueueEntryKey>();
            try
            {
                cursor = getDeliveryDb().openCursor(null, null);
                DatabaseEntry key = new DatabaseEntry();
                QueueEntryBinding keyBinding = QueueEntryBinding.getInstance();

                DatabaseEntry value = new DatabaseEntry();
                value.setPartial(0, 0, true);
                while (cursor.getNext(key, value, LockMode.DEFAULT) == OperationStatus.SUCCESS)
                {
                    QueueEntryKey entry = keyBinding.entryToObject(key);
                    entries.add(entry);
                }
            }
            catch (RuntimeException e)
            {
                throw getEnvironmentFacade().handleDatabaseException("Cannot visit message instances", e);
            }
            finally
            {
                closeCursorSafely(cursor, getEnvironmentFacade());
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

            Cursor cursor = null;
            try
            {
                cursor = getXidDb().openCursor(null, null);
                DatabaseEntry key = new DatabaseEntry();
                XidBinding keyBinding = XidBinding.getInstance();
                PreparedTransactionBinding valueBinding = new PreparedTransactionBinding();
                DatabaseEntry value = new DatabaseEntry();

                while (cursor.getNext(key, value, LockMode.RMW) == OperationStatus.SUCCESS)
                {
                    Xid xid = keyBinding.entryToObject(key);
                    PreparedTransaction preparedTransaction = valueBinding.entryToObject(value);
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
            finally
            {
                closeCursorSafely(cursor, getEnvironmentFacade());
            }
        }


    }
}
