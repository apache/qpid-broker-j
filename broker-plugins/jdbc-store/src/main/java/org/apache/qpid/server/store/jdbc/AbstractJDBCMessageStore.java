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
package org.apache.qpid.server.store.jdbc;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.EnqueueableMessage;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.plugin.MessageMetaDataType;
import org.apache.qpid.server.store.Event;
import org.apache.qpid.server.store.EventListener;
import org.apache.qpid.server.store.EventManager;
import org.apache.qpid.server.store.MessageDurability;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.store.MessageHandle;
import org.apache.qpid.server.store.MessageMetaDataTypeRegistry;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.store.Transaction;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.server.store.handler.DistributedTransactionHandler;
import org.apache.qpid.server.store.handler.MessageHandler;
import org.apache.qpid.server.store.handler.MessageInstanceHandler;
import org.apache.qpid.server.txn.Xid;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.util.CachingUUIDFactory;

public abstract class AbstractJDBCMessageStore implements MessageStore
{
    private static final String DB_VERSION_TABLE_NAME_SUFFIX = "QPID_DB_VERSION";
    private static final String QUEUE_ENTRY_TABLE_NAME_SUFFIX = "QPID_QUEUE_ENTRIES";
    private static final String META_DATA_TABLE_NAME_SUFFIX = "QPID_MESSAGE_METADATA";
    private static final String MESSAGE_CONTENT_TABLE_NAME_SUFFIX = "QPID_MESSAGE_CONTENT";
    private static final String XID_TABLE_NAME_SUFFIX = "QPID_XIDS";
    private static final String XID_ACTIONS_TABLE_NAME_SUFFIX = "QPID_XID_ACTIONS";

    private static final int IN_CLAUSE_MAX_SIZE_DEFAULT = 1000;
    static final String IN_CLAUSE_MAX_SIZE = "qpid.jdbcstore.inClauseMaxSize";

    private static final int EXECUTOR_THREADS_DEFAULT = Runtime.getRuntime().availableProcessors();
    private static final String EXECUTOR_THREADS = "qpid.jdbcstore.executorThreads";
    private static final String EXECUTOR_SHUTDOWN_TIMEOUT = "qpid.jdbcstore.executorShutdownTimeoutInSeconds";
    private static final int EXECUTOR_SHUTDOWN_TIMEOUT_DEFAULT = 5;

    private static final int DB_VERSION = 8;

    private final AtomicLong _messageId = new AtomicLong(0);

    private static final List<Long> EMPTY_LIST = Collections.emptyList();
    private final AtomicReference<List<Long>> _messagesToDelete = new AtomicReference<>(EMPTY_LIST);
    private final AtomicBoolean _messageRemovalScheduled = new AtomicBoolean();


    protected final EventManager _eventManager = new EventManager();
    private ConfiguredObject<?> _parent;
    private String _tablePrefix = "";
    private final AtomicLong _inMemorySize = new AtomicLong();
    private final AtomicLong _bytesEvacuatedFromMemory = new AtomicLong();
    private final Set<StoredJDBCMessage<?>> _messages = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Set<MessageDeleteListener> _messageDeleteListeners = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Set<Action<Connection>> _deleteActions = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Thread.UncaughtExceptionHandler _uncaughtExceptionHandler;

    protected abstract boolean isMessageStoreOpen();

    protected abstract void checkMessageStoreOpen();
    private ScheduledThreadPoolExecutor _executor;
    private volatile int _inClauseMaxSize;
    private volatile int _executorShutdownTimeOut;

    public AbstractJDBCMessageStore()
    {
        _uncaughtExceptionHandler = Thread.getDefaultUncaughtExceptionHandler();
    }

    protected void setMaximumMessageId()
    {
        try (Connection conn = newAutoCommitConnection())
        {
            setMaxMessageId(conn, "SELECT max(message_id) FROM " + getMessageContentTableName(), 1);
            setMaxMessageId(conn, "SELECT max(message_id) FROM " + getMetaDataTableName(), 1);
            setMaxMessageId(conn, "SELECT queue_id, max(message_id) FROM " + getQueueEntryTableName()
                                  + " GROUP BY queue_id ", 2);
        }
        catch (SQLException e)
        {
            throw new StoreException("Failed to determine maximum ids", e);
        }
    }

    private void setMaxMessageId(final Connection conn, final String query, int col) throws SQLException
    {
        try (PreparedStatement statement = conn.prepareStatement(query))
        {
            try (ResultSet rs = statement.executeQuery())
            {
                while (rs.next())
                {
                    long maxMessageId = rs.getLong(col);
                    if (_messageId.get() < maxMessageId)
                    {
                        _messageId.set(maxMessageId);
                    }
                }
            }
        }
    }

    protected void upgrade(ConfiguredObject<?> parent) throws StoreException
    {
        try(Connection conn = newAutoCommitConnection())
        {
            if (tableExists(getDbVersionTableName(), conn))
            {
                upgradeIfNecessary(parent);
            }
        }
        catch (SQLException e)
        {
            throw new StoreException("Failed to upgrade database", e);
        }
    }

    private void upgradeIfNecessary(ConfiguredObject<?> parent) throws SQLException
    {
        try (Connection conn = newAutoCommitConnection())
        {

            try (PreparedStatement statement = conn.prepareStatement("SELECT version FROM " + getDbVersionTableName()))
            {
                try (ResultSet rs = statement.executeQuery())
                {
                    if (!rs.next())
                    {
                        throw new StoreException(getDbVersionTableName() + " does not contain the database version");
                    }
                    int version = rs.getInt(1);
                    switch (version)
                    {
                        case 6:
                            upgradeFromV6();
                        case 7:
                            upgradeFromV7();
                        case DB_VERSION:
                            return;
                        default:
                            throw new StoreException("Unknown database version: " + version);
                    }
                }
            }
        }

    }

    private void upgradeFromV7() throws SQLException
    {
        updateDbVersion(8);
    }

    private void upgradeFromV6() throws SQLException
    {
        updateDbVersion(7);
    }

    private void updateDbVersion(int newVersion) throws SQLException
    {
        try (Connection conn = newAutoCommitConnection())
        {
            try (PreparedStatement statement = conn.prepareStatement("UPDATE " + getDbVersionTableName()
                                                                     + " SET version = ?"))
            {
                statement.setInt(1, newVersion);
                statement.execute();
            }
        }
    }

    protected void initMessageStore(final ConfiguredObject<?> parent)
    {
        _parent = parent;

        int corePoolSize = getContextValue(Integer.class, EXECUTOR_THREADS, EXECUTOR_THREADS_DEFAULT);

        _executorShutdownTimeOut = getContextValue(Integer.class, EXECUTOR_SHUTDOWN_TIMEOUT, EXECUTOR_SHUTDOWN_TIMEOUT_DEFAULT);

        _executor = new ScheduledThreadPoolExecutor(corePoolSize, new ThreadFactory()
        {
            private final AtomicInteger _count = new AtomicInteger();
            @Override
            public Thread newThread(final Runnable r)
            {
                final Thread thread = Executors.defaultThreadFactory().newThread(r);
                thread.setName(parent.getName() + "-store-"+_count.incrementAndGet());
                return thread;
            }
        });
        _executor.prestartAllCoreThreads();

        _inClauseMaxSize = getContextValue(Integer.class, IN_CLAUSE_MAX_SIZE, IN_CLAUSE_MAX_SIZE_DEFAULT);
    }

    @Override
    public void closeMessageStore()
    {
        for (StoredJDBCMessage<?> message : _messages)
        {
            message.clear(true);
        }
        _messages.clear();
        _inMemorySize.set(0);
        _bytesEvacuatedFromMemory.set(0);
        if(_executor != null)
        {
            _executor.shutdown();
            if (_executorShutdownTimeOut > 0)
            {
                try
                {
                    if (!_executor.awaitTermination(_executorShutdownTimeOut, TimeUnit.SECONDS))
                    {
                        _executor.shutdownNow();
                    }
                }
                catch (InterruptedException e)
                {
                    getLogger().warn("Interrupted during store executor shutdown:", e);
                    Thread.currentThread().interrupt();
                }
            }
        }

    }

    protected abstract Logger getLogger();

    protected abstract String getSqlBlobType();

    protected abstract String getSqlBlobStorage(String columnName);

    protected abstract String getSqlVarBinaryType(int size);

    protected abstract String getSqlBigIntType();

    protected void createOrOpenMessageStoreDatabase() throws StoreException
    {
        try(Connection conn =  newAutoCommitConnection())
        {
            createVersionTable(conn);
            createQueueEntryTable(conn);
            createMetaDataTable(conn);
            createMessageContentTable(conn);
            createXidTable(conn);
            createXidActionTable(conn);
        }
        catch (SQLException e)
        {
            throw new StoreException("Failed to create message store tables", e);
        }
    }

    private void createVersionTable(final Connection conn) throws SQLException
    {
        if(!tableExists(getDbVersionTableName(), conn))
        {
            try (Statement stmt = conn.createStatement())
            {
                stmt.execute("CREATE TABLE " + getDbVersionTableName() + " ( version int not null )");
            }

            try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO " + getDbVersionTableName()
                                                                 + " ( version ) VALUES ( ? )"))
            {
                pstmt.setInt(1, DB_VERSION);
                pstmt.execute();
            }
        }
    }

    private void createQueueEntryTable(final Connection conn) throws SQLException
    {
        if(!tableExists(getQueueEntryTableName(), conn))
        {
            try (Statement stmt = conn.createStatement())
            {
                stmt.execute("CREATE TABLE " + getQueueEntryTableName()
                             + " ( queue_id varchar(36) not null, message_id "
                             + getSqlBigIntType() + " not null, PRIMARY KEY (queue_id, message_id) )");
            }
        }

    }

    private void createMetaDataTable(final Connection conn) throws SQLException
    {
        if(!tableExists(getMetaDataTableName(), conn))
        {
            try (Statement stmt = conn.createStatement())
            {
                stmt.execute("CREATE TABLE "
                             + getMetaDataTableName()
                             + " ( message_id "
                             + getSqlBigIntType()
                             + " not null, meta_data "
                             + getSqlBlobType()
                             + ", PRIMARY KEY ( message_id ) ) "
                             + getSqlBlobStorage("meta_data"));
            }
        }

    }

    private void createMessageContentTable(final Connection conn) throws SQLException
    {
        if(!tableExists(getMessageContentTableName(), conn))
        {
            try (Statement stmt = conn.createStatement())
            {
                stmt.execute("CREATE TABLE "
                             + getMessageContentTableName()
                             + " ( message_id "
                             + getSqlBigIntType()
                             + " not null, content "
                             + getSqlBlobType()
                             + ", PRIMARY KEY (message_id) ) "
                             + getSqlBlobStorage("content"));
            }
        }

    }

    private void createXidTable(final Connection conn) throws SQLException
    {
        if(!tableExists(getXidTableName(), conn))
        {
            try (Statement stmt = conn.createStatement())
            {
                stmt.execute("CREATE TABLE "
                             + getXidTableName()
                             + " ( format " + getSqlBigIntType() + " not null,"
                             + " global_id "
                             + getSqlVarBinaryType(64)
                             + ", branch_id "
                             + getSqlVarBinaryType(64)
                             + " ,  PRIMARY KEY ( format, "
                             +
                             "global_id, branch_id ))");
            }
        }
    }

    private void createXidActionTable(final Connection conn) throws SQLException
    {
        if(!tableExists(getXidActionsTableName(), conn))
        {
            try (Statement stmt = conn.createStatement())
            {
                stmt.execute("CREATE TABLE "
                             + getXidActionsTableName()
                             + " ( format "
                             + getSqlBigIntType()
                             + " not null,"
                             + " global_id "
                             + getSqlVarBinaryType(64)
                             + " not null, branch_id "
                             + getSqlVarBinaryType(
                        64)
                             + " not null, "
                             +
                             "action_type char not null, queue_id varchar(36) not null, message_id "
                             + getSqlBigIntType()
                             + " not null"
                             +
                             ",  PRIMARY KEY ( "
                             +
                             "format, global_id, branch_id, action_type, queue_id, message_id))");
            }
        }
    }

    protected boolean tableExists(final String tableName, final Connection conn) throws SQLException
    {
        return JdbcUtils.tableExists(tableName, conn);
    }

    @Override
    public <T extends StorableMessageMetaData> MessageHandle<T> addMessage(T metaData)
    {
        checkMessageStoreOpen();

        return createStoredJDBCMessage(getNextMessageId(), metaData, false);
    }

    public <T extends StorableMessageMetaData> StoredJDBCMessage<T> createStoredJDBCMessage(final long newMessageId,
                                                                                          final T metaData,
                                                                                          final boolean recovered)
    {
        final StoredJDBCMessage<T> message = new StoredJDBCMessage<>(newMessageId, metaData, recovered);
        _messages.add(message);
        return message;
    }

    @Override
    public long getNextMessageId()
    {
        return _messageId.incrementAndGet();
    }

    private void removeMessageAsync(long messageId)
    {
        List<Long> orig;
        List<Long> updated;
        do
        {
            orig = _messagesToDelete.get();
            updated = new ArrayList<>(orig.size()+1);
            updated.addAll(orig);
            updated.add(messageId);
            updated = Collections.unmodifiableList(updated);
        } while (! _messagesToDelete.compareAndSet(orig, updated));
        scheduleMessageRemoval();
    }

    private void scheduleMessageRemoval()
    {
        if(_messageRemovalScheduled.compareAndSet(false, true))
        {
            try
            {
                _executor.submit(this::removeScheduledMessages);
            }
            catch (RejectedExecutionException e)
            {
                _messageRemovalScheduled.set(false);
                throw new IllegalStateException("Cannot schedule removal of messages", e);
            }
        }
    }

    private void removeScheduledMessages()
    {
        try
        {
            removeScheduledMessagesAndRescheduleIfRequired();
        }
        catch (RuntimeException e)
        {
            handleExceptionOnScheduledMessageRemoval(e);
        }
    }

    private void removeScheduledMessagesAndRescheduleIfRequired()
    {
        List<Long> messageIds;
        try
        {
            do
            {
                messageIds = _messagesToDelete.getAndSet(EMPTY_LIST);
                removeMessages(messageIds);
            } while (!messageIds.isEmpty());
        }
        finally
        {
            _messageRemovalScheduled.set(false);
        }
        if (!_messagesToDelete.get().isEmpty() && isMessageStoreOpen())
        {
            scheduleMessageRemoval();
        }
    }

    private void handleExceptionOnScheduledMessageRemoval(final RuntimeException e)
    {
        if (isMessageStoreOpen())
        {
            if (_uncaughtExceptionHandler == null)
            {
                getLogger().error("Unexpected exception on asynchronous message removal", e);
            }
            else
            {
                _uncaughtExceptionHandler.uncaughtException(Thread.currentThread(), e);
            }
        }
        else
        {
            getLogger().warn("Ignoring unexpected exception on asynchronous message removal as store is not open", e);
        }
    }

    boolean isMessageRemovalScheduled()
    {
        return _messageRemovalScheduled.get();
    }

    void removeMessages(List<Long> messageIds)
    {
        if (messageIds != null && !messageIds.isEmpty())
        {
            try (Connection conn = newConnection())
            {
                try
                {
                    for (List<Long> boundMessageIds : Lists.partition(messageIds, _inClauseMaxSize))
                    {
                        removeMessagesFromDatabase(conn, boundMessageIds);
                    }
                }
                catch (SQLException e)
                {
                    try
                    {
                        conn.rollback();
                    }
                    catch (SQLException t)
                    {
                        // ignore - we are re-throwing underlying exception
                    }

                    throw e;
                }
            }
            catch (SQLException e)
            {
                throw new StoreException("Error removing messages with ids "
                                         + messageIds
                                         + " from database: "
                                         + e.getMessage(), e);
            }
        }
    }

    void removeMessagesFromDatabase(Connection conn, List<Long> messageIds) throws SQLException
    {
        String inpart = messageIds.stream().map(Object::toString).collect(Collectors.joining(", ", "(", ")"));

        try (Statement stmt = conn.createStatement())
        {
            int results = stmt.executeUpdate("DELETE FROM " + getMetaDataTableName() + " WHERE message_id IN " + inpart);
            stmt.close();

            if (results != messageIds.size())
            {
                getLogger().debug(
                        "Some message ids in {} not found (attempt to remove failed - probably application initiated rollback)",
                        messageIds);
            }
            getLogger().debug("Deleted metadata for messages {}", messageIds);
        }

        try (Statement stmt = conn.createStatement())
        {
            stmt.executeUpdate("DELETE FROM " + getMessageContentTableName() + " WHERE message_id IN " + inpart);
            getLogger().debug("Deleted content for messages {}", messageIds);
        }
        conn.commit();
    }

    /**
     * Convenience method to create a new Connection configured for TRANSACTION_READ_COMMITED
     * isolation and with auto-commit transactions enabled.
     */
    protected Connection newAutoCommitConnection() throws SQLException
    {
        final Connection connection = newConnection();
        try
        {
            connection.setAutoCommit(true);
        }
        catch (SQLException sqlEx)
        {

            try
            {
                connection.close();
            }
            finally
            {
                throw sqlEx;
            }
        }

        return connection;
    }

    /**
     * Convenience method to create a new Connection configured for TRANSACTION_READ_COMMITED
     * isolation and with auto-commit transactions disabled.
     */
    protected Connection newConnection() throws SQLException
    {
        final Connection connection = getConnection();
        try
        {
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        }
        catch (SQLException sqlEx)
        {
            try
            {
                connection.close();
            }
            finally
            {
                throw sqlEx;
            }
        }
        return connection;
    }

    public abstract Connection getConnection() throws SQLException;

    @Override
    public Transaction newTransaction()
    {
        checkMessageStoreOpen();

        return new JDBCTransaction();
    }

    private void enqueueMessages(ConnectionWrapper connWrapper, Map<Long, List<TransactionLogResource>> queuesPerMessage) throws StoreException
    {
        if (queuesPerMessage.isEmpty())
        {
            return;
        }
        Connection conn = connWrapper.getConnection();
        String sql = String.format("INSERT INTO %s (queue_id, message_id) values (?,?)", getQueueEntryTableName());

        try (PreparedStatement stmt = conn.prepareStatement(sql))
        {
            for(Long messageId : queuesPerMessage.keySet())
            {
                for(TransactionLogResource queue : queuesPerMessage.get(messageId))
                {
                    if (getLogger().isDebugEnabled())
                    {
                        getLogger().debug("Enqueuing message {} on queue {} with id {} [Connection {}]",
                                messageId, queue.getName(), queue.getId(), conn);
                    }
                    stmt.setString(1, queue.getId().toString());
                    stmt.setLong(2, messageId);
                    stmt.addBatch();
                }
            }
            stmt.executeBatch();
        }
        catch (SQLException e)
        {
            getLogger().error("Failed to enqueue messages", e);
            throw new StoreException("Error writing enqueued messages to database", e);
        }
    }

    private void dequeueMessage(ConnectionWrapper connWrapper, final UUID queueId,
                                Long messageId) throws StoreException
    {

        Connection conn = connWrapper.getConnection();

        try
        {
            try (PreparedStatement stmt = conn.prepareStatement("DELETE FROM " + getQueueEntryTableName()
                                                                + " WHERE queue_id = ? AND message_id =?"))
            {
                stmt.setString(1, queueId.toString());
                stmt.setLong(2, messageId);
                int results = stmt.executeUpdate();


                if (results != 1)
                {
                    throw new StoreException("Unable to find message with id " + messageId
                                             + " on queue with id " + queueId);
                }

                getLogger().debug("Dequeuing message {} on queue with id {}", messageId, queueId);

            }

        }
        catch (SQLException e)
        {
            getLogger().error("Failed to dequeue message {}", messageId, e);
            throw new StoreException("Error deleting enqueued message with id " + messageId + " for queue with id "
                                     + queueId + " from database", e);
        }

    }

    private void removeXid(ConnectionWrapper connWrapper, long format, byte[] globalId, byte[] branchId)
            throws StoreException
    {
        Connection conn = connWrapper.getConnection();

        try
        {
            try(PreparedStatement stmt = conn.prepareStatement("DELETE FROM " + getXidTableName()
                                                                + " WHERE format = ? and global_id = ? and branch_id = ?"))
            {
                stmt.setLong(1,format);
                stmt.setBytes(2,globalId);
                stmt.setBytes(3,branchId);
                int results = stmt.executeUpdate();



                if(results != 1)
                {
                    throw new StoreException("Unable to find message with xid");
                }
            }

            try(PreparedStatement stmt = conn.prepareStatement("DELETE FROM " + getXidActionsTableName()
                                                               + " WHERE format = ? and global_id = ? and branch_id = ?"))
            {
                stmt.setLong(1,format);
                stmt.setBytes(2,globalId);
                stmt.setBytes(3,branchId);
                int results = stmt.executeUpdate();

            }
        }
        catch (SQLException e)
        {
            getLogger().error("Failed to remove xid", e);
            throw new StoreException("Error deleting enqueued message with xid", e);
        }

    }

    private List<Runnable> recordXid(ConnectionWrapper connWrapper, long format, byte[] globalId, byte[] branchId,
                                     Transaction.EnqueueRecord[] enqueues, Transaction.DequeueRecord[] dequeues) throws StoreException
    {
        Connection conn = connWrapper.getConnection();


        try
        {

            try(PreparedStatement stmt = conn.prepareStatement("INSERT INTO " + getXidTableName()
                                                               + " ( format, global_id, branch_id ) values (?, ?, ?)"))
            {
                stmt.setLong(1,format);
                stmt.setBytes(2, globalId);
                stmt.setBytes(3, branchId);
                stmt.executeUpdate();
            }

            for(Transaction.EnqueueRecord enqueue : enqueues)
            {
                StoredMessage storedMessage = enqueue.getMessage().getStoredMessage();
                if(storedMessage instanceof StoredJDBCMessage)
                {
                    ((StoredJDBCMessage) storedMessage).store(conn);
                }
            }

            try(PreparedStatement stmt = conn.prepareStatement("INSERT INTO " + getXidActionsTableName()
                                                               + " ( format, global_id, branch_id, action_type, " +
                                                               "queue_id, message_id ) values (?,?,?,?,?,?) "))
            {
                stmt.setLong(1,format);
                stmt.setBytes(2, globalId);
                stmt.setBytes(3, branchId);

                if(enqueues != null)
                {
                    stmt.setString(4, "E");
                    for(Transaction.EnqueueRecord record : enqueues)
                    {
                        stmt.setString(5, record.getResource().getId().toString());
                        stmt.setLong(6, record.getMessage().getMessageNumber());
                        stmt.executeUpdate();
                    }
                }

                if(dequeues != null)
                {
                    stmt.setString(4, "D");
                    for(Transaction.DequeueRecord record : dequeues)
                    {
                        stmt.setString(5, record.getEnqueueRecord().getQueueId().toString());
                        stmt.setLong(6, record.getEnqueueRecord().getMessageNumber());
                        stmt.executeUpdate();
                    }
                }

            }
            return Collections.emptyList();
        }
        catch (SQLException e)
        {
            getLogger().error("Failed to record xid", e);
            throw new StoreException("Error writing xid ", e);
        }

    }

    protected void setTablePrefix(final String tablePrefix)
    {
        _tablePrefix = tablePrefix == null ? "" : tablePrefix;
    }

    private String getDbVersionTableName()
    {
        return _tablePrefix + DB_VERSION_TABLE_NAME_SUFFIX;
    }

    private String getQueueEntryTableName()
    {
        return _tablePrefix + QUEUE_ENTRY_TABLE_NAME_SUFFIX;
    }

    private String getMetaDataTableName()
    {
        return _tablePrefix + META_DATA_TABLE_NAME_SUFFIX;
    }

    private String getMessageContentTableName()
    {
        return _tablePrefix + MESSAGE_CONTENT_TABLE_NAME_SUFFIX;
    }

    private String getXidTableName()
    {
        return _tablePrefix + XID_TABLE_NAME_SUFFIX;
    }

    private String getXidActionsTableName()
    {
        return _tablePrefix + XID_ACTIONS_TABLE_NAME_SUFFIX;
    }

    public void addDeleteAction(final Action<Connection> action)
    {
        _deleteActions.add(action);
    }

    public void removeDeleteAction(final Action<Connection> action)
    {
        _deleteActions.remove(action);
    }

    private static final class ConnectionWrapper
    {
        private final Connection _connection;

        public ConnectionWrapper(Connection conn)
        {
            _connection = conn;
        }

        public Connection getConnection()
        {
            return _connection;
        }
    }


    private void commitTran(ConnectionWrapper connWrapper) throws StoreException
    {
        try
        {
            Connection conn = connWrapper.getConnection();
            conn.commit();

            getLogger().debug("commit tran completed");

            conn.close();
        }
        catch (SQLException e)
        {
            throw new StoreException("Error commit tx", e);
        }
    }

    private <X> ListenableFuture<X> commitTranAsync(final ConnectionWrapper connWrapper, final X val) throws StoreException
    {
        final SettableFuture<X> future = SettableFuture.create();
        _executor.submit(new Runnable()
                        {
                            @Override
                            public void run()
                            {
                                try
                                {
                                    commitTran(connWrapper);
                                    future.set(val);
                                }
                                catch (RuntimeException e)
                                {
                                    future.setException(e);
                                }
                            }
                        });
        return future;
    }

    private void abortTran(ConnectionWrapper connWrapper) throws StoreException
    {
        if (connWrapper == null)
        {
            throw new StoreException("Fatal internal error: transactional context is empty at abortTran");
        }

        getLogger().debug("abort tran called: {}", connWrapper.getConnection());

        try
        {
            Connection conn = connWrapper.getConnection();
            conn.rollback();
            conn.close();
        }
        catch (SQLException e)
        {
            throw new StoreException("Error aborting transaction: " + e.getMessage(), e);
        }

    }

    private void storeMetaData(Connection conn, long messageId, StorableMessageMetaData metaData)
            throws SQLException
    {
        getLogger().debug("Adding metadata for message {}", messageId);

        try(PreparedStatement stmt = conn.prepareStatement("INSERT INTO " + getMetaDataTableName()
                                                           + "( message_id , meta_data ) values (?, ?)"))
        {
            stmt.setLong(1, messageId);

            final int bodySize = 1 + metaData.getStorableSize();
            byte[] underlying = new byte[bodySize];
            underlying[0] = (byte) metaData.getType().ordinal();
            try (QpidByteBuffer buf = QpidByteBuffer.wrap(underlying))
            {
                buf.position(1);
                try (QpidByteBuffer bufSlice = buf.slice())
                {
                    metaData.writeToBuffer(buf);
                }
            }
            try(ByteArrayInputStream bis = new ByteArrayInputStream(underlying))
            {
                stmt.setBinaryStream(2, bis, underlying.length);
                int result = stmt.executeUpdate();

                if (result == 0)
                {
                    throw new StoreException("Unable to add meta data for message " + messageId);
                }
            }
            catch (IOException e)
            {
                throw new SQLException("Failed to close ByteArrayInputStream", e);
            }
        }

    }


    private static class RecordImpl implements Transaction.EnqueueRecord, Transaction.DequeueRecord, TransactionLogResource, EnqueueableMessage
    {

        private final JDBCEnqueueRecord _record;
        private long _messageNumber;
        private UUID _queueId;

        public RecordImpl(UUID queueId, long messageNumber)
        {
            _messageNumber = messageNumber;
            _queueId = queueId;
            _record = new JDBCEnqueueRecord(queueId, messageNumber);
        }

        @Override
        public MessageEnqueueRecord getEnqueueRecord()
        {
            return _record;
        }

        @Override
        public TransactionLogResource getResource()
        {
            return this;
        }

        @Override
        public EnqueueableMessage getMessage()
        {
            return this;
        }

        @Override
        public long getMessageNumber()
        {
            return _messageNumber;
        }

        @Override
        public boolean isPersistent()
        {
            return true;
        }

        @Override
        public StoredMessage getStoredMessage()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getName()
        {
            return _queueId.toString();
        }

        @Override
        public UUID getId()
        {
            return _queueId;
        }

        @Override
        public MessageDurability getMessageDurability()
        {
            return MessageDurability.DEFAULT;
        }
    }

    private StorableMessageMetaData getMetaData(long messageId) throws SQLException
    {

        try (Connection conn = newAutoCommitConnection())
        {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT meta_data FROM " + getMetaDataTableName()
                                                                + " WHERE message_id = ?"))
            {
                stmt.setLong(1, messageId);
                try (ResultSet rs = stmt.executeQuery())
                {

                    if (rs.next())
                    {
                        try (InputStream blobAsInputStream = getBlobAsInputStream(rs, 1))
                        {
                            return getStorableMessageMetaData(messageId, blobAsInputStream);
                        }
                        catch (IOException e)
                        {
                            throw new StoreException("Error reading meta data from the store for message with id " + messageId, e);
                        }
                    }
                    else
                    {
                        throw new StoreException("Meta data not found for message with id " + messageId);
                    }
                }
            }
        }
    }

    private StorableMessageMetaData getStorableMessageMetaData(final long messageId, final InputStream stream)
            throws SQLException
    {
        try
        {
            int typeOrdinal = stream.read() & 0xff;
            MessageMetaDataType type = MessageMetaDataTypeRegistry.fromOrdinal(typeOrdinal);

            try (QpidByteBuffer buf = QpidByteBuffer.asQpidByteBuffer(stream))
            {
                return type.createMetaData(buf);
            }
        }
        catch (IOException | RuntimeException e)
        {
            throw new StoreException("Failed to stream metadata for message with id " + messageId, e);
        }
    }

    protected abstract InputStream getBlobAsInputStream(ResultSet rs, int col) throws SQLException;

    private void addContent(final Connection conn, long messageId,
                            QpidByteBuffer contentBody)
    {
        getLogger().debug("Adding content for message {}", messageId);

        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO " + getMessageContentTableName()
                                                            + "( message_id, content ) values (?, ?)");
             QpidByteBuffer bodyDuplicate = contentBody.duplicate();
             InputStream inputStream = bodyDuplicate.asInputStream())
        {
            stmt.setLong(1, messageId);
            stmt.setBinaryStream(2, inputStream, contentBody.remaining());
            stmt.executeUpdate();
        }
        catch (SQLException | IOException e)
        {
            JdbcUtils.closeConnection(conn, getLogger());
            throw new StoreException("Error adding content for message " + messageId + ": " + e.getMessage(), e);
        }
    }

    QpidByteBuffer getAllContent(long messageId) throws StoreException
    {
        getLogger().debug("Message Id: {} Getting content body", messageId);

        try(Connection conn = newAutoCommitConnection();
            PreparedStatement stmt = conn.prepareStatement("SELECT content FROM " + getMessageContentTableName()
        + " WHERE message_id = ?"))
        {
            stmt.setLong(1,messageId);
            ResultSet rs = stmt.executeQuery();

            if (rs.next())
            {
                try (InputStream blobAsInputStream = getBlobAsInputStream(rs, 1))
                {
                    return QpidByteBuffer.asQpidByteBuffer(blobAsInputStream);
                }
            }
            else
            {
                throw new StoreException("Unable to find message with id " + messageId);
            }

        }
        catch (SQLException | IOException e)
        {
            throw new StoreException("Error retrieving content for message " + messageId + ": " + e.getMessage(), e);
        }
    }

    @Override
    public boolean isPersistent()
    {
        return true;
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

    protected class JDBCTransaction implements Transaction
    {
        private final ConnectionWrapper _connWrapper;
        private int _storeSizeIncrease;
        private final List<Runnable> _preCommitActions = new ArrayList<>();
        private final List<Runnable> _postCommitActions = new ArrayList<>();
        private final Map<Long, List<TransactionLogResource>> _messagesToEnqueue = new HashMap<>();

        protected JDBCTransaction()
        {
            try
            {
                _connWrapper = new ConnectionWrapper(newConnection());
            }
            catch (SQLException e)
            {
                throw new StoreException(e);
            }

            _preCommitActions.add(() -> AbstractJDBCMessageStore.this.enqueueMessages(_connWrapper, _messagesToEnqueue));
        }

        @Override
        public MessageEnqueueRecord enqueueMessage(TransactionLogResource queue, EnqueueableMessage message)
        {
            checkMessageStoreOpen();

            final StoredMessage storedMessage = message.getStoredMessage();
            if(storedMessage instanceof StoredJDBCMessage)
            {
                _preCommitActions.add(() -> {
                    try
                    {
                        ((StoredJDBCMessage) storedMessage).store(_connWrapper.getConnection());
                        _storeSizeIncrease += storedMessage.getContentSize();
                    }
                    catch (SQLException e)
                    {
                        throw new StoreException("Exception on enqueuing message into message store" + _messageId, e);
                    }
                });
            }
            List<TransactionLogResource> queues = _messagesToEnqueue.computeIfAbsent(message.getMessageNumber(), messageId -> new ArrayList<>());
            queues.add(queue);
            return new JDBCEnqueueRecord(queue.getId(), message.getMessageNumber());
        }

        @Override
        public void dequeueMessage(final MessageEnqueueRecord enqueueRecord)
        {
            checkMessageStoreOpen();

            AbstractJDBCMessageStore.this.dequeueMessage(_connWrapper,
                                                         enqueueRecord.getQueueId(),
                                                         enqueueRecord.getMessageNumber());
        }

        @Override
        public void commitTran()
        {
            checkMessageStoreOpen();
            doPreCommitActions();
            AbstractJDBCMessageStore.this.commitTran(_connWrapper);
            storedSizeChange(_storeSizeIncrease);
            doPostCommitActions();
        }

        @Override
        public <X> ListenableFuture<X> commitTranAsync(final X val)
        {
            checkMessageStoreOpen();
            doPreCommitActions();
            ListenableFuture<X> futureResult = AbstractJDBCMessageStore.this.commitTranAsync(_connWrapper, val);
            storedSizeChange(_storeSizeIncrease);
            doPostCommitActions();
            return futureResult;
        }

        private void doPreCommitActions()
        {
            for(Runnable action : _preCommitActions)
            {
                action.run();
            }
            _preCommitActions.clear();
            _messagesToEnqueue.clear();
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
        public void abortTran()
        {
            checkMessageStoreOpen();
            _preCommitActions.clear();
            _messagesToEnqueue.clear();
            AbstractJDBCMessageStore.this.abortTran(_connWrapper);
        }

        @Override
        public void removeXid(final StoredXidRecord record)
        {
            checkMessageStoreOpen();

            AbstractJDBCMessageStore.this.removeXid(_connWrapper,
                                                    record.getFormat(),
                                                    record.getGlobalId(),
                                                    record.getBranchId());
        }

        @Override
        public StoredXidRecord recordXid(final long format,
                                         final byte[] globalId,
                                         final byte[] branchId,
                                         EnqueueRecord[] enqueues,
                                         DequeueRecord[] dequeues)
        {
            checkMessageStoreOpen();

            _postCommitActions.addAll(AbstractJDBCMessageStore.this.recordXid(_connWrapper, format, globalId, branchId, enqueues, dequeues));
            return new JDBCStoredXidRecord(format, globalId, branchId);
        }


    }

    private static class JDBCStoredXidRecord implements Transaction.StoredXidRecord
    {
        private final long _format;
        private final byte[] _globalId;
        private final byte[] _branchId;

        public JDBCStoredXidRecord(final long format, final byte[] globalId, final byte[] branchId)
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

            final JDBCStoredXidRecord that = (JDBCStoredXidRecord) o;

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
            if (_metaData != null)
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

    private class StoredJDBCMessage<T extends StorableMessageMetaData> implements StoredMessage<T>, MessageHandle<T>
    {

        private final long _messageId;
        private final int _contentSize;
        private final int _metadataSize;

        private MessageDataRef<T> _messageDataRef;

        StoredJDBCMessage(long messageId,
                          T metaData, boolean isRecovered)
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
                    try
                    {
                        metaData = (T) AbstractJDBCMessageStore.this.getMetaData(_messageId);
                        _messageDataRef = new MessageDataRef<>(metaData, _messageDataRef.getData(), false);
                        _inMemorySize.addAndGet(getMetadataSize());
                    }
                    catch (SQLException e)
                    {
                        throw new StoreException("Failed to get metadata for message id: " + _messageId, e);
                    }
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
                    data = AbstractJDBCMessageStore.this.getAllContent(_messageId);
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

        synchronized void store(final Connection conn) throws SQLException
        {
            if (!stored())
            {
                AbstractJDBCMessageStore.this.storeMetaData(conn, _messageId, _messageDataRef.getMetaData());
                AbstractJDBCMessageStore.this.addContent(conn, _messageId,
                                                         _messageDataRef.getData() == null
                                                                ? QpidByteBuffer.emptyQpidByteBuffer()
                                                                : _messageDataRef.getData());

                getLogger().debug("Storing message {} to store", _messageId);

                _messageDataRef.setSoft();
            }
        }

        synchronized ListenableFuture<Void> flushToStore()
        {
            if (_messageDataRef != null)
            {
                if(!stored())
                {
                    try (Connection conn = newConnection())
                    {
                        store(conn);
                        conn.commit();
                        storedSizeChange(getContentSize());
                    }
                    catch (SQLException e)
                    {
                        throw new StoreException("Failed to flow to disk", e);
                    }
                }

            }
            return Futures.immediateFuture(null);
        }

        @Override
        public synchronized void remove()
        {
            getLogger().debug("REMOVE called on message: {}", _messageId);

            checkMessageStoreOpen();
            _messages.remove(this);
            if(stored())
            {
                AbstractJDBCMessageStore.this.removeMessageAsync(_messageId);
                storedSizeChange(-getContentSize());
            }

            final T metaData;
            long bytesCleared = 0;
            if ((metaData = _messageDataRef.getMetaData()) != null)
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

        @Override
        public String toString()
        {
            return this.getClass() + "[messageId=" + _messageId + "]";
        }
    }

    @Override
    public void addEventListener(EventListener eventListener, Event... events)
    {
        _eventManager.addEventListener(eventListener, events);
    }

    @Override
    public MessageStoreReader newMessageStoreReader()
    {
        return new JDBCMessageStoreReader();
    }

    private class JDBCMessageStoreReader implements MessageStoreReader
    {

        @Override
        public StoredMessage<?> getMessage(long messageId) throws StoreException
        {
            checkMessageStoreOpen();

            StoredJDBCMessage message;
            try(Connection conn = newAutoCommitConnection())
            {
                try (PreparedStatement stmt = conn.prepareStatement("SELECT message_id, meta_data FROM " + getMetaDataTableName()
                                                                    + " WHERE message_id = ?"))
                {
                    stmt.setLong(1, messageId);
                    try (ResultSet rs = stmt.executeQuery())
                    {
                        if (rs.next())
                        {
                            try (InputStream blobAsInputStream = getBlobAsInputStream(rs, 2))
                            {
                                final StorableMessageMetaData metaData = getStorableMessageMetaData(messageId, blobAsInputStream);
                                message = createStoredJDBCMessage(messageId, metaData, true);
                            }
                        }
                        else
                        {
                            message = null;
                        }
                    }
                }
                return message;
            }
            catch (SQLException | IOException e)
            {
                throw new StoreException("Error encountered when visiting messages", e);
            }
        }

        @Override
        public void close()
        {

        }


        @Override
        public void visitMessages(MessageHandler handler) throws StoreException
        {
            checkMessageStoreOpen();
            while(isMessageRemovalScheduled());
            try(Connection conn = newAutoCommitConnection())
            {
                try (Statement stmt = conn.createStatement())
                {
                    try (ResultSet rs = stmt.executeQuery("SELECT message_id, meta_data FROM "
                                                          + getMetaDataTableName()))
                    {
                        while (rs.next())
                        {
                            long messageId = rs.getLong(1);
                            try (InputStream dataAsInputStream = getBlobAsInputStream(rs, 2))
                            {
                                StorableMessageMetaData metaData = getStorableMessageMetaData(messageId, dataAsInputStream);
                                StoredJDBCMessage message = createStoredJDBCMessage(messageId, metaData, true);
                                if (!handler.handle(message))
                                {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            catch (SQLException | IOException e)
            {
                throw new StoreException("Error encountered when visiting messages", e);
            }
        }

        @Override
        public void visitMessageInstances(TransactionLogResource queue, MessageInstanceHandler handler)
                throws StoreException
        {
            checkMessageStoreOpen();

            try(Connection conn = newAutoCommitConnection())
            {
                CachingUUIDFactory uuidFactory = new CachingUUIDFactory();
                try (PreparedStatement stmt = conn.prepareStatement("SELECT queue_id, message_id FROM "
                                                                    + getQueueEntryTableName()
                                                                    + " WHERE queue_id = ? ORDER BY queue_id, "
                                                                    + "message_id"))
                {
                    stmt.setString(1, queue.getId().toString());
                    try (ResultSet rs = stmt.executeQuery())
                    {
                        while (rs.next())
                        {
                            String id = rs.getString(1);
                            long messageId = rs.getLong(2);
                            UUID uuid = uuidFactory.createUuidFromString(id);
                            if (!handler.handle(new JDBCEnqueueRecord(uuid, messageId)))
                            {
                                break;
                            }
                        }
                    }
                }
            }
            catch (SQLException e)
            {
                throw new StoreException("Error encountered when visiting message instances", e);
            }
        }

        @Override
        public void visitMessageInstances(MessageInstanceHandler handler) throws StoreException
        {
            checkMessageStoreOpen();

            try(Connection conn = newAutoCommitConnection())
            {
                CachingUUIDFactory uuidFactory = new CachingUUIDFactory();
                try (Statement stmt = conn.createStatement())
                {
                    try (ResultSet rs = stmt.executeQuery("SELECT queue_id, message_id FROM " + getQueueEntryTableName()
                                                          + " ORDER BY queue_id, message_id"))
                    {
                        while (rs.next())
                        {
                            String id = rs.getString(1);
                            long messageId = rs.getLong(2);
                            UUID queueId = uuidFactory.createUuidFromString(id);
                            if (!handler.handle(new JDBCEnqueueRecord(queueId, messageId)))
                            {
                                break;
                            }
                        }
                    }
                }
            }
            catch (SQLException e)
            {
                throw new StoreException("Error encountered when visiting message instances", e);
            }
        }

        @Override
        public void visitDistributedTransactions(DistributedTransactionHandler handler) throws StoreException
        {
            checkMessageStoreOpen();

            try(Connection conn = newAutoCommitConnection())
            {
                List<Xid> xids = new ArrayList<Xid>();

                try (Statement stmt = conn.createStatement())
                {
                    try (ResultSet rs = stmt.executeQuery("SELECT format, global_id, branch_id FROM "
                                                          + getXidTableName()))
                    {
                        while (rs.next())
                        {

                            long format = rs.getLong(1);
                            byte[] globalId = rs.getBytes(2);
                            byte[] branchId = rs.getBytes(3);
                            xids.add(new Xid(format, globalId, branchId));
                        }
                    }
                }


                for (Xid xid : xids)
                {
                    CachingUUIDFactory uuidFactory = new CachingUUIDFactory();
                    List<RecordImpl> enqueues = new ArrayList<>();
                    List<RecordImpl> dequeues = new ArrayList<>();

                    try (PreparedStatement pstmt = conn.prepareStatement(
                            "SELECT action_type, queue_id, message_id FROM " + getXidActionsTableName()
                            +
                            " WHERE format = ? and global_id = ? and branch_id = ?"))
                    {
                        pstmt.setLong(1, xid.getFormat());
                        pstmt.setBytes(2, xid.getGlobalId());
                        pstmt.setBytes(3, xid.getBranchId());

                        try (ResultSet rs = pstmt.executeQuery())
                        {
                            while (rs.next())
                            {

                                String actionType = rs.getString(1);
                                UUID queueId = uuidFactory.createUuidFromString(rs.getString(2));
                                long messageId = rs.getLong(3);

                                RecordImpl record = new RecordImpl(queueId, messageId);
                                List<RecordImpl> records = "E".equals(actionType) ? enqueues : dequeues;
                                records.add(record);
                            }
                        }
                    }

                    if (!handler.handle(new JDBCStoredXidRecord(xid.getFormat(), xid.getGlobalId(), xid.getBranchId()),
                                        enqueues.toArray(new RecordImpl[enqueues.size()]),
                                        dequeues.toArray(new RecordImpl[dequeues.size()])))
                    {
                        break;
                    }
                }

            }
            catch (SQLException e)
            {
                throw new StoreException("Error encountered when visiting distributed transactions", e);

            }
        }
    }

    protected abstract void storedSizeChange(int storeSizeIncrease);

    protected void onDelete(final Connection conn)
    {
        try
        {
            for (Action<Connection> deleteAction: _deleteActions)
            {
                deleteAction.performAction(conn);
            }
            _deleteActions.clear();
        }
        finally
        {
            JdbcUtils.dropTables(conn, getLogger(), getTableNames());
        }
    }

    public List<String> getTableNames()
    {
        return Arrays.asList(getDbVersionTableName(),
                             getMetaDataTableName(),
                             getMessageContentTableName(),
                             getQueueEntryTableName(),
                             getXidTableName(),
                             getXidActionsTableName());
    }


    private <T> T getContextValue(final Class<T> variableClass,
                                  final String name,
                                  final T defaultValue)
    {
        if (_parent.getContextKeys(false).contains(name))
        {
            return _parent.getContextValue(variableClass, name);
        }
        else
        {
            return defaultValue;
        }
    }

    private static class JDBCEnqueueRecord implements MessageEnqueueRecord
    {
        private final UUID _queueId;
        private final long _messageNumber;

        public JDBCEnqueueRecord(final UUID queueId,
                                 final long messageNumber)
        {
            _queueId = queueId;
            _messageNumber = messageNumber;
        }

        @Override
        public UUID getQueueId()
        {
            return _queueId;
        }

        @Override
        public long getMessageNumber()
        {
            return _messageNumber;
        }
    }
}
