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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ListenableFuture;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.message.internal.InternalMessage;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.store.MessageDurability;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.MessageStoreTestCase;
import org.apache.qpid.server.store.Transaction;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.server.virtualhost.jdbc.JDBCVirtualHost;

public class JDBCMessageStoreTest extends MessageStoreTestCase
{
    private static final String TEST_TABLE_PREFIX = "TEST_TABLE_PREFIX_";
    private String _connectionURL;
    private static final int BUFFER_SIZE = 10;
    private static final int POOL_SIZE = 20;
    private static final double SPARSITY_FRACTION = 1.0;


    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        QpidByteBuffer.deinitialisePool();
        QpidByteBuffer.initialisePool(BUFFER_SIZE, POOL_SIZE, SPARSITY_FRACTION);
    }

    @Override
    public void tearDown() throws Exception
    {
        try
        {
            if (_connectionURL != null)
            {
                shutdownDerby(_connectionURL);
            }
        }
        finally
        {
            QpidByteBuffer.deinitialisePool();
            super.tearDown();
        }
    }

    public void testTablePrefix() throws Exception
    {
        Collection<String> expectedTables = ((GenericJDBCMessageStore)getStore()).getTableNames();
        for (String expectedTable : expectedTables)
        {
            assertTrue(String.format("Table '%s' does not start with expected prefix '%s'", expectedTable, TEST_TABLE_PREFIX), expectedTable.startsWith(TEST_TABLE_PREFIX));
        }
        assertTablesExist(expectedTables, true);
    }

    public void testOnDelete() throws Exception
    {
        Collection<String> expectedTables = ((GenericJDBCMessageStore)getStore()).getTableNames();
        assertTablesExist(expectedTables, true);
        getStore().closeMessageStore();
        assertTablesExist(expectedTables, true);
        getStore().onDelete(mock(JDBCVirtualHost.class));
        assertTablesExist(expectedTables, false);
    }

    public void testEnqueueTransactionCommitAsync() throws Exception
    {
        final String queueName = getTestName();
        final UUID transactionalLogId = UUID.randomUUID();

        final MessageStore store = getStore();
        final TransactionLogResource transactionalLog = mockTransactionLogResource(transactionalLogId, queueName);
        final InternalMessage message = addTestMessage(store, queueName, "test");

        final Transaction transaction = store.newTransaction();
        final MessageEnqueueRecord record = transaction.enqueueMessage(transactionalLog, message);

        assertNotNull("Message enqueue record should not be null", record);
        assertEquals("Unexpected queue id", transactionalLogId, record.getQueueId());
        assertEquals("Unexpected message number", message.getMessageNumber(), record.getMessageNumber());

        final ListenableFuture<Void> future = transaction.commitTranAsync(null);
        future.get(1000, TimeUnit.MILLISECONDS);
    }

    public void testDequeueTransactionCommitAsync() throws Exception
    {
        final String queueName = getTestName();
        final UUID transactionalLogId = UUID.randomUUID();

        final MessageStore store = getStore();
        final TransactionLogResource transactionalLog = mockTransactionLogResource(transactionalLogId, queueName);
        final InternalMessage message = addTestMessage(store, queueName, "test2");

        final Transaction enqueueTransaction = store.newTransaction();
        MessageEnqueueRecord record = enqueueTransaction.enqueueMessage(transactionalLog, message);
        enqueueTransaction.commitTran();

        final Transaction dequeueTransaction = store.newTransaction();
        dequeueTransaction.dequeueMessage(record);

        final ListenableFuture<Void> future = dequeueTransaction.commitTranAsync(null);
        future.get(1000, TimeUnit.MILLISECONDS);
    }

    private InternalMessage addTestMessage(final MessageStore store,
                                           final String transactionalLogName,
                                           final String messageContent)
    {
        final AMQMessageHeader amqpHeader = mock(AMQMessageHeader.class);
        return InternalMessage.createMessage(store, amqpHeader, messageContent, true, transactionalLogName);
    }

    private TransactionLogResource mockTransactionLogResource(final UUID transactionalLogId,
                                                              final String transactionalLogName)
    {
        final TransactionLogResource transactionalLog = mock(TransactionLogResource.class);
        when(transactionalLog.getId()).thenReturn(transactionalLogId);
        when(transactionalLog.getName()).thenReturn(transactionalLogName);
        when(transactionalLog.getMessageDurability()).thenReturn(MessageDurability.ALWAYS);
        return transactionalLog;
    }

    @Override
    protected VirtualHost createVirtualHost()
    {
        _connectionURL = "jdbc:derby:memory:/" + getTestName();

        final JDBCVirtualHost jdbcVirtualHost = mock(JDBCVirtualHost.class);
        when(jdbcVirtualHost.getConnectionUrl()).thenReturn(_connectionURL + ";create=true");
        when(jdbcVirtualHost.getUsername()).thenReturn("test");
        when(jdbcVirtualHost.getPassword()).thenReturn("pass");
        when(jdbcVirtualHost.getTableNamePrefix()).thenReturn(TEST_TABLE_PREFIX);
        return jdbcVirtualHost;
    }


    @Override
    protected MessageStore createMessageStore()
    {
        return new GenericJDBCMessageStore();
    }

    private void assertTablesExist(Collection<String> expectedTables, boolean exists) throws SQLException
    {
        Set<String> existingTables = getTableNames();
        for (String tableName : expectedTables)
        {
            assertEquals("Table " + tableName + (exists ? " is not found" : " actually exist"), exists,
                    existingTables.contains(tableName));
        }
    }

    private Set<String> getTableNames() throws SQLException
    {
        Set<String> tableNames = new HashSet<>();
        Connection conn = null;
        try
        {
            conn = openConnection();
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet tables = metaData.getTables(null, null, null, new String[]{"TABLE"}))
            {
                while (tables.next())
                {
                    tableNames.add(tables.getString("TABLE_NAME"));
                }
            }
        }
        finally
        {
            if (conn != null)
            {
                conn.close();
            }
        }
        return tableNames;
    }

    private Connection openConnection() throws SQLException
    {
        return DriverManager.getConnection(_connectionURL);
    }

    static void shutdownDerby(String connectionURL) throws SQLException
    {
        Connection connection = null;
        try
        {
            connection = DriverManager.getConnection(connectionURL + ";shutdown=true");
        }
        catch(SQLException e)
        {
            if (e.getSQLState().equalsIgnoreCase("08006"))
            {
                //expected and represents a clean shutdown of this database only, do nothing.
            }
            else
            {
                throw e;
            }
        }
        finally
        {
            if (connection != null)
            {
                connection.close();
            }
        }
    }
}
