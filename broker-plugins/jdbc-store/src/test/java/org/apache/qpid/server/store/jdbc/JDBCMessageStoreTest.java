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

import static org.apache.qpid.server.store.jdbc.TestJdbcUtils.assertTablesExistence;
import static org.apache.qpid.server.store.jdbc.TestJdbcUtils.getTableNames;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.util.concurrent.ListenableFuture;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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


    @Before
    public void setUp() throws Exception
    {
        super.setUp();
        QpidByteBuffer.deinitialisePool();
        QpidByteBuffer.initialisePool(BUFFER_SIZE, POOL_SIZE, SPARSITY_FRACTION);
    }

    @After
    public void tearDown() throws Exception
    {
        try
        {
            if (_connectionURL != null)
            {
                TestJdbcUtils.shutdownDerby(_connectionURL);
            }
        }
        finally
        {
            QpidByteBuffer.deinitialisePool();
            super.tearDown();
        }
    }

    @Test
    public void testTablePrefix() throws Exception
    {
        Collection<String> expectedTables = ((GenericJDBCMessageStore)getStore()).getTableNames();
        for (String expectedTable : expectedTables)
        {
            assertTrue(String.format("Table '%s' does not start with expected prefix '%s'", expectedTable, TEST_TABLE_PREFIX), expectedTable.startsWith(TEST_TABLE_PREFIX));
        }
        try(Connection connection = openConnection())
        {
            assertTablesExistence(expectedTables, getTableNames(connection), true);
        }
    }

    @Test
    public void testOnDelete() throws Exception
    {
        try(Connection connection = openConnection())
        {
            GenericJDBCMessageStore store = (GenericJDBCMessageStore) getStore();
            Collection<String> expectedTables = store.getTableNames();
            assertTablesExistence(expectedTables, getTableNames(connection), true);
            store.closeMessageStore();
            assertTablesExistence(expectedTables, getTableNames(connection), true);
            store.onDelete(getVirtualHost());
            assertTablesExistence(expectedTables, getTableNames(connection), false);
        }
    }

    @Test
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

    @Test
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

    @Test
    public void testDeleteAction()
    {
        GenericJDBCMessageStore store = (GenericJDBCMessageStore) getStore();
        AtomicBoolean deleted = new AtomicBoolean();
        store.addDeleteAction(object -> deleted.set(true));

        store.closeMessageStore();
        store.onDelete(getVirtualHost());
        assertEquals("Delete action was not invoked", true, deleted.get());
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

    private Connection openConnection() throws SQLException
    {
        return TestJdbcUtils.openConnection(_connectionURL);
    }
}
