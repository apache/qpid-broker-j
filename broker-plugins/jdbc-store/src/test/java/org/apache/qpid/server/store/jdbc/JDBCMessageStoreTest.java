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

import static org.apache.qpid.server.store.jdbc.AbstractJDBCMessageStore.IN_CLAUSE_MAX_SIZE;
import static org.apache.qpid.server.store.jdbc.TestJdbcUtils.assertTablesExistence;
import static org.apache.qpid.server.store.jdbc.TestJdbcUtils.getTableNames;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import com.google.common.util.concurrent.ListenableFuture;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.message.internal.InternalMessage;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.store.MessageDurability;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.MessageStoreTestCase;
import org.apache.qpid.server.store.StoredMessage;
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

    @Test
    public void testRemoveMessages() throws Exception
    {
        GenericJDBCMessageStore store = spy((GenericJDBCMessageStore) getStore());
        when(store.newConnection()).thenReturn(mock(Connection.class, Mockito.RETURNS_MOCKS));

        store.removeMessages(LongStream.rangeClosed(1,1000).boxed().collect(Collectors.toList()));

        verify(store).removeMessagesFromDatabase(any(Connection.class), any(List.class));

        Mockito.reset(store);

        store.removeMessages(LongStream.rangeClosed(1,2001).boxed().collect(Collectors.toList()));

        verify(store).removeMessagesFromDatabase(any(Connection.class), eq(LongStream.rangeClosed(1,1000).boxed().collect(Collectors.toList())));
        verify(store).removeMessagesFromDatabase(any(Connection.class), eq(LongStream.rangeClosed(1001,2000).boxed().collect(Collectors.toList())));
        verify(store).removeMessagesFromDatabase(any(Connection.class), eq(Collections.singletonList(2001L)));
    }

    @Test
    public void testRemoveMessagesWhenNumberOfMessagesEqualsInClauseMaxSize()
    {
        final String queueName = getTestName();
        final UUID transactionalLogId = UUID.randomUUID();
        final TransactionLogResource resource = mockTransactionLogResource(transactionalLogId, queueName);
        final int numberOfMessages = 10;
        final GenericJDBCMessageStore store = (GenericJDBCMessageStore) getStore();
        reOpenStoreWithInClauseMaxSize(store, numberOfMessages);

        final List<MessageEnqueueRecord> records = enqueueMessages(store, resource, numberOfMessages);
        assertEquals(numberOfMessages, records.size());
        assertRecords(store, resource, records);

        store.removeMessages(records.stream().map(MessageEnqueueRecord::getMessageNumber).collect(Collectors.toList()));

        final List<StoredMessage> stored = new ArrayList<>();
        store.newMessageStoreReader().visitMessages(m-> {
            stored.add(m);
            return true;
        });

        assertTrue(stored.isEmpty());
    }

    @Test
    public void testInClauseMaxSize() throws Exception
    {
        final GenericJDBCMessageStore store = spy((GenericJDBCMessageStore) getStore());
        reOpenStoreWithInClauseMaxSize(store, 10);

        store.removeMessages(LongStream.rangeClosed(1, 21L).boxed().collect(Collectors.toList()));

        verify(store).removeMessagesFromDatabase(any(Connection.class),
                                                 eq(LongStream.rangeClosed(1L, 10L)
                                                              .boxed()
                                                              .collect(Collectors.toList())));
        verify(store).removeMessagesFromDatabase(any(Connection.class),
                                                 eq(LongStream.rangeClosed(11L, 20L)
                                                              .boxed()
                                                              .collect(Collectors.toList())));
        verify(store).removeMessagesFromDatabase(any(Connection.class), eq(Collections.singletonList(21L)));
    }

    private void reOpenStoreWithInClauseMaxSize(final GenericJDBCMessageStore store, final int inClauseMaxSize)
    {
        final ConfiguredObject<?> parent = getVirtualHost();
        when(parent.getContextValue(Integer.class, IN_CLAUSE_MAX_SIZE)).thenReturn(inClauseMaxSize);
        when(parent.getContextKeys(false)).thenReturn(Collections.singleton(IN_CLAUSE_MAX_SIZE));

        store.closeMessageStore();
        store.openMessageStore(parent);
    }

    private List<MessageEnqueueRecord> enqueueMessages(final MessageStore store,
                                                       final TransactionLogResource resource,
                                                       final int numberOfMessages)
    {
        final Transaction transaction = store.newTransaction();
        final String name = resource.getName();
        final List<MessageEnqueueRecord> records = LongStream.rangeClosed(1, numberOfMessages)
                                                             .boxed()
                                                             .map(i -> {
                                                                 final InternalMessage m =
                                                                         addTestMessage(store, name, i + "");
                                                                 return transaction.enqueueMessage(resource, m);
                                                             }).collect(Collectors.toList());
        transaction.commitTran();
        return records;
    }

    private void assertRecords(final MessageStore store,
                               final TransactionLogResource resource,
                               final List<MessageEnqueueRecord> records)
    {
        final List<MessageEnqueueRecord> visited = new ArrayList<>();
        store.newMessageStoreReader().visitMessageInstances(resource, (r) -> {
            visited.add(r);
            return true;
        });
        assertEquals(records.stream().map(MessageEnqueueRecord::getMessageNumber).collect(Collectors.toSet()),
                     visited.stream().map(MessageEnqueueRecord::getMessageNumber).collect(Collectors.toSet()));
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

    @Override
    protected boolean flowToDiskSupported()
    {
        return true;
    }

    private Connection openConnection() throws SQLException
    {
        return TestJdbcUtils.openConnection(_connectionURL);
    }
}
