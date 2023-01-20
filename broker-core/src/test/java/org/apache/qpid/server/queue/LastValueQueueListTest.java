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
 *
 */
package org.apache.qpid.server.queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.message.MessageReference;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.test.utils.UnitTestBase;

public class LastValueQueueListTest extends UnitTestBase
{
    private static final String CONFLATION_KEY = "CONFLATION_KEY";
    private static final String TEST_KEY_VALUE = "testKeyValue";
    private static final String TEST_KEY_VALUE1 = "testKeyValue1";
    private static final String TEST_KEY_VALUE2 = "testKeyValue2";

    private QueueManagingVirtualHost<?> _virtualHost;
    private LastValueQueueList _list;
    private LastValueQueueImpl _queue;

    @BeforeAll
    public void beforeAll() throws Exception
    {
        _virtualHost = BrokerTestHelper.createVirtualHost(getTestClassName(), this);
    }

    @BeforeEach
    public void setUp() throws Exception
    {
        final Map<String,Object> queueAttributes = Map.of(Queue.ID, randomUUID(),
                Queue.NAME, getTestName(),
                LastValueQueue.LVQ_KEY, CONFLATION_KEY);
        _queue = new LastValueQueueImpl(queueAttributes, _virtualHost);
        _queue.open();
        _list = _queue.getEntries();
    }

    @Test
    public void testListHasNoEntries()
    {
        final int numberOfEntries = countEntries(_list);
        assertEquals(0, (long) numberOfEntries);
    }

    @Test
    public void testAddMessageWithoutConflationKeyValue()
    {
        final ServerMessage<?> message = createTestServerMessage(null);
        _list.add(message, null);
        final int numberOfEntries = countEntries(_list);
        assertEquals(1, (long) numberOfEntries);
    }

    @Test
    public void testAddAndDiscardMessageWithoutConflationKeyValue()
    {
        final ServerMessage<?> message = createTestServerMessage(null);
        final QueueEntry addedEntry = _list.add(message, null);
        addedEntry.acquire();
        addedEntry.delete();

        final int numberOfEntries = countEntries(_list);
        assertEquals(0, (long) numberOfEntries);
    }

    @Test
    public void testAddMessageWithConflationKeyValue()
    {
        final ServerMessage<?> message = createTestServerMessage(TEST_KEY_VALUE);
        _list.add(message, null);
        final int numberOfEntries = countEntries(_list);
        assertEquals(1, (long) numberOfEntries);
    }

    @Test
    public void testAddAndRemoveMessageWithConflationKeyValue()
    {
        final ServerMessage<?> message = createTestServerMessage(TEST_KEY_VALUE);
        final QueueEntry addedEntry = _list.add(message, null);
        addedEntry.acquire();
        addedEntry.delete();

        final int numberOfEntries = countEntries(_list);
        assertEquals(0, (long) numberOfEntries);
    }

    @Test
    public void testAddTwoMessagesWithDifferentConflationKeyValue()
    {
        final ServerMessage<?> message1 = createTestServerMessage(TEST_KEY_VALUE1);
        final ServerMessage<?> message2 = createTestServerMessage(TEST_KEY_VALUE2);

        _list.add(message1, null);
        _list.add(message2, null);

        final int numberOfEntries = countEntries(_list);
        assertEquals(2, (long) numberOfEntries);
    }

    @Test
    public void testAddTwoMessagesWithSameConflationKeyValue()
    {
        final ServerMessage<?> message1 = createTestServerMessage(TEST_KEY_VALUE);
        final ServerMessage<?> message2 = createTestServerMessage(TEST_KEY_VALUE);

        _list.add(message1, null);
        _list.add(message2, null);

        final int numberOfEntries = countEntries(_list);
        assertEquals(1, (long) numberOfEntries);
    }

    @Test
    public void testSupersededEntryIsDiscardedOnRelease()
    {
        final ServerMessage<?> message1 = createTestServerMessage(TEST_KEY_VALUE);
        final ServerMessage<?> message2 = createTestServerMessage(TEST_KEY_VALUE);
        final QueueEntry entry1 = _list.add(message1, null);
        entry1.acquire(); // simulate an in-progress delivery to consumer

        _list.add(message2, null);

        assertFalse(entry1.isDeleted());
        assertEquals(2, (long) countEntries(_list));

        entry1.release(); // simulate consumer rollback/recover

        assertEquals(1, (long) countEntries(_list));
        assertTrue(entry1.isDeleted());
    }

    @Test
    public void testConflationMapMaintained()
    {
        assertEquals(0, (long) _list.getLatestValuesMap().size());

        final ServerMessage<?> message = createTestServerMessage(TEST_KEY_VALUE);
        final QueueEntry addedEntry = _list.add(message, null);

        assertEquals(1, (long) countEntries(_list));
        assertEquals(1, (long) _list.getLatestValuesMap().size());

        addedEntry.acquire();
        addedEntry.delete();

        assertEquals(0, (long) countEntries(_list));
        assertEquals(0, (long) _list.getLatestValuesMap().size());
    }

    @Test
    public void testConflationMapMaintainedWithDifferentConflationKeyValue()
    {
        assertEquals(0, (long) _list.getLatestValuesMap().size());

        final ServerMessage<?> message1 = createTestServerMessage(TEST_KEY_VALUE1);
        final ServerMessage<?> message2 = createTestServerMessage(TEST_KEY_VALUE2);
        final QueueEntry addedEntry1 = _list.add(message1, null);
        final QueueEntry addedEntry2 = _list.add(message2, null);

        assertEquals(2, (long) countEntries(_list));
        assertEquals(2, (long) _list.getLatestValuesMap().size());

        addedEntry1.acquire();
        addedEntry1.delete();
        addedEntry2.acquire();
        addedEntry2.delete();

        assertEquals(0, (long) countEntries(_list));
        assertEquals(0, (long) _list.getLatestValuesMap().size());
    }

    @Test
    public void testGetLesserOldestEntry()
    {
        final LastValueQueueList queueEntryList = new LastValueQueueList(_queue, _queue.getQueueStatistics());

        final QueueEntry entry1 =  queueEntryList.add(createTestServerMessage(TEST_KEY_VALUE1), null);
        assertEquals(entry1, queueEntryList.getLeastSignificantOldestEntry(), "Unexpected last message");

        final QueueEntry entry2 =  queueEntryList.add(createTestServerMessage(TEST_KEY_VALUE2), null);
        assertEquals(entry1, queueEntryList.getLeastSignificantOldestEntry(), "Unexpected last message");

        final QueueEntry entry3 =  queueEntryList.add(createTestServerMessage(TEST_KEY_VALUE1), null);
        assertEquals(entry2, queueEntryList.getLeastSignificantOldestEntry(), "Unexpected last message");

        queueEntryList.add(createTestServerMessage(TEST_KEY_VALUE2), null);
        assertEquals(entry3, queueEntryList.getLeastSignificantOldestEntry(), "Unexpected last message");
    }

    private int countEntries(LastValueQueueList list)
    {
        final QueueEntryIterator iterator = list.iterator();
        int count = 0;
        while (iterator.advance())
        {
            count++;
        }
        return count;
    }

    @SuppressWarnings("rawtypes")
    private ServerMessage<?> createTestServerMessage(String conflationKeyValue)
    {
        final ServerMessage<?> mockMessage = mock(ServerMessage.class);

        final AMQMessageHeader messageHeader = mock(AMQMessageHeader.class);
        when(messageHeader.getHeader(CONFLATION_KEY)).thenReturn(conflationKeyValue);
        when(mockMessage.getMessageHeader()).thenReturn(messageHeader);

        final MessageReference messageReference = mock(MessageReference.class);
        when(mockMessage.newReference()).thenReturn(messageReference);
        when(mockMessage.newReference(any(TransactionLogResource.class))).thenReturn(messageReference);
        when(messageReference.getMessage()).thenReturn(mockMessage);

        return mockMessage;
    }
}
