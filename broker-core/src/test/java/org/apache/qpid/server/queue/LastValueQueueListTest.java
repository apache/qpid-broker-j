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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

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

    private LastValueQueueList _list;
    private LastValueQueueImpl _queue;

    @Before
    public void setUp() throws Exception
    {
        Map<String,Object> queueAttributes = new HashMap<String, Object>();
        queueAttributes.put(Queue.ID, UUID.randomUUID());
        queueAttributes.put(Queue.NAME, getTestName());
        queueAttributes.put(LastValueQueue.LVQ_KEY, CONFLATION_KEY);
        final QueueManagingVirtualHost virtualHost = BrokerTestHelper.createVirtualHost("testVH", this);
        _queue = new LastValueQueueImpl(queueAttributes, virtualHost);
        _queue.open();
        _list = _queue.getEntries();
    }

    @Test
    public void testListHasNoEntries()
    {
        int numberOfEntries = countEntries(_list);
        assertEquals((long) 0, (long) numberOfEntries);
    }

    @Test
    public void testAddMessageWithoutConflationKeyValue()
    {
        ServerMessage message = createTestServerMessage(null);

        _list.add(message, null);
        int numberOfEntries = countEntries(_list);
        assertEquals((long) 1, (long) numberOfEntries);
    }

    @Test
    public void testAddAndDiscardMessageWithoutConflationKeyValue()
    {
        ServerMessage message = createTestServerMessage(null);

        QueueEntry addedEntry = _list.add(message, null);
        addedEntry.acquire();
        addedEntry.delete();

        int numberOfEntries = countEntries(_list);
        assertEquals((long) 0, (long) numberOfEntries);
    }

    @Test
    public void testAddMessageWithConflationKeyValue()
    {
        ServerMessage message = createTestServerMessage(TEST_KEY_VALUE);

        _list.add(message, null);
        int numberOfEntries = countEntries(_list);
        assertEquals((long) 1, (long) numberOfEntries);
    }

    @Test
    public void testAddAndRemoveMessageWithConflationKeyValue()
    {
        ServerMessage message = createTestServerMessage(TEST_KEY_VALUE);

        QueueEntry addedEntry = _list.add(message, null);
        addedEntry.acquire();
        addedEntry.delete();

        int numberOfEntries = countEntries(_list);
        assertEquals((long) 0, (long) numberOfEntries);
    }

    @Test
    public void testAddTwoMessagesWithDifferentConflationKeyValue()
    {
        ServerMessage message1 = createTestServerMessage(TEST_KEY_VALUE1);
        ServerMessage message2 = createTestServerMessage(TEST_KEY_VALUE2);

        _list.add(message1, null);
        _list.add(message2, null);

        int numberOfEntries = countEntries(_list);
        assertEquals((long) 2, (long) numberOfEntries);
    }

    @Test
    public void testAddTwoMessagesWithSameConflationKeyValue()
    {
        ServerMessage message1 = createTestServerMessage(TEST_KEY_VALUE);
        ServerMessage message2 = createTestServerMessage(TEST_KEY_VALUE);

        _list.add(message1, null);
        _list.add(message2, null);

        int numberOfEntries = countEntries(_list);
        assertEquals((long) 1, (long) numberOfEntries);
    }

    @Test
    public void testSupersededEntryIsDiscardedOnRelease()
    {
        ServerMessage message1 = createTestServerMessage(TEST_KEY_VALUE);
        ServerMessage message2 = createTestServerMessage(TEST_KEY_VALUE);

        QueueEntry entry1 = _list.add(message1, null);
        entry1.acquire(); // simulate an in-progress delivery to consumer

        _list.add(message2, null);
        assertFalse(entry1.isDeleted());

        assertEquals((long) 2, (long) countEntries(_list));

        entry1.release(); // simulate consumer rollback/recover

        assertEquals((long) 1, (long) countEntries(_list));
        assertTrue(entry1.isDeleted());
    }

    @Test
    public void testConflationMapMaintained()
    {
        assertEquals((long) 0, (long) _list.getLatestValuesMap().size());

        ServerMessage message = createTestServerMessage(TEST_KEY_VALUE);

        QueueEntry addedEntry = _list.add(message, null);

        assertEquals((long) 1, (long) countEntries(_list));
        assertEquals((long) 1, (long) _list.getLatestValuesMap().size());

        addedEntry.acquire();
        addedEntry.delete();

        assertEquals((long) 0, (long) countEntries(_list));
        assertEquals((long) 0, (long) _list.getLatestValuesMap().size());
    }

    @Test
    public void testConflationMapMaintainedWithDifferentConflationKeyValue()
    {

        assertEquals((long) 0, (long) _list.getLatestValuesMap().size());

        ServerMessage message1 = createTestServerMessage(TEST_KEY_VALUE1);
        ServerMessage message2 = createTestServerMessage(TEST_KEY_VALUE2);

        QueueEntry addedEntry1 = _list.add(message1, null);
        QueueEntry addedEntry2 = _list.add(message2, null);

        assertEquals((long) 2, (long) countEntries(_list));
        assertEquals((long) 2, (long) _list.getLatestValuesMap().size());

        addedEntry1.acquire();
        addedEntry1.delete();
        addedEntry2.acquire();
        addedEntry2.delete();

        assertEquals((long) 0, (long) countEntries(_list));
        assertEquals((long) 0, (long) _list.getLatestValuesMap().size());
    }

    @Test
    public void testGetLesserOldestEntry()
    {
        LastValueQueueList queueEntryList = new LastValueQueueList(_queue, _queue.getQueueStatistics());

        QueueEntry entry1 =  queueEntryList.add(createTestServerMessage(TEST_KEY_VALUE1), null);
        assertEquals("Unexpected last message", entry1, queueEntryList.getLeastSignificantOldestEntry());

        QueueEntry entry2 =  queueEntryList.add(createTestServerMessage(TEST_KEY_VALUE2), null);
        assertEquals("Unexpected last message", entry1, queueEntryList.getLeastSignificantOldestEntry());

        QueueEntry entry3 =  queueEntryList.add(createTestServerMessage(TEST_KEY_VALUE1), null);
        assertEquals("Unexpected last message", entry2, queueEntryList.getLeastSignificantOldestEntry());

        queueEntryList.add(createTestServerMessage(TEST_KEY_VALUE2), null);
        assertEquals("Unexpected last message", entry3, queueEntryList.getLeastSignificantOldestEntry());
    }

    private int countEntries(LastValueQueueList list)
    {
        QueueEntryIterator iterator =
                list.iterator();
        int count = 0;
        while(iterator.advance())
        {
            count++;
        }
        return count;
    }

    private ServerMessage createTestServerMessage(String conflationKeyValue)
    {
        ServerMessage mockMessage = mock(ServerMessage.class);

        AMQMessageHeader messageHeader = mock(AMQMessageHeader.class);
        when(messageHeader.getHeader(CONFLATION_KEY)).thenReturn(conflationKeyValue);
        when(mockMessage.getMessageHeader()).thenReturn(messageHeader);

        MessageReference messageReference = mock(MessageReference.class);
        when(mockMessage.newReference()).thenReturn(messageReference);
        when(mockMessage.newReference(any(TransactionLogResource.class))).thenReturn(messageReference);

        when(messageReference.getMessage()).thenReturn(mockMessage);

        return mockMessage;
    }

}
