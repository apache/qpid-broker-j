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
package org.apache.qpid.server.queue;

import static org.apache.qpid.server.model.Queue.QUEUE_SCAVANGE_COUNT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.message.MessageReference;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;

public class StandardQueueEntryListTest extends QueueEntryListTestBase
{
    private QueueManagingVirtualHost<?> _virtualHost;
    private StandardQueueImpl _testQueue;
    private StandardQueueEntryList _sqel;

    @BeforeAll
    public void beforeAll() throws Exception
    {
        _virtualHost = BrokerTestHelper.createVirtualHost(getTestClassName(), this);
    }

    @BeforeEach
    public void setUp() throws Exception
    {
        final Map<String,Object> queueAttributes = Map.of(Queue.ID, randomUUID(),
                Queue.NAME, getTestName());
        _testQueue = new StandardQueueImpl(queueAttributes, _virtualHost);
        _testQueue.open();
        _sqel = _testQueue.getEntries();
        for (int i = 1; i <= 100; i++)
        {
            final QueueEntry bleh = _sqel.add(createServerMessage(i), null);
            assertNotNull(bleh, "QE should not have been null");
        }
    }

    @Override
    public StandardQueueEntryList getTestList()
    {
        return getTestList(false);
    }

    @Override
    public StandardQueueEntryList getTestList(boolean newList)
    {
        if (newList)
        {
            final Map<String,Object> queueAttributes = Map.of(Queue.ID, randomUUID(),
                    Queue.NAME, getTestName());
            final StandardQueueImpl queue = new StandardQueueImpl(queueAttributes, _virtualHost);
            queue.open();
            return queue.getEntries();
        }
        else
        {
            return _sqel;
        }
    }

    @Override
    public long getExpectedFirstMsgId()
    {
        return 1;
    }

    @Override
    public int getExpectedListLength()
    {
        return 100;
    }

    @Override
    public ServerMessage<?> getTestMessageToAdd()
    {
        return createServerMessage(1);
    }

    @Override
    protected StandardQueueImpl getTestQueue()
    {
        return _testQueue;
    }

    @Test
    public void testScavenge()
    {
        final StandardQueueImpl mockQueue = mock(StandardQueueImpl.class);
        when(mockQueue.getContextValue(Integer.class, QUEUE_SCAVANGE_COUNT)).thenReturn(9);
        final OrderedQueueEntryList sqel = new StandardQueueEntryList(mockQueue, new QueueStatistics());
        final ConcurrentMap<Integer,QueueEntry> entriesMap = new ConcurrentHashMap<>();

        //Add messages to generate QueueEntry's
        for (int i = 1; i <= 100 ; i++)
        {
            final QueueEntry bleh = sqel.add(createServerMessage(i), null);
            assertNotNull(bleh, "QE should not have been null");
            entriesMap.put(i,bleh);
        }

        final OrderedQueueEntry head = (OrderedQueueEntry) sqel.getHead();

        //We shall now delete some specific messages mid-queue that will lead to
        //requiring a scavenge once the requested threshold of 9 deletes is passed

        //Delete the 2nd message only
        assertTrue(remove(entriesMap, 2), "Failed to delete QueueEntry");
        verifyDeletedButPresentBeforeScavenge(head, 2);

        //Delete messages 12 to 14
        assertTrue(remove(entriesMap, 12), "Failed to delete QueueEntry");
        verifyDeletedButPresentBeforeScavenge(head, 12);
        assertTrue(remove(entriesMap, 13), "Failed to delete QueueEntry");
        verifyDeletedButPresentBeforeScavenge(head, 13);
        assertTrue(remove(entriesMap, 14), "Failed to delete QueueEntry");
        verifyDeletedButPresentBeforeScavenge(head, 14);

        //Delete message 20 only
        assertTrue(remove(entriesMap, 20), "Failed to delete QueueEntry");
        verifyDeletedButPresentBeforeScavenge(head, 20);

        //Delete messages 81 to 84
        assertTrue(remove(entriesMap, 81), "Failed to delete QueueEntry");
        verifyDeletedButPresentBeforeScavenge(head, 81);
        assertTrue(remove(entriesMap, 82), "Failed to delete QueueEntry");
        verifyDeletedButPresentBeforeScavenge(head, 82);
        assertTrue(remove(entriesMap, 83), "Failed to delete QueueEntry");
        verifyDeletedButPresentBeforeScavenge(head, 83);
        assertTrue(remove(entriesMap, 84), "Failed to delete QueueEntry");
        verifyDeletedButPresentBeforeScavenge(head, 84);

        //Delete message 99 - this is the 10th message deleted that is after the queue head
        //and so will invoke the scavenge() which is set to go after 9 previous deletions
        assertTrue(remove(entriesMap, 99), "Failed to delete QueueEntry");

        verifyAllDeletedMessagedNotPresent(head, entriesMap);
    }
    
    private boolean remove(final Map<Integer,QueueEntry> entriesMap, final int pos)
    {
        final QueueEntry entry = entriesMap.remove(pos);
        final boolean wasDeleted = entry.isDeleted();
        entry.acquire();
        entry.delete();
        return entry.isDeleted() && !wasDeleted;
    }

    private void verifyDeletedButPresentBeforeScavenge(final OrderedQueueEntry head, final long messageId)
    {
        //Use the head to get the initial entry in the queue
        OrderedQueueEntry entry = head.getNextNode();

        for (long i = 1; i < messageId ; i++)
        {
            assertEquals(i, entry.getMessage().getMessageNumber(), "Expected QueueEntry was not found in the list");

            entry = entry.getNextNode();
        }

        assertTrue(entry.isDeleted(), "Entry should have been deleted");
    }

    private void verifyAllDeletedMessagedNotPresent(final OrderedQueueEntry head, final Map<Integer,QueueEntry> remainingMessages)
    {
        //Use the head to get the initial entry in the queue
        OrderedQueueEntry entry = head.getNextNode();

        assertNotNull(entry, "Initial entry should not have been null");

        int count = 0;

        while (entry != null)
        {
            assertFalse(entry.isDeleted(),
                        "Entry " + entry.getMessage().getMessageNumber() + " should not have been deleted");

            assertNotNull(remainingMessages.get((int)(entry.getMessage().getMessageNumber())),
                          "QueueEntry " + entry.getMessage().getMessageNumber() + " was not found in the list of remaining entries " + remainingMessages);

            count++;
            entry = entry.getNextNode();
        }

        assertEquals(count, (long) remainingMessages.size(), "Count should have been equal");
    }

    @Test
    public void testGettingNextElement()
    {
        final int numberOfEntries = 5;
        final OrderedQueueEntry[] entries = new OrderedQueueEntry[numberOfEntries];
        final OrderedQueueEntryList queueEntryList = getTestList(true);

        // create test entries
        for (int i = 0; i < numberOfEntries; i++)
        {
            entries[i] = (OrderedQueueEntry) queueEntryList.add(createServerMessage(i), null);
        }

        // test getNext for not acquired entries
        for (int i = 0; i < numberOfEntries; i++)
        {
            final OrderedQueueEntry next = entries[i].getNextValidEntry();
            if (i < numberOfEntries - 1)
            {
                assertEquals(entries[i + 1], next, "Unexpected entry from QueueEntryImpl#getNext()");
            }
            else
            {
                assertNull(next, "The next entry after the last should be null");
            }
        }

        // delete second
        entries[1].acquire();
        entries[1].delete();

        // dequeue third
        entries[2].acquire();
        entries[2].delete();

        OrderedQueueEntry next = entries[2].getNextValidEntry();
        assertEquals(entries[3], next, "expected forth entry");
        next = next.getNextValidEntry();
        assertEquals(entries[4], next, "expected fifth entry");
        next = next.getNextValidEntry();
        assertNull(next, "The next entry after the last should be null");
    }

    @Test
    public void testGetLesserOldestEntry()
    {
        final StandardQueueEntryList queueEntryList = new StandardQueueEntryList(_testQueue, _testQueue.getQueueStatistics());
        final QueueEntry entry1 =  queueEntryList.add(createServerMessage(1), null);
        assertEquals(entry1, queueEntryList.getLeastSignificantOldestEntry(), "Unexpected last message");

        queueEntryList.add(createServerMessage(2), null);
        assertEquals(entry1, queueEntryList.getLeastSignificantOldestEntry(), "Unexpected last message");

        queueEntryList.add(createServerMessage(3), null);
        assertEquals(entry1, queueEntryList.getLeastSignificantOldestEntry(), "Unexpected last message");
    }

    @SuppressWarnings("rawtypes")
    private ServerMessage<?> createServerMessage(final long id)
    {
        ServerMessage<?> message =  mock(ServerMessage.class);
        when(message.getMessageNumber()).thenReturn(id);
        final MessageReference reference = mock(MessageReference.class);
        when(reference.getMessage()).thenReturn(message);
        when(message.newReference()).thenReturn(reference);
        when(message.newReference(any(TransactionLogResource.class))).thenReturn(reference);
        return message;
    }
}
