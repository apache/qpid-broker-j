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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.message.MessageReference;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.LifetimePolicy;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;

public class SortedQueueEntryListTest extends QueueEntryListTestBase
{
    private static SelfValidatingSortedQueueEntryList _sqel;

    public final static String[] KEYS = { " 73", " 18", " 11", "127", "166", "163", " 69", " 60", "191", "144",
                                          " 17", "161", "145", "140", "157", " 47", "136", " 56", "176", " 81",
                                          "195", " 96", "  2", " 68", "101", "141", "159", "187", "149", " 45",
                                          " 64", "100", " 83", " 51", " 79", " 82", "180", " 26", " 61", " 62",
                                          " 78", " 46", "147", " 91", "120", "164", " 92", "172", "188", " 50",
                                          "111", " 89", "  4", "  8", " 16", "151", "122", "178", " 33", "124",
                                          "171", "165", "116", "113", "155", "148", " 29", "  0", " 37", "131",
                                          "146", " 57", "112", " 97", " 23", "108", "123", "117", "167", " 52",
                                          " 98", "  6", "160", " 25", " 49", " 34", "182", "185", " 30", " 66",
                                          "152", " 58", " 86", "118", "189", " 84", " 36", "104", "  7", " 76",
                                          " 87", "  1", " 80", " 10", "142", " 59", "137", " 12", " 67", " 22",
                                          "  9", "106", " 75", "109", " 93", " 42", "177", "134", " 77", " 88",
                                          "114", " 43", "143", "135", " 55", "181", " 32", "174", "175", "184",
                                          "133", "107", " 28", "126", "103", " 85", " 38", "158", " 39", "162",
                                          "129", "194", " 15", " 24", " 19", " 35", "186", " 31", " 65", " 99",
                                          "192", " 74", "156", " 27", " 95", " 54", " 70", " 13", "110", " 41",
                                          " 90", "173", "125", "196", "130", "183", "102", "190", "132", "105",
                                          " 21", " 53", "139", " 94", "115", " 48", " 44", "179", "128", " 14",
                                          " 72", "119", "153", "168", "197", " 40", "150", "138", "  5", "154",
                                          "169", " 71", "199", "198", "170", "  3", "121", " 20", " 63", "193" };

    public final static String[] TEXTKEYS = { "AAA", "BBB", "CCC", "DDD", "EEE", "FFF", "GGG", "HHH", "III", "JJJ",
                                            "KKK", "LLL", "MMM", "NNN", "OOO", "PPP", "QQQ", "RRR", "SSS", "TTT",
                                            "UUU", "VVV", "XXX", "YYY", "ZZZ"};

    private final static String[] KEYS_SORTED = KEYS.clone();

    private QueueManagingVirtualHost<?> _virtualHost;
    private SortedQueueImpl _testQueue;

    @BeforeAll
    public void beforeAll() throws Exception
    {
        _virtualHost = BrokerTestHelper.createVirtualHost(getTestClassName(), this);
    }

    @BeforeEach
    public void setUp() throws Exception
    {
        final Map<String,Object> attributes = Map.of(Queue.ID,randomUUID(),
                Queue.NAME, getTestName(),
                Queue.DURABLE, false,
                Queue.LIFETIME_POLICY, LifetimePolicy.PERMANENT,
                SortedQueue.SORT_KEY, "KEY");

        // Create test list
        _testQueue = new SortedQueueImpl(attributes, _virtualHost)
        {
            SelfValidatingSortedQueueEntryList _entries;
            @Override
            protected void onOpen()
            {
                super.onOpen();
                _entries = new SelfValidatingSortedQueueEntryList(this);
            }

            @Override
            SelfValidatingSortedQueueEntryList getEntries()
            {
                return _entries;
            }
        };
        _testQueue.open();
        _sqel = (SelfValidatingSortedQueueEntryList) _testQueue.getEntries();

        // Create result array
        Arrays.sort(KEYS_SORTED);

        // Build test list
        long messageId = 0L;
        for (final String key : KEYS)
        {
            final ServerMessage<?> msg = generateTestMessage(messageId++, key);
            _sqel.add(msg, null);
        }
    }

    @Override
    public SortedQueueEntryList getTestList()
    {
        return getTestList(false);
    }

    @Override
    public SortedQueueEntryList getTestList(final boolean newList)
    {
        if (newList)
        {
            return new SelfValidatingSortedQueueEntryList(_testQueue);
        }
        else
        {
            return _sqel;
        }
    }

    @Override
    public int getExpectedListLength()
    {
        return KEYS.length;
    }

    @Override
    public long getExpectedFirstMsgId()
    {
        return 67L;
    }

    @Override
    public ServerMessage<?> getTestMessageToAdd()
    {
        return generateTestMessage(1, "test value");
    }

    @Override
    protected SortedQueueImpl getTestQueue()
    {
        return _testQueue;
    }

    @SuppressWarnings("rawtypes")
    private ServerMessage<?> generateTestMessage(final long id, final String keyValue)
    {
        final ServerMessage<?> message = mock(ServerMessage.class);
        final AMQMessageHeader hdr = mock(AMQMessageHeader.class);
        when(message.getMessageHeader()).thenReturn(hdr);
        when(hdr.getHeader(eq("KEY"))).thenReturn(keyValue);
        when(hdr.containsHeader(eq("KEY"))).thenReturn(true);
        when(hdr.getHeaderNames()).thenReturn(Set.of("KEY"));
        final MessageReference ref = mock(MessageReference.class);
        when(ref.getMessage()).thenReturn(message);
        when(message.newReference()).thenReturn(ref);
        when(message.newReference(any(TransactionLogResource.class))).thenReturn(ref);
        when(message.getMessageNumber()).thenReturn(id);
        return message;
    }

    @Override
    @Test
    public void testIterator() throws Exception
    {
        super.testIterator();

        // Test sorted order of list
        final QueueEntryIterator iter = getTestList().iterator();
        int count = 0;
        while (iter.advance())
        {
            final Object expected = KEYS_SORTED[count++];
            assertEquals(expected, getSortedKeyValue(iter),
                    "Sorted queue entry value does not match sorted key array");
        }
    }

    private Object getSortedKeyValue(final QueueEntryIterator iter)
    {
        return (iter.getNode()).getMessage().getMessageHeader().getHeader("KEY");
    }

    private Long getMessageId(final QueueEntryIterator iter)
    {
        return (iter.getNode()).getMessage().getMessageNumber();
    }

    @Test
    public void testNonUniqueSortKeys()
    {
        _sqel = new SelfValidatingSortedQueueEntryList(_testQueue);

        // Build test list
        long messageId = 0L;
        while (messageId < 200)
        {
            final ServerMessage<?> msg = generateTestMessage(messageId++, "samekey");
            _sqel.add(msg, null);
        }

        final QueueEntryIterator iter = getTestList().iterator();
        int count=0;
        while (iter.advance())
        {
            assertEquals("samekey", getSortedKeyValue(iter), "Sorted queue entry value is not as expected");
            final Object expected = (long) count++;
            assertEquals(expected, getMessageId(iter), "Message id not as expected");
        }
    }

    @Test
    public void testNullSortKeys()
    {
        _sqel = new SelfValidatingSortedQueueEntryList(_testQueue);

        // Build test list
        long messageId = 0L;
        while (messageId < 200)
        {
            final ServerMessage<?> msg = generateTestMessage(messageId++, null);
            _sqel.add(msg, null);
        }

        final QueueEntryIterator iter = getTestList().iterator();
        int count=0;
        while (iter.advance())
        {
            assertNull(getSortedKeyValue(iter), "Sorted queue entry value is not as expected");
            final Object expected = (long) count++;
            assertEquals(expected, getMessageId(iter), "Message id not as expected");
        }
    }

    @Test
    public void testAscendingSortKeys()
    {
        _sqel = new SelfValidatingSortedQueueEntryList(_testQueue);

        // Build test list
        long messageId = 0L;
        for (String textKey : TEXTKEYS)
        {
            final ServerMessage<?> msg = generateTestMessage(messageId, textKey);
            messageId++;
            _sqel.add(msg, null);
        }

        final QueueEntryIterator iter = getTestList().iterator();
        int count=0;
        while (iter.advance())
        {
            assertEquals(TEXTKEYS[count], getSortedKeyValue(iter),
                    "Sorted queue entry value is not as expected");
            assertEquals(Long.valueOf(count), getMessageId(iter), "Message id not as expected");
            count++;
        }
    }

    @Test
    public void testDescendingSortKeys()
    {
        _sqel = new SelfValidatingSortedQueueEntryList(_testQueue);

        // Build test list
        long messageId = 0L;
        for (int i = TEXTKEYS.length - 1; i >= 0; i--)
        {
            final ServerMessage<?> msg = generateTestMessage(messageId, TEXTKEYS[i]);
            messageId++;
            _sqel.add(msg, null);
        }

        final QueueEntryIterator iter = getTestList().iterator();
        int count=0;
        while (iter.advance())
        {
            assertEquals(TEXTKEYS[count], getSortedKeyValue(iter),
                    "Sorted queue entry value is not as expected");
            assertEquals(Long.valueOf(TEXTKEYS.length - count - 1), getMessageId(iter), "Message id not as expected");
            count++;
        }
    }

    @Test
    public void testInsertAfter()
    {
        _sqel = new SelfValidatingSortedQueueEntryList(_testQueue);

        ServerMessage<?> msg = generateTestMessage(1, "A");
        _sqel.add(msg, null);

        SortedQueueEntry entry = _sqel.next(_sqel.getHead());
        validateEntry(entry, "A", 1);

        msg = generateTestMessage(2, "B");
        _sqel.add(msg, null);

        entry = _sqel.next(_sqel.getHead());
        validateEntry(entry, "A", 1);

        entry = _sqel.next(entry);
        validateEntry(entry, "B", 2);
    }

    @Test
    public void testInsertBefore()
    {
        _sqel = new SelfValidatingSortedQueueEntryList(_testQueue);

        ServerMessage<?> msg = generateTestMessage(1, "B");
        _sqel.add(msg, null);

        SortedQueueEntry entry = _sqel.next(_sqel.getHead());
        validateEntry(entry, "B", 1);

        msg = generateTestMessage(2, "A");
        _sqel.add(msg, null);

        entry = _sqel.next(_sqel.getHead());
        validateEntry(entry, "A", 2);

        entry = _sqel.next(entry);
        validateEntry(entry, "B", 1);
    }

    @Test
    public void testInsertInbetween()
    {
        _sqel = new SelfValidatingSortedQueueEntryList(_testQueue);

        ServerMessage<?> msg = generateTestMessage(1, "A");
        _sqel.add(msg, null);
        SortedQueueEntry entry = _sqel.next(_sqel.getHead());
        validateEntry(entry, "A", 1);

        msg = generateTestMessage(2, "C");
        _sqel.add(msg, null);

        entry = _sqel.next(_sqel.getHead());
        validateEntry(entry, "A", 1);

        entry = _sqel.next(entry);
        validateEntry(entry, "C", 2);

        msg = generateTestMessage(3, "B");
        _sqel.add(msg, null);

        entry = _sqel.next(_sqel.getHead());
        validateEntry(entry, "A", 1);

        entry = _sqel.next(entry);
        validateEntry(entry, "B", 3);

        entry = _sqel.next(entry);
        validateEntry(entry, "C", 2);
    }

    @Test
    public void testInsertAtHead()
    {
        _sqel = new SelfValidatingSortedQueueEntryList(_testQueue);

        ServerMessage<?> msg = generateTestMessage(1, "B");
        _sqel.add(msg, null);

        SortedQueueEntry entry = _sqel.next(_sqel.getHead());
        validateEntry(entry, "B", 1);

        msg = generateTestMessage(2, "D");
        _sqel.add(msg, null);

        entry = _sqel.next(_sqel.getHead());
        validateEntry(entry, "B", 1);

        entry = _sqel.next(entry);
        validateEntry(entry, "D", 2);

        msg = generateTestMessage(3, "C");
        _sqel.add(msg, null);

        entry = _sqel.next(_sqel.getHead());
        validateEntry(entry, "B", 1);

        entry = _sqel.next(entry);
        validateEntry(entry, "C", 3);

        entry = _sqel.next(entry);
        validateEntry(entry, "D", 2);

        msg = generateTestMessage(4, "A");
        _sqel.add(msg, null);

        entry = _sqel.next(_sqel.getHead());
        validateEntry(entry, "A", 4);

        entry = _sqel.next(entry);
        validateEntry(entry, "B", 1);

        entry = _sqel.next(entry);
        validateEntry(entry, "C", 3);

        entry = _sqel.next(entry);
        validateEntry(entry, "D", 2);
    }

    @Test
    public void testGetLeastSignificantOldestEntry()
    {
        final SortedQueueEntryList list = new SortedQueueEntryList(_testQueue, _testQueue.getQueueStatistics());

        final SortedQueueEntry entry1 = list.add(generateTestMessage(1, "B"), null);
        assertEquals(entry1, list.getLeastSignificantOldestEntry(), "Unexpected last entry");

        list.add(generateTestMessage(2, "C"), null);
        assertEquals(entry1, list.getLeastSignificantOldestEntry(), "Unexpected last entry");

        list.add(generateTestMessage(3, null), null);
        assertEquals(entry1, list.getLeastSignificantOldestEntry(), "Unexpected last entry");

        list.add(generateTestMessage(4, "A"), null);
        assertEquals(entry1, list.getLeastSignificantOldestEntry(), "Unexpected last entry");
    }

    private void validateEntry(final SortedQueueEntry entry, final String expectedSortKey, final long expectedMessageId)
    {
        assertEquals(expectedSortKey, entry.getMessage().getMessageHeader().getHeader("KEY"),
                "Sorted queue entry value is not as expected");
        assertEquals(expectedMessageId, entry.getMessage().getMessageNumber(),
                "Sorted queue entry id is not as expected");
    }
}
