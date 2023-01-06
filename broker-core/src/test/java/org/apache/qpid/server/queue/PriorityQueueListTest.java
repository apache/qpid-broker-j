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

import static org.junit.jupiter.api.Assertions.assertEquals;
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

public class PriorityQueueListTest extends UnitTestBase
{
    private static final byte[] PRIORITIES = {4, 5, 5, 4};

    private PriorityQueueList _list;
    private QueueManagingVirtualHost<?> _virtualHost;
    private QueueEntry _priority4message1;
    private QueueEntry _priority4message2;
    private QueueEntry _priority5message1;
    private QueueEntry _priority5message2;

    @BeforeAll
    public void beforeAll() throws Exception
    {
        _virtualHost = BrokerTestHelper.createVirtualHost(getTestClassName(), this);
    }

    @BeforeEach
    public void setUp() throws Exception
    {
        final QueueEntry[] entries = new QueueEntry[PRIORITIES.length];
        final Map<String,Object> queueAttributes = Map.of(Queue.ID, randomUUID(),
                Queue.NAME, getTestName(),
                PriorityQueue.PRIORITIES, 10);
        final PriorityQueueImpl queue = new PriorityQueueImpl(queueAttributes, _virtualHost);
        queue.open();
        _list = queue.getEntries();

        for (int i = 0; i < PRIORITIES.length; i++)
        {
            final ServerMessage<?> message = mock(ServerMessage.class);
            final AMQMessageHeader header = mock(AMQMessageHeader.class);
            @SuppressWarnings({ "rawtypes", "unchecked" })
            final MessageReference<ServerMessage> ref = mock(MessageReference.class);

            when(message.getMessageHeader()).thenReturn(header);
            when(message.newReference()).thenReturn(ref);
            when(message.newReference(any(TransactionLogResource.class))).thenReturn(ref);
            when(ref.getMessage()).thenReturn(message);
            when(header.getPriority()).thenReturn(PRIORITIES[i]);

            entries[i] = _list.add(message, null);
        }

        _priority4message1 = entries[0];
        _priority4message2 = entries[3];
        _priority5message1 = entries[1];
        _priority5message2 = entries[2];
    }

    @Test
    public void testPriorityQueueEntryCompareToItself()
    {
        //check messages compare to themselves properly
        assertEquals(0, (long) _priority4message1.compareTo(_priority4message1),
                "message should compare 'equal' to itself");

        assertEquals(0, (long) _priority5message2.compareTo(_priority5message2),
                "message should compare 'equal' to itself");
    }

    @Test
    public void testPriorityQueueEntryCompareToSamePriority()
    {
        //check messages with the same priority are ordered properly
        assertEquals(-1, (long) _priority4message1.compareTo(_priority4message2),
                "first message should be 'earlier' than second message of the same priority");

        assertEquals(-1, (long) _priority5message1.compareTo(_priority5message2),
                "first message should be 'earlier' than second message of the same priority");

        //and in reverse
        assertEquals(1, (long) _priority4message2.compareTo(_priority4message1),
                "second message should be 'later' than first message of the same priority");

        assertEquals(1, (long) _priority5message2.compareTo(_priority5message1),
                "second message should be 'later' than first message of the same priority");
    }

    @Test
    public void testPriorityQueueEntryCompareToDifferentPriority()
    {
        //check messages with higher priority are ordered 'earlier' than those with lower priority
        assertEquals(-1, (long) _priority5message1.compareTo(_priority4message1),
                "first message with priority 5 should be 'earlier' than first message of priority 4");
        assertEquals(-1, (long) _priority5message1.compareTo(_priority4message2),
                "first message with priority 5 should be 'earlier' than second message of priority 4");

        assertEquals(-1, (long) _priority5message2.compareTo(_priority4message1),
                "second message with priority 5 should be 'earlier' than first message of priority 4");
        assertEquals(-1, (long) _priority5message2.compareTo(_priority4message2),
                "second message with priority 5 should be 'earlier' than second message of priority 4");

        //and in reverse
        assertEquals(1, (long) _priority4message1.compareTo(_priority5message1),
                "first message with priority 4 should be 'later' than first message of priority 5");
        assertEquals(1, (long) _priority4message1.compareTo(_priority5message2),
                "first message with priority 4 should be 'later' than second message of priority 5");

        assertEquals(1, (long) _priority4message2.compareTo(_priority5message1),
                "second message with priority 4 should be 'later' than first message of priority 5");
        assertEquals(1, (long) _priority4message2.compareTo(_priority5message2),
                "second message with priority 4 should be 'later' than second message of priority 5");
    }

    @Test
    public void testGetLeastSignificantOldestEntry()
    {
        assertEquals(_priority4message1, _list.getLeastSignificantOldestEntry(), "Unexpected last entry");

        final ServerMessage<?> message = mock(ServerMessage.class);
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        @SuppressWarnings({ "rawtypes", "unchecked" })
        final MessageReference<ServerMessage> ref = mock(MessageReference.class);

        when(message.getMessageHeader()).thenReturn(header);
        when(message.newReference()).thenReturn(ref);
        when(message.newReference(any(TransactionLogResource.class))).thenReturn(ref);
        when(ref.getMessage()).thenReturn(message);
        when(header.getPriority()).thenReturn((byte)3);

        final QueueEntry newEntry = _list.add(message, null);

        assertEquals(newEntry, _list.getLeastSignificantOldestEntry(), "Unexpected last entry");
    }
}
