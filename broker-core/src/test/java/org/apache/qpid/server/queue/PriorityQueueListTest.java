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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.After;
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

public class PriorityQueueListTest extends UnitTestBase
{
    private static final byte[] PRIORITIES = {4, 5, 5, 4};
    PriorityQueueList _list;

    private QueueEntry _priority4message1;
    private QueueEntry _priority4message2;
    private QueueEntry _priority5message1;
    private QueueEntry _priority5message2;

    @Before
    public void setUp() throws Exception
    {
        BrokerTestHelper.setUp();
        QueueEntry[] entries = new QueueEntry[PRIORITIES.length];
        Map<String,Object> queueAttributes = new HashMap<String, Object>();
        queueAttributes.put(Queue.ID, UUID.randomUUID());
        queueAttributes.put(Queue.NAME, getTestName());
        queueAttributes.put(PriorityQueue.PRIORITIES, 10);
        final QueueManagingVirtualHost<?> virtualHost = BrokerTestHelper.createVirtualHost("testVH", this);
        PriorityQueueImpl queue = new PriorityQueueImpl(queueAttributes, virtualHost);
        queue.open();
        _list = queue.getEntries();

        for (int i = 0; i < PRIORITIES.length; i++)
        {
            ServerMessage<?> message = mock(ServerMessage.class);
            AMQMessageHeader header = mock(AMQMessageHeader.class);
            @SuppressWarnings({ "rawtypes", "unchecked" })
            MessageReference<ServerMessage> ref = mock(MessageReference.class);

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

    @After
    public void tearDown() throws Exception
    {
        try
        {
        }
        finally
        {
            BrokerTestHelper.tearDown();
        }
    }

    @Test
    public void testPriorityQueueEntryCompareToItself()
    {
        //check messages compare to themselves properly
        assertEquals("message should compare 'equal' to itself",
                            (long) 0,
                            (long) _priority4message1.compareTo(_priority4message1));

        assertEquals("message should compare 'equal' to itself",
                            (long) 0,
                            (long) _priority5message2.compareTo(_priority5message2));
    }

    @Test
    public void testPriorityQueueEntryCompareToSamePriority()
    {
        //check messages with the same priority are ordered properly
        assertEquals("first message should be 'earlier' than second message of the same priority",
                            (long) -1,
                            (long) _priority4message1.compareTo(_priority4message2));

        assertEquals("first message should be 'earlier' than second message of the same priority",
                            (long) -1,
                            (long) _priority5message1.compareTo(_priority5message2));

        //and in reverse
        assertEquals("second message should be 'later' than first message of the same priority",
                            (long) 1,
                            (long) _priority4message2.compareTo(_priority4message1));

        assertEquals("second message should be 'later' than first message of the same priority",
                            (long) 1,
                            (long) _priority5message2.compareTo(_priority5message1));
    }

    @Test
    public void testPriorityQueueEntryCompareToDifferentPriority()
    {
        //check messages with higher priority are ordered 'earlier' than those with lower priority
        assertEquals("first message with priority 5 should be 'earlier' than first message of priority 4",
                            (long) -1,
                            (long) _priority5message1.compareTo(_priority4message1));
        assertEquals("first message with priority 5 should be 'earlier' than second message of priority 4",
                            (long) -1,
                            (long) _priority5message1.compareTo(_priority4message2));

        assertEquals("second message with priority 5 should be 'earlier' than first message of priority 4",
                            (long) -1,
                            (long) _priority5message2.compareTo(_priority4message1));
        assertEquals("second message with priority 5 should be 'earlier' than second message of priority 4",
                            (long) -1,
                            (long) _priority5message2.compareTo(_priority4message2));

        //and in reverse
        assertEquals("first message with priority 4 should be 'later' than first message of priority 5",
                            (long) 1,
                            (long) _priority4message1.compareTo(_priority5message1));
        assertEquals("first message with priority 4 should be 'later' than second message of priority 5",
                            (long) 1,
                            (long) _priority4message1.compareTo(_priority5message2));

        assertEquals("second message with priority 4 should be 'later' than first message of priority 5",
                            (long) 1,
                            (long) _priority4message2.compareTo(_priority5message1));
        assertEquals("second message with priority 4 should be 'later' than second message of priority 5",
                            (long) 1,
                            (long) _priority4message2.compareTo(_priority5message2));
    }

    @Test
    public void testGetLeastSignificantOldestEntry()
    {
        assertEquals("Unexpected last entry", _priority4message1, _list.getLeastSignificantOldestEntry());

        ServerMessage<?> message = mock(ServerMessage.class);
        AMQMessageHeader header = mock(AMQMessageHeader.class);
        @SuppressWarnings({ "rawtypes", "unchecked" })
        MessageReference<ServerMessage> ref = mock(MessageReference.class);

        when(message.getMessageHeader()).thenReturn(header);
        when(message.newReference()).thenReturn(ref);
        when(message.newReference(any(TransactionLogResource.class))).thenReturn(ref);
        when(ref.getMessage()).thenReturn(message);
        when(header.getPriority()).thenReturn((byte)3);

        QueueEntry newEntry = _list.add(message, null);

        assertEquals("Unexpected last entry", newEntry, _list.getLeastSignificantOldestEntry());
    }
}
