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

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.Before;

import org.apache.qpid.server.message.MessageReference;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
public class SimpleQueueEntryImplTest extends QueueEntryImplTestBase
{

    private OrderedQueueEntryList queueEntryList;

    @Before
    public void setUp() throws Exception
    {
        Map<String,Object> queueAttributes = new HashMap<String, Object>();
        queueAttributes.put(Queue.ID, UUID.randomUUID());
        queueAttributes.put(Queue.NAME, "SimpleQueueEntryImplTest");
        final QueueManagingVirtualHost virtualHost = BrokerTestHelper.createVirtualHost("testVH", this);
        StandardQueueImpl queue = new StandardQueueImpl(queueAttributes, virtualHost);
        queue.open();
        queueEntryList = queue.getEntries();

        super.setUp();
    }


    @Override
    public QueueEntryImpl getQueueEntryImpl(int msgId)
    {
        ServerMessage message = mock(ServerMessage.class);
        when(message.getMessageNumber()).thenReturn((long)msgId);
        when(message.checkValid()).thenReturn(true);
        final MessageReference reference = mock(MessageReference.class);
        when(reference.getMessage()).thenReturn(message);
        when(message.newReference()).thenReturn(reference);
        when(message.newReference(any(TransactionLogResource.class))).thenReturn(reference);
        return (QueueEntryImpl) queueEntryList.add(message, null);
    }

    @Override
    public void testCompareTo()
    {
        assertTrue(_queueEntry.compareTo(_queueEntry2) < 0);
        assertTrue(_queueEntry2.compareTo(_queueEntry3) < 0);
        assertTrue(_queueEntry.compareTo(_queueEntry3) < 0);

        assertTrue(_queueEntry2.compareTo(_queueEntry) > 0);
        assertTrue(_queueEntry3.compareTo(_queueEntry2) > 0);
        assertTrue(_queueEntry3.compareTo(_queueEntry) > 0);

        assertTrue(_queueEntry.compareTo(_queueEntry) == 0);
        assertTrue(_queueEntry2.compareTo(_queueEntry2) == 0);
        assertTrue(_queueEntry3.compareTo(_queueEntry3) == 0);
    }

    @Override
    public void testTraverseWithNoDeletedEntries()
    {
        QueueEntry current = _queueEntry;

        current = current.getNextValidEntry();
        assertSame("Unexpected current entry", _queueEntry2, current);

        current = current.getNextValidEntry();
        assertSame("Unexpected current entry", _queueEntry3, current);

        current = current.getNextValidEntry();
        assertNull(current);
    }

    @Override
    public void testTraverseWithDeletedEntries()
    {
        // Delete 2nd queue entry
        _queueEntry2.acquire();
        _queueEntry2.delete();
        assertTrue(_queueEntry2.isDeleted());

        QueueEntry current = _queueEntry;

        current = current.getNextValidEntry();
        assertSame("Unexpected current entry", _queueEntry3, current);

        current = current.getNextValidEntry();
        assertNull(current);
    }
}
