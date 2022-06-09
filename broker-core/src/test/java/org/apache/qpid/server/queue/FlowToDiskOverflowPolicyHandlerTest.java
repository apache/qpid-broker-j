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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.MessageReference;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.OverflowPolicy;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.test.utils.UnitTestBase;

/**
 * Tests for FlowToDiskOverflowPolicyHandler.
 *
 * Here mockito verify() for flowToDisk() checks whether message was flowed to disk or not,
 * and mockito verify() for getContent() checks whether message was restored into memory or not
 */
public class FlowToDiskOverflowPolicyHandlerTest extends UnitTestBase
{
    private Queue<?> _queue;

    private Map<StoredMessage<?>, Boolean> _state;

    @Before
    public void setUp() throws Exception
    {
        BrokerTestHelper.setUp();

        VirtualHost<?> virtualHost = BrokerTestHelper.createVirtualHost(getClass().getName(), this);

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(Queue.NAME, "testQueue");
        attributes.put(Queue.OVERFLOW_POLICY, OverflowPolicy.FLOW_TO_DISK);

        _queue = (AbstractQueue<?>) virtualHost.createChild(Queue.class, attributes);
        _state = new HashMap<>();
    }

    /**
     * Lowers the overflow limit, forcing messages to be flowed to the disk
     */
    @Test
    public void overflowAfterLoweringLimit()
    {
        _queue.setAttributes(Collections.singletonMap(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES, 10));

        List<ServerMessage<?>> messages = new ArrayList<>();

        for (int i = 0; i < 15; i ++)
        {
            messages.add(createMessage(10L));
            _queue.enqueue(messages.get(i), null, null);
        }

        for (int i = 0; i < 10; i ++)
        {
            verify(messages.get(i).getStoredMessage(), never()).flowToDisk();
            verify(messages.get(i).getStoredMessage(), never()).getContent(anyInt(), anyInt());
        }

        for (int i = 10; i < 15; i ++)
        {
            verify(messages.get(i).getStoredMessage()).flowToDisk();
            verify(messages.get(i).getStoredMessage(), never()).getContent(anyInt(), anyInt());
        }

        _queue.setAttributes(Collections.singletonMap(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES, 5));

        for (int i = 0; i < 5; i ++)
        {
            verify(messages.get(i).getStoredMessage(), never()).flowToDisk();
            verify(messages.get(i).getStoredMessage(), never()).getContent(anyInt(), anyInt());
        }

        for (int i = 5; i < 10; i ++)
        {
            verify(messages.get(i).getStoredMessage()).flowToDisk();
            verify(messages.get(i).getStoredMessage(), never()).getContent(anyInt(), anyInt());
        }

        for (int i = 11; i < 15; i ++)
        {
            verify(messages.get(i).getStoredMessage()).flowToDisk();
            verify(messages.get(i).getStoredMessage(), never()).getContent(anyInt(), anyInt());
        }
    }

    /**
     * Rises the overflow limit, forcing messages to be restored in the memory from the flowed to disk state
     */
    @Test
    public void overflowAfterRisingLimit()
    {
        _queue.setAttributes(Collections.singletonMap(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES, 5));

        List<ServerMessage<?>> messages = new ArrayList<>();

        for (int i = 0; i < 15; i ++)
        {
            messages.add(createMessage(10L));
            _queue.enqueue(messages.get(i), null, null);
        }

        for (int i = 0; i < 5; i ++)
        {
            verify(messages.get(i).getStoredMessage(), never()).flowToDisk();
            verify(messages.get(i).getStoredMessage(), never()).getContent(anyInt(), anyInt());
        }

        for (int i = 5; i < 15; i ++)
        {
            verify(messages.get(i).getStoredMessage()).flowToDisk();
            verify(messages.get(i).getStoredMessage(), never()).getContent(anyInt(), anyInt());
        }

        _queue.setAttributes(Collections.singletonMap(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES, 10));

        // first five messages should be neither be flowed to the disk nor restored to memory (nothing changed to them)
        for (int i = 0; i < 5; i ++)
        {
            verify(messages.get(i).getStoredMessage(), never()).flowToDisk();
            verify(messages.get(i).getStoredMessage(), never()).getContent(anyInt(), anyInt());
        }

        // middle five messages should be restored to memory
        for (int i = 5; i < 10; i ++)
        {
            verify(messages.get(i).getStoredMessage()).flowToDisk();
            verify(messages.get(i).getStoredMessage()).getContent(anyInt(), anyInt());
        }

        // last five messages should remain flowed to the disk
        for (int i = 11; i < 15; i ++)
        {
            verify(messages.get(i).getStoredMessage()).flowToDisk();
            verify(messages.get(i).getStoredMessage(), never()).getContent(anyInt(), anyInt());
        }
    }

    @Test
    public void overflowOnSecondMessage()
    {
        _queue.setAttributes(Collections.singletonMap(Queue.MAXIMUM_QUEUE_DEPTH_BYTES, 10));
        ServerMessage<?> message = createMessage(10L);
        _queue.enqueue(message, null, null);
        StoredMessage<?> storedMessage = message.getStoredMessage();
        verify(storedMessage, never()).flowToDisk();

        ServerMessage<?> message2 = createMessage(10L);
        _queue.enqueue(message2, null, null);
        StoredMessage<?> storedMessage2 = message2.getStoredMessage();
        verify(storedMessage2).flowToDisk();
    }

    @Test
    public void bytesOverflow()
    {
        _queue.setAttributes(Collections.singletonMap(Queue.MAXIMUM_QUEUE_DEPTH_BYTES, 0));
        ServerMessage<?> message = createMessage(1L);
        _queue.enqueue(message, null, null);
        StoredMessage<?> storedMessage = message.getStoredMessage();
        verify(storedMessage).flowToDisk();
    }

    @Test
    public void messagesOverflow()
    {
        _queue.setAttributes(Collections.singletonMap(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES, 0));
        ServerMessage<?> message = createMessage(1L);
        _queue.enqueue(message, null, null);
        StoredMessage<?> storedMessage = message.getStoredMessage();
        verify(storedMessage).flowToDisk();
    }

    @Test
    public void noOverflow()
    {
        _queue.setAttributes(Collections.singletonMap(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES, 10));
        _queue.setAttributes(Collections.singletonMap(Queue.MAXIMUM_QUEUE_DEPTH_BYTES, 10));
        ServerMessage<?> message = createMessage(1L);
        _queue.enqueue(message, null, null);
        StoredMessage<?> storedMessage = message.getStoredMessage();
        verify(storedMessage, never()).flowToDisk();
    }

    @Test
    public void oneByOneDeletion()
    {
        _queue.setAttributes(Collections.singletonMap(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES, 5));

        List<ServerMessage<?>> messages = new ArrayList<>();

        for (int i = 0; i < 10; i ++)
        {
            messages.add(createMessage(10L));
            _queue.enqueue(messages.get(i), null, null);
        }

        for (int i = 0; i < 5; i ++)
        {
            verify(messages.get(i).getStoredMessage(), never()).flowToDisk();
        }

        for (int i = 5; i < 10; i ++)
        {
            verify(messages.get(i).getStoredMessage()).flowToDisk();
        }

        QueueEntryIterator it = _queue.queueEntryIterator();
        it.advance();
        _queue.deleteEntry(it.getNode());

        for (int i = 0; i < 5; i ++)
        {
            verify(messages.get(i).getStoredMessage(), never()).flowToDisk();
        }
        verify(messages.get(5).getStoredMessage()).getContent(anyInt(), anyInt());
        verify(messages.get(6).getStoredMessage(), never()).getContent(anyInt(), anyInt());
        verify(messages.get(7).getStoredMessage(), never()).getContent(anyInt(), anyInt());
        verify(messages.get(8).getStoredMessage(), never()).getContent(anyInt(), anyInt());
        verify(messages.get(9).getStoredMessage(), never()).getContent(anyInt(), anyInt());

        it.advance();
        _queue.deleteEntry(it.getNode());

        for (int i = 0; i < 5; i ++)
        {
            verify(messages.get(i).getStoredMessage(), never()).flowToDisk();
        }
        verify(messages.get(5).getStoredMessage()).getContent(anyInt(), anyInt());
        verify(messages.get(6).getStoredMessage()).getContent(anyInt(), anyInt());
        verify(messages.get(7).getStoredMessage(), never()).getContent(anyInt(), anyInt());
        verify(messages.get(8).getStoredMessage(), never()).getContent(anyInt(), anyInt());
        verify(messages.get(9).getStoredMessage(), never()).getContent(anyInt(), anyInt());

        it.advance();
        _queue.deleteEntry(it.getNode());

        for (int i = 0; i < 5; i ++)
        {
            verify(messages.get(i).getStoredMessage(), never()).flowToDisk();
        }
        verify(messages.get(5).getStoredMessage()).getContent(anyInt(), anyInt());
        verify(messages.get(6).getStoredMessage()).getContent(anyInt(), anyInt());
        verify(messages.get(7).getStoredMessage()).getContent(anyInt(), anyInt());
        verify(messages.get(8).getStoredMessage(), never()).getContent(anyInt(), anyInt());
        verify(messages.get(9).getStoredMessage(), never()).getContent(anyInt(), anyInt());

        it.advance();
        _queue.deleteEntry(it.getNode());

        for (int i = 0; i < 5; i ++)
        {
            verify(messages.get(i).getStoredMessage(), never()).flowToDisk();
        }
        verify(messages.get(5).getStoredMessage()).getContent(anyInt(), anyInt());
        verify(messages.get(6).getStoredMessage()).getContent(anyInt(), anyInt());
        verify(messages.get(7).getStoredMessage()).getContent(anyInt(), anyInt());
        verify(messages.get(8).getStoredMessage()).getContent(anyInt(), anyInt());
        verify(messages.get(9).getStoredMessage(), never()).getContent(anyInt(), anyInt());

        it.advance();
        _queue.deleteEntry(it.getNode());

        for (int i = 0; i < 5; i ++)
        {
            verify(messages.get(i).getStoredMessage(), never()).flowToDisk();
        }
        verify(messages.get(5).getStoredMessage()).getContent(anyInt(), anyInt());
        verify(messages.get(6).getStoredMessage()).getContent(anyInt(), anyInt());
        verify(messages.get(7).getStoredMessage()).getContent(anyInt(), anyInt());
        verify(messages.get(8).getStoredMessage()).getContent(anyInt(), anyInt());
        verify(messages.get(9).getStoredMessage()).getContent(anyInt(), anyInt());
    }

    @Test
    public void clearQueue()
    {
        _queue.setAttributes(Collections.singletonMap(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES, 5));

        List<ServerMessage<?>> messages = new ArrayList<>();

        for (int i = 0; i < 15; i ++)
        {
            messages.add(createMessage(10L));
            _queue.enqueue(messages.get(i), null, null);
        }
        assertEquals(15, _queue.getQueueDepthMessages());

        for (int i = 0; i < 5; i ++)
        {
            verify(messages.get(i).getStoredMessage(), never()).flowToDisk();
            verify(messages.get(i).getStoredMessage(), never()).getContent(anyInt(), anyInt());
        }

        for (int i = 5; i < 15; i ++)
        {
            verify(messages.get(i).getStoredMessage()).flowToDisk();
            verify(messages.get(i).getStoredMessage(), never()).getContent(anyInt(), anyInt());
        }

        long deleted = _queue.clearQueue();

        assertEquals(15, deleted);
        assertEquals(0, _queue.getQueueDepthMessages());

        for (int i = 0; i < 5; i ++)
        {
            verify(messages.get(i).getStoredMessage(), never()).flowToDisk();
            verify(messages.get(i).getStoredMessage(), never()).getContent(anyInt(), anyInt());
        }

        for (int i = 5; i < 15; i ++)
        {
            verify(messages.get(i).getStoredMessage()).flowToDisk();
            verify(messages.get(i).getStoredMessage()).getContent(anyInt(), anyInt());
        }
    }

    /**
     * Deletes messages 5-10 of 15 in the queue with the limit 5:
     *
     * o o o o o | _ _ _ _ _ | _ _ _ _ _
     * =>
     * o o o o o | x x x x x | _ _ _ _ _
     * =>
     * o o o o o | _ _ _ _ _
     */
    @Test
    public void deleteMessagesAfterLimit()
    {
        _queue.setAttributes(Collections.singletonMap(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES, 5));

        List<ServerMessage<?>> messages = new ArrayList<>();
        List<QueueEntry> entries = new ArrayList<>();

        for (int i = 0; i < 15; i ++)
        {
            messages.add(createMessage(10L));
            _queue.enqueue(messages.get(i), null, null);
        }
        assertEquals(15, _queue.getQueueDepthMessages());

        for (int i = 0; i < 5; i ++)
        {
            verify(messages.get(i).getStoredMessage(), never()).flowToDisk();
            verify(messages.get(i).getStoredMessage(), never()).getContent(anyInt(), anyInt());
        }

        for (int i = 5; i < 15; i ++)
        {
            verify(messages.get(i).getStoredMessage()).flowToDisk();
            verify(messages.get(i).getStoredMessage(), never()).getContent(anyInt(), anyInt());
        }

        QueueEntryIterator it = _queue.queueEntryIterator();
        while (it.advance())
        {
            entries.add(it.getNode());
        }

        for (int i = 5; i < 10; i ++)
        {
            _queue.deleteEntry(entries.get(i));
        }

        assertEquals(10, _queue.getQueueDepthMessages());

        // first 5 messages shouldn't be either flowed to disk or restored in memory, they remain without changes
        for (int i = 0; i < 5; i ++)
        {
            verify(messages.get(i).getStoredMessage(), never()).flowToDisk();
            verify(messages.get(i).getStoredMessage(), never()).getContent(anyInt(), anyInt());
        }

        // last 5 messages should be first flowed to disk but never restored in memory
        for (int i = 5; i < 10; i ++)
        {
            verify(messages.get(i).getStoredMessage()).flowToDisk();
            verify(messages.get(i).getStoredMessage(), never()).getContent(anyInt(), anyInt());
        }
    }

    /**
     * Deletes messages 3-7 of 15 in the queue with the limit 5:
     *
     * o o o o o | _ _ _ _ _ | _ _ _ _ _
     * =>
     * o o o x x | x x x _ _ | _ _ _ _ _
     * =>
     * o o o o o | _ _ _ _ _
     */
    @Test
    public void deleteMessagesAroundLimit()
    {
        _queue.setAttributes(Collections.singletonMap(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES, 5));

        List<ServerMessage<?>> messages = new ArrayList<>();
        List<QueueEntry> entries = new ArrayList<>();

        for (int i = 0; i < 15; i ++)
        {
            messages.add(createMessage(10L));
            _queue.enqueue(messages.get(i), null, null);
        }
        assertEquals(15, _queue.getQueueDepthMessages());

        for (int i = 0; i < 5; i ++)
        {
            verify(messages.get(i).getStoredMessage(), never()).flowToDisk();
            verify(messages.get(i).getStoredMessage(), never()).getContent(anyInt(), anyInt());
        }

        for (int i = 5; i < 15; i ++)
        {
            verify(messages.get(i).getStoredMessage()).flowToDisk();
            verify(messages.get(i).getStoredMessage(), never()).getContent(anyInt(), anyInt());
        }

        QueueEntryIterator it = _queue.queueEntryIterator();
        while (it.advance())
        {
            entries.add(it.getNode());
        }

        for (int i = 3; i < 8; i ++)
        {
            _queue.deleteEntry(entries.get(i));
        }

        assertEquals(10, _queue.getQueueDepthMessages());

        // first 5 messages shouldn't be either flowed to disk or restored in memory
        for (int i = 0; i < 5; i ++)
        {
            verify(messages.get(i).getStoredMessage(), never()).flowToDisk();
            verify(messages.get(i).getStoredMessage(), never()).getContent(anyInt(), anyInt());
        }

        // messages 5-10 should be first flowed to disk and restored in memory
        for (int i = 5; i < 10; i ++)
        {
            verify(messages.get(i).getStoredMessage()).flowToDisk();
            verify(messages.get(i).getStoredMessage()).getContent(anyInt(), anyInt());
        }

        // messages 5-10 should be flowed to disk and never restored in memory
        for (int i = 11; i < 15; i ++)
        {
            verify(messages.get(i).getStoredMessage()).flowToDisk();
            verify(messages.get(i).getStoredMessage(), never()).getContent(anyInt(), anyInt());
        }
    }

    @SuppressWarnings("rawtypes")
    private ServerMessage createMessage(long size)
    {
        ServerMessage message = mock(ServerMessage.class);
        when(message.getSizeIncludingHeader()).thenReturn(size);
        when(message.checkValid()).thenReturn(true);
        when(message.getValidationStatus()).thenReturn(ServerMessage.ValidationStatus.VALID);

        StoredMessage storedMessage = mock(StoredMessage.class);
        _state.put(storedMessage, true);
        when(message.getStoredMessage()).thenReturn(storedMessage);
        when(storedMessage.isInContentInMemory()).thenAnswer(invocation -> _state.get(message.getStoredMessage()));
        when(storedMessage.getInMemorySize()).thenReturn(size);
        when(storedMessage.flowToDisk()).thenAnswer(invocation ->
        {
            StoredMessage sm = (StoredMessage) invocation.getMock();
            _state.put(sm, false);
            return true;
        });
        when(storedMessage.getContent(anyInt(), anyInt())).thenAnswer(invocation ->
        {
            StoredMessage sm = (StoredMessage) invocation.getMock();
            _state.put(sm, true);
            return QpidByteBuffer.allocate((int)size);
        });

        MessageReference ref = mock(MessageReference.class);
        when(ref.getMessage()).thenReturn(message);

        when(message.newReference()).thenReturn(ref);
        when(message.newReference(any(TransactionLogResource.class))).thenReturn(ref);

        return message;
    }
}
