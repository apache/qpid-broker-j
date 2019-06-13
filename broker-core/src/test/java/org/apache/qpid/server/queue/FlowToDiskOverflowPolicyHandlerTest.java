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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.message.MessageReference;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.OverflowPolicy;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.test.utils.UnitTestBase;

public class FlowToDiskOverflowPolicyHandlerTest extends UnitTestBase
{
    private Queue<?> _queue;

    @Before
    public void setUp() throws Exception
    {
        BrokerTestHelper.setUp();

        VirtualHost<?> virtualHost = BrokerTestHelper.createVirtualHost(getClass().getName(), this);

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(Queue.NAME, "testQueue");
        attributes.put(Queue.OVERFLOW_POLICY, OverflowPolicy.FLOW_TO_DISK);

        _queue = (AbstractQueue<?>) virtualHost.createChild(Queue.class, attributes);
    }

    @Test
    public void testOverflowAfterLoweringLimit() throws Exception
    {
        ServerMessage<?> message = createMessage(10L);
        _queue.enqueue(message, null, null);
        StoredMessage<?> storedMessage = message.getStoredMessage();
        verify(storedMessage, never()).flowToDisk();

        ServerMessage<?> message2 = createMessage(10L);
        _queue.enqueue(message2, null, null);
        StoredMessage<?> storedMessage2 = message2.getStoredMessage();
        verify(storedMessage2, never()).flowToDisk();

        _queue.setAttributes(Collections.singletonMap(Queue.MAXIMUM_QUEUE_DEPTH_BYTES, 10));

        verify(storedMessage2).flowToDisk();
    }

    @Test
    public void testOverflowOnSecondMessage() throws Exception
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
    public void testBytesOverflow() throws Exception
    {
        _queue.setAttributes(Collections.singletonMap(Queue.MAXIMUM_QUEUE_DEPTH_BYTES, 0));
        ServerMessage<?> message = createMessage(1L);
        _queue.enqueue(message, null, null);
        StoredMessage<?> storedMessage = message.getStoredMessage();
        verify(storedMessage).flowToDisk();
    }

    @Test
    public void testMessagesOverflow() throws Exception
    {
        _queue.setAttributes(Collections.singletonMap(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES, 0));
        ServerMessage<?> message = createMessage(1L);
        _queue.enqueue(message, null, null);
        StoredMessage<?> storedMessage = message.getStoredMessage();
        verify(storedMessage).flowToDisk();
    }

    @Test
    public void testNoOverflow() throws Exception
    {
        _queue.setAttributes(Collections.singletonMap(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES, 10));
        _queue.setAttributes(Collections.singletonMap(Queue.MAXIMUM_QUEUE_DEPTH_BYTES, 10));
        ServerMessage<?> message = createMessage(1L);
        _queue.enqueue(message, null, null);
        StoredMessage<?> storedMessage = message.getStoredMessage();
        verify(storedMessage, never()).flowToDisk();
    }

    private ServerMessage createMessage(long size)
    {
        ServerMessage message = mock(ServerMessage.class);
        when(message.getSizeIncludingHeader()).thenReturn(size);
        when(message.checkValid()).thenReturn(true);
        when(message.getValidationStatus()).thenReturn(ServerMessage.ValidationStatus.VALID);

        StoredMessage storedMessage = mock(StoredMessage.class);
        when(message.getStoredMessage()).thenReturn(storedMessage);
        when(storedMessage.isInContentInMemory()).thenReturn(true);
        when(storedMessage.getInMemorySize()).thenReturn(size);

        MessageReference ref = mock(MessageReference.class);
        when(ref.getMessage()).thenReturn(message);

        when(message.newReference()).thenReturn(ref);
        when(message.newReference(any(TransactionLogResource.class))).thenReturn(ref);

        return message;
    }
}
