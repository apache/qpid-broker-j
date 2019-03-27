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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

import junit.framework.AssertionFailedError;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.consumer.ConsumerOption;
import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.message.internal.InternalMessage;
import org.apache.qpid.server.message.internal.InternalMessageHeader;
import org.apache.qpid.server.message.internal.InternalMessageMetaData;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.store.MessageHandle;
import org.apache.qpid.server.store.StoredMessage;

public class PriorityQueueTest extends AbstractQueueTestBase
{

    @Before
    public void setUp() throws Exception
    {
        setArguments(Collections.singletonMap(PriorityQueue.PRIORITIES,(Object)3));
        super.setUp();
    }

    @Test
    public void testPriorityOrdering() throws Exception
    {

        // Enqueue messages in order
        PriorityQueue<?> queue = (PriorityQueue<?>) getQueue();
        queue.enqueue(createMessage(1L, (byte) 10), null, null);
        queue.enqueue(createMessage(2L, (byte) 4), null, null);
        queue.enqueue(createMessage(3L, (byte) 0), null, null);

        // Enqueue messages in reverse order
        queue.enqueue(createMessage(4L, (byte) 0), null, null);
        queue.enqueue(createMessage(5L, (byte) 4), null, null);
        queue.enqueue(createMessage(6L, (byte) 10), null, null);

        // Enqueue messages out of order
        queue.enqueue(createMessage(7L, (byte) 4), null, null);
        queue.enqueue(createMessage(8L, (byte) 10), null, null);
        queue.enqueue(createMessage(9L, (byte) 0), null, null);

        final List<MessageInstance> msgs = consumeMessages(queue);;
        try
        {
            assertEquals(1L, msgs.get(0).getMessage().getMessageNumber());
            assertEquals(6L, msgs.get(1).getMessage().getMessageNumber());
            assertEquals(8L, msgs.get(2).getMessage().getMessageNumber());

            assertEquals(2L, msgs.get(3).getMessage().getMessageNumber());
            assertEquals(5L, msgs.get(4).getMessage().getMessageNumber());
            assertEquals(7L, msgs.get(5).getMessage().getMessageNumber());

            assertEquals(3L, msgs.get(6).getMessage().getMessageNumber());
            assertEquals(4L, msgs.get(7).getMessage().getMessageNumber());
            assertEquals(9L, msgs.get(8).getMessage().getMessageNumber());
        }
        catch (AssertionFailedError afe)
        {
            // Show message order on failure.
            showMessageOrderOnFailure(msgs, afe);
        }

    }

    @Test
    public void changeMessagePriority() throws Exception
    {
        final PriorityQueue<?> queue = (PriorityQueue<?>) getQueue();
        final InternalMessage internalMessage1 = createInternalMessage((byte) 3, 0);
        final InternalMessage internalMessage2 = createInternalMessage((byte) 3, 1);
        final InternalMessage internalMessage3 = createInternalMessage((byte) 4, 2);
        queue.enqueue(internalMessage1, null, null);
        queue.enqueue(internalMessage2, null, null);
        queue.enqueue(internalMessage3, null, null);

        final long result = queue.reenqueueMessageForPriorityChange(internalMessage2.getMessageNumber(), (byte)5);
        assertEquals("Unexpected operation result", internalMessage3.getMessageNumber() + 1, result);

        final List<MessageInstance> msgs = consumeMessages(queue);
        try
        {
            assertEquals(internalMessage3.getMessageNumber() + 1, msgs.get(0).getMessage().getMessageNumber());
            assertEquals(internalMessage3.getMessageNumber(), msgs.get(1).getMessage().getMessageNumber());
            assertEquals(internalMessage1.getMessageNumber(), msgs.get(2).getMessage().getMessageNumber());
        }
        catch (AssertionFailedError afe)
        {
            showMessageOrderOnFailure(msgs, afe);
        }
    }

    @Test
    public void changeMessagePriorityForNonExistingMessageId() throws Exception
    {
        final PriorityQueue<?> queue = (PriorityQueue<?>) getQueue();
        final InternalMessage internalMessage1 = createInternalMessage((byte) 3, 0);
        final InternalMessage internalMessage2 = createInternalMessage((byte) 5, 1);
        final InternalMessage internalMessage3 = createInternalMessage((byte) 4, 2);
        queue.enqueue(internalMessage1, null, null);
        queue.enqueue(internalMessage2, null, null);
        queue.enqueue(internalMessage3, null, null);

        final long result = queue.reenqueueMessageForPriorityChange(internalMessage3.getMessageNumber() + 1, (byte)6);
        assertEquals("Unexpected operation result", -1, result);

        final List<MessageInstance> msgs = consumeMessages(queue);
        try
        {
            assertEquals(internalMessage2.getMessageNumber(), msgs.get(0).getMessage().getMessageNumber());
            assertEquals(internalMessage3.getMessageNumber(), msgs.get(1).getMessage().getMessageNumber());
            assertEquals(internalMessage1.getMessageNumber(), msgs.get(2).getMessage().getMessageNumber());
        }
        catch (AssertionFailedError afe)
        {
            showMessageOrderOnFailure(msgs, afe);
        }
    }

    @Test
    public void changeMessagesPriority() throws Exception
    {
        final PriorityQueue<?> queue = (PriorityQueue<?>) getQueue();
        final InternalMessage internalMessage1 = createInternalMessage((byte) 3, 0);
        final InternalMessage internalMessage2 = createInternalMessage((byte) 3, 1);
        final InternalMessage internalMessage3 = createInternalMessage((byte) 4, 2);
        queue.enqueue(internalMessage1, null, null);
        queue.enqueue(internalMessage2, null, null);
        queue.enqueue(internalMessage3, null, null);

        final List<Long> result = queue.reenqueueMessagesForPriorityChange("id in ('2','0')", (byte)5);
        assertEquals("Unexpected operation result", 2, result.size());

        final List<MessageInstance> msgs = consumeMessages(queue);
        try
        {
            assertEquals(internalMessage3.getMessageNumber() + 1, msgs.get(0).getMessage().getMessageNumber());
            assertEquals("2", msgs.get(0).getMessage().getMessageHeader().getHeader("id"));
            assertEquals(internalMessage3.getMessageNumber() + 2, msgs.get(1).getMessage().getMessageNumber());
            assertEquals("0", msgs.get(1).getMessage().getMessageHeader().getHeader("id"));
            assertEquals(internalMessage2.getMessageNumber(), msgs.get(2).getMessage().getMessageNumber());
            assertEquals("1", msgs.get(2).getMessage().getMessageHeader().getHeader("id"));
        }
        catch (AssertionFailedError afe)
        {
            showMessageOrderOnFailure(msgs, afe);
        }
    }

    @Test
    public void changeMessagesPriorityForNonExistingMessageId() throws Exception
    {
        final PriorityQueue<?> queue = (PriorityQueue<?>) getQueue();
        final InternalMessage internalMessage1 = createInternalMessage((byte) 3, 0);
        final InternalMessage internalMessage2 = createInternalMessage((byte) 4, 1);
        final InternalMessage internalMessage3 = createInternalMessage((byte) 3, 2);
        queue.enqueue(internalMessage1, null, null);
        queue.enqueue(internalMessage2, null, null);
        queue.enqueue(internalMessage3, null, null);

        final List<Long> result = queue.reenqueueMessagesForPriorityChange("id in ('3','2')", (byte)5);
        assertEquals("Unexpected operation result", 1, result.size());

        final List<MessageInstance> msgs = consumeMessages(queue);
        try
        {
            assertEquals(internalMessage3.getMessageNumber() + 1, msgs.get(0).getMessage().getMessageNumber());
            assertEquals("2", msgs.get(0).getMessage().getMessageHeader().getHeader("id"));
            assertEquals(internalMessage2.getMessageNumber(), msgs.get(1).getMessage().getMessageNumber());
            assertEquals("1", msgs.get(1).getMessage().getMessageHeader().getHeader("id"));
            assertEquals(internalMessage1.getMessageNumber(), msgs.get(2).getMessage().getMessageNumber());
            assertEquals("0", msgs.get(2).getMessage().getMessageHeader().getHeader("id"));
        }
        catch (AssertionFailedError afe)
        {
            showMessageOrderOnFailure(msgs, afe);
        }
    }

    private List<MessageInstance> consumeMessages(final Queue queue)
            throws Exception
    {
        queue.addConsumer(getConsumer(), null, null, "test", EnumSet.noneOf(ConsumerOption.class), 0);

        while(getConsumer().processPending());
        return getConsumer().getMessages();
    }

    private void showMessageOrderOnFailure(final List<MessageInstance> msgs, final AssertionFailedError afe)
    {
        int index = 1;
        for (MessageInstance qe : msgs)
        {
            System.err.println(index + ":" + qe.getMessage().getMessageNumber());
            index++;
        }

        throw afe;
    }

    private InternalMessage createInternalMessage(byte priority, int index)
    {
        final AMQMessageHeader messageHeader = mock(AMQMessageHeader.class);
        when(messageHeader.getPriority()).thenReturn(priority);
        when(messageHeader.getHeader("id")).thenReturn(String.valueOf(index));
        when(messageHeader.getHeaderNames()).thenReturn(Collections.singleton("id"));
        final InternalMessageHeader internalMessageHeader = new InternalMessageHeader(messageHeader);
        final InternalMessageMetaData metaData =  new InternalMessageMetaData(true, internalMessageHeader, 0);
        MessageHandle<InternalMessageMetaData> handle = getQueue().getVirtualHost().getMessageStore().addMessage(metaData);
        final StoredMessage<InternalMessageMetaData> storedMessage = handle.allContentAdded();
        return new InternalMessage(storedMessage, internalMessageHeader, null, getQueue().getName());
    }

    protected ServerMessage createMessage(Long id, byte i)
    {

        ServerMessage msg = super.createMessage(id);
        AMQMessageHeader hdr = msg.getMessageHeader();
        when(hdr.getPriority()).thenReturn(i);
        return msg;
    }

    @Override
    protected ServerMessage createMessage(Long id)
    {
        return createMessage(id, (byte) 0);
    }

}
