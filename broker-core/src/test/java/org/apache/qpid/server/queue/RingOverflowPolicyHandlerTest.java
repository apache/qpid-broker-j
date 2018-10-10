/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.qpid.server.queue;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.LogMessage;
import org.apache.qpid.server.logging.LogSubject;
import org.apache.qpid.server.logging.messages.QueueMessages;
import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.OverflowPolicy;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.queue.ProducerFlowControlOverflowPolicyHandlerTest.LogMessageMatcher;
import org.apache.qpid.test.utils.UnitTestBase;

public class RingOverflowPolicyHandlerTest extends UnitTestBase
{
    private RingOverflowPolicyHandler _ringOverflowPolicyHandler;
    private Queue<?> _queue;
    private EventLogger _eventLogger;
    private LogSubject _subject;

    @Before
    public void setUp() throws Exception
    {

        _eventLogger = mock(EventLogger.class);
        _subject = mock(LogSubject.class);

        _queue = mock(AbstractQueue.class);
        when(_queue.getMaximumQueueDepthBytes()).thenReturn(-1L);
        when(_queue.getMaximumQueueDepthMessages()).thenReturn(-1L);
        when(_queue.getOverflowPolicy()).thenReturn(OverflowPolicy.RING);
        when(_queue.getQueueDepthMessages()).thenReturn(0);
        when(_queue.getLogSubject()).thenReturn(_subject);

        _ringOverflowPolicyHandler = new RingOverflowPolicyHandler(_queue, _eventLogger);
    }

    @Test
    public void testCheckOverflowWhenOverfullBytes() throws Exception
    {
        QueueEntry lastEntry = createLastEntry();
        when(_queue.getLeastSignificantOldestEntry()).thenReturn(lastEntry, (QueueEntry) null);
        when(_queue.getQueueDepthBytes()).thenReturn(10L, 4L);
        when(_queue.getMaximumQueueDepthBytes()).thenReturn(5L);
        when(_queue.getQueueDepthMessages()).thenReturn(3, 1);

        _ringOverflowPolicyHandler.checkOverflow(null);

        verify(_queue).deleteEntry(lastEntry);
        LogMessage dropped = QueueMessages.DROPPED(1L, 4, 1, 5,-1);
        verify(_eventLogger).message(same(_subject), argThat(new LogMessageMatcher(dropped)));
        verifyNoMoreInteractions(_eventLogger);
    }

    @Test
    public void testCheckOverflowWhenOverfullMessages() throws Exception
    {
        QueueEntry lastEntry = createLastEntry();
        when(_queue.getLeastSignificantOldestEntry()).thenReturn(lastEntry, (QueueEntry) null);
        when(_queue.getQueueDepthMessages()).thenReturn(10, 5);
        when(_queue.getMaximumQueueDepthMessages()).thenReturn(5L);
        when(_queue.getQueueDepthBytes()).thenReturn(10L, 4L);

        _ringOverflowPolicyHandler.checkOverflow(null);

        verify((AbstractQueue<?>) _queue).deleteEntry(lastEntry);
        LogMessage dropped = QueueMessages.DROPPED(1, 4, 5, -1,5);
        verify(_eventLogger).message(same(_subject), argThat(new LogMessageMatcher(dropped)));
        verifyNoMoreInteractions(_eventLogger);
    }

    @Test
    public void testCheckOverflowWhenUnderfullBytes() throws Exception
    {
        when(_queue.getQueueDepthBytes()).thenReturn(5L);
        when(_queue.getMaximumQueueDepthBytes()).thenReturn(5L);
        when(_queue.getQueueDepthMessages()).thenReturn(3);

        _ringOverflowPolicyHandler.checkOverflow(null);

        verify(_queue, never()).deleteEntry(any(QueueEntry.class));
        verifyNoMoreInteractions(_eventLogger);
    }

    @Test
    public void testCheckOverflowWhenUnderfullMessages() throws Exception
    {
        when(_queue.getQueueDepthMessages()).thenReturn(5);
        when(_queue.getMaximumQueueDepthMessages()).thenReturn(5L);
        when(_queue.getQueueDepthBytes()).thenReturn(10L);

        _ringOverflowPolicyHandler.checkOverflow(null);

        verify(_queue, never()).deleteEntry(any(QueueEntry.class));
        verifyNoMoreInteractions(_eventLogger);
    }

    private QueueEntry createLastEntry()
    {
        AMQMessageHeader oldestMessageHeader = mock(AMQMessageHeader.class);
        ServerMessage oldestMessage = mock(ServerMessage.class);
        when(oldestMessage.getMessageHeader()).thenReturn(oldestMessageHeader);
        QueueEntry oldestEntry = mock(QueueEntry.class);
        when(oldestEntry.getMessage()).thenReturn(oldestMessage);
        return oldestEntry;
    }
}
