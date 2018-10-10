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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.security.PrivilegedAction;
import java.util.Collections;

import javax.security.auth.Subject;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

import org.apache.qpid.server.connection.SessionPrincipal;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.LogMessage;
import org.apache.qpid.server.logging.LogSubject;
import org.apache.qpid.server.logging.messages.QueueMessages;
import org.apache.qpid.server.model.OverflowPolicy;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.session.AMQPSession;
import org.apache.qpid.test.utils.UnitTestBase;

public class ProducerFlowControlOverflowPolicyHandlerTest extends UnitTestBase
{
    private ProducerFlowControlOverflowPolicyHandler _producerFlowControlOverflowPolicyHandler;
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
        when(_queue.getOverflowPolicy()).thenReturn(OverflowPolicy.PRODUCER_FLOW_CONTROL);
        when(_queue.getContextValue(Double.class, Queue.QUEUE_FLOW_RESUME_LIMIT)).thenReturn(80.0);
        when(_queue.getQueueDepthBytes()).thenReturn(0L);
        when(_queue.getQueueDepthMessages()).thenReturn(0);
        when(_queue.getLogSubject()).thenReturn(_subject);

        _producerFlowControlOverflowPolicyHandler = new ProducerFlowControlOverflowPolicyHandler(_queue, _eventLogger);
    }

    @Test
    public void testCheckOverflowBlocksSessionWhenOverfullBytes() throws Exception
    {
        AMQPSession<?, ?> session = mock(AMQPSession.class);
        when(_queue.getQueueDepthBytes()).thenReturn(11L);
        when(_queue.getMaximumQueueDepthBytes()).thenReturn(10L);

        checkOverflow(session);

        verify(session, times(1)).block(_queue);
        LogMessage logMessage = QueueMessages.OVERFULL(11, 10, 0, -1);
        verify(_eventLogger).message(same(_subject), argThat(new LogMessageMatcher(logMessage)));
        verifyNoMoreInteractions(_eventLogger);
        verifyNoMoreInteractions(session);
    }

    @Test
    public void testCheckOverflowBlocksSessionWhenOverfullMessages() throws Exception
    {
        AMQPSession<?, ?> session = mock(AMQPSession.class);
        when(_queue.getMaximumQueueDepthMessages()).thenReturn(10L);
        when(_queue.getQueueDepthMessages()).thenReturn(11);

        checkOverflow(session);

        verify(session, times(1)).block(_queue);
        LogMessage logMessage = QueueMessages.OVERFULL(0, -1, 11, 10);
        verify(_eventLogger).message(same(_subject), argThat(new LogMessageMatcher(logMessage)));
        verifyNoMoreInteractions(_eventLogger);
        verifyNoMoreInteractions(session);
    }

    @Test
    public void testIsQueueFlowStopped() throws Exception
    {
        assertFalse("Flow should not be stopped", _producerFlowControlOverflowPolicyHandler.isQueueFlowStopped());


        when(_queue.getQueueDepthBytes()).thenReturn(11L);
        when(_queue.getMaximumQueueDepthBytes()).thenReturn(10L);

        checkOverflow(mock(AMQPSession.class));

        assertTrue("Flow should be stopped", _producerFlowControlOverflowPolicyHandler.isQueueFlowStopped());
    }

    @Test
    public void testCheckOverflowResumesFlowWhenUnderfullBytes() throws Exception
    {
        AMQPSession<?, ?> session = mock(AMQPSession.class);
        when(_queue.getQueueDepthBytes()).thenReturn(11L);
        when(_queue.getMaximumQueueDepthBytes()).thenReturn(10L);

        checkOverflow(session);

        verify(session, times(1)).block(_queue);
        LogMessage overfullMessage = QueueMessages.OVERFULL(11, 10, 0, -1);
        verify(_eventLogger).message(same(_subject), argThat(new LogMessageMatcher(overfullMessage)));
        assertTrue("Flow should be stopped", _producerFlowControlOverflowPolicyHandler.isQueueFlowStopped());

        when(_queue.getQueueDepthBytes()).thenReturn(8L);

        _producerFlowControlOverflowPolicyHandler.checkOverflow(null);

        verify(session, times(1)).unblock(_queue);
        assertFalse("Flow should not be stopped", _producerFlowControlOverflowPolicyHandler.isQueueFlowStopped());
        LogMessage underfullMessage = QueueMessages.UNDERFULL(8, 8, 0, -1);
        verify(_eventLogger).message(same(_subject), argThat(new LogMessageMatcher(underfullMessage)));
        verifyNoMoreInteractions(_eventLogger);
        verifyNoMoreInteractions(session);
    }

    @Test
    public void testCheckOverflowResumesFlowWhenUnderfullMessages() throws Exception
    {
        AMQPSession<?, ?> session = mock(AMQPSession.class);
        when(_queue.getQueueDepthMessages()).thenReturn(11);
        when(_queue.getMaximumQueueDepthMessages()).thenReturn(10L);

        checkOverflow(session);

        verify(session, times(1)).block(_queue);
        LogMessage overfullMessage = QueueMessages.OVERFULL(0, -1, 11, 10);
        verify(_eventLogger).message(same(_subject), argThat(new LogMessageMatcher(overfullMessage)));
        assertTrue("Flow should be stopped", _producerFlowControlOverflowPolicyHandler.isQueueFlowStopped());

        when(_queue.getQueueDepthMessages()).thenReturn(8);

        _producerFlowControlOverflowPolicyHandler.checkOverflow(null);

        verify(session, times(1)).unblock(_queue);
        assertFalse("Flow should not be stopped", _producerFlowControlOverflowPolicyHandler.isQueueFlowStopped());
        LogMessage underfullMessage = QueueMessages.UNDERFULL(0, -1, 8, 8);
        verify(_eventLogger).message(same(_subject), argThat(new LogMessageMatcher(underfullMessage)));
        verifyNoMoreInteractions(_eventLogger);
        verifyNoMoreInteractions(session);
    }

    private void checkOverflow(AMQPSession<?, ?> session)
    {
        Subject subject = createSubject(session);
        Subject.doAs(subject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                _producerFlowControlOverflowPolicyHandler.checkOverflow(null);
                return null;
            }
        });
    }

    private Subject createSubject(final AMQPSession<?, ?> session)
    {
        SessionPrincipal sessionPrincipal = new SessionPrincipal(session);
        return new Subject(true,
                           Collections.singleton(sessionPrincipal),
                           Collections.EMPTY_SET,
                           Collections.EMPTY_SET);
    }

    public static class LogMessageMatcher implements ArgumentMatcher<LogMessage>
    {
        private final LogMessage _expected;

        LogMessageMatcher(final LogMessage expected)
        {
            this._expected = expected;
        }

        @Override
        public boolean matches(final LogMessage argument)
        {
            return _expected.toString().equals((argument).toString());
        }
    }
}
