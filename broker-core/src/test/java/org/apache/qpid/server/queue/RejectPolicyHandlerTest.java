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

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.OverflowPolicy;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.test.utils.UnitTestBase;

public class RejectPolicyHandlerTest extends UnitTestBase
{
    private RejectPolicyHandler _rejectOverflowPolicyHandler;
    private Queue<?> _queue;

    @Before
    public void setUp() throws Exception
    {

        _queue = mock(Queue.class);
        when(_queue.getName()).thenReturn("testQueue");
        when(_queue.getMaximumQueueDepthBytes()).thenReturn(-1L);
        when(_queue.getMaximumQueueDepthMessages()).thenReturn(-1L);
        when(_queue.getOverflowPolicy()).thenReturn(OverflowPolicy.REJECT);
        when(_queue.getQueueDepthMessages()).thenReturn(0);
        when(_queue.getVirtualHost()).thenReturn(mock(QueueManagingVirtualHost.class));

        _rejectOverflowPolicyHandler = new RejectPolicyHandler(_queue);
    }

    @Test
    public void testOverfullBytes() throws Exception
    {
        ServerMessage incomingMessage = createIncomingMessage(6);
        when(_queue.getQueueDepthBytes()).thenReturn(5L);
        when(_queue.getMaximumQueueDepthBytes()).thenReturn(10L);
        when(_queue.getQueueDepthMessages()).thenReturn(1);

        try
        {
            _rejectOverflowPolicyHandler.checkReject(incomingMessage);
            fail("Exception expected");
        }
        catch (MessageUnacceptableException e)
        {
            // pass
        }
    }

    @Test
    public void testOverfullMessages() throws Exception
    {
        ServerMessage incomingMessage = createIncomingMessage(5);
        when(_queue.getMaximumQueueDepthMessages()).thenReturn(7L);
        when(_queue.getQueueDepthMessages()).thenReturn(7);
        when(_queue.getQueueDepthBytes()).thenReturn(10L);

        try
        {
            _rejectOverflowPolicyHandler.checkReject(incomingMessage);
            fail("Exception expected");
        }
        catch (MessageUnacceptableException e)
        {
            // pass
        }
    }

    @Test
    public void testNotOverfullMessages() throws Exception
    {
        when(_queue.getMaximumQueueDepthMessages()).thenReturn(1L);

        ServerMessage incomingMessage1 = createIncomingMessage(2);
        MessageInstance messageInstance1 = mock(MessageInstance.class);
        when(messageInstance1.getMessage()).thenReturn(incomingMessage1);

        ServerMessage incomingMessage2 = createIncomingMessage(2);

        _rejectOverflowPolicyHandler.checkReject(incomingMessage1);
        _rejectOverflowPolicyHandler.postEnqueue(messageInstance1);

        _rejectOverflowPolicyHandler.checkReject(incomingMessage2);
   }
    @Test
    public void testNotOverfullBytes() throws Exception
    {
        when(_queue.getMaximumQueueDepthBytes()).thenReturn(9L);
        ServerMessage incomingMessage1 = createIncomingMessage(5);
        MessageInstance messageInstance1 = mock(MessageInstance.class);
        when(messageInstance1.getMessage()).thenReturn(incomingMessage1);

        ServerMessage incomingMessage2 = createIncomingMessage(5);

        _rejectOverflowPolicyHandler.checkReject(incomingMessage1);
        _rejectOverflowPolicyHandler.postEnqueue(messageInstance1);

        _rejectOverflowPolicyHandler.checkReject(incomingMessage2);
    }

    @Test
    public void testIncomingMessageDeleted() throws Exception
    {
        when(_queue.getMaximumQueueDepthMessages()).thenReturn(1L);

        ServerMessage incomingMessage1 = createIncomingMessage(2);

        ServerMessage incomingMessage2 = createIncomingMessage(2);

        _rejectOverflowPolicyHandler.checkReject(incomingMessage1);
        _rejectOverflowPolicyHandler.messageDeleted(incomingMessage1.getStoredMessage());

        _rejectOverflowPolicyHandler.checkReject(incomingMessage2);
    }

    private ServerMessage createIncomingMessage(final long size)
    {
        AMQMessageHeader incomingMessageHeader = mock(AMQMessageHeader.class);
        ServerMessage incomingMessage = mock(ServerMessage.class);
        when(incomingMessage.getMessageHeader()).thenReturn(incomingMessageHeader);
        when(incomingMessage.getSizeIncludingHeader()).thenReturn(size);
        when(incomingMessage.getStoredMessage()).thenReturn(mock(StoredMessage.class));
        return incomingMessage;
    }
}
