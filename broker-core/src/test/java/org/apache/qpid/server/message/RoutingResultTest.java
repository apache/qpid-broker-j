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

package org.apache.qpid.server.message;


import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;

import org.apache.qpid.server.queue.BaseQueue;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.test.utils.QpidTestCase;

public class RoutingResultTest extends QpidTestCase
{
    public void testSendToNotAcceptableQueue() throws Exception
    {
        ServerMessage<? extends StorableMessageMetaData> serverMessage = mock(ServerMessage.class);
        RoutingResult routingResult = new RoutingResult<>(serverMessage);

        BaseQueue notAcceptableQueue = mock(BaseQueue.class);
        routingResult.addQueue(notAcceptableQueue);

        when(serverMessage.isResourceAcceptable(notAcceptableQueue)).thenReturn(false);
        when(serverMessage.isReferenced(notAcceptableQueue)).thenReturn(false);
        ServerTransaction tx = mock(ServerTransaction.class);
        int queuesNumber = routingResult.send(tx, null);

        assertEquals("Unexpected enqueue number", 0, queuesNumber);

        verifyNoMoreInteractions(tx);
    }

    public void testSendToReferencedQueue() throws Exception
    {
        ServerMessage<? extends StorableMessageMetaData> serverMessage = mock(ServerMessage.class);
        RoutingResult routingResult = new RoutingResult<>(serverMessage);

        BaseQueue referencedQueue = mock(BaseQueue.class);
        routingResult.addQueue(referencedQueue);

        when(serverMessage.isResourceAcceptable(referencedQueue)).thenReturn(true);
        when(serverMessage.isReferenced(referencedQueue)).thenReturn(true);
        ServerTransaction tx = mock(ServerTransaction.class);
        int queuesNumber = routingResult.send(tx, null);

        assertEquals("Unexpected enqueue number", 0, queuesNumber);

        verifyNoMoreInteractions(tx);
    }

    public void testSendToMultiple() throws Exception
    {
        ServerMessage<? extends StorableMessageMetaData> serverMessage = mock(ServerMessage.class);
        BaseQueue referencedQueue = mock(BaseQueue.class);
        BaseQueue notAcceptableQueue = mock(BaseQueue.class);
        BaseQueue queue = mock(BaseQueue.class);
        when(queue.toString()).thenReturn("queue");
        when(notAcceptableQueue.toString()).thenReturn("notAcceptableQueue");
        when(referencedQueue.toString()).thenReturn("referencedQueue");

        when(serverMessage.isResourceAcceptable(notAcceptableQueue)).thenReturn(false);
        when(serverMessage.isResourceAcceptable(referencedQueue)).thenReturn(true);
        when(serverMessage.isResourceAcceptable(queue)).thenReturn(true);
        when(serverMessage.isReferenced(notAcceptableQueue)).thenReturn(false);
        when(serverMessage.isReferenced(referencedQueue)).thenReturn(true);
        when(serverMessage.isReferenced(queue)).thenReturn(false);

        RoutingResult routingResult = new RoutingResult<>(serverMessage);
        routingResult.addQueue(referencedQueue);
        routingResult.addQueue(notAcceptableQueue);
        routingResult.addQueue(queue);

        ServerTransaction tx = mock(ServerTransaction.class);

        int queuesNumber = routingResult.send(tx, null);

        assertEquals("Unexpected enqueue number", 1, queuesNumber);
        QueueCollectionMatcher queueCollectionMatcher = new QueueCollectionMatcher(Collections.singleton(queue));
        verify(tx).enqueue(argThat(queueCollectionMatcher), same(serverMessage), any(ServerTransaction.EnqueueAction.class));
    }

    private class QueueCollectionMatcher extends BaseMatcher<Collection<BaseQueue>>
    {
        private final Set<? extends BaseQueue> _expected;
        private Object _actual;

        private QueueCollectionMatcher(Collection<? extends BaseQueue> expected)
        {
            _expected = new HashSet<>(expected);
        }

        @Override
        public void describeTo(final Description description)
        {
            description.appendText("Expected '");
            description.appendText(String.valueOf(_expected));
            description.appendText("' but got '");
            description.appendText(String.valueOf(_actual));
            description.appendText("'");
        }

        @Override
        public boolean matches(final Object item)
        {
            _actual = item;
            return item instanceof Collection && new HashSet<>((Collection<? extends BaseQueue>)item).equals(_expected);
        }
    }
}