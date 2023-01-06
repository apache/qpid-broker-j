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
 */
package org.apache.qpid.server.queue;

import static org.apache.qpid.server.message.MessageInstance.NON_CONSUMER_ACQUIRED_STATE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.message.MessageInstance.EntryState;
import org.apache.qpid.server.message.MessageInstance.StealableConsumerAcquiredState;
import org.apache.qpid.server.message.MessageInstance.UnstealableConsumerAcquiredState;
import org.apache.qpid.server.message.MessageReference;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.AlternateBinding;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.util.StateChangeListener;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.test.utils.UnitTestBase;

/**
 * Tests for {@link QueueEntryImpl}
 */
public abstract class QueueEntryImplTestBase extends UnitTestBase
{
    // tested entry
    protected QueueEntryImpl _queueEntry;
    protected QueueEntryImpl _queueEntry2;
    protected QueueEntryImpl _queueEntry3;
    protected QueueManagingVirtualHost<?> _virtualHost;
    private long _consumerId;

    @BeforeAll
    public void beforeAll() throws Exception
    {
        _virtualHost = BrokerTestHelper.createVirtualHost(getTestClassName(), this);
    }

    public abstract QueueEntryImpl getQueueEntryImpl(int msgId);

    @Test
    public abstract void testCompareTo();

    @Test
    public abstract void testTraverseWithNoDeletedEntries();

    @Test
    public abstract void testTraverseWithDeletedEntries();

    @BeforeEach
    public void setUp() throws Exception
    {
        _queueEntry = getQueueEntryImpl(1);
        _queueEntry2 = getQueueEntryImpl(2);
        _queueEntry3 = getQueueEntryImpl(3);
    }

    @Test
    public void testAcquire()
    {
        assertTrue(_queueEntry.isAvailable(),
                "Queue entry should be in AVAILABLE state before invoking of acquire method");

        acquire();
    }

    @Test
    public void testDelete()
    {
        delete();
    }

    /**
     * Tests release method for entry in acquired state.
     * <p>
     * Entry in state ACQUIRED should be released and its status should be
     * changed to AVAILABLE.
     */
    @Test
    public void testReleaseAcquired()
    {
        acquire();
        _queueEntry.release();
        assertTrue(_queueEntry.isAvailable(),
                "Queue entry should be in AVAILABLE state after invoking of release method");
    }

    /**
     * Tests release method for entry in deleted state.
     * <p>
     * Invoking release on deleted entry should not have any effect on its
     * state.
     */
    @Test
    public void testReleaseDeleted()
    {
        delete();
        _queueEntry.release();
        assertTrue(_queueEntry.isDeleted(),
                "Invoking of release on entry in DELETED state should not have any effect");
    }

    /**
     * A helper method to put tested object into deleted state and assert the state
     */
    private void delete()
    {
        _queueEntry.acquire();
        _queueEntry.delete();
        assertTrue(_queueEntry.isDeleted(),
                "Queue entry should be in DELETED state after invoking of delete method");
    }


    /**
     * A helper method to put tested entry into acquired state and assert the sate
     */
    private void acquire()
    {
        _queueEntry.acquire(newConsumer());
        assertTrue(_queueEntry.isAcquired(),
                "Queue entry should be in ACQUIRED state after invoking of acquire method");
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private QueueConsumer<?, ?> newConsumer()
    {
        final QueueConsumer<?, ?> consumer = mock(QueueConsumer.class);
        final StealableConsumerAcquiredState owningState = new StealableConsumerAcquiredState<>(consumer);
        when(consumer.getOwningState()).thenReturn(owningState);
        final Long consumerNum = _consumerId++;
        when(consumer.getConsumerNumber()).thenReturn(consumerNum);
        when(consumer.getIdentifier()).thenReturn(consumerNum);
        return consumer;
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testStateChanges()
    {
        final QueueConsumer<?, ?> consumer = newConsumer();
        final StateChangeListener<MessageInstance, EntryState> stateChangeListener = mock(StateChangeListener.class);
        _queueEntry.addStateChangeListener(stateChangeListener);
        _queueEntry.acquire(consumer);
        verify(stateChangeListener).stateChanged(eq(_queueEntry), eq(MessageInstance.AVAILABLE_STATE),
                isA(UnstealableConsumerAcquiredState.class));
        _queueEntry.makeAcquisitionStealable();
        verify(stateChangeListener).stateChanged(eq(_queueEntry), isA(UnstealableConsumerAcquiredState.class),
                isA(StealableConsumerAcquiredState.class));
        _queueEntry.removeAcquisitionFromConsumer(consumer);
        verify(stateChangeListener).stateChanged(eq(_queueEntry), isA(StealableConsumerAcquiredState.class),
                eq(NON_CONSUMER_ACQUIRED_STATE));
    }

    @Test
    public void testLocking()
    {
        final QueueConsumer<?, ?> consumer = newConsumer();
        final QueueConsumer<?, ?> consumer2 = newConsumer();

        _queueEntry.acquire(consumer);
        assertTrue(_queueEntry.isAcquired(),
                "Queue entry should be in ACQUIRED state after invoking of acquire method");

        assertFalse(_queueEntry.removeAcquisitionFromConsumer(consumer),
                "Acquisition should initially be locked");

        assertTrue(_queueEntry.makeAcquisitionStealable(), "Should be able to unlock locked queue entry");
        assertFalse(_queueEntry.removeAcquisitionFromConsumer(consumer2),
                "Acquisition should not be able to be removed from the wrong consumer");
        assertTrue(_queueEntry.removeAcquisitionFromConsumer(consumer),
                "Acquisition should be able to be removed once unlocked");
        assertTrue(_queueEntry.isAcquired(), "Queue Entry should still be acquired");
        assertFalse(_queueEntry.acquiredByConsumer(),
                "Queue Entry should not be marked as acquired by a consumer");

        _queueEntry.release();

        assertFalse(_queueEntry.isAcquired(), "Hijacked queue entry should be able to be released");

        _queueEntry.acquire(consumer);
        assertTrue(_queueEntry.isAcquired(), "Queue entry should be in ACQUIRED state after invoking of acquire method");

        assertFalse(_queueEntry.removeAcquisitionFromConsumer(consumer),
                "Acquisition should initially be locked");
        assertTrue(_queueEntry.makeAcquisitionStealable(), "Should be able to unlock locked queue entry");
        assertTrue(_queueEntry.makeAcquisitionUnstealable(consumer), "Should be able to lock queue entry");
        assertFalse(_queueEntry.removeAcquisitionFromConsumer(consumer),
                "Acquisition should not be able to be hijacked when locked");

        _queueEntry.delete();
        assertTrue(_queueEntry.isDeleted(), "Locked queue entry should be able to be deleted");
    }

    @Test
    public void testLockAcquisitionOwnership()
    {
        final QueueConsumer<?, ?> consumer1 = newConsumer();
        final QueueConsumer<?, ?> consumer2 = newConsumer();

        _queueEntry.acquire(consumer1);
        assertTrue(_queueEntry.acquiredByConsumer(), "Queue entry should be acquired by consumer1");

        assertTrue(_queueEntry.makeAcquisitionUnstealable(consumer1), "Consumer1 relocking should be allowed");
        assertFalse(_queueEntry.makeAcquisitionUnstealable(consumer2), "Consumer2 should not be allowed");

        _queueEntry.makeAcquisitionStealable();

        assertTrue(_queueEntry.acquiredByConsumer(), "Queue entry should still be acquired by consumer1");

        _queueEntry.release(consumer1);

        assertFalse(_queueEntry.acquiredByConsumer(), "Queue entry should no longer be acquired by consumer1");
    }

    /**
     * Tests rejecting a queue entry records the Consumer ID
     * for later verification by isRejectedBy(consumerId).
     */
    @Test
    public void testRejectAndRejectedBy()
    {
        final QueueConsumer<?, ?> sub = newConsumer();

        assertFalse(_queueEntry.isRejectedBy(sub),
                "Queue entry should not yet have been rejected by the consumer");
        assertFalse(_queueEntry.isAcquired(), "Queue entry should not yet have been acquired by a consumer");

        //acquire, reject, and release the message using the consumer
        assertTrue(_queueEntry.acquire(sub), "Queue entry should have been able to be acquired");
        _queueEntry.reject(sub);
        _queueEntry.release();

        //verify the rejection is recorded
        assertTrue(_queueEntry.isRejectedBy(sub), "Queue entry should have been rejected by the consumer");

        //repeat rejection using a second consumer
        final QueueConsumer<?, ?> sub2 = newConsumer();

        assertFalse(_queueEntry.isRejectedBy(sub2),
                "Queue entry should not yet have been rejected by the consumer");
        assertTrue(_queueEntry.acquire(sub2), "Queue entry should have been able to be acquired");
        _queueEntry.reject(sub2);

        //verify it still records being rejected by both consumers
        assertTrue(_queueEntry.isRejectedBy(sub), "Queue entry should have been rejected by the consumer");
        assertTrue(_queueEntry.isRejectedBy(sub2), "Queue entry should have been rejected by the consumer");
    }

    /**
     * Tests if entries in DEQUEUED or DELETED state are not returned by getNext method.
     */
    @Test
    @SuppressWarnings("rawtypes")
    public void testGetNext() throws Exception
    {
        final int numberOfEntries = 5;
        final QueueEntryImpl[] entries = new QueueEntryImpl[numberOfEntries];
        final Map<String,Object> queueAttributes = Map.of(Queue.ID, randomUUID(),
                Queue.NAME, getTestName());
        final QueueManagingVirtualHost<?> virtualHost = BrokerTestHelper.createVirtualHost("testVH", this);
        final StandardQueueImpl queue = new StandardQueueImpl(queueAttributes, virtualHost);
        queue.open();
        final OrderedQueueEntryList queueEntryList = queue.getEntries();

        // create test entries
        for (int i = 0; i < numberOfEntries ; i++)
        {
            final ServerMessage<?> message = mock(ServerMessage.class);
            when(message.getMessageNumber()).thenReturn((long)i);
            final MessageReference reference = mock(MessageReference.class);
            when(reference.getMessage()).thenReturn(message);
            when(message.newReference()).thenReturn(reference);
            when(message.newReference(any(TransactionLogResource.class))).thenReturn(reference);
            final QueueEntryImpl entry = (QueueEntryImpl) queueEntryList.add(message, null);
            entries[i] = entry;
        }

        // test getNext for not acquired entries
        for (int i = 0; i < numberOfEntries ; i++)
        {
            final QueueEntryImpl queueEntry = entries[i];
            final QueueEntry next = queueEntry.getNextValidEntry();
            if (i < numberOfEntries - 1)
            {
                assertEquals(entries[i + 1], next, "Unexpected entry from QueueEntryImpl#getNext()");
            }
            else
            {
                assertNull(next, "The next entry after the last should be null");
            }
        }

        // discard second
        entries[1].acquire();
        entries[1].delete();

        // discard third
        entries[2].acquire();
        entries[2].delete();

        QueueEntry next = entries[0].getNextValidEntry();
        assertEquals(entries[3], next, "expected forth entry");
        next = next.getNextValidEntry();
        assertEquals(entries[4], next, "expected fifth entry");
        next = next.getNextValidEntry();
        assertNull(next, "The next entry after the last should be null");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testRouteToAlternateInvokesAction()
    {
        final String dlqName = "dlq";
        final Map<String, Object> dlqAttributes = Map.of(Queue.ID, randomUUID(),
                Queue.NAME, dlqName);
        final Queue<?> dlq = (Queue<?>) _queueEntry.getQueue().getVirtualHost().createChild(Queue.class, dlqAttributes);

        final AlternateBinding alternateBinding = mock(AlternateBinding.class);
        when(alternateBinding.getDestination()).thenReturn(dlqName);
        _queueEntry.getQueue().setAttributes(Map.of(Queue.ALTERNATE_BINDING, alternateBinding));

        final Action<? super MessageInstance> action = mock(Action.class);
        when(_queueEntry.getMessage().isResourceAcceptable(dlq)).thenReturn(true);
        when(_queueEntry.getMessage().checkValid()).thenReturn(true);
        _queueEntry.acquire();
        final int enqueues = _queueEntry.routeToAlternate(action, null, null);

        assertEquals(1, enqueues, "Unexpected number of enqueues");
        verify(action).performAction(any());

        assertEquals(1, dlq.getQueueDepthMessages(), "Unexpected number of messages on DLQ");
    }
}
