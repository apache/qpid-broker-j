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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

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
    private long _consumerId;

    public abstract QueueEntryImpl getQueueEntryImpl(int msgId);

    @Test
    public abstract void testCompareTo();

    @Test
    public abstract void testTraverseWithNoDeletedEntries();

    @Test
    public abstract void testTraverseWithDeletedEntries();

    @Before
    public void setUp() throws Exception
    {
        _queueEntry = getQueueEntryImpl(1);
        _queueEntry2 = getQueueEntryImpl(2);
        _queueEntry3 = getQueueEntryImpl(3);
    }

    @Test
    public void testAcquire()
    {
        assertTrue("Queue entry should be in AVAILABLE state before invoking of acquire method",
                          _queueEntry.isAvailable());

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
    public void testReleaseAcquired()
    {
        acquire();
        _queueEntry.release();
        assertTrue("Queue entry should be in AVAILABLE state after invoking of release method",
                          _queueEntry.isAvailable());
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
        assertTrue("Invoking of release on entry in DELETED state should not have any effect",
                          _queueEntry.isDeleted());
    }

    /**
     * A helper method to put tested object into deleted state and assert the state
     */
    private void delete()
    {
        _queueEntry.acquire();
        _queueEntry.delete();
        assertTrue("Queue entry should be in DELETED state after invoking of delete method",
                          _queueEntry.isDeleted());
    }


    /**
     * A helper method to put tested entry into acquired state and assert the sate
     */
    private void acquire()
    {
        _queueEntry.acquire(newConsumer());
        assertTrue("Queue entry should be in ACQUIRED state after invoking of acquire method",
                          _queueEntry.isAcquired());
    }

    private QueueConsumer newConsumer()
    {
        final QueueConsumer consumer = mock(QueueConsumer.class);

        StealableConsumerAcquiredState
                owningState = new StealableConsumerAcquiredState(consumer);
        when(consumer.getOwningState()).thenReturn(owningState);
        final Long consumerNum = _consumerId++;
        when(consumer.getConsumerNumber()).thenReturn(consumerNum);
        when(consumer.getIdentifier()).thenReturn(consumerNum);
        return consumer;
    }

    @Test
    public void testStateChanges()
    {
        QueueConsumer consumer = newConsumer();
        StateChangeListener<MessageInstance, EntryState> stateChangeListener = mock(StateChangeListener.class);
        _queueEntry.addStateChangeListener(stateChangeListener);
        _queueEntry.acquire(consumer);
        verify(stateChangeListener).stateChanged(eq(_queueEntry),
                                                 eq(MessageInstance.AVAILABLE_STATE),
                                                 isA(UnstealableConsumerAcquiredState.class));
        _queueEntry.makeAcquisitionStealable();
        verify(stateChangeListener).stateChanged(eq(_queueEntry),
                                                 isA(UnstealableConsumerAcquiredState.class),
                                                 isA(StealableConsumerAcquiredState.class));
        _queueEntry.removeAcquisitionFromConsumer(consumer);
        verify(stateChangeListener).stateChanged(eq(_queueEntry),
                                                 isA(StealableConsumerAcquiredState.class),
                                                 eq(NON_CONSUMER_ACQUIRED_STATE));
    }

    @Test
    public void testLocking()
    {
        QueueConsumer consumer = newConsumer();
        QueueConsumer consumer2 = newConsumer();

        _queueEntry.acquire(consumer);
        assertTrue("Queue entry should be in ACQUIRED state after invoking of acquire method",
                          _queueEntry.isAcquired());

        assertFalse("Acquisition should initially be locked",
                           _queueEntry.removeAcquisitionFromConsumer(consumer));

        assertTrue("Should be able to unlock locked queue entry", _queueEntry.makeAcquisitionStealable());
        assertFalse("Acquisition should not be able to be removed from the wrong consumer",
                           _queueEntry.removeAcquisitionFromConsumer(consumer2));
        assertTrue("Acquisition should be able to be removed once unlocked",
                          _queueEntry.removeAcquisitionFromConsumer(consumer));
        assertTrue("Queue Entry should still be acquired", _queueEntry.isAcquired());
        assertFalse("Queue Entry should not be marked as acquired by a consumer",
                           _queueEntry.acquiredByConsumer());

        _queueEntry.release();

        assertFalse("Hijacked queue entry should be able to be released", _queueEntry.isAcquired());

        _queueEntry.acquire(consumer);
        assertTrue("Queue entry should be in ACQUIRED state after invoking of acquire method",
                          _queueEntry.isAcquired());

        assertFalse("Acquisition should initially be locked",
                           _queueEntry.removeAcquisitionFromConsumer(consumer));
        assertTrue("Should be able to unlock locked queue entry", _queueEntry.makeAcquisitionStealable());
        assertTrue("Should be able to lock queue entry", _queueEntry.makeAcquisitionUnstealable(consumer));
        assertFalse("Acquisition should not be able to be hijacked when locked",
                           _queueEntry.removeAcquisitionFromConsumer(consumer));

        _queueEntry.delete();
        assertTrue("Locked queue entry should be able to be deleted", _queueEntry.isDeleted());
    }

    @Test
    public void testLockAcquisitionOwnership()
    {
        QueueConsumer consumer1 = newConsumer();
        QueueConsumer consumer2 = newConsumer();

        _queueEntry.acquire(consumer1);
        assertTrue("Queue entry should be acquired by consumer1", _queueEntry.acquiredByConsumer());

        assertTrue("Consumer1 relocking should be allowed", _queueEntry.makeAcquisitionUnstealable(consumer1));
        assertFalse("Consumer2 should not be allowed", _queueEntry.makeAcquisitionUnstealable(consumer2));

        _queueEntry.makeAcquisitionStealable();

        assertTrue("Queue entry should still be acquired by consumer1", _queueEntry.acquiredByConsumer());

        _queueEntry.release(consumer1);

        assertFalse("Queue entry should no longer be acquired by consumer1", _queueEntry.acquiredByConsumer());
    }

    /**
     * A helper method to get entry state
     *
     * @return entry state
     */
    private EntryState getState()
    {
        EntryState state = null;
        try
        {
            Field f = QueueEntryImpl.class.getDeclaredField("_state");
            f.setAccessible(true);
            state = (EntryState) f.get(_queueEntry);
        }
        catch (Exception e)
        {
            fail("Failure to get a state field: " + e.getMessage());
        }
        return state;
    }

    /**
     * Tests rejecting a queue entry records the Consumer ID
     * for later verification by isRejectedBy(consumerId).
     */
    @Test
    public void testRejectAndRejectedBy()
    {
        QueueConsumer sub = newConsumer();

        assertFalse("Queue entry should not yet have been rejected by the consumer",
                           _queueEntry.isRejectedBy(sub));
        assertFalse("Queue entry should not yet have been acquired by a consumer", _queueEntry.isAcquired());

        //acquire, reject, and release the message using the consumer
        assertTrue("Queue entry should have been able to be acquired", _queueEntry.acquire(sub));
        _queueEntry.reject(sub);
        _queueEntry.release();

        //verify the rejection is recorded
        assertTrue("Queue entry should have been rejected by the consumer", _queueEntry.isRejectedBy(sub));

        //repeat rejection using a second consumer
        QueueConsumer sub2 = newConsumer();

        assertFalse("Queue entry should not yet have been rejected by the consumer",
                           _queueEntry.isRejectedBy(sub2));
        assertTrue("Queue entry should have been able to be acquired", _queueEntry.acquire(sub2));
        _queueEntry.reject(sub2);

        //verify it still records being rejected by both consumers
        assertTrue("Queue entry should have been rejected by the consumer", _queueEntry.isRejectedBy(sub));
        assertTrue("Queue entry should have been rejected by the consumer", _queueEntry.isRejectedBy(sub2));
    }

    /**
     * Tests if entries in DEQUEUED or DELETED state are not returned by getNext method.
     */
    @Test
    public void testGetNext() throws Exception
    {
        int numberOfEntries = 5;
        QueueEntryImpl[] entries = new QueueEntryImpl[numberOfEntries];
        Map<String,Object> queueAttributes = new HashMap<String, Object>();
        queueAttributes.put(Queue.ID, UUID.randomUUID());
        queueAttributes.put(Queue.NAME, getTestName());
        final QueueManagingVirtualHost virtualHost = BrokerTestHelper.createVirtualHost("testVH", this);
        StandardQueueImpl queue = new StandardQueueImpl(queueAttributes, virtualHost);
        queue.open();
        OrderedQueueEntryList queueEntryList = queue.getEntries();

        // create test entries
        for(int i = 0; i < numberOfEntries ; i++)
        {
            ServerMessage message = mock(ServerMessage.class);
            when(message.getMessageNumber()).thenReturn((long)i);
            final MessageReference reference = mock(MessageReference.class);
            when(reference.getMessage()).thenReturn(message);
            when(message.newReference()).thenReturn(reference);
            when(message.newReference(any(TransactionLogResource.class))).thenReturn(reference);
            QueueEntryImpl entry = (QueueEntryImpl) queueEntryList.add(message, null);
            entries[i] = entry;
        }

        // test getNext for not acquired entries
        for(int i = 0; i < numberOfEntries ; i++)
        {
            QueueEntryImpl queueEntry = entries[i];
            QueueEntry next = queueEntry.getNextValidEntry();
            if (i < numberOfEntries - 1)
            {
                assertEquals("Unexpected entry from QueueEntryImpl#getNext()", entries[i + 1], next);
            }
            else
            {
                assertNull("The next entry after the last should be null", next);
            }
        }

        // discard second
        entries[1].acquire();
        entries[1].delete();

        // discard third
        entries[2].acquire();
        entries[2].delete();

        QueueEntry next = entries[0].getNextValidEntry();
        assertEquals("expected forth entry", entries[3], next);
        next = next.getNextValidEntry();
        assertEquals("expected fifth entry", entries[4], next);
        next = next.getNextValidEntry();
        assertNull("The next entry after the last should be null", next);
    }

    @Test
    public void testRouteToAlternateInvokesAction()
    {
        String dlqName = "dlq";
        Map<String, Object> dlqAttributes = new HashMap<>();
        dlqAttributes.put(Queue.ID, UUID.randomUUID());
        dlqAttributes.put(Queue.NAME, dlqName);
        Queue<?> dlq = (Queue<?>) _queueEntry.getQueue().getVirtualHost().createChild(Queue.class, dlqAttributes);

        final AlternateBinding alternateBinding = mock(AlternateBinding.class);
        when(alternateBinding.getDestination()).thenReturn(dlqName);
        _queueEntry.getQueue().setAttributes(Collections.singletonMap(Queue.ALTERNATE_BINDING, alternateBinding));

        final Action<? super MessageInstance> action = mock(Action.class);
        when(_queueEntry.getMessage().isResourceAcceptable(dlq)).thenReturn(true);
        when(_queueEntry.getMessage().checkValid()).thenReturn(true);
        _queueEntry.acquire();
        int enqueues = _queueEntry.routeToAlternate(action, null, null);

        assertEquals("Unexpected number of enqueues", 1, enqueues);
        verify(action).performAction(any());

        assertEquals("Unexpected number of messages on DLQ", 1, dlq.getQueueDepthMessages());
    }
}
