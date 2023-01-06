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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.consumer.ConsumerOption;
import org.apache.qpid.server.consumer.TestConsumerTarget;
import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.message.MessageInstanceConsumer;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.LifetimePolicy;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.store.MessageDurability;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;

public class StandardQueueTest extends AbstractQueueTestBase
{
    @Test
    public void testAutoDeleteQueue() throws Exception
    {
        getQueue().close();
        getQueue().delete();
        final Map<String,Object> queueAttributes = Map.of(Queue.NAME, getQname(),
                Queue.LIFETIME_POLICY, LifetimePolicy.DELETE_ON_NO_OUTBOUND_LINKS);
        final StandardQueueImpl queue = new StandardQueueImpl(queueAttributes, getVirtualHost());
        queue.open();
        setQueue(queue);

        final ServerMessage<?> message = createMessage(25L);
        final QueueConsumer<?, ?> consumer = (QueueConsumer<?, ?>) getQueue()
                .addConsumer(getConsumerTarget(), null, message.getClass(), "test",
                EnumSet.of(ConsumerOption.ACQUIRES, ConsumerOption.SEES_REQUEUES), 0);

        getQueue().enqueue(message, null, null);
        consumer.close();
        assertTrue(getQueue().isDeleted(), "Queue was not deleted when consumer was removed");
    }

    /**
     * Tests that entry in dequeued state are not enqueued and not delivered to consumer
     */
    @Test
    public void testEnqueueDequeuedEntry() throws Exception
    {
        // create a queue where each even entry is considered a dequeued
        final AbstractQueue<?> queue = new DequeuedQueue(getVirtualHost());
        queue.create();
        // create a consumer
        final TestConsumerTarget consumer = new TestConsumerTarget();

        // register consumer
        queue.addConsumer(consumer,
                          null,
                          createMessage(-1L).getClass(),
                          "test",
                          EnumSet.of(ConsumerOption.ACQUIRES, ConsumerOption.SEES_REQUEUES), 0);

        // put test messages into a queue
        putGivenNumberOfMessages(queue, 4);
        while(consumer.processPending());

        // assert received messages
        final List<MessageInstance> messages = consumer.getMessages();
        assertEquals(2, (long) messages.size(), "Only 2 messages should be returned");
        assertEquals(1L, (messages.get(0).getMessage()).getMessageNumber(),
                "ID of first message should be 1");
        assertEquals(3L, (messages.get(1).getMessage()).getMessageNumber(),
                "ID of second message should be 3");
    }

    /**
     * Tests whether dequeued entry is sent to subscriber
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testProcessQueueWithDequeuedEntry() throws Exception
    {
        // total number of messages to send
        final int messageNumber = 4;
        final int dequeueMessageIndex = 1;

        final Map<String,Object> queueAttributes = Map.of(Queue.NAME, "test");
        // create queue with overridden method deliverAsync
        final StandardQueueImpl testQueue = new StandardQueueImpl(queueAttributes, getVirtualHost());
        testQueue.create();

        // put messages
        final List<StandardQueueEntry> entries =
                (List<StandardQueueEntry>) enqueueGivenNumberOfMessages(testQueue, messageNumber);

        // dequeue message
        dequeueMessage(testQueue, dequeueMessageIndex);

        // latch to wait for message receipt
        final CountDownLatch latch = new CountDownLatch(messageNumber -1);

        // create a consumer
        final TestConsumerTarget consumer = new TestConsumerTarget()
        {

            @Override
            public void notifyWork()
            {
                while(processPending());
            }

            /**
             * Send a message and decrement latch
             * @param consumer
             * @param entry
             * @param batch
             */
            @Override
            @SuppressWarnings("rawtypes")
            public void send(final MessageInstanceConsumer consumer, final MessageInstance entry, final  boolean batch)
            {
                super.send(consumer, entry, batch);
                latch.countDown();
            }
        };

        // subscribe
        testQueue.addConsumer(consumer,
                              null,
                              entries.get(0).getMessage().getClass(),
                              "test",
                              EnumSet.of(ConsumerOption.ACQUIRES, ConsumerOption.SEES_REQUEUES), 0);


        // wait up to 1 minute for message receipt
        try
        {
            latch.await(1, TimeUnit.MINUTES);
        }
        catch (InterruptedException e1)
        {
            Thread.currentThread().interrupt();
        }
        final List<MessageInstance> expected = Arrays.asList(entries.get(0), entries.get(2), entries.get(3));
        verifyReceivedMessages(expected, consumer.getMessages());
    }

    @Test
    @SuppressWarnings("rawtypes")
    public void testNonDurableImpliesMessageDurabilityNever()
    {
        getQueue().close();
        getQueue().delete();

        final Map<String,Object> attributes = Map.of(Queue.NAME, getQname(),
                Queue.DURABLE, Boolean.FALSE,
                Queue.MESSAGE_DURABILITY, MessageDurability.ALWAYS);

        final Queue queue =  getVirtualHost().createChild(Queue.class, attributes);
        assertNotNull(queue, "Queue was not created");
        setQueue(queue);

        assertEquals(MessageDurability.NEVER, queue.getMessageDurability());
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private final class DequeuedQueue extends AbstractQueue
    {
        private final QueueEntryList _entries = new DequeuedQueueEntryList(this, getQueueStatistics());

        public DequeuedQueue(final QueueManagingVirtualHost<?> virtualHost)
        {
            super(Map.of(Queue.NAME, getTestName(),
                    Queue.DURABLE, false,
                    Queue.LIFETIME_POLICY, LifetimePolicy.PERMANENT), virtualHost);
        }

        @Override
        QueueEntryList getEntries()
        {
            return _entries;
        }
    }

    private static class DequeuedQueueEntryList extends OrderedQueueEntryList
    {
        private static final HeadCreator HEAD_CREATOR = list ->
                new DequeuedQueueEntry((DequeuedQueueEntryList) list);

        public DequeuedQueueEntryList(final DequeuedQueue queue, final QueueStatistics queueStatistics)
        {
            super(queue, queueStatistics, HEAD_CREATOR);
        }

        /**
         * Entries with even message id are considered
         * dequeued!
         */
        @Override
        protected DequeuedQueueEntry createQueueEntry(final ServerMessage<?> message,
                                                      final MessageEnqueueRecord enqueueRecord)
        {
            return new DequeuedQueueEntry(this, message);
        }

        @Override
        public QueueEntry getLeastSignificantOldestEntry()
        {
            return getOldestEntry();
        }
    }

    private static class DequeuedQueueEntry extends OrderedQueueEntry
    {
        private final ServerMessage<?> _message;

        private DequeuedQueueEntry(final DequeuedQueueEntryList queueEntryList)
        {
            super(queueEntryList);
            _message = null;
        }

        public DequeuedQueueEntry(final DequeuedQueueEntryList list, final ServerMessage<?> message)
        {
            super(list, message, null);
            _message = message;
        }

        @Override
        public boolean isDeleted()
        {
            return (_message.getMessageNumber() % 2 == 0);
        }

        @Override
        public boolean isAvailable()
        {
            return !(_message.getMessageNumber() % 2 == 0);
        }

        @Override
        public boolean acquire(final MessageInstanceConsumer<?> consumer)
        {
            if(_message.getMessageNumber() % 2 == 0)
            {
                return false;
            }
            else
            {
                return super.acquire(consumer);
            }
        }

        @Override
        public boolean makeAcquisitionUnstealable(final MessageInstanceConsumer<?> consumer)
        {
            return true;
        }

        @Override
        public boolean makeAcquisitionStealable()
        {
            return true;
        }
    }
}
