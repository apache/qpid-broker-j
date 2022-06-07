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

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.MessageDeletedException;
import org.apache.qpid.server.message.MessageReference;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.OverflowPolicy;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.store.StoredMessage;

/**
 * If the queue breaches a limit, newly arriving messages are written to disk and the in-memory representation of
 * the message is minimised. The Broker will transparently retrieve messages from disk as they are required by a
 * consumer or management. The flow to disk policy does not actually restrict the overall size of the queue,
 * merely the space occupied in memory.
 */
public class FlowToDiskOverflowPolicyHandler implements OverflowPolicyHandler
{
    /**
     * Delegate handler
     */
    private final Handler _handler;

    /**
     * Constructor injects queue
     *
     * @param queue Queue
     */
    FlowToDiskOverflowPolicyHandler(final Queue<?> queue)
    {
        _handler = new Handler(queue);
        queue.addChangeListener(_handler);
    }

    /**
     * Checks queue overflow (is called when adding or deleting queue entry)
     *
     * @param newlyEnqueued QueueEntry enqueued (is null on deletion)
     */
    @Override
    public void checkOverflow(final QueueEntry newlyEnqueued)
    {
        _handler.checkOverflow(newlyEnqueued, true);
    }

    /**
     * Delegate handler
     */
    private static class Handler extends OverflowPolicyMaximumQueueDepthChangeListener
    {
        /**
         * Queue instance
         */
        private final Queue<?> _queue;

        /**
         * Constructor injects queue
         *
         * @param queue Queue
         */
        private Handler(final Queue<?> queue)
        {
            super(OverflowPolicy.FLOW_TO_DISK);
            _queue = queue;
        }

        /**
         * Is called when max queue depth is changed
         *
         * @param queue Queue
         */
        @Override
        void onMaximumQueueDepthChange(final Queue<?> queue)
        {
            checkOverflow(null, false);
        }

        /**
         * Either flows messages to the disk or restores them into the memory
         *
         * @param newlyEnqueued QueueEntry (could be null in case of deletion or limit change)
         * @param stopOnFirstMatch Whether flowing to disk / restoring to memory should be stopped after fist match
         */
        private void checkOverflow(final QueueEntry newlyEnqueued, final boolean stopOnFirstMatch)
        {
            final long maximumQueueDepthBytes = _queue.getMaximumQueueDepthBytes();
            final long maximumQueueDepthMessages = _queue.getMaximumQueueDepthMessages();
            if (maximumQueueDepthBytes >= 0L || maximumQueueDepthMessages >= 0L)
            {
                if (newlyEnqueued == null)
                {
                    balanceTailIfNecessary(maximumQueueDepthBytes, maximumQueueDepthMessages, stopOnFirstMatch);
                }
                else
                {
                    flowNewEntryToDiskIfNecessary(newlyEnqueued, maximumQueueDepthBytes, maximumQueueDepthMessages);
                }
            }
        }

        /**
         * Either flows tail messages to disk or restores them into the memory depending on the overflow limit.
         * Boolean flag stopOnFirstMatch is true when enqueueing or deleting messages, is false when overflow limit
         * was changed
         *
         * @param maximumQueueDepthBytes Max queue depth in bytes
         * @param maximumQueueDepthMessages Max queue depth in messages
         * @param stopOnFirstMatch Whether flowing to disk / restoring to memory should be stopped after fist match
         */
        private void balanceTailIfNecessary(
            final long maximumQueueDepthBytes,
            final long maximumQueueDepthMessages,
            final boolean stopOnFirstMatch)
        {
            final long queueDepthBytes = _queue.getQueueDepthBytes();
            final long queueDepthMessages = _queue.getQueueDepthMessages();
            final boolean isMaximumQueueDepthBytesUnlimited = maximumQueueDepthBytes >= 0L;

            if ((isMaximumQueueDepthBytesUnlimited && queueDepthBytes > maximumQueueDepthBytes) ||
                (maximumQueueDepthMessages >= 0L && queueDepthMessages >= maximumQueueDepthMessages))
            {

                long cumulativeDepthBytes = 0;
                long cumulativeDepthMessages = 0;

                final QueueEntryIterator queueEntryIterator = _queue.queueEntryIterator();
                while (queueEntryIterator.advance())
                {
                    final QueueEntry node = queueEntryIterator.getNode();

                    if (node != null && !node.isDeleted())
                    {
                        final ServerMessage<?> message = node.getMessage();
                        if (message != null)
                        {
                            cumulativeDepthMessages++;
                            cumulativeDepthBytes += message.getSizeIncludingHeader();

                            final boolean isInMemory = message.getStoredMessage().isInContentInMemory();

                            if ((isMaximumQueueDepthBytesUnlimited && cumulativeDepthBytes > maximumQueueDepthBytes)
                                || cumulativeDepthMessages > maximumQueueDepthMessages)
                            {
                                if (stopOnFirstMatch || !isInMemory)
                                {
                                    break;
                                }
                                flowToDisk(node);
                            }
                            else if (!isInMemory)
                            {
                                restoreInMemory(node);
                            }
                        }
                    }
                }
            }
        }

        /**
         * Flows queue entry to the disk (when overflow limit is exceeded)
         *
         * @param newlyEnqueued QueueEntry
         * @param maximumQueueDepthBytes Max queue depth in bytes
         * @param maximumQueueDepthMessages Max queue depth in messages
         */
        private void flowNewEntryToDiskIfNecessary(
            final QueueEntry newlyEnqueued,
            final long maximumQueueDepthBytes,
            final long maximumQueueDepthMessages)
        {
            final long queueDepthBytes = _queue.getQueueDepthBytes();
            final long queueDepthMessages = _queue.getQueueDepthMessages();

            if ((maximumQueueDepthBytes >= 0L && queueDepthBytes > maximumQueueDepthBytes) ||
                (maximumQueueDepthMessages >= 0L && queueDepthMessages > maximumQueueDepthMessages))
            {
                flowToDisk(newlyEnqueued);
            }
        }

        /**
         * Flows queue entry to the disk
         *
         * @param node QueueEntry
         */
        private void flowToDisk(final QueueEntry node)
        {
            try (final MessageReference<?> messageReference = node.getMessage().newReference())
            {
                if (node.getQueue().checkValid(node))
                {
                    messageReference.getMessage().getStoredMessage().flowToDisk();
                }
            }
            catch (MessageDeletedException mde)
            {
                // pass
            }
        }

        /**
         * Restores queue entry in memory
         *
         * @param node QueueEntry
         */
        private void restoreInMemory(final QueueEntry node)
        {
            try (final MessageReference<?> messageReference = node.getMessage().newReference())
            {
                if (node.getQueue().checkValid(node))
                {
                    final StoredMessage<?> storedMessage = messageReference.getMessage().getStoredMessage();
                    try (final QpidByteBuffer qpidByteBuffer = storedMessage.getContent(0, storedMessage.getContentSize()))
                    {
                        qpidByteBuffer.dispose();
                    }
                }
            }
            catch (MessageDeletedException mde)
            {
                // pass
            }
        }
    }
}
