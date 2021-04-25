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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.messages.QueueMessages;
import org.apache.qpid.server.model.OverflowPolicy;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.txn.AsyncAutoCommitTransaction;
import org.apache.qpid.server.txn.ServerTransaction;

public class RingOverflowPolicyHandler implements OverflowPolicyHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RingOverflowPolicyHandler.class);
    private final Handler _handler;

    RingOverflowPolicyHandler(final Queue<?> queue,
                              final EventLogger eventLogger)
    {
        _handler = new Handler(queue, eventLogger);
        queue.addChangeListener(_handler);
    }

    @Override
    public void checkOverflow(final QueueEntry newlyEnqueued)
    {
        _handler.checkOverflow(newlyEnqueued);
    }

    private static class Handler extends OverflowPolicyMaximumQueueDepthChangeListener
    {
        private final Queue<?> _queue;
        private final EventLogger _eventLogger;
        private final ThreadLocal<Boolean> _recursionTracker = ThreadLocal.withInitial(() -> Boolean.FALSE);

        public Handler(final Queue<?> queue, final EventLogger eventLogger)
        {
            super(OverflowPolicy.RING);
            _queue = queue;
            _eventLogger = eventLogger;
        }

        @Override
        void onMaximumQueueDepthChange(final Queue<?> queue)
        {
            checkOverflow(null);
        }

        private void checkOverflow(final QueueEntry newlyEnqueued)
        {
            // When this method causes an entry to be deleted, the size of the queue is changed, leading to
            // checkOverflow being called again (because for other policies this may trigger relaxation of flow control,
            // for instance.  This needless recursion is avoided by using this ThreadLocal to track if the method is
            // being called (indirectly) from itself
            if (!_recursionTracker.get())
            {
                _recursionTracker.set(Boolean.TRUE);
                try
                {
                    final long maximumQueueDepthMessages = _queue.getMaximumQueueDepthMessages();
                    final long maximumQueueDepthBytes = _queue.getMaximumQueueDepthBytes();

                    boolean bytesOverflow, messagesOverflow, overflow = false;
                    int counter = 0;
                    int queueDepthMessages;
                    long queueDepthBytes;
                    QueueEntry lastSeenEntry = null;
                    do
                    {
                        queueDepthMessages = _queue.getQueueDepthMessages();
                        queueDepthBytes = _queue.getQueueDepthBytes();

                        messagesOverflow =
                                maximumQueueDepthMessages >= 0 && queueDepthMessages > maximumQueueDepthMessages;
                        bytesOverflow = maximumQueueDepthBytes >= 0 && queueDepthBytes > maximumQueueDepthBytes;

                        if (bytesOverflow || messagesOverflow)
                        {
                            if (!overflow)
                            {
                                overflow = true;
                            }

                            lastSeenEntry = lastSeenEntry == null
                                    ? _queue.getLeastSignificantOldestEntry()
                                    : lastSeenEntry.getNextValidEntry();
                            if (lastSeenEntry != null)
                            {
                                // ensure that we are deleting only entries before the newly enqueued one
                                if (newlyEnqueued != null && lastSeenEntry.compareTo(newlyEnqueued) >= 0)
                                {
                                    // stop at new entry
                                    lastSeenEntry = null;
                                }
                                else if (lastSeenEntry.acquireOrSteal(null))
                                {
                                    counter++;
                                    deleteAcquiredEntry(lastSeenEntry);
                                }
                            }
                        }
                    }
                    while ((bytesOverflow || messagesOverflow) && lastSeenEntry != null);

                    if (overflow)
                    {
                        _eventLogger.message(_queue.getLogSubject(), QueueMessages.DROPPED(counter,
                                                                                           queueDepthBytes,
                                                                                           queueDepthMessages,
                                                                                           maximumQueueDepthBytes,
                                                                                           maximumQueueDepthMessages));
                    }
                }
                finally
                {
                    _recursionTracker.set(Boolean.FALSE);
                }
            }
        }

        private void deleteAcquiredEntry(final QueueEntry entry)
        {
            final MessageStore messageStore = _queue.getVirtualHost().getMessageStore();
            final ServerTransaction txn =
                    new AsyncAutoCommitTransaction(messageStore, (future, action) -> action.postCommit());
            txn.dequeue(entry.getEnqueueRecord(),
                        new ServerTransaction.Action()
                        {
                            @Override
                            public void postCommit()
                            {
                                entry.delete();
                            }

                            @Override
                            public void onRollback()
                            {

                            }
                        });
        }
    }
}
