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

import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.messages.QueueMessages;
import org.apache.qpid.server.model.OverflowPolicy;
import org.apache.qpid.server.model.Queue;

public class RingOverflowPolicyHandler implements OverflowPolicyHandler
{
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
        _handler.checkOverflow();
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
            checkOverflow();
        }

        private void checkOverflow()
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

                            QueueEntry entry = _queue.getLeastSignificantOldestEntry();

                            if (entry != null)
                            {
                                counter++;
                                _queue.deleteEntry(entry);
                            }
                            else
                            {
                                queueDepthMessages = _queue.getQueueDepthMessages();
                                queueDepthBytes = _queue.getQueueDepthBytes();
                                break;
                            }
                        }
                    }
                    while (bytesOverflow || messagesOverflow);

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
    }

}
