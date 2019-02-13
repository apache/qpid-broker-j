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

import org.apache.qpid.server.message.MessageDeletedException;
import org.apache.qpid.server.message.MessageReference;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.OverflowPolicy;
import org.apache.qpid.server.model.Queue;

public class FlowToDiskOverflowPolicyHandler implements OverflowPolicyHandler
{
    private final Handler _handler;

    FlowToDiskOverflowPolicyHandler(final Queue<?> queue)
    {
        _handler = new Handler(queue);
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

        private Handler(final Queue<?> queue)
        {
            super(OverflowPolicy.FLOW_TO_DISK);
            _queue = queue;
        }

        @Override
        void onMaximumQueueDepthChange(final Queue<?> queue)
        {
            checkOverflow(null);
        }

        private void checkOverflow(final QueueEntry newlyEnqueued)
        {
            long maximumQueueDepthBytes = _queue.getMaximumQueueDepthBytes();
            long maximumQueueDepthMessages = _queue.getMaximumQueueDepthMessages();
            if (maximumQueueDepthBytes >= 0L || maximumQueueDepthMessages >= 0L)
            {
                if (newlyEnqueued == null)
                {
                    flowTailToDiskIfNecessary(maximumQueueDepthBytes, maximumQueueDepthMessages);
                }
                else
                {
                    flowNewEntryToDiskIfNecessary(newlyEnqueued, maximumQueueDepthBytes, maximumQueueDepthMessages);
                }
            }
        }

        private void flowTailToDiskIfNecessary(final long maximumQueueDepthBytes, final long maximumQueueDepthMessages)
        {
            final long queueDepthBytes = _queue.getQueueDepthBytes();
            final long queueDepthMessages = _queue.getQueueDepthMessages();

            if ((maximumQueueDepthBytes >= 0L && queueDepthBytes > maximumQueueDepthBytes) ||
                (maximumQueueDepthMessages >= 0L && queueDepthMessages > maximumQueueDepthMessages))
            {

                long cumulativeDepthBytes = 0;
                long cumulativeDepthMessages = 0;

                QueueEntryIterator queueEntryIterator = _queue.queueEntryIterator();
                while (queueEntryIterator.advance())
                {
                    QueueEntry node = queueEntryIterator.getNode();

                    if (node != null && !node.isDeleted())
                    {
                        ServerMessage message = node.getMessage();
                        if (message != null)
                        {
                            cumulativeDepthMessages++;
                            cumulativeDepthBytes += message.getSizeIncludingHeader();

                            if (cumulativeDepthBytes > maximumQueueDepthBytes
                                || cumulativeDepthMessages > maximumQueueDepthMessages)
                            {
                                flowToDisk(node);
                            }
                        }
                    }
                }
            }
        }

        private void flowNewEntryToDiskIfNecessary(final QueueEntry newlyEnqueued,
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

        private void flowToDisk(final QueueEntry node)
        {
            try (MessageReference messageReference = node.getMessage().newReference())
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
    }
}
