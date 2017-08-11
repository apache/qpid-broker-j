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
import org.apache.qpid.server.model.AbstractConfigurationChangeListener;
import org.apache.qpid.server.model.ConfiguredObject;
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

    private static class Handler extends AbstractConfigurationChangeListener
    {
        private final Queue<?> _queue;
        private boolean _limitsChanged;

        private Handler(final Queue<?> queue)
        {
            _queue = queue;
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

        @Override
        public void attributeSet(final ConfiguredObject<?> object,
                                 final String attributeName,
                                 final Object oldAttributeValue,
                                 final Object newAttributeValue)
        {
            super.attributeSet(object, attributeName, oldAttributeValue, newAttributeValue);
            if (Queue.MAXIMUM_QUEUE_DEPTH_BYTES.equals(attributeName)
                || Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES.equals(attributeName))
            {
                _limitsChanged = true;
            }
        }

        @Override
        public void bulkChangeEnd(final ConfiguredObject<?> object)
        {
            super.bulkChangeEnd(object);
            if (_queue.getOverflowPolicy() == OverflowPolicy.FLOW_TO_DISK)
            {
                if (_limitsChanged)
                {
                    _limitsChanged = false;
                    flowTailToDiskIfNecessary(_queue.getMaximumQueueDepthBytes(), _queue.getMaximumQueueDepthMessages());
                }
            }
            else
            {
                _queue.removeChangeListener(this);
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
                                flowToDisk(message);
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
                ServerMessage message = newlyEnqueued.getMessage();
                if (message != null)
                {
                    flowToDisk(message);
                }
            }
        }

        private void flowToDisk(final ServerMessage message)
        {
            try (MessageReference messageReference = message.newReference())
            {
                message.getStoredMessage().flowToDisk();
            }
            catch (MessageDeletedException mde)
            {
                // pass
            }
        }
    }
}
