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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.AbstractConfigurationChangeListener;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.OverflowPolicy;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.StoredMessage;


public class RejectPolicyHandler
{
    private final Handler _handler;

    RejectPolicyHandler(final Queue<?> queue)
    {
        _handler = new Handler(queue);
        addMessageDeleteListener();
        queue.addChangeListener(_handler);
    }

    void messageDeleted(final StoredMessage storedMessage)
    {
        _handler.messageDeleted(storedMessage);
    }

    void checkReject(final ServerMessage<?> newMessage) throws MessageUnacceptableException
    {
        _handler.checkReject(newMessage);
    }

    void postEnqueue(MessageInstance instance)
    {
        _handler.postEnqueue(instance);
    }

    private void addMessageDeleteListener()
    {
        MessageStore messageStore = _handler.getMessageStore();
        if (messageStore != null)
        {
            messageStore.addMessageDeleteListener(_handler);
        }
    }

    private static class Handler extends AbstractConfigurationChangeListener implements MessageStore.MessageDeleteListener
    {
        private final Queue<?> _queue;
        private final AtomicLong _pendingDepthBytes = new AtomicLong();
        private final AtomicInteger _pendingDepthMessages = new AtomicInteger();
        private final Map<StoredMessage<?>, Long> _pendingMessages = new ConcurrentHashMap<>();

        private Handler(final Queue<?> queue)
        {
            _queue = queue;
        }

        @Override
        public void messageDeleted(final StoredMessage<?> m)
        {
            decrementPendingCountersIfNecessary(m);
        }

        @Override
        public void bulkChangeEnd(final ConfiguredObject<?> object)
        {
            super.bulkChangeEnd(object);
            if (_queue.getOverflowPolicy() != OverflowPolicy.REJECT)
            {
                _queue.removeChangeListener(this);

                MessageStore messageStore = getMessageStore();
                if (messageStore != null)
                {
                    messageStore.removeMessageDeleteListener(this);
                }
            }
        }

        private void checkReject(final ServerMessage<?> newMessage) throws MessageUnacceptableException
        {
            final long maximumQueueDepthMessages = _queue.getMaximumQueueDepthMessages();
            final long maximumQueueDepthBytes = _queue.getMaximumQueueDepthBytes();
            final int queueDepthMessages = _queue.getQueueDepthMessages();
            final long queueDepthBytes = _queue.getQueueDepthBytes();
            final long size = newMessage.getSizeIncludingHeader();
            if (_pendingMessages.putIfAbsent(newMessage.getStoredMessage(), size) == null)
            {
                int pendingMessages = _pendingDepthMessages.addAndGet(1);
                long pendingBytes = _pendingDepthBytes.addAndGet(size);

                boolean messagesOverflow = maximumQueueDepthMessages >= 0
                                           && queueDepthMessages + pendingMessages > maximumQueueDepthMessages;
                boolean bytesOverflow = maximumQueueDepthBytes >= 0
                                        && queueDepthBytes + pendingBytes > maximumQueueDepthBytes;
                if (bytesOverflow || messagesOverflow)
                {
                    _pendingDepthBytes.addAndGet(-size);
                    _pendingDepthMessages.addAndGet(-1);
                    _pendingMessages.remove(newMessage.getStoredMessage());
                    final String message = String.format(
                            "Maximum depth exceeded on '%s' : current=[count: %d, size: %d], max=[count: %d, size: %d]",
                            _queue.getName(),
                            queueDepthMessages + pendingMessages,
                            queueDepthBytes + pendingBytes,
                            maximumQueueDepthMessages,
                            maximumQueueDepthBytes);
                    throw new MessageUnacceptableException(message);
                }
            }
        }

        private void postEnqueue(MessageInstance instance)
        {
            decrementPendingCountersIfNecessary(instance.getMessage().getStoredMessage());
        }

        private void decrementPendingCountersIfNecessary(final StoredMessage<?> m)
        {
            Long size;
            if ((size = _pendingMessages.remove(m)) != null)
            {
                _pendingDepthBytes.addAndGet(-size);
                _pendingDepthMessages.addAndGet(-1);
            }
        }

        private MessageStore getMessageStore()
        {
            return _queue.getVirtualHost().getMessageStore();
        }
    }
}
