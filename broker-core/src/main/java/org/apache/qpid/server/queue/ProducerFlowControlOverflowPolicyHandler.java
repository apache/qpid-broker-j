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

import java.security.AccessController;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.security.auth.Subject;

import org.apache.qpid.server.connection.SessionPrincipal;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.messages.QueueMessages;
import org.apache.qpid.server.model.AbstractConfigurationChangeListener;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.OverflowPolicy;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.session.AMQPSession;

public class ProducerFlowControlOverflowPolicyHandler implements OverflowPolicyHandler
{
    private final Handler _handler;

    ProducerFlowControlOverflowPolicyHandler(Queue<?> queue, EventLogger eventLogger)
    {
        _handler = new Handler(queue, eventLogger);
        queue.addChangeListener(_handler);
    }

    boolean isQueueFlowStopped()
    {
        return _handler.isQueueFlowStopped();
    }

    @Override
    public void checkOverflow(final QueueEntry newlyEnqueued)
    {
        _handler.checkOverflow(newlyEnqueued);
    }

    private static class Handler extends AbstractConfigurationChangeListener
    {
        private final Queue<?> _queue;
        private final EventLogger _eventLogger;
        private final AtomicBoolean _overfullReported = new AtomicBoolean(false);
        private final Set<AMQPSession<?, ?>> _blockedSessions =
                Collections.newSetFromMap(new ConcurrentHashMap<AMQPSession<?, ?>, Boolean>());
        private volatile double _queueFlowResumeLimit;
        private boolean _checkCapacity;

        private Handler(final Queue<?> queue, final EventLogger eventLogger)
        {
            _queue = queue;
            _eventLogger = eventLogger;
            Double value = _queue.getContextValue(Double.class, Queue.QUEUE_FLOW_RESUME_LIMIT);
            if (value != null)
            {
                _queueFlowResumeLimit = value;
            }
        }

        private void checkOverflow(final QueueEntry newlyEnqueued)
        {
            long maximumQueueDepthBytes = _queue.getMaximumQueueDepthBytes();
            long maximumQueueDepthMessages = _queue.getMaximumQueueDepthMessages();
            if (maximumQueueDepthBytes >= 0L || maximumQueueDepthMessages >= 0L)
            {
                checkOverfull(maximumQueueDepthBytes, maximumQueueDepthMessages);
            }

            checkUnderfull(maximumQueueDepthBytes, maximumQueueDepthMessages);
        }

        @Override
        public void attributeSet(final ConfiguredObject<?> object,
                                 final String attributeName,
                                 final Object oldAttributeValue,
                                 final Object newAttributeValue)
        {
            super.attributeSet(object, attributeName, oldAttributeValue, newAttributeValue);
            if (Queue.CONTEXT.equals(attributeName))
            {
                Double value = _queue.getContextValue(Double.class, Queue.QUEUE_FLOW_RESUME_LIMIT);
                double queueFlowResumePercentage = value == null ? 0 : value;
                if (queueFlowResumePercentage != _queueFlowResumeLimit)
                {
                    _queueFlowResumeLimit = queueFlowResumePercentage;
                    _checkCapacity = true;
                }
            }
            if (Queue.MAXIMUM_QUEUE_DEPTH_BYTES.equals(attributeName)
                || Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES.equals(attributeName))
            {
                _checkCapacity = true;
            }
        }

        @Override
        public void bulkChangeEnd(final ConfiguredObject<?> object)
        {
            super.bulkChangeEnd(object);
            if (_queue.getOverflowPolicy() == OverflowPolicy.PRODUCER_FLOW_CONTROL)
            {
                if (_checkCapacity)
                {
                    _checkCapacity = false;
                    checkUnderfull(_queue.getMaximumQueueDepthBytes(), _queue.getMaximumQueueDepthMessages());
                }
            }
            else
            {
                _queue.removeChangeListener(this);
                checkUnderfull(-1, -1);
            }
        }

        boolean isQueueFlowStopped()
        {
            return _overfullReported.get();
        }

        private void checkUnderfull(long maximumQueueDepthBytes, long maximumQueueDepthMessages)
        {
            long queueDepthBytes = _queue.getQueueDepthBytes();
            long queueDepthMessages = _queue.getQueueDepthMessages();

            if (isUnderfull(queueDepthBytes, maximumQueueDepthBytes)
                && isUnderfull(queueDepthMessages, maximumQueueDepthMessages))
            {
                if (_overfullReported.compareAndSet(true, false))
                {
                    _eventLogger.message(_queue.getLogSubject(),
                                         QueueMessages.UNDERFULL(queueDepthBytes,
                                                                 getFlowResumeLimit(maximumQueueDepthBytes),
                                                                 queueDepthMessages,
                                                                 getFlowResumeLimit(maximumQueueDepthMessages)));
                }

                for (final AMQPSession<?, ?> blockedSession : _blockedSessions)
                {
                    blockedSession.unblock(_queue);
                    _blockedSessions.remove(blockedSession);
                }
            }
        }

        private void checkOverfull(final long maximumQueueDepthBytes, final long maximumQueueDepthMessages)
        {
            final long queueDepthBytes = _queue.getQueueDepthBytes();
            final long queueDepthMessages = _queue.getQueueDepthMessages();

            if ((maximumQueueDepthBytes >= 0L && queueDepthBytes > maximumQueueDepthBytes) ||
                (maximumQueueDepthMessages >= 0L && queueDepthMessages > maximumQueueDepthMessages))
            {
                Subject subject = Subject.getSubject(AccessController.getContext());
                Set<SessionPrincipal> sessionPrincipals = subject.getPrincipals(SessionPrincipal.class);
                if (!sessionPrincipals.isEmpty())
                {
                    SessionPrincipal sessionPrincipal = sessionPrincipals.iterator().next();
                    if (sessionPrincipal != null)
                    {

                        if (_overfullReported.compareAndSet(false, true))
                        {
                            _eventLogger.message(_queue.getLogSubject(),
                                                 QueueMessages.OVERFULL(queueDepthBytes,
                                                                        maximumQueueDepthBytes,
                                                                        queueDepthMessages,
                                                                        maximumQueueDepthMessages));
                        }

                        final AMQPSession<?, ?> session = sessionPrincipal.getSession();
                        _blockedSessions.add(session);
                        session.block(_queue);
                    }
                }
            }
        }

        private boolean isUnderfull(final long queueDepth,
                                    final long maximumQueueDepth)
        {
            return maximumQueueDepth < 0 || queueDepth <= getFlowResumeLimit(maximumQueueDepth);
        }

        private long getFlowResumeLimit(final long maximumQueueDepth)
        {
            if (maximumQueueDepth >= 0)
            {
                return (long) Math.ceil(_queueFlowResumeLimit / 100.0 * maximumQueueDepth);
            }
            return -1;
        }
    }

}

