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
package org.apache.qpid.server.message;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.queue.BaseQueue;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.server.util.Action;

public class RoutingResult<M extends ServerMessage<? extends StorableMessageMetaData>>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RoutingResult.class);

    private final M _message;

    private final Set<BaseQueue> _queues = new HashSet<>();
    private final Map<BaseQueue, RejectReason> _rejectingRoutableQueues = new HashMap<>();

    public RoutingResult(final M message)
    {
        _message = message;
    }

    public void addQueue(BaseQueue q)
    {
        if(q.isDeleted())
        {
            LOGGER.debug("Attempt to enqueue message onto deleted queue {}",  q.getName());
        }
        else
        {
            _queues.add(q);
        }
    }

    private void addQueues(Collection<? extends BaseQueue> queues)
    {
        boolean deletedQueues = false;
        for(BaseQueue q : queues)
        {
            if(q.isDeleted())
            {
                if(!deletedQueues)
                {
                    deletedQueues = true;
                    queues = new ArrayList<>(queues);
                }
                LOGGER.debug("Attempt to enqueue message onto deleted queue {}",  q.getName());

                queues.remove(q);
            }
        }

        _queues.addAll(queues);
    }

    public void add(RoutingResult<M> result)
    {
        addQueues(result._queues);
        for (Map.Entry<BaseQueue, RejectReason> e : result._rejectingRoutableQueues.entrySet())
        {
            if (!e.getKey().isDeleted())
            {
                _rejectingRoutableQueues.put(e.getKey(), e.getValue());
            }
        }
    }

    public void filter(Predicate<BaseQueue> predicate)
    {
        Iterator<BaseQueue> iter = _queues.iterator();
        while(iter.hasNext())
        {
            BaseQueue queue = iter.next();
            if(!predicate.test(queue))
            {
                iter.remove();
                _rejectingRoutableQueues.remove(queue);
            }
        }
    }

    public int send(ServerTransaction txn,
                    final Action<? super MessageInstance> postEnqueueAction)
    {
        if (containsReject(RejectType.LIMIT_EXCEEDED, RejectType.PRECONDITION_FAILED))
        {
            return 0;
        }

        final BaseQueue[] queues = _queues.toArray(new BaseQueue[_queues.size()]);
        txn.enqueue(_queues, _message, new ServerTransaction.EnqueueAction()
        {
            MessageReference _reference = _message.newReference();

            @Override
            public void postCommit(MessageEnqueueRecord... records)
            {
                try
                {
                    for(int i = 0; i < queues.length; i++)
                    {
                        queues[i].enqueue(_message, postEnqueueAction, records[i]);
                    }
                }
                finally
                {
                    _reference.release();
                }
            }

            @Override
            public void onRollback()
            {
                _reference.release();
            }
        });
        return _queues.size();
    }

    public boolean hasRoutes()
    {
        return !_queues.isEmpty();
    }

    public void addRejectReason(BaseQueue q, final RejectType rejectType, String reason)
    {
        _rejectingRoutableQueues.put(q, new RejectReason(rejectType, reason));
    }

    public boolean isRejected()
    {
        return !_rejectingRoutableQueues.isEmpty();
    }

    public boolean containsReject(RejectType... type)
    {
        for(RejectReason reason: _rejectingRoutableQueues.values())
        {
            for(RejectType t: type)
            {
                if (reason.getRejectType() == t)
                {
                    return true;
                }
            }
        }
        return false;
    }

    public String getRejectReason()
    {
        StringBuilder refusalMessages = new StringBuilder();
        for (RejectReason reason : _rejectingRoutableQueues.values())
        {
            if (refusalMessages.length() > 0)
            {
                refusalMessages.append(";");
            }
            refusalMessages.append(reason.getReason());
        }
        return refusalMessages.toString();
    }

    public int getNumberOfRoutes()
    {
        return _queues.size();
    }

    public Collection<BaseQueue> getRoutes()
    {
        return Collections.unmodifiableCollection(_queues);
    }

    private static class RejectReason
    {
        private final RejectType _rejectType;
        private final String _reason;

        private RejectReason(final RejectType rejectType, final String reason)
        {
            _rejectType = rejectType;
            _reason = reason;
        }

        private RejectType getRejectType()
        {
            return _rejectType;
        }

        public String getReason()
        {
            return _reason;
        }
    }
}
