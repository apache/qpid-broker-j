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
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.queue.BaseQueue;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.server.util.Action;

public class RoutingResult<M extends ServerMessage<? extends StorableMessageMetaData>>
{
    private static final Logger _logger = LoggerFactory.getLogger(RoutingResult.class);

    private final M _message;

    private final Set<BaseQueue> _queues = new HashSet<>();

    public RoutingResult(final M message)
    {
        _message = message;
    }

    public void addQueue(BaseQueue q)
    {
        if(q.isDeleted())
        {
            _logger.debug("Attempt to enqueue message onto deleted queue {}",  q.getName());
        }
        else
        {
            _queues.add(q);
        }
    }

    public void addQueues(Collection<? extends BaseQueue> queues)
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
                _logger.debug("Attempt to enqueue message onto deleted queue {}",  q.getName());

                queues.remove(q);
            }
        }

        _queues.addAll(queues);
    }

    public void add(RoutingResult<M> result)
    {
        addQueues(result._queues);
    }

    public int send(ServerTransaction txn,
                    final Action<? super MessageInstance> postEnqueueAction)
    {
        for(BaseQueue q : _queues)
        {
            if(!_message.isResourceAcceptable(q))
            {
                return 0;
            }
        }
        final BaseQueue[] baseQueues;

        if(_message.isReferenced())
        {
            ArrayList<BaseQueue> uniqueQueues = new ArrayList<>(_queues.size());
            for(BaseQueue q : _queues)
            {
                if(!_message.isReferenced(q))
                {
                    uniqueQueues.add(q);
                }
            }
            baseQueues = uniqueQueues.toArray(new BaseQueue[uniqueQueues.size()]);
        }
        else
        {
            baseQueues = _queues.toArray(new BaseQueue[_queues.size()]);
        }
        txn.enqueue(_queues, _message, new ServerTransaction.EnqueueAction()
        {
            MessageReference _reference = _message.newReference();

            public void postCommit(MessageEnqueueRecord... records)
            {
                try
                {
                    for(int i = 0; i < baseQueues.length; i++)
                    {
                        baseQueues[i].enqueue(_message, postEnqueueAction, records[i]);
                    }
                }
                finally
                {
                    _reference.release();
                }
            }

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
}
