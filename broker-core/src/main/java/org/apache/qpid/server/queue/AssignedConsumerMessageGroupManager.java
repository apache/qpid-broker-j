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

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.message.AMQMessageHeader;


public class AssignedConsumerMessageGroupManager implements MessageGroupManager
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AssignedConsumerMessageGroupManager.class);


    private final String _groupId;
    private final ConcurrentMap<Integer, QueueConsumer<?,?>> _groupMap = new ConcurrentHashMap<>();
    private final int _groupMask;

    AssignedConsumerMessageGroupManager(final String groupId, final int maxGroups)
    {
        _groupId = groupId;
        _groupMask = pow2(maxGroups)-1;
    }

    private static int pow2(final int i)
    {
        int val = 1;
        while(val < i)
        {
            val<<=1;
        }
        return val;
    }

    @Override
    public boolean mightAssign(final QueueEntry entry, QueueConsumer sub)
    {
        Object groupVal = getGroupValue(entry);

        if(groupVal == null)
        {
            return true;
        }
        else
        {
            QueueConsumer<?,?> assignedSub = _groupMap.get(groupVal.hashCode() & _groupMask);
            return assignedSub == null || assignedSub == sub;
        }
    }

    @Override
    public boolean acceptMessage(QueueConsumer<?,?> sub, QueueEntry entry)
    {
        return assignMessage(sub, entry) && entry.acquire(sub);
    }

    private Object getGroupValue(final QueueEntry entry)
    {
        final AMQMessageHeader messageHeader = entry.getMessage().getMessageHeader();
        return _groupId == null ? messageHeader.getGroupId() : messageHeader.getHeader(_groupId);
    }

    private boolean assignMessage(QueueConsumer<?,?> sub, QueueEntry entry)
    {
        Object groupVal = getGroupValue(entry);
        if(groupVal == null)
        {
            return true;
        }
        else
        {
            Integer group = groupVal.hashCode() & _groupMask;
            QueueConsumer<?,?> assignedSub = _groupMap.get(group);
            if(assignedSub == sub)
            {
                return true;
            }
            else
            {
                if(assignedSub == null)
                {
                    LOGGER.debug("Assigning group {} to sub {}", groupVal, sub);
                    assignedSub = _groupMap.putIfAbsent(group, sub);
                    return assignedSub == null || assignedSub == sub;
                }
                else
                {
                    return false;
                }
            }
        }
    }

    @Override
    public QueueEntry findEarliestAssignedAvailableEntry(QueueConsumer<?,?> sub)
    {
        EntryFinder visitor = new EntryFinder(sub);
        sub.getQueue().visit(visitor);
        return visitor.getEntry();
    }

    private class EntryFinder implements QueueEntryVisitor
    {
        private QueueEntry _entry;
        private QueueConsumer<?,?> _sub;

        EntryFinder(final QueueConsumer<?, ?> sub)
        {
            _sub = sub;
        }

        @Override
        public boolean visit(final QueueEntry entry)
        {
            if(!entry.isAvailable())
            {
                return false;
            }

            Object groupVal = getGroupValue(entry);
            if(groupVal == null)
            {
                return false;
            }

            Integer group = groupVal.hashCode() & _groupMask;
            QueueConsumer<?,?> assignedSub = _groupMap.get(group);
            if(assignedSub == _sub)
            {
                _entry = entry;
                return true;
            }
            else
            {
                return false;
            }
        }

        public QueueEntry getEntry()
        {
            return _entry;
        }
    }

    @Override
    public void clearAssignments(QueueConsumer<?,?> sub)
    {
        Iterator<QueueConsumer<?,?>> subIter = _groupMap.values().iterator();
        while(subIter.hasNext())
        {
            if(subIter.next() == sub)
            {
                subIter.remove();
            }
        }
    }
}
