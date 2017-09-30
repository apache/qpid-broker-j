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

import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.message.MessageInstance.ConsumerAcquiredState;
import org.apache.qpid.server.message.MessageInstance.EntryState;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.util.StateChangeListener;

public class DefinedGroupMessageGroupManager implements MessageGroupManager
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DefinedGroupMessageGroupManager.class);
    private final String _groupId;
    private final String _defaultGroup;
    private final Map<Object, Group> _groupMap = new HashMap<>();
    private final ConsumerResetHelper _resetHelper;

    private final class Group
    {
        private final Object _group;
        private final SortedSet<QueueEntry> _skippedEntries = new TreeSet<>();
        private QueueConsumer<?,?> _consumer;
        private int _activeCount;

        private Group(final Object key, final QueueConsumer<?,?> consumer)
        {
            _group = key;
            _consumer = consumer;
        }
        
        public boolean add()
        {
            if(_consumer != null)
            {
                _activeCount++;
                return true;
            }
            else
            {
                return false;
            }
        }
        
        void subtract(final QueueEntry entry, final boolean released)
        {
            if(!released)
            {
                _skippedEntries.remove(entry);
            }
            if(--_activeCount == 0)
            {
                _groupMap.remove(_group);
                if(!_skippedEntries.isEmpty())
                {
                    _resetHelper.resetSubPointersForGroups(_skippedEntries.first());
                    _skippedEntries.clear();
                }
                _consumer = null;
            }
        }

        @Override
        public boolean equals(final Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || getClass() != o.getClass())
            {
                return false;
            }

            Group group = (Group) o;

            return _group.equals(group._group);
        }

        @Override
        public int hashCode()
        {
            return _group.hashCode();
        }

        public boolean isValid()
        {
            return !(_consumer == null || (_activeCount == 0 && _consumer.isClosed()));
        }

        public QueueConsumer<?,?> getConsumer()
        {
            return _consumer;
        }

        @Override
        public String toString()
        {
            return "Group{" +
                    "_group=" + _group +
                    ", _consumer=" + _consumer +
                    ", _activeCount=" + _activeCount +
                    '}';
        }

        void addSkippedEntry(final QueueEntry entry)
        {
            _skippedEntries.add(entry);
        }
    }

    DefinedGroupMessageGroupManager(final String groupId, String defaultGroup, ConsumerResetHelper resetHelper)
    {
        _groupId = groupId;
        _defaultGroup = defaultGroup;
        _resetHelper = resetHelper;
    }
    
    @Override
    public synchronized boolean mightAssign(final QueueEntry entry, final QueueConsumer sub)
    {
        Object groupId = getKey(entry);

        Group group = _groupMap.get(groupId);
        final boolean possibleAssignment = group == null || !group.isValid() || group.getConsumer() == sub;
        if(!possibleAssignment)
        {
            group.addSkippedEntry(entry);
        }
        return possibleAssignment;
    }

    @Override
    public synchronized boolean acceptMessage(final QueueConsumer<?,?> sub, final QueueEntry entry)
    {
        return assignMessage(sub, entry) && entry.acquire(sub);
    }

    private boolean assignMessage(final QueueConsumer<?,?> sub, final QueueEntry entry)
    {
        Object groupId = getKey(entry);
        Group group = _groupMap.get(groupId);

        if(group == null || !group.isValid())
        {
            group = new Group(groupId, sub);

            _groupMap.put(groupId, group);

            // there's a small chance that the group became empty between the point at which getNextAvailable() was
            // called on the consumer, and when accept message is called... in that case we want to avoid delivering
            // out of order
            if(_resetHelper.isEntryAheadOfConsumer(entry, sub))
            {
                return false;
            }
        }

        QueueConsumer<?,?> assignedSub = group.getConsumer();

        if(assignedSub == sub)
        {
            entry.addStateChangeListener(new GroupStateChangeListener(group));
            return true;
        }
        else
        {
            group.addSkippedEntry(entry);
            return false;            
        }
    }

    @Override
    public synchronized QueueEntry findEarliestAssignedAvailableEntry(final QueueConsumer<?,?> sub)
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

            Object groupId = getKey(entry);

            Group group = _groupMap.get(groupId);
            if(group != null && group.getConsumer() == _sub)
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
    public void clearAssignments(final QueueConsumer<?,?> sub)
    {
    }
    
    private Object getKey(QueueEntry entry)
    {
        ServerMessage message = entry.getMessage();
        AMQMessageHeader messageHeader = message == null ? null : message.getMessageHeader();
        Object groupVal = messageHeader == null
                ? _defaultGroup
                : _groupId == null
                        ? messageHeader.getGroupId()
                        : messageHeader.getHeader(_groupId);
        if(groupVal == null)
        {
            groupVal = _defaultGroup;
        }
        return groupVal;
    }

    private class GroupStateChangeListener implements StateChangeListener<MessageInstance, EntryState>
    {
        private final Group _group;

        GroupStateChangeListener(final Group group)
        {
            _group = group;
        }

        @Override
        public void stateChanged(final MessageInstance entry, final EntryState oldState, final EntryState newState)
        {
            synchronized (DefinedGroupMessageGroupManager.this)
            {
                if(_group.isValid())
                {
                    if (isConsumerAcquiredStateForThisGroup(newState) && !isConsumerAcquiredStateForThisGroup(oldState))
                    {
                        _group.add();
                    }
                    else if (isConsumerAcquiredStateForThisGroup(oldState) && !isConsumerAcquiredStateForThisGroup(newState))
                    {
                        _group.subtract((QueueEntry) entry, newState.getState() == MessageInstance.State.AVAILABLE);
                    }
                }
                else
                {
                    entry.removeStateChangeListener(this);
                }
            }
        }

        private boolean isConsumerAcquiredStateForThisGroup(EntryState state)
        {
            return state instanceof ConsumerAcquiredState
                   && ((ConsumerAcquiredState) state).getConsumer() == _group.getConsumer();
        }
    }
}
