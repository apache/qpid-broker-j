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
package org.apache.qpid.server.transfer;

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.regex.Pattern;

import org.apache.qpid.server.message.AcquiringMessageInstanceConsumer;
import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.queue.QueueEntry;

public class TransferQueueConsumer implements AcquiringMessageInstanceConsumer<TransferQueueConsumer, TransferTarget>
{
    private final TransferQueueImpl _transferQueue;
    private final TransferTarget _target;
    private final String _name;

    private final ConcurrentLinkedQueue<TransferQueueEntry> _entries = new ConcurrentLinkedQueue<>();
    private final Pattern _matchPattern;
    private volatile QueueContext _queueContext;


    private final MessageInstance.StealableConsumerAcquiredState<TransferQueueConsumer>
            _owningState = new MessageInstance.StealableConsumerAcquiredState<>(this);
    private final Object _identifier = new Object();


    TransferQueueConsumer(final TransferQueueImpl transferQueue,
                          final TransferTarget target,
                          final String consumerName)
    {
        _transferQueue = transferQueue;
        _target = target;
        _name = consumerName;

        Collection<String> globalAddressDomains = _target.getGlobalAddressDomains();
        StringBuilder matchPattern = new StringBuilder();
        boolean isFirst = true;
        for(String domain : globalAddressDomains)
        {
            if(isFirst)
            {
                isFirst = false;
            }
            else
            {
                matchPattern.append('|');
            }
            matchPattern.append('(');
            matchPattern.append(Pattern.quote(domain.endsWith("/") ? domain : (domain + "/")));
            matchPattern.append(".*)");
        }
        _matchPattern = Pattern.compile(matchPattern.toString());
    }

    boolean hasInterest(final QueueEntry entry)
    {
        String initialRoutingAddress = entry.getMessage().getInitialRoutingAddress();
        boolean matches = _matchPattern.matcher(initialRoutingAddress).matches();
        return matches;
    }

    public boolean processPending()
    {
        if(!isSuspended())
        {
            TransferQueueEntry entry = _transferQueue.getNextAvailableEntry(this);
            if(entry != null && !wouldSuspend(entry))
            {
                if (!entry.acquire(this))
                {
                    // restore credit here that would have been taken away by wouldSuspend since we didn't manage
                    // to acquire the entry for this consumer
                    restoreCredit(entry);
                }
                else
                {
                    _transferQueue.setLastSeenEntry(this, entry);

                    send(entry);
                    return true;
                }
            }

        }
        return false;
    }

    void setQueueContext(final QueueContext queueContext)
    {
        _queueContext = queueContext;
    }

    QueueContext getQueueContext()
    {
        return _queueContext;
    }

    @Override
    public MessageInstance.StealableConsumerAcquiredState<TransferQueueConsumer> getOwningState()
    {
        return _owningState;
    }

    public boolean isSuspended()
    {
        return _target.isSuspended();
    }

    boolean wouldSuspend(final TransferQueueEntry entry)
    {
        return _target.wouldSuspend(entry);
    }

    public TransferTarget getTarget()
    {
        return _target;
    }

    @Override
    public void acquisitionRemoved(final QueueEntry queueEntry)
    {

    }

    @Override
    public long getConsumerNumber()
    {
        return 0;
    }

    @Override
    public boolean resend(final QueueEntry queueEntry)
    {
        return false;
    }

    @Override
    public boolean isClosed()
    {
        return false;
    }

    @Override
    public boolean acquires()
    {
        return true;
    }

    @Override
    public String getName()
    {
        return "$transfer";
    }

    @Override
    public void close()
    {

    }

    @Override
    public void flush()
    {

    }

    @Override
    public void externalStateChange()
    {

    }

    @Override
    public Object getIdentifier()
    {
        return _identifier;
    }

    @Override
    public boolean hasAvailableMessages()
    {
        return _transferQueue.hasAvailableMessages(this);
    }

    @Override
    public void pullMessage()
    {

    }

    public void send(final TransferQueueEntry entry)
    {
        _target.send(entry);
    }

    public void restoreCredit(final TransferQueueEntry entry)
    {
        _target.restoreCredit(entry.getMessage());
    }

    public void notifyWork()
    {
        _target.notifyWork();
    }
}
