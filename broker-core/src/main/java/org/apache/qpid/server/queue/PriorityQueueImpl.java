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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import org.apache.qpid.server.filter.JMSSelectorFilter;
import org.apache.qpid.server.filter.SelectorParsingException;
import org.apache.qpid.server.filter.selector.ParseException;
import org.apache.qpid.server.filter.selector.TokenMgrError;
import org.apache.qpid.server.logging.LogMessage;
import org.apache.qpid.server.logging.messages.QueueMessages;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.message.ServerMessageMutator;
import org.apache.qpid.server.message.ServerMessageMutatorFactory;
import org.apache.qpid.server.model.LifetimePolicy;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.txn.LocalTransaction;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;

public class PriorityQueueImpl extends OutOfOrderQueue<PriorityQueueImpl> implements PriorityQueue<PriorityQueueImpl>
{

    private PriorityQueueList _entries;

    @ManagedAttributeField
    private int _priorities;

    @ManagedObjectFactoryConstructor
    public PriorityQueueImpl(Map<String, Object> attributes, QueueManagingVirtualHost<?> virtualHost)
    {
        super(attributes, virtualHost);
    }

    @Override
    protected void onOpen()
    {
        super.onOpen();
        _entries = PriorityQueueList.newInstance(this);
    }

    @Override
    public int getPriorities()
    {
        return _priorities;
    }

    @Override
    PriorityQueueList getEntries()
    {
        return _entries;
    }

    @Override
    protected LogMessage getCreatedLogMessage()
    {
        String ownerString = getOwner();
        return QueueMessages.CREATED(getId().toString(),
                                     ownerString,
                                     getPriorities(),
                                     ownerString != null,
                                     getLifetimePolicy() != LifetimePolicy.PERMANENT,
                                     isDurable(),
                                     !isDurable(),
                                     true);
    }

    @Override
    public long reenqueueMessageForPriorityChange(final long messageId, final int newPriority)
    {
        final QueueEntry entry = getMessageOnTheQueue(messageId);
        if (entry != null)
        {
            final ServerMessage message = entry.getMessage();
            if (message != null && message.getMessageHeader().getPriority() != newPriority && entry.acquire())
            {
                final MessageStore store = getVirtualHost().getMessageStore();
                final LocalTransaction txn = new LocalTransaction(store);
                final long newMessageId = reenqueueEntryWithPriority(entry, txn, (byte) newPriority);
                txn.commit();
                return newMessageId;
            }
        }
        return -1;
    }

    @Override
    public List<Long> reenqueueMessagesForPriorityChange(final String selector, final int newPriority)
    {
        final JMSSelectorFilter filter;
        try
        {
            filter = selector == null ? null : new JMSSelectorFilter(selector);
        }
        catch (ParseException | SelectorParsingException | TokenMgrError e)
        {
            throw new IllegalArgumentException("Cannot parse selector \"" + selector + "\"", e);
        }

        final List<Long> messageIds =
                reenqueueEntriesForPriorityChange(entry -> filter == null || filter.matches(entry.asFilterable()),
                                                  newPriority);
        return Collections.unmodifiableList(messageIds);
    }

    private List<Long> reenqueueEntriesForPriorityChange(final Predicate<QueueEntry> condition,
                                                         final int newPriority)
    {
        final Predicate<QueueEntry> isNotNullMessageAndPriorityDiffers = entry -> {
            final ServerMessage message = entry.getMessage();
            return message != null && message.getMessageHeader().getPriority() != newPriority;
        };
        return handleMessagesWithinStoreTransaction(isNotNullMessageAndPriorityDiffers.and(condition),
                                                    (txn, entry) -> reenqueueEntryWithPriority(entry, txn, (byte) newPriority));
    }

    private long reenqueueEntryWithPriority(final QueueEntry entry,
                                            final ServerTransaction txn,
                                            final byte newPriority)
    {
        txn.dequeue(entry.getEnqueueRecord(),
                    new ServerTransaction.Action()
                    {
                        @Override
                        public void postCommit()
                        {
                            entry.delete();
                        }

                        @Override
                        public void onRollback()
                        {
                            entry.release();
                        }
                    });

        final ServerMessage newMessage = createMessageWithPriority(entry.getMessage(), newPriority);
        txn.enqueue(this,
                    newMessage,
                    new ServerTransaction.EnqueueAction()
                    {
                        @Override
                        public void postCommit(MessageEnqueueRecord... records)
                        {
                            PriorityQueueImpl.this.enqueue(newMessage, null, records[0]);
                        }

                        @Override
                        public void onRollback()
                        {
                            // noop
                        }
                    });
        return newMessage.getMessageNumber();
    }

    private List<Long> handleMessagesWithinStoreTransaction(final Predicate<QueueEntry> entryMatchCondition,
                                                            final BiFunction<ServerTransaction, QueueEntry, Long> handle)
    {
        final MessageStore store = getVirtualHost().getMessageStore();
        final LocalTransaction txn = new LocalTransaction(store);
        final List<Long> result = new ArrayList<>();
        visit(entry -> {
            if (entryMatchCondition.test(entry) && entry.acquire())
            {
                result.add(handle.apply(txn, entry));
            }
            return false;
        });
        txn.commit();
        return result;
    }

    private ServerMessage createMessageWithPriority(final ServerMessage message, final byte newPriority)
    {
        final ServerMessageMutator messageMutator =
                ServerMessageMutatorFactory.createMutator(message, getVirtualHost().getMessageStore());
        messageMutator.setPriority(newPriority);
        return messageMutator.create();
    }
}
