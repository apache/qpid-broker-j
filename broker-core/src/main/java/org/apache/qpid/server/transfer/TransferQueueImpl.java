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

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.qpid.server.message.BaseMessageInstance;
import org.apache.qpid.server.message.InstanceProperties;
import org.apache.qpid.server.message.MessageReference;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.queue.QueueEntry;
import org.apache.qpid.server.store.MessageDurability;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.txn.AutoCommitTransaction;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.virtualhost.VirtualHostUnavailableException;

public class TransferQueueImpl implements TransferQueue
{
    private static final UUID TRANSFER_QUEUE_ID = UUID.nameUUIDFromBytes("$transfer".getBytes(StandardCharsets.UTF_8));

    private static final int RECOVERING = 1;
    private static final int COMPLETING_RECOVERY = 2;
    private static final int RECOVERED = 3;

    private final AtomicInteger _recovering = new AtomicInteger(RECOVERING);
    private final AtomicInteger _enqueuingWhileRecovering = new AtomicInteger(0);

    private final ConcurrentLinkedQueue<EnqueueRequest> _postRecoveryQueue = new ConcurrentLinkedQueue<>();

    private final VirtualHost<?> _virtualHost;

    private final TransferQueueEntryList _queueEntryList;
    private Collection<TransferQueueConsumer> _consumers = new CopyOnWriteArrayList<>();


    public TransferQueueImpl(final VirtualHost<?> virtualHost)
    {
        _virtualHost = virtualHost;
        _queueEntryList = new TransferQueueEntryList(this);
    }

    @Override
    public void enqueue(final ServerMessage message,
                        final Action<? super BaseMessageInstance> action,
                        final MessageEnqueueRecord enqueueRecord)
    {
        if(_recovering.get() != RECOVERED)
        {
            _enqueuingWhileRecovering.incrementAndGet();

            boolean addedToRecoveryQueue;
            try
            {
                if(addedToRecoveryQueue = (_recovering.get() == RECOVERING))
                {
                    _postRecoveryQueue.add(new EnqueueRequest(message, action, enqueueRecord));
                }
            }
            finally
            {
                _enqueuingWhileRecovering.decrementAndGet();
            }

            if(!addedToRecoveryQueue)
            {
                while(_recovering.get() != RECOVERED)
                {
                    Thread.yield();
                }
                doEnqueue(message, action, enqueueRecord);
            }
        }
        else
        {
            doEnqueue(message, action, enqueueRecord);
        }


    }

    @Override
    public void recover(final ServerMessage<?> message, final MessageEnqueueRecord enqueueRecord)
    {
        doEnqueue(message, null, enqueueRecord);
    }

    @Override
    public final void completeRecovery()
    {
        if(_recovering.compareAndSet(RECOVERING, COMPLETING_RECOVERY))
        {
            while(_enqueuingWhileRecovering.get() != 0)
            {
                Thread.yield();
            }

            // at this point we can assert that any new enqueue to the queue will not try to put into the post recovery
            // queue (because the state is no longer RECOVERING, but also no threads are currently trying to enqueue
            // because the _enqueuingWhileRecovering count is 0.

            enqueueFromPostRecoveryQueue();

            _recovering.set(RECOVERED);

        }
    }

    @Override
    public VirtualHost<?> getVirtualHost()
    {
        return _virtualHost;
    }

    private void enqueueFromPostRecoveryQueue()
    {
        while(!_postRecoveryQueue.isEmpty())
        {
            EnqueueRequest request = _postRecoveryQueue.poll();
            MessageReference<?> messageReference = request.getMessage();
            doEnqueue(messageReference.getMessage(), request.getAction(), request.getEnqueueRecord());
            messageReference.release();
        }
    }



    protected void doEnqueue(final ServerMessage message, final Action<? super BaseMessageInstance> action, MessageEnqueueRecord enqueueRecord)
    {
        final TransferQueueEntry entry = (TransferQueueEntry) _queueEntryList.add(message, enqueueRecord);
        for (TransferQueueConsumer consumer : getConsumers())
        {
            if (consumer.hasInterest(entry))
            {
                consumer.notifyWork();
            }
        }
    }

    private Collection<TransferQueueConsumer> getConsumers()
    {
        return _consumers;
    }

    @Override
    public TransferQueueConsumer addConsumer(final TransferTarget target,
                                             final String consumerName)
    {



        TransferQueueConsumer consumer = new TransferQueueConsumer(this,
                                                                   target,
                                                                   consumerName);

        QueueContext queueContext = new QueueContext(_queueEntryList.getHead());
        consumer.setQueueContext(queueContext);
        _consumers.add(consumer);
        consumer.notifyWork();


  /*      consumer.setStateListener(this);
        QueueContext queueContext;
        if(filters == null || !filters.startAtTail())
        {
            queueContext = new QueueContext(getEntries().getHead());
        }
        else
        {
            queueContext = new QueueContext(getEntries().getTail());
        }
        consumer.setQueueContext(queueContext);

        if (!isDeleted())
        {
            _consumerList.add(consumer);

            if (isDeleted())
            {
                consumer.queueDeleted();
            }
        }
        else
        {
            // TODO
        }

        consumer.addChangeListener(_deletedChildListener);

        deliverAsync();
*/
        return consumer;
    }


    void setLastSeenEntry(final TransferQueueConsumer sub, final TransferQueueEntry entry)
    {
        QueueContext subContext = sub.getQueueContext();
        if (subContext != null)
        {
            QueueEntry releasedEntry = subContext.getReleasedEntry();

            QueueContext._lastSeenUpdater.set(subContext, entry);
            if(releasedEntry == entry)
            {
                QueueContext._releasedUpdater.compareAndSet(subContext, releasedEntry, null);
            }
        }
    }

    TransferQueueEntry getNextAvailableEntry(final TransferQueueConsumer sub)
    {
        QueueContext context = sub.getQueueContext();
        if(context != null)
        {
            QueueEntry lastSeen = context.getLastSeenEntry();
            QueueEntry releasedNode = context.getReleasedEntry();

            TransferQueueEntry node =
                    (TransferQueueEntry) ((releasedNode != null && lastSeen.compareTo(releasedNode) >= 0)
                                        ? releasedNode : _queueEntryList.next(lastSeen));

            boolean expired = false;
            while (node != null
                   && (!node.isAvailable()
                       || (expired = node.expired())
                       || !sub.hasInterest(node)))
            {
                if (expired)
                {
                    expired = false;
                    if (node.acquire())
                    {
                        dequeueEntry(node);
                    }
                }

                if(QueueContext._lastSeenUpdater.compareAndSet(context, lastSeen, node))
                {
                    QueueContext._releasedUpdater.compareAndSet(context, releasedNode, null);
                }

                lastSeen = context.getLastSeenEntry();
                releasedNode = context.getReleasedEntry();
                node = (TransferQueueEntry) ((releasedNode != null && lastSeen.compareTo(releasedNode) >= 0)
                                        ? releasedNode
                                        : _queueEntryList.next(lastSeen));
            }
            return node;
        }
        else
        {
            return null;
        }
    }

    private void dequeueEntry(final QueueEntry node)
    {
        ServerTransaction txn = new AutoCommitTransaction(_virtualHost.getMessageStore());
        dequeueEntry(node, txn);
    }

    private void dequeueEntry(final QueueEntry node, ServerTransaction txn)
    {
        txn.dequeue(node.getEnqueueRecord(),
                    new ServerTransaction.Action()
                    {

                        public void postCommit()
                        {
                            node.delete();
                        }

                        public void onRollback()
                        {

                        }
                    });
    }

    @Override
    public boolean isDurable()
    {
        return true;
    }

    @Override
    public boolean isDeleted()
    {
        return false;
    }

    @Override
    public String getName()
    {
        return "$transfer";
    }

    @Override
    public <M extends ServerMessage<? extends StorableMessageMetaData>> int send(final M message,
                                                                                 final String routingAddress,
                                                                                 final InstanceProperties instanceProperties,
                                                                                 final ServerTransaction txn,
                                                                                 final Action<? super BaseMessageInstance> postEnqueueAction)
    {
        if (_virtualHost.getState() != State.ACTIVE)
        {
            throw new VirtualHostUnavailableException(this._virtualHost);
        }

        if(!message.isReferenced(this))
        {
            txn.enqueue(this, message, new ServerTransaction.EnqueueAction()
            {
                MessageReference _reference = message.newReference();

                public void postCommit(MessageEnqueueRecord... records)
                {
                    try
                    {
                        TransferQueueImpl.this.enqueue(message, postEnqueueAction, records[0]);
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
            return 1;
        }
        else
        {
            return 0;
        }

    }

    @Override
    public UUID getId()
    {
        return TRANSFER_QUEUE_ID;
    }

    @Override
    public MessageDurability getMessageDurability()
    {
        return MessageDurability.DEFAULT;
    }

    private static class EnqueueRequest
    {
        private final MessageReference<?> _message;
        private final Action<? super BaseMessageInstance> _action;
        private final MessageEnqueueRecord _enqueueRecord;

        public EnqueueRequest(final ServerMessage message,
                              final Action<? super BaseMessageInstance> action,
                              final MessageEnqueueRecord enqueueRecord)
        {
            _enqueueRecord = enqueueRecord;
            _message = message.newReference();
            _action = action;
        }

        public MessageReference<?> getMessage()
        {
            return _message;
        }

        public Action<? super BaseMessageInstance> getAction()
        {
            return _action;
        }

        public MessageEnqueueRecord getEnqueueRecord()
        {
            return _enqueueRecord;
        }
    }

}
