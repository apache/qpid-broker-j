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
package org.apache.qpid.server.txn;

import java.util.Collection;
import java.util.List;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.message.EnqueueableMessage;
import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.queue.BaseQueue;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.Transaction;
import org.apache.qpid.server.store.TransactionLogResource;

/**
 * An implementation of ServerTransaction where each enqueue/dequeue
 * operation takes place within it own transaction.
 *
 * Since there is no long-lived transaction, the commit and rollback methods of
 * this implementation are empty.
 */
public class AsyncAutoCommitTransaction implements ServerTransaction
{
    static final String QPID_STRICT_ORDER_WITH_MIXED_DELIVERY_MODE = "qpid.strict_order_with_mixed_delivery_mode";

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncAutoCommitTransaction.class);

    private final MessageStore _messageStore;
    private final FutureRecorder _futureRecorder;

    //Set true to ensure strict ordering when enqueuing messages with mixed delivery mode, i.e. disable async persistence
    private boolean _strictOrderWithMixedDeliveryMode = Boolean.getBoolean(QPID_STRICT_ORDER_WITH_MIXED_DELIVERY_MODE);

    public interface FutureRecorder
    {
        void recordFuture(ListenableFuture<Void> future, Action action);

    }

    public AsyncAutoCommitTransaction(MessageStore transactionLog, FutureRecorder recorder)
    {
        _messageStore = transactionLog;
        _futureRecorder = recorder;
    }

    @Override
    public long getTransactionStartTime()
    {
        return 0L;
    }

    @Override
    public long getTransactionUpdateTime()
    {
        return 0L;
    }

    /**
     * Since AutoCommitTransaction have no concept of a long lived transaction, any Actions registered
     * by the caller are executed immediately.
     */
    @Override
    public void addPostTransactionAction(final Action immediateAction)
    {
        addFuture(Futures.<Void>immediateFuture(null), immediateAction);

    }

    @Override
    public void dequeue(MessageEnqueueRecord record, Action postTransactionAction)
    {
        Transaction txn = null;
        try
        {
            ListenableFuture<Void> future;
            if(record != null)
            {
                LOGGER.debug("Dequeue of message number {} from transaction log. Queue : {}", record.getMessageNumber(), record.getQueueId());

                txn = _messageStore.newTransaction();
                txn.dequeueMessage(record);
                future = txn.commitTranAsync((Void) null);

                txn = null;
            }
            else
            {
                future = Futures.immediateFuture(null);
            }
            addFuture(future, postTransactionAction);
            postTransactionAction = null;
        }
        finally
        {
            rollbackIfNecessary(postTransactionAction, txn);
        }

    }


    private void addFuture(final ListenableFuture<Void> future, final Action action)
    {
        if(action != null)
        {
            if(future.isDone())
            {
                action.postCommit();
            }
            else
            {
                _futureRecorder.recordFuture(future, action);
            }
        }
    }

    private void addEnqueueFuture(final ListenableFuture<Void> future, final Action action, boolean persistent)
    {
        if(action != null)
        {
            // For persistent messages, do not synchronously invoke postCommit even if the future  is completed.
            // Otherwise, postCommit (which actually does the enqueuing) might be called on successive messages out of order.
            if(future.isDone() && !persistent && !_strictOrderWithMixedDeliveryMode)
            {
                action.postCommit();
            }
            else
            {
                _futureRecorder.recordFuture(future, action);
            }
        }
    }

    @Override
    public void dequeue(Collection<MessageInstance> queueEntries, Action postTransactionAction)
    {
        Transaction txn = null;
        try
        {
            for(MessageInstance entry : queueEntries)
            {
                MessageEnqueueRecord record = entry.getEnqueueRecord();

                if(record != null)
                {
                    LOGGER.debug("Dequeue of message number {} from transaction log. Queue : {}", record.getMessageNumber(), record.getQueueId());

                    if(txn == null)
                    {
                        txn = _messageStore.newTransaction();
                    }

                    txn.dequeueMessage(record);
                }

            }
            ListenableFuture<Void> future;
            if(txn != null)
            {
                future = txn.commitTranAsync((Void) null);
                txn = null;
            }
            else
            {
                future = Futures.immediateFuture(null);
            }
            addFuture(future, postTransactionAction);
            postTransactionAction = null;
        }
        finally
        {
            rollbackIfNecessary(postTransactionAction, txn);
        }

    }


    @Override
    public void enqueue(TransactionLogResource queue, EnqueueableMessage message, EnqueueAction postTransactionAction)
    {
        Transaction txn = null;
        try
        {
            ListenableFuture<Void> future;
            final MessageEnqueueRecord enqueueRecord;
            if(queue.getMessageDurability().persist(message.isPersistent()))
            {
                LOGGER.debug("Enqueue of message number {} to transaction log. Queue : {}", message.getMessageNumber(), queue.getName());

                txn = _messageStore.newTransaction();
                enqueueRecord = txn.enqueueMessage(queue, message);
                future = txn.commitTranAsync(null);
                txn = null;
            }
            else
            {
                future = Futures.immediateFuture(null);
                enqueueRecord = null;
            }
            final EnqueueAction underlying = postTransactionAction;
            addEnqueueFuture(future, new Action()
            {
                @Override
                public void postCommit()
                {
                    underlying.postCommit(enqueueRecord);
                }

                @Override
                public void onRollback()
                {
                    underlying.onRollback();
                }
            }, message.isPersistent());
            postTransactionAction = null;
        }
        finally
        {
            final EnqueueAction underlying = postTransactionAction;

            rollbackIfNecessary(new Action()
            {
                @Override
                public void postCommit()
                {

                }

                @Override
                public void onRollback()
                {
                    if(underlying != null)
                    {
                        underlying.onRollback();
                    }
                }
            }, txn);
        }


    }

    @Override
    public void enqueue(Collection<? extends BaseQueue> queues, EnqueueableMessage message, EnqueueAction postTransactionAction)
    {
        Transaction txn = null;
        try
        {
            final MessageEnqueueRecord[] records = new MessageEnqueueRecord[queues.size()];
            int i = 0;
            for(BaseQueue queue : queues)
            {
                if (queue.getMessageDurability().persist(message.isPersistent()))
                {
                    LOGGER.debug("Enqueue of message number {} to transaction log. Queue : {}", message.getMessageNumber(), queue.getName());

                    if (txn == null)
                    {
                        txn = _messageStore.newTransaction();
                    }
                    records[i] = txn.enqueueMessage(queue, message);


                }
                i++;
            }

            ListenableFuture<Void> future;
            if (txn != null)
            {
                future = txn.commitTranAsync((Void) null);
                txn = null;
            }
            else
            {
                future = Futures.immediateFuture(null);
            }
            final EnqueueAction underlying = postTransactionAction;
            addEnqueueFuture(future, new Action()
            {
                @Override
                public void postCommit()
                {
                    if(underlying != null)
                    {
                        underlying.postCommit(records);
                    }
                }

                @Override
                public void onRollback()
                {
                     underlying.onRollback();
                }
            }, message.isPersistent());
            postTransactionAction = null;


        }
        finally
        {
            final EnqueueAction underlying = postTransactionAction;

            rollbackIfNecessary(new Action()
            {
                @Override
                public void postCommit()
                {

                }

                @Override
                public void onRollback()
                {
                    if(underlying != null)
                    {
                        underlying.onRollback();
                    }
                }
            }, txn);
        }

    }


    @Override
    public void commit(final Runnable immediatePostTransactionAction)
    {
        if(immediatePostTransactionAction != null)
        {
            addFuture(Futures.<Void>immediateFuture(null), new Action()
            {
                @Override
                public void postCommit()
                {
                    immediatePostTransactionAction.run();
                }

                @Override
                public void onRollback()
                {
                }
            });
        }
    }

    @Override
    public void commit()
    {
    }

    @Override
    public void rollback()
    {
    }

    @Override
    public boolean isTransactional()
    {
        return false;
    }

    private void rollbackIfNecessary(Action postTransactionAction, Transaction txn)
    {
        if (txn != null)
        {
            txn.abortTran();
        }
        if (postTransactionAction != null)
        {
            postTransactionAction.onRollback();
        }
    }

}
