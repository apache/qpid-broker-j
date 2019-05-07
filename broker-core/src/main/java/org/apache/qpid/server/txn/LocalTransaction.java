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

import static org.apache.qpid.server.txn.TransactionObserver.NOOP_TRANSACTION_OBSERVER;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

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
import org.apache.qpid.server.util.ServerScopedRuntimeException;

/**
 * A concrete implementation of ServerTransaction where enqueue/dequeue
 * operations share a single long-lived transaction.
 *
 * The caller is responsible for invoking commit() (or rollback()) as necessary.
 */
public class LocalTransaction implements ServerTransaction
{
    enum LocalTransactionState
    {
        ACTIVE,
        ROLLBACK_ONLY,
        DISCHARGING,
        DISCHARGED
    }

    public interface LocalTransactionListener
    {
        void transactionCompleted(LocalTransaction tx);
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(LocalTransaction.class);

    private final List<Action> _postTransactionActions = new ArrayList<>();
    private final TransactionObserver _transactionObserver;

    private volatile Transaction _transaction;
    private final ActivityTimeAccessor _activityTime;
    private final MessageStore _transactionLog;
    private volatile long _txnStartTime = 0L;
    private volatile long _txnUpdateTime = 0l;
    private ListenableFuture<Runnable> _asyncTran;
    private volatile boolean _outstandingWork;
    private final LocalTransactionState _finalState;
    private final Set<LocalTransactionListener> _localTransactionListeners = new CopyOnWriteArraySet<>();
    private final AtomicReference<LocalTransactionState> _state = new AtomicReference<>(LocalTransactionState.ACTIVE);

    public LocalTransaction(MessageStore transactionLog)
    {
        this(transactionLog, NOOP_TRANSACTION_OBSERVER);
    }

    public LocalTransaction(MessageStore transactionLog, TransactionObserver transactionObserver)
    {
        this(transactionLog, null, transactionObserver, false);
    }

    public LocalTransaction(MessageStore transactionLog,
                            ActivityTimeAccessor activityTime,
                            TransactionObserver transactionObserver,
                            boolean resetable)
    {
        _transactionLog = transactionLog;
        _activityTime = activityTime == null ? () -> System.currentTimeMillis() : activityTime;
        _transactionObserver = transactionObserver == null ? NOOP_TRANSACTION_OBSERVER : transactionObserver;
        _finalState = resetable ? LocalTransactionState.ACTIVE : LocalTransactionState.DISCHARGED;
    }

    @Override
    public long getTransactionStartTime()
    {
        return _txnStartTime;
    }

    @Override
    public long getTransactionUpdateTime()
    {
        return _txnUpdateTime;
    }

    @Override
    public void addPostTransactionAction(Action postTransactionAction)
    {
        sync();
        _postTransactionActions.add(postTransactionAction);
    }

    @Override
    public void dequeue(MessageEnqueueRecord record, Action postTransactionAction)
    {
        sync();
        _outstandingWork = true;
        _postTransactionActions.add(postTransactionAction);
        initTransactionStartTimeIfNecessaryAndAdvanceUpdateTime();

        if(record != null)
        {
            try
            {
                if (LOGGER.isDebugEnabled())
                {
                    LOGGER.debug("Dequeue of message number " + record.getMessageNumber() + " from transaction log. Queue : " + record.getQueueId());
                }

                beginTranIfNecessary();
                _transaction.dequeueMessage(record);
            }
            catch(RuntimeException e)
            {
                tidyUpOnError(e);
            }
        }
    }

    @Override
    public void dequeue(Collection<MessageInstance> queueEntries, Action postTransactionAction)
    {
        sync();
        _outstandingWork = true;
        _postTransactionActions.add(postTransactionAction);
        initTransactionStartTimeIfNecessaryAndAdvanceUpdateTime();

        try
        {
            for(MessageInstance entry : queueEntries)
            {
                final MessageEnqueueRecord record = entry.getEnqueueRecord();
                if(record != null)
                {
                    if (LOGGER.isDebugEnabled())
                    {
                        LOGGER.debug("Dequeue of message number " + record.getMessageNumber() + " from transaction log. Queue : " + record.getQueueId());
                    }

                    beginTranIfNecessary();
                    _transaction.dequeueMessage(record);
                }
            }

        }
        catch(RuntimeException e)
        {
            tidyUpOnError(e);
        }
    }

    private void tidyUpOnError(RuntimeException e)
    {
        try
        {
            doRollbackActions();
        }
        finally
        {
            try
            {
                if (_transaction != null)
                {
                    _transaction.abortTran();
                }
            }
            finally
            {
                resetDetails();
            }
        }

        throw e;
    }
    private void beginTranIfNecessary()
    {

        if(_transaction == null)
        {
            _transaction = _transactionLog.newTransaction();
        }
    }

    @Override
    public void enqueue(TransactionLogResource queue, EnqueueableMessage message, EnqueueAction postTransactionAction)
    {
        sync();
        _outstandingWork = true;
        initTransactionStartTimeIfNecessaryAndAdvanceUpdateTime();
        _transactionObserver.onMessageEnqueue(this, message);
        if(queue.getMessageDurability().persist(message.isPersistent()))
        {
            try
            {
                if (LOGGER.isDebugEnabled())
                {
                    LOGGER.debug("Enqueue of message number " + message.getMessageNumber() + " to transaction log. Queue : " + queue.getName());
                }

                beginTranIfNecessary();
                final MessageEnqueueRecord record = _transaction.enqueueMessage(queue, message);
                if(postTransactionAction != null)
                {
                    final EnqueueAction underlying = postTransactionAction;

                    _postTransactionActions.add(new Action()
                    {
                        @Override
                        public void postCommit()
                        {
                            underlying.postCommit(record);
                        }

                        @Override
                        public void onRollback()
                        {
                            underlying.onRollback();
                        }
                    });
                }
            }
            catch(RuntimeException e)
            {
                if(postTransactionAction != null)
                {
                    final EnqueueAction underlying = postTransactionAction;

                    _postTransactionActions.add(new Action()
                    {
                        @Override
                        public void postCommit()
                        {

                        }

                        @Override
                        public void onRollback()
                        {
                            underlying.onRollback();
                        }
                    });
                }
                tidyUpOnError(e);
            }
        }
        else
        {
            if(postTransactionAction != null)
            {
                final EnqueueAction underlying = postTransactionAction;
                _postTransactionActions.add(new Action()
                {
                    @Override
                    public void postCommit()
                    {
                        underlying.postCommit((MessageEnqueueRecord)null);
                    }

                    @Override
                    public void onRollback()
                    {
                        underlying.onRollback();
                    }
                });
            }
        }
    }

    @Override
    public void enqueue(Collection<? extends BaseQueue> queues, EnqueueableMessage message, EnqueueAction postTransactionAction)
    {
        sync();
        _outstandingWork = true;
        initTransactionStartTimeIfNecessaryAndAdvanceUpdateTime();
        _transactionObserver.onMessageEnqueue(this, message);
        try
        {
            final MessageEnqueueRecord[] records = new MessageEnqueueRecord[queues.size()];
            int i = 0;
            for(BaseQueue queue : queues)
            {
                if(queue.getMessageDurability().persist(message.isPersistent()))
                {
                    if (LOGGER.isDebugEnabled())
                    {
                        LOGGER.debug("Enqueue of message number " + message.getMessageNumber() + " to transaction log. Queue : " + queue.getName() );
                    }

                    beginTranIfNecessary();
                    records[i] = _transaction.enqueueMessage(queue, message);

                }
                i++;
            }
            if(postTransactionAction != null)
            {
                final EnqueueAction underlying = postTransactionAction;

                _postTransactionActions.add(new Action()
                {
                    @Override
                    public void postCommit()
                    {
                        underlying.postCommit(records);
                    }

                    @Override
                    public void onRollback()
                    {
                        underlying.onRollback();
                    }
                });
                postTransactionAction = null;
            }
        }
        catch(RuntimeException e)
        {
            if(postTransactionAction != null)
            {
                final EnqueueAction underlying = postTransactionAction;

                _postTransactionActions.add(new Action()
                {
                    @Override
                    public void postCommit()
                    {

                    }

                    @Override
                    public void onRollback()
                    {
                        underlying.onRollback();
                    }
                });
            }
            tidyUpOnError(e);
        }
    }

    @Override
    public void commit()
    {
        commit(null);
    }

    @Override
    public void commit(Runnable immediateAction)
    {
        sync();
        if(!_state.compareAndSet(LocalTransactionState.ACTIVE, LocalTransactionState.DISCHARGING))
        {
            LocalTransactionState state = _state.get();
            String message = state == LocalTransactionState.ROLLBACK_ONLY
                    ? "Transaction has been marked as rollback only"
                    : String.format("Cannot commit transaction in state %s", state);
            throw new IllegalStateException(message);
        }

        try
        {
            if(_transaction != null)
            {
                _transaction.commitTran();
            }

            if(immediateAction != null)
            {
                immediateAction.run();
            }

            doPostTransactionActions();
        }
        finally
        {
            resetDetails();
        }
    }

    private void doRollbackActions()
    {
        for(Action action : _postTransactionActions)
        {
            action.onRollback();
        }
    }

    public void commitAsync(final Runnable deferred)
    {
        sync();
        if(!_state.compareAndSet(LocalTransactionState.ACTIVE, LocalTransactionState.DISCHARGING))
        {
            LocalTransactionState state = _state.get();
            String message = state == LocalTransactionState.ROLLBACK_ONLY
                    ? "Transaction has been marked as rollback only"
                    : String.format("Cannot commit transaction with state '%s'", state);
            throw new IllegalStateException(message);
        }

        if(_transaction != null)
        {

            Runnable action = new Runnable()
                                {
                                    @Override
                                    public void run()
                                    {
                                        try
                                        {
                                            doPostTransactionActions();
                                            deferred.run();
                                        }
                                        finally
                                        {
                                            resetDetails();
                                        }

                                    }
                                };
            _asyncTran = _transaction.commitTranAsync(action);

        }
        else
        {
                try
                {
                    doPostTransactionActions();
                    deferred.run();
                }
                finally
                {
                    resetDetails();
                }
        }
    }

    private void doPostTransactionActions()
    {
        LOGGER.debug("Beginning {} post transaction actions",  _postTransactionActions.size());

        for(int i = 0; i < _postTransactionActions.size(); i++)
        {
            _postTransactionActions.get(i).postCommit();
        }

        LOGGER.debug("Completed post transaction actions");

    }

    @Override
    public void rollback()
    {
        sync();
        if (!_state.compareAndSet(LocalTransactionState.ACTIVE, LocalTransactionState.DISCHARGING)
            && !_state.compareAndSet(LocalTransactionState.ROLLBACK_ONLY, LocalTransactionState.DISCHARGING)
            && _state.get() != LocalTransactionState.DISCHARGING)
        {
            throw new IllegalStateException(String.format("Cannot roll back transaction with state '%s'",
                                                          _state.get()));
        }

        try
        {
            if(_transaction != null)
            {
                _transaction.abortTran();
            }
        }
        finally
        {
            try
            {
                doRollbackActions();
            }
            finally
            {
                resetDetails();
            }
        }
    }

    public void sync()
    {
        if(_asyncTran != null)
        {
            boolean interrupted = false;
            try
            {
                while (true)
                {
                    try
                    {
                        _asyncTran.get().run();
                        break;
                    }
                    catch (InterruptedException e)
                    {
                        interrupted = true;
                    }

                }
            }
            catch(ExecutionException e)
            {
                if(e.getCause() instanceof RuntimeException)
                {
                    throw (RuntimeException)e.getCause();
                }
                else if(e.getCause() instanceof Error)
                {
                    throw (Error) e.getCause();
                }
                else
                {
                    throw new ServerScopedRuntimeException(e.getCause());
                }
            }
            if(interrupted)
            {
                Thread.currentThread().interrupt();
            }
            _asyncTran = null;
        }
    }

    private void initTransactionStartTimeIfNecessaryAndAdvanceUpdateTime()
    {
        long currentTime = _activityTime.getActivityTime();

        if (_txnStartTime == 0)
        {
            _txnStartTime = currentTime;
        }
        _txnUpdateTime = currentTime;
    }

    private void resetDetails()
    {
        _outstandingWork = false;
        _transactionObserver.onDischarge(this);
        _asyncTran = null;
        _transaction = null;
        _postTransactionActions.clear();
        _txnStartTime = 0L;
        _txnUpdateTime = 0;
        _state.set(_finalState);
        if (!_localTransactionListeners.isEmpty())
        {
            _localTransactionListeners.forEach(t -> t.transactionCompleted(this));
            _localTransactionListeners.clear();
        }
    }

    @Override
    public boolean isTransactional()
    {
        return true;
    }

    public interface ActivityTimeAccessor
    {
        long getActivityTime();
    }

    public boolean setRollbackOnly()
    {
        return _state.compareAndSet(LocalTransactionState.ACTIVE, LocalTransactionState.ROLLBACK_ONLY);
    }

    public boolean isRollbackOnly()
    {
        return _state.get() == LocalTransactionState.ROLLBACK_ONLY;
    }


    public boolean hasOutstandingWork()
    {
        return _outstandingWork;
    }

    public boolean isDischarged()
    {
        return _state.get() == LocalTransactionState.DISCHARGED;
    }

    public void addTransactionListener(LocalTransactionListener listener)
    {
        _localTransactionListeners.add(listener);
    }

    public void removeTransactionListener(LocalTransactionListener listener)
    {
        _localTransactionListeners.remove(listener);
    }

}
