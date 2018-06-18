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

import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import org.apache.qpid.server.store.MessageStore;

public class SyncLocalTransaction extends LocalTransaction implements ServerLocalTransaction
{
    enum LocalTransactionState
    {
        ACTIVE,
        ROLLBACK_ONLY,
        COMMITTING,
        ROLLING_BACK,
        COMPLETE
    }

    private final AtomicReference<LocalTransactionState>
            _state = new AtomicReference<>(LocalTransactionState.ACTIVE);
    private final SettableFuture<Void> _dischargeFuture = SettableFuture.create();

    public SyncLocalTransaction(final MessageStore transactionLog,
                                final ActivityTimeAccessor activityTime,
                                final TransactionObserver transactionObserver)
    {
        super(transactionLog, activityTime, transactionObserver);
    }

    @Override
    public void commit(final Runnable immediateAction)
    {
        if (_state.compareAndSet(LocalTransactionState.ACTIVE, LocalTransactionState.COMMITTING))
        {
            super.commit(immediateAction);
        }
        else
        {
            throw new IllegalStateException(String.format("Transaction cannot be committed in state '%s'",
                                                          _state.get()));
        }
    }

    @Override
    public void commitAsync(final Runnable deferred)
    {
        if (_state.compareAndSet(LocalTransactionState.ACTIVE, LocalTransactionState.COMMITTING))
        {
            super.commitAsync(deferred);
        }
        else
        {
            throw new IllegalStateException(String.format("Transaction cannot be committed in state '%s'",
                                                          _state.get()));
        }
    }

    @Override
    public void rollback()
    {
        if (_state.compareAndSet(LocalTransactionState.ACTIVE, LocalTransactionState.ROLLING_BACK)
            || _state.compareAndSet(LocalTransactionState.ROLLBACK_ONLY, LocalTransactionState.ROLLING_BACK))
        {
            super.rollback();
        }
        else
        {
            throw new IllegalStateException(String.format("Transaction cannot be rolled back in state '%s'",
                                                          _state.get()));
        }
    }

    @Override
    protected void resetDetails()
    {
        try
        {
            super.resetDetails();
        }
        finally
        {
            _state.set(LocalTransactionState.COMPLETE);
            _dischargeFuture.set(null);
        }
    }

    @Override
    public boolean setRollbackOnly()
    {
        return _state.compareAndSet(LocalTransactionState.ACTIVE, LocalTransactionState.ROLLBACK_ONLY)
               || isRollbackOnly();
    }

    @Override
    public boolean isRollbackOnly()
    {
        return _state.get() == LocalTransactionState.ROLLBACK_ONLY;
    }

    public boolean isComplete()
    {
        return _state.get() == LocalTransactionState.COMPLETE;
    }

    @Override
    public ListenableFuture<Void> getCompletionFuture()
    {
        return _dischargeFuture;
    }
}
