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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.message.EnqueueableMessage;
import org.apache.qpid.server.session.AMQPSession;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.server.store.Transaction;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.server.util.Action;

public class DtxBranch
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DtxBranch.class);

    private final Xid _xid;
    private final List<ServerTransaction.Action> _postTransactionActions = new ArrayList<ServerTransaction.Action>();
    private       State                          _state = State.ACTIVE;
    private long _timeout;
    private Map<AMQPSession<?,?>, State> _associatedSessions = new HashMap<>();
    private final List<EnqueueRecord> _enqueueRecords = new ArrayList<>();
    private final List<DequeueRecord> _dequeueRecords = new ArrayList<>();

    private Transaction _transaction;
    private long _expiration;
    private ScheduledFuture<?> _timeoutFuture;
    private final DtxRegistry _dtxRegistry;
    private Transaction.StoredXidRecord _storedXidRecord;


    public enum State
    {
        ACTIVE,
        PREPARED,
        TIMEDOUT,
        SUSPENDED,
        FORGOTTEN,
        HEUR_COM,
        HEUR_RB,
        ROLLBACK_ONLY
    }

    public DtxBranch(Xid xid, DtxRegistry dtxRegistry)
    {
        _xid = xid;
        _dtxRegistry = dtxRegistry;
    }

    public DtxBranch(Transaction.StoredXidRecord storedXidRecord, DtxRegistry dtxRegistry)
    {
        this(new Xid(storedXidRecord.getFormat(), storedXidRecord.getGlobalId(), storedXidRecord.getBranchId()), dtxRegistry);
        _storedXidRecord = storedXidRecord;
    }

    public Xid getXid()
    {
        return _xid;
    }

    public State getState()
    {
        return _state;
    }

    public void setState(State state)
    {
        _state = state;
    }

    public long getTimeout()
    {
        return _timeout;
    }

    public void setTimeout(long timeout)
    {
        LOGGER.debug("Setting timeout to {}s for DtxBranch {}", timeout, _xid);

        if(_timeoutFuture != null)
        {
            LOGGER.debug("Attempting to cancel previous timeout task future for DtxBranch {}", _xid);

            boolean succeeded = _timeoutFuture.cancel(false);

            LOGGER.debug("Cancelling previous timeout task {} for DtxBranch {}", (succeeded ? "succeeded" : "failed"), _xid);
        }

        _timeout = timeout;
        _expiration = timeout == 0 ? 0 : System.currentTimeMillis() + (1000 * timeout);

        if(_timeout == 0)
        {
            _timeoutFuture = null;
        }
        else
        {
            long delay = 1000*_timeout;

            LOGGER.debug("Scheduling timeout and rollback after {}s for DtxBranch {}", delay/1000, _xid);

            _timeoutFuture = _dtxRegistry.scheduleTask(delay, new Runnable()
            {
                @Override
                public void run()
                {
                    LOGGER.debug("Timing out DtxBranch {}", _xid);

                    setState(State.TIMEDOUT);
                    rollback();
                }
            });
        }
    }

    public boolean expired()
    {
        return _timeout != 0 && _expiration < System.currentTimeMillis();
    }

    public synchronized boolean isAssociated(AMQPSession<?,?> session)
    {
        return _associatedSessions.containsKey(session);
    }

    public synchronized boolean hasAssociatedSessions()
    {
        return !_associatedSessions.isEmpty();
    }


    public synchronized boolean hasAssociatedActiveSessions()
    {
        if(hasAssociatedSessions())
        {
            for(State state : _associatedSessions.values())
            {
                if(state != State.SUSPENDED)
                {
                    return true;
                }
            }
        }
        return false;
    }

    public synchronized void clearAssociations()
    {
        _associatedSessions.clear();
    }

    synchronized boolean associateSession(AMQPSession<?,?> associatedSession)
    {
        return _associatedSessions.put(associatedSession, State.ACTIVE) != null;
    }

    synchronized boolean disassociateSession(AMQPSession<?,?> associatedSession)
    {
        return _associatedSessions.remove(associatedSession) != null;
    }

    public synchronized boolean resumeSession(AMQPSession<?,?> session)
    {
        if(_associatedSessions.containsKey(session) && _associatedSessions.get(session) == State.SUSPENDED)
        {
            _associatedSessions.put(session, State.ACTIVE);
            return true;
        }
        return false;
    }

    public synchronized boolean suspendSession(AMQPSession<?,?> session)
    {
        if(_associatedSessions.containsKey(session) && _associatedSessions.get(session) == State.ACTIVE)
        {
            _associatedSessions.put(session, State.SUSPENDED);
            return true;
        }
        return false;
    }

    public void prepare() throws StoreException
    {
        LOGGER.debug("Performing prepare for DtxBranch {}", _xid);

        Transaction txn = _dtxRegistry.getMessageStore().newTransaction();
        _storedXidRecord = txn.recordXid(_xid.getFormat(),
                      _xid.getGlobalId(),
                      _xid.getBranchId(),
                      _enqueueRecords.toArray(new EnqueueRecord[_enqueueRecords.size()]),
                      _dequeueRecords.toArray(new DequeueRecord[_dequeueRecords.size()]));
        txn.commitTran();

        prePrepareTransaction();
    }

    public synchronized void rollback() throws StoreException
    {
        LOGGER.debug("Performing rollback for DtxBranch {}", _xid);

        if(_timeoutFuture != null)
        {
            LOGGER.debug("Attempting to cancel previous timeout task future for DtxBranch {}", _xid);

            boolean succeeded = _timeoutFuture.cancel(false);
            _timeoutFuture = null;

            LOGGER.debug("Cancelling previous timeout task {} for DtxBranch {}", (succeeded ? "succeeded" : "failed"), _xid);
        }

        if(_transaction != null)
        {
            // prepare has previously been called

            Transaction txn = _dtxRegistry.getMessageStore().newTransaction();
            txn.removeXid(_storedXidRecord);
            txn.commitTran();

            _transaction.abortTran();
        }

        for(ServerTransaction.Action action : _postTransactionActions)
        {
            action.onRollback();
        }
        _postTransactionActions.clear();
    }

    public void commit() throws StoreException
    {
        LOGGER.debug("Performing commit for DtxBranch {}", _xid);

        if(_timeoutFuture != null)
        {
            LOGGER.debug("Attempting to cancel previous timeout task future for DtxBranch {}", _xid);

            boolean succeeded = _timeoutFuture.cancel(false);
            _timeoutFuture = null;

            LOGGER.debug("Cancelling previous timeout task {} for DtxBranch {}", (succeeded ? "succeeded" : "failed"), _xid);
        }

        if(_transaction == null)
        {
            prePrepareTransaction();
        }
        else
        {
            _transaction.removeXid(_storedXidRecord);
        }
        _transaction.commitTran();

        for(ServerTransaction.Action action : _postTransactionActions)
        {
            action.postCommit();
        }
        _postTransactionActions.clear();
    }

    public void prePrepareTransaction() throws StoreException
    {
        _transaction = _dtxRegistry.getMessageStore().newTransaction();

        for(final EnqueueRecord enqueue : _enqueueRecords)
        {
            final MessageEnqueueRecord record;
            if(enqueue.isDurable())
            {
                record = _transaction.enqueueMessage(enqueue.getResource(), enqueue.getMessage());

            }
            else
            {
                record = null;
            }
            enqueue.getEnqueueAction().performAction(record);
        }


        for(DequeueRecord dequeue : _dequeueRecords)
        {
            _transaction.dequeueMessage(dequeue.getEnqueueRecord());
        }
    }


    public void addPostTransactionAction(ServerTransaction.Action postTransactionAction)
    {
        _postTransactionActions.add(postTransactionAction);
    }


    public void dequeue(MessageEnqueueRecord record)
    {
        if(record != null)
        {
            _dequeueRecords.add(new DequeueRecord(record));
        }
    }

    public void enqueue(TransactionLogResource queue,
                        EnqueueableMessage message,
                        final Action<MessageEnqueueRecord> enqueueAction)
    {
        _enqueueRecords.add(new EnqueueRecord(queue, message, enqueueAction));
    }

    private static class DequeueRecord implements Transaction.DequeueRecord
    {
        private final MessageEnqueueRecord _enqueueRecord;

        public DequeueRecord(MessageEnqueueRecord enqueueRecord)
        {
            _enqueueRecord = enqueueRecord;
        }

        @Override
        public MessageEnqueueRecord getEnqueueRecord()
        {
            return _enqueueRecord;
        }


    }

    private static class EnqueueRecord implements Transaction.EnqueueRecord
    {
        private final TransactionLogResource _resource;
        private final EnqueueableMessage _message;

        private final Action<MessageEnqueueRecord> _enqueueAction;

        public EnqueueRecord(final TransactionLogResource resource,
                             final EnqueueableMessage message,
                             final Action<MessageEnqueueRecord> enqueueAction)
        {
            _resource = resource;
            _message = message;
            _enqueueAction = enqueueAction;
        }

        public Action<MessageEnqueueRecord> getEnqueueAction()
        {
            return _enqueueAction;
        }

        @Override
        public TransactionLogResource getResource()
        {
            return _resource;
        }

        @Override
        public EnqueueableMessage getMessage()
        {
            return _message;
        }

        public boolean isDurable()
        {
            return _resource.getMessageDurability().persist(_message.isPersistent());
        }

    }


    public void close()
    {
        if(_transaction != null)
        {
            _state = null;
            _transaction.abortTran();
        }
    }
}
