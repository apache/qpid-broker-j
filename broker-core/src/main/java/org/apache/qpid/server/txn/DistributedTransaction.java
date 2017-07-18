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

import org.apache.qpid.server.message.EnqueueableMessage;
import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.queue.BaseQueue;
import org.apache.qpid.server.session.AMQPSession;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.store.TransactionLogResource;

public class DistributedTransaction implements ServerTransaction
{

    private final AutoCommitTransaction _autoCommitTransaction;

    private DtxBranch _branch;
    private final AMQPSession<?,?> _session;
    private final DtxRegistry _dtxRegistry;


    public DistributedTransaction(AMQPSession<?,?> session, DtxRegistry dtxRegistry)
    {
        _session = session;
        _dtxRegistry = dtxRegistry;
        _autoCommitTransaction = new AutoCommitTransaction(dtxRegistry.getMessageStore());
    }

    @Override
    public long getTransactionStartTime()
    {
        return 0;
    }

    @Override
    public long getTransactionUpdateTime()
    {
        return 0;
    }

    @Override
    public void addPostTransactionAction(Action postTransactionAction)
    {
        if(_branch != null)
        {
            _branch.addPostTransactionAction(postTransactionAction);
        }
        else
        {
            _autoCommitTransaction.addPostTransactionAction(postTransactionAction);
        }
    }

    @Override
    public void dequeue(MessageEnqueueRecord record, Action postTransactionAction)
    {
        if(_branch != null)
        {
            _branch.dequeue(record);
            _branch.addPostTransactionAction(postTransactionAction);
        }
        else
        {
            _autoCommitTransaction.dequeue(record, postTransactionAction);
        }
    }


    @Override
    public void dequeue(Collection<MessageInstance> messages, Action postTransactionAction)
    {
        if(_branch != null)
        {
            for(MessageInstance entry : messages)
            {
                _branch.dequeue(entry.getEnqueueRecord());
            }
            _branch.addPostTransactionAction(postTransactionAction);
        }
        else
        {
            _autoCommitTransaction.dequeue(messages, postTransactionAction);
        }
    }

    @Override
    public void enqueue(TransactionLogResource queue, EnqueueableMessage message, final EnqueueAction postTransactionAction)
    {
        if(_branch != null)
        {
            final MessageEnqueueRecord[] enqueueRecords = new MessageEnqueueRecord[1];
                _branch.enqueue(queue, message, new org.apache.qpid.server.util.Action<MessageEnqueueRecord>()
                {
                    @Override
                    public void performAction(final MessageEnqueueRecord record)
                    {
                        enqueueRecords[0] = record;
                    }
                });
            addPostTransactionAction(new Action()
            {
                @Override
                public void postCommit()
                {
                    postTransactionAction.postCommit(enqueueRecords);
                }

                @Override
                public void onRollback()
                {
                    postTransactionAction.onRollback();
                }
            });
        }
        else
        {
            _autoCommitTransaction.enqueue(queue, message, postTransactionAction);
        }
    }

    @Override
    public void enqueue(Collection<? extends BaseQueue> queues, EnqueueableMessage message,
                        final EnqueueAction postTransactionAction)
    {
        if(_branch != null)
        {
            final MessageEnqueueRecord[] enqueueRecords = new MessageEnqueueRecord[queues.size()];
            int i = 0;
            for(BaseQueue queue : queues)
            {
                final int pos = i;
                _branch.enqueue(queue, message, new org.apache.qpid.server.util.Action<MessageEnqueueRecord>()
                {
                    @Override
                    public void performAction(final MessageEnqueueRecord record)
                    {
                        enqueueRecords[pos] = record;
                    }
                });
                i++;
            }
            addPostTransactionAction(new Action()
            {
                @Override
                public void postCommit()
                {
                    postTransactionAction.postCommit(enqueueRecords);
                }

                @Override
                public void onRollback()
                {
                    postTransactionAction.onRollback();
                }
            });
        }
        else
        {
            _autoCommitTransaction.enqueue(queues, message, postTransactionAction);
        }
    }

    @Override
    public void commit()
    {
        throw new IllegalStateException("Cannot call tx.commit() on a distributed transaction");
    }

    @Override
    public void commit(Runnable immediatePostTransactionAction)
    {
        throw new IllegalStateException("Cannot call tx.commit() on a distributed transaction");
    }

    @Override
    public void rollback()
    {
        throw new IllegalStateException("Cannot call tx.rollback() on a distributed transaction");
    }

    @Override
    public boolean isTransactional()
    {
        return _branch != null;
    }
    
    public void start(Xid id, boolean join, boolean resume)
            throws UnknownDtxBranchException, AlreadyKnownDtxException, JoinAndResumeDtxException
    {
        if(join && resume)
        {
            throw new JoinAndResumeDtxException(id);
        }

        DtxBranch branch = _dtxRegistry.getBranch(id);

        if(branch == null)
        {
            if(join || resume)
            {
                throw new UnknownDtxBranchException(id);
            }
            branch = new DtxBranch(id, _dtxRegistry);
            if(_dtxRegistry.registerBranch(branch))
            {
                _branch = branch;
                branch.associateSession(_session);
            }
            else
            {
                throw new AlreadyKnownDtxException(id);
            }
        }
        else
        {
            if(join)
            {
                branch.associateSession(_session);
            }
            else if(resume)
            {
                branch.resumeSession(_session);
            }
            else
            {
                throw new AlreadyKnownDtxException(id);
            }
            _branch = branch;
        }
    }
    
    public void end(Xid id, boolean fail, boolean suspend)
            throws UnknownDtxBranchException, NotAssociatedDtxException, SuspendAndFailDtxException, TimeoutDtxException
    {
        DtxBranch branch = _dtxRegistry.getBranch(id);

        if(suspend && fail)
        {
            branch.disassociateSession(_session);
            _branch = null;
            throw new SuspendAndFailDtxException(id);
        }


        if(branch == null)
        {
            throw new UnknownDtxBranchException(id);
        }
        else
        {
            if(!branch.isAssociated(_session))
            {
                throw new NotAssociatedDtxException(id);
            }
            if(branch.expired() || branch.getState() == DtxBranch.State.TIMEDOUT)
            {
                branch.disassociateSession(_session);
                throw new TimeoutDtxException(id);
            }

            if(suspend)
            {
                branch.suspendSession(_session);
            }
            else
            {
                if(fail)
                {
                    branch.setState(DtxBranch.State.ROLLBACK_ONLY);
                }
                branch.disassociateSession(_session);
            }

            _branch = null;

        }
    }

}
