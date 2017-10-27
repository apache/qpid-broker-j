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
package org.apache.qpid.server.virtualhost;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.messages.TransactionLogMessages;
import org.apache.qpid.server.logging.subjects.MessageStoreLogSubject;
import org.apache.qpid.server.message.MessageReference;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.plugin.MessageMetaDataType;
import org.apache.qpid.server.queue.QueueEntry;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.store.Transaction;
import org.apache.qpid.server.store.handler.DistributedTransactionHandler;
import org.apache.qpid.server.store.handler.MessageHandler;
import org.apache.qpid.server.store.handler.MessageInstanceHandler;
import org.apache.qpid.server.transport.util.Functions;
import org.apache.qpid.server.txn.DtxBranch;
import org.apache.qpid.server.txn.DtxRegistry;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.server.txn.Xid;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.util.ServerScopedRuntimeException;

public class AsynchronousMessageStoreRecoverer implements MessageStoreRecoverer
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AsynchronousMessageStoreRecoverer.class);
    private AsynchronousRecoverer _asynchronousRecoverer;

    @Override
    public ListenableFuture<Void> recover(final QueueManagingVirtualHost<?> virtualHost)
    {
        _asynchronousRecoverer = new AsynchronousRecoverer(virtualHost);

        return _asynchronousRecoverer.recover();
    }

    @Override
    public void cancel()
    {
        if (_asynchronousRecoverer != null)
        {
            _asynchronousRecoverer.cancel();
        }
    }

    private static class AsynchronousRecoverer
    {

        public static final int THREAD_POOL_SHUTDOWN_TIMEOUT = 5000;
        private final QueueManagingVirtualHost<?> _virtualHost;
        private final EventLogger _eventLogger;
        private final MessageStore _store;
        private final MessageStoreLogSubject _logSubject;
        private final long _maxMessageId;
        private final Set<Queue<?>> _recoveringQueues = new CopyOnWriteArraySet<>();
        private final AtomicBoolean _recoveryComplete = new AtomicBoolean();
        private final Map<Long, MessageReference<? extends ServerMessage<?>>> _recoveredMessages = new HashMap<>();
        private final ListeningExecutorService _queueRecoveryExecutor =
                MoreExecutors.listeningDecorator(new ThreadPoolExecutor(0,
                                                                        Integer.MAX_VALUE,
                                                                        60L,
                                                                        TimeUnit.SECONDS,
                                                                        new SynchronousQueue<>(),
                                                                        QpidByteBuffer.createQpidByteBufferTrackingThreadFactory(Executors.defaultThreadFactory())));

        private final MessageStore.MessageStoreReader _storeReader;
        private AtomicBoolean _continueRecovery = new AtomicBoolean(true);

        private AsynchronousRecoverer(final QueueManagingVirtualHost<?> virtualHost)
        {
            _virtualHost = virtualHost;
            _eventLogger = virtualHost.getEventLogger();
            _store = virtualHost.getMessageStore();
            _storeReader = _store.newMessageStoreReader();
            _logSubject = new MessageStoreLogSubject(virtualHost.getName(), _store.getClass().getSimpleName());

            _maxMessageId = _store.getNextMessageId();
            Collection children = _virtualHost.getChildren(Queue.class);
            _recoveringQueues.addAll((Collection<? extends Queue<?>>) children);

        }

        public ListenableFuture<Void> recover()
        {
            getStoreReader().visitDistributedTransactions(new DistributedTransactionVisitor());

            List<ListenableFuture<Void>> queueRecoveryFutures = new ArrayList<>();
            if(_recoveringQueues.isEmpty())
            {
                return _queueRecoveryExecutor.submit(new RemoveOrphanedMessagesTask(), null);
            }
            else
            {
                for (Queue<?> queue : _recoveringQueues)
                {
                    ListenableFuture<Void> result = _queueRecoveryExecutor.submit(new QueueRecoveringTask(queue), null);
                    queueRecoveryFutures.add(result);
                }
                ListenableFuture<List<Void>> combinedFuture = Futures.allAsList(queueRecoveryFutures);
                return Futures.transform(combinedFuture, voids -> null, MoreExecutors.directExecutor());
            }
        }

        public QueueManagingVirtualHost<?> getVirtualHost()
        {
            return _virtualHost;
        }

        public EventLogger getEventLogger()
        {
            return _eventLogger;
        }

        public MessageStore.MessageStoreReader getStoreReader()
        {
            return _storeReader;
        }

        public MessageStoreLogSubject getLogSubject()
        {
            return _logSubject;
        }

        private boolean isRecovering(Queue<?> queue)
        {
            return _recoveringQueues.contains(queue);
        }

        private void recoverQueue(Queue<?> queue)
        {
            MessageInstanceVisitor handler = new MessageInstanceVisitor(queue);
            _storeReader.visitMessageInstances(queue, handler);

            if (handler.getNumberOfUnknownMessageInstances() > 0)
            {
                LOGGER.info("Discarded {} entry(s) associated with queue '{}' as the referenced message "
                             + "does not exist.", handler.getNumberOfUnknownMessageInstances(), queue.getName());
            }

            getEventLogger().message(getLogSubject(), TransactionLogMessages.RECOVERED(handler.getRecoveredCount(), queue.getName()));
            getEventLogger().message(getLogSubject(), TransactionLogMessages.RECOVERY_COMPLETE(queue.getName(), true));
            queue.completeRecovery();

            _recoveringQueues.remove(queue);
            if (_recoveringQueues.isEmpty() && _recoveryComplete.compareAndSet(false, true))
            {
                completeRecovery();
            }
        }

        private synchronized void completeRecovery()
        {
            // at this point nothing should be writing to the map of recovered messages
            for (Map.Entry<Long,MessageReference<? extends ServerMessage<?>>> entry : _recoveredMessages.entrySet())
            {
                entry.getValue().release();
                entry.setValue(null); // free up any memory associated with the reference object
            }
            final List<StoredMessage<?>> messagesToDelete = new ArrayList<>();
            getStoreReader().visitMessages(new MessageHandler()
            {
                @Override
                public boolean handle(final StoredMessage<?> storedMessage)
                {
                    long messageNumber = storedMessage.getMessageNumber();
                    if ( _continueRecovery.get() && messageNumber < _maxMessageId)
                    {
                        if (!_recoveredMessages.containsKey(messageNumber))
                        {
                            messagesToDelete.add(storedMessage);
                        }
                        return true;
                    }
                    return false;
                }
            });
            int unusedMessageCounter = 0;
            for(StoredMessage<?> storedMessage : messagesToDelete)
            {
                if (_continueRecovery.get())
                {
                    LOGGER.debug("Message id '{}' is orphaned, removing", storedMessage.getMessageNumber());
                    storedMessage.remove();
                    unusedMessageCounter++;
                }
            }

            if (unusedMessageCounter > 0)
            {
                LOGGER.info("Discarded {} orphaned message(s).", unusedMessageCounter);
            }

            messagesToDelete.clear();
            _recoveredMessages.clear();
            _storeReader.close();
            _queueRecoveryExecutor.shutdown();
        }

        private synchronized ServerMessage<?> getRecoveredMessage(final long messageId)
        {
            MessageReference<? extends ServerMessage<?>> ref = _recoveredMessages.get(messageId);
            if (ref == null)
            {
                StoredMessage<?> message = _storeReader.getMessage(messageId);
                if(message != null)
                {
                    StorableMessageMetaData metaData = message.getMetaData();

                    @SuppressWarnings("rawtypes")
                    MessageMetaDataType type = metaData.getType();

                    @SuppressWarnings("unchecked")
                    ServerMessage<?> serverMessage = type.createMessage(message);

                    ref = serverMessage.newReference();
                    _recoveredMessages.put(messageId, ref);
                }
            }
            return ref == null ? null : ref.getMessage();
        }

        public void cancel()
        {
            _continueRecovery.set(false);
            _queueRecoveryExecutor.shutdown();
            try
            {
                boolean wasShutdown = _queueRecoveryExecutor.awaitTermination(THREAD_POOL_SHUTDOWN_TIMEOUT, TimeUnit.MILLISECONDS);
                if (!wasShutdown)
                {
                    LOGGER.warn("Failed to gracefully shutdown queue recovery executor within permitted time period");
                    _queueRecoveryExecutor.shutdownNow();
                }
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
            }
            finally
            {
                _storeReader.close();
            }
        }


        private class DistributedTransactionVisitor implements DistributedTransactionHandler
        {


            @Override
            public boolean handle(final Transaction.StoredXidRecord storedXid,
                                  final Transaction.EnqueueRecord[] enqueues,
                                  final Transaction.DequeueRecord[] dequeues)
            {
                Xid id = new Xid(storedXid.getFormat(), storedXid.getGlobalId(), storedXid.getBranchId());
                DtxRegistry dtxRegistry = getVirtualHost().getDtxRegistry();
                DtxBranch branch = dtxRegistry.getBranch(id);
                if (branch == null)
                {
                    branch = new DtxBranch(storedXid, dtxRegistry);
                    dtxRegistry.registerBranch(branch);
                }
                for (Transaction.EnqueueRecord record : enqueues)
                {
                    final Queue<?> queue = getVirtualHost().getAttainedQueue(record.getResource().getId());
                    if (queue != null)
                    {
                        final long messageId = record.getMessage().getMessageNumber();
                        final ServerMessage<?> message = getRecoveredMessage(messageId);

                        if (message != null)
                        {
                            final MessageReference<?> ref = message.newReference();

                            final MessageEnqueueRecord[] records = new MessageEnqueueRecord[1];

                            branch.enqueue(queue, message, new Action<MessageEnqueueRecord>()
                            {
                                @Override
                                public void performAction(final MessageEnqueueRecord record)
                                {
                                    records[0] = record;
                                }
                            });
                            branch.addPostTransactionAction(new ServerTransaction.Action()
                            {
                                @Override
                                public void postCommit()
                                {
                                    queue.enqueue(message, null, records[0]);
                                    ref.release();
                                }

                                @Override
                                public void onRollback()
                                {
                                    ref.release();
                                }
                            });

                        }
                        else
                        {
                            StringBuilder xidString = xidAsString(id);
                            getEventLogger().message(getLogSubject(),
                                                            TransactionLogMessages.XA_INCOMPLETE_MESSAGE(xidString.toString(),
                                                                                                         Long.toString(
                                                                                                                 messageId)));
                        }
                    }
                    else
                    {
                        StringBuilder xidString = xidAsString(id);
                        getEventLogger().message(getLogSubject(),
                                                        TransactionLogMessages.XA_INCOMPLETE_QUEUE(xidString.toString(),
                                                                                                   record.getResource()
                                                                                                           .getId()
                                                                                                           .toString()));

                    }
                }
                for (Transaction.DequeueRecord record : dequeues)
                {

                    final Queue<?> queue = getVirtualHost().getAttainedQueue(record.getEnqueueRecord().getQueueId());

                    if (queue != null)
                    {
                        // For DTX to work correctly the queues which have uncommitted branches with dequeues
                        // must be synchronously recovered

                        if (isRecovering(queue))
                        {
                            recoverQueue(queue);
                        }

                        final long messageId = record.getEnqueueRecord().getMessageNumber();
                        final ServerMessage<?> message = getRecoveredMessage(messageId);

                        if (message != null)
                        {
                            final QueueEntry entry = queue.getMessageOnTheQueue(messageId);

                            if (entry.acquire())
                            {
                                branch.dequeue(entry.getEnqueueRecord());

                                branch.addPostTransactionAction(new ServerTransaction.Action()
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
                            }
                            else
                            {
                                // Should never happen - dtx recovery is always synchronous and occurs before
                                // any other message actors are allowed to act on the virtualhost.
                                throw new ServerScopedRuntimeException(
                                        "Distributed transaction dequeue handler failed to acquire " + entry +
                                        " during recovery of queue " + queue);

                            }
                        }
                        else
                        {
                            StringBuilder xidString = xidAsString(id);
                            getEventLogger().message(getLogSubject(),
                                                            TransactionLogMessages.XA_INCOMPLETE_MESSAGE(xidString.toString(),
                                                                                                         Long.toString(
                                                                                                                 messageId)));

                        }

                    }
                    else
                    {
                        StringBuilder xidString = xidAsString(id);
                        getEventLogger().message(getLogSubject(),
                                                        TransactionLogMessages.XA_INCOMPLETE_QUEUE(xidString.toString(),
                                                                                                   record.getEnqueueRecord()
                                                                                                           .getQueueId()
                                                                                                           .toString()));
                    }

                }


                branch.setState(DtxBranch.State.PREPARED);
                branch.prePrepareTransaction();

                return _continueRecovery.get();
            }

            private StringBuilder xidAsString(Xid id)
            {
                return new StringBuilder("(")
                        .append(id.getFormat())
                        .append(',')
                        .append(Functions.str(id.getGlobalId()))
                        .append(',')
                        .append(Functions.str(id.getBranchId()))
                        .append(')');
            }


        }

        private class QueueRecoveringTask implements Runnable
        {
            private final Queue<?> _queue;

            QueueRecoveringTask(final Queue<?> queue)
            {
                _queue = queue;
            }

            @Override
            public void run()
            {
                String originalThreadName = Thread.currentThread().getName();
                Thread.currentThread().setName("Queue Recoverer : " + _queue.getName() + " (vh: " + getVirtualHost().getName() + ")");

                try
                {
                    recoverQueue(_queue);
                }
                finally
                {
                    Thread.currentThread().setName(originalThreadName);
                }
            }

        }


        private class RemoveOrphanedMessagesTask implements Runnable
        {
            RemoveOrphanedMessagesTask()
            {
            }

            @Override
            public void run()
            {
                String originalThreadName = Thread.currentThread().getName();
                Thread.currentThread().setName("Orphaned message removal");

                try
                {
                    completeRecovery();
                }
                finally
                {
                    Thread.currentThread().setName(originalThreadName);
                }
            }

        }


        private class MessageInstanceVisitor implements MessageInstanceHandler
        {
            private final Queue<?> _queue;
            long _recoveredCount;
            private int _numberOfUnknownMessageInstances;

            private MessageInstanceVisitor(Queue<?> queue)
            {
                _queue = queue;
                _numberOfUnknownMessageInstances = 0;
            }

            @Override
            public boolean handle(final MessageEnqueueRecord record)
            {
                long messageId = record.getMessageNumber();
                String queueName = _queue.getName();

                if(messageId < _maxMessageId)
                {
                    ServerMessage<?> message = getRecoveredMessage(messageId);

                    if (message != null)
                    {
                        LOGGER.debug("Delivering message id '{}' to queue '{}'", message.getMessageNumber(), queueName);

                        _queue.recover(message, record);
                        _recoveredCount++;
                    }
                    else
                    {
                        LOGGER.debug("Message id '{}' referenced in log as enqueued in queue '{}' is unknown, entry will be discarded",
                                      messageId, queueName);
                        Transaction txn = _store.newTransaction();
                        txn.dequeueMessage(record);
                        txn.commitTranAsync((Void) null);
                        _numberOfUnknownMessageInstances++;
                    }
                    return _continueRecovery.get();
                }
                else
                {
                    return false;
                }

            }

            long getRecoveredCount()
            {
                return _recoveredCount;
            }

            int getNumberOfUnknownMessageInstances()
            {
                return _numberOfUnknownMessageInstances;
            }
        }
    }


}
