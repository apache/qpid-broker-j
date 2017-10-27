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

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.messages.MessageStoreMessages;
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
import org.apache.qpid.server.store.Transaction.EnqueueRecord;
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

public class SynchronousMessageStoreRecoverer implements MessageStoreRecoverer
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SynchronousMessageStoreRecoverer.class);

    @Override
    public ListenableFuture<Void> recover(QueueManagingVirtualHost<?> virtualHost)
    {
        EventLogger eventLogger = virtualHost.getEventLogger();
        MessageStore store = virtualHost.getMessageStore();
        MessageStore.MessageStoreReader storeReader = store.newMessageStoreReader();
        MessageStoreLogSubject logSubject = new MessageStoreLogSubject(virtualHost.getName(), store.getClass().getSimpleName());

        Map<Queue<?>, Integer> queueRecoveries = new TreeMap<>();
        Map<Long, ServerMessage<?>> recoveredMessages = new HashMap<>();
        Map<Long, StoredMessage<?>> unusedMessages = new TreeMap<>();
        Map<UUID, Integer> unknownQueuesWithMessages = new HashMap<>();
        Map<Queue<?>, Integer> queuesWithUnknownMessages = new HashMap<>();

        eventLogger.message(logSubject, MessageStoreMessages.RECOVERY_START());

        storeReader.visitMessages(new MessageVisitor(recoveredMessages, unusedMessages));

        eventLogger.message(logSubject, TransactionLogMessages.RECOVERY_START(null, false));
        try
        {
            storeReader.visitMessageInstances(new MessageInstanceVisitor(virtualHost,
                                                                         store,
                                                                         queueRecoveries,
                                                                         recoveredMessages,
                                                                         unusedMessages,
                                                                         unknownQueuesWithMessages,
                                                                         queuesWithUnknownMessages));
        }
        finally
        {
            if (!unknownQueuesWithMessages.isEmpty())
            {
                unknownQueuesWithMessages.forEach((queueId, count) -> {
                    LOGGER.info("Discarded {} entry(s) associated with queue id '{}' as a queue with this "
                                 + "id does not appear in the configuration.",
                                 count, queueId);
                });
            }
            if (!queuesWithUnknownMessages.isEmpty())
            {
                queuesWithUnknownMessages.forEach((queue, count) -> {
                    LOGGER.info("Discarded {} entry(s) associated with queue '{}' as the referenced message "
                                 + "does not exist.",
                                 count, queue.getName());
                });
            }
        }

        for(Map.Entry<Queue<?>, Integer> entry : queueRecoveries.entrySet())
        {
            Queue<?> queue = entry.getKey();
            Integer deliveredCount = entry.getValue();
            eventLogger.message(logSubject, TransactionLogMessages.RECOVERED(deliveredCount, queue.getName()));
            eventLogger.message(logSubject, TransactionLogMessages.RECOVERY_COMPLETE(queue.getName(), true));
            queue.completeRecovery();
        }

        for (Queue<?> q : virtualHost.getChildren(Queue.class))
        {
            if (!queueRecoveries.containsKey(q))
            {
                q.completeRecovery();
            }
        }

        storeReader.visitDistributedTransactions(new DistributedTransactionVisitor(virtualHost,
                                                                                   eventLogger,
                                                                                   logSubject, recoveredMessages, unusedMessages));

        for(StoredMessage<?> m : unusedMessages.values())
        {
            LOGGER.debug("Message id '{}' is orphaned, removing", m.getMessageNumber());
            m.remove();
        }

        if (unusedMessages.size() > 0)
        {
            LOGGER.info("Discarded {} orphaned message(s).", unusedMessages.size());
        }

        eventLogger.message(logSubject, TransactionLogMessages.RECOVERY_COMPLETE(null, false));

        eventLogger.message(logSubject,
                             MessageStoreMessages.RECOVERED(recoveredMessages.size() - unusedMessages.size()));
        eventLogger.message(logSubject, MessageStoreMessages.RECOVERY_COMPLETE());

        return Futures.immediateFuture(null);
    }

    @Override
    public void cancel()
    {
        // No-op
    }

    private static class MessageVisitor implements MessageHandler
    {

        private final Map<Long, ServerMessage<?>> _recoveredMessages;
        private final Map<Long, StoredMessage<?>> _unusedMessages;

        MessageVisitor(final Map<Long, ServerMessage<?>> recoveredMessages,
                       final Map<Long, StoredMessage<?>> unusedMessages)
        {
            _recoveredMessages = recoveredMessages;
            _unusedMessages = unusedMessages;
        }

        @Override
        public boolean handle(StoredMessage<?> message)
        {
            StorableMessageMetaData metaData = message.getMetaData();

            @SuppressWarnings("rawtypes")
            MessageMetaDataType type = metaData.getType();

            @SuppressWarnings("unchecked")
            ServerMessage<?> serverMessage = type.createMessage(message);

            _recoveredMessages.put(message.getMessageNumber(), serverMessage);
            _unusedMessages.put(message.getMessageNumber(), message);
            return true;
        }

    }

    private static class MessageInstanceVisitor implements MessageInstanceHandler
    {
        private final QueueManagingVirtualHost<?> _virtualHost;
        private final MessageStore _store;

        private final Map<Queue<?>, Integer> _queueRecoveries;
        private final Map<Long, ServerMessage<?>> _recoveredMessages;
        private final Map<Long, StoredMessage<?>> _unusedMessages;
        private final Map<UUID, Integer> _unknownQueuesWithMessages;
        private Map<Queue<?>, Integer> _queuesWithUnknownMessages;

        private MessageInstanceVisitor(final QueueManagingVirtualHost<?> virtualHost,
                                       final MessageStore store,
                                       final Map<Queue<?>, Integer> queueRecoveries,
                                       final Map<Long, ServerMessage<?>> recoveredMessages,
                                       final Map<Long, StoredMessage<?>> unusedMessages,
                                       final Map<UUID, Integer> unknownQueuesWithMessages,
                                       final Map<Queue<?>, Integer> queuesWithUnknownMessages)
        {
            _virtualHost = virtualHost;
            _store = store;
            _queueRecoveries = queueRecoveries;
            _recoveredMessages = recoveredMessages;
            _unusedMessages = unusedMessages;
            _unknownQueuesWithMessages = unknownQueuesWithMessages;
            _queuesWithUnknownMessages = queuesWithUnknownMessages;
        }

        @Override
        public boolean handle(final MessageEnqueueRecord record)
        {
            final UUID queueId = record.getQueueId();
            long messageId = record.getMessageNumber();
            Queue<?> queue = _virtualHost.getAttainedQueue(queueId);
            boolean dequeueMessageInstance = true;
            if(queue != null)
            {
                String queueName = queue.getName();
                ServerMessage<?> message = _recoveredMessages.get(messageId);
                _unusedMessages.remove(messageId);

                if (message != null)
                {
                    LOGGER.debug("Delivering message id '{}' to queue '{}'", message.getMessageNumber(), queueName);

                    _queueRecoveries.merge(queue, 1, (old, unused) -> old + 1);

                    queue.recover(message, record);

                    dequeueMessageInstance = false;
                }
                else
                {
                    LOGGER.debug("Message id '{}' referenced in log as enqueued in queue '{}' is unknown, entry will be discarded",
                            messageId, queueName);

                    _queuesWithUnknownMessages.merge(queue, 1, (old, unused) -> old + 1);

                }
            }
            else
            {
                LOGGER.debug(
                        "Message id '{}' in log references queue with id '{}' which is not in the configuration, entry will be discarded",
                        messageId, queueId);
                _unknownQueuesWithMessages.merge(queueId, 1, (old, unused) -> old + 1);
            }

            if (dequeueMessageInstance)
            {
                Transaction txn = _store.newTransaction();
                txn.dequeueMessage(record);
                txn.commitTranAsync((Void) null);
            }

            return true;
        }
    }

    private static class DistributedTransactionVisitor implements DistributedTransactionHandler
    {

        private final QueueManagingVirtualHost<?> _virtualHost;
        private final EventLogger _eventLogger;
        private final MessageStoreLogSubject _logSubject;

        private final Map<Long, ServerMessage<?>> _recoveredMessages;
        private final Map<Long, StoredMessage<?>> _unusedMessages;

        private DistributedTransactionVisitor(final QueueManagingVirtualHost<?> virtualHost,
                                              final EventLogger eventLogger,
                                              final MessageStoreLogSubject logSubject,
                                              final Map<Long, ServerMessage<?>> recoveredMessages,
                                              final Map<Long, StoredMessage<?>> unusedMessages)
        {
            _virtualHost = virtualHost;
            _eventLogger = eventLogger;
            _logSubject = logSubject;
            _recoveredMessages = recoveredMessages;
            _unusedMessages = unusedMessages;
        }

        @Override
        public boolean handle(final Transaction.StoredXidRecord storedXid,
                              final Transaction.EnqueueRecord[] enqueues,
                              final Transaction.DequeueRecord[] dequeues)
        {
            Xid id = new Xid(storedXid.getFormat(), storedXid.getGlobalId(), storedXid.getBranchId());
            DtxRegistry dtxRegistry = _virtualHost.getDtxRegistry();
            DtxBranch branch = dtxRegistry.getBranch(id);
            if(branch == null)
            {
                branch = new DtxBranch(storedXid, dtxRegistry);
                dtxRegistry.registerBranch(branch);
            }
            for(EnqueueRecord record : enqueues)
            {
                final Queue<?> queue = _virtualHost.getAttainedQueue(record.getResource().getId());
                if(queue != null)
                {
                    final long messageId = record.getMessage().getMessageNumber();
                    final ServerMessage<?> message = _recoveredMessages.get(messageId);
                    _unusedMessages.remove(messageId);

                    if(message != null)
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
                        _eventLogger.message(_logSubject,
                                          TransactionLogMessages.XA_INCOMPLETE_MESSAGE(xidString.toString(),
                                                                                       Long.toString(messageId)));
                    }
                }
                else
                {
                    StringBuilder xidString = xidAsString(id);
                    _eventLogger.message(_logSubject,
                                      TransactionLogMessages.XA_INCOMPLETE_QUEUE(xidString.toString(),
                                                                                 record.getResource().getId().toString()));

                }
            }
            for(Transaction.DequeueRecord record : dequeues)
            {
                final Queue<?> queue = _virtualHost.getAttainedQueue(record.getEnqueueRecord().getQueueId());
                if(queue != null)
                {
                    final long messageId = record.getEnqueueRecord().getMessageNumber();
                    final ServerMessage<?> message = _recoveredMessages.get(messageId);
                    _unusedMessages.remove(messageId);

                    if(message != null)
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
                        _eventLogger.message(_logSubject,
                                          TransactionLogMessages.XA_INCOMPLETE_MESSAGE(xidString.toString(),
                                                                                       Long.toString(messageId)));

                    }

                }
                else
                {
                    StringBuilder xidString = xidAsString(id);
                    _eventLogger.message(_logSubject,
                                      TransactionLogMessages.XA_INCOMPLETE_QUEUE(xidString.toString(),
                                                                                 record.getEnqueueRecord().getQueueId().toString()));
                }

            }

            branch.setState(DtxBranch.State.PREPARED);
            branch.prePrepareTransaction();
            return true;
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


}
