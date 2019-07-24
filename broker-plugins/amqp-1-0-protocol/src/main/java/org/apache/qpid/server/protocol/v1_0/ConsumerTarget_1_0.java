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
package org.apache.qpid.server.protocol.v1_0;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.consumer.AbstractConsumerTarget;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.LogSubject;
import org.apache.qpid.server.logging.messages.ChannelMessages;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.message.MessageInstanceConsumer;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.plugin.MessageConverter;
import org.apache.qpid.server.protocol.MessageConverterRegistry;
import org.apache.qpid.server.protocol.converter.MessageConversionException;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.DeliveryState;
import org.apache.qpid.server.protocol.v1_0.type.Outcome;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Accepted;
import org.apache.qpid.server.protocol.v1_0.type.messaging.EncodingRetainingSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Header;
import org.apache.qpid.server.protocol.v1_0.type.messaging.HeaderSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Modified;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Rejected;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Released;
import org.apache.qpid.server.protocol.v1_0.type.transaction.TransactionError;
import org.apache.qpid.server.protocol.v1_0.type.transaction.TransactionalState;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Error;
import org.apache.qpid.server.protocol.v1_0.type.transport.SenderSettleMode;
import org.apache.qpid.server.protocol.v1_0.type.transport.Transfer;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.transport.ProtocolEngine;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.server.txn.TransactionMonitor;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.util.ServerScopedRuntimeException;
import org.apache.qpid.server.util.StateChangeListener;

class ConsumerTarget_1_0 extends AbstractConsumerTarget<ConsumerTarget_1_0>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerTarget_1_0.class);
    private final boolean _acquires;

    private long _deliveryTag = 0L;

    private Binary _transactionId;
    private final SendingLinkEndpoint _linkEndpoint;

    private final StateChangeListener<MessageInstance, MessageInstance.EntryState> _unacknowledgedMessageListener = new StateChangeListener<MessageInstance, MessageInstance.EntryState>()
    {
        @Override
        public void stateChanged(MessageInstance entry, MessageInstance.EntryState oldState, MessageInstance.EntryState newState)
        {
            if (isConsumerAcquiredStateForThis(oldState) && !isConsumerAcquiredStateForThis(newState))
            {
                removeUnacknowledgedMessage(entry);
                entry.removeStateChangeListener(this);
            }
        }

        private boolean isConsumerAcquiredStateForThis(MessageInstance.EntryState state)
        {
            return state instanceof MessageInstance.ConsumerAcquiredState
                   && ((MessageInstance.ConsumerAcquiredState) state).getConsumer().getTarget() == ConsumerTarget_1_0.this;
        }
    };

    public ConsumerTarget_1_0(final SendingLinkEndpoint linkEndpoint, boolean acquires)
    {
        super(false, linkEndpoint.getSession().getAMQPConnection());
        _linkEndpoint = linkEndpoint;
        _acquires = acquires;
    }

    private SendingLinkEndpoint getEndpoint()
    {
        return _linkEndpoint;
    }

    @Override
    public void updateNotifyWorkDesired()
    {
        boolean state = false;
        Session_1_0 session = _linkEndpoint.getSession();
        if (session != null)
        {
            final AMQPConnection<?> amqpConnection = session.getAMQPConnection();

            state = !amqpConnection.isTransportBlockedForWriting()
                    && _linkEndpoint.isAttached()
                    && getEndpoint().hasCreditToSend();
        }
        setNotifyWorkDesired(state);

    }

    @Override
    public void doSend(final MessageInstanceConsumer consumer, final MessageInstance entry, boolean batch)
    {
        ServerMessage serverMessage = entry.getMessage();
        Message_1_0 message;
        final MessageConverter<? super ServerMessage, Message_1_0> converter;
        if(serverMessage instanceof Message_1_0)
        {
            converter = null;
            message = (Message_1_0) serverMessage;
        }
        else
        {
            if (!serverMessage.checkValid())
            {
                throw new MessageConversionException(String.format("Cannot convert malformed message '%s'", serverMessage));
            }
            converter =
                    (MessageConverter<? super ServerMessage, Message_1_0>) MessageConverterRegistry.getConverter(serverMessage.getClass(), Message_1_0.class);
            if (converter == null)
            {
                throw new ServerScopedRuntimeException(String.format(
                        "Could not find message converter from '%s' to '%s'."
                        + " This is unexpected since we should not try to send if the converter is not present.",
                        serverMessage.getClass(),
                        Message_1_0.class));
            }
            message = converter.convert(serverMessage, _linkEndpoint.getAddressSpace());
        }

        Transfer transfer = new Transfer();
        try
        {
            QpidByteBuffer bodyContent = message.getContent();
            HeaderSection headerSection = message.getHeaderSection();

            UnsignedInteger ttl = headerSection == null ? null : headerSection.getValue().getTtl();
            if (entry.getDeliveryCount() != 0 || ttl != null)
            {
                Header header = new Header();
                if (headerSection != null)
                {
                    final Header oldHeader = headerSection.getValue();
                    header.setDurable(oldHeader.getDurable());
                    header.setPriority(oldHeader.getPriority());

                    if (ttl != null)
                    {
                        long timeSpentOnBroker = System.currentTimeMillis() - message.getArrivalTime();
                        final long adjustedTtl = Math.max(0L, ttl.longValue() - timeSpentOnBroker);
                        header.setTtl(UnsignedInteger.valueOf(adjustedTtl));
                    }
                    headerSection.dispose();
                }

                if (entry.getDeliveryCount() != 0)
                {
                    header.setDeliveryCount(UnsignedInteger.valueOf(entry.getDeliveryCount()));
                }

                headerSection = header.createEncodingRetainingSection();
            }
            List<QpidByteBuffer> payload = new ArrayList<>();
            if(headerSection != null)
            {
                payload.add(headerSection.getEncodedForm());
                headerSection.dispose();
            }
            EncodingRetainingSection<?> section;
            if((section = message.getDeliveryAnnotationsSection()) != null)
            {
                payload.add(section.getEncodedForm());
                section.dispose();
            }

            if((section = message.getMessageAnnotationsSection()) != null)
            {
                payload.add(section.getEncodedForm());
                section.dispose();
            }

            if((section = message.getPropertiesSection()) != null)
            {
                payload.add(section.getEncodedForm());
                section.dispose();
            }

            if((section = message.getApplicationPropertiesSection()) != null)
            {
                payload.add(section.getEncodedForm());
                section.dispose();
            }

            payload.add(bodyContent);

            if((section = message.getFooterSection()) != null)
            {
                payload.add(section.getEncodedForm());
                section.dispose();
            }

            try (QpidByteBuffer combined = QpidByteBuffer.concatenate(payload))
            {
                transfer.setPayload(combined);
            }

            payload.forEach(QpidByteBuffer::dispose);

            byte[] data = new byte[8];
            ByteBuffer.wrap(data).putLong(_deliveryTag++);
            final Binary tag = new Binary(data);

            transfer.setDeliveryTag(tag);

            if (_linkEndpoint.isAttached())
            {
                boolean sendPreSettled = SenderSettleMode.SETTLED.equals(getEndpoint().getSendingSettlementMode());
                if (sendPreSettled)
                {
                    transfer.setSettled(true);
                    if (_acquires && _transactionId == null)
                    {
                        transfer.setState(new Accepted());
                    }
                }
                else
                {
                    final UnsettledAction action;
                    if (_acquires)
                    {
                        action = new DispositionAction(tag, entry, consumer);
                        addUnacknowledgedMessage(entry);
                    }
                    else
                    {
                        action = new DoNothingAction();
                    }

                    _linkEndpoint.addUnsettled(tag, action, entry);
                }

                if (_transactionId != null)
                {
                    TransactionalState state = new TransactionalState();
                    state.setTxnId(_transactionId);
                    transfer.setState(state);
                }
                if (_acquires && _transactionId != null)
                {
                    try
                    {
                        ServerTransaction txn = _linkEndpoint.getTransaction(_transactionId);

                        txn.addPostTransactionAction(new ServerTransaction.Action()
                        {
                            @Override
                            public void postCommit()
                            {
                            }

                            @Override
                            public void onRollback()
                            {
                                entry.release(consumer);
                                _linkEndpoint.updateDisposition(tag, null, true);
                            }
                        });
                        final TransactionLogResource owningResource = entry.getOwningResource();
                        if (owningResource instanceof TransactionMonitor)
                        {
                            ((TransactionMonitor) owningResource).registerTransaction(txn);
                        }
                    }
                    catch (UnknownTransactionException e)
                    {
                        entry.release(consumer);
                        getEndpoint().close(new Error(TransactionError.UNKNOWN_ID, e.getMessage()));
                        return;
                    }

                }
                getSession().getAMQPConnection().registerMessageDelivered(message.getSize());
                getEndpoint().transfer(transfer, false);

                if (sendPreSettled && _acquires && _transactionId == null)
                {
                    handleAcquiredEntrySentPareSettledNonTransactional(entry, consumer);
                }
            }
            else
            {
                entry.release(consumer);
            }

        }
        finally
        {
            transfer.dispose();
            if(converter != null)
            {
                converter.dispose(message);
            }
        }
    }

    private void handleAcquiredEntrySentPareSettledNonTransactional(final MessageInstance entry,
                                                                    final MessageInstanceConsumer consumer)
    {
        if (entry.makeAcquisitionUnstealable(consumer))
        {
            final ServerTransaction txn = _linkEndpoint.getAsyncAutoCommitTransaction();
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
                                entry.release(consumer);
                            }
                        });
            txn.commit();
        }
        else
        {
            entry.release(consumer);
        }
    }

    @Override
    public void flushBatched()
    {
        // TODO
    }

    @Override
    public void queueDeleted(final Queue queue, final MessageInstanceConsumer sub)
    {
        getSession().getConnection().doOnIOThreadAsync(() -> {
            getEndpoint().close(new Error(AmqpError.RESOURCE_DELETED,
                                          String.format("Destination '%s' has been removed.", queue.getName())));
            consumerRemoved(sub);
        });
    }

    @Override
    public boolean allocateCredit(final ServerMessage msg)
    {
        ProtocolEngine protocolEngine = getSession().getConnection();
        final boolean hasCredit = _linkEndpoint.isAttached() && getEndpoint().hasCreditToSend();

        updateNotifyWorkDesired();

        if (hasCredit)
        {
            _linkEndpoint.setLinkCredit(_linkEndpoint.getLinkCredit().subtract(UnsignedInteger.ONE));
        }

        return hasCredit;
    }


    @Override
    public void restoreCredit(final ServerMessage message)
    {
        _linkEndpoint.setLinkCredit(_linkEndpoint.getLinkCredit().add(UnsignedInteger.ONE));
        updateNotifyWorkDesired();
    }

    @Override
    public void noMessagesAvailable()
    {
        if(_linkEndpoint.drained())
        {
            updateNotifyWorkDesired();
        }
    }

    public void flowStateChanged()
    {
        updateNotifyWorkDesired();

        if (_linkEndpoint != null)
        {
            _transactionId = _linkEndpoint.getTransactionId();
        }
    }

    @Override
    public Session_1_0 getSession()
    {
        return _linkEndpoint.getSession();
    }

    private class DispositionAction implements UnsettledAction
    {

        private final MessageInstance _queueEntry;
        private final Binary _deliveryTag;
        private final MessageInstanceConsumer _consumer;

        public DispositionAction(Binary tag, MessageInstance queueEntry, final MessageInstanceConsumer consumer)
        {
            _deliveryTag = tag;
            _queueEntry = queueEntry;
            _consumer = consumer;
        }

        public MessageInstanceConsumer getConsumer()
        {
            return _consumer;
        }

        @Override
        public boolean process(DeliveryState state, final Boolean settled)
        {

            Binary transactionId = null;
            final Outcome outcome;
            ServerTransaction txn;
            // If disposition is settled this overrides the txn?
            if(state instanceof TransactionalState)
            {
                transactionId = ((TransactionalState)state).getTxnId();
                outcome = ((TransactionalState)state).getOutcome();
                try
                {
                    txn = _linkEndpoint.getTransaction(transactionId);
                    getSession().getConnection().registerTransactedMessageDelivered();
                    TransactionLogResource owningResource = _queueEntry.getOwningResource();
                    if (owningResource instanceof TransactionMonitor)
                    {
                        ((TransactionMonitor) owningResource).registerTransaction(txn);
                    }
                }
                catch (UnknownTransactionException e)
                {
                    getEndpoint().close(new Error(TransactionError.UNKNOWN_ID, e.getMessage()));
                    applyModifiedOutcome();
                    return false;
                }
            }
            else if (state instanceof Outcome)
            {
                outcome = (Outcome) state;
                txn = _linkEndpoint.getAsyncAutoCommitTransaction();
            }
            else
            {
                outcome = null;
                txn = null;
            }

            if(outcome instanceof Accepted)
            {
                if (_queueEntry.makeAcquisitionUnstealable(getConsumer()))
                {
                    txn.dequeue(_queueEntry.getEnqueueRecord(),
                                new ServerTransaction.Action()
                                {
                                    @Override
                                    public void postCommit()
                                    {
                                        if (_queueEntry.isAcquiredBy(getConsumer()))
                                        {
                                            _queueEntry.delete();
                                        }
                                    }

                                    @Override
                                    public void onRollback()
                                    {

                                    }
                                });
                }
                txn.addPostTransactionAction(new ServerTransaction.Action()
                    {
                        @Override
                        public void postCommit()
                        {
                            if(Boolean.TRUE.equals(settled))
                            {
                                _linkEndpoint.settle(_deliveryTag);
                            }
                            else
                            {
                                _linkEndpoint.updateDisposition(_deliveryTag, outcome, true);
                            }
                        }

                        @Override
                        public void onRollback()
                        {
                            if(Boolean.TRUE.equals(settled))
                            {
                                // TODO: apply source's default outcome
                                applyModifiedOutcome();
                            }
                        }
                    });
            }
            else if(outcome instanceof Released)
            {
                txn.addPostTransactionAction(new ServerTransaction.Action()
                {
                    @Override
                    public void postCommit()
                    {

                        _queueEntry.release(getConsumer());
                        _linkEndpoint.settle(_deliveryTag);
                    }

                    @Override
                    public void onRollback()
                    {
                        _linkEndpoint.settle(_deliveryTag);

                        // TODO: apply source's default outcome if settled
                    }
                });
            }
            else if(outcome instanceof Modified)
            {
                txn.addPostTransactionAction(new ServerTransaction.Action()
                {
                    @Override
                    public void postCommit()
                    {
                        Modified modifiedOutcome = (Modified) outcome;
                        if (Boolean.TRUE.equals(modifiedOutcome.getUndeliverableHere()))
                        {
                            _queueEntry.reject(getConsumer());
                        }

                        if(Boolean.TRUE.equals(modifiedOutcome.getDeliveryFailed()))
                        {
                            incrementDeliveryCountOrRouteToAlternateOrDiscard();
                        }
                        else
                        {
                            _queueEntry.release(getConsumer());
                        }
                        _linkEndpoint.settle(_deliveryTag);
                    }

                    @Override
                    public void onRollback()
                    {
                        if(Boolean.TRUE.equals(settled))
                        {
                            // TODO: apply source's default outcome
                            applyModifiedOutcome();
                        }
                    }
                });
            }
            else if (outcome instanceof Rejected)
            {
                txn.addPostTransactionAction(new ServerTransaction.Action()
                {
                    @Override
                    public void postCommit()
                    {
                        _linkEndpoint.settle(_deliveryTag);
                        incrementDeliveryCountOrRouteToAlternateOrDiscard();
                        _linkEndpoint.sendFlowConditional();
                    }

                    @Override
                    public void onRollback()
                    {
                        if(Boolean.TRUE.equals(settled))
                        {
                            // TODO: apply source's default outcome
                            applyModifiedOutcome();
                        }
                    }
                });
            }

            return (transactionId == null && outcome != null);
        }

        private void applyModifiedOutcome()
        {
            final Modified modified = new Modified();
            modified.setDeliveryFailed(true);
            _linkEndpoint.updateDisposition(_deliveryTag, modified, true);
            _linkEndpoint.sendFlowConditional();
            incrementDeliveryCountOrRouteToAlternateOrDiscard();
        }

        private void incrementDeliveryCountOrRouteToAlternateOrDiscard()
        {
            _queueEntry.incrementDeliveryCount();
            if (_queueEntry.getMaximumDeliveryCount() > 0
                && _queueEntry.getDeliveryCount() >= _queueEntry.getMaximumDeliveryCount())
            {
                routeToAlternateOrDiscard();
            }
            else
            {
                _queueEntry.release(getConsumer());
            }
        }

        private void routeToAlternateOrDiscard()
        {
            final Session_1_0 session = _linkEndpoint.getSession();
            final ServerMessage message = _queueEntry.getMessage();
            final EventLogger eventLogger = session.getEventLogger();
            final LogSubject logSubject = session.getLogSubject();
            int requeues = 0;
            if (_queueEntry.makeAcquisitionUnstealable(getConsumer()))
            {
                requeues = _queueEntry.routeToAlternate(new Action<MessageInstance>()
                {
                    @Override
                    public void performAction(final MessageInstance requeueEntry)
                    {

                        eventLogger.message(logSubject,
                                            ChannelMessages.DEADLETTERMSG(message.getMessageNumber(),
                                                                          requeueEntry.getOwningResource().getName()));
                    }
                }, null, null);
            }

            if (requeues == 0)
            {
                final TransactionLogResource owningResource = _queueEntry.getOwningResource();
                if (owningResource instanceof Queue)
                {
                    final Queue<?> queue = (Queue<?>) owningResource;

                    final MessageDestination alternateBindingDestination = queue.getAlternateBindingDestination();

                    if (alternateBindingDestination == null)
                    {
                        eventLogger.message(logSubject,
                                            ChannelMessages.DISCARDMSG_NOALTEXCH(message.getMessageNumber(),
                                                                                 queue.getName(),
                                                                                 message.getInitialRoutingAddress()));
                    }
                    else
                    {
                        eventLogger.message(logSubject,
                                            ChannelMessages.DISCARDMSG_NOROUTE(message.getMessageNumber(),
                                                                               alternateBindingDestination.getName()));
                    }
                }
            }
        }
    }

    private void addUnacknowledgedMessage(MessageInstance entry)
    {
        _unacknowledgedCount.incrementAndGet();
        _unacknowledgedBytes.addAndGet(entry.getMessage().getSizeIncludingHeader());
        entry.addStateChangeListener(_unacknowledgedMessageListener);
    }

    private void removeUnacknowledgedMessage(MessageInstance entry)
    {
        _unacknowledgedBytes.addAndGet(-entry.getMessage().getSizeIncludingHeader());
        _unacknowledgedCount.decrementAndGet();
    }

    private class DoNothingAction implements UnsettledAction
    {
        public DoNothingAction()
        {
        }

        @Override
        public boolean process(final DeliveryState state, final Boolean settled)
        {
            return true;
        }
    }

    @Override
    public String getTargetAddress()
    {
        return _linkEndpoint.getTarget().getAddress();
    }

    @Override
    public String toString()
    {
        return "ConsumerTarget_1_0[linkSession=" + _linkEndpoint.getSession().toLogString() + "]";
    }
}
