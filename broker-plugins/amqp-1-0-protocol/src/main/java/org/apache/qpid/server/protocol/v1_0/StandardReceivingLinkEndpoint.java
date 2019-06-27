/*
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
 */

package org.apache.qpid.server.protocol.v1_0;

import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.message.MessageReference;
import org.apache.qpid.server.message.MessageSender;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.PublishingLink;
import org.apache.qpid.server.plugin.MessageFormat;
import org.apache.qpid.server.protocol.MessageFormatRegistry;
import org.apache.qpid.server.protocol.v1_0.delivery.DeliveryRegistry;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorRuntimeException;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.DeliveryState;
import org.apache.qpid.server.protocol.v1_0.type.Outcome;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Accepted;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Rejected;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Target;
import org.apache.qpid.server.protocol.v1_0.type.messaging.TerminusDurability;
import org.apache.qpid.server.protocol.v1_0.type.messaging.TerminusExpiryPolicy;
import org.apache.qpid.server.protocol.v1_0.type.transaction.Coordinator;
import org.apache.qpid.server.protocol.v1_0.type.transaction.TransactionError;
import org.apache.qpid.server.protocol.v1_0.type.transaction.TransactionalState;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Error;
import org.apache.qpid.server.protocol.v1_0.type.transport.ReceiverSettleMode;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.server.txn.AsyncAutoCommitTransaction;
import org.apache.qpid.server.txn.AsyncCommand;
import org.apache.qpid.server.txn.AutoCommitTransaction;
import org.apache.qpid.server.txn.LocalTransaction;
import org.apache.qpid.server.txn.ServerTransaction;

public class StandardReceivingLinkEndpoint extends AbstractReceivingLinkEndpoint<Target>
        implements AsyncAutoCommitTransaction.FutureRecorder
{
    private static final Logger LOGGER = LoggerFactory.getLogger(StandardReceivingLinkEndpoint.class);
    private static final Symbol DELIVERY_TAG = Symbol.valueOf("delivery-tag");
    private static final Accepted ACCEPTED = new Accepted();
    private static final String LINK = "link";

    private final java.util.Queue<AsyncCommand> _unfinishedCommandsQueue = new ConcurrentLinkedQueue<>();
    private final Set<PendingDispositionHolder> _pendingDispositions =
            Collections.synchronizedSet(new LinkedHashSet<>());

    private volatile ReceivingDestination _receivingDestination;
    private volatile boolean _rejectedOutcomeSupportedBySource;

    private final PublishingLink _publishingLink = new PublishingLink()
    {
        @Override
        public String getName()
        {
            return getLinkName();
        }

        @Override
        public String getType()
        {
            return LINK;
        }

        @Override
        public String getDestination()
        {
            final ReceivingDestination receivingDestination = _receivingDestination;
            return receivingDestination == null ? "" : _receivingDestination.getAddress();
        }
    };

    private final MessageSender _messageSender = new MessageSender()
    {
        @Override
        public void destinationRemoved(final MessageDestination destination)
        {
            getSession().getConnection()
                        .doOnIOThreadAsync(() -> close(new Error(AmqpError.RESOURCE_DELETED,
                                                                 String.format("Destination '%s' has been removed.",
                                                                               destination.getName()))));
        }

        @Override
        public Collection<? extends PublishingLink> getPublishingLinks(final MessageDestination destination)
        {
            final ReceivingDestination receivingDestination = _receivingDestination;
            MessageDestination actualDestination = receivingDestination == null ? null : receivingDestination.getMessageDestination();
            return actualDestination != null && actualDestination.equals(destination) ? Collections.singleton(_publishingLink) : Collections.emptyList();
        }
    };

    public StandardReceivingLinkEndpoint(final Session_1_0 session,
                                         final Link_1_0<Source, Target> link)
    {
        super(session, link);
    }

    @Override
    public void start()
    {
        setLinkCredit(UnsignedInteger.valueOf(getReceivingDestination().getCredit()));
        setCreditWindow();
    }


    private TerminusDurability getDurability()
    {
        return getTarget().getDurable();
    }

    @Override
    protected Error receiveDelivery(Delivery delivery)
    {
        ReceiverSettleMode transferReceiverSettleMode = delivery.getReceiverSettleMode();

        if(delivery.getResume())
        {
            DeliveryState deliveryState = _unsettled.get(delivery.getDeliveryTag());
            if (deliveryState instanceof Outcome)
            {
                boolean settled = shouldReceiverSettleFirst(transferReceiverSettleMode);
                updateDisposition(delivery.getDeliveryTag(), deliveryState, settled);
                return null;
            }
            else
            {
                // TODO: QPID-7845: create message ?
            }
        }
        else
        {
            ServerMessage<?> serverMessage;
            UnsignedInteger messageFormat = delivery.getMessageFormat();
            DeliveryState xfrState = delivery.getState();
            MessageFormat format = MessageFormatRegistry.getFormat(messageFormat.intValue());
            if(format != null)
            {
                try (QpidByteBuffer payload = delivery.getPayload())
                {
                    serverMessage = format.createMessage(payload,
                                                         getAddressSpace().getMessageStore(),
                                                         getSession().getConnection().getReference());
                }
                catch (AmqpErrorRuntimeException e)
                {
                    return e.getCause().getError();
                }
            }
            else
            {
                final Error err = new Error();
                err.setCondition(AmqpError.NOT_IMPLEMENTED);
                err.setDescription("Unknown message format: " + messageFormat);
                return err;
            }


            MessageReference<?> reference = serverMessage.newReference();
            try
            {
                Binary transactionId = null;
                if (xfrState != null)
                {
                    if (xfrState instanceof TransactionalState)
                    {
                        transactionId = ((TransactionalState) xfrState).getTxnId();
                    }
                }

                final ServerTransaction transaction;
                boolean setRollbackOnly = true;
                if (transactionId != null)
                {
                    try
                    {
                        transaction = getSession().getTransaction(transactionId);
                    }
                    catch (UnknownTransactionException e)
                    {
                        return new Error(TransactionError.UNKNOWN_ID,
                                         String.format("transaction-id '%s' is unknown.", transactionId));
                    }
                    if (!(transaction instanceof AutoCommitTransaction))
                    {
                        transaction.addPostTransactionAction(new ServerTransaction.Action()
                        {
                            @Override
                            public void postCommit()
                            {
                                updateDisposition(delivery.getDeliveryTag(), null, true);
                            }

                            @Override
                            public void onRollback()
                            {
                                updateDisposition(delivery.getDeliveryTag(), null, true);
                            }
                        });
                    }
                }
                else
                {
                    transaction = new AsyncAutoCommitTransaction(getAddressSpace().getMessageStore(), this);
                }

                try
                {
                    Session_1_0 session = getSession();

                    session.getAMQPConnection()
                           .checkAuthorizedMessagePrincipal(serverMessage.getMessageHeader().getUserId());

                    Outcome outcome;
                    if (serverMessage.isPersistent() && !getAddressSpace().getMessageStore().isPersistent())
                    {
                        final Error preconditionFailedError = new Error(AmqpError.PRECONDITION_FAILED,
                                                                        "Non-durable message store cannot accept durable message.");
                        if (_rejectedOutcomeSupportedBySource)
                        {
                            final Rejected rejected = new Rejected();
                            rejected.setError(preconditionFailedError);
                            outcome = rejected;
                        }
                        else
                        {
                            // TODO - disposition not updated for the non-transaction case
                            return preconditionFailedError;
                        }
                    }
                    else
                    {
                        try
                        {
                            getReceivingDestination().send(serverMessage,
                                                           transaction,
                                                           session.getSecurityToken());
                            outcome = ACCEPTED;
                        }
                        catch (UnroutableMessageException e)
                        {
                            final Error error = new Error();
                            error.setCondition(e.getErrorCondition());
                            error.setDescription(e.getMessage());
                            String targetAddress = getTarget().getAddress();
                            if (targetAddress == null || "".equals(targetAddress.trim()))
                            {
                                error.setInfo(Collections.singletonMap(DELIVERY_TAG, delivery.getDeliveryTag()));
                            }
                            if (!_rejectedOutcomeSupportedBySource ||
                                (delivery.isSettled() && !(transaction instanceof LocalTransaction)))
                            {
                                return error;
                            }
                            else
                            {
                                if (delivery.isSettled() && transaction instanceof LocalTransaction)
                                {
                                    ((LocalTransaction) transaction).setRollbackOnly();
                                }

                                Rejected rejected = new Rejected();
                                rejected.setError(error);
                                outcome = rejected;
                            }
                        }
                    }

                    Outcome sourceDefaultOutcome = getSource().getDefaultOutcome();
                    boolean defaultOutcome = sourceDefaultOutcome != null &&
                                             sourceDefaultOutcome.getSymbol().equals(outcome.getSymbol());
                    DeliveryState resultantState;
                    if (transactionId == null)
                    {
                        resultantState = defaultOutcome ? null : outcome;
                    }
                    else
                    {
                        TransactionalState transactionalState = new TransactionalState();
                        transactionalState.setOutcome(defaultOutcome ? null : outcome);
                        transactionalState.setTxnId(transactionId);
                        resultantState = transactionalState;
                    }

                    boolean settled = shouldReceiverSettleFirst(transferReceiverSettleMode);

                    if (transaction instanceof AsyncAutoCommitTransaction)
                    {
                        _pendingDispositions.add(new PendingDispositionHolder(delivery.getDeliveryTag(),
                                                                              resultantState,
                                                                              settled));
                    }
                    else
                    {
                        getSession().receivedComplete();
                        updateDisposition(delivery.getDeliveryTag(), resultantState, settled);
                    }

                    getSession().getAMQPConnection().registerMessageReceived(serverMessage.getSize());
                    if (transactionId != null)
                    {
                        getSession().getAMQPConnection().registerTransactedMessageReceived();
                    }

                    setRollbackOnly = false;
                }
                catch (AccessControlException e)
                {
                    final Error err = new Error();
                    err.setCondition(AmqpError.NOT_ALLOWED);
                    err.setDescription(e.getMessage());
                    return err;
                }
                finally
                {
                    if (setRollbackOnly && transaction instanceof LocalTransaction)
                    {
                        ((LocalTransaction) transaction).setRollbackOnly();
                    }
                }
            }
            finally
            {
                reference.release();
            }
        }
        return null;
    }

    private boolean shouldReceiverSettleFirst(ReceiverSettleMode transferReceiverSettleMode)
    {
        if (transferReceiverSettleMode == null)
        {
            transferReceiverSettleMode = getReceivingSettlementMode();
        }

        return transferReceiverSettleMode == null || ReceiverSettleMode.FIRST.equals(transferReceiverSettleMode);
    }

    @Override
    protected void remoteDetachedPerformDetach(Detach detach)
    {
        final TerminusExpiryPolicy expiryPolicy = getTarget().getExpiryPolicy();
        if((detach != null && Boolean.TRUE.equals(detach.getClosed()))
           || TerminusExpiryPolicy.LINK_DETACH.equals(expiryPolicy)
           || ((expiryPolicy == null || TerminusExpiryPolicy.SESSION_END.equals(expiryPolicy)) && getSession().isClosing())
           || (TerminusExpiryPolicy.CONNECTION_CLOSE.equals(expiryPolicy) && getSession().getConnection().isClosing()))
        {
            close();
        }
        else if(detach == null || detach.getError() != null)
        {
            detach();
            destroy();
        }
        else
        {
            detach();

            // TODO: QPID-7845: Resuming links is unsupported at the moment. Destroying link unconditionally.
            destroy();
        }
    }

    @Override
    protected Map<Binary, DeliveryState> getLocalUnsettled()
    {
        return new HashMap<>(_unsettled);
    }


    @Override
    public void attachReceived(final Attach attach) throws AmqpErrorException
    {
        super.attachReceived(attach);

        Source source = (Source) attach.getSource();
        Target target = new Target();
        Target attachTarget = (Target) attach.getTarget();

        setDeliveryCount(new SequenceNumber(attach.getInitialDeliveryCount().intValue()));

        target.setAddress(attachTarget.getAddress());
        target.setDynamic(attachTarget.getDynamic());
        if (Boolean.TRUE.equals(attachTarget.getDynamic()) && attachTarget.getDynamicNodeProperties() != null)
        {
            Map<Symbol, Object> dynamicNodeProperties = new HashMap<>();
            if (attachTarget.getDynamicNodeProperties().containsKey(Session_1_0.LIFETIME_POLICY))
            {
                dynamicNodeProperties.put(Session_1_0.LIFETIME_POLICY,
                                          attachTarget.getDynamicNodeProperties().get(Session_1_0.LIFETIME_POLICY));
            }
            target.setDynamicNodeProperties(dynamicNodeProperties);
        }
        target.setDurable(TerminusDurability.min(attachTarget.getDurable(),
                                                 getLink().getHighestSupportedTerminusDurability()));
        final List<Symbol> targetCapabilities = new ArrayList<>();
        if (attachTarget.getCapabilities() != null)
        {
            final List<Symbol> desiredCapabilities = Arrays.asList(attachTarget.getCapabilities());
            if (desiredCapabilities.contains(Symbol.valueOf("temporary-topic")))
            {
                targetCapabilities.add(Symbol.valueOf("temporary-topic"));
            }
            if (desiredCapabilities.contains(Symbol.valueOf("temporary-queue")))
            {
                targetCapabilities.add(Symbol.valueOf("temporary-queue"));
            }
            if (desiredCapabilities.contains(Symbol.valueOf("topic")))
            {
                targetCapabilities.add(Symbol.valueOf("topic"));
            }
            target.setCapabilities(targetCapabilities.toArray(new Symbol[targetCapabilities.size()]));
        }
        target.setExpiryPolicy(attachTarget.getExpiryPolicy());

        final ReceivingDestination destination = getSession().getReceivingDestination(getLink(), target);

        targetCapabilities.addAll(Arrays.asList(destination.getCapabilities()));
        target.setCapabilities(targetCapabilities.toArray(new Symbol[targetCapabilities.size()]));

        setCapabilities(targetCapabilities);
        setDestination(destination);

        if (!Boolean.TRUE.equals(attach.getIncompleteUnsettled()))
        {
            Map remoteUnsettled = attach.getUnsettled();
            Map<Binary, DeliveryState> unsettledCopy = new HashMap<>(_unsettled);
            for (Map.Entry<Binary, DeliveryState> entry : unsettledCopy.entrySet())
            {
                Binary deliveryTag = entry.getKey();
                if (remoteUnsettled == null || !remoteUnsettled.containsKey(deliveryTag))
                {
                    _unsettled.remove(deliveryTag);
                }
            }
        }
        getLink().setTermini(source, target);
        _rejectedOutcomeSupportedBySource =
                source.getOutcomes() != null && Arrays.asList(source.getOutcomes()).contains(Rejected.REJECTED_SYMBOL);
    }

    public ReceivingDestination getReceivingDestination()
    {
        return _receivingDestination;
    }

    public void setDestination(final ReceivingDestination receivingDestination)
    {
        if(_receivingDestination != receivingDestination)
        {
            if (_receivingDestination != null && _receivingDestination.getMessageDestination() != null)
            {
                _receivingDestination.getMessageDestination().linkRemoved(_messageSender, _publishingLink);
            }
            _receivingDestination = receivingDestination;
            if(receivingDestination != null && receivingDestination.getMessageDestination() != null)
            {
                receivingDestination.getMessageDestination().linkAdded(_messageSender, _publishingLink);
            }

        }
    }

    @Override
    public void destroy()
    {
        super.destroy();
        if(_receivingDestination != null && _receivingDestination.getMessageDestination() != null)
        {
            _receivingDestination.getMessageDestination().linkRemoved(_messageSender, _publishingLink);
            _receivingDestination = null;
        }
    }

    @Override
    protected void recoverLink(final Attach attach) throws AmqpErrorException
    {
        if (getTarget() == null)
        {
            throw new AmqpErrorException(new Error(AmqpError.NOT_FOUND,
                                                   String.format("Link '%s' not found", getLinkName())));
        }

        attach.setTarget(getTarget());
        receiveAttach(attach);
    }


    @Override
    protected void reattachLink(final Attach attach) throws AmqpErrorException
    {
        if (attach.getTarget() instanceof Coordinator)
        {
            throw new AmqpErrorException(new Error(AmqpError.PRECONDITION_FAILED, "Cannot reattach standard receiving Link as a transaction coordinator"));
        }

        attachReceived(attach);
    }

    @Override
    protected void resumeLink(final Attach attach) throws AmqpErrorException
    {
        if (getTarget() == null)
        {
            throw new IllegalStateException("Terminus should be set when resuming a Link.");
        }
        if (attach.getTarget() == null)
        {
            throw new IllegalStateException("Attach.getTarget should not be null when resuming a Link. That would be recovering the Link.");
        }
        if (attach.getTarget() instanceof Coordinator)
        {
            throw new AmqpErrorException(new Error(AmqpError.PRECONDITION_FAILED, "Cannot resume standard receiving Link as a transaction coordinator"));
        }

        attachReceived(attach);
    }

    @Override
    protected void establishLink(final Attach attach) throws AmqpErrorException
    {
        if (getSource() != null || getTarget() != null)
        {
            throw new IllegalStateException("Termini should be null when establishing a Link.");
        }

        attachReceived(attach);
    }

    @Override
    public void recordFuture(final ListenableFuture<Void> future, final ServerTransaction.Action action)
    {
        _unfinishedCommandsQueue.add(new AsyncCommand(future, action));
    }

    @Override
    public void receiveComplete()
    {
        AsyncCommand cmd;
        while((cmd = _unfinishedCommandsQueue.poll()) != null)
        {
            cmd.complete();
        }

        processPendingDispositions();
    }

    private void processPendingDispositions()
    {
        Iterator<PendingDispositionHolder> itr = _pendingDispositions.isEmpty() ? Collections.emptyIterator() : _pendingDispositions.iterator();
        if (itr.hasNext())
        {
            try
            {
                PendingDispositionHolder disposition = itr.next();
                PendingDispositionHolder current = disposition;

                Set<Binary> deliveryTags = new HashSet<>();
                deliveryTags.add(disposition.getDeliveryTag());

                while (itr.hasNext())
                {
                    disposition = itr.next();

                    if (current.isSettled() != disposition.isSettled() ||
                        !Objects.equals(current.getResultantState(), disposition.getResultantState()))
                    {
                        updateDispositions(deliveryTags, current.getResultantState(), current.isSettled());
                        deliveryTags.clear();
                        current = disposition;
                    }
                    deliveryTags.add(disposition.getDeliveryTag());
                }
                if (!deliveryTags.isEmpty())
                {
                    updateDispositions(deliveryTags, current.getResultantState(), current.isSettled());
                }
            }
            finally
            {
                _pendingDispositions.clear();
            }
        }
    }

    private static class PendingDispositionHolder
    {
        private final Binary _deliveryTag;
        private final DeliveryState _resultantState;
        private final boolean _settled;

        PendingDispositionHolder(final Binary deliveryTag,
                                 final DeliveryState resultantState,
                                 final boolean settled)
        {
            _deliveryTag = deliveryTag;
            _resultantState = resultantState;
            _settled = settled;
        }

        Binary getDeliveryTag()
        {
            return _deliveryTag;
        }

        DeliveryState getResultantState()
        {
            return _resultantState;
        }

        boolean isSettled()
        {
            return _settled;
        }


    }
}
