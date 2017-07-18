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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.MessageReference;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.plugin.MessageFormat;
import org.apache.qpid.server.protocol.MessageFormatRegistry;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
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
import org.apache.qpid.server.protocol.v1_0.type.transaction.TransactionalState;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Error;
import org.apache.qpid.server.protocol.v1_0.type.transport.ReceiverSettleMode;
import org.apache.qpid.server.txn.AutoCommitTransaction;
import org.apache.qpid.server.txn.LocalTransaction;
import org.apache.qpid.server.txn.ServerTransaction;

public class StandardReceivingLinkEndpoint extends AbstractReceivingLinkEndpoint<Target>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(StandardReceivingLinkEndpoint.class);

    private ReceivingDestination _receivingDestination;

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
                // TODO: create message ?
            }
        }
        else
        {
            ServerMessage<?> serverMessage;
            UnsignedInteger messageFormat = delivery.getMessageFormat();
            DeliveryState xfrState = delivery.getState();
            List<QpidByteBuffer> fragments = delivery.getPayload();
            MessageFormat format = MessageFormatRegistry.getFormat(messageFormat == null ? 0 : messageFormat.intValue());
            if(format != null)
            {
                serverMessage = format.createMessage(fragments, getAddressSpace().getMessageStore(), getSession().getConnection().getReference());
            }
            else
            {
                final Error err = new Error();
                err.setCondition(AmqpError.NOT_IMPLEMENTED);
                err.setDescription("Unknown message format: " + messageFormat);
                return err;
            }

            for(QpidByteBuffer fragment: fragments)
            {
                fragment.dispose();
            }
            fragments = null;

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

                ServerTransaction transaction = null;
                if (transactionId != null)
                {
                    transaction = getSession().getTransaction(transactionId);
                }
                else
                {
                    transaction = new AutoCommitTransaction(getAddressSpace().getMessageStore());
                }

                try
                {
                    Session_1_0 session = getSession();
                    // locally cache arrival time to ensure that we don't reload metadata
                    final long arrivalTime = serverMessage.getArrivalTime();
                    session.getAMQPConnection()
                           .checkAuthorizedMessagePrincipal(serverMessage.getMessageHeader().getUserId());

                    Outcome outcome;
                    Source source = getSource();
                    if (serverMessage.isPersistent() && !getAddressSpace().getMessageStore().isPersistent())
                    {
                        final Error preconditionFailedError = new Error(AmqpError.PRECONDITION_FAILED,
                                                                        "Non-durable message store cannot accept durable message.");
                        if (source.getOutcomes() != null && Arrays.asList(source.getOutcomes())
                                                                  .contains(Rejected.REJECTED_SYMBOL))
                        {
                            final Rejected rejected = new Rejected();
                            rejected.setError(preconditionFailedError);
                            outcome = rejected;
                        }
                        else
                        {
                            return preconditionFailedError;
                        }
                    }
                    else
                    {
                        outcome = getReceivingDestination().send(serverMessage, transaction,
                                                                 session.getSecurityToken());
                    }

                    DeliveryState resultantState;

                    final List<Symbol> sourceSupportedOutcomes = new ArrayList<>();
                    if (source.getOutcomes() != null)
                    {
                        sourceSupportedOutcomes.addAll(Arrays.asList(source.getOutcomes()));
                    }
                    else if (source.getDefaultOutcome() == null)
                    {
                        sourceSupportedOutcomes.add(Accepted.ACCEPTED_SYMBOL);
                    }

                    if (sourceSupportedOutcomes.contains(outcome.getSymbol()))
                    {
                        if (transactionId == null)
                        {
                            resultantState = outcome;
                        }
                        else
                        {
                            TransactionalState transactionalState = new TransactionalState();
                            transactionalState.setOutcome(outcome);
                            transactionalState.setTxnId(transactionId);
                            resultantState = transactionalState;
                        }
                    }
                    else
                    {
                        if(transactionId != null && transaction instanceof LocalTransaction
                           && source.getDefaultOutcome() != null
                           && outcome.getSymbol() != source.getDefaultOutcome().getSymbol())
                        {
                            ((LocalTransaction) transaction).setRollbackOnly();
                        }
                        resultantState = null;
                    }

                    boolean settled = shouldReceiverSettleFirst(transferReceiverSettleMode);

                    updateDisposition(delivery.getDeliveryTag(), resultantState, settled);

                    getSession().getAMQPConnection()
                                .registerMessageReceived(serverMessage.getSize(), arrivalTime);

                    if (!(transaction instanceof AutoCommitTransaction))
                    {
                        ServerTransaction.Action a;
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
                catch (AccessControlException e)
                {
                    final Error err = new Error();
                    err.setCondition(AmqpError.NOT_ALLOWED);
                    err.setDescription(e.getMessage());
                    close(err);

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
    }

    public ReceivingDestination getReceivingDestination()
    {
        return _receivingDestination;
    }

    public void setDestination(final ReceivingDestination receivingDestination)
    {
        _receivingDestination = receivingDestination;
    }

    @Override
    protected void recoverLink(final Attach attach) throws AmqpErrorException
    {
        if (getTarget() == null)
        {
            throw new AmqpErrorException(new Error(AmqpError.NOT_FOUND,
                                                   String.format("Link '%s' not found", getLinkName())));
        }

        Source source = (Source) attach.getSource();
        Target target = getTarget();

        // TODO: This seems a bit weird. Similar code is in attachReceived.
        // We also seem to send back a different target than we are using.
        final ReceivingDestination destination = getSession().getReceivingDestination(getLink(), getTarget());
        target.setCapabilities(destination.getCapabilities());
        setCapabilities(Arrays.asList(destination.getCapabilities()));
        setDestination(destination);
        attachReceived(attach);

        getLink().setTermini(source, target);
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
}
