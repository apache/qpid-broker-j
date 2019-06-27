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

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.qpid.server.protocol.v1_0.delivery.UnsettledDelivery;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionDecoder;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionDecoderImpl;
import org.apache.qpid.server.protocol.v1_0.type.BaseTarget;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.DeliveryState;
import org.apache.qpid.server.protocol.v1_0.type.Outcome;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
import org.apache.qpid.server.protocol.v1_0.type.transaction.TransactionError;
import org.apache.qpid.server.protocol.v1_0.type.transaction.TransactionalState;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.End;
import org.apache.qpid.server.protocol.v1_0.type.transport.Error;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.LinkError;
import org.apache.qpid.server.protocol.v1_0.type.transport.ReceiverSettleMode;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.server.protocol.v1_0.type.transport.SessionError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Transfer;

public abstract class AbstractReceivingLinkEndpoint<T extends BaseTarget> extends AbstractLinkEndpoint<Source, T>
{
    private final SectionDecoder _sectionDecoder;
    final Map<Binary, DeliveryState> _unsettled = Collections.synchronizedMap(new LinkedHashMap<>());

    private volatile boolean _creditWindow;
    private volatile Delivery _currentDelivery;

    public AbstractReceivingLinkEndpoint(final Session_1_0 session, final Link_1_0<Source, T> link)
    {
        super(session, link);
        _sectionDecoder = new SectionDecoderImpl(session.getConnection()
                                                        .getDescribedTypeRegistry()
                                                        .getSectionDecoderRegistry());
    }

    @Override
    protected Map<Symbol, Object> initProperties(final Attach attach)
    {
        return Collections.emptyMap();
    }


    @Override public Role getRole()
    {
        return Role.RECEIVER;
    }

    void receiveTransfer(final Transfer transfer)
    {
        if (!isErrored())
        {
            Error error = validateTransfer(transfer);
            if (error != null)
            {
                transfer.dispose();
                if (_currentDelivery != null)
                {
                    _currentDelivery.discard();
                    _currentDelivery = null;
                }
                close(error);
                return;
            }

            if (_currentDelivery == null)
            {
                error = validateNewTransfer(transfer);
                if (error != null)
                {
                    transfer.dispose();
                    close(error);
                    return;
                }
                _currentDelivery = new Delivery(transfer, this);

                setLinkCredit(getLinkCredit().subtract(UnsignedInteger.ONE));
                getDeliveryCount().incr();

                getSession().getIncomingDeliveryRegistry()
                            .addDelivery(transfer.getDeliveryId(),
                                         new UnsettledDelivery(transfer.getDeliveryTag(), this));
            }
            else
            {
                error = validateSubsequentTransfer(transfer);
                if (error != null)
                {
                    transfer.dispose();
                    _currentDelivery.discard();
                    _currentDelivery = null;
                    close(error);
                    return;
                }
                _currentDelivery.addTransfer(transfer);
            }

            if (_currentDelivery.getTotalPayloadSize() > getSession().getConnection().getMaxMessageSize())
            {
                error = new Error(LinkError.MESSAGE_SIZE_EXCEEDED,
                                  String.format("delivery '%s' exceeds max-message-size %d",
                                                _currentDelivery.getDeliveryTag(),
                                                getSession().getConnection().getMaxMessageSize()));
                _currentDelivery.discard();
                _currentDelivery = null;
                close(error);
                return;
            }

            if (!_currentDelivery.getResume())
            {
                _unsettled.put(_currentDelivery.getDeliveryTag(), _currentDelivery.getState());
            }

            if (_currentDelivery.isAborted() || (_currentDelivery.getResume() && !_unsettled.containsKey(_currentDelivery.getDeliveryTag())))
            {
                _unsettled.remove(_currentDelivery.getDeliveryTag());
                getSession().getIncomingDeliveryRegistry().removeDelivery(_currentDelivery.getDeliveryId());
                _currentDelivery = null;

                setLinkCredit(getLinkCredit().add(UnsignedInteger.ONE));
                getDeliveryCount().decr();
            }
            else if (_currentDelivery.isComplete())
            {
                try
                {
                    if (_currentDelivery.isSettled())
                    {
                        _unsettled.remove(_currentDelivery.getDeliveryTag());
                        getSession().getIncomingDeliveryRegistry().removeDelivery(_currentDelivery.getDeliveryId());
                    }
                    error = receiveDelivery(_currentDelivery);
                    if (error != null)
                    {
                        close(error);
                    }
                }
                finally
                {
                    _currentDelivery = null;
                }
            }
        }
        else
        {
            End end = new End();
            end.setError(new Error(SessionError.ERRANT_LINK,
                                   String.format("Received TRANSFER for link handle %s which is in errored state.",
                                                 transfer.getHandle())));
            getSession().end(end);
        }
    }

    private Error validateTransfer(final Transfer transfer)
    {
        Error error = null;
        if (!ReceiverSettleMode.SECOND.equals(getReceivingSettlementMode())
            && ReceiverSettleMode.SECOND.equals(transfer.getRcvSettleMode()))
        {
            error = new Error(AmqpError.INVALID_FIELD,
                              "Transfer \"rcv-settle-mode\" cannot be \"first\" when link \"rcv-settle-mode\" is set to \"second\".");
        }
        else if (transfer.getState() instanceof TransactionalState)
        {
            final Binary txnId = ((TransactionalState) transfer.getState()).getTxnId();
            try
            {
                getSession().getTransaction(txnId);
            }
            catch (UnknownTransactionException e)
            {
                error = new Error(TransactionError.UNKNOWN_ID,
                                  String.format("Transfer has an unknown transaction-id '%s'.", txnId));
            }
        }
        return error;
    }

    private Error validateNewTransfer(final Transfer transfer)
    {
        Error error = null;
        if (transfer.getDeliveryId() == null)
        {
            error = new Error(AmqpError.INVALID_FIELD,
                                    "Transfer \"delivery-id\" is required for a new delivery.");
        }
        else if (transfer.getDeliveryTag() == null)
        {
            error = new Error(AmqpError.INVALID_FIELD,
                                    "Transfer \"delivery-tag\" is required for a new delivery.");
        }
        else if (!Boolean.TRUE.equals(transfer.getResume()))
        {
            if (_unsettled.containsKey(transfer.getDeliveryTag()))
            {
                error = new Error(AmqpError.ILLEGAL_STATE,
                                  String.format("Delivery-tag '%s' is used by another unsettled delivery."
                                                + " The delivery-tag MUST be unique amongst all deliveries that"
                                                + " could be considered unsettled by either end of the link.",
                                                transfer.getDeliveryTag()));
            }
            else if (_localIncompleteUnsettled || _remoteIncompleteUnsettled)
            {
                error = new Error(AmqpError.ILLEGAL_STATE,
                                  "Cannot accept new deliveries while incomplete-unsettled is true.");
            }
        }

        return error;
    }

    private Error validateSubsequentTransfer(final Transfer transfer)
    {
        Error error = null;
        if (transfer.getDeliveryId() != null && !_currentDelivery.getDeliveryId()
                                                                 .equals(transfer.getDeliveryId()))
        {
            error = new Error(AmqpError.INVALID_FIELD,
                              String.format(
                                      "Unexpected transfer \"delivery-id\" for multi-transfer delivery: found '%s', expected '%s'.",
                                      transfer.getDeliveryId(),
                                      _currentDelivery.getDeliveryId()));
        }
        else if (transfer.getDeliveryTag() != null && !_currentDelivery.getDeliveryTag()
                                                                       .equals(transfer.getDeliveryTag()))
        {
            error = new Error(AmqpError.INVALID_FIELD,
                              String.format(
                                      "Unexpected transfer \"delivery-tag\" for multi-transfer delivery: found '%s', expected '%s'.",
                                      transfer.getDeliveryTag(),
                                      _currentDelivery.getDeliveryTag()));
        }
        else if (_currentDelivery.getReceiverSettleMode() != null && transfer.getRcvSettleMode() != null
                 && !_currentDelivery.getReceiverSettleMode().equals(transfer.getRcvSettleMode()))
        {
            error = new Error(AmqpError.INVALID_FIELD,
                              "Transfer \"rcv-settle-mode\" is set to different value than on previous transfer.");
        }
        else if (transfer.getMessageFormat() != null && !_currentDelivery.getMessageFormat()
                                                                         .equals(transfer.getMessageFormat()))
        {
            error = new Error(AmqpError.INVALID_FIELD,
                              "Transfer \"message-format\" is set to different value than on previous transfer.");
        }

        return error;
    }

    protected abstract Error receiveDelivery(final Delivery delivery);

    @Override
    public void receiveFlow(final Flow flow)
    {
        setAvailable(flow.getAvailable());
        setDeliveryCount(new SequenceNumber(flow.getDeliveryCount().intValue()));

        if (Boolean.TRUE.equals(flow.getEcho()))
        {
            sendFlow();
        }
    }

    private boolean settled(final Binary deliveryTag)
    {
        return _unsettled.remove(deliveryTag) != null;
    }

    void updateDisposition(final Binary deliveryTag,
                           final DeliveryState state,
                           final boolean settled)
    {
        updateDispositions(Collections.singleton(deliveryTag), state, settled);
    }

    void updateDispositions(final Set<Binary> deliveryTags,
                           final DeliveryState state,
                           final boolean settled)
    {

        final Set<Binary> unsettledKeys = new HashSet<>(_unsettled.keySet());
        unsettledKeys.retainAll(deliveryTags);
        final int settledDeliveryCount = deliveryTags.size() - unsettledKeys.size();

        if (!unsettledKeys.isEmpty())
        {
            boolean outcomeUpdate = false;
            Outcome outcome = null;
            if (state instanceof Outcome)
            {
                outcome = (Outcome) state;
            }
            else if (state instanceof TransactionalState)
            {
                outcome = ((TransactionalState) state).getOutcome();
            }

            if (outcome != null)
            {
                for (final Binary deliveryTag : unsettledKeys)
                {
                    if (!(_unsettled.get(deliveryTag) instanceof Outcome))
                    {
                        Object oldOutcome = _unsettled.put(deliveryTag, outcome);
                        outcomeUpdate = outcomeUpdate || !outcome.equals(oldOutcome);
                    }
                }
            }

            if (outcomeUpdate || settled)
            {
                getSession().updateDisposition(this, deliveryTags, state, settled);
            }

            if (settled)
            {

                int credit = 0;
                for (final Binary deliveryTag : unsettledKeys)
                {
                    if (settled(deliveryTag))
                    {
                        if (!isDetached() && _creditWindow)
                        {
                            credit++;
                        }
                    }
                }

                if (credit > 0)
                {
                    setLinkCredit(getLinkCredit().add(UnsignedInteger.valueOf(credit)));
                    sendFlowConditional();
                }
                else
                {
                    getSession().sendFlowConditional();
                }
            }
        }

        if (settledDeliveryCount > 0 && _creditWindow)
        {
            setLinkCredit(getLinkCredit().add(UnsignedInteger.ONE));
            sendFlowConditional();
        }
    }

    void setCreditWindow()
    {
        setCreditWindow(true);
    }

    private void setCreditWindow(boolean window)
    {
        _creditWindow = window;
        sendFlowConditional();
    }

    SectionDecoder getSectionDecoder()
    {
        return _sectionDecoder;
    }

    @Override
    public void settle(Binary deliveryTag)
    {
        super.settle(deliveryTag);
        _unsettled.remove(deliveryTag);
        if (_creditWindow)
        {
            setLinkCredit(getLinkCredit().add(UnsignedInteger.ONE));
            sendFlowConditional();
        }
    }

    @Override
    public void flowStateChanged()
    {
    }

    @Override
    protected void detach(final Error error, final boolean close)
    {
        try
        {
            super.detach(error, close);
        }
        finally
        {
            if (_currentDelivery != null)
            {
                _currentDelivery.discard();
                _currentDelivery = null;
            }
        }
    }

    @Override
    protected void handleDeliveryState(Binary deliveryTag, DeliveryState state, Boolean settled)
    {
        if(Boolean.TRUE.equals(settled))
        {
            _unsettled.remove(deliveryTag);
        }
    }
}
