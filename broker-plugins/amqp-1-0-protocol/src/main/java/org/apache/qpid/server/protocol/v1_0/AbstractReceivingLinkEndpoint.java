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
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.server.protocol.v1_0.messaging.SectionDecoder;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionDecoderImpl;
import org.apache.qpid.server.protocol.v1_0.type.BaseTarget;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.DeliveryState;
import org.apache.qpid.server.protocol.v1_0.type.Outcome;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
import org.apache.qpid.server.protocol.v1_0.type.transaction.TransactionalState;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Error;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.server.protocol.v1_0.type.transport.Transfer;

public abstract class AbstractReceivingLinkEndpoint<T extends BaseTarget> extends AbstractLinkEndpoint<Source, T>
{
    private final SectionDecoder _sectionDecoder;
    private UnsignedInteger _lastDeliveryId;
    private ReceivingDestination _receivingDestination;
    private Map<Binary, Object> _unsettledMap = new LinkedHashMap<>();
    private Map<Binary, TransientState> _unsettledIds = new LinkedHashMap<>();
    private boolean _creditWindow;


    private static class TransientState
    {

        UnsignedInteger _deliveryId;
        int _credit = 1;
        boolean _settled;

        private TransientState(final UnsignedInteger transferId)
        {
            _deliveryId = transferId;
        }

        void incrementCredit()
        {
            _credit++;
        }

        public int getCredit()
        {
            return _credit;
        }

        public UnsignedInteger getDeliveryId()
        {
            return _deliveryId;
        }

        public boolean isSettled()
        {
            return _settled;
        }

        public void setSettled(boolean settled)
        {
            _settled = settled;
        }
    }


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

    Error receiveTransfer(final Transfer transfer, final Delivery delivery)
    {
        if(isAttached())
        {
            TransientState transientState;
            final Binary deliveryTag = delivery.getDeliveryTag();
            boolean existingState = _unsettledMap.containsKey(deliveryTag);
            if (!existingState || transfer.getState() != null)
            {
                _unsettledMap.put(deliveryTag, transfer.getState());
            }
            if (!existingState)
            {
                transientState = new TransientState(transfer.getDeliveryId());
                if (delivery.isSettled())
                {
                    transientState.setSettled(true);
                }
                _unsettledIds.put(deliveryTag, transientState);
                setLinkCredit(getLinkCredit().subtract(UnsignedInteger.ONE));
                setDeliveryCount(getDeliveryCount().add(UnsignedInteger.ONE));
            }
            else
            {
                transientState = _unsettledIds.get(deliveryTag);
                transientState.incrementCredit();
                if (delivery.isSettled())
                {
                    transientState.setSettled(true);
                }
            }

            if (transientState.isSettled() && delivery.isComplete())
            {
                _unsettledMap.remove(deliveryTag);
            }
            return messageTransfer(transfer);
        }
        else
        {
            getSession().updateDisposition(Role.RECEIVER, transfer.getDeliveryId(), transfer.getDeliveryId(),null, true);
            return null;
        }
    }

    protected abstract Error messageTransfer(final Transfer transfer);

    public ReceivingDestination getReceivingDestination()
    {
        return _receivingDestination;
    }

    public void setDestination(final ReceivingDestination receivingDestination)
    {
        _receivingDestination = receivingDestination;
    }

    @Override public void receiveFlow(final Flow flow)
    {
        setAvailable(flow.getAvailable());
        setDeliveryCount(flow.getDeliveryCount());
    }

    public boolean settled(final Binary deliveryTag)
    {
        boolean deleted;
        if (deleted = (_unsettledIds.remove(deliveryTag) != null))
        {
            _unsettledMap.remove(deliveryTag);

        }

        return deleted;
    }

    public void updateDisposition(final Binary deliveryTag, DeliveryState state, boolean settled)
    {
        if (_unsettledMap.containsKey(deliveryTag))
        {
            boolean outcomeUpdate = false;
            Outcome outcome = null;
            if (state instanceof Outcome)
            {
                outcome = (Outcome) state;
            }
            else if (state instanceof TransactionalState)
            {
                // TODO? Is this correct
                outcome = ((TransactionalState) state).getOutcome();
            }

            if (outcome != null)
            {
                Object oldOutcome = _unsettledMap.put(deliveryTag, outcome);
                outcomeUpdate = !outcome.equals(oldOutcome);
            }


            TransientState transientState = _unsettledIds.get(deliveryTag);
            if (outcomeUpdate || settled)
            {

                final UnsignedInteger transferId = transientState.getDeliveryId();

                getSession().updateDisposition(getRole(), transferId, transferId, state, settled);
            }


            if (settled)
            {

                if (settled(deliveryTag))
                {
                    if (!isDetached() && _creditWindow)
                    {
                        setLinkCredit(getLinkCredit().add(UnsignedInteger.ONE));
                        sendFlowConditional();
                    }
                    else
                    {
                        getSession().sendFlowConditional();
                    }
                }
            }
        }
        else
        {
            TransientState transientState = _unsettledIds.get(deliveryTag);
            if (_creditWindow)
            {
                setLinkCredit(getLinkCredit().add(UnsignedInteger.ONE));
                sendFlowConditional();
            }

        }
    }

    public void setCreditWindow()
    {
        setCreditWindow(true);
    }

    public void setCreditWindow(boolean window)
    {
        _creditWindow = window;
        sendFlowConditional();
    }

    @Override
    public void receiveDeliveryState(final Delivery unsettled, final DeliveryState state, final Boolean settled)
    {
        super.receiveDeliveryState(unsettled, state, settled);
        if(_creditWindow)
        {
            if(Boolean.TRUE.equals(settled))
            {
                setLinkCredit(getLinkCredit().add(UnsignedInteger.ONE));
                sendFlowConditional();
            }
        }
    }

    SectionDecoder getSectionDecoder()
    {
        return _sectionDecoder;
    }

    @Override
    public void settle(Binary deliveryTag)
    {
        super.settle(deliveryTag);
        _unsettledIds.remove(deliveryTag);
        _unsettledMap.remove(deliveryTag);
        if(_creditWindow)
        {
             sendFlowConditional();
        }

    }

    public void flowStateChanged()
    {
    }

    UnsignedInteger getLastDeliveryId()
    {
        return _lastDeliveryId;
    }

    void setLastDeliveryId(UnsignedInteger lastDeliveryId)
    {
        _lastDeliveryId = lastDeliveryId;
    }


}
