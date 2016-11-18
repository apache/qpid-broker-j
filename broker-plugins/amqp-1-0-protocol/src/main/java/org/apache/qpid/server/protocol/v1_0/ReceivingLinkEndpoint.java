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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.DeliveryState;
import org.apache.qpid.server.protocol.v1_0.type.Outcome;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.transaction.TransactionalState;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.server.protocol.v1_0.type.transport.Transfer;

public class ReceivingLinkEndpoint extends LinkEndpoint<ReceivingLinkListener>
{


    private UnsignedInteger _lastDeliveryId;

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

    private Map<Binary, Object> _unsettledMap = new LinkedHashMap<Binary, Object>();
    private Map<Binary, TransientState> _unsettledIds = new LinkedHashMap<Binary, TransientState>();
    private boolean _creditWindow;
    private boolean _remoteDrain;
    private UnsignedInteger _remoteTransferCount;
    private UnsignedInteger _drainLimit;


    public ReceivingLinkEndpoint(final Session_1_0 session, final Attach attach)
    {
        super(session, attach);
        setDeliveryCount(attach.getInitialDeliveryCount());
        setLinkEventListener(ReceivingLinkListener.DEFAULT);
        setSendingSettlementMode(attach.getSndSettleMode());
        setReceivingSettlementMode(attach.getRcvSettleMode());
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

    void receiveTransfer(final Transfer transfer, final Delivery delivery)
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
        getLinkEventListener().messageTransfer(transfer);

    }

    @Override public void receiveFlow(final Flow flow)
    {
        super.receiveFlow(flow);
        _remoteDrain = Boolean.TRUE.equals((Boolean) flow.getDrain());
        setAvailable(flow.getAvailable());
        setDeliveryCount(flow.getDeliveryCount());
    }


    public boolean isDrained()
    {
        return getDrain() && getDeliveryCount().equals(getDrainLimit());
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

    public void drain()
    {
        setDrain(true);
        _creditWindow = false;
        _drainLimit = getDeliveryCount().add(getLinkCredit());
        sendFlowWithEcho();
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

    public void requestTransactionalSend(Object txnId)
    {
        setDrain(true);
        _creditWindow = false;
        setTransactionId(txnId);
        sendFlow();
    }

    private void sendFlow(final Object transactionId)
    {
        sendFlow();
    }


    public void clearDrain()
    {
        setDrain(false);
        sendFlow();
    }

    public void updateAllDisposition(Binary deliveryTag, DeliveryState deliveryState, boolean settled)
    {
        if(!_unsettledIds.isEmpty())
        {
            Binary firstTag = _unsettledIds.keySet().iterator().next();
            Binary lastTag = deliveryTag;
            updateDispositions(firstTag, lastTag, deliveryState, settled);
        }
    }

    private void updateDispositions(Binary firstTag, Binary lastTag, DeliveryState state, boolean settled)
    {
        SortedMap<UnsignedInteger, UnsignedInteger> ranges = new TreeMap<UnsignedInteger, UnsignedInteger>();

        Iterator<Binary> iter = _unsettledIds.keySet().iterator();
        List<Binary> tagsToUpdate = new ArrayList<Binary>();
        Binary tag = null;

        while (iter.hasNext() && !(tag = iter.next()).equals(firstTag)) ;

        if (firstTag.equals(tag))
        {
            tagsToUpdate.add(tag);

            UnsignedInteger deliveryId = _unsettledIds.get(firstTag).getDeliveryId();

            UnsignedInteger first = deliveryId;
            UnsignedInteger last = first;

            if (!firstTag.equals(lastTag))
            {
                while (iter.hasNext())
                {
                    tag = iter.next();
                    tagsToUpdate.add(tag);

                    deliveryId = _unsettledIds.get(tag).getDeliveryId();

                    if (deliveryId.equals(last.add(UnsignedInteger.ONE)))
                    {
                        last = deliveryId;
                    }
                    else
                    {
                        ranges.put(first, last);
                        first = last = deliveryId;
                    }

                    if (tag.equals(lastTag))
                    {
                        break;
                    }
                }
            }
            ranges.put(first, last);
        }

        if (settled)
        {

            for (Binary deliveryTag : tagsToUpdate)
            {
                if (settled(deliveryTag) && _creditWindow)
                {
                    setLinkCredit(getLinkCredit().add(UnsignedInteger.ONE));
                }
            }
            sendFlowConditional();
        }


        for (Map.Entry<UnsignedInteger, UnsignedInteger> range : ranges.entrySet())
        {
            getSession().updateDisposition(getRole(), range.getKey(), range.getValue(), state, settled);
        }
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

    public UnsignedInteger getDrainLimit()
    {
        return _drainLimit;
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
