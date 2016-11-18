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
import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.DeliveryState;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.server.protocol.v1_0.type.transport.Transfer;

public class SendingLinkEndpoint extends LinkEndpoint<SendingLink_1_0>
{

    public static final Symbol PRIORITY = Symbol.valueOf("priority");
    private UnsignedInteger _lastDeliveryId;
    private Binary _lastDeliveryTag;
    private Map<Binary, UnsignedInteger> _unsettledMap = new HashMap<Binary, UnsignedInteger>();
    private Binary _transactionId;
    private Integer _priority;


    public SendingLinkEndpoint(final Session_1_0 sessionEndpoint, final Attach attach)
    {
        super(sessionEndpoint, attach);
        setSendingSettlementMode(attach.getSndSettleMode());
        setReceivingSettlementMode(attach.getRcvSettleMode());
        init();
    }

    @Override
    protected Map<Symbol, Object> initProperties(final Attach attach)
    {

        Map<Symbol, Object> peerProperties = attach.getProperties();
        if(peerProperties != null)
        {
            Map<Symbol, Object> actualProperties = new HashMap<>();
            if(peerProperties.containsKey(PRIORITY))
            {
                Object value = peerProperties.get(PRIORITY);
                if(value instanceof Number)
                {
                    _priority = ((Number)value).intValue();
                }
                else if(value instanceof String)
                {
                    try
                    {
                        _priority = Integer.parseInt(value.toString());
                    }
                    catch (NumberFormatException e)
                    {
                    }
                }
                if(_priority != null)
                {
                    actualProperties.put(PRIORITY, _priority);
                }
            }
            return actualProperties;
        }
        else
        {

            return Collections.emptyMap();
        }
    }

    private void init()
    {
        setDeliveryCount(UnsignedInteger.valueOf(0));
        setAvailable(UnsignedInteger.valueOf(0));
    }

    @Override public Role getRole()
    {
        return Role.SENDER;
    }

    public Integer getPriority()
    {
        return _priority;
    }

    public boolean transfer(final Transfer xfr, final boolean decrementCredit)
    {
        Session_1_0 s = getSession();
        xfr.setMessageFormat(UnsignedInteger.ZERO);
        if(decrementCredit)
        {
            setLinkCredit(getLinkCredit().subtract(UnsignedInteger.ONE));
        }

        setDeliveryCount(UnsignedInteger.valueOf((getDeliveryCount().intValue() + 1)));

        xfr.setHandle(getLocalHandle());

        s.sendTransfer(xfr, this, !xfr.getDeliveryTag().equals(_lastDeliveryTag));

        if(!Boolean.TRUE.equals(xfr.getSettled()))
        {
            _unsettledMap.put(xfr.getDeliveryTag(), xfr.getDeliveryId());
        }

        if(Boolean.TRUE.equals(xfr.getMore()))
        {
            _lastDeliveryTag = xfr.getDeliveryTag();
        }
        else
        {
            _lastDeliveryTag = null;
        }

        return true;
    }


    public void drained()
    {
        setDeliveryCount(getDeliveryCount().add(getLinkCredit()));
        setLinkCredit(UnsignedInteger.ZERO);
        sendFlow();
    }

    @Override
    public void receiveFlow(final Flow flow)
    {
        super.receiveFlow(flow);
        UnsignedInteger t = flow.getDeliveryCount();
        UnsignedInteger c = flow.getLinkCredit();
        setDrain(flow.getDrain());

        Map options;
        if((options = flow.getProperties()) != null)
        {
             _transactionId = (Binary) options.get(Symbol.valueOf("txn-id"));
        }

        if(t == null)
        {
            setLinkCredit(c);
        }
        else
        {
            UnsignedInteger limit = t.add(c);
            if(limit.compareTo(getDeliveryCount())<=0)
            {
                setLinkCredit(UnsignedInteger.valueOf(0));
            }
            else
            {
                setLinkCredit(limit.subtract(getDeliveryCount()));
            }
        }
        flowStateChanged();

    }

    @Override
    public void flowStateChanged()
    {
        getLink().flowStateChanged();
    }

    public boolean hasCreditToSend()
    {
        UnsignedInteger linkCredit = getLinkCredit();
        return linkCredit != null && (linkCredit.compareTo(UnsignedInteger.valueOf(0)) > 0)
               && getSession().hasCreditToSend();
    }

    public void receiveDeliveryState(final Delivery unsettled,
                                               final DeliveryState state,
                                               final Boolean settled)
    {
        super.receiveDeliveryState(unsettled, state, settled);
        if(settled)
        {
            _unsettledMap.remove(unsettled.getDeliveryTag());
        }
    }

    public UnsignedInteger getLastDeliveryId()
    {
        return _lastDeliveryId;
    }

    public void setLastDeliveryId(final UnsignedInteger deliveryId)
    {
        _lastDeliveryId = deliveryId;
    }

    public void updateDisposition(final Binary deliveryTag, DeliveryState state, boolean settled)
    {
        UnsignedInteger deliveryId;
        if(settled && (deliveryId = _unsettledMap.remove(deliveryTag))!=null)
        {
            settle(deliveryTag);
            getSession().updateDisposition(getRole(), deliveryId, deliveryId, state, settled);
        }
    }

    public Binary getTransactionId()
    {
        return _transactionId;
    }

}
