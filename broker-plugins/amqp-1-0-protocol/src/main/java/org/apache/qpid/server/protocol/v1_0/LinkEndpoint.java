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
import org.apache.qpid.server.protocol.v1_0.type.Source;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.Target;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedLong;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Error;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.ReceiverSettleMode;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.server.protocol.v1_0.type.transport.SenderSettleMode;

public abstract class LinkEndpoint<T extends Link_1_0>
{

    private T _link;
    private Object _flowTransactionId;
    private SenderSettleMode _sendingSettlementMode;
    private ReceiverSettleMode _receivingSettlementMode;
    private Map _initialUnsettledMap;
    private Map _localUnsettled;
    private UnsignedInteger _lastSentCreditLimit;
    private volatile boolean _stopped;
    private volatile boolean _stoppedUpdated;

    private enum State
    {
        DETACHED,
        ATTACH_SENT,
        ATTACH_RECVD,
        ATTACHED,
        DETACH_SENT,
        DETACH_RECVD
    };

    private final String _name;

    private Session_1_0 _session;


    private volatile State _state = State.DETACHED;

    private Source _source;
    private Target _target;
    private UnsignedInteger _deliveryCount;
    private UnsignedInteger _linkCredit;
    private UnsignedInteger _available;
    private Boolean _drain;
    private UnsignedInteger _localHandle;
    private UnsignedLong _maxMessageSize;
    private Map<Symbol, Object> _properties;

    private Map<Binary,Delivery> _unsettledTransfers = new HashMap<Binary,Delivery>();

    LinkEndpoint(final Session_1_0 sessionEndpoint,final Attach attach)
    {
        _session = sessionEndpoint;

        _name = attach.getName();
        _initialUnsettledMap = attach.getUnsettled();
        _properties = initProperties(attach);
        _state = State.ATTACH_RECVD;
    }

    public boolean isStopped()
    {
        return _stopped;
    }

    public void setStopped(final boolean stopped)
    {
        if(_stopped != stopped)
        {
            _stopped = stopped;
            _stoppedUpdated = true;
            sendFlowConditional();
        }
    }

    protected abstract Map<Symbol,Object> initProperties(final Attach attach);

    public String getName()
    {
        return _name;
    }

    public abstract Role getRole();

    public Source getSource()
    {
        return _source;
    }

    public void setSource(final Source source)
    {
        _source = source;
    }

    public Target getTarget()
    {
        return _target;
    }

    public void setTarget(final Target target)
    {
        _target = target;
    }

    public void setDeliveryCount(final UnsignedInteger deliveryCount)
    {
        _deliveryCount = deliveryCount;
    }

    public void setLinkCredit(final UnsignedInteger linkCredit)
    {
        _linkCredit = linkCredit;
    }

    public void setAvailable(final UnsignedInteger available)
    {
        _available = available;
    }

    public void setDrain(final Boolean drain)
    {
        _drain = drain;
    }

    public UnsignedInteger getDeliveryCount()
    {
        return _deliveryCount;
    }

    public UnsignedInteger getAvailable()
    {
        return _available;
    }

    public Boolean getDrain()
    {
        return _drain;
    }

    public UnsignedInteger getLinkCredit()
    {
        return _linkCredit;
    }

    public void remoteDetached(final Detach detach)
    {
        switch (_state)
        {
            case DETACH_SENT:
                _state = State.DETACHED;
                break;
            case ATTACHED:
                _state = State.DETACH_RECVD;
                _link.remoteDetached(LinkEndpoint.this, detach);
                break;
        }
    }

    public void receiveFlow(final Flow flow)
    {
    }

    public void addUnsettled(final Delivery unsettled)
    {
        _unsettledTransfers.put(unsettled.getDeliveryTag(), unsettled);
    }

    public void receiveDeliveryState(final Delivery unsettled,
                                     final DeliveryState state,
                                     final Boolean settled)
    {
        // TODO
        if (_link != null)
        {
            _link.handle(unsettled.getDeliveryTag(), state, settled);
        }

        if (Boolean.TRUE.equals(settled))
        {
            settle(unsettled.getDeliveryTag());
        }
    }

    public void settle(final Binary deliveryTag)
    {
        _unsettledTransfers.remove(deliveryTag);
    }

    void setLocalHandle(final UnsignedInteger localHandle)
    {
        _localHandle = localHandle;
    }

    void receiveAttach(final Attach attach)
    {
        switch (_state)
        {
            case ATTACH_SENT:
            {

                _state = State.ATTACHED;

                _initialUnsettledMap = attach.getUnsettled();
                    /*  TODO - don't yet handle:

                        attach.getProperties();
                        attach.getDurable();
                        attach.getExpiryPolicy();
                        attach.getTimeout();
                     */

                break;
            }

            case DETACHED:
            {
                _state = State.ATTACH_RECVD;
                break;
            }


        }

        if (attach.getRole() == Role.SENDER)
        {
            _source = attach.getSource();
        }
        else
        {
            _target = attach.getTarget();
        }

        if (getRole() == Role.SENDER)
        {
            _maxMessageSize = attach.getMaxMessageSize();
        }
    }

    boolean isAttached()
    {
        return _state == State.ATTACHED;
    }

    boolean isDetached()
    {
        return _state == State.DETACHED || _session.isEnded();
    }

    public Session_1_0 getSession()
    {
        return _session;
    }

    UnsignedInteger getLocalHandle()
    {
        return _localHandle;
    }


    public void attach()
    {
        Attach attachToSend = new Attach();
        attachToSend.setName(getName());
        attachToSend.setRole(getRole());
        attachToSend.setHandle(getLocalHandle());
        attachToSend.setSource(getSource());
        attachToSend.setTarget(getTarget());
        attachToSend.setSndSettleMode(getSendingSettlementMode());
        attachToSend.setRcvSettleMode(getReceivingSettlementMode());
        attachToSend.setUnsettled(_localUnsettled);
        attachToSend.setProperties(_properties);

        if (getRole() == Role.SENDER)
        {
            attachToSend.setInitialDeliveryCount(_deliveryCount);
        }

        switch (_state)
        {
            case DETACHED:
                _state = State.ATTACH_SENT;
                break;
            case ATTACH_RECVD:
                _state = State.ATTACHED;
                break;
            default:
                // TODO ERROR
        }

        getSession().sendAttach(attachToSend);

    }


    public void detach()
    {
        detach(null, false);
    }

    public void close()
    {
        detach(null, true);
    }

    public void close(Error error)
    {
        detach(error, true);
    }

    public void detach(Error error)
    {
        detach(error, false);
    }

    private void detach(Error error, boolean close)
    {
        //TODO
        switch (_state)
        {
            case ATTACHED:
                _state = State.DETACH_SENT;
                break;
            case DETACH_RECVD:
                _state = State.DETACHED;
                break;
            default:
                return;
        }

        if (!(getSession().getState() == SessionState.END_RECVD || getSession().isEnded()))
        {
            Detach detach = new Detach();
            detach.setHandle(getLocalHandle());
            if (close)
                detach.setClosed(close);
            detach.setError(error);

            getSession().sendDetach(detach);
        }
    }




    public void setTransactionId(final Object txnId)
    {
        _flowTransactionId = txnId;
    }

    public void sendFlowConditional()
    {
        if(_lastSentCreditLimit != null)
        {
            if(_stoppedUpdated)
            {
                sendFlow(_flowTransactionId != null);
                _stoppedUpdated = false;
            }
            else
            {
                UnsignedInteger clientsCredit = _lastSentCreditLimit.subtract(_deliveryCount);

                // client has used up over half their credit allowance ?
                boolean sendFlow = _linkCredit.subtract(clientsCredit).compareTo(clientsCredit) >= 0;
                if (sendFlow)
                {
                    sendFlow(_flowTransactionId != null);
                }
                else
                {
                    getSession().sendFlowConditional();
                }
            }
        }
        else
        {
            sendFlow(_flowTransactionId != null);
        }
    }


    public void sendFlow()
    {
        sendFlow(_flowTransactionId != null);
    }

    public void sendFlowWithEcho()
    {
        sendFlow(_flowTransactionId != null, true);
    }


    public void sendFlow(boolean setTransactionId)
    {
        sendFlow(setTransactionId, false);
    }

    public void sendFlow(boolean setTransactionId, boolean echo)
    {
        if(_state == State.ATTACHED || _state == State.ATTACH_SENT)
        {
            Flow flow = new Flow();
            flow.setDeliveryCount(_deliveryCount);
            flow.setEcho(echo);
            if(_stopped)
            {
                flow.setLinkCredit(UnsignedInteger.ZERO);
                flow.setDrain(true);
                _lastSentCreditLimit = _deliveryCount;
            }
            else
            {
                flow.setLinkCredit(_linkCredit);
                _lastSentCreditLimit = _linkCredit.add(_deliveryCount);
                flow.setDrain(_drain);
            }
            flow.setAvailable(_available);
            if(setTransactionId)
            {
                flow.setProperties(Collections.singletonMap(Symbol.valueOf("txn-id"), _flowTransactionId));
            }
            flow.setHandle(getLocalHandle());
            getSession().sendFlow(flow);
        }
    }

    public T getLink()
    {
        return _link;
    }

    public void setLink(final T link)
    {
        _link = link;
    }

    public void setSendingSettlementMode(SenderSettleMode sendingSettlementMode)
    {
        _sendingSettlementMode = sendingSettlementMode;
    }

    public SenderSettleMode getSendingSettlementMode()
    {
        return _sendingSettlementMode;
    }

    public ReceiverSettleMode getReceivingSettlementMode()
    {
        return _receivingSettlementMode;
    }

    public void setReceivingSettlementMode(ReceiverSettleMode receivingSettlementMode)
    {
        _receivingSettlementMode = receivingSettlementMode;
    }

    public Map getInitialUnsettledMap()
    {
        return _initialUnsettledMap;
    }


    public abstract void flowStateChanged();

    public void setLocalUnsettled(Map unsettled)
    {
        _localUnsettled = unsettled;
    }

    @Override public String toString()
    {
        return "LinkEndpoint{" +
               "_name='" + _name + '\'' +
               ", _session=" + _session +
               ", _state=" + _state +
               ", _role=" + getRole() +
               ", _source=" + _source +
               ", _target=" + _target +
               ", _transferCount=" + _deliveryCount +
               ", _linkCredit=" + _linkCredit +
               ", _available=" + _available +
               ", _drain=" + _drain +
               ", _localHandle=" + _localHandle +
               ", _maxMessageSize=" + _maxMessageSize +
               '}';
    }
}
