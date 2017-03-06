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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.BaseSource;
import org.apache.qpid.server.protocol.v1_0.type.BaseTarget;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.DeliveryState;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
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
    private static final Logger LOGGER = LoggerFactory.getLogger(LinkEndpoint.class);
    private final T _link;
    private Session_1_0 _session;
    private Object _flowTransactionId;
    private SenderSettleMode _sendingSettlementMode;
    private ReceiverSettleMode _receivingSettlementMode;
    private Map _initialUnsettledMap;
    private Map _localUnsettled;
    private UnsignedInteger _lastSentCreditLimit;
    private volatile boolean _stopped;
    private volatile boolean _stoppedUpdated;
    private Symbol[] _capabilities;
    private UnsignedInteger _deliveryCount;
    private UnsignedInteger _linkCredit;
    private UnsignedInteger _available;
    private Boolean _drain;
    private UnsignedInteger _localHandle;
    private UnsignedLong _maxMessageSize;
    private Map<Symbol, Object> _properties;

    protected volatile State _state = State.ATTACH_RECVD;

    protected enum State
    {
        DETACHED,
        ATTACH_SENT,
        ATTACH_RECVD,
        ATTACHED,
        DETACH_SENT,
        DETACH_RECVD
    }


    LinkEndpoint(final T link)
    {
        _link = link;
    }

    public abstract void start();

    public abstract Role getRole();

    public abstract void flowStateChanged();

    public abstract void receiveFlow(final Flow flow);

    protected abstract void handle(final Binary deliveryTag, final DeliveryState state, final Boolean settled);

    protected abstract void remoteDetachedPerformDetach(final Detach detach);

    protected abstract Map<Symbol,Object> initProperties(final Attach attach);

    public void attachReceived(final Attach attach) throws AmqpErrorException
    {
        _sendingSettlementMode = attach.getSndSettleMode();
        _receivingSettlementMode = attach.getRcvSettleMode();
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

    public String getLinkName()
    {
        return _link.getName();
    }

    public BaseSource getSource()
    {
        return _link.getSource();
    }

    public BaseTarget getTarget()
    {
        return _link.getTarget();
    }

    public NamedAddressSpace getAddressSpace()
    {
        return getSession().getConnection().getAddressSpace();
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
                remoteDetachedPerformDetach(detach);
                break;
        }
    }

    public void addUnsettled(final Delivery unsettled)
    {
    }

    public void receiveDeliveryState(final Delivery unsettled,
                                     final DeliveryState state,
                                     final Boolean settled)
    {
        handle(unsettled.getDeliveryTag(), state, settled);

        if (Boolean.TRUE.equals(settled))
        {
            settle(unsettled.getDeliveryTag());
        }
    }

    public void settle(final Binary deliveryTag)
    {

    }

    void setLocalHandle(final UnsignedInteger localHandle)
    {
        _localHandle = localHandle;
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

    public void associateSession(final Session_1_0 session)
    {
        if (session == null)
        {
            throw new IllegalStateException("To dissociate session from Endpoint call LinkEndpoint#dissociateSession() "
                                            + "instead of LinkEndpoint#associate(null)");
        }
        _session = session;
    }

    public void dissociateSession()
    {
        setLocalHandle(null);
        _session = null;
        getLink().discardEndpoint();
    }

    UnsignedInteger getLocalHandle()
    {
        return _localHandle;
    }

    public void attach()
    {
        Attach attachToSend = new Attach();
        attachToSend.setName(getLinkName());
        attachToSend.setRole(getRole());
        attachToSend.setHandle(getLocalHandle());
        attachToSend.setSource(getSource());
        attachToSend.setTarget(getTarget());
        attachToSend.setSndSettleMode(getSendingSettlementMode());
        attachToSend.setRcvSettleMode(getReceivingSettlementMode());
        attachToSend.setUnsettled(_localUnsettled);
        attachToSend.setProperties(_properties);
        attachToSend.setOfferedCapabilities(_capabilities);

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
                throw new UnsupportedOperationException(_state.toString());
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

    public void detach(Error error)
    {
        detach(error, false);
    }

    public void close(Error error)
    {
        detach(error, true);
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

        if (getSession().getSessionState() != SessionState.END_RECVD && !getSession().isEnded())
        {
            Detach detach = new Detach();
            detach.setHandle(getLocalHandle());
            if (close)
            {
                detach.setClosed(close);
            }
            detach.setError(error);

            getSession().sendDetach(detach);
        }

        if (close)
        {
            dissociateSession();
            _link.linkClosed();
        }
        setLocalHandle(null);
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

    public SenderSettleMode getSendingSettlementMode()
    {
        return _sendingSettlementMode;
    }

    public ReceiverSettleMode getReceivingSettlementMode()
    {
        return _receivingSettlementMode;
    }

    public List<Symbol> getCapabilities()
    {
        return _capabilities == null ? null : Collections.unmodifiableList(Arrays.asList(_capabilities));
    }

    public void setCapabilities(Collection<Symbol> capabilities)
    {
        _capabilities = capabilities == null ? null : capabilities.toArray(new Symbol[capabilities.size()]);
    }

    public Map getInitialUnsettledMap()
    {
        return _initialUnsettledMap;
    }

    public void setLocalUnsettled(Map unsettled)
    {
        _localUnsettled = unsettled;
    }

    @Override public String toString()
    {
        return "LinkEndpoint{" +
               "_name='" + getLinkName() + '\'' +
               ", _session=" + _session +
               ", _state=" + _state +
               ", _role=" + getRole() +
               ", _source=" + getSource() +
               ", _target=" + getTarget() +
               ", _transferCount=" + _deliveryCount +
               ", _linkCredit=" + _linkCredit +
               ", _available=" + _available +
               ", _drain=" + _drain +
               ", _localHandle=" + _localHandle +
               ", _maxMessageSize=" + _maxMessageSize +
               '}';
    }
}
