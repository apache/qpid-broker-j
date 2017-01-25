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

import static org.apache.qpid.server.logging.subjects.LogSubjectFormat.CHANNEL_FORMAT;
import static org.apache.qpid.server.protocol.v1_0.ExchangeDestination.SHARED_CAPABILITY;

import java.security.AccessControlContext;
import java.security.AccessControlException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.security.auth.Subject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.bytebuffer.QpidByteBuffer;
import org.apache.qpid.common.AMQPFilterTypes;
import org.apache.qpid.exchange.ExchangeDefaults;
import org.apache.qpid.filter.SelectorParsingException;
import org.apache.qpid.filter.selector.ParseException;
import org.apache.qpid.filter.selector.TokenMgrError;
import org.apache.qpid.server.logging.LogMessage;
import org.apache.qpid.server.logging.LogSubject;
import org.apache.qpid.server.logging.messages.ChannelMessages;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.message.MessageInstanceConsumer;
import org.apache.qpid.server.message.MessageSource;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.Binding;
import org.apache.qpid.server.model.Consumer;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.ExclusivityPolicy;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.NotFoundException;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.Session;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.protocol.CapacityChecker;
import org.apache.qpid.server.protocol.LinkRegistry;
import org.apache.qpid.server.protocol.v1_0.codec.QpidByteBufferUtils;
import org.apache.qpid.server.protocol.v1_0.framing.OversizeFrameException;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.DeliveryState;
import org.apache.qpid.server.protocol.v1_0.type.ErrorCondition;
import org.apache.qpid.server.protocol.v1_0.type.FrameBody;
import org.apache.qpid.server.protocol.v1_0.type.LifetimePolicy;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.messaging.DeleteOnClose;
import org.apache.qpid.server.protocol.v1_0.type.messaging.DeleteOnNoLinks;
import org.apache.qpid.server.protocol.v1_0.type.messaging.DeleteOnNoLinksOrMessages;
import org.apache.qpid.server.protocol.v1_0.type.messaging.DeleteOnNoMessages;
import org.apache.qpid.server.protocol.v1_0.type.messaging.ExactSubjectFilter;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Filter;
import org.apache.qpid.server.protocol.v1_0.type.messaging.JMSSelectorFilter;
import org.apache.qpid.server.protocol.v1_0.type.messaging.MatchingSubjectFilter;
import org.apache.qpid.server.protocol.v1_0.type.messaging.NoLocalFilter;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
import org.apache.qpid.server.protocol.v1_0.type.messaging.StdDistMode;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Target;
import org.apache.qpid.server.protocol.v1_0.type.messaging.TerminusDurability;
import org.apache.qpid.server.protocol.v1_0.type.messaging.TerminusExpiryPolicy;
import org.apache.qpid.server.protocol.v1_0.type.transaction.Coordinator;
import org.apache.qpid.server.protocol.v1_0.type.transaction.TxnCapability;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.ConnectionError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Disposition;
import org.apache.qpid.server.protocol.v1_0.type.transport.End;
import org.apache.qpid.server.protocol.v1_0.type.transport.Error;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.LinkError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.server.protocol.v1_0.type.transport.Transfer;
import org.apache.qpid.server.security.SecurityToken;
import org.apache.qpid.server.session.AbstractAMQPSession;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.txn.AutoCommitTransaction;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.transport.network.Ticker;

public class Session_1_0 extends AbstractAMQPSession<Session_1_0, ConsumerTarget_1_0>
        implements LogSubject, org.apache.qpid.server.util.Deletable<Session_1_0>
{
    public static final Symbol DELAYED_DELIVERY = Symbol.valueOf("DELAYED_DELIVERY");
    private static final Logger _logger = LoggerFactory.getLogger(Session_1_0.class);
    private static final Symbol LIFETIME_POLICY = Symbol.valueOf("lifetime-policy");
    private static final EnumSet<SessionState> END_STATES =
            EnumSet.of(SessionState.END_RECVD, SessionState.END_PIPE, SessionState.END_SENT, SessionState.ENDED);
    private AutoCommitTransaction _transaction;

    private final LinkedHashMap<Integer, ServerTransaction> _openTransactions =
            new LinkedHashMap<Integer, ServerTransaction>();

    private final AMQPConnection_1_0 _connection;
    private AtomicBoolean _closed = new AtomicBoolean();

    private final CopyOnWriteArrayList<Consumer<?, ConsumerTarget_1_0>> _consumers = new CopyOnWriteArrayList<>();

    private Session<?> _modelObject = this;

    private SessionState _sessionState;

    private final Map<String, SendingLinkEndpoint> _sendingLinkMap = new HashMap<>();
    private final Map<String, ReceivingLinkEndpoint> _receivingLinkMap = new HashMap<>();
    private final Map<LinkEndpoint, UnsignedInteger> _localLinkEndpoints = new HashMap<>();
    private final Map<UnsignedInteger, LinkEndpoint> _remoteLinkEndpoints = new HashMap<>();
    private long _lastAttachedTime;

    private short _receivingChannel;
    private final short _sendingChannel;

    private final CapacityCheckAction _capacityCheckAction = new CapacityCheckAction();


    // has to be a power of two
    private static final int DEFAULT_SESSION_BUFFER_SIZE = 1 << 11;
    private static final int BUFFER_SIZE_MASK = DEFAULT_SESSION_BUFFER_SIZE - 1;



    private int _nextOutgoingDeliveryId;

    private UnsignedInteger _outgoingSessionCredit;
    private UnsignedInteger _initialOutgoingId = UnsignedInteger.valueOf(0);
    private SequenceNumber _nextIncomingTransferId;
    private SequenceNumber _nextOutgoingTransferId = new SequenceNumber(_initialOutgoingId.intValue());

    private LinkedHashMap<UnsignedInteger,Delivery> _outgoingUnsettled = new LinkedHashMap<>(DEFAULT_SESSION_BUFFER_SIZE);
    private LinkedHashMap<UnsignedInteger,Delivery> _incomingUnsettled = new LinkedHashMap<>(DEFAULT_SESSION_BUFFER_SIZE);

    private int _availableIncomingCredit = DEFAULT_SESSION_BUFFER_SIZE;
    private int _availableOutgoingCredit = DEFAULT_SESSION_BUFFER_SIZE;
    private UnsignedInteger _lastSentIncomingLimit;

    private final Error _sessionEndedLinkError =
            new Error(LinkError.DETACH_FORCED,
                      "Force detach the link because the session is remotely ended.");

    private final String _primaryDomain;
    private final Set<Object> _blockingEntities = Collections.newSetFromMap(new ConcurrentHashMap<Object,Boolean>());
    private volatile long _startedTransactions;
    private volatile long _committedTransactions;
    private volatile long _rolledBackTransactions;
    private volatile int _unacknowledgedMessages;

    public Session_1_0(final AMQPConnection_1_0 connection, Begin begin, short sendingChannelId)
    {
        super(connection, sendingChannelId);
        _sendingChannel = sendingChannelId;
        _sessionState = SessionState.BEGIN_RECVD;
        _nextIncomingTransferId = new SequenceNumber(begin.getNextOutgoingId().intValue());
        _connection = connection;
        _primaryDomain = getPrimaryDomain();
    }

    public void setReceivingChannel(final short receivingChannel)
    {
        _receivingChannel = receivingChannel;
        switch(_sessionState)
        {
            case INACTIVE:
                _sessionState = SessionState.BEGIN_RECVD;
                break;
            case BEGIN_SENT:
                _sessionState = SessionState.ACTIVE;
                break;
            case END_PIPE:
                _sessionState = SessionState.END_SENT;
                break;
            default:
                // TODO error

        }
    }

    public void sendDetach(final Detach detach)
    {
        send(detach);
    }

    public void receiveAttach(final Attach attach)
    {
        if(_sessionState == SessionState.ACTIVE)
        {
            UnsignedInteger handle = attach.getHandle();
            if(_remoteLinkEndpoints.containsKey(handle))
            {
                // TODO - Error - handle busy?
            }
            else
            {
                Map<String, ? extends LinkEndpoint> linkMap =
                        attach.getRole() == Role.RECEIVER ? _sendingLinkMap : _receivingLinkMap;
                LinkEndpoint endpoint = linkMap.get(attach.getName());
                if(endpoint == null)
                {
                    endpoint = attach.getRole() == Role.RECEIVER
                               ? new SendingLinkEndpoint(this, attach)
                               : new ReceivingLinkEndpoint(this, attach);

                    if(_blockingEntities.contains(this) && attach.getRole() == Role.SENDER)
                    {
                        endpoint.setStopped(true);
                    }

                    // TODO : fix below - distinguish between local and remote owned
                    endpoint.setSource(attach.getSource());
                    endpoint.setTarget(attach.getTarget());
                    ((Map<String,LinkEndpoint>)linkMap).put(attach.getName(), endpoint);
                }
                else
                {
                    endpoint.receiveAttach(attach);
                }

                if(attach.getRole() == Role.SENDER)
                {
                    endpoint.setDeliveryCount(attach.getInitialDeliveryCount());
                }

                _remoteLinkEndpoints.put(handle, endpoint);

                if(!_localLinkEndpoints.containsKey(endpoint))
                {
                    UnsignedInteger localHandle = findNextAvailableHandle();
                    endpoint.setLocalHandle(localHandle);
                    _localLinkEndpoints.put(endpoint, localHandle);

                    remoteLinkCreation(endpoint, attach);
                }
                else
                {
                    // TODO - error already attached
                }
            }
        }
    }

    public void updateDisposition(final Role role,
                                  final UnsignedInteger first,
                                  final UnsignedInteger last,
                                  final DeliveryState state, final boolean settled)
    {


        Disposition disposition = new Disposition();
        disposition.setRole(role);
        disposition.setFirst(first);
        disposition.setLast(last);
        disposition.setSettled(settled);

        disposition.setState(state);

        if (settled)
        {
            final LinkedHashMap<UnsignedInteger, Delivery> unsettled =
                    role == Role.RECEIVER ? _incomingUnsettled : _outgoingUnsettled;
            SequenceNumber pos = new SequenceNumber(first.intValue());
            SequenceNumber end = new SequenceNumber(last.intValue());
            while (pos.compareTo(end) <= 0)
            {
                unsettled.remove(new UnsignedInteger(pos.intValue()));
                pos.incr();
            }
        }

        send(disposition);
        //TODO - check send flow
    }

    public boolean hasCreditToSend()
    {
        boolean b = _outgoingSessionCredit != null && _outgoingSessionCredit.intValue() > 0;
        boolean b1 = getOutgoingWindowSize() != null && getOutgoingWindowSize().compareTo(UnsignedInteger.ZERO) > 0;
        return b && b1;
    }

    public void end()
    {
        end(new End());
    }

    public void sendTransfer(final Transfer xfr, final SendingLinkEndpoint endpoint, final boolean newDelivery)
    {
        _nextOutgoingTransferId.incr();
        UnsignedInteger deliveryId;
        final boolean settled = Boolean.TRUE.equals(xfr.getSettled());
        if (newDelivery)
        {
            deliveryId = UnsignedInteger.valueOf(_nextOutgoingDeliveryId++);
            endpoint.setLastDeliveryId(deliveryId);
            if (!settled)
            {
                final Delivery delivery = new Delivery(xfr, endpoint);
                _outgoingUnsettled.put(deliveryId, delivery);
                _outgoingSessionCredit = _outgoingSessionCredit.subtract(UnsignedInteger.ONE);
                endpoint.addUnsettled(delivery);
            }
        }
        else
        {
            deliveryId = endpoint.getLastDeliveryId();
            final Delivery delivery = _outgoingUnsettled.get(deliveryId);
            if (delivery != null)
            {
                if (!settled)
                {
                    delivery.addTransfer(xfr);
                    _outgoingSessionCredit = _outgoingSessionCredit.subtract(UnsignedInteger.ONE);
                }
                else
                {
                    _outgoingSessionCredit = _outgoingSessionCredit.add(new UnsignedInteger(delivery.getNumberOfTransfers()));
                    endpoint.settle(delivery.getDeliveryTag());
                    _outgoingUnsettled.remove(deliveryId);
                }
            }
        }
        xfr.setDeliveryId(deliveryId);

        try
        {
            List<QpidByteBuffer> payload = xfr.getPayload();
            final long remaining = QpidByteBufferUtils.remaining(payload);
            int payloadSent = _connection.sendFrame(_sendingChannel, xfr, payload);

            if(payload != null && payloadSent < remaining && payloadSent >= 0)
            {
                // TODO - should make this iterative and not recursive

                Transfer secondTransfer = new Transfer();

                secondTransfer.setDeliveryTag(xfr.getDeliveryTag());
                secondTransfer.setHandle(xfr.getHandle());
                secondTransfer.setSettled(xfr.getSettled());
                secondTransfer.setState(xfr.getState());
                secondTransfer.setMessageFormat(xfr.getMessageFormat());
                secondTransfer.setPayload(payload);

                sendTransfer(secondTransfer, endpoint, false);

                secondTransfer.dispose();
            }

            if (payload != null)
            {
                for (QpidByteBuffer buf : payload)
                {
                    buf.dispose();
                }
            }
        }
        catch (OversizeFrameException e)
        {
            throw new ConnectionScopedRuntimeException(e);
        }
    }

    public boolean isActive()
    {
        return _sessionState == SessionState.ACTIVE;
    }

    public void receiveEnd(final End end)
    {
        switch (_sessionState)
        {
            case END_SENT:
                _sessionState = SessionState.ENDED;
                break;
            case ACTIVE:
                detachLinks();
                remoteEnd(end);
                short sendChannel = _sendingChannel;
                _connection.sendEnd(sendChannel, new End(), true);
                _sessionState = SessionState.ENDED;
                break;
            default:
                sendChannel = _sendingChannel;
                End reply = new End();
                Error error = new Error();
                error.setCondition(AmqpError.ILLEGAL_STATE);
                error.setDescription("END called on Session which has not been opened");
                reply.setError(error);
                _connection.sendEnd(sendChannel, reply, true);
                break;
        }
    }

    public UnsignedInteger getNextOutgoingId()
    {
        return UnsignedInteger.valueOf(_nextOutgoingTransferId.intValue());
    }

    public void sendFlowConditional()
    {
        if(_nextIncomingTransferId != null)
        {
            UnsignedInteger clientsCredit =
                    _lastSentIncomingLimit.subtract(UnsignedInteger.valueOf(_nextIncomingTransferId.intValue()));
            int i = UnsignedInteger.valueOf(_availableIncomingCredit).subtract(clientsCredit).compareTo(clientsCredit);
            if (i >= 0)
            {
                sendFlow();
            }
        }

    }

    public UnsignedInteger getOutgoingWindowSize()
    {
        return UnsignedInteger.valueOf(_availableOutgoingCredit);
    }

    public void receiveFlow(final Flow flow)
    {
        UnsignedInteger handle = flow.getHandle();
        final LinkEndpoint endpoint = handle == null ? null : _remoteLinkEndpoints.get(handle);

        final UnsignedInteger nextOutgoingId =
                flow.getNextIncomingId() == null ? _initialOutgoingId : flow.getNextIncomingId();
        int limit = (nextOutgoingId.intValue() + flow.getIncomingWindow().intValue());
        _outgoingSessionCredit = UnsignedInteger.valueOf(limit - _nextOutgoingTransferId.intValue());

        if (endpoint != null)
        {
            endpoint.receiveFlow(flow);
        }
        else
        {
            final Collection<LinkEndpoint> allLinkEndpoints = _remoteLinkEndpoints.values();
            for (LinkEndpoint le : allLinkEndpoints)
            {
                le.flowStateChanged();
            }
        }
    }

    public void setNextIncomingId(final UnsignedInteger nextIncomingId)
    {
        _nextIncomingTransferId = new SequenceNumber(nextIncomingId.intValue());

    }

    public void receiveDisposition(final Disposition disposition)
    {
        Role dispositionRole = disposition.getRole();

        LinkedHashMap<UnsignedInteger, Delivery> unsettledTransfers;

        if(dispositionRole == Role.RECEIVER)
        {
            unsettledTransfers = _outgoingUnsettled;
        }
        else
        {
            unsettledTransfers = _incomingUnsettled;

        }

        UnsignedInteger deliveryId = disposition.getFirst();
        UnsignedInteger last = disposition.getLast();
        if(last == null)
        {
            last = deliveryId;
        }


        while(deliveryId.compareTo(last)<=0)
        {

            Delivery delivery = unsettledTransfers.get(deliveryId);
            if(delivery != null)
            {
                delivery.getLinkEndpoint().receiveDeliveryState(delivery,
                                                                disposition.getState(),
                                                                disposition.getSettled());
                if (Boolean.TRUE.equals(disposition.getSettled()))
                {
                    unsettledTransfers.remove(deliveryId);
                }
            }
            deliveryId = deliveryId.add(UnsignedInteger.ONE);
        }
        if(Boolean.TRUE.equals(disposition.getSettled()))
        {
            //TODO - check send flow
        }

    }

    public SessionState getSessionState()
    {
        return _sessionState;
    }

    public void sendFlow()
    {
        sendFlow(new Flow());
    }

    public void setSendingChannel(final short sendingChannel)
    {
        switch(_sessionState)
        {
            case INACTIVE:
                _sessionState = SessionState.BEGIN_SENT;
                break;
            case BEGIN_RECVD:
                _sessionState = SessionState.ACTIVE;
                break;
            default:
                // TODO error

        }

        AccessController.doPrivileged((new PrivilegedAction<Object>()
        {
            @Override
            public Object run()
            {
                _connection.getEventLogger().message(ChannelMessages.CREATE());

                return null;
            }
        }), _accessControllerContext);
    }

    public void sendFlow(final Flow flow)
    {
        if(_nextIncomingTransferId != null)
        {
            final int nextIncomingId = _nextIncomingTransferId.intValue();
            flow.setNextIncomingId(UnsignedInteger.valueOf(nextIncomingId));
            _lastSentIncomingLimit = UnsignedInteger.valueOf(nextIncomingId + _availableIncomingCredit);
        }
        flow.setIncomingWindow(UnsignedInteger.valueOf(_availableIncomingCredit));

        flow.setNextOutgoingId(UnsignedInteger.valueOf(_nextOutgoingTransferId.intValue()));
        flow.setOutgoingWindow(UnsignedInteger.valueOf(_availableOutgoingCredit));
        send(flow);
    }

    public void setOutgoingSessionCredit(final UnsignedInteger outgoingSessionCredit)
    {
        _outgoingSessionCredit = outgoingSessionCredit;
    }

    public void receiveDetach(final Detach detach)
    {
        UnsignedInteger handle = detach.getHandle();
        detach(handle, detach);
    }

    public void sendAttach(final Attach attach)
    {
        send(attach);
    }

    private void send(final FrameBody frameBody)
    {
        _connection.sendFrame(_sendingChannel, frameBody);
    }

    public boolean isSyntheticError(final Error error)
    {
        return error == _sessionEndedLinkError;
    }

    public void end(final End end)
    {
        switch (_sessionState)
        {
            case BEGIN_SENT:
                _connection.sendEnd(_sendingChannel, end, false);
                _sessionState = SessionState.END_PIPE;
                break;
            case ACTIVE:
                detachLinks();
                short sendChannel = _sendingChannel;
                _connection.sendEnd(sendChannel, end, true);
                _sessionState = SessionState.END_SENT;
                break;
            default:
                sendChannel = _sendingChannel;
                End reply = new End();
                Error error = new Error();
                error.setCondition(AmqpError.ILLEGAL_STATE);
                error.setDescription("END called on Session which has not been opened");
                reply.setError(error);
                _connection.sendEnd(sendChannel, reply, true);
                break;


        }
    }

    public void receiveTransfer(final Transfer transfer)
    {
        _nextIncomingTransferId.incr();

        UnsignedInteger handle = transfer.getHandle();


        LinkEndpoint linkEndpoint = _remoteLinkEndpoints.get(handle);

        if (linkEndpoint == null)
        {
            Error error = new Error();
            error.setCondition(AmqpError.ILLEGAL_STATE);
            error.setDescription("TRANSFER called on Session for link handle " + handle + " which is not attached");
            _connection.close(error);

        }
        else if(!(linkEndpoint instanceof ReceivingLinkEndpoint))
        {

            Error error = new Error();
            error.setCondition(ConnectionError.FRAMING_ERROR);
            error.setDescription("TRANSFER called on Session for link handle " + handle + " which is a sending ink not a receiving link");
            _connection.close(error);

        }
        else
        {
            ReceivingLinkEndpoint endpoint = ((ReceivingLinkEndpoint) linkEndpoint);

            UnsignedInteger deliveryId = transfer.getDeliveryId();
            if (deliveryId == null)
            {
                deliveryId = endpoint.getLastDeliveryId();
            }

            Delivery delivery = _incomingUnsettled.get(deliveryId);
            if (delivery == null)
            {
                delivery = new Delivery(transfer, endpoint);
                _incomingUnsettled.put(deliveryId, delivery);

                if (Boolean.TRUE.equals(transfer.getMore()))
                {
                    endpoint.setLastDeliveryId(transfer.getDeliveryId());
                }
            }
            else
            {
                if (delivery.getDeliveryId().equals(deliveryId))
                {
                    delivery.addTransfer(transfer);

                    if (!Boolean.TRUE.equals(transfer.getMore()))
                    {
                        endpoint.setLastDeliveryId(null);
                    }
                }
                else
                {
                    End reply = new End();

                    Error error = new Error();
                    error.setCondition(AmqpError.ILLEGAL_STATE);
                    error.setDescription("TRANSFER called on Session for link handle "
                                         + handle
                                         + " with incorrect delivery id "
                                         + transfer.getDeliveryId());
                    reply.setError(error);
                    _connection.sendEnd(_sendingChannel, reply, true);

                    return;

                }
            }

            Error error = endpoint.receiveTransfer(transfer, delivery);
            if(error != null)
            {
                endpoint.close(error);
            }
            if ((delivery.isComplete() && delivery.isSettled() || Boolean.TRUE.equals(transfer.getAborted())))
            {
                _incomingUnsettled.remove(deliveryId);
            }
        }
    }

    private Collection<LinkEndpoint> getLocalLinkEndpoints()
    {
        return new ArrayList<>(_localLinkEndpoints.keySet());
    }

    boolean isEnded()
    {
        return _sessionState == SessionState.ENDED || _connection.isClosed();
    }

    UnsignedInteger getIncomingWindowSize()
    {
        return UnsignedInteger.valueOf(_availableIncomingCredit);
    }

    AccessControlContext getAccessControllerContext()
    {
        return _accessControllerContext;
    }

    public void remoteLinkCreation(final LinkEndpoint endpoint, Attach attach)
    {
        Link_1_0 link = null;
        Error error = null;
        Set<Symbol> capabilities = new HashSet<>();
        try
        {
            if (endpoint.getRole() == Role.SENDER)
            {
                link = createSendingLink(endpoint, attach);
                if (link != null)
                {
                    capabilities.add(AMQPConnection_1_0Impl.SHARED_SUBSCRIPTIONS);
                }
            }
            else if (endpoint.getTarget() instanceof Coordinator)
            {
                link = createCoordinatorLink(endpoint);
            }
            else // standard  (non-Coordinator) receiver
            {
                link = createReceivingLink(endpoint, capabilities);
            }
        }
        catch (AmqpErrorException e)
        {

            if (e.getError() == null || e.getError().getCondition() == AmqpError.INTERNAL_ERROR)
            {
                _logger.error("Could not create link", e);
            }
            else
            {
                _logger.debug("Could not create link", e);
            }
            if (endpoint.getRole() == Role.SENDER)
            {
                endpoint.setSource(null);
            }
            else
            {
                endpoint.setTarget(null);
            }
            error = e.getError();
        }

        endpoint.setCapabilities(capabilities);
        endpoint.attach();

        if (link == null)
        {
            if (error == null)
            {
                error = new Error();
                error.setCondition(AmqpError.NOT_FOUND);
            }
            endpoint.close(error);
        }
        else
        {
            link.start();
        }
    }

    private Link_1_0 createReceivingLink(final LinkEndpoint endpoint,
                                         final Set<Symbol> capabilities)
    {
        Link_1_0 link = null;
        Destination destination;
        final LinkRegistry linkRegistry = getAddressSpace().getLinkRegistry(getConnection().getRemoteContainerId());
        StandardReceivingLink_1_0 previousLink =
                (StandardReceivingLink_1_0) linkRegistry.getDurableReceivingLink(endpoint.getName());

        if (previousLink == null)
        {

            Target target = (Target) endpoint.getTarget();

            if (target != null)
            {
                if (Boolean.TRUE.equals(target.getDynamic()))
                {

                    MessageDestination tempQueue = createDynamicDestination(target.getDynamicNodeProperties());
                    target.setAddress(tempQueue.getName());
                }

                String addr = target.getAddress();
                if (addr == null || "".equals(addr.trim()))
                {
                    MessageDestination messageDestination = getAddressSpace().getDefaultDestination();
                    destination = new NodeReceivingDestination(messageDestination, target.getDurable(),
                                                               target.getExpiryPolicy(), "",
                                                               target.getCapabilities(),
                                                               _connection.getEventLogger());
                    target.setCapabilities(destination.getCapabilities());

                    if (_blockingEntities.contains(messageDestination))
                    {
                        endpoint.setStopped(true);
                    }
                }
                else if (!addr.startsWith("/") && addr.contains("/"))
                {
                    String[] parts = addr.split("/", 2);
                    Exchange<?> exchange = getExchange(parts[0]);
                    if (exchange != null)
                    {
                        Symbol[] capabilities1 = target.getCapabilities();
                        ExchangeDestination exchangeDestination = new ExchangeDestination(exchange,
                                                                                          null,
                                                                                          target.getDurable(),
                                                                                          target.getExpiryPolicy(),
                                                                                          parts[0],
                                                                                          parts[1],
                                                                                          capabilities1 != null ? Arrays.asList(capabilities1) : Collections.<Symbol>emptyList());
                        target.setCapabilities(exchangeDestination.getCapabilities());
                        destination = exchangeDestination;
                    }
                    else
                    {
                        endpoint.setTarget(null);
                        destination = null;
                    }
                }
                else
                {
                    MessageDestination messageDestination =
                            getAddressSpace().getAttainedMessageDestination(addr);
                    if (messageDestination != null)
                    {
                        destination =
                                new NodeReceivingDestination(messageDestination,
                                                             target.getDurable(),
                                                             target.getExpiryPolicy(),
                                                             addr,
                                                             target.getCapabilities(),
                                                             _connection.getEventLogger());
                        target.setCapabilities(destination.getCapabilities());
                    }
                    else
                    {
                        Queue<?> queue = getQueue(addr);
                        if (queue != null)
                        {

                            destination = new QueueDestination(queue, addr);
                        }
                        else
                        {
                            endpoint.setTarget(null);
                            destination = null;
                        }
                    }
                }
            }
            else
            {
                destination = null;
            }
            if (destination != null)
            {
                final ReceivingDestination receivingDestination = (ReceivingDestination) destination;
                MessageDestination messageDestination = receivingDestination.getMessageDestination();
                if(!(messageDestination instanceof Queue) || ((Queue<?>)messageDestination).isHoldOnPublishEnabled())
                {
                    capabilities.add(DELAYED_DELIVERY);
                }
                final ReceivingLinkEndpoint receivingLinkEndpoint = (ReceivingLinkEndpoint) endpoint;
                final StandardReceivingLink_1_0 receivingLink =
                        new StandardReceivingLink_1_0(new ReceivingLinkAttachment(this, receivingLinkEndpoint),
                                                      getAddressSpace(),
                                                      receivingDestination);

                receivingLinkEndpoint.setLink(receivingLink);
                link = receivingLink;
                if (TerminusDurability.UNSETTLED_STATE.equals(target.getDurable())
                    || TerminusDurability.CONFIGURATION.equals(target.getDurable()))
                {
                    linkRegistry.registerReceivingLink(endpoint.getName(), receivingLink);
                }
            }
        }
        else
        {
            ReceivingLinkEndpoint receivingLinkEndpoint = (ReceivingLinkEndpoint) endpoint;
            previousLink.setLinkAttachment(new ReceivingLinkAttachment(this, receivingLinkEndpoint));
            receivingLinkEndpoint.setLink(previousLink);
            link = previousLink;
            endpoint.setLocalUnsettled(previousLink.getUnsettledOutcomeMap());
        }
        return link;
    }

    private TxnCoordinatorReceivingLink_1_0 createCoordinatorLink(final LinkEndpoint endpoint) throws AmqpErrorException
    {
        Coordinator coordinator = (Coordinator) endpoint.getTarget();
        TxnCapability[] coordinatorCapabilities = coordinator.getCapabilities();
        boolean localTxn = false;
        boolean multiplePerSession = false;
        if (coordinatorCapabilities != null)
        {
            for (TxnCapability capability : coordinatorCapabilities)
            {
                if (capability.equals(TxnCapability.LOCAL_TXN))
                {
                    localTxn = true;
                }
                else if (capability.equals(TxnCapability.MULTI_TXNS_PER_SSN))
                {
                    multiplePerSession = true;
                }
                else
                {
                    Error error = new Error();
                    error.setCondition(AmqpError.NOT_IMPLEMENTED);
                    error.setDescription("Unsupported capability: " + capability);
                    throw new AmqpErrorException(error);
                }
            }
        }

        final ReceivingLinkEndpoint receivingLinkEndpoint = (ReceivingLinkEndpoint) endpoint;
        final TxnCoordinatorReceivingLink_1_0 coordinatorLink =
                new TxnCoordinatorReceivingLink_1_0(getAddressSpace(),
                                                    this,
                                                    receivingLinkEndpoint,
                                                    _openTransactions);
        receivingLinkEndpoint.setLink(coordinatorLink);
        return coordinatorLink;
    }

    private SendingLink_1_0 createSendingLink(final LinkEndpoint endpoint, Attach attach) throws AmqpErrorException
    {
        final SendingLinkEndpoint sendingLinkEndpoint = (SendingLinkEndpoint) endpoint;
        SendingLink_1_0 link = null;
        final LinkRegistry linkRegistry = getAddressSpace().getLinkRegistry(getConnection().getRemoteContainerId());
        final SendingLink_1_0 previousLink = (SendingLink_1_0) linkRegistry.getDurableSendingLink(endpoint.getName());

        if (previousLink == null)
        {
            Source source = (Source) sendingLinkEndpoint.getSource();
            SendingDestination destination = null;
            if (source != null)
            {
                destination = getSendingDestination(sendingLinkEndpoint.getName(), source);
                if (destination == null)
                {
                    sendingLinkEndpoint.setSource(null);
                }
                else
                {
                    source.setCapabilities(destination.getCapabilities());
                }
            }
            else
            {
                final Symbol[] linkCapabilities = attach.getDesiredCapabilities();
                boolean isGlobal = hasCapability(linkCapabilities, ExchangeDestination.GLOBAL_CAPABILITY);
                final String queueName = getMangledSubscriptionName(endpoint.getName(), true, true, isGlobal);
                final MessageSource messageSource = getAddressSpace().getAttainedMessageSource(queueName);
                // TODO START The Source should be persisted on the LinkEndpoint
                if (messageSource instanceof Queue)
                {
                    Queue<?> queue = (Queue<?>) messageSource;
                    source = new Source();
                    List<Symbol> capabilities = new ArrayList<>();
                    if (queue.getExclusive() == ExclusivityPolicy.SHARED_SUBSCRIPTION)
                    {
                        capabilities.add(ExchangeDestination.SHARED_CAPABILITY);
                    }
                    if (isGlobal)
                    {
                        capabilities.add(ExchangeDestination.GLOBAL_CAPABILITY);
                    }
                    capabilities.add(ExchangeDestination.TOPIC_CAPABILITY);
                    source.setCapabilities(capabilities.toArray(new Symbol[capabilities.size()]));

                    final Collection<Exchange> exchanges = queue.getVirtualHost().getChildren(Exchange.class);
                    String bindingKey = null;
                    Exchange<?> foundExchange = null;
                    for (Exchange<?> exchange : exchanges)
                    {
                        for (Binding binding : exchange.getPublishingLinks(queue))
                        {
                            String exchangeName = exchange.getName();
                            bindingKey = binding.getName();
                            final Map<String, Object> bindingArguments = binding.getArguments();
                            Map<Symbol, Filter> filter = new HashMap<>();
                            if (bindingArguments.containsKey(AMQPFilterTypes.JMS_SELECTOR))
                            {
                                filter.put(Symbol.getSymbol("jms-selector"), new JMSSelectorFilter((String) bindingArguments.get(AMQPFilterTypes.JMS_SELECTOR)));
                            }
                            if (bindingArguments.containsKey(AMQPFilterTypes.NO_LOCAL))
                            {
                                filter.put(Symbol.getSymbol("no-local"), NoLocalFilter.INSTANCE);
                            }
                            foundExchange = exchange;
                            source.setAddress(exchangeName + "/" + bindingKey);
                            source.setFilter(filter);
                            break;
                        }
                        if (foundExchange != null)
                        {
                            break;
                        }
                    }
                    if (foundExchange != null)
                    {
                        source.setDurable(TerminusDurability.CONFIGURATION);
                        TerminusExpiryPolicy terminusExpiryPolicy;
                        switch (queue.getLifetimePolicy())
                        {
                            case PERMANENT:
                                terminusExpiryPolicy = TerminusExpiryPolicy.NEVER;
                                break;
                            case DELETE_ON_NO_LINKS:
                            case DELETE_ON_NO_OUTBOUND_LINKS:
                                terminusExpiryPolicy = TerminusExpiryPolicy.LINK_DETACH;
                                break;
                            case DELETE_ON_CONNECTION_CLOSE:
                                terminusExpiryPolicy = TerminusExpiryPolicy.CONNECTION_CLOSE;
                                break;
                            case DELETE_ON_SESSION_END:
                                terminusExpiryPolicy = TerminusExpiryPolicy.SESSION_END;
                                break;
                            default:
                                throw new AmqpErrorException(new Error(AmqpError.NOT_IMPLEMENTED, "unexpected liftetime policy " + queue.getLifetimePolicy()));
                        }
                        sendingLinkEndpoint.setSource(source);
                        destination = new ExchangeDestination(foundExchange,
                                                              queue,
                                                              TerminusDurability.CONFIGURATION,
                                                              terminusExpiryPolicy,
                                                              foundExchange.getName(),
                                                              bindingKey,
                                                              capabilities);
                    }

                }
                // TODO END
            }

            if (destination != null)
            {
                final SendingLink_1_0 sendingLink =
                        new SendingLink_1_0(new SendingLinkAttachment(this, sendingLinkEndpoint),
                                            getAddressSpace(),
                                            destination);
                sendingLink.createConsumerTarget();

                sendingLinkEndpoint.setLink(sendingLink);
                registerConsumer(sendingLink);

                if (destination instanceof ExchangeDestination)
                {
                    ExchangeDestination exchangeDestination = (ExchangeDestination) destination;
                    exchangeDestination.getQueue().setAttributes(Collections.<String, Object>singletonMap(Queue.DESIRED_STATE, State.ACTIVE));
                }

                link = sendingLink;
                if (TerminusDurability.UNSETTLED_STATE.equals(source.getDurable())
                    || TerminusDurability.CONFIGURATION.equals(source.getDurable()))
                {
                    linkRegistry.registerSendingLink(endpoint.getName(), sendingLink);
                }

            }
        }
        else
        {
            Source newSource = (Source) attach.getSource();
            Source oldSource = (Source) previousLink.getEndpoint().getSource();

            if (previousLink.getDestination() instanceof ExchangeDestination && newSource != null && !Boolean.TRUE.equals(newSource.getDynamic()))
            {
                final SendingDestination newDestination = getSendingDestination(previousLink.getEndpoint().getName(), newSource);
                if (updateSourceForSubscription(previousLink, newSource, newDestination))
                {
                    previousLink.setDestination(newDestination);
                }
            }

            sendingLinkEndpoint.setSource(oldSource);
            previousLink.setLinkAttachment(new SendingLinkAttachment(this, sendingLinkEndpoint));
            sendingLinkEndpoint.setLink(previousLink);
            link = previousLink;
            sendingLinkEndpoint.setLocalUnsettled(previousLink.getUnsettledOutcomeMap());
            registerConsumer(previousLink);

        }
        return link;
    }

    private boolean updateSourceForSubscription(final SendingLink_1_0 previousLink,
                                                final Source newSource,
                                                final SendingDestination newDestination)
    {
        SendingDestination oldDestination = previousLink.getDestination();
        if (oldDestination instanceof ExchangeDestination)
        {
            ExchangeDestination oldExchangeDestination = (ExchangeDestination) oldDestination;
            String newAddress = newSource.getAddress();
            if (newDestination instanceof ExchangeDestination)
            {
                ExchangeDestination newExchangeDestination = (ExchangeDestination) newDestination;
                if (oldExchangeDestination.getQueue() != newExchangeDestination.getQueue())
                {
                    Source oldSource = (Source) previousLink.getEndpoint().getSource();
                    oldSource.setAddress(newAddress);
                    oldSource.setFilter(newSource.getFilter());
                    return true;
                }
            }
        }
        return false;
    }

    private SendingDestination getSendingDestination(final String linkName, final Source source) throws AmqpErrorException
    {
        SendingDestination destination = null;

        if (Boolean.TRUE.equals(source.getDynamic()))
        {
            MessageSource tempQueue = createDynamicSource(source.getDynamicNodeProperties());
            source.setAddress(tempQueue.getName()); // todo : temporary topic
        }

        String address = source.getAddress();
        if (address != null)
        {
            if (!address.startsWith("/") && address.contains("/"))
            {
                destination = createExchangeDestination(address, linkName, source);
            }
            else
            {
                MessageSource queue = getAddressSpace().getAttainedMessageSource(address);
                if (queue != null)
                {
                    destination = new MessageSourceDestination(queue);
                }
                else
                {
                    destination = createExchangeDestination(address, null, linkName, source);
                }
            }
        }

        return destination;
    }

    private ExchangeDestination createExchangeDestination(String address, final String linkName, final Source source)
            throws AmqpErrorException
    {
        String[] parts = address.split("/", 2);
        String exchangeName = parts[0];
        String bindingKey = parts[1];
        return createExchangeDestination(exchangeName, bindingKey, linkName, source);
    }

    private ExchangeDestination createExchangeDestination(final String exchangeName,
                                                          final String bindingKey,
                                                          final String linkName,
                                                          final Source source) throws AmqpErrorException
    {
        ExchangeDestination exchangeDestination = null;
        Exchange<?> exchange = getExchange(exchangeName);
        List<Symbol> sourceCapabilities = new ArrayList<>();
        if (exchange != null)
        {
            Queue queue = null;
            if (!Boolean.TRUE.equals(source.getDynamic()))
            {
                final Map<String, Object> attributes = new HashMap<>();
                boolean isDurable = source.getDurable() != TerminusDurability.NONE;
                boolean isShared = hasCapability(source.getCapabilities(), SHARED_CAPABILITY);
                boolean isGlobal = hasCapability(source.getCapabilities(), ExchangeDestination.GLOBAL_CAPABILITY);

                final String name = getMangledSubscriptionName(linkName, isDurable, isShared, isGlobal);

                if (isGlobal)
                {
                    sourceCapabilities.add(ExchangeDestination.GLOBAL_CAPABILITY);
                }

                ExclusivityPolicy exclusivityPolicy;
                if (isShared)
                {
                    exclusivityPolicy = ExclusivityPolicy.SHARED_SUBSCRIPTION;
                    sourceCapabilities.add(SHARED_CAPABILITY);
                }
                else
                {
                    exclusivityPolicy = ExclusivityPolicy.LINK;
                }

                org.apache.qpid.server.model.LifetimePolicy lifetimePolicy = getLifetimePolicy(source.getExpiryPolicy());

                attributes.put(Queue.ID, UUID.randomUUID());
                attributes.put(Queue.NAME, name);
                attributes.put(Queue.LIFETIME_POLICY, lifetimePolicy);
                attributes.put(Queue.EXCLUSIVE, exclusivityPolicy);
                attributes.put(Queue.DURABLE, isDurable);

                BindingInfo bindingInfo = new BindingInfo(exchange, name,
                                                          bindingKey, source.getFilter());
                Map<String, Map<String, Object>> bindings = bindingInfo.getBindings();
                try
                {
                    if (getAddressSpace() instanceof QueueManagingVirtualHost)
                    {
                        try
                        {
                            queue = ((QueueManagingVirtualHost) getAddressSpace()).getSubscriptionQueue(exchangeName, attributes, bindings);
                        }
                        catch (NotFoundException e)
                        {
                            throw new AmqpErrorException(new Error(AmqpError.NOT_FOUND, e.getMessage()));
                        }
                    }
                    else
                    {
                        throw new AmqpErrorException(new Error(AmqpError.INTERNAL_ERROR,
                                                               "Address space of unexpected type"));
                    }
                }
                catch(IllegalStateException e)
                {
                    throw new AmqpErrorException(new Error(AmqpError.RESOURCE_LOCKED,
                                                           "Subscription is already in use"));
                }
                source.setFilter(bindingInfo.getActualFilters().isEmpty() ? null : bindingInfo.getActualFilters());
                source.setDistributionMode(StdDistMode.COPY);
                exchangeDestination = new ExchangeDestination(exchange,
                                                              queue,
                                                              source.getDurable(),
                                                              source.getExpiryPolicy(),
                                                              exchangeName,
                                                              bindingKey,
                                                              sourceCapabilities);
            }
            else
            {
                // TODO
                throw new AmqpErrorException(new Error(AmqpError.NOT_IMPLEMENTED, "Temporary subscription is not implemented"));
            }
        }
        return exchangeDestination;
    }

    private org.apache.qpid.server.model.LifetimePolicy getLifetimePolicy(final TerminusExpiryPolicy expiryPolicy) throws AmqpErrorException
    {
        org.apache.qpid.server.model.LifetimePolicy lifetimePolicy;
        if (expiryPolicy == null || expiryPolicy == TerminusExpiryPolicy.SESSION_END)
        {
            lifetimePolicy = org.apache.qpid.server.model.LifetimePolicy.DELETE_ON_SESSION_END;
        }
        else if (expiryPolicy == TerminusExpiryPolicy.LINK_DETACH)
        {
            lifetimePolicy = org.apache.qpid.server.model.LifetimePolicy.DELETE_ON_NO_OUTBOUND_LINKS;
        }
        else if (expiryPolicy == TerminusExpiryPolicy.CONNECTION_CLOSE)
        {
            lifetimePolicy = org.apache.qpid.server.model.LifetimePolicy.DELETE_ON_CONNECTION_CLOSE;
        }
        else if (expiryPolicy == TerminusExpiryPolicy.NEVER)
        {
            lifetimePolicy = org.apache.qpid.server.model.LifetimePolicy.PERMANENT;
        }
        else
        {
            Error error = new Error(AmqpError.NOT_IMPLEMENTED,
                                    String.format("unknown ExpiryPolicy '%s'", expiryPolicy.getValue()));
            throw new AmqpErrorException(error);
        }
        return lifetimePolicy;
    }

    private String getMangledSubscriptionName(final String linkName,
                                              final boolean isDurable,
                                              final boolean isShared,
                                              final boolean isGlobal)
    {
        String remoteContainerId = getConnection().getRemoteContainerId();
        if (isGlobal)
        {
            remoteContainerId = "_global_";
        }
        else
        {
            remoteContainerId = sanitizeName(remoteContainerId);
        }

        String subscriptionName;
        if (!isDurable && !isShared)
        {
            subscriptionName = UUID.randomUUID().toString();
        }
        else
        {
            subscriptionName = linkName;
            if (isShared)
            {
                int separator = subscriptionName.indexOf("|");
                if (separator > 0)
                {
                    subscriptionName = subscriptionName.substring(0, separator);
                }
            }
            subscriptionName = sanitizeName(subscriptionName);
        }
        return "qpidsub_/" + remoteContainerId + "_/" + subscriptionName + "_/" + (isDurable
                ? "durable"
                : "nondurable");
    }

    private String sanitizeName(String name)
    {
        return name.replace("_", "__")
                   .replace(".", "_:")
                   .replace("(", "_O")
                   .replace(")", "_C")
                   .replace("<", "_L")
                   .replace(">", "_R");
    }

    private boolean hasCapability(final Symbol[] capabilities,
                                  final Symbol expectedCapability)
    {
        if (capabilities != null)
        {
            for (Symbol capability : capabilities)
            {
                if (expectedCapability.equals(capability))
                {
                    return true;
                }
            }
        }
        return false;
    }


    private void registerConsumer(final SendingLink_1_0 link)
    {
        MessageInstanceConsumer consumer = link.getConsumer();
        if(consumer instanceof Consumer<?,?>)
        {
            Consumer<?,ConsumerTarget_1_0> modelConsumer = (Consumer<?,ConsumerTarget_1_0>) consumer;
            _consumers.add(modelConsumer);
        }
    }


    private MessageSource createDynamicSource(Map properties)
    {
        final String queueName = _primaryDomain + "TempQueue" + UUID.randomUUID().toString();
        MessageSource queue = null;
        try
        {
            Map<String, Object> attributes = convertDynamicNodePropertiesToAttributes(properties, queueName);



            queue = getAddressSpace().createMessageSource(MessageSource.class, attributes);
        }
        catch (AccessControlException e)
        {
            Error error = new Error();
            error.setCondition(AmqpError.UNAUTHORIZED_ACCESS);
            error.setDescription(e.getMessage());

            _connection.close(error);
        }
        catch (AbstractConfiguredObject.DuplicateNameException e)
        {
            _logger.error("A temporary queue was created with a name which collided with an existing queue name");
            throw new ConnectionScopedRuntimeException(e);
        }

        return queue;
    }


    private MessageDestination createDynamicDestination(Map properties)
    {
        final String queueName = _primaryDomain + "TempQueue" + UUID.randomUUID().toString();
        MessageDestination queue = null;
        try
        {
            Map<String, Object> attributes = convertDynamicNodePropertiesToAttributes(properties, queueName);



            queue = getAddressSpace().createMessageDestination(MessageDestination.class, attributes);
        }
        catch (AccessControlException e)
        {
            Error error = new Error();
            error.setCondition(AmqpError.UNAUTHORIZED_ACCESS);
            error.setDescription(e.getMessage());

            _connection.close(error);
        }
        catch (AbstractConfiguredObject.DuplicateNameException e)
        {
            _logger.error("A temporary queue was created with a name which collided with an existing queue name");
            throw new ConnectionScopedRuntimeException(e);
        }

        return queue;
    }

    private Map<String, Object> convertDynamicNodePropertiesToAttributes(final Map properties, final String queueName)
    {
        // TODO convert AMQP 1-0 node properties to queue attributes
        LifetimePolicy lifetimePolicy = properties == null
                                        ? null
                                        : (LifetimePolicy) properties.get(LIFETIME_POLICY);
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(Queue.ID, UUID.randomUUID());
        attributes.put(Queue.NAME, queueName);
        attributes.put(Queue.DURABLE, false);

        if(lifetimePolicy instanceof DeleteOnNoLinks)
        {
            attributes.put(Queue.LIFETIME_POLICY,
                           org.apache.qpid.server.model.LifetimePolicy.DELETE_ON_NO_LINKS);
        }
        else if(lifetimePolicy instanceof DeleteOnNoLinksOrMessages)
        {
            attributes.put(Queue.LIFETIME_POLICY,
                           org.apache.qpid.server.model.LifetimePolicy.IN_USE);
        }
        else if(lifetimePolicy instanceof DeleteOnClose)
        {
            attributes.put(Queue.LIFETIME_POLICY,
                           org.apache.qpid.server.model.LifetimePolicy.DELETE_ON_CONNECTION_CLOSE);
        }
        else if(lifetimePolicy instanceof DeleteOnNoMessages)
        {
            attributes.put(Queue.LIFETIME_POLICY,
                           org.apache.qpid.server.model.LifetimePolicy.IN_USE);
        }
        else
        {
            attributes.put(Queue.LIFETIME_POLICY,
                           org.apache.qpid.server.model.LifetimePolicy.DELETE_ON_CONNECTION_CLOSE);
        }
        return attributes;
    }

    ServerTransaction getTransaction(Binary transactionId)
    {

        ServerTransaction transaction = _openTransactions.get(binaryToInteger(transactionId));
        if(transactionId == null)
        {
            if(_transaction == null)
            {
                _transaction = new AutoCommitTransaction(_connection.getAddressSpace().getMessageStore());
            }
            transaction = _transaction;
        }
        return transaction;
    }

    void remoteEnd(End end)
    {
        // TODO - if the end has a non empty error we should log it
        Iterator<Map.Entry<Integer, ServerTransaction>> iter = _openTransactions.entrySet().iterator();

        while(iter.hasNext())
        {
            Map.Entry<Integer, ServerTransaction> entry = iter.next();
            entry.getValue().rollback();
            iter.remove();
        }

        for(LinkEndpoint linkEndpoint : getLocalLinkEndpoints())
        {
            linkEndpoint.remoteDetached(new Detach());
        }

        _connection.sessionEnded(this);
        performCloseTasks();
        if(_modelObject != null)
        {
            _modelObject.delete();
        }

    }

    Integer binaryToInteger(final Binary txnId)
    {
        if(txnId == null)
        {
            return null;
        }

        byte[] data = txnId.getArray();
        if(data.length > 4)
        {
            throw new IllegalArgumentException();
        }

        int id = 0;
        for(int i = 0; i < data.length; i++)
        {
            id <<= 8;
            id |= ((int)data[i] & 0xff);
        }

        return id;

    }

    Binary integerToBinary(final int txnId)
    {
        byte[] data = new byte[4];
        data[3] = (byte) (txnId & 0xff);
        data[2] = (byte) ((txnId & 0xff00) >> 8);
        data[1] = (byte) ((txnId & 0xff0000) >> 16);
        data[0] = (byte) ((txnId & 0xff000000) >> 24);
        return new Binary(data);
    }

    @Override
    public void close()
    {
        performCloseTasks();
        end();
        if(_modelObject != null)
        {
            _modelObject.delete();
        }
    }

    private void performCloseTasks()
    {

        if(_closed.compareAndSet(false, true))
        {
            List<Action<? super Session_1_0>> taskList = new ArrayList<Action<? super Session_1_0>>(_taskList);
            _taskList.clear();
            for(Action<? super Session_1_0> task : taskList)
            {
                task.performAction(this);
            }
            getAMQPConnection().getEventLogger().message(_logSubject,ChannelMessages.CLOSE());
        }
    }


    public void close(ErrorCondition condition, String message)
    {
        performCloseTasks();
        final End end = new End();
        final Error theError = new Error();
        theError.setDescription(message);
        theError.setCondition(condition);
        end.setError(theError);
        end(end);
    }

    @Override
    public void transportStateChanged()
    {
        for(SendingLinkEndpoint endpoint : _sendingLinkMap.values())
        {
            Link_1_0 link = endpoint.getLink();
            ConsumerTarget_1_0 target = ((SendingLink_1_0)link).getConsumerTarget();
            target.flowStateChanged();
        }

        if (!_consumersWithPendingWork.isEmpty() && !getAMQPConnection().isTransportBlockedForWriting())
        {
            getAMQPConnection().notifyWork(this);
        }

    }

    @Override
    public void block(final Queue<?> queue)
    {
        getAMQPConnection().doOnIOThreadAsync(
                new Runnable()
                {
                    @Override
                    public void run()
                    {
                        doBlock(queue);
                    }
                });
    }

    private void doBlock(final Queue<?> queue)
    {
        if(_blockingEntities.add(queue))
        {
            messageWithSubject(ChannelMessages.FLOW_ENFORCED(queue.getName()));

            for (ReceivingLinkEndpoint endpoint : _receivingLinkMap.values())
            {
                StandardReceivingLink_1_0 link = (StandardReceivingLink_1_0) endpoint.getLink();

                if (isQueueDestinationForLink(queue, link.getDestination()))
                {
                    endpoint.setStopped(true);
                }
            }

        }
    }

    private boolean isQueueDestinationForLink(final Queue<?> queue, final ReceivingDestination recvDest)
    {
        return (recvDest instanceof NodeReceivingDestination
                && queue == ((NodeReceivingDestination) recvDest).getDestination())
               || recvDest instanceof QueueDestination && queue == ((QueueDestination) recvDest).getQueue();
    }

    @Override
    public void unblock(final Queue<?> queue)
    {
        getAMQPConnection().doOnIOThreadAsync(
                new Runnable()
                {
                    @Override
                    public void run()
                    {
                        doUnblock(queue);
                    }
                });
    }

    private void doUnblock(final Queue<?> queue)
    {
        if(_blockingEntities.remove(queue) && !_blockingEntities.contains(this))
        {
            if(_blockingEntities.isEmpty())
            {
                messageWithSubject(ChannelMessages.FLOW_REMOVED());
            }
            for (ReceivingLinkEndpoint endpoint : _receivingLinkMap.values())
            {
                StandardReceivingLink_1_0 link = (StandardReceivingLink_1_0) endpoint.getLink();
                if (isQueueDestinationForLink(queue, link.getDestination()))
                {
                    endpoint.setStopped(false);
                }
            }
        }
    }

    @Override
    public void block()
    {
        getAMQPConnection().doOnIOThreadAsync(
                new Runnable()
                {
                    @Override
                    public void run()
                    {
                        doBlock();
                    }
                });
    }

    private void doBlock()
    {
        if(_blockingEntities.add(this))
        {
            messageWithSubject(ChannelMessages.FLOW_ENFORCED("** All Queues **"));

            for(LinkEndpoint endpoint : _receivingLinkMap.values())
            {
                endpoint.setStopped(true);
            }
        }
    }



    @Override
    public void unblock()
    {
        getAMQPConnection().doOnIOThreadAsync(
                new Runnable()
                {
                    @Override
                    public void run()
                    {
                        doUnblock();
                    }
                });
    }

    private void doUnblock()
    {
        if(_blockingEntities.remove(this))
        {
            if(_blockingEntities.isEmpty())
            {
                messageWithSubject(ChannelMessages.FLOW_REMOVED());
            }
            for(ReceivingLinkEndpoint endpoint : _receivingLinkMap.values())
            {
                StandardReceivingLink_1_0 link = (StandardReceivingLink_1_0) endpoint.getLink();
                if(!_blockingEntities.contains(link.getDestination()))
                {
                    endpoint.setStopped(false);
                }
            }
        }
    }

    @Override
    public boolean getBlocking()
    {
        return !_blockingEntities.isEmpty();
    }

    private void messageWithSubject(final LogMessage operationalLogMessage)
    {
        getEventLogger().message(_logSubject, operationalLogMessage);
    }

    @Override
    public Object getConnectionReference()
    {
        return getConnection().getReference();
    }

    @Override
    public int getUnacknowledgedMessageCount()
    {
        return _unacknowledgedMessages;
    }

    @Override
    public long getTxnStart()
    {
        return _startedTransactions;
    }

    @Override
    public long getTxnCommits()
    {
        return _committedTransactions;
    }

    @Override
    public long getTxnRejects()
    {
        return _rolledBackTransactions;
    }

    @Override
    public long getConsumerCount()
    {
        return getConsumers().size();
    }

    @Override
    public String toLogString()
    {
        final AMQPConnection<?> amqpConnection = getAMQPConnection();
        long connectionId = amqpConnection.getConnectionId();

        String remoteAddress = amqpConnection.getRemoteAddressString();
        final String authorizedPrincipal = amqpConnection.getAuthorizedPrincipal() == null ? "?" : amqpConnection.getAuthorizedPrincipal().getName();
        return "[" +
               MessageFormat.format(CHANNEL_FORMAT,
                                    connectionId,
                                    authorizedPrincipal,
                                    remoteAddress,
                                    getAddressSpace().getName(),
                                    _sendingChannel) + "] ";
    }

    public AMQPConnection_1_0 getConnection()
    {
        return _connection;
    }

    @Override
    public void addDeleteTask(final Action<? super Session_1_0> task)
    {
        // TODO is the closed guard important?
        if(!_closed.get())
        {
            super.addDeleteTask(task);
        }
    }

    public Subject getSubject()
    {
        return _subject;
    }

    private NamedAddressSpace getAddressSpace()
    {
        return _connection.getAddressSpace();
    }

    public SecurityToken getSecurityToken()
    {
        return _token;
    }

    @Override
    public Collection<Consumer<?, ConsumerTarget_1_0>> getConsumers()
    {
        return Collections.unmodifiableCollection(_consumers);
    }

    @Override
    public long getTransactionStartTimeLong()
    {
        return 0L;
    }

    @Override
    public long getTransactionUpdateTimeLong()
    {
        return 0L;
    }

    @Override
    protected void updateBlockedStateIfNecessary()
    {

    }

    @Override
    public boolean isClosing()
    {
        return END_STATES.contains(getSessionState());
    }

    @Override
    public void doTimeoutAction(final String reason)
    {
        getAMQPConnection().closeSessionAsync(this, AMQPConnection.CloseReason.TRANSACTION_TIMEOUT, reason);
    }

    void incrementStartedTransactions()
    {
        _startedTransactions++;
    }

    void incrementCommittedTransactions()
    {
        _committedTransactions++;
    }

    void incrementRolledBackTransactions()
    {
        _rolledBackTransactions++;
    }

    void incrementUnacknowledged()
    {
        _unacknowledgedMessages++;
    }

    void decrementUnacknowledged()
    {
        _unacknowledgedMessages--;
    }

    public CapacityCheckAction getCapacityCheckAction()
    {
        return _capacityCheckAction;
    }

    @Override
    public String toString()
    {
        return "Session_1_0[" + _connection + ": " + _sendingChannel + ']';
    }


    private void detach(UnsignedInteger handle, Detach detach)
    {
        if(_remoteLinkEndpoints.containsKey(handle))
        {
            LinkEndpoint endpoint = _remoteLinkEndpoints.remove(handle);

            endpoint.remoteDetached(detach);

            _localLinkEndpoints.remove(endpoint);

            if (Boolean.TRUE.equals(detach.getClosed()))
            {
                Map<String, ? extends LinkEndpoint> linkMap = endpoint.getRole() == Role.SENDER ? _sendingLinkMap : _receivingLinkMap;
                linkMap.remove(endpoint.getName());
            }
        }
        else
        {
            // TODO
        }
    }

    private void detachLinks()
    {
        Collection<UnsignedInteger> handles = new ArrayList<UnsignedInteger>(_remoteLinkEndpoints.keySet());
        for(UnsignedInteger handle : handles)
        {
            Detach detach = new Detach();
            detach.setClosed(false);
            detach.setHandle(handle);
            detach.setError(_sessionEndedLinkError);
            detach(handle, detach);
        }

        final LinkRegistry linkRegistry = getAddressSpace().getLinkRegistry(getConnection().getRemoteContainerId());

        for(LinkEndpoint<?> linkEndpoint : _sendingLinkMap.values())
        {
            final SendingLink_1_0 link = (SendingLink_1_0) linkRegistry.getDurableSendingLink(linkEndpoint.getName());

            if (link != null)
            {
                synchronized (link)
                {
                    if (link.getEndpoint() == linkEndpoint)
                    {
                        try
                        {
                            link.setLinkAttachment(new SendingLinkAttachment(null, (SendingLinkEndpoint) linkEndpoint));
                        }
                        catch (AmqpErrorException e)
                        {
                            throw new ConnectionScopedRuntimeException(e);
                        }
                    }
                }
            }
        }

        for(LinkEndpoint<?> linkEndpoint : _receivingLinkMap.values())
        {
            final StandardReceivingLink_1_0
                    link = (StandardReceivingLink_1_0) linkRegistry.getDurableReceivingLink(linkEndpoint.getName());

            if (link != null)
            {
                synchronized (link)
                {
                    if (link.getEndpoint() == linkEndpoint)
                    {
                        link.setLinkAttachment(new ReceivingLinkAttachment(null, (ReceivingLinkEndpoint) linkEndpoint));
                    }
                }
            }
        }
    }


    private UnsignedInteger findNextAvailableHandle()
    {
        int i = 0;
        do
        {
            if(!_localLinkEndpoints.containsValue(UnsignedInteger.valueOf(i)))
            {
                return UnsignedInteger.valueOf(i);
            }
        }
        while(++i != 0);

        // TODO
        throw new RuntimeException();
    }


    private Exchange<?> getExchange(String name)
    {
        MessageDestination destination = getAddressSpace().getAttainedMessageDestination(name);
        return destination instanceof Exchange ? (Exchange<?>) destination : null;
    }

    private Queue<?> getQueue(String name)
    {
        MessageSource source = getAddressSpace().getAttainedMessageSource(name);
        return source instanceof Queue ? (Queue<?>) source : null;
    }

    private String getPrimaryDomain()
    {
        String primaryDomain = "";
        final List<String> globalAddressDomains = getAddressSpace().getGlobalAddressDomains();
        if (globalAddressDomains != null && !globalAddressDomains.isEmpty())
        {
            primaryDomain = globalAddressDomains.get(0);
            if(primaryDomain != null)
            {
                primaryDomain = primaryDomain.trim();
                if(!primaryDomain.endsWith("/"))
                {
                    primaryDomain += "/";
                }
            }
        }
        return primaryDomain;
    }

    private final class CapacityCheckAction implements Action<MessageInstance>
    {
        @Override
        public void performAction(final MessageInstance entry)
        {
            TransactionLogResource queue = entry.getOwningResource();
            if(queue instanceof CapacityChecker)
            {
                ((CapacityChecker)queue).checkCapacity(Session_1_0.this);
            }
        }
    }

    private final class BindingInfo
    {
        private final Map<Symbol, Filter> _actualFilters = new HashMap<>();
        private final Map<String, Map<String, Object>> _bindings = new HashMap<>();

        private BindingInfo(Exchange<?> exchange,
                            final String queueName,
                            String bindingKey,
                            Map<Symbol, Filter> filters) throws AmqpErrorException
        {
            String binding = null;
            final Map<String, Object> arguments = new HashMap<>();
            if (filters != null && !filters.isEmpty())
            {
                boolean hasBindingFilter = false;
                boolean hasMessageFilter = false;
                for(Map.Entry<Symbol,Filter> entry : filters.entrySet())
                {
                    if(!hasBindingFilter
                       && entry.getValue() instanceof ExactSubjectFilter
                       && exchange.getType().equals(ExchangeDefaults.DIRECT_EXCHANGE_CLASS))
                    {
                        ExactSubjectFilter filter = (ExactSubjectFilter) entry.getValue();
                        binding = filter.getValue();
                        _actualFilters.put(entry.getKey(), filter);
                        hasBindingFilter = true;
                    }
                    else if(!hasBindingFilter
                            && entry.getValue() instanceof MatchingSubjectFilter
                            && exchange.getType().equals(ExchangeDefaults.TOPIC_EXCHANGE_CLASS))
                    {
                        MatchingSubjectFilter filter = (MatchingSubjectFilter) entry.getValue();
                        binding = filter.getValue();
                        _actualFilters.put(entry.getKey(), filter);
                        hasBindingFilter = true;
                    }
                    else if(entry.getValue() instanceof NoLocalFilter)
                    {
                        _actualFilters.put(entry.getKey(), entry.getValue());
                        arguments.put(AMQPFilterTypes.NO_LOCAL.toString(), true);
                    }
                    else if (!hasMessageFilter
                             && entry.getValue() instanceof org.apache.qpid.server.protocol.v1_0.type.messaging.JMSSelectorFilter)
                    {
                        org.apache.qpid.server.protocol.v1_0.type.messaging.JMSSelectorFilter selectorFilter =
                                (org.apache.qpid.server.protocol.v1_0.type.messaging.JMSSelectorFilter) entry.getValue();

                        // TODO: QPID-7642 - due to inconsistent handling of invalid filters
                        // by different exchange implementations
                        // we need to validate filter before creation of binding
                        try
                        {
                            new org.apache.qpid.server.filter.JMSSelectorFilter(selectorFilter.getValue());
                        }
                        catch (ParseException | SelectorParsingException | TokenMgrError e)
                        {
                            Error error = new Error();
                            error.setCondition(AmqpError.INVALID_FIELD);
                            error.setDescription("Invalid JMS Selector: " + selectorFilter.getValue());
                            error.setInfo(Collections.singletonMap(Symbol.valueOf("field"), Symbol.valueOf("filter")));
                            throw new AmqpErrorException(error);
                        }

                        arguments.put(AMQPFilterTypes.JMS_SELECTOR.toString(), selectorFilter.getValue());
                        _actualFilters.put(entry.getKey(), selectorFilter);
                        hasMessageFilter = true;
                    }
                }
            }

            if(binding != null)
            {
                _bindings.put(binding, arguments);
            }
            if(bindingKey != null)
            {
                _bindings.put(bindingKey, arguments);
            }
            if(binding == null
               && bindingKey == null
               && exchange.getType().equals(ExchangeDefaults.FANOUT_EXCHANGE_CLASS))
            {
                _bindings.put(queueName, arguments);
            }
            else if(binding == null
                    && bindingKey == null
                    && exchange.getType().equals(ExchangeDefaults.TOPIC_EXCHANGE_CLASS))
            {
                _bindings.put("#", arguments);
            }
        }

        private Map<Symbol, Filter> getActualFilters()
        {
            return _actualFilters;
        }

        private Map<String, Map<String, Object>> getBindings()
        {
            return _bindings;
        }


        @Override
        public boolean equals(final Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || getClass() != o.getClass())
            {
                return false;
            }

            final BindingInfo that = (BindingInfo) o;

            return _actualFilters.equals(that._actualFilters) && _bindings.equals(that._bindings);
        }

        @Override
        public int hashCode()
        {
            int result = _actualFilters.hashCode();
            result = 31 * result + _bindings.hashCode();
            return result;
        }
    }
}
