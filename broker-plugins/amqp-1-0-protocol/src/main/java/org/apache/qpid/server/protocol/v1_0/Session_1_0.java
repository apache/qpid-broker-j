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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.security.auth.Subject;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.exchange.ExchangeDefaults;
import org.apache.qpid.server.filter.AMQPFilterTypes;
import org.apache.qpid.server.filter.SelectorParsingException;
import org.apache.qpid.server.filter.selector.ParseException;
import org.apache.qpid.server.filter.selector.TokenMgrError;
import org.apache.qpid.server.logging.LogMessage;
import org.apache.qpid.server.logging.LogSubject;
import org.apache.qpid.server.logging.messages.ChannelMessages;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.message.MessageSource;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Consumer;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.ExclusivityPolicy;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.NotFoundException;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.Session;
import org.apache.qpid.server.bytebuffer.QpidByteBufferUtils;
import org.apache.qpid.server.protocol.v1_0.framing.OversizeFrameException;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.BaseSource;
import org.apache.qpid.server.protocol.v1_0.type.BaseTarget;
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
import org.apache.qpid.server.protocol.v1_0.type.messaging.MatchingSubjectFilter;
import org.apache.qpid.server.protocol.v1_0.type.messaging.NoLocalFilter;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
import org.apache.qpid.server.protocol.v1_0.type.messaging.StdDistMode;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Target;
import org.apache.qpid.server.protocol.v1_0.type.messaging.TerminusExpiryPolicy;
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
import org.apache.qpid.server.protocol.v1_0.type.transport.SessionError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Transfer;
import org.apache.qpid.server.queue.CreatingLinkInfo;
import org.apache.qpid.server.queue.CreatingLinkInfoImpl;
import org.apache.qpid.server.security.SecurityToken;
import org.apache.qpid.server.session.AbstractAMQPSession;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;

public class Session_1_0 extends AbstractAMQPSession<Session_1_0, ConsumerTarget_1_0>
        implements LogSubject, org.apache.qpid.server.util.Deletable<Session_1_0>
{
    public static final Symbol DELAYED_DELIVERY = Symbol.valueOf("DELAYED_DELIVERY");
    private static final Logger _logger = LoggerFactory.getLogger(Session_1_0.class);
    public static final Symbol LIFETIME_POLICY = Symbol.valueOf("lifetime-policy");
    private static final EnumSet<SessionState> END_STATES =
            EnumSet.of(SessionState.END_RECVD, SessionState.END_PIPE, SessionState.END_SENT, SessionState.ENDED);

    private final AMQPConnection_1_0<?> _connection;
    private AtomicBoolean _closed = new AtomicBoolean();

    private final CopyOnWriteArrayList<Consumer<?, ConsumerTarget_1_0>> _consumers = new CopyOnWriteArrayList<>();

    private Session<?> _modelObject = this;

    private SessionState _sessionState;

    private final Map<LinkEndpoint<? extends BaseSource, ? extends BaseTarget>, UnsignedInteger> _endpointToOutputHandle = new HashMap<>();
    private final Map<UnsignedInteger, LinkEndpoint<? extends BaseSource, ? extends BaseTarget>> _inputHandleToEndpoint = new HashMap<>();
    private final Set<LinkEndpoint<? extends BaseSource, ? extends BaseTarget>> _associatedLinkEndpoints = new HashSet<>();

    private final int _receivingChannel;
    private final int _sendingChannel;


    private static final int DEFAULT_SESSION_BUFFER_SIZE = 1 << 11;

    private int _nextOutgoingDeliveryId;

    private UnsignedInteger _initialOutgoingId = UnsignedInteger.ZERO;
    private SequenceNumber _nextIncomingId;
    private final UnsignedInteger _incomingWindow;
    private SequenceNumber _nextOutgoingId = new SequenceNumber(_initialOutgoingId.intValue());
    private final UnsignedInteger _outgoingWindow = UnsignedInteger.valueOf(DEFAULT_SESSION_BUFFER_SIZE);
    private volatile long _remoteIncomingWindow;
    private UnsignedInteger _remoteOutgoingWindow = UnsignedInteger.ZERO;
    private UnsignedInteger _lastSentIncomingLimit;

    private LinkedHashMap<UnsignedInteger,Delivery> _outgoingUnsettled = new LinkedHashMap<>(DEFAULT_SESSION_BUFFER_SIZE);
    private LinkedHashMap<UnsignedInteger,Delivery> _incomingUnsettled = new LinkedHashMap<>(DEFAULT_SESSION_BUFFER_SIZE);


    private final Error _sessionEndedLinkError =
            new Error(LinkError.DETACH_FORCED,
                      "Force detach the link because the session is remotely ended.");

    private final String _primaryDomain;
    private final Set<Object> _blockingEntities = Collections.newSetFromMap(new ConcurrentHashMap<Object,Boolean>());
    private volatile long _startedTransactions;
    private volatile long _committedTransactions;
    private volatile long _rolledBackTransactions;
    private volatile int _unacknowledgedMessages;

    public Session_1_0(final AMQPConnection_1_0 connection,
                       Begin begin,
                       int sendingChannelId,
                       int receivingChannelId,
                       long incomingWindow)
    {
        super(connection, sendingChannelId);
        _sendingChannel = sendingChannelId;
        _receivingChannel = receivingChannelId;
        _sessionState = SessionState.ACTIVE;
        _nextIncomingId = new SequenceNumber(begin.getNextOutgoingId().intValue());
        _connection = connection;
        _primaryDomain = getPrimaryDomain();
        _incomingWindow = UnsignedInteger.valueOf(incomingWindow);

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

    public void sendDetach(final Detach detach)
    {
        send(detach);
    }

    public void receiveAttach(final Attach attach)
    {
        if(_sessionState == SessionState.ACTIVE)
        {
            UnsignedInteger inputHandle = attach.getHandle();
            if (_inputHandleToEndpoint.containsKey(inputHandle))
            {
                String errorMessage = String.format("Input Handle '%d' already in use", inputHandle.intValue());
                getConnection().close(new Error(SessionError.HANDLE_IN_USE, errorMessage));
                throw new ConnectionScopedRuntimeException(errorMessage);
            }
            else
            {
                final Link_1_0<? extends BaseSource, ? extends BaseTarget> link;
                if (attach.getRole() == Role.RECEIVER)
                {
                    link = getAddressSpace().getSendingLink(getConnection().getRemoteContainerId(), attach.getName());
                }
                else
                {
                    link = getAddressSpace().getReceivingLink(getConnection().getRemoteContainerId(), attach.getName());
                }

                final ListenableFuture<? extends LinkEndpoint<?,?>> future = link.attach(this, attach);

                addFutureCallback(future, new EndpointCreationCallback(attach), MoreExecutors.directExecutor());
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
                unsettled.remove(UnsignedInteger.valueOf(pos.intValue()));
                pos.incr();
            }
        }

        send(disposition);
        //TODO - check send flow
    }

    public boolean hasCreditToSend()
    {
        boolean b = _remoteIncomingWindow > 0;
        boolean b1 = getOutgoingWindow() != null && getOutgoingWindow().compareTo(UnsignedInteger.ZERO) > 0;
        return b && b1;
    }

    public void end()
    {
        end(new End());
    }

    public void sendTransfer(final Transfer xfr, final SendingLinkEndpoint endpoint, final boolean newDelivery)
    {
        _nextOutgoingId.incr();
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
                }
                else
                {
                    endpoint.settle(delivery.getDeliveryTag());
                    _outgoingUnsettled.remove(deliveryId);
                }
            }
        }
        xfr.setDeliveryId(deliveryId);
        _remoteIncomingWindow--;
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
                _sessionState = SessionState.END_RECVD;
                detachLinks();
                remoteEnd(end);
                _connection.sendEnd(_sendingChannel, new End(), true);
                _sessionState = SessionState.ENDED;
                break;
            default:
                End reply = new End();
                Error error = new Error();
                error.setCondition(AmqpError.ILLEGAL_STATE);
                error.setDescription("END called on Session which has not been opened");
                reply.setError(error);
                _connection.sendEnd(_sendingChannel, reply, true);
                break;
        }
    }

    public UnsignedInteger getNextOutgoingId()
    {
        return UnsignedInteger.valueOf(_nextOutgoingId.intValue());
    }

    public void sendFlowConditional()
    {
        if(_nextIncomingId != null)
        {
            UnsignedInteger clientsCredit =
                    _lastSentIncomingLimit.subtract(UnsignedInteger.valueOf(_nextIncomingId.intValue()));

            // TODO - we should use a better metric here, and/or manage session credit across the whole connection
            // send a flow if the window is at least half used up
            if (_incomingWindow.subtract(clientsCredit).compareTo(clientsCredit) >= 0)
            {
                sendFlow();
            }
        }

    }

    public UnsignedInteger getOutgoingWindow()
    {
        return _outgoingWindow;
    }

    public void receiveFlow(final Flow flow)
    {
        final SequenceNumber flowNextIncomingId = new SequenceNumber(flow.getNextIncomingId() == null
                                                                             ? _initialOutgoingId.intValue()
                                                                             : flow.getNextIncomingId().intValue());
        if (flowNextIncomingId.compareTo(_nextOutgoingId) > 0)
        {
            final End end = new End();
            end.setError(new Error(SessionError.WINDOW_VIOLATION,
                                   String.format("Next incoming id '%d' exceeds next outgoing id '%d'",
                                                 flowNextIncomingId,
                                                 _nextOutgoingId)));
            end(end);
        }
        else
        {
            _remoteIncomingWindow = flowNextIncomingId.longValue() + flow.getIncomingWindow().longValue()
                                    - _nextOutgoingId.longValue();

            _nextIncomingId = new SequenceNumber(flow.getNextOutgoingId().intValue());
            _remoteOutgoingWindow = flow.getOutgoingWindow();

            UnsignedInteger handle = flow.getHandle();
            final LinkEndpoint<? extends BaseSource, ? extends BaseTarget> endpoint =
                    handle == null ? null : _inputHandleToEndpoint.get(handle);

            if (endpoint != null)
            {
                endpoint.receiveFlow(flow);

                if (Boolean.TRUE.equals(flow.getEcho()))
                {
                    endpoint.sendFlow();
                }
            }
            else
            {
                final Collection<LinkEndpoint<? extends BaseSource, ? extends BaseTarget>> allLinkEndpoints =
                        _inputHandleToEndpoint.values();
                for (LinkEndpoint<? extends BaseSource, ? extends BaseTarget> le : allLinkEndpoints)
                {
                    le.flowStateChanged();
                }

                if (Boolean.TRUE.equals(flow.getEcho()))
                {
                    sendFlow();
                }
            }
        }
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

        SequenceNumber deliveryId = new SequenceNumber(disposition.getFirst().intValue());
        SequenceNumber last;
        if(disposition.getLast() == null)
        {
            last = deliveryId;
        }
        else
        {
            last = new SequenceNumber(disposition.getLast().intValue());
        }


        while(deliveryId.compareTo(last)<=0)
        {
            UnsignedInteger deliveryIdUnsigned = UnsignedInteger.valueOf(deliveryId.intValue());
            Delivery delivery = unsettledTransfers.get(deliveryIdUnsigned);
            if(delivery != null)
            {
                delivery.getLinkEndpoint().receiveDeliveryState(delivery,
                                                                disposition.getState(),
                                                                disposition.getSettled());
                if (Boolean.TRUE.equals(disposition.getSettled()))
                {
                    unsettledTransfers.remove(deliveryIdUnsigned);
                }
            }
            deliveryId.incr();
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

    public void sendFlow(final Flow flow)
    {
        if(_nextIncomingId != null)
        {
            flow.setNextIncomingId(_nextIncomingId.unsignedIntegerValue());
            _lastSentIncomingLimit = _incomingWindow.add(_nextIncomingId.unsignedIntegerValue());
        }
        flow.setIncomingWindow(_incomingWindow);

        flow.setNextOutgoingId(UnsignedInteger.valueOf(_nextOutgoingId.intValue()));
        flow.setOutgoingWindow(_outgoingWindow);
        send(flow);
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
                _connection.sendEnd(_sendingChannel, end, true);
                _sessionState = SessionState.END_SENT;
                break;
            default:
                End reply = new End();
                Error error = new Error();
                error.setCondition(AmqpError.ILLEGAL_STATE);
                error.setDescription("END called on Session which has not been opened");
                reply.setError(error);
                _connection.sendEnd(_sendingChannel, reply, true);
                break;


        }
    }

    public void receiveTransfer(final Transfer transfer)
    {
        _nextIncomingId.incr();
        _remoteOutgoingWindow = _remoteOutgoingWindow.subtract(UnsignedInteger.ONE);

        UnsignedInteger inputHandle = transfer.getHandle();
        LinkEndpoint<? extends BaseSource, ? extends BaseTarget> linkEndpoint = _inputHandleToEndpoint.get(inputHandle);

        if (linkEndpoint == null)
        {
            Error error = new Error();
            error.setCondition(AmqpError.ILLEGAL_STATE);
            error.setDescription("TRANSFER called on Session for link handle " + inputHandle + " which is not attached");
            _connection.close(error);

        }
        else if(!(linkEndpoint instanceof AbstractReceivingLinkEndpoint))
        {

            Error error = new Error();
            error.setCondition(ConnectionError.FRAMING_ERROR);
            error.setDescription("TRANSFER called on Session for link handle " + inputHandle + " which is a sending ink not a receiving link");
            _connection.close(error);

        }
        else
        {
            AbstractReceivingLinkEndpoint endpoint = ((AbstractReceivingLinkEndpoint) linkEndpoint);

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
                                         + inputHandle
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

    boolean isEnded()
    {
        return _sessionState == SessionState.ENDED || _connection.isClosed();
    }

    UnsignedInteger getIncomingWindow()
    {
        return _incomingWindow;
    }

    AccessControlContext getAccessControllerContext()
    {
        return _accessControllerContext;
    }

    public ReceivingDestination getReceivingDestination(final Link_1_0<?, ?> link,
                                                        final Target target) throws AmqpErrorException
    {
        final ReceivingDestination destination;
        if (target != null)
        {
            if (Boolean.TRUE.equals(target.getDynamic()))
            {
                MessageDestination tempDestination = createDynamicDestination(link, target.getDynamicNodeProperties(), target.getCapabilities());
                // TODO: avoid NPE
                target.setAddress(tempDestination.getName());
            }

            String addr = target.getAddress();
            if (addr == null || "".equals(addr.trim()))
            {
                destination = new AnonymousRelayDestination(getAddressSpace(), target, _connection.getEventLogger());
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
                    destination = exchangeDestination;
                }
                else
                {
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
                        destination = null;
                    }
                }
            }
        }
        else
        {
            destination = null;
        }

        if (destination == null)
        {
            throw new AmqpErrorException(AmqpError.NOT_FOUND,
                                         String.format("Could not find destination for target '%s'", target));
        }

        return destination;
    }

    public boolean updateSourceForSubscription(final SendingLinkEndpoint linkEndpoint, final Source newSource,
                                                final SendingDestination newDestination)
    {
        SendingDestination oldDestination = linkEndpoint.getDestination();
        if (oldDestination instanceof ExchangeDestination)
        {
            ExchangeDestination oldExchangeDestination = (ExchangeDestination) oldDestination;
            String newAddress = newSource.getAddress();
            if (newDestination instanceof ExchangeDestination)
            {
                ExchangeDestination newExchangeDestination = (ExchangeDestination) newDestination;
                if (oldExchangeDestination.getQueue() != newExchangeDestination.getQueue())
                {
                    Source oldSource = linkEndpoint.getSource();
                    oldSource.setAddress(newAddress);
                    oldSource.setFilter(newSource.getFilter());
                    return true;
                }
            }
        }
        return false;
    }

    public SendingDestination getSendingDestination(final Link_1_0<?, ?> link,
                                                    final Source source) throws AmqpErrorException
    {
        SendingDestination destination = null;

        if (Boolean.TRUE.equals(source.getDynamic()))
        {
            MessageSource tempQueue = createDynamicSource(link, source.getDynamicNodeProperties());
            source.setAddress(tempQueue.getName()); // todo : temporary topic
        }

        String address = source.getAddress();
        if (address != null)
        {
            if (!address.startsWith("/") && address.contains("/"))
            {
                destination = createExchangeDestination(address, link.getName(), source);
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
                    destination = createExchangeDestination(address, null, link.getName(), source);
                }
            }
        }

        if (destination == null)
        {
            throw new AmqpErrorException(AmqpError.NOT_FOUND,
                                         String.format("Could not find destination for source '%s'", source));
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
                boolean isDurable = source.getExpiryPolicy() == TerminusExpiryPolicy.NEVER;
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

    private MessageSource createDynamicSource(final Link_1_0<?, ?> link,
                                              Map properties)
    {
        final String queueName = _primaryDomain + "TempQueue" + UUID.randomUUID().toString();
        try
        {
            Map<String, Object> attributes = convertDynamicNodePropertiesToAttributes(link, properties, queueName);

            return Subject.doAs(getSubjectWithAddedSystemRights(),
                                (PrivilegedAction<MessageSource>) () -> getAddressSpace().createMessageSource(MessageSource.class, attributes));
        }
        catch (AccessControlException e)
        {
            Error error = new Error();
            error.setCondition(AmqpError.UNAUTHORIZED_ACCESS);
            error.setDescription(e.getMessage());

            _connection.close(error);
            return null;
        }
        catch (AbstractConfiguredObject.DuplicateNameException e)
        {
            _logger.error("A temporary queue was created with a name which collided with an existing queue name");
            throw new ConnectionScopedRuntimeException(e);
        }
    }


    private MessageDestination createDynamicDestination(final Link_1_0<?, ?> link,
                                                        Map properties,
                                                        final Symbol[] capabilities)
    {
        final Set<Symbol> capabilitySet = capabilities == null ? Collections.emptySet() : Sets.newHashSet(capabilities);
        boolean isTopic = capabilitySet.contains(Symbol.valueOf("temporary-topic")) || capabilitySet.contains(Symbol.valueOf("topic"));
        final String destName = _primaryDomain + (isTopic ? "TempTopic" : "TempQueue") + UUID.randomUUID().toString();
        try
        {
            Map<String, Object> attributes = convertDynamicNodePropertiesToAttributes(link, properties, destName);


            Class<? extends MessageDestination> clazz = isTopic ? Exchange.class : MessageDestination.class;
            if (isTopic)
            {
                attributes.put(Exchange.TYPE, ExchangeDefaults.FANOUT_EXCHANGE_CLASS);
            }

            return Subject.doAs(getSubjectWithAddedSystemRights(),
                                (PrivilegedAction<MessageDestination>) () -> getAddressSpace().createMessageDestination(clazz, attributes));
        }
        catch (AccessControlException e)
        {
            Error error = new Error();
            error.setCondition(AmqpError.UNAUTHORIZED_ACCESS);
            error.setDescription(e.getMessage());

            _connection.close(error);
            return null;
        }
        catch (AbstractConfiguredObject.DuplicateNameException e)
        {
            _logger.error("A temporary destination was created with a name which collided with an existing destination name '{}'", destName);
            throw new ConnectionScopedRuntimeException(e);
        }
    }

    private Map<String, Object> convertDynamicNodePropertiesToAttributes(final Link_1_0<?, ?> link,
                                                                         final Map properties,
                                                                         final String nodeName)
    {
        // TODO convert AMQP 1-0 node properties to queue attributes
        LifetimePolicy lifetimePolicy = properties == null
                                        ? null
                                        : (LifetimePolicy) properties.get(LIFETIME_POLICY);

        Map<String,Object> attributes = new HashMap<>();
        attributes.put(ConfiguredObject.ID, UUID.randomUUID());
        attributes.put(ConfiguredObject.NAME, nodeName);
        attributes.put(ConfiguredObject.DURABLE, true);

        if(lifetimePolicy instanceof DeleteOnNoLinks)
        {
            attributes.put(ConfiguredObject.LIFETIME_POLICY,
                           org.apache.qpid.server.model.LifetimePolicy.DELETE_ON_NO_LINKS);
        }
        else if(lifetimePolicy instanceof DeleteOnNoLinksOrMessages)
        {
            attributes.put(ConfiguredObject.LIFETIME_POLICY,
                           org.apache.qpid.server.model.LifetimePolicy.IN_USE);
        }
        else if(lifetimePolicy instanceof DeleteOnClose)
        {
            attributes.put(ConfiguredObject.LIFETIME_POLICY,
                           org.apache.qpid.server.model.LifetimePolicy.DELETE_ON_CREATING_LINK_CLOSE);
            final CreatingLinkInfo linkInfo = new CreatingLinkInfoImpl(link.getRole() == Role.SENDER, link.getRemoteContainerId(), link.getName());
            attributes.put("creatingLinkInfo", linkInfo);
        }
        else if(lifetimePolicy instanceof DeleteOnNoMessages)
        {
            attributes.put(ConfiguredObject.LIFETIME_POLICY,
                           org.apache.qpid.server.model.LifetimePolicy.IN_USE);
        }
        else
        {
            attributes.put(ConfiguredObject.LIFETIME_POLICY,
                           org.apache.qpid.server.model.LifetimePolicy.DELETE_ON_CONNECTION_CLOSE);
        }
        return attributes;
    }

    ServerTransaction getTransaction(Binary transactionId)
    {
        // TODO - deal with the case where the txn id is invalid
        return _connection.getTransaction(binaryToInteger(transactionId));
    }

    void remoteEnd(End end)
    {
        Set<LinkEndpoint<? extends BaseSource, ? extends BaseTarget>> associatedLinkEndpoints = new HashSet<>(_associatedLinkEndpoints);
        for (LinkEndpoint<? extends BaseSource, ? extends BaseTarget> linkEndpoint : associatedLinkEndpoints)
        {
            linkEndpoint.remoteDetached(new Detach());
            linkEndpoint.destroy();
        }
        _associatedLinkEndpoints.clear();

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
        for (LinkEndpoint<? extends BaseSource, ? extends BaseTarget> linkEndpoint : _endpointToOutputHandle.keySet())
        {
            if (linkEndpoint instanceof SendingLinkEndpoint)
            {
                ConsumerTarget_1_0 target = ((SendingLinkEndpoint) linkEndpoint).getConsumerTarget();
                target.flowStateChanged();
            }
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

            for (LinkEndpoint<? extends BaseSource, ? extends BaseTarget> linkEndpoint : _endpointToOutputHandle.keySet())
            {
                if (linkEndpoint instanceof StandardReceivingLinkEndpoint
                    && isQueueDestinationForLink(queue, ((StandardReceivingLinkEndpoint) linkEndpoint).getReceivingDestination()))
                {
                    linkEndpoint.setStopped(true);
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
            for (LinkEndpoint<? extends BaseSource, ? extends BaseTarget> linkEndpoint : _endpointToOutputHandle.keySet())
            {
                if (linkEndpoint instanceof StandardReceivingLinkEndpoint
                        && isQueueDestinationForLink(queue, ((StandardReceivingLinkEndpoint) linkEndpoint).getReceivingDestination()))
                {
                    linkEndpoint.setStopped(false);
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

            for (LinkEndpoint<? extends BaseSource, ? extends BaseTarget> linkEndpoint : _endpointToOutputHandle.keySet())
            {
                if (linkEndpoint instanceof StandardReceivingLinkEndpoint)
                {
                    linkEndpoint.setStopped(true);
                }
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
            for (LinkEndpoint<? extends BaseSource, ? extends BaseTarget> linkEndpoint : _endpointToOutputHandle.keySet())
            {
                if (linkEndpoint instanceof StandardReceivingLinkEndpoint
                    && !_blockingEntities.contains(((StandardReceivingLinkEndpoint) linkEndpoint).getReceivingDestination()))
                {
                    linkEndpoint.setStopped(false);
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
        return _consumers.size();
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

    public AMQPConnection_1_0<?> getConnection()
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

    @Override
    public String toString()
    {
        return "Session_1_0[" + _connection + ": " + _sendingChannel + ']';
    }

    public void dissociateEndpoint(LinkEndpoint<? extends BaseSource, ? extends BaseTarget> linkEndpoint)
    {
        for (Map.Entry<UnsignedInteger, LinkEndpoint<? extends BaseSource, ? extends BaseTarget>> entry : _inputHandleToEndpoint.entrySet())
        {
            if (entry.getValue() == linkEndpoint)
            {
                _inputHandleToEndpoint.remove(entry.getKey());
                break;
            }
        }
        _endpointToOutputHandle.remove(linkEndpoint);
        _associatedLinkEndpoints.remove(linkEndpoint);
    }

    private void detach(UnsignedInteger handle, Detach detach)
    {
        if(_inputHandleToEndpoint.containsKey(handle))
        {
            LinkEndpoint<? extends BaseSource, ? extends BaseTarget> endpoint = _inputHandleToEndpoint.remove(handle);
            endpoint.remoteDetached(detach);
            _endpointToOutputHandle.remove(endpoint);
        }
        else
        {
            // TODO
        }
    }

    private void detachLinks()
    {
        Collection<UnsignedInteger> handles = new ArrayList<>(_inputHandleToEndpoint.keySet());
        for(UnsignedInteger handle : handles)
        {
            Detach detach = new Detach();
            detach.setClosed(false);
            detach.setHandle(handle);
            detach.setError(_sessionEndedLinkError);
            detach(handle, detach);
        }
    }


    private UnsignedInteger findNextAvailableOutputHandle()
    {
        int i = 0;
        do
        {
            if(!_endpointToOutputHandle.containsValue(UnsignedInteger.valueOf(i)))
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

    private class EndpointCreationCallback<T extends LinkEndpoint<? extends BaseSource, ? extends BaseTarget>> implements FutureCallback<T>
    {

        private final Attach _attach;

        EndpointCreationCallback(final Attach attach)
        {
            _attach = attach;
        }

        @Override
        public void onSuccess(final T endpoint)
        {
            doOnIOThreadAsync(new Runnable()
            {
                @Override
                public void run()
                {
                    _associatedLinkEndpoints.add(endpoint);
                    endpoint.setLocalHandle(findNextAvailableOutputHandle());
                    if (endpoint instanceof ErrantLinkEndpoint)
                    {
                        endpoint.sendAttach();
                        ((ErrantLinkEndpoint) endpoint).closeWithError();
                    }
                    else
                    {
                        if (endpoint instanceof StandardReceivingLinkEndpoint
                            && (_blockingEntities.contains(Session_1_0.this)
                                || _blockingEntities.contains(((StandardReceivingLinkEndpoint) endpoint).getReceivingDestination())))
                        {
                            endpoint.setStopped(true);
                        }
                        _inputHandleToEndpoint.put(_attach.getHandle(), endpoint);
                        if (!_endpointToOutputHandle.containsKey(endpoint))
                        {
                            _endpointToOutputHandle.put(endpoint, endpoint.getLocalHandle());
                            endpoint.sendAttach();
                            endpoint.start();
                        }
                        else
                        {
                            // TODO - link stealing???
                        }

                    }
                }
            });
        }

        @Override
        public void onFailure(final Throwable t)
        {
            String errorMessage = String.format("Failed to create LinkEndpoint in response to Attach: %s", _attach);
            _logger.error(errorMessage, t);
            throw new ConnectionScopedRuntimeException(errorMessage, t);
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
