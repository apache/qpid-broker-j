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

import java.security.AccessControlContext;
import java.security.AccessControlException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import javax.security.auth.Subject;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.exchange.ExchangeDefaults;
import org.apache.qpid.server.logging.LogMessage;
import org.apache.qpid.server.logging.LogSubject;
import org.apache.qpid.server.logging.messages.ChannelMessages;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.message.MessageSource;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.DestinationAddress;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.ExclusivityPolicy;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.protocol.v1_0.delivery.DeliveryRegistry;
import org.apache.qpid.server.protocol.v1_0.delivery.DeliveryRegistryImpl;
import org.apache.qpid.server.protocol.v1_0.delivery.UnsettledDelivery;
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
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
import org.apache.qpid.server.protocol.v1_0.type.messaging.StdDistMode;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Target;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Terminus;
import org.apache.qpid.server.protocol.v1_0.type.messaging.TerminusExpiryPolicy;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
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

public class Session_1_0 extends AbstractAMQPSession<Session_1_0, ConsumerTarget_1_0>
        implements LogSubject, org.apache.qpid.server.util.Deletable<Session_1_0>
{
    static final Symbol DELAYED_DELIVERY = Symbol.valueOf("DELAYED_DELIVERY");
    static final Symbol SHARED_CAPABILITY = Symbol.getSymbol("shared");
    static final Symbol GLOBAL_CAPABILITY = Symbol.getSymbol("global");
    private static final Logger LOGGER = LoggerFactory.getLogger(Session_1_0.class);
    public static final Symbol LIFETIME_POLICY = Symbol.valueOf("lifetime-policy");
    private static final EnumSet<SessionState> END_STATES =
            EnumSet.of(SessionState.END_RECVD, SessionState.END_PIPE, SessionState.END_SENT, SessionState.ENDED);

    private final AMQPConnection_1_0<?> _connection;
    private final AtomicBoolean _closed = new AtomicBoolean();

    private SessionState _sessionState;

    private final Map<LinkEndpoint<? extends BaseSource, ? extends BaseTarget>, UnsignedInteger> _endpointToOutputHandle = new HashMap<>();
    private final Map<UnsignedInteger, LinkEndpoint<? extends BaseSource, ? extends BaseTarget>> _inputHandleToEndpoint = new HashMap<>();
    private final Set<LinkEndpoint<? extends BaseSource, ? extends BaseTarget>> _associatedLinkEndpoints = new HashSet<>();

    private final int _sendingChannel;


    private static final int DEFAULT_SESSION_BUFFER_SIZE = 1 << 11;

    private int _nextOutgoingDeliveryId;

    private final UnsignedInteger _initialOutgoingId = UnsignedInteger.ZERO;
    private SequenceNumber _nextIncomingId;
    private final UnsignedInteger _incomingWindow;
    private final SequenceNumber _nextOutgoingId = new SequenceNumber(_initialOutgoingId.intValue());
    private final UnsignedInteger _outgoingWindow = UnsignedInteger.valueOf(DEFAULT_SESSION_BUFFER_SIZE);
    private volatile long _remoteIncomingWindow;
    private UnsignedInteger _remoteOutgoingWindow = UnsignedInteger.ZERO;
    private UnsignedInteger _lastSentIncomingLimit;

    private final DeliveryRegistry _outgoingDeliveryRegistry = new DeliveryRegistryImpl();
    private final DeliveryRegistry _incomingDeliveryRegistry = new DeliveryRegistryImpl();

    private final Error _sessionEndedLinkError =
            new Error(LinkError.DETACH_FORCED,
                      "Force detach the link because the session is remotely ended.");

    private final String _primaryDomain;
    private final Set<Object> _blockingEntities = Collections.newSetFromMap(new ConcurrentHashMap<>());

    public Session_1_0(final AMQPConnection_1_0 connection,
                       Begin begin,
                       int sendingChannelId,
                       int receivingChannelId,
                       long incomingWindow)
    {
        super(connection, sendingChannelId);
        _sendingChannel = sendingChannelId;
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
        receivedComplete();
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

    private void updateDisposition(final Role role,
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
            final DeliveryRegistry deliveryRegistry = role == Role.RECEIVER ? _incomingDeliveryRegistry : _outgoingDeliveryRegistry;

            SequenceNumber pos = new SequenceNumber(first.intValue());
            SequenceNumber end = new SequenceNumber(last.intValue());
            while (pos.compareTo(end) <= 0)
            {
                deliveryRegistry.removeDelivery(UnsignedInteger.valueOf(pos.intValue()));
                pos.incr();
            }
        }

        send(disposition);
    }

    void updateDisposition(final LinkEndpoint<?,?> linkEndpoint,
                           final Binary deliveryTag,
                           final DeliveryState state,
                           final boolean settled)
    {
        final UnsignedInteger deliveryId = getDeliveryId(deliveryTag, linkEndpoint);
        updateDisposition(linkEndpoint.getRole(), deliveryId, deliveryId, state, settled);
    }

    private UnsignedInteger getDeliveryId(final DeliveryRegistry deliveryRegistry,
                                          final Binary deliveryTag,
                                          final LinkEndpoint<?, ?> linkEndpoint)
    {
        final UnsignedInteger deliveryId = deliveryRegistry.getDeliveryId(deliveryTag, linkEndpoint);
        if (deliveryId == null)
        {
            throw new ConnectionScopedRuntimeException(String.format(
                    "Delivery with tag '%s' is not found in unsettled deliveries", deliveryTag));
        }
        return deliveryId;
    }

    private SortedSet<UnsignedInteger> getDeliveryIds(final Set<Binary> deliveryTags, final LinkEndpoint<?, ?> linkEndpoint)
    {
        final DeliveryRegistry deliveryRegistry = getDeliveryRegistry(linkEndpoint.getRole());
        return deliveryTags.stream()
                           .map(deliveryTag -> getDeliveryId(deliveryRegistry, deliveryTag, linkEndpoint))
                           .collect(Collectors.toCollection(TreeSet::new));
    }

    private UnsignedInteger getDeliveryId(final Binary deliveryTag, final LinkEndpoint<?, ?> linkEndpoint)
    {
        final DeliveryRegistry deliveryRegistry = getDeliveryRegistry(linkEndpoint.getRole());
        return getDeliveryId(deliveryRegistry, deliveryTag, linkEndpoint);
    }

    private DeliveryRegistry getDeliveryRegistry(final Role role)
    {
        return role == Role.RECEIVER ? getIncomingDeliveryRegistry() : getOutgoingDeliveryRegistry();
    }

    void updateDisposition(final LinkEndpoint<?,?> linkEndpoint,
                           final Set<Binary> deliveryTags,
                           final DeliveryState state,
                           final boolean settled)
    {
        final Role role = linkEndpoint.getRole();
        final Iterator<UnsignedInteger> iterator = getDeliveryIds(deliveryTags, linkEndpoint).iterator();
        if (iterator.hasNext())
        {
            UnsignedInteger begin = iterator.next();
            UnsignedInteger end = begin;
            while (iterator.hasNext())
            {
                final UnsignedInteger deliveryId = iterator.next();
                if (!end.add(UnsignedInteger.ONE).equals(deliveryId))
                {
                    updateDisposition(role, begin, end, state, settled);
                    begin = deliveryId;
                    end = begin;
                }
                else
                {
                    end = deliveryId;
                }
            }
            updateDisposition(role, begin, end, state, settled);
        }
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

    void sendTransfer(final Transfer xfr, final SendingLinkEndpoint endpoint)
    {
        _nextOutgoingId.incr();
        final boolean settled = Boolean.TRUE.equals(xfr.getSettled());
        UnsignedInteger deliveryId = UnsignedInteger.valueOf(_nextOutgoingDeliveryId++);
        xfr.setDeliveryId(deliveryId);
        if (!settled)
        {
            final UnsettledDelivery delivery = new UnsettledDelivery(xfr.getDeliveryTag(), endpoint);
            _outgoingDeliveryRegistry.addDelivery(deliveryId, delivery);
        }

        _remoteIncomingWindow--;
        try (QpidByteBuffer payload = xfr.getPayload())
        {
            long remaining = payload == null ? 0 : (long) payload.remaining();
            int payloadSent = _connection.sendFrame(_sendingChannel, xfr, payload);
            if(payload != null)
            {
                while (payloadSent < remaining && payloadSent >= 0)
                {
                    Transfer continuationTransfer = new Transfer();

                    continuationTransfer.setHandle(xfr.getHandle());
                    continuationTransfer.setRcvSettleMode(xfr.getRcvSettleMode());
                    continuationTransfer.setState(xfr.getState());
                    continuationTransfer.setPayload(payload);

                    _nextOutgoingId.incr();
                    _remoteIncomingWindow--;

                    remaining = (long) payload.remaining();
                    payloadSent = _connection.sendFrame(_sendingChannel, continuationTransfer, payload);

                    continuationTransfer.dispose();
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
        receivedComplete();
        switch (_sessionState)
        {
            case END_SENT:
                remoteEnd(end);
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
        receivedComplete();
        final SequenceNumber flowNextIncomingId = new SequenceNumber(flow.getNextIncomingId() == null
                                                                             ? _initialOutgoingId.intValue()
                                                                             : flow.getNextIncomingId().intValue());
        if (flowNextIncomingId.compareTo(_nextOutgoingId) > 0)
        {
            final End end = new End();
            end.setError(new Error(SessionError.WINDOW_VIOLATION,
                                   String.format("Next incoming id '%d' exceeds next outgoing id '%d'",
                                                 flowNextIncomingId.longValue(),
                                                 _nextOutgoingId.longValue())));
            end(end);
        }
        else
        {
            _remoteIncomingWindow = flowNextIncomingId.longValue() + flow.getIncomingWindow().longValue()
                                    - _nextOutgoingId.longValue();

            _nextIncomingId = new SequenceNumber(flow.getNextOutgoingId().intValue());
            _remoteOutgoingWindow = flow.getOutgoingWindow();

            UnsignedInteger handle = flow.getHandle();
            if (handle != null)
            {
                final LinkEndpoint<? extends BaseSource, ? extends BaseTarget> endpoint = _inputHandleToEndpoint.get(handle);
                if (endpoint == null)
                {
                    End end = new End();
                    end.setError(new Error(SessionError.UNATTACHED_HANDLE,
                                           String.format("Received Flow with unknown handle %d", handle.intValue())));
                    end(end);
                }
                else
                {
                    endpoint.receiveFlow(flow);
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

        DeliveryRegistry unsettledDeliveries;

        if(dispositionRole == Role.RECEIVER)
        {
            unsettledDeliveries = _outgoingDeliveryRegistry;
        }
        else
        {
            unsettledDeliveries = _incomingDeliveryRegistry;
        }

        SequenceNumber deliveryId = new SequenceNumber(disposition.getFirst().intValue());
        SequenceNumber last;
        if(disposition.getLast() == null)
        {
            last = new SequenceNumber(deliveryId.intValue());
        }
        else
        {
            last = new SequenceNumber(disposition.getLast().intValue());
        }

        while(deliveryId.compareTo(last)<=0)
        {
            UnsignedInteger deliveryIdUnsigned = UnsignedInteger.valueOf(deliveryId.intValue());
            UnsettledDelivery unsettledDelivery = unsettledDeliveries.getDelivery(deliveryIdUnsigned);

            if(unsettledDelivery != null)
            {
                LinkEndpoint<?,?> linkEndpoint  = unsettledDelivery.getLinkEndpoint();
                linkEndpoint.receiveDeliveryState(unsettledDelivery.getDeliveryTag(), disposition.getState(), disposition.getSettled());
                if (Boolean.TRUE.equals(disposition.getSettled()))
                {
                    unsettledDeliveries.removeDelivery(deliveryIdUnsigned);
                }
            }
            deliveryId.incr();
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
        receivedComplete();
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
            error.setCondition(SessionError.UNATTACHED_HANDLE);
            error.setDescription("TRANSFER called on Session for link handle " + inputHandle + " which is not attached.");
            _connection.close(error);

        }
        else if(!(linkEndpoint instanceof AbstractReceivingLinkEndpoint))
        {

            Error error = new Error();
            error.setCondition(AmqpError.PRECONDITION_FAILED);
            error.setDescription("Received TRANSFER for link handle " + inputHandle + " which is a sending link not a receiving link.");
            _connection.close(error);

        }
        else
        {
            AbstractReceivingLinkEndpoint endpoint = ((AbstractReceivingLinkEndpoint) linkEndpoint);
            endpoint.receiveTransfer(transfer);
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
                MessageDestination tempDestination = createDynamicDestination(link, target);
                if(tempDestination != null)
                {
                    target.setAddress(_primaryDomain + tempDestination.getName());
                }
                else
                {
                    throw new AmqpErrorException(AmqpError.INTERNAL_ERROR, "Cannot create dynamic destination");
                }
            }

            String addr = target.getAddress();
            if (addr == null || "".equals(addr.trim()))
            {
                destination = new AnonymousRelayDestination(getAddressSpace(), target, _connection.getEventLogger());
            }
            else
            {
                DestinationAddress destinationAddress = new DestinationAddress(getAddressSpace(), addr, true);
                MessageDestination messageDestination = destinationAddress.getMessageDestination();

                if (messageDestination != null)
                {
                    destination = new NodeReceivingDestination(destinationAddress,
                                                               target.getDurable(),
                                                               target.getExpiryPolicy(),
                                                               target.getCapabilities(),
                                                               _connection.getEventLogger());
                }
                else
                {
                    destination = null;
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
        if (oldDestination instanceof ExchangeSendingDestination)
        {
            ExchangeSendingDestination oldExchangeDestination = (ExchangeSendingDestination) oldDestination;
            String newAddress = newSource.getAddress();
            if (newDestination instanceof ExchangeSendingDestination)
            {
                ExchangeSendingDestination newExchangeDestination = (ExchangeSendingDestination) newDestination;
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
            MessageSource tempSource = createDynamicSource(link, source);
            if(tempSource != null)
            {
                source.setAddress(_primaryDomain + tempSource.getName());
            }
            else
            {
                throw new AmqpErrorException(AmqpError.INTERNAL_ERROR, "Cannot create dynamic source");
            }
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
                    destination = new StandardSendingDestination(queue);
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

    private ExchangeSendingDestination createExchangeDestination(String address, final String linkName, final Source source)
            throws AmqpErrorException
    {
        String[] parts = address.split("/", 2);
        String exchangeName = parts[0];
        String bindingKey = parts[1];
        return createExchangeDestination(exchangeName, bindingKey, linkName, source);
    }

    private ExchangeSendingDestination createExchangeDestination(final String exchangeName,
                                                                 final String bindingKey,
                                                                 final String linkName,
                                                                 final Source source) throws AmqpErrorException
    {
        ExchangeSendingDestination exchangeDestination = null;
        Exchange<?> exchange = getExchange(exchangeName);
        if (exchange != null)
        {
            if (!Boolean.TRUE.equals(source.getDynamic()))
            {
                String remoteContainerId = getConnection().getRemoteContainerId();
                exchangeDestination = new ExchangeSendingDestination(exchange, linkName, bindingKey, remoteContainerId, source);
                source.setFilter(exchangeDestination.getFilters());
                source.setDistributionMode(StdDistMode.COPY);
            }
            else
            {
                // TODO
                throw new AmqpErrorException(new Error(AmqpError.NOT_IMPLEMENTED, "Temporary subscription is not implemented"));
            }
        }
        return exchangeDestination;
    }

    private MessageSource createDynamicSource(final Link_1_0<?, ?> link,
                                              final Terminus terminus) throws AmqpErrorException
    {
        // TODO temporary topics?
        final String queueName = "TempQueue" + UUID.randomUUID().toString();
        try
        {
            final Map<String, Object> attributes = createDynamicNodeAttributes(link, terminus, queueName);

            if (terminus.getCapabilities() != null)
            {
                final Set<Symbol> capabilities = Sets.newHashSet(terminus.getCapabilities());
                if (capabilities.contains(Symbol.valueOf("temporary-queue"))
                    || capabilities.contains(Symbol.valueOf("temporary-topic")))
                {
                    attributes.put(Queue.EXCLUSIVE, ExclusivityPolicy.CONNECTION);
                }
            }
            return Subject.doAs(getSubjectWithAddedSystemRights(),
                                (PrivilegedAction<MessageSource>) () -> getAddressSpace().createMessageSource(MessageSource.class, attributes));
        }
        catch (AccessControlException e)
        {
            throw new AmqpErrorException(AmqpError.UNAUTHORIZED_ACCESS, e.getMessage());
        }
        catch (AbstractConfiguredObject.DuplicateNameException e)
        {
            LOGGER.error("A temporary queue was created with a name which collided with an existing queue name");
            throw new ConnectionScopedRuntimeException(e);
        }
    }


    private MessageDestination createDynamicDestination(final Link_1_0<?, ?> link,
                                                        final Terminus terminus) throws AmqpErrorException
    {
        final Symbol[] capabilities = terminus.getCapabilities();
        final Set<Symbol> capabilitySet = capabilities == null ? Collections.emptySet() : Sets.newHashSet(capabilities);
        boolean isTopic = capabilitySet.contains(Symbol.valueOf("temporary-topic")) || capabilitySet.contains(Symbol.valueOf("topic"));
        final String destName = (isTopic ? "TempTopic" : "TempQueue") + UUID.randomUUID().toString();
        try
        {
            final Map<String, Object> attributes = createDynamicNodeAttributes(link, terminus, destName);


            Class<? extends MessageDestination> clazz = isTopic ? Exchange.class : MessageDestination.class;
            if (isTopic)
            {
                attributes.put(Exchange.TYPE, ExchangeDefaults.FANOUT_EXCHANGE_CLASS);
            }
            else if (capabilitySet.contains(Symbol.valueOf("temporary-queue")))
            {
                attributes.put(Queue.EXCLUSIVE, ExclusivityPolicy.CONNECTION);
            }

            return Subject.doAs(getSubjectWithAddedSystemRights(),
                                (PrivilegedAction<MessageDestination>) () -> getAddressSpace().createMessageDestination(clazz, attributes));
        }
        catch (AccessControlException e)
        {
            throw new AmqpErrorException(AmqpError.UNAUTHORIZED_ACCESS, e.getMessage());
        }
        catch (AbstractConfiguredObject.DuplicateNameException e)
        {
            LOGGER.error("A temporary destination was created with a name which collided with an existing destination name '{}'", destName);
            throw new ConnectionScopedRuntimeException(e);
        }
    }

    private Map<String, Object> createDynamicNodeAttributes(final Link_1_0<?, ?> link,
                                                            final Terminus terminus,
                                                            final String nodeName)
    {
        // TODO convert AMQP 1-0 node properties to queue attributes

        final Map<Symbol, Object> properties = terminus.getDynamicNodeProperties();
        final TerminusExpiryPolicy expiryPolicy = terminus.getExpiryPolicy();
        LifetimePolicy lifetimePolicy = properties == null
                                        ? null
                                        : (LifetimePolicy) properties.get(LIFETIME_POLICY);

        Map<String,Object> attributes = new HashMap<>();
        attributes.put(ConfiguredObject.ID, UUID.randomUUID());
        attributes.put(ConfiguredObject.NAME, nodeName);
        attributes.put(ConfiguredObject.DURABLE, TerminusExpiryPolicy.NEVER.equals(expiryPolicy));

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
        final int txnId;
        try
        {
            txnId = transactionIdToInteger(transactionId);
        }
        catch (IllegalArgumentException e)
        {
            throw new UnknownTransactionException(e.getMessage());
        }
        return _connection.getTransaction(txnId);
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
    }

    static Integer transactionIdToInteger(final Binary txnId)
    {
        if (txnId == null)
        {
            throw new UnknownTransactionException("'null' is not a valid transaction-id.");
        }

        byte[] data = txnId.getArray();
        if (data.length > 4)
        {
            throw new IllegalArgumentException("transaction-id cannot have more than 32-bit.");
        }

        int id = 0;
        for (int i = 0; i < data.length; i++)
        {
            id <<= 8;
            id |= ((int) data[i] & 0xff);
        }

        return id;
    }

    static Binary integerToTransactionId(final int txnId)
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
    }

    private void performCloseTasks()
    {

        if(_closed.compareAndSet(false, true))
        {
            List<Action<? super Session_1_0>> taskList = new ArrayList<>(_taskList);
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
        getAMQPConnection().doOnIOThreadAsync(() -> doBlock(queue));
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
                && queue == ((NodeReceivingDestination) recvDest).getDestination());
    }

    @Override
    public void unblock(final Queue<?> queue)
    {
        getAMQPConnection().doOnIOThreadAsync(() -> doUnblock(queue));
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
        getAMQPConnection().doOnIOThreadAsync(this::doBlock);
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
        getAMQPConnection().doOnIOThreadAsync(this::doUnblock);
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
        return _outgoingDeliveryRegistry.size();
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
        // TODO: QPID-7951 is the closed guard important?
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
        return END_STATES.contains(getSessionState()) || getConnection().isClosing();
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
        if (linkEndpoint.getRole() == Role.RECEIVER)
        {
            getIncomingDeliveryRegistry().removeDeliveriesForLinkEndpoint(linkEndpoint);
        }
        else
        {
            getOutgoingDeliveryRegistry().removeDeliveriesForLinkEndpoint(linkEndpoint);
        }
    }

    private void detach(UnsignedInteger handle, Detach detach)
    {
        if(_inputHandleToEndpoint.containsKey(handle))
        {
            LinkEndpoint<? extends BaseSource, ? extends BaseTarget> endpoint = _inputHandleToEndpoint.remove(handle);
            endpoint.remoteDetached(detach);
            _endpointToOutputHandle.remove(endpoint);
            _associatedLinkEndpoints.remove(endpoint);
        }
        else
        {
            //TODO: QPID-7954
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

        return null;
    }


    private Exchange<?> getExchange(String name)
    {
        MessageDestination destination = getAddressSpace().getAttainedMessageDestination(name, false);
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

    DeliveryRegistry getOutgoingDeliveryRegistry()
    {
        return _outgoingDeliveryRegistry;
    }

    DeliveryRegistry getIncomingDeliveryRegistry()
    {
        return _incomingDeliveryRegistry;
    }

    void receivedComplete()
    {
        _associatedLinkEndpoints.forEach(linkedEnpoint -> linkedEnpoint.receiveComplete());
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
            doOnIOThreadAsync(() ->
            {
                _associatedLinkEndpoints.add(endpoint);
                _inputHandleToEndpoint.put(_attach.getHandle(), endpoint);
                UnsignedInteger nextAvailableOutputHandle = findNextAvailableOutputHandle();
                if (nextAvailableOutputHandle == null)
                {
                    endpoint.close(new Error(AmqpError.RESOURCE_LIMIT_EXCEEDED,
                                             String.format(
                                                     "Cannot find free handle for endpoint '%s' on session '%s'",
                                                     _attach.getName(),
                                                     endpoint.getSession().toLogString())));
                }
                else
                {
                    endpoint.setLocalHandle(nextAvailableOutputHandle);
                    if (endpoint instanceof ErrantLinkEndpoint)
                    {
                        endpoint.sendAttach();
                        ((ErrantLinkEndpoint) endpoint).closeWithError();
                    }
                    else
                    {
                        if (endpoint instanceof StandardReceivingLinkEndpoint
                            && (_blockingEntities.contains(Session_1_0.this)
                                || _blockingEntities.contains(((StandardReceivingLinkEndpoint) endpoint)
                                                                      .getReceivingDestination())))
                        {
                            endpoint.setStopped(true);
                        }

                        if (!_endpointToOutputHandle.containsKey(endpoint))
                        {
                            _endpointToOutputHandle.put(endpoint, endpoint.getLocalHandle());
                            endpoint.sendAttach();
                            endpoint.start();
                        }
                        else
                        {
                            final End end = new End();
                            end.setError(new Error(AmqpError.INTERNAL_ERROR,
                                                   "Endpoint is already registered with session."));
                            endpoint.getSession().end(end);
                        }
                    }
                }
            });
        }

        @Override
        public void onFailure(final Throwable t)
        {
            String errorMessage = String.format("Failed to create LinkEndpoint in response to Attach: %s", _attach);
            LOGGER.error(errorMessage, t);
            throw new ConnectionScopedRuntimeException(errorMessage, t);
        }
    }
}
