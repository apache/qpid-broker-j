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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.security.auth.Subject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.bytebuffer.QpidByteBuffer;
import org.apache.qpid.protocol.AMQConstant;
import org.apache.qpid.server.connection.SessionPrincipal;
import org.apache.qpid.server.consumer.ConsumerImpl;
import org.apache.qpid.server.consumer.ConsumerTarget;
import org.apache.qpid.server.logging.LogSubject;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.message.MessageSource;
import org.apache.qpid.server.model.AbstractConfigurationChangeListener;
import org.apache.qpid.server.model.ConfigurationChangeListener;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Consumer;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.Session;
import org.apache.qpid.server.protocol.AMQSessionModel;
import org.apache.qpid.server.protocol.ConsumerListener;
import org.apache.qpid.server.protocol.LinkRegistry;
import org.apache.qpid.server.protocol.v1_0.framing.OversizeFrameException;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.DeliveryState;
import org.apache.qpid.server.protocol.v1_0.type.FrameBody;
import org.apache.qpid.server.protocol.v1_0.type.LifetimePolicy;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.messaging.DeleteOnClose;
import org.apache.qpid.server.protocol.v1_0.type.messaging.DeleteOnNoLinks;
import org.apache.qpid.server.protocol.v1_0.type.messaging.DeleteOnNoLinksOrMessages;
import org.apache.qpid.server.protocol.v1_0.type.messaging.DeleteOnNoMessages;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Target;
import org.apache.qpid.server.protocol.v1_0.type.messaging.TerminusDurability;
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
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.txn.AutoCommitTransaction;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.server.virtualhost.QueueExistsException;
import org.apache.qpid.transport.network.Ticker;

public class Session_1_0 implements AMQSessionModel<Session_1_0>, LogSubject
{
    private static final Logger _logger = LoggerFactory.getLogger(Session_1_0.class);
    private static final Symbol LIFETIME_POLICY = Symbol.valueOf("lifetime-policy");
    private final AccessControlContext _accessControllerContext;
    private final SecurityToken _securityToken;
    private AutoCommitTransaction _transaction;

    private final LinkedHashMap<Integer, ServerTransaction> _openTransactions =
            new LinkedHashMap<Integer, ServerTransaction>();

    private final CopyOnWriteArrayList<Action<? super Session_1_0>> _taskList =
            new CopyOnWriteArrayList<Action<? super Session_1_0>>();

    private final AMQPConnection_1_0 _connection;
    private UUID _id = UUID.randomUUID();
    private AtomicBoolean _closed = new AtomicBoolean();
    private final Subject _subject = new Subject();

    private final CopyOnWriteArrayList<Consumer<?>> _consumers = new CopyOnWriteArrayList<Consumer<?>>();
    private final CopyOnWriteArrayList<SendingLink_1_0> _sendingLinks = new CopyOnWriteArrayList<>();
    private final ConfigurationChangeListener _consumerClosedListener = new ConsumerClosedListener();
    private final CopyOnWriteArrayList<ConsumerListener> _consumerListeners = new CopyOnWriteArrayList<ConsumerListener>();
    private Session<?> _modelObject;
    private final List<ConsumerTarget_1_0> _consumersWithPendingWork = new ArrayList<>();

    private SessionState _state ;

    private final Map<String, LinkEndpoint> _linkMap = new HashMap<>();
    private final Map<LinkEndpoint, UnsignedInteger> _localLinkEndpoints = new HashMap<>();
    private final Map<UnsignedInteger, LinkEndpoint> _remoteLinkEndpoints = new HashMap<>();
    private long _lastAttachedTime;

    private short _receivingChannel;
    private short _sendingChannel;


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


    public Session_1_0(final AMQPConnection_1_0 connection)
    {
        this(connection, SessionState.INACTIVE, null);
    }

    public Session_1_0(final AMQPConnection_1_0 connection, Begin begin)
    {
        this(connection, SessionState.BEGIN_RECVD, new SequenceNumber(begin.getNextOutgoingId().intValue()));
    }


    private Session_1_0(final AMQPConnection_1_0 connection, SessionState state, SequenceNumber nextIncomingId)
    {

        _state = state;
        _nextIncomingTransferId = nextIncomingId;
        _connection = connection;
        _subject.getPrincipals().addAll(connection.getSubject().getPrincipals());
        _subject.getPrincipals().add(new SessionPrincipal(this));
        _accessControllerContext = connection.getAccessControlContextFromSubject(_subject);
        _securityToken = connection.getAddressSpace() instanceof ConfiguredObject
                ? ((ConfiguredObject)connection.getAddressSpace()).newToken(_subject)
                : connection.getBroker().newToken(_subject);
    }

    public void setReceivingChannel(final short receivingChannel)
    {
        _receivingChannel = receivingChannel;
        switch(_state)
        {
            case INACTIVE:
                _state = SessionState.BEGIN_RECVD;
                break;
            case BEGIN_SENT:
                _state = SessionState.ACTIVE;
                break;
            case END_PIPE:
                _state = SessionState.END_SENT;
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
        if(_state == SessionState.ACTIVE)
        {
            UnsignedInteger handle = attach.getHandle();
            if(_remoteLinkEndpoints.containsKey(handle))
            {
                // TODO - Error - handle busy?
            }
            else
            {
                LinkEndpoint endpoint = _linkMap.get(attach.getName());
                if(endpoint == null)
                {
                    endpoint = attach.getRole() == Role.RECEIVER
                               ? new SendingLinkEndpoint(this, attach)
                               : new ReceivingLinkEndpoint(this, attach);

                    // TODO : fix below - distinguish between local and remote owned
                    endpoint.setSource(attach.getSource());
                    endpoint.setTarget(attach.getTarget());


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

                    remoteLinkCreation(endpoint);

                }
                else
                {
                    endpoint.receiveAttach(attach);
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


        if(settled)
        {
            if(role == Role.RECEIVER)
            {
                SequenceNumber pos = new SequenceNumber(first.intValue());
                SequenceNumber end = new SequenceNumber(last.intValue());
                while(pos.compareTo(end)<=0)
                {
                    Delivery d = _incomingUnsettled.remove(new UnsignedInteger(pos.intValue()));

/*
                    _availableIncomingCredit += d.getTransfers().size();
*/

                    pos.incr();
                }
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
        if(newDelivery)
        {
            deliveryId = UnsignedInteger.valueOf(_nextOutgoingDeliveryId++);
            endpoint.setLastDeliveryId(deliveryId);
        }
        else
        {
            deliveryId = endpoint.getLastDeliveryId();
        }
        xfr.setDeliveryId(deliveryId);

        if(!Boolean.TRUE.equals(xfr.getSettled()))
        {
            Delivery delivery;
            if((delivery = _outgoingUnsettled.get(deliveryId))== null)
            {
                delivery = new Delivery(xfr, endpoint);
                _outgoingUnsettled.put(deliveryId, delivery);

            }
            else
            {
                delivery.addTransfer(xfr);
            }
            _outgoingSessionCredit = _outgoingSessionCredit.subtract(UnsignedInteger.ONE);
            endpoint.addUnsettled(delivery);

        }

        try
        {
            QpidByteBuffer payload = xfr.getPayload();
            int payloadSent = _connection.sendFrame(_sendingChannel, xfr, payload);

            if(payload != null && payloadSent < payload.remaining() && payloadSent >= 0)
            {
                payload = payload.duplicate();
                try
                {
                    payload.position(payload.position()+payloadSent);

                    Transfer secondTransfer = new Transfer();

                    secondTransfer.setDeliveryTag(xfr.getDeliveryTag());
                    secondTransfer.setHandle(xfr.getHandle());
                    secondTransfer.setSettled(xfr.getSettled());
                    secondTransfer.setState(xfr.getState());
                    secondTransfer.setMessageFormat(xfr.getMessageFormat());
                    secondTransfer.setPayload(payload);

                    sendTransfer(secondTransfer, endpoint, false);
                }
                finally
                {
                    payload.dispose();
                }

            }
        }
        catch(OversizeFrameException e)
        {
            throw new ConnectionScopedRuntimeException(e);
        }
    }

    public boolean isActive()
    {
        return _state == SessionState.ACTIVE;
    }

    public void receiveEnd(final End end)
    {
        switch (_state)
        {
            case END_SENT:
                _state = SessionState.ENDED;
                break;
            case ACTIVE:
                detachLinks();
                remoteEnd(end);
                short sendChannel = _sendingChannel;
                _connection.sendEnd(sendChannel, new End(), true);
                _state = SessionState.ENDED;
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
            }
            deliveryId = deliveryId.add(UnsignedInteger.ONE);
        }
        if(disposition.getSettled())
        {
            //TODO - check send flow
        }

    }

    public SessionState getState()
    {
        return _state;
    }

    public void sendFlow()
    {
        sendFlow(new Flow());
    }

    public void setSendingChannel(final short sendingChannel)
    {
        _sendingChannel = sendingChannel;
        switch(_state)
        {
            case INACTIVE:
                _state = SessionState.BEGIN_SENT;
                break;
            case BEGIN_RECVD:
                _state = SessionState.ACTIVE;
                break;
            default:
                // TODO error

        }
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
        switch (_state)
        {
            case BEGIN_SENT:
                _connection.sendEnd(_sendingChannel, end, false);
                _state = SessionState.END_PIPE;
                break;
            case ACTIVE:
                detachLinks();
                short sendChannel = _sendingChannel;
                _connection.sendEnd(sendChannel, end, true);
                _state = SessionState.END_SENT;
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

            endpoint.receiveTransfer(transfer, delivery);

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
        return _state == SessionState.ENDED || _connection.isClosed();
    }

    UnsignedInteger getIncomingWindowSize()
    {
        return UnsignedInteger.valueOf(_availableIncomingCredit);
    }

    AccessControlContext getAccessControllerContext()
    {
        return _accessControllerContext;
    }

    public void remoteLinkCreation(final LinkEndpoint endpoint)
    {
        Destination destination;
        Link_1_0 link = null;
        Error error = null;

        final LinkRegistry linkRegistry = getAddressSpace().getLinkRegistry(getConnection().getRemoteContainerId());


        if (endpoint.getRole() == Role.SENDER)
        {

            final SendingLink_1_0 previousLink =
                    (SendingLink_1_0) linkRegistry.getDurableSendingLink(endpoint.getName());

            if (previousLink == null)
            {

                Target target = (Target) endpoint.getTarget();
                Source source = (Source) endpoint.getSource();


                if (source != null)
                {
                    if (Boolean.TRUE.equals(source.getDynamic()))
                    {
                        Queue<?> tempQueue = createTemporaryQueue(source.getDynamicNodeProperties());
                        source.setAddress(tempQueue.getName());
                    }
                    String addr = source.getAddress();
                    if (!addr.startsWith("/") && addr.contains("/"))
                    {
                        String[] parts = addr.split("/", 2);
                        Exchange<?> exchg = getExchange(parts[0]);
                        if (exchg != null)
                        {
                            ExchangeDestination exchangeDestination =
                                    new ExchangeDestination(exchg,
                                                            source.getDurable(),
                                                            source.getExpiryPolicy(),
                                                            parts[0],
                                                            target.getCapabilities());
                            exchangeDestination.setInitialRoutingAddress(parts[1]);
                            destination = exchangeDestination;
                            target.setCapabilities(exchangeDestination.getCapabilities());
                        }
                        else
                        {
                            endpoint.setSource(null);
                            destination = null;
                        }
                    }
                    else
                    {
                        MessageSource queue = getAddressSpace().getAttainedMessageSource(addr);
                        if (queue != null)
                        {
                            destination = new MessageSourceDestination(queue);
                        }
                        else
                        {
                            Exchange<?> exchg = getExchange(addr);
                            if (exchg != null)
                            {
                                ExchangeDestination exchangeDestination =
                                              new ExchangeDestination(exchg,
                                                                      source.getDurable(),
                                                                      source.getExpiryPolicy(),
                                                                      addr,
                                                                      target.getCapabilities());
                                destination = exchangeDestination;
                                target.setCapabilities(exchangeDestination.getCapabilities());
                            }
                            else
                            {
                                endpoint.setSource(null);
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
                    final SendingLinkEndpoint sendingLinkEndpoint = (SendingLinkEndpoint) endpoint;
                    try
                    {
                        final SendingLink_1_0 sendingLink =
                                new SendingLink_1_0(new SendingLinkAttachment(this, sendingLinkEndpoint),
                                                    getAddressSpace(),
                                                    (SendingDestination) destination
                                );

                        sendingLinkEndpoint.setLinkEventListener(new SubjectSpecificSendingLinkListener(
                                sendingLink));
                        registerConsumer(sendingLink);

                        link = sendingLink;
                        if (TerminusDurability.UNSETTLED_STATE.equals(source.getDurable())
                            || TerminusDurability.CONFIGURATION.equals(source.getDurable()))
                        {
                            linkRegistry.registerSendingLink(endpoint.getName(), sendingLink);
                        }
                    }
                    catch (AmqpErrorException e)
                    {
                        _logger.error("Error creating sending link", e);
                        destination = null;
                        sendingLinkEndpoint.setSource(null);
                        error = e.getError();
                    }
                }
            }
            else
            {
                Source newSource = (Source) endpoint.getSource();

                Source oldSource = (Source) previousLink.getEndpoint().getSource();
                final TerminusDurability newSourceDurable = newSource == null ? null : newSource.getDurable();
                if (newSourceDurable != null)
                {
                    oldSource.setDurable(newSourceDurable);
                    if (newSourceDurable.equals(TerminusDurability.NONE))
                    {
                        linkRegistry.unregisterSendingLink(endpoint.getName());
                    }
                }
                endpoint.setSource(oldSource);
                SendingLinkEndpoint sendingLinkEndpoint = (SendingLinkEndpoint) endpoint;
                previousLink.setLinkAttachment(new SendingLinkAttachment(this, sendingLinkEndpoint));
                sendingLinkEndpoint.setLinkEventListener(new SubjectSpecificSendingLinkListener(previousLink));
                link = previousLink;
                endpoint.setLocalUnsettled(previousLink.getUnsettledOutcomeMap());
                registerConsumer(previousLink);

            }
        }
        else
        {
            if (endpoint.getTarget() instanceof Coordinator)
            {
                Coordinator coordinator = (Coordinator) endpoint.getTarget();
                TxnCapability[] capabilities = coordinator.getCapabilities();
                boolean localTxn = false;
                boolean multiplePerSession = false;
                if (capabilities != null)
                {
                    for (TxnCapability capability : capabilities)
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
                            error = new Error();
                            error.setCondition(AmqpError.NOT_IMPLEMENTED);
                            error.setDescription("Unsupported capability: " + capability);
                            break;
                        }
                    }
                }

       /*         if(!localTxn)
                {
                    capabilities.add(TxnCapabilities.LOCAL_TXN);
                }*/

                final ReceivingLinkEndpoint receivingLinkEndpoint = (ReceivingLinkEndpoint) endpoint;
                final TxnCoordinatorLink_1_0 coordinatorLink =
                        new TxnCoordinatorLink_1_0(getAddressSpace(),
                                                   this,
                                                   receivingLinkEndpoint,
                                                   _openTransactions);
                receivingLinkEndpoint.setLinkEventListener(new SubjectSpecificReceivingLinkListener(
                        coordinatorLink));
                link = coordinatorLink;


            }
            else
            {

                ReceivingLink_1_0 previousLink =
                        (ReceivingLink_1_0) linkRegistry.getDurableReceivingLink(endpoint.getName());

                if (previousLink == null)
                {

                    Target target = (Target) endpoint.getTarget();

                    if (target != null)
                    {
                        if (Boolean.TRUE.equals(target.getDynamic()))
                        {

                            Queue<?> tempQueue = createTemporaryQueue(target.getDynamicNodeProperties());
                            target.setAddress(tempQueue.getName());
                        }

                        String addr = target.getAddress();
                        if (addr == null || "".equals(addr.trim()))
                        {
                            MessageDestination messageDestination = getAddressSpace().getDefaultDestination();
                            destination = new NodeReceivingDestination(messageDestination, target.getDurable(),
                                                                       target.getExpiryPolicy(), "",
                                                                       target.getCapabilities());
                            target.setCapabilities(destination.getCapabilities());

                        }
                        else if (!addr.startsWith("/") && addr.contains("/"))
                        {
                            String[] parts = addr.split("/", 2);
                            Exchange<?> exchange =getExchange(parts[0]);
                            if (exchange != null)
                            {
                                ExchangeDestination exchangeDestination =
                                        new ExchangeDestination(exchange,
                                                                target.getDurable(),
                                                                target.getExpiryPolicy(),
                                                                parts[0],
                                                                target.getCapabilities());

                                exchangeDestination.setInitialRoutingAddress(parts[1]);
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
                                        new NodeReceivingDestination(messageDestination, target.getDurable(),
                                                                     target.getExpiryPolicy(), addr, target.getCapabilities());
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
                        final ReceivingLinkEndpoint receivingLinkEndpoint = (ReceivingLinkEndpoint) endpoint;
                        final ReceivingLink_1_0 receivingLink =
                                new ReceivingLink_1_0(new ReceivingLinkAttachment(this, receivingLinkEndpoint),
                                                      getAddressSpace(),
                                                      (ReceivingDestination) destination);

                        receivingLinkEndpoint.setLinkEventListener(new SubjectSpecificReceivingLinkListener(
                                receivingLink));

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
                    receivingLinkEndpoint.setLinkEventListener(previousLink);
                    link = previousLink;
                    endpoint.setLocalUnsettled(previousLink.getUnsettledOutcomeMap());

                }
            }
        }

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


    private void registerConsumer(final SendingLink_1_0 link)
    {
        ConsumerImpl consumer = link.getConsumer();
        if(consumer instanceof Consumer<?>)
        {
            Consumer<?> modelConsumer = (Consumer<?>) consumer;
            _consumers.add(modelConsumer);
            _sendingLinks.add(link);
            modelConsumer.addChangeListener(_consumerClosedListener);
            consumerAdded(modelConsumer);
        }
    }


    private Queue<?> createTemporaryQueue(Map properties)
    {
        final String queueName = UUID.randomUUID().toString();
        Queue<?> queue = null;
        try
        {
            LifetimePolicy lifetimePolicy = properties == null
                                            ? null
                                            : (LifetimePolicy) properties.get(LIFETIME_POLICY);
            Map<String,Object> attributes = new HashMap<String,Object>();
            attributes.put(org.apache.qpid.server.model.Queue.ID, UUID.randomUUID());
            attributes.put(org.apache.qpid.server.model.Queue.NAME, queueName);
            attributes.put(org.apache.qpid.server.model.Queue.DURABLE, false);

            if(lifetimePolicy instanceof DeleteOnNoLinks)
            {
                attributes.put(org.apache.qpid.server.model.Queue.LIFETIME_POLICY,
                               org.apache.qpid.server.model.LifetimePolicy.DELETE_ON_NO_LINKS);
            }
            else if(lifetimePolicy instanceof DeleteOnNoLinksOrMessages)
            {
                attributes.put(org.apache.qpid.server.model.Queue.LIFETIME_POLICY,
                               org.apache.qpid.server.model.LifetimePolicy.IN_USE);
            }
            else if(lifetimePolicy instanceof DeleteOnClose)
            {
                attributes.put(org.apache.qpid.server.model.Queue.LIFETIME_POLICY,
                               org.apache.qpid.server.model.LifetimePolicy.DELETE_ON_CONNECTION_CLOSE);
            }
            else if(lifetimePolicy instanceof DeleteOnNoMessages)
            {
                attributes.put(org.apache.qpid.server.model.Queue.LIFETIME_POLICY,
                               org.apache.qpid.server.model.LifetimePolicy.IN_USE);
            }
            else
            {
                attributes.put(org.apache.qpid.server.model.Queue.LIFETIME_POLICY,
                               org.apache.qpid.server.model.LifetimePolicy.DELETE_ON_CONNECTION_CLOSE);
            }


            // TODO convert AMQP 1-0 node properties to queue attributes

            queue = getAddressSpace().createMessageSource(Queue.class, attributes);
        }
        catch (AccessControlException e)
        {
            Error error = new Error();
            error.setCondition(AmqpError.UNAUTHORIZED_ACCESS);
            error.setDescription(e.getMessage());

            _connection.close(error);
        }
        catch (QueueExistsException e)
        {
            _logger.error("A temporary queue was created with a name which collided with an existing queue name");
            throw new ConnectionScopedRuntimeException(e);
        }

        return queue;
    }

    ServerTransaction getTransaction(Binary transactionId)
    {
        // TODO should treat invalid id differently to null
        ServerTransaction transaction = _openTransactions.get(binaryToInteger(transactionId));
        if(transaction == null)
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

        if(txnId.getLength() > 4)
            throw new IllegalArgumentException();

        int id = 0;
        byte[] data = txnId.getArray();
        for(int i = 0; i < txnId.getLength(); i++)
        {
            id <<= 8;
            id += ((int)data[i+txnId.getArrayOffset()] & 0xff);
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
    public UUID getId()
    {
        return _id;
    }

    @Override
    public AMQPConnection<?> getAMQPConnection()
    {
        return _connection;
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
        }
    }


    @Override
    public void close(AMQConstant cause, String message)
    {
        performCloseTasks();
        final End end = new End();
        final Error theError = new Error();
        theError.setDescription(message);
        theError.setCondition(ConnectionError.CONNECTION_FORCED);
        end.setError(theError);
        end(end);
    }

    @Override
    public void transportStateChanged()
    {
        for(SendingLink_1_0 link : _sendingLinks)
        {
            ConsumerTarget_1_0 target = link.getConsumerTarget();
            target.flowStateChanged();


        }


    }

    @Override
    public LogSubject getLogSubject()
    {
        return this;
    }

    @Override
    public void block(Queue<?> queue)
    {
        // TODO - required for AMQSessionModel / producer side flow control
    }

    @Override
    public void unblock(Queue<?> queue)
    {
        // TODO - required for AMQSessionModel / producer side flow control
    }

    @Override
    public void block()
    {
        // TODO - required for AMQSessionModel / producer side flow control
    }

    @Override
    public void unblock()
    {
        // TODO - required for AMQSessionModel / producer side flow control
    }

    @Override
    public boolean getBlocking()
    {
        // TODO
        return false;
    }

    @Override
    public Object getConnectionReference()
    {
        return getConnection().getReference();
    }

    @Override
    public int getUnacknowledgedMessageCount()
    {
        // TODO
        return 0;
    }

    @Override
    public Long getTxnCount()
    {
        // TODO
        return 0l;
    }

    @Override
    public Long getTxnStart()
    {
        // TODO
        return 0l;
    }

    @Override
    public Long getTxnCommits()
    {
        // TODO
        return 0l;
    }

    @Override
    public Long getTxnRejects()
    {
        // TODO
        return 0l;
    }

    @Override
    public int getChannelId()
    {
        return _sendingChannel;
    }

    @Override
    public int getConsumerCount()
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

    @Override
    public int compareTo(AMQSessionModel o)
    {
        return getId().compareTo(o.getId());
    }

    public AMQPConnection_1_0 getConnection()
    {
        return _connection;
    }

    @Override
    public void addDeleteTask(final Action<? super Session_1_0> task)
    {
        if(!_closed.get())
        {
            _taskList.add(task);
        }
    }

    @Override
    public void removeDeleteTask(final Action<? super Session_1_0> task)
    {
        _taskList.remove(task);
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
        return _securityToken;
    }


    private class SubjectSpecificReceivingLinkListener implements ReceivingLinkListener
    {
        private final ReceivingLinkListener _linkListener;

        public SubjectSpecificReceivingLinkListener(final ReceivingLinkListener linkListener)
        {
            _linkListener = linkListener;
        }

        @Override
        public void messageTransfer(final Transfer xfr)
        {
            AccessController.doPrivileged(new PrivilegedAction<Object>()
            {
                @Override
                public Object run()
                {
                    _linkListener.messageTransfer(xfr);
                    return null;
                }
            }, _accessControllerContext);
        }

        @Override
        public void remoteDetached(final LinkEndpoint endpoint, final Detach detach)
        {
            AccessController.doPrivileged(new PrivilegedAction<Object>()
            {
                @Override
                public Object run()
                {
                    _linkListener.remoteDetached(endpoint, detach);
                    return null;
                }
            }, _accessControllerContext);
        }
    }

    private class SubjectSpecificSendingLinkListener implements SendingLinkListener
    {
        private final SendingLink_1_0 _previousLink;

        public SubjectSpecificSendingLinkListener(final SendingLink_1_0 previousLink)
        {
            _previousLink = previousLink;
        }

        @Override
        public void flowStateChanged()
        {
            AccessController.doPrivileged(new PrivilegedAction<Object>()
            {
                @Override
                public Object run()
                {
                    _previousLink.flowStateChanged();
                    return null;
                }
            }, _accessControllerContext);
        }

        @Override
        public void remoteDetached(final LinkEndpoint endpoint, final Detach detach)
        {
            AccessController.doPrivileged(new PrivilegedAction<Object>()
            {
                @Override
                public Object run()
                {
                    _previousLink.remoteDetached(endpoint, detach);
                    return null;
                }
            }, _accessControllerContext);
        }
    }


    @Override
    public Collection<Consumer<?>> getConsumers()
    {
        return Collections.unmodifiableCollection(_consumers);
    }

    @Override
    public void addConsumerListener(final ConsumerListener listener)
    {
        _consumerListeners.add(listener);
    }

    @Override
    public void removeConsumerListener(final ConsumerListener listener)
    {
        _consumerListeners.remove(listener);
    }

    @Override
    public void setModelObject(final Session<?> session)
    {
        _modelObject = session;
    }

    @Override
    public Session<?> getModelObject()
    {
        return _modelObject;
    }

    @Override
    public long getTransactionStartTime()
    {
        return 0L;
    }

    @Override
    public long getTransactionUpdateTime()
    {
        return 0L;
    }

    @Override
    public boolean processPending()
    {
        if (!getAMQPConnection().isIOThread())
        {
            return false;
        }

        boolean consumerListNeedsRefreshing;
        if(_consumersWithPendingWork.isEmpty())
        {
            for(SendingLink_1_0 link : _sendingLinks)
            {
                _consumersWithPendingWork.add(link.getConsumerTarget());
            }
            consumerListNeedsRefreshing = false;
        }
        else
        {
            consumerListNeedsRefreshing = true;
        }

        // QPID-7447: prevent unnecessary allocation of empty iterator
        Iterator<ConsumerTarget_1_0> iter = _consumersWithPendingWork.isEmpty() ? Collections.<ConsumerTarget_1_0>emptyIterator() : _consumersWithPendingWork.iterator();
        boolean consumerHasMoreWork = false;
        while(iter.hasNext())
        {
            final ConsumerTarget target = iter.next();
            iter.remove();
            if(target.hasPendingWork())
            {
                consumerHasMoreWork = true;
                target.processPending();
                break;
            }
        }

        return consumerHasMoreWork || consumerListNeedsRefreshing;
    }

    @Override
    public void addTicker(final Ticker ticker)
    {
        getConnection().getAggregateTicker().addTicker(ticker);
        // trigger a wakeup to ensure the ticker will be taken into account
        getAMQPConnection().notifyWork();
    }

    @Override
    public void removeTicker(final Ticker ticker)
    {
        getConnection().getAggregateTicker().removeTicker(ticker);
    }

    @Override
    public void notifyConsumerTargetCurrentStates()
    {
        for(SendingLink_1_0 link : _sendingLinks)
        {
            ConsumerTarget_1_0 consumerTarget = link.getConsumerTarget();
            if(!consumerTarget.isPullOnly())
            {
                consumerTarget.notifyCurrentState();
            }
        }
    }

    @Override
    public void ensureConsumersNoticedStateChange()
    {
        for(SendingLink_1_0 link : _sendingLinks)
        {
            ConsumerTarget_1_0 consumerTarget = link.getConsumerTarget();
            try
            {
                consumerTarget.getSendLock();
            }
            finally
            {
                consumerTarget.releaseSendLock();
            }
        }
    }

    @Override
    public void doTimeoutAction(final String reason)
    {
        getAMQPConnection().closeSessionAsync(this, AMQConstant.RESOURCE_ERROR, reason);
    }

    private void consumerAdded(Consumer<?> consumer)
    {
        for(ConsumerListener l : _consumerListeners)
        {
            l.consumerAdded(consumer);
        }
    }

    private void consumerRemoved(Consumer<?> consumer)
    {
        for(ConsumerListener l : _consumerListeners)
        {
            l.consumerRemoved(consumer);
        }
    }

    private class ConsumerClosedListener extends AbstractConfigurationChangeListener
    {
        @Override
        public void stateChanged(final ConfiguredObject object, final org.apache.qpid.server.model.State oldState, final org.apache.qpid.server.model.State newState)
        {
            if(newState == org.apache.qpid.server.model.State.DELETED)
            {
                consumerRemoved((Consumer<?>)object);
            }
        }
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

}
