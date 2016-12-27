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

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.bytebuffer.QpidByteBuffer;
import org.apache.qpid.common.ServerPropertyNames;
import org.apache.qpid.configuration.CommonProperties;
import org.apache.qpid.server.logging.messages.ConnectionMessages;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.Connection;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.protocol.AMQSessionModel;
import org.apache.qpid.server.protocol.ConnectionClosingTicker;
import org.apache.qpid.server.protocol.v1_0.codec.DescribedTypeConstructorRegistry;
import org.apache.qpid.server.protocol.v1_0.codec.FrameWriter;
import org.apache.qpid.server.protocol.v1_0.codec.ProtocolHandler;
import org.apache.qpid.server.protocol.v1_0.codec.QpidByteBufferUtils;
import org.apache.qpid.server.protocol.v1_0.codec.ValueWriter;
import org.apache.qpid.server.protocol.v1_0.framing.AMQFrame;
import org.apache.qpid.server.protocol.v1_0.framing.FrameHandler;
import org.apache.qpid.server.protocol.v1_0.framing.OversizeFrameException;
import org.apache.qpid.server.protocol.v1_0.framing.SASLFrame;
import org.apache.qpid.server.protocol.v1_0.framing.TransportFrame;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.ErrorCondition;
import org.apache.qpid.server.protocol.v1_0.type.FrameBody;
import org.apache.qpid.server.protocol.v1_0.type.SaslFrameBody;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedShort;
import org.apache.qpid.server.protocol.v1_0.type.codec.AMQPDescribedTypeRegistry;
import org.apache.qpid.server.protocol.v1_0.codec.SectionDecoderRegistry;
import org.apache.qpid.server.protocol.v1_0.type.security.SaslChallenge;
import org.apache.qpid.server.protocol.v1_0.type.security.SaslCode;
import org.apache.qpid.server.protocol.v1_0.type.security.SaslInit;
import org.apache.qpid.server.protocol.v1_0.type.security.SaslMechanisms;
import org.apache.qpid.server.protocol.v1_0.type.security.SaslOutcome;
import org.apache.qpid.server.protocol.v1_0.type.security.SaslResponse;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Close;
import org.apache.qpid.server.protocol.v1_0.type.transport.ConnectionError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Disposition;
import org.apache.qpid.server.protocol.v1_0.type.transport.End;
import org.apache.qpid.server.protocol.v1_0.type.transport.Error;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.Open;
import org.apache.qpid.server.protocol.v1_0.type.transport.Transfer;
import org.apache.qpid.server.security.SubjectCreator;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.SubjectAuthenticationResult;
import org.apache.qpid.server.security.auth.manager.AnonymousAuthenticationManager;
import org.apache.qpid.server.security.auth.manager.ExternalAuthenticationManagerImpl;
import org.apache.qpid.server.security.auth.sasl.SaslNegotiator;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.server.transport.AbstractAMQPConnection;
import org.apache.qpid.server.transport.AggregateTicker;
import org.apache.qpid.server.transport.ProtocolEngine;
import org.apache.qpid.server.transport.ServerNetworkConnection;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.server.util.ServerScopedRuntimeException;
import org.apache.qpid.server.virtualhost.VirtualHostUnavailableException;
import org.apache.qpid.transport.ByteBufferSender;
import org.apache.qpid.transport.util.Functions;

public class AMQPConnection_1_0 extends AbstractAMQPConnection<AMQPConnection_1_0, ConnectionHandler>
        implements FrameOutputHandler, DescribedTypeConstructorRegistry.Source,
                   ValueWriter.Registry.Source,
                   SASLEndpoint,
                   ConnectionHandler
{

    private static Logger LOGGER = LoggerFactory.getLogger(AMQPConnection_1_0.class);
    private static final Logger FRAME_LOGGER = LoggerFactory.getLogger("org.apache.qpid.server.protocol.frame");

    private final AtomicBoolean _stateChanged = new AtomicBoolean();
    private final AtomicReference<Action<ProtocolEngine>> _workListener = new AtomicReference<>();


    private static final byte[] SASL_HEADER = new byte[]
            {
                    (byte) 'A',
                    (byte) 'M',
                    (byte) 'Q',
                    (byte) 'P',
                    (byte) 3,
                    (byte) 1,
                    (byte) 0,
                    (byte) 0
            };

    private static final byte[] AMQP_HEADER = new byte[]
            {
                    (byte) 'A',
                    (byte) 'M',
                    (byte) 'Q',
                    (byte) 'P',
                    (byte) 0,
                    (byte) 1,
                    (byte) 0,
                    (byte) 0
            };

    private static final Symbol ANONYMOUS_RELAY = Symbol.valueOf("ANONYMOUS-RELAY");

    private FrameWriter _frameWriter;
    private ProtocolHandler _frameHandler;
    private volatile boolean _transportBlockedForWriting;
    private volatile SubjectAuthenticationResult _successfulAuthenticationResult;
    private boolean _blocking;
    private final Object _blockingLock = new Object();
    private List<Symbol> _offeredCapabilities;

    private enum FrameReceivingState
    {
        AMQP_OR_SASL_HEADER,
        SASL_INIT_ONLY,
        SASL_RESPONSE_ONLY,
        AMQP_HEADER,
        OPEN_ONLY,
        ANY_FRAME,
        CLOSED
    }

    private volatile FrameReceivingState _frameReceivingState = FrameReceivingState.AMQP_OR_SASL_HEADER;

    private static final short CONNECTION_CONTROL_CHANNEL = (short) 0;
    private static final QpidByteBuffer EMPTY_BYTE_BUFFER = QpidByteBuffer.wrap(new byte[0]);

    private static final int DEFAULT_CHANNEL_MAX = Math.min(Integer.getInteger("amqp.channel_max", 255), 0xFFFF);

    private AmqpPort<?> _port;
    private SubjectCreator _subjectCreator;

    private int _channelMax = DEFAULT_CHANNEL_MAX;
    private int _maxFrameSize = 4096;
    private String _remoteContainerId;

    private SocketAddress _remoteAddress;

    // positioned by the *outgoing* channel
    private Session_1_0[] _sendingSessions;

    // positioned by the *incoming* channel
    private Session_1_0[] _receivingSessions;
    private boolean _closedForInput;
    private boolean _closedForOutput;

    private long _idleTimeout;

    private ConnectionState _connectionState = ConnectionState.UNOPENED;

    private AMQPDescribedTypeRegistry _describedTypeRegistry = AMQPDescribedTypeRegistry.newInstance()
                                                                                        .registerTransportLayer()
                                                                                        .registerMessagingLayer()
                                                                                        .registerTransactionLayer()
                                                                                        .registerSecurityLayer();


    private Map _properties;
    private boolean _saslComplete;

    private SaslNegotiator _saslNegotiator;
    private String _localHostname;
    private long _desiredIdleTimeout;

    private Error _remoteError;

    private static final long MINIMUM_SUPPORTED_IDLE_TIMEOUT = 1000L;

    private Map _remoteProperties;

    private final AtomicBoolean _orderlyClose = new AtomicBoolean(false);

    private final Collection<Session_1_0>
            _sessions = Collections.synchronizedCollection(new ArrayList<Session_1_0>());

    private final Object _reference = new Object();

    private final Queue<Action<? super ConnectionHandler>> _asyncTaskList =
            new ConcurrentLinkedQueue<>();

    private boolean _closedOnOpen;

    private final Set<AMQSessionModel<?,?>> _sessionsWithWork =
            Collections.newSetFromMap(new ConcurrentHashMap<AMQSessionModel<?,?>, Boolean>());

    AMQPConnection_1_0(final Broker<?> broker,
                       final ServerNetworkConnection network,
                       AmqpPort<?> port,
                       Transport transport,
                       long id,
                       final AggregateTicker aggregateTicker)
    {
        super(broker, network, port, transport, Protocol.AMQP_1_0, id, aggregateTicker);

        _subjectCreator = port.getAuthenticationProvider().getSubjectCreator(transport.isSecure());

        _port = port;

        Map<Symbol, Object> serverProperties = new LinkedHashMap<>();
        serverProperties.put(Symbol.valueOf(ServerPropertyNames.PRODUCT), CommonProperties.getProductName());
        serverProperties.put(Symbol.valueOf(ServerPropertyNames.VERSION), CommonProperties.getReleaseVersion());
        serverProperties.put(Symbol.valueOf(ServerPropertyNames.QPID_BUILD), CommonProperties.getBuildVersion());
        serverProperties.put(Symbol.valueOf(ServerPropertyNames.QPID_INSTANCE_NAME), broker.getName());

        setProperties(serverProperties);

        List<Symbol> offeredCapabilities = new ArrayList<>();
        offeredCapabilities.add(ANONYMOUS_RELAY);
        setOfferedCapabilities(offeredCapabilities);

        setRemoteAddress(network.getRemoteAddress());

        setDesiredIdleTimeout(1000L * broker.getConnection_heartBeatDelay());

        _frameWriter = new FrameWriter(getDescribedTypeRegistry(), getSender());
    }


    private void setUserPrincipal(final Principal user)
    {
        setSubject(_subjectCreator.createSubjectWithGroups(user));
    }

    private long getDesiredIdleTimeout()
    {
        return _desiredIdleTimeout;
    }

    public void receiveAttach(final short channel, final Attach attach)
    {
        assertState(FrameReceivingState.ANY_FRAME);
        final Session_1_0 session = getSession(channel);
        if (session != null)
        {
            AccessController.doPrivileged(new PrivilegedAction<Object>()
            {
                @Override
                public Object run()
                {
                    session.receiveAttach(attach);
                    return null;
                }
            }, session.getAccessControllerContext());
        }
        else
        {
            closeConnection(AmqpError.INVALID_FIELD, "Channel " + channel + " is not associated with a session");
        }
    }

    public void receive(final short channel, final Object frame)
    {
        FRAME_LOGGER.debug("RECV[{}|{}] : {}", _remoteAddress, channel, frame);
        if (frame instanceof FrameBody)
        {
            ((FrameBody) frame).invoke(channel, this);
        }
        else if (frame instanceof SaslFrameBody)
        {
            ((SaslFrameBody) frame).invoke(channel, this);
        }
    }

    private void closeSaslWithFailure()
    {
        _saslComplete = true;
        disposeSaslNegotiator();
        _frameReceivingState = FrameReceivingState.CLOSED;
        setClosedForInput(true);
        addCloseTicker();
    }

    private void disposeSaslNegotiator()
    {
        _saslNegotiator.dispose();
        _saslNegotiator = null;
    }

    public void receiveSaslChallenge(final SaslChallenge saslChallenge)
    {
        LOGGER.info("{} : Unexpected frame sasl-challenge", getLogSubject());
        closeSaslWithFailure();
    }

    @Override
    public void receiveClose(final short channel, final Close close)
    {
        assertState(FrameReceivingState.ANY_FRAME);
        _frameReceivingState = FrameReceivingState.CLOSED;
        setClosedForInput(true);
        closeReceived();
        switch (_connectionState)
        {
            case UNOPENED:
            case AWAITING_OPEN:
                closeConnection(ConnectionError.CONNECTION_FORCED,
                                "Connection close sent before connection was opened");
                break;
            case OPEN:
                _connectionState = ConnectionState.CLOSE_RECEIVED;
                if(close.getError() != null)
                {
                    final Error error = close.getError();
                    ErrorCondition condition = error.getCondition();
                    Symbol errorCondition = condition == null ? null : condition.getValue();
                    LOGGER.info("{} : Connection closed with error : {} - {}", getLogSubject(),
                                errorCondition, close.getError().getDescription());
                }
                sendClose(new Close());
                _connectionState = ConnectionState.CLOSED;
                _orderlyClose.set(true);
                addCloseTicker();
                break;
            case CLOSE_SENT:
                _connectionState = ConnectionState.CLOSED;
                _orderlyClose.set(true);

            default:
        }
        _remoteError = close.getError();
    }

    private void closeReceived()
    {
        Collection<Session_1_0> sessions = new ArrayList<>(_sessions);

        for (final Session_1_0 session : sessions)
        {
            AccessController.doPrivileged(new PrivilegedAction<Object>()
            {
                @Override
                public Object run()
                {
                    session.remoteEnd(new End());
                    return null;
                }
            }, session.getAccessControllerContext());
        }
    }

    private void setClosedForInput(final boolean closed)
    {
        _closedForInput = closed;
    }

    public void receiveSaslMechanisms(final SaslMechanisms saslMechanisms)
    {
        LOGGER.info("{} : Unexpected frame sasl-mechanisms", getLogSubject());
        closeSaslWithFailure();
    }

    public void receiveSaslResponse(final SaslResponse saslResponse)
    {
        final Binary responseBinary = saslResponse.getResponse();
        byte[] response = responseBinary == null ? new byte[0] : responseBinary.getArray();

        assertState(FrameReceivingState.SASL_RESPONSE_ONLY);

        processSaslResponse(response);
    }

    public AMQPDescribedTypeRegistry getDescribedTypeRegistry()
    {
        return _describedTypeRegistry;
    }

    public SectionDecoderRegistry getSectionDecoderRegistry()
    {
        return _describedTypeRegistry.getSectionDecoderRegistry();
    }




    private boolean closedForOutput()
    {
        return _closedForOutput;
    }

    public boolean isClosed()
    {
        return _connectionState == ConnectionState.CLOSED
               || _connectionState == ConnectionState.CLOSE_RECEIVED;
    }

    public boolean closedForInput()
    {
        return _closedForInput;
    }

    void sessionEnded(final Session_1_0 session)
    {
        if (!_closedOnOpen)
        {
            _sessions.remove(session);
            sessionRemoved(session);
        }
    }

    private void inputClosed()
    {
        if (!_closedForInput)
        {
            _closedForInput = true;
            FRAME_LOGGER.debug("RECV[{}] : {}", _remoteAddress, "Underlying connection closed");
            switch (_connectionState)
            {
                case UNOPENED:
                case AWAITING_OPEN:
                case CLOSE_SENT:
                    _connectionState = ConnectionState.CLOSED;
                    closeSender();
                    break;
                case OPEN:
                    _connectionState = ConnectionState.CLOSE_RECEIVED;
                case CLOSED:
                    // already sent our close - too late to do anything more
                    break;
                default:
            }
            closeReceived();
        }
    }

    private void closeSender()
    {
        setClosedForOutput(true);
    }

    String getRemoteContainerId()
    {
        return _remoteContainerId;
    }

    private void setDesiredIdleTimeout(final long desiredIdleTimeout)
    {
        _desiredIdleTimeout = desiredIdleTimeout;
    }

    public boolean isOpen()
    {
        return _connectionState == ConnectionState.OPEN;
    }

    void sendEnd(final short channel, final End end, final boolean remove)
    {
        sendFrame(channel, end);
        if (remove)
        {
            _sendingSessions[channel] = null;
        }
    }

    public void receiveSaslOutcome(final SaslOutcome saslOutcome)
    {
        LOGGER.info("{} : Unexpected frame sasl-outcome", getLogSubject());
        closeSaslWithFailure();
    }

    public void receiveEnd(final short channel, final End end)
    {

        assertState(FrameReceivingState.ANY_FRAME);
        final Session_1_0 session = getSession(channel);
        if (session != null)
        {
            AccessController.doPrivileged(new PrivilegedAction<Object>()
            {
                @Override
                public Object run()
                {
                    _receivingSessions[channel] = null;

                    session.receiveEnd(end);
                    return null;
                }
            }, session.getAccessControllerContext());
        }
        else
        {
            closeConnectionWithInvalidChannel(channel, end);
        }
    }

    private void closeConnectionWithInvalidChannel(final short channel, final FrameBody frame)
    {
        closeConnection(AmqpError.INVALID_FIELD, String.format("%s frame received on channel %d which is not mapped", frame.getClass().getSimpleName().toLowerCase(), channel));
    }

    public void receiveDisposition(final short channel,
                                   final Disposition disposition)
    {
        assertState(FrameReceivingState.ANY_FRAME);
        final Session_1_0 session = getSession(channel);
        if (session != null)
        {
            AccessController.doPrivileged(new PrivilegedAction<Object>()
            {
                @Override
                public Object run()
                {
                    session.receiveDisposition(disposition);
                    return null;
                }
            }, session.getAccessControllerContext());
        }
        else
        {
            closeConnectionWithInvalidChannel(channel, disposition);
        }

    }

    public void receiveBegin(final short channel, final Begin begin)
    {

        assertState(FrameReceivingState.ANY_FRAME);
        short myChannelId;
        if (begin.getRemoteChannel() != null)
        {
            closeConnection(ConnectionError.FRAMING_ERROR,
                            "BEGIN received on channel "
                            + channel
                            + " with given remote-channel "
                            + begin.getRemoteChannel()
                            + ". Since the broker does not spontaneously start channels, this must be an error.");

        }
        else // Peer requesting session creation
        {

            if (_receivingSessions[channel] == null)
            {
                myChannelId = getFirstFreeChannel();
                if (myChannelId == -1)
                {

                    closeConnection(ConnectionError.FRAMING_ERROR,
                                    "BEGIN received on channel "
                                    + channel
                                    + ". There are no free channels for the broker to responsd on.");

                }
                Session_1_0 session = new Session_1_0(this, begin);

                _receivingSessions[channel] = session;
                _sendingSessions[myChannelId] = session;

                Begin beginToSend = new Begin();

                session.setReceivingChannel(channel);
                session.setSendingChannel(myChannelId);
                beginToSend.setRemoteChannel(UnsignedShort.valueOf(channel));
                beginToSend.setNextOutgoingId(session.getNextOutgoingId());
                beginToSend.setOutgoingWindow(session.getOutgoingWindowSize());
                beginToSend.setIncomingWindow(session.getIncomingWindowSize());
                sendFrame(myChannelId, beginToSend);

                _sessions.add(session);
                sessionAdded(session);

            }
            else
            {
                closeConnection(ConnectionError.FRAMING_ERROR,
                                "BEGIN received on channel " + channel + " which is already in use.");
            }

        }

    }

    private short getFirstFreeChannel()
    {
        for (int i = 0; i <= _channelMax; i++)
        {
            if (_sendingSessions[i] == null)
            {
                return (short) i;
            }
        }
        return -1;
    }

    public void handleError(final Error error)
    {
        if (!closedForOutput())
        {
            Close close = new Close();
            close.setError(error);
            sendFrame((short) 0, close);

            setClosedForOutput(true);
        }

    }

    public void receiveTransfer(final short channel, final Transfer transfer)
    {
        assertState(FrameReceivingState.ANY_FRAME);
        final Session_1_0 session = getSession(channel);
        if (session != null)
        {
            AccessController.doPrivileged(new PrivilegedAction<Object>()
            {
                @Override
                public Object run()
                {
                    session.receiveTransfer(transfer);
                    return null;
                }
            }, session.getAccessControllerContext());
        }
        else
        {
            closeConnectionWithInvalidChannel(channel, transfer);
        }
    }

    public void receiveFlow(final short channel, final Flow flow)
    {
        assertState(FrameReceivingState.ANY_FRAME);
        final Session_1_0 session = getSession(channel);
        if (session != null)
        {
            AccessController.doPrivileged(new PrivilegedAction<Object>()
            {
                @Override
                public Object run()
                {
                    session.receiveFlow(flow);
                    return null;
                }
            }, session.getAccessControllerContext());
        }
        else
        {
            closeConnectionWithInvalidChannel(channel, flow);
        }

    }

    public void receiveOpen(final short channel, final Open open)
    {
        assertState(FrameReceivingState.OPEN_ONLY);
        _frameReceivingState = FrameReceivingState.ANY_FRAME;
        _channelMax = open.getChannelMax() == null ? _channelMax
                : open.getChannelMax().intValue() < _channelMax
                        ? open.getChannelMax().intValue()
                        : _channelMax;
        if (_receivingSessions == null)
        {
            _receivingSessions = new Session_1_0[_channelMax + 1];
            _sendingSessions = new Session_1_0[_channelMax + 1];
        }
        _maxFrameSize = open.getMaxFrameSize() == null
                ? getBroker().getNetworkBufferSize()
                : Math.min(open.getMaxFrameSize().intValue(), getBroker().getNetworkBufferSize());
        _remoteContainerId = open.getContainerId();
        _localHostname = open.getHostname();
        if (open.getIdleTimeOut() != null)
        {
            _idleTimeout = open.getIdleTimeOut().longValue();
        }
        _remoteProperties = open.getProperties();
        if (_remoteProperties != null)
        {
            if (_remoteProperties.containsKey(Symbol.valueOf("product")))
            {
                setClientProduct(_remoteProperties.get(Symbol.valueOf("product")).toString());
            }
            if (_remoteProperties.containsKey(Symbol.valueOf("version")))
            {
                setClientVersion(_remoteProperties.get(Symbol.valueOf("version")).toString());
            }
            setClientId(_remoteContainerId);
        }
        if (_idleTimeout != 0L && _idleTimeout < MINIMUM_SUPPORTED_IDLE_TIMEOUT)
        {
            closeConnection(ConnectionError.CONNECTION_FORCED,
                            "Requested idle timeout of "
                            + _idleTimeout
                            + " is too low. The minimum supported timeout is"
                            + MINIMUM_SUPPORTED_IDLE_TIMEOUT);
            _closedOnOpen = true;
        }
        else
        {
            long desiredIdleTimeout = getDesiredIdleTimeout();
            initialiseHeartbeating(_idleTimeout / 2L, desiredIdleTimeout);
            final NamedAddressSpace addressSpace = ((AmqpPort) _port).getAddressSpace(_localHostname);
            if (addressSpace == null)
            {
                closeWithError(AmqpError.NOT_FOUND, "Unknown hostname in connection open: '" + _localHostname + "'");
            }
            else
            {
                if (!addressSpace.isActive())
                {
                    _closedOnOpen = true;

                    final Error err = new Error();
                    err.setCondition(AmqpError.NOT_FOUND);
                    populateConnectionRedirect(addressSpace, err);

                    closeConnection(err);

                    _closedOnOpen = true;

                }
                else
                {
                    if (AuthenticatedPrincipal.getOptionalAuthenticatedPrincipalFromSubject(getSubject()) == null)
                    {
                        closeWithError(AmqpError.NOT_ALLOWED, "Connection has not been authenticated");
                    }
                    else
                    {
                        try
                        {
                            setAddressSpace(addressSpace);
                        }
                        catch (VirtualHostUnavailableException e)
                        {
                            closeWithError(AmqpError.NOT_ALLOWED, e.getMessage());
                        }
                    }
                }
            }
        }
        switch (_connectionState)
        {
            case UNOPENED:
                sendOpen(_channelMax, _maxFrameSize);
            case AWAITING_OPEN:
                _connectionState = ConnectionState.OPEN;
            default:
                // TODO bad stuff (connection already open)

        }

    }

    private void populateConnectionRedirect(final NamedAddressSpace addressSpace, final Error err)
    {
        final String redirectHost = addressSpace.getRedirectHost(((AmqpPort) _port));

        if(redirectHost == null)
        {
            err.setDescription("Virtual host '" + _localHostname + "' is not active");
        }
        else
        {
            String networkHost;
            int port;
            if(redirectHost.matches("\\[[0-9a-f:]+\\](:[0-9]+)?"))
            {
                // IPv6 case
                networkHost = redirectHost.substring(1, redirectHost.indexOf("]"));
                if(redirectHost.contains("]:"))
                {
                    port = Integer.parseInt(redirectHost.substring(redirectHost.indexOf("]")+2));
                }
                else
                {
                    port = -1;
                }
            }
            else
            {
                if(redirectHost.contains(":"))
                {
                    networkHost = redirectHost.substring(0, redirectHost.lastIndexOf(":"));
                    try
                    {
                        String portString = redirectHost.substring(redirectHost.lastIndexOf(":")+1);
                        port = Integer.parseInt(portString);
                    }
                    catch (NumberFormatException e)
                    {
                        port = -1;
                    }
                }
                else
                {
                    networkHost = redirectHost;
                    port = -1;
                }
            }
            final Map<Symbol, Object> infoMap = new HashMap<>();
            infoMap.put(Symbol.valueOf("network-host"), networkHost);
            if(port > 0)
            {
                infoMap.put(Symbol.valueOf("port"), UnsignedInteger.valueOf(port));
            }
            err.setInfo(infoMap);
        }
    }

    public void receiveDetach(final short channel, final Detach detach)
    {
        assertState(FrameReceivingState.ANY_FRAME);
        final Session_1_0 session = getSession(channel);
        if (session != null)
        {
            AccessController.doPrivileged(new PrivilegedAction<Object>()
            {
                @Override
                public Object run()
                {
                    session.receiveDetach(detach);
                    return null;
                }
            }, session.getAccessControllerContext());
        }
        else
        {
            closeConnectionWithInvalidChannel(channel, detach);
        }
    }

    private void transportStateChanged()
    {
        for (Session_1_0 session : _sessions)
        {
            session.transportStateChanged();
        }
    }

    public void close(final Error error)
    {
        closeConnection(error);
    }

    private void setRemoteAddress(final SocketAddress remoteAddress)
    {
        _remoteAddress = remoteAddress;
    }

    public void setProperties(final Map<Symbol, Object> properties)
    {
        _properties = properties;
    }

    public void setOfferedCapabilities(final List<Symbol> offeredCapabilities)
    {
        _offeredCapabilities = offeredCapabilities;
    }


    private void setClosedForOutput(final boolean closed)
    {
        _closedForOutput = closed;
    }

    public void receiveSaslInit(final SaslInit saslInit)
    {
        assertState(FrameReceivingState.SASL_INIT_ONLY);
        String mechanism = saslInit.getMechanism() == null ? null : saslInit.getMechanism().toString();
        final Binary initialResponse = saslInit.getInitialResponse();
        byte[] response = initialResponse == null ? new byte[0] : initialResponse.getArray();

        _saslNegotiator = _subjectCreator.createSaslNegotiator(mechanism, this);
        processSaslResponse(response);
    }

    private void processSaslResponse(final byte[] response)
    {
        byte[] challenge = null;
        SubjectAuthenticationResult authenticationResult = _successfulAuthenticationResult;
        if (authenticationResult == null)
        {
            authenticationResult = _subjectCreator.authenticate(_saslNegotiator, response != null ? response : new byte[0]);
            challenge = authenticationResult.getChallenge();
        }

        if (authenticationResult.getStatus() == AuthenticationResult.AuthenticationStatus.SUCCESS)
        {
            _successfulAuthenticationResult = authenticationResult;
            if (challenge == null || challenge.length == 0)
            {
                setSubject(_successfulAuthenticationResult.getSubject());
                SaslOutcome outcome = new SaslOutcome();
                outcome.setCode(SaslCode.OK);
                send(new SASLFrame(outcome), null);
                _saslComplete = true;
                _frameReceivingState = FrameReceivingState.AMQP_HEADER;
                disposeSaslNegotiator();
            }
            else
            {
                continueSaslNegotiation(challenge);
            }
        }
        else if(authenticationResult.getStatus() == AuthenticationResult.AuthenticationStatus.CONTINUE)
        {
            continueSaslNegotiation(challenge);
        }
        else
        {
            handleSaslError();
        }
    }

    private void continueSaslNegotiation(final byte[] challenge)
    {
        SaslChallenge challengeBody = new SaslChallenge();
        challengeBody.setChallenge(new Binary(challenge));
        send(new SASLFrame(challengeBody), null);

        _frameReceivingState = FrameReceivingState.SASL_RESPONSE_ONLY;
    }

    private void handleSaslError()
    {
        SaslOutcome outcome = new SaslOutcome();
        outcome.setCode(SaslCode.AUTH);
        send(new SASLFrame(outcome), null);
        _saslComplete = true;
        closeSaslWithFailure();
    }

    public int getMaxFrameSize()
    {
        return _maxFrameSize;
    }

    Object getReference()
    {
        return _reference;
    }

    private void endpointClosed()
    {
        try
        {
            performDeleteTasks();
            closeReceived();
        }
        finally
        {
            NamedAddressSpace virtualHost = getAddressSpace();
            if (virtualHost != null)
            {
                virtualHost.deregisterConnection(this);
            }
        }
    }

    private void closeConnection(ErrorCondition errorCondition, String description)
    {
        closeConnection(new Error(errorCondition, description));
    }

    private void closeConnection(final Error error)
    {
        Close close = new Close();
        close.setError(error);
        switch (_connectionState)
        {
            case UNOPENED:
                sendOpen(0, 0);
                sendClose(close);
                _connectionState = ConnectionState.CLOSED;
                break;
            case AWAITING_OPEN:
            case OPEN:
                sendClose(close);
                _connectionState = ConnectionState.CLOSE_SENT;
                addCloseTicker();
            case CLOSE_SENT:
            case CLOSED:
                // already sent our close - too late to do anything more
                break;
            default:
                throw new ServerScopedRuntimeException("Unknown state: " + _connectionState);
        }
    }

    int sendFrame(final short channel, final FrameBody body, final List<QpidByteBuffer> payload)
    {
        if (!_closedForOutput)
        {
            ValueWriter<FrameBody> writer = _describedTypeRegistry.getValueWriter(body);
            if (payload == null)
            {
                send(AMQFrame.createAMQFrame(channel, body));
                return 0;
            }
            else
            {
                int size = writer.getEncodedSize();
                int maxPayloadSize = _maxFrameSize - (size + 9);
                long payloadLength = QpidByteBufferUtils.remaining(payload);
                if(payloadLength <= maxPayloadSize)
                {
                    send(AMQFrame.createAMQFrame(channel, body, payload));
                    return (int)payloadLength;
                }
                else
                {
                    ((Transfer) body).setMore(Boolean.TRUE);

                    writer = _describedTypeRegistry.getValueWriter(body);
                    size = writer.getEncodedSize();
                    maxPayloadSize = _maxFrameSize - (size + 9);

                    List<QpidByteBuffer> payloadDup = new ArrayList<>(payload.size());
                    int payloadSize = 0;
                    for(QpidByteBuffer buf : payload)
                    {
                        if(payloadSize + buf.remaining() < maxPayloadSize)
                        {
                            payloadSize += buf.remaining();
                            payloadDup.add(buf.duplicate());
                        }
                        else
                        {
                            QpidByteBuffer dup = buf.slice();
                            dup.limit(maxPayloadSize-payloadSize);
                            payloadDup.add(dup);
                            break;
                        }
                    }

                    QpidByteBufferUtils.skip(payload, maxPayloadSize);
                    send(AMQFrame.createAMQFrame(channel, body, payloadDup));
                    for(QpidByteBuffer buf : payloadDup)
                    {
                        buf.dispose();
                    }

                    return maxPayloadSize;
                }
            }
        }
        else
        {
            return -1;
        }
    }

    void sendFrame(final short channel, final FrameBody body)
    {
        sendFrame(channel, body, null);
    }

    public ByteBufferSender getSender()
    {
        return getNetwork().getSender();
    }

    @Override
    public void writerIdle()
    {
        send(TransportFrame.createAMQFrame((short)0,null));
    }

    @Override
    public void readerIdle()
    {
        AccessController.doPrivileged(new PrivilegedAction<Object>()
        {
            @Override
            public Object run()
            {
                getEventLogger().message(ConnectionMessages.IDLE_CLOSE("", false));
                getNetwork().close();
                return null;
            }
        }, getAccessControllerContext());
    }

    @Override
    public void encryptedTransport()
    {
    }

    public String getAddress()
    {
        return getNetwork().getRemoteAddress().toString();
    }



    public void received(final QpidByteBuffer msg)
    {

        AccessController.doPrivileged(new PrivilegedAction<Object>()
        {
            @Override
            public Object run()
            {
                updateLastReadTime();
                try
                {
                    int remaining;

                    do
                    {
                        remaining = msg.remaining();

                        switch (_frameReceivingState)
                        {
                            case AMQP_OR_SASL_HEADER:
                            case AMQP_HEADER:
                                if (remaining >= 8)
                                {
                                    processProtocolHeader(msg);
                                }
                                break;
                            case OPEN_ONLY:
                            case ANY_FRAME:
                            case SASL_INIT_ONLY:
                            case SASL_RESPONSE_ONLY:
                                _frameHandler.parse(msg);
                                break;
                            case CLOSED:
                                // ignore;
                                break;
                        }


                    }
                    while (msg.remaining() != remaining);
                }
                catch (IllegalArgumentException | IllegalStateException e)
                {
                    throw new ConnectionScopedRuntimeException(e);
                }
                catch (StoreException e)
                {
                    if (getAddressSpace().isActive())
                    {
                        throw new ServerScopedRuntimeException(e);
                    }
                    else
                    {
                        throw new ConnectionScopedRuntimeException(e);
                    }
                }
                return null;
            }
        }, getAccessControllerContext());

    }

    private void processProtocolHeader(final QpidByteBuffer msg)
    {
        if(msg.remaining() >= 8)
        {
            byte[] header = new byte[8];
            msg.get(header);

            final AuthenticationProvider authenticationProvider = getPort().getAuthenticationProvider();
            final SubjectCreator subjectCreator = authenticationProvider.getSubjectCreator(getTransport().isSecure());

            if(Arrays.equals(header, SASL_HEADER))
            {
                if(_saslComplete)
                {
                    throw new ConnectionScopedRuntimeException("SASL Layer header received after SASL already established");
                }

                getSender().send(QpidByteBuffer.wrap(SASL_HEADER));

                SaslMechanisms mechanisms = new SaslMechanisms();
                ArrayList<Symbol> mechanismsList = new ArrayList<>();
                for (String name :  subjectCreator.getMechanisms())
                {
                    mechanismsList.add(Symbol.valueOf(name));
                }
                mechanisms.setSaslServerMechanisms(mechanismsList.toArray(new Symbol[mechanismsList.size()]));
                send(new SASLFrame(mechanisms), null);

                _frameReceivingState = FrameReceivingState.SASL_INIT_ONLY;
                _frameHandler = new FrameHandler(this, true);
            }
            else if(Arrays.equals(header, AMQP_HEADER))
            {
                if(!_saslComplete)
                {
                    final List<String> mechanisms = subjectCreator.getMechanisms();

                    if(mechanisms.contains(ExternalAuthenticationManagerImpl.MECHANISM_NAME) && getNetwork().getPeerPrincipal() != null)
                    {
                        setUserPrincipal(new AuthenticatedPrincipal(getNetwork().getPeerPrincipal()));
                    }
                    else if(mechanisms.contains(AnonymousAuthenticationManager.MECHANISM_NAME))
                    {
                        setUserPrincipal(new AuthenticatedPrincipal(((AnonymousAuthenticationManager) authenticationProvider).getAnonymousPrincipal()));
                    }
                    else
                    {
                        LOGGER.warn("{} : attempt to initiate AMQP connection without correctly authenticating", getLogSubject());
                        _connectionState = ConnectionState.CLOSED;
                        getNetwork().close();
                    }

                }
                getSender().send(QpidByteBuffer.wrap(AMQP_HEADER));
                _frameReceivingState = FrameReceivingState.OPEN_ONLY;
                _frameHandler = new FrameHandler(this, false);

            }
            else
            {
                LOGGER.warn("{} : unknown AMQP header {}", getLogSubject(), Functions.str(header));
                _connectionState = ConnectionState.CLOSED;
                getNetwork().close();
            }

        }

    }


    public void closed()
    {
        try
        {
            inputClosed();
        }
        catch(RuntimeException e)
        {
            LOGGER.error("Exception while closing", e);
        }
        finally
        {
            try
            {
                endpointClosed();
            }
            finally
            {
                markTransportClosed();
            }
        }
    }

    public void send(final AMQFrame amqFrame)
    {
        send(amqFrame, null);
    }



    public void send(final AMQFrame amqFrame, ByteBuffer buf)
    {
        updateLastWriteTime();
        FRAME_LOGGER.debug("SEND[{}|{}] : {}",
                           getNetwork().getRemoteAddress(),
                           amqFrame.getChannel(),
                           amqFrame.getFrameBody());

        int size = _frameWriter.send(amqFrame);
        if (size > getMaxFrameSize())
        {
            throw new OversizeFrameException(amqFrame, size);
        }
    }

    public void send(short channel, FrameBody body)
    {
        AMQFrame frame = AMQFrame.createAMQFrame(channel, body);
        send(frame);

    }

    private void addCloseTicker()
    {
        long timeoutTime = System.currentTimeMillis() + getContextValue(Long.class, Connection.CLOSE_RESPONSE_TIMEOUT);

        getAggregateTicker().addTicker(new ConnectionClosingTicker(timeoutTime, getNetwork()));

        // trigger a wakeup to ensure the ticker will be taken into account
        notifyWork();
    }

    @Override
    public boolean isTransportBlockedForWriting()
    {
        return _transportBlockedForWriting;
    }
    @Override
    public void setTransportBlockedForWriting(final boolean blocked)
    {
        if(_transportBlockedForWriting != blocked)
        {
            _transportBlockedForWriting = blocked;
            transportStateChanged();
        }

    }

    @Override
    public Iterator<Runnable> processPendingIterator()
    {
        if (isIOThread())
        {
            return new ProcessPendingIterator();
        }
        else
        {
            return Collections.emptyIterator();
        }
    }

    @Override
    public boolean hasWork()
    {
        return _stateChanged.get();
    }

    @Override
    public void notifyWork()
    {
        _stateChanged.set(true);

        final Action<ProtocolEngine> listener = _workListener.get();
        if(listener != null)
        {
            listener.performAction(this);
        }
    }

    @Override
    public void notifyWork(final AMQSessionModel<?,?> sessionModel)
    {
        _sessionsWithWork.add(sessionModel);
        notifyWork();
    }

    @Override
    public void clearWork()
    {
        _stateChanged.set(false);
    }

    @Override
    public void setWorkListener(final Action<ProtocolEngine> listener)
    {
        _workListener.set(listener);
    }

    public boolean hasSessionWithName(final byte[] name)
    {
        return false;
    }

    @Override
    public void sendConnectionCloseAsync(final CloseReason reason, final String description)
    {

        stopConnection();
        final ErrorCondition cause;
        switch(reason)
        {
            case MANAGEMENT:
                cause = ConnectionError.CONNECTION_FORCED;
                break;
            case TRANSACTION_TIMEOUT:
                cause = AmqpError.RESOURCE_LIMIT_EXCEEDED;
                break;
            default:
                cause = AmqpError.INTERNAL_ERROR;
        }
        Action<ConnectionHandler> action = new Action<ConnectionHandler>()
        {
            @Override
            public void performAction(final ConnectionHandler object)
            {
                closeConnection(cause, description);

            }
        };
        addAsyncTask(action);
    }

    public void closeSessionAsync(final AMQSessionModel<?,?> session,
                                  final CloseReason reason, final String message)
    {
        final ErrorCondition cause;
        switch(reason)
        {
            case MANAGEMENT:
                cause = ConnectionError.CONNECTION_FORCED;
                break;
            case TRANSACTION_TIMEOUT:
                cause = AmqpError.RESOURCE_LIMIT_EXCEEDED;
                break;
            default:
                cause = AmqpError.INTERNAL_ERROR;
        }
        addAsyncTask(new Action<ConnectionHandler>()
        {
            @Override
            public void performAction(final ConnectionHandler object)
            {
                AccessController.doPrivileged(new PrivilegedAction<Void>() {
                    @Override
                    public Void run()
                    {
                        ((Session_1_0)session).close(cause, message);
                        return null;
                    }
                }, ((Session_1_0)session).getAccessControllerContext());
            }
        });

    }

    public void block()
    {
        synchronized (_blockingLock)
        {
            if (!_blocking)
            {
                _blocking = true;
                doOnIOThreadAsync(
                        new Runnable()
                        {
                            @Override
                            public void run()
                            {
                                doBlock();
                            }
                        });
            }
        }
    }

    private void doBlock()
    {
        for(Session_1_0 session : _sessions)
        {
            session.block();
        }
    }

    public String getRemoteContainerName()
    {
        return _remoteContainerId;
    }

    public Collection<? extends Session_1_0> getSessionModels()
    {
        return Collections.unmodifiableCollection(_sessions);
    }

    public void unblock()
    {
        synchronized (_blockingLock)
        {
            if(_blocking)
            {
                _blocking = false;
                doOnIOThreadAsync(
                        new Runnable()
                        {
                            @Override
                            public void run()
                            {
                                doUnblock();
                            }
                        });
            }
        }
    }

    private void doUnblock()
    {
        for(Session_1_0 session : _sessions)
        {
            session.unblock();
        }
    }

    @Override
    public long getSessionCountLimit()
    {
        return _channelMax+1;
    }

    @Override
    public boolean isOrderlyClose()
    {
        return _orderlyClose.get();
    }

    @Override
    protected void addAsyncTask(final Action<? super ConnectionHandler> action)
    {
        _asyncTaskList.add(action);
        notifyWork();
    }


    private void sendOpen(final int channelMax, final int maxFrameSize)
    {
        Open open = new Open();

        if (_receivingSessions == null)
        {
            _receivingSessions = new Session_1_0[channelMax + 1];
            _sendingSessions = new Session_1_0[channelMax + 1];
        }
        if (channelMax < _channelMax)
        {
            _channelMax = channelMax;
        }
        open.setChannelMax(UnsignedShort.valueOf((short) channelMax));
        open.setContainerId(getAddressSpace() == null ? UUID.randomUUID().toString() : getAddressSpace().getId().toString());
        open.setMaxFrameSize(UnsignedInteger.valueOf(maxFrameSize));
        // TODO - should we try to set the hostname based on the connection information?
        // open.setHostname();
        open.setIdleTimeOut(UnsignedInteger.valueOf(_desiredIdleTimeout));

        // set the offered capabilities
        if(_offeredCapabilities != null && !_offeredCapabilities.isEmpty())
        {
            open.setOfferedCapabilities(_offeredCapabilities.toArray(new Symbol[_offeredCapabilities.size()]));
        }

        if (_properties != null)
        {
            open.setProperties(_properties);
        }

        sendFrame(CONNECTION_CONTROL_CHANNEL, open);
    }

    private void closeWithError(final AmqpError amqpError, final String errorDescription)
    {
        final Error err = new Error();
        err.setCondition(amqpError);
        err.setDescription(errorDescription);
        closeConnection(err);
        _closedOnOpen = true;
    }

    private Session_1_0 getSession(final short channel)
    {
        Session_1_0 session = _receivingSessions[channel];
        if (session == null)
        {
            Error error = new Error();
            error.setCondition(ConnectionError.FRAMING_ERROR);
            error.setDescription("Frame received on channel " + channel + " which is not known as a begun session.");
            handleError(error);
        }

        return session;
    }

    private void sendClose(Close closeToSend)
    {
        sendFrame(CONNECTION_CONTROL_CHANNEL, closeToSend);
        closeSender();
    }


    private void assertState(final FrameReceivingState state)
    {
        if(_frameReceivingState != state)
        {
            throw new ConnectionScopedRuntimeException("Unexpected state, client has sent frame in an illegal order.  Required state: " + state + ", actual state: " + _frameReceivingState);
        }
    }


    private class ProcessPendingIterator implements Iterator<Runnable>
    {
        private Iterator<? extends AMQSessionModel<?,?>> _sessionIterator;
        private ProcessPendingIterator()
        {
            _sessionIterator = _sessionsWithWork.iterator();
        }

        @Override
        public boolean hasNext()
        {
            return (!_sessionsWithWork.isEmpty() && !isClosed() && !isConnectionStopped()) || !_asyncTaskList.isEmpty();
        }

        @Override
        public Runnable next()
        {
            if(!_sessionsWithWork.isEmpty())
            {
                if(isClosed() || isConnectionStopped())
                {

                    final Action<? super ConnectionHandler> asyncAction = _asyncTaskList.poll();
                    if(asyncAction != null)
                    {
                        return new Runnable()
                        {
                            @Override
                            public void run()
                            {
                                asyncAction.performAction(AMQPConnection_1_0.this);
                            }
                        };
                    }
                    else
                    {
                        return new Runnable()
                        {
                            @Override
                            public void run()
                            {

                            }
                        };
                    }
                }
                else
                {
                    if (!_sessionIterator.hasNext())
                    {
                        _sessionIterator = _sessionsWithWork.iterator();
                    }
                    final AMQSessionModel<?,?> session = _sessionIterator.next();
                    return new Runnable()
                    {
                        @Override
                        public void run()
                        {
                            _sessionIterator.remove();
                            if (session.processPending())
                            {
                                _sessionsWithWork.add(session);
                            }
                        }
                    };
                }
            }
            else if(!_asyncTaskList.isEmpty())
            {
                final Action<? super ConnectionHandler> asyncAction = _asyncTaskList.poll();
                return new Runnable()
                {
                    @Override
                    public void run()
                    {
                        asyncAction.performAction(AMQPConnection_1_0.this);
                    }
                };
            }
            else
            {
                throw new NoSuchElementException();
            }
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public void initialiseHeartbeating(final long writerDelay, final long readerDelay)
    {
        super.initialiseHeartbeating(writerDelay, readerDelay);
    }
}
