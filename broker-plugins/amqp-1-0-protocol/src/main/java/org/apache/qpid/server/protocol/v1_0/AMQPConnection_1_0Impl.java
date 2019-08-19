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

import static com.google.common.util.concurrent.Futures.allAsList;
import static org.apache.qpid.server.protocol.v1_0.type.extensions.soleconn.SoleConnectionConnectionProperties.SOLE_CONNECTION_ENFORCEMENT_POLICY;

import java.net.SocketAddress;
import java.security.AccessControlContext;
import java.security.AccessControlException;
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
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.logging.messages.ConnectionMessages;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.Connection;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.plugin.ConnectionPropertyEnricher;
import org.apache.qpid.server.protocol.ConnectionClosingTicker;
import org.apache.qpid.server.protocol.v1_0.codec.DescribedTypeConstructorRegistry;
import org.apache.qpid.server.protocol.v1_0.codec.FrameWriter;
import org.apache.qpid.server.protocol.v1_0.codec.ProtocolHandler;
import org.apache.qpid.server.protocol.v1_0.codec.SectionDecoderRegistry;
import org.apache.qpid.server.protocol.v1_0.codec.ValueHandler;
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
import org.apache.qpid.server.protocol.v1_0.type.extensions.soleconn.SoleConnectionConnectionProperties;
import org.apache.qpid.server.protocol.v1_0.type.extensions.soleconn.SoleConnectionDetectionPolicy;
import org.apache.qpid.server.protocol.v1_0.type.extensions.soleconn.SoleConnectionEnforcementPolicy;
import org.apache.qpid.server.protocol.v1_0.type.security.SaslChallenge;
import org.apache.qpid.server.protocol.v1_0.type.security.SaslCode;
import org.apache.qpid.server.protocol.v1_0.type.security.SaslInit;
import org.apache.qpid.server.protocol.v1_0.type.security.SaslMechanisms;
import org.apache.qpid.server.protocol.v1_0.type.security.SaslOutcome;
import org.apache.qpid.server.protocol.v1_0.type.security.SaslResponse;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.ChannelFrameBody;
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
import org.apache.qpid.server.session.AMQPSession;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.transport.AbstractAMQPConnection;
import org.apache.qpid.server.transport.AggregateTicker;
import org.apache.qpid.server.transport.ByteBufferSender;
import org.apache.qpid.server.transport.ProtocolEngine;
import org.apache.qpid.server.transport.ServerNetworkConnection;
import org.apache.qpid.server.transport.util.Functions;
import org.apache.qpid.server.txn.LocalTransaction;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.server.util.ServerScopedRuntimeException;
import org.apache.qpid.server.virtualhost.VirtualHostUnavailableException;

public class AMQPConnection_1_0Impl extends AbstractAMQPConnection<AMQPConnection_1_0Impl, ConnectionHandler>
        implements DescribedTypeConstructorRegistry.Source,
                   ValueWriter.Registry.Source,
                   SASLEndpoint,
                   AMQPConnection_1_0<AMQPConnection_1_0Impl>
{

    private static final Logger LOGGER = LoggerFactory.getLogger(AMQPConnection_1_0Impl.class);
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

    private final FrameWriter _frameWriter;
    private ProtocolHandler _frameHandler;
    private volatile boolean _transportBlockedForWriting;
    private volatile SubjectAuthenticationResult _successfulAuthenticationResult;
    private boolean _blocking;
    private final Object _blockingLock = new Object();
    private List<Symbol> _offeredCapabilities;
    private SoleConnectionEnforcementPolicy _soleConnectionEnforcementPolicy;

    private static final int CONNECTION_CONTROL_CHANNEL = 0;
    private final SubjectCreator _subjectCreator;

    private int _channelMax = 0;
    private int _maxFrameSize = 4096;
    private String _remoteContainerId;

    private SocketAddress _remoteAddress;

    // positioned by the *outgoing* channel
    private Session_1_0[] _sendingSessions;

    // positioned by the *incoming* channel
    private Session_1_0[] _receivingSessions;
    private volatile boolean _closedForOutput;

    private final long _incomingIdleTimeout;

    private volatile long _outgoingIdleTimeout;

    private volatile ConnectionState _connectionState = ConnectionState.AWAIT_AMQP_OR_SASL_HEADER;

    private final AMQPDescribedTypeRegistry _describedTypeRegistry = AMQPDescribedTypeRegistry.newInstance()
                                                                                              .registerTransportLayer()
                                                                                              .registerMessagingLayer()
                                                                                              .registerTransactionLayer()
                                                                                              .registerSecurityLayer()
                                                                                              .registerExtensionSoleconnLayer();

    private final Map<Symbol, Object> _properties = new LinkedHashMap<>();
    private volatile boolean _saslComplete;

    private volatile SaslNegotiator _saslNegotiator;
    private String _localHostname;

    private static final long MINIMUM_SUPPORTED_IDLE_TIMEOUT = 1000L;

    private Set<Symbol> _remoteDesiredCapabilities;

    private final AtomicBoolean _orderlyClose = new AtomicBoolean(false);

    private final Collection<Session_1_0>
            _sessions = Collections.synchronizedCollection(new ArrayList<Session_1_0>());

    private final Object _reference = new Object();

    private final Queue<Action<? super ConnectionHandler>> _asyncTaskList =
            new ConcurrentLinkedQueue<>();

    private final Set<AMQPSession<?,?>> _sessionsWithWork =
            Collections.newSetFromMap(new ConcurrentHashMap<AMQPSession<?,?>, Boolean>());


    // Multi session transactions
    private volatile ServerTransaction[] _openTransactions = new ServerTransaction[16];
    private volatile boolean _sendSaslFinalChallengeAsChallenge;
    private volatile String _closeCause;

    AMQPConnection_1_0Impl(final Broker<?> broker,
                           final ServerNetworkConnection network,
                           AmqpPort<?> port,
                           Transport transport,
                           long id,
                           final AggregateTicker aggregateTicker)
    {
        super(broker, network, port, transport, Protocol.AMQP_1_0, id, aggregateTicker);

        _subjectCreator = port.getSubjectCreator(transport.isSecure(), network.getSelectedHost());

        List<Symbol> offeredCapabilities = new ArrayList<>();
        offeredCapabilities.add(ANONYMOUS_RELAY);
        offeredCapabilities.add(SHARED_SUBSCRIPTIONS);
        offeredCapabilities.add(SoleConnectionConnectionProperties.SOLE_CONNECTION_FOR_CONTAINER);

        setOfferedCapabilities(offeredCapabilities);

        setRemoteAddress(network.getRemoteAddress());

        _incomingIdleTimeout = 1000L * port.getHeartbeatDelay();

        _frameWriter = new FrameWriter(getDescribedTypeRegistry(), getSender());
    }

    @Override
    protected void onOpen()
    {
        super.onOpen();
        _sendSaslFinalChallengeAsChallenge = getContextValue(Boolean.class, AMQPConnection_1_0.SEND_SASL_FINAL_CHALLENGE_AS_CHALLENGE);
    }

    @Override
    public void receiveSaslInit(final SaslInit saslInit)
    {
        assertState(ConnectionState.AWAIT_SASL_INIT);
        if(saslInit.getHostname() != null && !"".equals(saslInit.getHostname().trim()))
        {
            _localHostname = saslInit.getHostname();
        }
        else if(getNetwork().getSelectedHost() != null)
        {
            _localHostname = getNetwork().getSelectedHost();
        }
        String mechanism = saslInit.getMechanism().toString();
        final Binary initialResponse = saslInit.getInitialResponse();
        byte[] response = initialResponse == null ? new byte[0] : initialResponse.getArray();

        List<String> availableMechanisms =
                _subjectCreator.getAuthenticationProvider().getAvailableMechanisms(getTransport().isSecure());
        if (!availableMechanisms.contains(mechanism))
        {
            handleSaslError();
        }
        else
        {
            _saslNegotiator = _subjectCreator.createSaslNegotiator(mechanism, this);
            processSaslResponse(response);
        }
    }

    @Override
    public void receiveSaslResponse(final SaslResponse saslResponse)
    {
        assertState(ConnectionState.AWAIT_SASL_RESPONSE);
        final Binary responseBinary = saslResponse.getResponse();
        byte[] response = responseBinary == null ? new byte[0] : responseBinary.getArray();

        processSaslResponse(response);
    }

    @Override
    public void receiveSaslMechanisms(final SaslMechanisms saslMechanisms)
    {
        LOGGER.info("{} : Unexpected frame sasl-mechanisms", getLogSubject());
        closeSaslWithFailure();
    }

    @Override
    public void receiveSaslChallenge(final SaslChallenge saslChallenge)
    {
        LOGGER.info("{} : Unexpected frame sasl-challenge", getLogSubject());
        closeSaslWithFailure();
    }

    @Override
    public void receiveSaslOutcome(final SaslOutcome saslOutcome)
    {
        LOGGER.info("{} : Unexpected frame sasl-outcome", getLogSubject());
        closeSaslWithFailure();
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
            final boolean finalChallenge = challenge != null && challenge.length != 0;
            _successfulAuthenticationResult = authenticationResult;
            if (_sendSaslFinalChallengeAsChallenge && finalChallenge)
            {
                continueSaslNegotiation(challenge);
            }
            else
            {
                setSubject(_successfulAuthenticationResult.getSubject());
                SaslOutcome outcome = new SaslOutcome();
                outcome.setCode(SaslCode.OK);
                if (finalChallenge)
                {
                    outcome.setAdditionalData(new Binary(challenge));
                }
                send(new SASLFrame(outcome));
                _saslComplete = true;
                _connectionState = ConnectionState.AWAIT_AMQP_HEADER;
                disposeSaslNegotiator();
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
        send(new SASLFrame(challengeBody));

        _connectionState = ConnectionState.AWAIT_SASL_RESPONSE;
    }

    private void handleSaslError()
    {
        SaslOutcome outcome = new SaslOutcome();
        outcome.setCode(SaslCode.AUTH);
        send(new SASLFrame(outcome));
        _saslComplete = true;
        closeSaslWithFailure();
    }

    private void closeSaslWithFailure()
    {
        _saslComplete = true;
        disposeSaslNegotiator();
        _connectionState = ConnectionState.CLOSED;
        addCloseTicker();
    }

    private void disposeSaslNegotiator()
    {
        if (_saslNegotiator != null)
        {
            _saslNegotiator.dispose();
        }
        _saslNegotiator = null;
    }

    private void setUserPrincipal(final Principal user)
    {
        setSubject(_subjectCreator.createSubjectWithGroups(user));
    }

    @Override
    public long getIncomingIdleTimeout()
    {
        return _incomingIdleTimeout;
    }

    @Override
    public long getOutgoingIdleTimeout()
    {
        return _outgoingIdleTimeout;
    }


    @Override
    public void receiveAttach(final int channel, final Attach attach)
    {
        assertState(ConnectionState.OPENED);
        final Session_1_0 session = getSession(channel);
        if (session != null)
        {
            session.receiveAttach(attach);
        }
        else
        {
            closeConnection(AmqpError.INVALID_FIELD, "Channel " + channel + " is not associated with a session");
        }
    }

    @Override
    public void receive(final List<ChannelFrameBody> channelFrameBodies)
    {
        if (!channelFrameBodies.isEmpty())
        {
            PeekingIterator<ChannelFrameBody> itr = Iterators.peekingIterator(channelFrameBodies.iterator());
            boolean cleanExit = false;
            try
            {
                while (itr.hasNext())
                {
                    final ChannelFrameBody channelFrameBody = itr.next();
                    final int frameChannel = channelFrameBody.getChannel();

                    Session_1_0 session = _receivingSessions == null || frameChannel >= _receivingSessions.length
                            ? null
                            : _receivingSessions[frameChannel];
                    if (session != null)
                    {
                        final AccessControlContext context = session.getAccessControllerContext();
                        AccessController.doPrivileged((PrivilegedAction<Void>) () ->
                        {
                            ChannelFrameBody channelFrame = channelFrameBody;
                            boolean nextIsSameChannel;
                            do
                            {
                                received(frameChannel, channelFrame.getFrameBody());
                                nextIsSameChannel = itr.hasNext() && frameChannel == itr.peek().getChannel();
                                if (nextIsSameChannel)
                                {
                                    channelFrame = itr.next();
                                }
                            }
                            while (nextIsSameChannel);
                            return null;
                        }, context);
                    }
                    else
                    {
                        received(frameChannel, channelFrameBody.getFrameBody());
                    }
                }
                cleanExit = true;
            }
            finally
            {
                if (!cleanExit)
                {
                    while (itr.hasNext())
                    {
                        final Object frameBody = itr.next().getFrameBody();
                        if (frameBody instanceof Transfer)
                        {
                            ((Transfer) frameBody).dispose();
                        }
                    }
                }
            }
        }
    }

    private void received(int channel, Object val)
    {
        if (channel > getChannelMax())
        {
            Error error = new Error(ConnectionError.FRAMING_ERROR,
                                    String.format("specified channel %d larger than maximum channel %d", channel, getChannelMax()));
            handleError(error);
            return;
        }

        FRAME_LOGGER.debug("RECV[{}|{}] : {}", _remoteAddress, channel, val);
        if (val instanceof FrameBody)
        {
            ((FrameBody) val).invoke(channel, this);
        }
        else if (val instanceof SaslFrameBody)
        {
            ((SaslFrameBody) val).invoke(channel, this);
        }
    }


    @Override
    public void receiveClose(final int channel, final Close close)
    {
        switch (_connectionState)
        {
            case AWAIT_AMQP_OR_SASL_HEADER:
            case AWAIT_SASL_INIT:
            case AWAIT_SASL_RESPONSE:
            case AWAIT_AMQP_HEADER:
                throw new ConnectionScopedRuntimeException("Received unexpected close when AMQP connection has not been established.");
            case AWAIT_OPEN:
                closeReceived();
                closeConnection(ConnectionError.CONNECTION_FORCED,
                                "Connection close sent before connection was opened");
                break;
            case OPENED:
                _connectionState = ConnectionState.CLOSE_RECEIVED;
                closeReceived();
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
                closeReceived();
                _connectionState = ConnectionState.CLOSED;
                _orderlyClose.set(true);
                break;
            case CLOSE_RECEIVED:
            case CLOSED:
                break;
            default:
                throw new ServerScopedRuntimeException("Unknown state: " + _connectionState);
        }
    }

    private void closeReceived()
    {
        Collection<Session_1_0> sessions = new ArrayList<>(_sessions);

        for (final Session_1_0 session : sessions)
        {
            AccessController.doPrivileged((PrivilegedAction<Object>) () ->
            {
                session.remoteEnd(new End());
                return null;
            }, session.getAccessControllerContext());
        }
    }


    @Override
    public AMQPDescribedTypeRegistry getDescribedTypeRegistry()
    {
        return _describedTypeRegistry;
    }

    @Override
    public SectionDecoderRegistry getSectionDecoderRegistry()
    {
        return _describedTypeRegistry.getSectionDecoderRegistry();
    }

    @Override
    public boolean isClosed()
    {
        return _connectionState == ConnectionState.CLOSED
               || _connectionState == ConnectionState.CLOSE_RECEIVED;
    }

    @Override
    public boolean isClosing()
    {
        return _connectionState == ConnectionState.CLOSED
               || _connectionState == ConnectionState.CLOSE_RECEIVED
               || _connectionState == ConnectionState.CLOSE_SENT;
    }

    @Override
    public boolean closedForInput()
    {
        return _connectionState == ConnectionState.CLOSE_RECEIVED || _connectionState == ConnectionState.CLOSED ;
    }

    @Override
    public void sessionEnded(final Session_1_0 session)
    {
        _sessions.remove(session);
    }

    private void inputClosed()
    {
        if (!closedForInput())
        {
            FRAME_LOGGER.debug("RECV[{}] : {}", _remoteAddress, "Underlying connection closed");
            _connectionState = ConnectionState.CLOSED;
            closeSender();
            closeReceived();
        }
    }

    private void closeSender()
    {
        setClosedForOutput(true);
    }

    @Override
    public String getRemoteContainerId()
    {
        return _remoteContainerId;
    }

    public boolean isOpen()
    {
        return _connectionState == ConnectionState.OPENED;
    }

    @Override
    public void sendEnd(final int channel, final End end, final boolean remove)
    {
        sendFrame(channel, end);
        if (remove)
        {
            _sendingSessions[channel] = null;
        }
    }

    @Override
    public void receiveEnd(final int channel, final End end)
    {
        assertState(ConnectionState.OPENED);
        final Session_1_0 session = getSession(channel);
        if (session != null)
        {
            _receivingSessions[channel] = null;
            session.receiveEnd(end);
        }
        else
        {
            closeConnectionWithInvalidChannel(channel, end);
        }
    }

    private void closeConnectionWithInvalidChannel(final int channel, final FrameBody frame)
    {
        closeConnection(AmqpError.INVALID_FIELD, String.format("%s frame received on channel %d which is not mapped", frame.getClass().getSimpleName().toLowerCase(), channel));
    }

    @Override
    public void receiveDisposition(final int channel,
                                   final Disposition disposition)
    {
        assertState(ConnectionState.OPENED);
        final Session_1_0 session = getSession(channel);
        if (session != null)
        {
            session.receiveDisposition(disposition);
        }
        else
        {
            closeConnectionWithInvalidChannel(channel, disposition);
        }

    }

    @Override
    public void receiveBegin(final int receivingChannelId, final Begin begin)
    {

        assertState(ConnectionState.OPENED);
        if (begin.getRemoteChannel() != null)
        {
            closeConnection(ConnectionError.FRAMING_ERROR,
                            "BEGIN received on channel "
                            + receivingChannelId
                            + " with given remote-channel "
                            + begin.getRemoteChannel()
                            + ". Since the broker does not spontaneously start channels, this must be an error.");

        }
        else // Peer requesting session creation
        {

            if (_receivingSessions[receivingChannelId] == null)
            {
                int sendingChannelId = getFirstFreeChannel();
                if (sendingChannelId == -1)
                {
                    closeConnection(ConnectionError.FRAMING_ERROR,
                                    "BEGIN received on channel "
                                    + receivingChannelId
                                    + ". There are no free channels for the broker to respond on.");
                }
                else
                {
                    Session_1_0 session = new Session_1_0(this,
                                                          begin,
                                                          sendingChannelId,
                                                          receivingChannelId,
                                                          getContextValue(Long.class, AMQPConnection_1_0.CONNECTION_SESSION_CREDIT_WINDOW_SIZE));
                    session.create();

                    _receivingSessions[receivingChannelId] = session;
                    _sendingSessions[sendingChannelId] = session;

                    Begin beginToSend = new Begin();
                    beginToSend.setRemoteChannel(UnsignedShort.valueOf(receivingChannelId));
                    beginToSend.setNextOutgoingId(session.getNextOutgoingId());
                    beginToSend.setOutgoingWindow(session.getOutgoingWindow());
                    beginToSend.setIncomingWindow(session.getIncomingWindow());
                    sendFrame(sendingChannelId, beginToSend);

                    synchronized (_blockingLock)
                    {
                        _sessions.add(session);
                        if (_blocking)
                        {
                            session.block();
                        }
                    }
                }
            }
            else
            {
                closeConnection(ConnectionError.FRAMING_ERROR,
                                "BEGIN received on channel " + receivingChannelId + " which is already in use.");
            }

        }

    }

    private int getFirstFreeChannel()
    {
        for (int i = 0; i <= _channelMax; i++)
        {
            if (_sendingSessions[i] == null)
            {
                return i;
            }
        }
        return -1;
    }

    @Override
    public void handleError(final Error error)
    {
        if (!_closedForOutput)
        {
            closeConnection(error);
        }
    }

    @Override
    public void receiveTransfer(final int channel, final Transfer transfer)
    {
        assertState(ConnectionState.OPENED);
        final Session_1_0 session = getSession(channel);
        if (session != null)
        {
            session.receiveTransfer(transfer);
        }
        else
        {
            closeConnectionWithInvalidChannel(channel, transfer);
        }
    }

    @Override
    public void receiveFlow(final int channel, final Flow flow)
    {
        assertState(ConnectionState.OPENED);
        final Session_1_0 session = getSession(channel);
        if (session != null)
        {
            session.receiveFlow(flow);
        }
        else
        {
            closeConnectionWithInvalidChannel(channel, flow);
        }

    }

    @Override
    public void receiveOpen(final int channel, final Open open)
    {
        assertState(ConnectionState.AWAIT_OPEN);

        int channelMax = getPort().getSessionCountLimit() - 1;
        _channelMax = open.getChannelMax() == null ? channelMax
                : open.getChannelMax().intValue() < channelMax
                        ? open.getChannelMax().intValue()
                        : channelMax;
        if (_receivingSessions == null)
        {
            _receivingSessions = new Session_1_0[_channelMax + 1];
            _sendingSessions = new Session_1_0[_channelMax + 1];
        }
        _maxFrameSize = open.getMaxFrameSize() == null
                        || open.getMaxFrameSize().longValue() > getBroker().getNetworkBufferSize()
                ? getBroker().getNetworkBufferSize()
                : open.getMaxFrameSize().intValue();
        _remoteContainerId = open.getContainerId();

        if(open.getHostname() != null && !"".equals(open.getHostname().trim()))
        {
            _localHostname = open.getHostname();
        }

        if(_localHostname == null || "".equals(_localHostname.trim()) && getNetwork().getSelectedHost() != null)
        {
            _localHostname = getNetwork().getSelectedHost();
        }
        if (open.getIdleTimeOut() != null)
        {
            _outgoingIdleTimeout = open.getIdleTimeOut().longValue();
        }
        final Map<Symbol, Object> remoteProperties = open.getProperties() == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<>(open.getProperties()));
        _remoteDesiredCapabilities = open.getDesiredCapabilities() == null ? Collections.emptySet() : Sets.newHashSet(open.getDesiredCapabilities());
        if (remoteProperties.containsKey(Symbol.valueOf("product")))
        {
            setClientProduct(remoteProperties.get(Symbol.valueOf("product")).toString());
        }
        if (remoteProperties.containsKey(Symbol.valueOf("version")))
        {
            setClientVersion(remoteProperties.get(Symbol.valueOf("version")).toString());
        }
        setClientId(_remoteContainerId);
        if (_remoteDesiredCapabilities.contains(SoleConnectionConnectionProperties.SOLE_CONNECTION_FOR_CONTAINER))
        {
            if (remoteProperties != null && remoteProperties.containsKey(SOLE_CONNECTION_ENFORCEMENT_POLICY))
            {
                try
                {
                    _soleConnectionEnforcementPolicy = SoleConnectionEnforcementPolicy.valueOf(remoteProperties.get(
                            SOLE_CONNECTION_ENFORCEMENT_POLICY));
                }
                catch (IllegalArgumentException e)
                {
                    closeConnection(AmqpError.INVALID_FIELD, e.getMessage());
                    return;
                }
            }
            else
            {
                _soleConnectionEnforcementPolicy = SoleConnectionEnforcementPolicy.REFUSE_CONNECTION;
            }
        }

        if (_outgoingIdleTimeout != 0L && _outgoingIdleTimeout < MINIMUM_SUPPORTED_IDLE_TIMEOUT)
        {
            closeConnection(ConnectionError.CONNECTION_FORCED,
                            "Requested idle timeout of "
                            + _outgoingIdleTimeout
                            + " is too low. The minimum supported timeout is"
                            + MINIMUM_SUPPORTED_IDLE_TIMEOUT);
        }
        else
        {
            initialiseHeartbeating(_outgoingIdleTimeout / 2L, _incomingIdleTimeout);
            final NamedAddressSpace addressSpace = getPort().getAddressSpace(_localHostname);
            if (addressSpace == null)
            {
                closeConnection(AmqpError.NOT_FOUND, "Unknown hostname in connection open: '" + _localHostname + "'");
            }
            else
            {
                receiveOpenInternal(addressSpace);
            }
        }
    }

    private void receiveOpenInternal(final NamedAddressSpace addressSpace)
    {
        if (!addressSpace.isActive())
        {
            final Error err = new Error();
            populateConnectionRedirect(addressSpace, err);
            closeConnection(err);
        }
        else
        {
            if (AuthenticatedPrincipal.getOptionalAuthenticatedPrincipalFromSubject(getSubject()) == null)
            {
                closeConnection(AmqpError.NOT_ALLOWED, "Connection has not been authenticated");
            }
            else
            {
                try
                {
                    boolean registerSucceeded = addressSpace.registerConnection(this, (existingConnections, newConnection) ->
                    {
                        boolean proceedWithRegistration = true;
                        if (newConnection instanceof AMQPConnection_1_0Impl && !newConnection.isClosing())
                        {
                            List<ListenableFuture<Void>> rescheduleFutures = new ArrayList<>();
                            for (AMQPConnection<?> existingConnection : StreamSupport.stream(existingConnections.spliterator(), false)
                                                                                     .filter(con -> con instanceof AMQPConnection_1_0)
                                                                                     .filter(con -> !con.isClosing())
                                                                                     .filter(con -> con.getRemoteContainerName().equals(newConnection.getRemoteContainerName()))
                                                                                     .collect(Collectors.toList()))
                            {
                                SoleConnectionEnforcementPolicy soleConnectionEnforcementPolicy = null;
                                if (((AMQPConnection_1_0Impl) existingConnection)._soleConnectionEnforcementPolicy
                                    != null)
                                {
                                    soleConnectionEnforcementPolicy =
                                            ((AMQPConnection_1_0Impl) existingConnection)._soleConnectionEnforcementPolicy;
                                }
                                else if (((AMQPConnection_1_0Impl) newConnection)._soleConnectionEnforcementPolicy != null)
                                {
                                    soleConnectionEnforcementPolicy =
                                            ((AMQPConnection_1_0Impl) newConnection)._soleConnectionEnforcementPolicy;
                                }
                                if (SoleConnectionEnforcementPolicy.REFUSE_CONNECTION.equals(soleConnectionEnforcementPolicy))
                                {
                                    _properties.put(Symbol.valueOf("amqp:connection-establishment-failed"), true);
                                    Error error = new Error(AmqpError.INVALID_FIELD,
                                                            String.format(
                                                                    "Connection closed due to sole-connection-enforcement-policy '%s'",
                                                                    soleConnectionEnforcementPolicy.toString()));
                                    error.setInfo(Collections.singletonMap(Symbol.valueOf("invalid-field"), Symbol.valueOf("container-id")));
                                    newConnection.doOnIOThreadAsync(() -> ((AMQPConnection_1_0Impl) newConnection).closeConnection(error));
                                    proceedWithRegistration = false;
                                    break;
                                }
                                else if (SoleConnectionEnforcementPolicy.CLOSE_EXISTING.equals(soleConnectionEnforcementPolicy))
                                {
                                    final Error error = new Error(AmqpError.RESOURCE_LOCKED,
                                                                  String.format(
                                                                          "Connection closed due to sole-connection-enforcement-policy '%s'",
                                                                          soleConnectionEnforcementPolicy.toString()));
                                    error.setInfo(Collections.singletonMap(Symbol.valueOf("sole-connection-enforcement"), true));
                                    rescheduleFutures.add(existingConnection.doOnIOThreadAsync(
                                            () -> ((AMQPConnection_1_0Impl) existingConnection).closeConnection(error)));
                                    proceedWithRegistration = false;
                                }
                            }
                            if (!rescheduleFutures.isEmpty())
                            {
                                doAfter(allAsList(rescheduleFutures), () -> newConnection.doOnIOThreadAsync(() -> receiveOpenInternal(addressSpace)));
                            }
                        }
                        return proceedWithRegistration;
                    });

                    if (registerSucceeded)
                    {
                        setAddressSpace(addressSpace);

                        if (!addressSpace.authoriseCreateConnection(this))
                        {
                            closeConnection(AmqpError.NOT_ALLOWED, "Connection refused");
                        }
                        else
                        {
                            switch (_connectionState)
                            {
                                case AWAIT_OPEN:
                                    sendOpen(_channelMax, _maxFrameSize);
                                    _connectionState = ConnectionState.OPENED;
                                    break;
                                case CLOSE_SENT:
                                case CLOSED:
                                    // already sent our close - probably due to an error
                                    break;
                                default:
                                    throw new ConnectionScopedRuntimeException(String.format(
                                            "Unexpected state %s during connection open.", _connectionState));
                            }
                        }
                    }
                }
                catch (VirtualHostUnavailableException | AccessControlException e)
                {
                    closeConnection(AmqpError.NOT_ALLOWED, e.getMessage());
                }
            }
        }
    }

    private void populateConnectionRedirect(final NamedAddressSpace addressSpace, final Error err)
    {
        final String redirectHost = addressSpace.getRedirectHost(getPort());

        if(redirectHost == null)
        {
            err.setCondition(ConnectionError.CONNECTION_FORCED);
            err.setDescription("Virtual host '" + _localHostname + "' is not active");
        }
        else
        {
            err.setCondition(ConnectionError.REDIRECT);
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

    @Override
    public void receiveDetach(final int channel, final Detach detach)
    {
        assertState(ConnectionState.OPENED);
        final Session_1_0 session = getSession(channel);
        if (session != null)
        {
            session.receiveDetach(detach);
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

    @Override
    public void close(final Error error)
    {
        closeConnection(error);
    }

    private void setRemoteAddress(final SocketAddress remoteAddress)
    {
        _remoteAddress = remoteAddress;
    }

    public void setOfferedCapabilities(final List<Symbol> offeredCapabilities)
    {
        _offeredCapabilities = offeredCapabilities;
    }


    private void setClosedForOutput(final boolean closed)
    {
        _closedForOutput = closed;
    }

    @Override
    public String getLocalFQDN()
    {
        return _localHostname != null ? _localHostname : super.getLocalFQDN();
    }

    @Override
    public int getMaxFrameSize()
    {
        return _maxFrameSize;
    }

    @Override
    public int getChannelMax()
    {
        return _channelMax;
    }

    @Override
    public Object getReference()
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
        _closeCause = error.getDescription();
        Close close = new Close();
        close.setError(error);
        switch (_connectionState)
        {
            case AWAIT_AMQP_OR_SASL_HEADER:
            case AWAIT_SASL_INIT:
            case AWAIT_SASL_RESPONSE:
            case AWAIT_AMQP_HEADER:
                throw new ConnectionScopedRuntimeException("Connection is closed before being fully established: " + error.getDescription());

            case AWAIT_OPEN:
                sendOpen(0, 0);
                sendClose(close);
                _connectionState = ConnectionState.CLOSED;
                break;
            case OPENED:
                sendClose(close);
                _connectionState = ConnectionState.CLOSE_SENT;
                addCloseTicker();
                break;
            case CLOSE_RECEIVED:
                sendClose(close);
                _connectionState = ConnectionState.CLOSED;
                addCloseTicker();
                break;
            case CLOSE_SENT:
            case CLOSED:
                // already sent our close - too late to do anything more
                break;
            default:
                throw new ServerScopedRuntimeException("Unknown state: " + _connectionState);
        }
    }

    @Override
    public int sendFrame(final int channel, final FrameBody body, final QpidByteBuffer payload)
    {
        if (!_closedForOutput)
        {
            ValueWriter<FrameBody> writer = _describedTypeRegistry.getValueWriter(body);
            if (payload == null)
            {
                send(new TransportFrame(channel, body));
                return 0;
            }
            else
            {
                int size = writer.getEncodedSize();
                int maxPayloadSize = _maxFrameSize - (size + 9);
                long payloadLength = (long) payload.remaining();
                if (payloadLength <= maxPayloadSize)
                {
                    send(new TransportFrame(channel, body, payload));
                    return (int)payloadLength;
                }
                else
                {
                    ((Transfer) body).setMore(Boolean.TRUE);

                    writer = _describedTypeRegistry.getValueWriter(body);
                    size = writer.getEncodedSize();
                    maxPayloadSize = _maxFrameSize - (size + 9);

                    try (QpidByteBuffer payloadDup = payload.view(0, maxPayloadSize))
                    {
                        payload.position(payload.position() + maxPayloadSize);
                        send(new TransportFrame(channel, body, payloadDup));
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

    @Override
    public void sendFrame(final int channel, final FrameBody body)
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
        send(TransportFrame.HEARTBEAT);
    }

    @Override
    public void readerIdle()
    {
        AccessController.doPrivileged((PrivilegedAction<Object>) () ->
        {
            getEventLogger().message(ConnectionMessages.IDLE_CLOSE("", false));
            getNetwork().close();
            return null;
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

    @Override
    protected void onReceive(final QpidByteBuffer msg)
    {
        try
        {
            int remaining;

            try
            {
                do
                {
                    remaining = msg.remaining();

                    switch (_connectionState)
                    {
                        case AWAIT_AMQP_OR_SASL_HEADER:
                        case AWAIT_AMQP_HEADER:
                            if (remaining >= 8)
                            {
                                processProtocolHeader(msg);
                            }
                            break;
                        case AWAIT_SASL_INIT:
                        case AWAIT_SASL_RESPONSE:
                        case AWAIT_OPEN:
                        case OPENED:
                        case CLOSE_SENT:
                            _frameHandler.parse(msg);
                            break;
                        case CLOSE_RECEIVED:
                        case CLOSED:
                            // ignore;
                            break;
                    }


                }
                while (msg.remaining() != remaining);
            }
            finally
            {
                receivedComplete();
            }
        }
        catch (IllegalArgumentException | IllegalStateException e)
        {
            throw new ConnectionScopedRuntimeException(e);
        }
    }

    @Override
    public void receivedComplete()
    {
        if (_receivingSessions != null)
        {
            for (final Session_1_0 session : _receivingSessions)
            {
                if (session != null)
                {
                    final AccessControlContext context = session.getAccessControllerContext();
                    AccessController.doPrivileged((PrivilegedAction<Void>) () ->
                    {
                        session.receivedComplete();
                        return null;
                    }, context);
                }
            }
        }
    }

    private void processProtocolHeader(final QpidByteBuffer msg)
    {
        if(msg.remaining() >= 8)
        {
            byte[] header = new byte[8];
            msg.get(header);

            final AuthenticationProvider<?> authenticationProvider = getPort().getAuthenticationProvider();

            if(Arrays.equals(header, SASL_HEADER))
            {
                if(_saslComplete)
                {
                    throw new ConnectionScopedRuntimeException("SASL Layer header received after SASL already established");
                }

                try (QpidByteBuffer protocolHeader = QpidByteBuffer.wrap(SASL_HEADER))
                {
                    getSender().send(protocolHeader);
                }
                SaslMechanisms mechanisms = new SaslMechanisms();
                ArrayList<Symbol> mechanismsList = new ArrayList<>();
                for (String name :  authenticationProvider.getAvailableMechanisms(getTransport().isSecure()))
                {
                    mechanismsList.add(Symbol.valueOf(name));
                }
                mechanisms.setSaslServerMechanisms(mechanismsList.toArray(new Symbol[mechanismsList.size()]));
                send(new SASLFrame(mechanisms));

                _connectionState = ConnectionState.AWAIT_SASL_INIT;
                _frameHandler = getFrameHandler(true);
            }
            else if(Arrays.equals(header, AMQP_HEADER))
            {
                if(!_saslComplete)
                {
                    final List<String> mechanisms = authenticationProvider.getAvailableMechanisms(getTransport().isSecure());

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
                try (QpidByteBuffer protocolHeader = QpidByteBuffer.wrap(AMQP_HEADER))
                {
                    getSender().send(protocolHeader);
                }
                _connectionState = ConnectionState.AWAIT_OPEN;
                _frameHandler = getFrameHandler(false);

            }
            else
            {
                LOGGER.warn("{} : unknown AMQP header {}", getLogSubject(), Functions.str(header));
                _connectionState = ConnectionState.CLOSED;
                getNetwork().close();
            }

        }

    }

    private FrameHandler getFrameHandler(final boolean sasl)
    {
        return new FrameHandler(new ValueHandler(this.getDescribedTypeRegistry()), this, sasl);
    }


    @Override
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

    private void send(final AMQFrame amqFrame)
    {
        updateLastWriteTime();
        FRAME_LOGGER.debug("SEND[{}|{}] : {}",
                           getNetwork().getRemoteAddress(),
                           amqFrame.getChannel(),
                           amqFrame.getFrameBody() == null ? "<<HEARTBEAT>>" : amqFrame.getFrameBody());

        int size = _frameWriter.send(amqFrame);
        if (size > getMaxFrameSize())
        {
            throw new OversizeFrameException(amqFrame, size);
        }
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
    public void notifyWork(final AMQPSession<?,?> sessionModel)
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

    @Override
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
        Action<ConnectionHandler> action = object -> closeConnection(cause, description);
        addAsyncTask(action);
    }

    @Override
    public void closeSessionAsync(final AMQPSession<?,?> session,
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
        addAsyncTask(object -> AccessController.doPrivileged(new PrivilegedAction<Void>() {
            @Override
            public Void run()
            {
                ((Session_1_0)session).close(cause, message);
                return null;
            }
        }, ((Session_1_0)session).getAccessControllerContext()));

    }

    @Override
    public void block()
    {
        synchronized (_blockingLock)
        {
            if (!_blocking)
            {
                _blocking = true;
                doOnIOThreadAsync(this::doBlock);
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

    @Override
    public String getRemoteContainerName()
    {
        return _remoteContainerId;
    }

    @Override
    public Collection<? extends Session_1_0> getSessionModels()
    {
        return Collections.unmodifiableCollection(_sessions);
    }

    @Override
    public void unblock()
    {
        synchronized (_blockingLock)
        {
            if(_blocking)
            {
                _blocking = false;
                doOnIOThreadAsync(this::doUnblock);
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
    public int getSessionCountLimit()
    {
        return _channelMax + 1;
    }

    @Override
    public boolean isOrderlyClose()
    {
        return _orderlyClose.get();
    }

    @Override
    protected String getCloseCause()
    {
        return _closeCause;
    }

    @Override
    public boolean getSendSaslFinalChallengeAsChallenge()
    {
        return _sendSaslFinalChallengeAsChallenge;
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

        Map<String,Object> props = Collections.emptyMap();
        for(ConnectionPropertyEnricher enricher : getPort().getConnectionPropertyEnrichers())
        {
            props = enricher.addConnectionProperties(this, props);
        }
        for(Map.Entry<String,Object> entry : props.entrySet())
        {
            _properties.put(Symbol.valueOf(entry.getKey()), entry.getValue());
        }

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
        open.setIdleTimeOut(UnsignedInteger.valueOf(_incomingIdleTimeout));

        // set the offered capabilities
        if(_offeredCapabilities != null && !_offeredCapabilities.isEmpty())
        {
            open.setOfferedCapabilities(_offeredCapabilities.toArray(new Symbol[_offeredCapabilities.size()]));
        }

        if (_remoteDesiredCapabilities != null
            && _remoteDesiredCapabilities.contains(SoleConnectionConnectionProperties.SOLE_CONNECTION_FOR_CONTAINER))
        {
            _properties.put(SoleConnectionConnectionProperties.SOLE_CONNECTION_DETECTION_POLICY,
                            SoleConnectionDetectionPolicy.STRONG);
        }

        if (_soleConnectionEnforcementPolicy == SoleConnectionEnforcementPolicy.CLOSE_EXISTING)
        {
            _properties.put(SOLE_CONNECTION_ENFORCEMENT_POLICY, SoleConnectionEnforcementPolicy.CLOSE_EXISTING.getValue());
        }

        open.setProperties(_properties);

        sendFrame(CONNECTION_CONTROL_CHANNEL, open);
    }

    private Session_1_0 getSession(final int channel)
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


    private void assertState(final ConnectionState state)
    {
        if (_connectionState != state)
        {
            throw new ConnectionScopedRuntimeException(String.format(
                    "Unexpected state, client has sent frame in an illegal order.  Required state: %s, actual state: %s",
                    state,
                    _connectionState));
        }
    }

    private class ProcessPendingIterator implements Iterator<Runnable>
    {
        private Iterator<? extends AMQPSession<?,?>> _sessionIterator;
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
                        return () -> asyncAction.performAction(AMQPConnection_1_0Impl.this);
                    }
                    else
                    {
                        return () -> { };
                    }
                }
                else
                {
                    if (!_sessionIterator.hasNext())
                    {
                        _sessionIterator = _sessionsWithWork.iterator();
                    }
                    final AMQPSession<?,?> session = _sessionIterator.next();
                    return () ->
                    {
                        _sessionIterator.remove();
                        if (session.processPending())
                        {
                            _sessionsWithWork.add(session);
                        }
                    };
                }
            }
            else if(!_asyncTaskList.isEmpty())
            {
                final Action<? super ConnectionHandler> asyncAction = _asyncTaskList.poll();
                return () -> asyncAction.performAction(AMQPConnection_1_0Impl.this);
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
    public Iterator<ServerTransaction> getOpenTransactions()
    {
        return new Iterator<ServerTransaction>()
        {
            int _index = 0;

            @Override
            public boolean hasNext()
            {
                for(int i = _index; i < _openTransactions.length; i++)
                {
                    if(_openTransactions[i] != null)
                    {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public ServerTransaction next()
            {
                IdentifiedTransaction txn;
                for( ; _index < _openTransactions.length; _index++)
                {
                    if(_openTransactions[_index] != null)
                    {
                        txn = new IdentifiedTransaction(_index, _openTransactions[_index]);
                        _index++;
                        return txn.getServerTransaction();
                    }
                }

                throw new NoSuchElementException();
            }

            @Override
            public void remove()
            {
                _openTransactions[_index] = null;
            }
        };
    }

    @Override
    public IdentifiedTransaction createIdentifiedTransaction()
    {
        ServerTransaction[] openTransactions = _openTransactions;
        final int maxOpenTransactions = openTransactions.length;
        int id = 0;
        for(; id < maxOpenTransactions; id++)
        {
            if(openTransactions[id] == null)
            {
                break;

            }
        }

        // we need to expand the transaction array;
        if(id == maxOpenTransactions)
        {
            final int newSize = maxOpenTransactions < 1024 ? 2*maxOpenTransactions : maxOpenTransactions + 1024;

            _openTransactions = new ServerTransaction[newSize];
            System.arraycopy(openTransactions, 0, _openTransactions, 0, maxOpenTransactions);

        }

        final LocalTransaction serverTransaction = createLocalTransaction();

        _openTransactions[id] = serverTransaction;
        return new IdentifiedTransaction(id, serverTransaction);
    }

    @Override
    public ServerTransaction getTransaction(final int txnId)
    {
        try
        {
            return _openTransactions[txnId];
        }
        catch (ArrayIndexOutOfBoundsException e)
        {
            throw new UnknownTransactionException(txnId);
        }
    }

    @Override
    public void removeTransaction(final int txnId)
    {
        try
        {
            _openTransactions[txnId] = null;
        }
        catch (ArrayIndexOutOfBoundsException e)
        {
            throw new UnknownTransactionException(txnId);
        }
    }

    @Override
    protected boolean isOpeningInProgress()
    {
        switch (_connectionState)
        {
            case AWAIT_AMQP_OR_SASL_HEADER:
            case AWAIT_SASL_INIT:
            case AWAIT_SASL_RESPONSE:
            case AWAIT_AMQP_HEADER:
            case AWAIT_OPEN:
                return true;
            case OPENED:
            case CLOSE_RECEIVED:
            case CLOSE_SENT:
            case CLOSED:
                return false;
            default:
                throw new IllegalStateException("Unsupported state " + _connectionState);
        }
    }
}
