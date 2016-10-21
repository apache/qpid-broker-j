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
package org.apache.qpid.server.protocol.v0_8.federation;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.security.auth.Subject;
import javax.security.auth.SubjectDomainCombiner;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.bytebuffer.QpidByteBuffer;
import org.apache.qpid.common.ServerPropertyNames;
import org.apache.qpid.configuration.CommonProperties;
import org.apache.qpid.framing.AMQDataBlock;
import org.apache.qpid.framing.AMQFrameDecodingException;
import org.apache.qpid.framing.AMQShortString;
import org.apache.qpid.framing.ClientChannelMethodProcessor;
import org.apache.qpid.framing.ClientMethodProcessor;
import org.apache.qpid.framing.FieldTable;
import org.apache.qpid.framing.FieldTableFactory;
import org.apache.qpid.framing.HeartbeatBody;
import org.apache.qpid.framing.MethodRegistry;
import org.apache.qpid.framing.ProtocolInitiation;
import org.apache.qpid.framing.ProtocolVersion;
import org.apache.qpid.server.configuration.BrokerProperties;
import org.apache.qpid.server.federation.OutboundProtocolEngine;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.Credential;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.RemoteHost;
import org.apache.qpid.server.model.RemoteHostAddress;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.server.transport.AggregateTicker;
import org.apache.qpid.server.transport.ProtocolEngine;
import org.apache.qpid.server.transport.SchedulableConnection;
import org.apache.qpid.server.transport.ServerIdleReadTimeoutTicker;
import org.apache.qpid.server.transport.ServerIdleWriteTimeoutTicker;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.server.util.ServerScopedRuntimeException;


class OutboundConnection_0_8 implements OutboundProtocolEngine, ClientMethodProcessor<ClientChannelMethodProcessor>
{

    private static final Map<Protocol, ProtocolVersion> PROTOCOL_VERSION_MAP;

    static
    {
        Map<Protocol, ProtocolVersion> protocolVersionMap = new HashMap<>();
        protocolVersionMap.put(Protocol.AMQP_0_8, ProtocolVersion.v0_8);
        protocolVersionMap.put(Protocol.AMQP_0_9, ProtocolVersion.v0_9);
        protocolVersionMap.put(Protocol.AMQP_0_9_1, ProtocolVersion.v0_91);

        PROTOCOL_VERSION_MAP = Collections.unmodifiableMap(protocolVersionMap);
    }

    private static final int TRANSFER_SESSION_ID = 0;

    private static final Logger LOGGER = LoggerFactory.getLogger(OutboundConnection_0_8.class);

    private final Protocol _protocol;
    private final RemoteHost<?> _remoteHost;
    private final RemoteHostAddress<?> _address;
    private final VirtualHost<?> _virtualHost;

    private final FederationDecoder _decoder;


    private final AggregateTicker _aggregateTicker;
    private final ProtocolVersion _protocolVersion;

    private final MethodRegistry _methodRegistry;
    private volatile long _lastReadTime;
    private volatile long _lastWriteTime;
    private SchedulableConnection _connection;
    private volatile AccessControlContext _accessControllerContext;



    private final AtomicBoolean _stateChanged = new AtomicBoolean();
    private final AtomicReference<Action<ProtocolEngine>> _workListener = new AtomicReference<>();
    private volatile Thread _ioThread;
    private final List<Runnable> _pendingTasks = new CopyOnWriteArrayList<>();
    private int _classId;
    private int _methodId;
    private SaslClient _saslClient;
    private int _maxFrameSize;
    private int _maxNoOfChannels;

    private static final OutboundChannel[] _channels = new OutboundChannel[3];
    private Action<Boolean> _onClosedTask;

    enum State
    {
        INIT,
        AWAIT_START,
        AWAIT_SECURE,
        AWAIT_TUNE,
        AWAIT_OPEN_OK,
        OPEN,
        AWAIT_CLOSE_OK,
        CLOSED
    }

    private State _state = State.INIT;


    public OutboundConnection_0_8(final RemoteHostAddress<?> address,
                                  final VirtualHost<?> virtualHost,
                                  final Protocol protocol,
                                  final byte[] protocolHeader)
    {
        _address = address;
        _protocol = protocol;
        _virtualHost = virtualHost;
        _aggregateTicker = new AggregateTicker();
        _remoteHost = address.getParent(RemoteHost.class);
        _decoder = new FederationDecoder(this);
        _protocolVersion = PROTOCOL_VERSION_MAP.get(protocol);
        _methodRegistry = new MethodRegistry(_protocolVersion);

        _pendingTasks.add(new Runnable()
        {
            @Override
            public void run()
            {
                changeState(State.INIT, State.AWAIT_START);
                _connection.send(QpidByteBuffer.wrap(protocolHeader));
            }
        });
        _stateChanged.set(true);
    }


    public VirtualHost<?> getVirtualHost()
    {
        return _virtualHost;
    }


    private synchronized void changeState(final State currentState, final State newState)
    {
        if(_state != currentState)
        {
            throw new ConnectionScopedRuntimeException("Incorrect state");
        }
        _state = newState;
    }

    private synchronized void assertState(final State currentState)
    {
        if (_state != currentState)
        {
            throw new ConnectionScopedRuntimeException("Incorrect state");
        }
    }

    public Protocol getProtocol()
    {
        return _protocol;
    }

    @Override
    public final AggregateTicker getAggregateTicker()
    {
        return _aggregateTicker;
    }

    public final Date getLastIoTime()
    {
        return new Date(Math.max(getLastReadTime(), getLastWriteTime()));
    }

    @Override
    public final long getLastReadTime()
    {
        return _lastReadTime;
    }

    public final void updateLastReadTime()
    {
        _lastReadTime = System.currentTimeMillis();
    }

    @Override
    public final long getLastWriteTime()
    {
        return _lastWriteTime;
    }

    public final void updateLastWriteTime()
    {
        _lastWriteTime = System.currentTimeMillis();
    }


    MethodRegistry getMethodRegistry()
    {
        return _methodRegistry;
    }


    @Override
    public void closed()
    {
        Action<Boolean> task = _onClosedTask;
        if(task != null)
        {
            task.performAction(_state == State.OPEN);
        }
    }

    public synchronized void writerIdle()
    {
        writeFrame(HeartbeatBody.FRAME);
    }

    @Override
    public void readerIdle()
    {

    }

    @Override
    public Subject getSubject()
    {
        return null;
    }

    @Override
    public boolean isTransportBlockedForWriting()
    {
        return false;
    }

    @Override
    public void setTransportBlockedForWriting(final boolean blocked)
    {

    }

    @Override
    public void setMessageAssignmentSuspended(final boolean value, final boolean notifyConsumers)
    {

    }

    @Override
    public boolean isMessageAssignmentSuspended()
    {
        return false;
    }


    @Override
    public Iterator<Runnable> processPendingIterator()
    {
        if (!isIOThread())
        {
            return Collections.emptyIterator();
        }
        return new ProcessPendingIterator();
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
    public void encryptedTransport()
    {

    }

    @Override
    public void received(final QpidByteBuffer msg)
    {
        AccessController.doPrivileged(new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                updateLastReadTime();

                try
                {
                    _decoder.decodeBuffer(msg);
                    receivedComplete();
                }
                catch (AMQFrameDecodingException | IOException e)
                {
                    LOGGER.error("Unexpected exception", e);
                    throw new ConnectionScopedRuntimeException(e);
                }
                catch (StoreException e)
                {
                    if (_virtualHost.isActive())
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

    private void receivedComplete()
    {
        for(OutboundChannel channel : _channels)
        {
            if (channel != null)
            {
                channel.receivedComplete();
            }
        }
    }


    public AccessControlContext getAccessControllerContext()
    {
        return _accessControllerContext;
    }

    public final void updateAccessControllerContext()
    {
        _accessControllerContext = getAccessControlContextFromSubject(
                getSubject());
    }

    public final AccessControlContext getAccessControlContextFromSubject(final Subject subject)
    {
        final AccessControlContext acc = AccessController.getContext();
        return AccessController.doPrivileged(
                new PrivilegedAction<AccessControlContext>()
                {
                    public AccessControlContext run()
                    {
                        if (subject == null)
                            return new AccessControlContext(acc, null);
                        else
                            return new AccessControlContext
                                    (acc,
                                     new SubjectDomainCombiner(subject));
                    }
                });
    }


    @Override
    public void setIOThread(final Thread ioThread)
    {
        _ioThread = ioThread;
    }

    public boolean isIOThread()
    {
        return Thread.currentThread() == _ioThread;
    }


    @Override
    public void setConnection(final SchedulableConnection connection)
    {
        _connection = connection;
    }

    @Override
    public void setOnClosedTask(final Action<Boolean> onClosedTask)
    {
        _onClosedTask = onClosedTask;
    }

    @Override
    public ProtocolVersion getProtocolVersion()
    {
        return _protocolVersion;
    }

    @Override
    public ClientChannelMethodProcessor getChannelMethodProcessor(final int channelId)
    {
        return getChannel(channelId);
    }


    public OutboundChannel getChannel(final int channelId)
    {
        return channelId < _channels.length ? _channels[channelId] : null;
    }

    public void setMaxFrameSize(int frameMax)
    {
        _maxFrameSize = frameMax;
        _decoder.setMaxFrameSize(frameMax);
    }

    public int getMaxFrameSize()
    {
        return _maxFrameSize;
    }

    @Override
    public void receiveConnectionClose(final int replyCode,
                                       final AMQShortString replyText,
                                       final int classId,
                                       final int methodId)
    {

        writeFrame(_methodRegistry.createConnectionCloseOkBody().generateFrame(0));
        _connection.close();
    }

    @Override
    public void receiveConnectionCloseOk()
    {
        // TODO
    }

    @Override
    public void receiveHeartbeat()
    {
    }

    @Override
    public void receiveProtocolHeader(final ProtocolInitiation protocolInitiation)
    {
        _connection.close();
    }

    @Override
    public void setCurrentMethod(final int classId, final int methodId)
    {
        _classId = classId;
        _methodId = methodId;
    }

    void writeFrame(AMQDataBlock frame)
    {
        LOGGER.debug("SEND: {}", frame);


        frame.writePayload(_connection);


        updateLastWriteTime();

    }


    @Override
    public boolean ignoreAllButCloseOk()
    {
        // TODO
        return false;
    }

    @Override
    public void receiveConnectionStart(final short versionMajor,
                                       final short versionMinor,
                                       final FieldTable serverProperties,
                                       final byte[] mechanisms,
                                       final byte[] locales)
    {
        changeState(State.AWAIT_START, State.AWAIT_SECURE);
        List<String> saslMechanisms = Arrays.asList(new String(mechanisms, StandardCharsets.UTF_8).split(" "));
        Map propMap = FieldTable.convertToMap(serverProperties);

        Collection<Credential> credentialList = _remoteHost.getChildren(Credential.class);

        SaslClient client = null;
        for(Credential<?> credentials : credentialList)
        {
            client = credentials.getSaslClient(saslMechanisms);
            if(client != null)
            {
                break;
            }
        }

        if(client != null)
        {
            _saslClient = client;
            try
            {
                byte[] initialResponse = client.hasInitialResponse() ? client.evaluateChallenge(new byte[0]) : new byte[0];

                FieldTable clientProperties = FieldTableFactory.newFieldTable();
                clientProperties.setString(ServerPropertyNames.PRODUCT,
                                           CommonProperties.getProductName());
                clientProperties.setString(ServerPropertyNames.VERSION,
                                           CommonProperties.getReleaseVersion());
                clientProperties.setString(ServerPropertyNames.QPID_BUILD,
                                           CommonProperties.getBuildVersion());
                clientProperties.setString(ServerPropertyNames.QPID_INSTANCE_NAME,
                                           _virtualHost.getName());


                writeFrame(_methodRegistry.createConnectionStartOkBody(clientProperties, AMQShortString.valueOf(_saslClient.getMechanismName()), initialResponse, null).generateFrame(0));
                if(client.isComplete())
                {
                    changeState(State.AWAIT_SECURE, State.AWAIT_TUNE);
                }
            }
            catch (SaslException e)
            {
                throw new ConnectionScopedRuntimeException(e);
            }
        }
        else
        {
            throw new ConnectionScopedRuntimeException("Unable to find acceptable sasl mechanism");
        }

    }

    @Override
    public void receiveConnectionSecure(final byte[] challenge)
    {
        assertState(State.AWAIT_SECURE);
        try
        {
            byte[] response = _saslClient.evaluateChallenge(challenge);

            writeFrame(_methodRegistry.createConnectionSecureOkBody(response).generateFrame(0));
            if(_saslClient.isComplete())
            {
                changeState(State.AWAIT_SECURE, State.AWAIT_TUNE);
            }
        }
        catch (SaslException e)
        {
            throw new ConnectionScopedRuntimeException(e);
        }
    }

    @Override
    public void receiveConnectionRedirect(final AMQShortString host, final AMQShortString knownHosts)
    {
        LOGGER.info("Connection redirect to {} received, however redirection is not followed for federation links");
        _connection.close();
    }

    private int getDefaultMaxFrameSize()
    {
        Broker<?> broker = _virtualHost.getBroker();

        return broker.getNetworkBufferSize();
    }

    protected void initialiseHeartbeating(final long writerDelay, final long readerDelay)
    {
        if (writerDelay > 0)
        {
            _aggregateTicker.addTicker(new ServerIdleWriteTimeoutTicker(this, (int) writerDelay));
            _connection.setMaxWriteIdleMillis(writerDelay);
        }

        if (readerDelay > 0)
        {
            _aggregateTicker.addTicker(new ServerIdleReadTimeoutTicker(_connection, this, (int) readerDelay));
            _connection.setMaxReadIdleMillis(readerDelay);
        }
    }



    public int getMaximumNumberOfChannels()
    {
        return _maxNoOfChannels;
    }

    private void setMaximumNumberOfChannels(int value)
    {
        _maxNoOfChannels = value;
    }


    @Override
    public void receiveConnectionTune(final int channelMax, final long frameMax, final int heartbeat)
    {
        assertState(State.AWAIT_TUNE);

        final int desiredHeartbeatInterval = _address.getDesiredHeartbeatInterval();
        int heartbeatDelay = heartbeat == 0
                ? desiredHeartbeatInterval
                : (desiredHeartbeatInterval == 0) ? heartbeat : Math.min(heartbeat, desiredHeartbeatInterval);

        long writerDelay = 1000L * heartbeatDelay;
        long readerDelay = 1000L * BrokerProperties.HEARTBEAT_TIMEOUT_FACTOR * heartbeatDelay;
        initialiseHeartbeating(writerDelay, readerDelay);

        int maxFrameSize = getDefaultMaxFrameSize();
        if (maxFrameSize <= 0)
        {
            maxFrameSize = Integer.MAX_VALUE;
        }

        if (frameMax > 0 && frameMax < maxFrameSize)
        {
            maxFrameSize = (int) frameMax;
        }

        setMaxFrameSize(maxFrameSize);

        //0 means no implied limit, except that forced by protocol limitations (0xFFFF)
        setMaximumNumberOfChannels( ((channelMax == 0l) || (channelMax > 0xFFFFL))
                                            ? 0xFFFF
                                            : (int)channelMax);



        writeFrame(_methodRegistry.createConnectionTuneOkBody(_maxNoOfChannels, maxFrameSize, heartbeatDelay).generateFrame(0));
        writeFrame(_methodRegistry.createConnectionOpenBody(AMQShortString.valueOf(_address.getHostName()), AMQShortString.EMPTY_STRING, false).generateFrame(0));

        changeState(State.AWAIT_TUNE, State.AWAIT_OPEN_OK);


    }

    @Override
    public void receiveConnectionOpenOk(final AMQShortString knownHosts)
    {
        changeState(State.AWAIT_OPEN_OK, State.OPEN);

        createTransferSession();

    }

    private void createTransferSession()
    {
        _channels[TRANSFER_SESSION_ID] = new TransferSession_0_8(TRANSFER_SESSION_ID, this);
    }


    private class ProcessPendingIterator implements Iterator<Runnable>
    {
        private final List<OutboundChannel> _sessionsWithPending;
        private Iterator<OutboundChannel> _sessionIterator;
        private ProcessPendingIterator()
        {
            _sessionsWithPending = new ArrayList<>();
            for(OutboundChannel channel : _channels)
            {
                if(channel != null)
                {
                    _sessionsWithPending.add(channel);
                }
            }
            _sessionIterator = _sessionsWithPending.iterator();
        }

        @Override
        public boolean hasNext()
        {
            return !(_sessionsWithPending.isEmpty() && _pendingTasks.isEmpty());
        }

        @Override
        public Runnable next()
        {
            if(!_sessionsWithPending.isEmpty())
            {
                if(!_sessionIterator.hasNext())
                {
                    _sessionIterator = _sessionsWithPending.iterator();
                }
                final OutboundChannel session = _sessionIterator.next();
                return new Runnable()
                {
                    @Override
                    public void run()
                    {
                        if(!session.processPending())
                        {
                            _sessionIterator.remove();
                        }
                    }
                };
            }
            else if(!_pendingTasks.isEmpty())
            {
                return _pendingTasks.remove(0);
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

}
