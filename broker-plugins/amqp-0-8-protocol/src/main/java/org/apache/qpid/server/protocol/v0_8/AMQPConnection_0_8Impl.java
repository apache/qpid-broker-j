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
package org.apache.qpid.server.protocol.v0_8;

import static java.nio.charset.StandardCharsets.US_ASCII;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.BufferUnderflowException;
import java.security.AccessControlException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.QpidException;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.logging.messages.ConnectionMessages;
import org.apache.qpid.server.message.InstanceProperties;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.Connection;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.plugin.ConnectionPropertyEnricher;
import org.apache.qpid.server.properties.ConnectionStartProperties;
import org.apache.qpid.server.protocol.ConnectionClosingTicker;
import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.protocol.ProtocolVersion;
import org.apache.qpid.server.protocol.v0_8.transport.*;
import org.apache.qpid.server.security.SubjectCreator;
import org.apache.qpid.server.security.auth.SubjectAuthenticationResult;
import org.apache.qpid.server.security.auth.sasl.SaslNegotiator;
import org.apache.qpid.server.session.AMQPSession;
import org.apache.qpid.server.transport.AbstractAMQPConnection;
import org.apache.qpid.server.transport.AggregateTicker;
import org.apache.qpid.server.transport.ByteBufferSender;
import org.apache.qpid.server.transport.ProtocolEngine;
import org.apache.qpid.server.transport.ServerNetworkConnection;
import org.apache.qpid.server.transport.TransportException;
import org.apache.qpid.server.txn.LocalTransaction;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.server.virtualhost.NoopConnectionEstablishmentPolicy;
import org.apache.qpid.server.virtualhost.VirtualHostUnavailableException;

public class AMQPConnection_0_8Impl
        extends AbstractAMQPConnection<AMQPConnection_0_8Impl, AMQPConnection_0_8Impl>
        implements ServerMethodProcessor<ServerChannelMethodProcessor>, AMQPConnection_0_8<AMQPConnection_0_8Impl>
{

    enum ConnectionState
    {
        INIT,
        AWAIT_START_OK,
        AWAIT_SECURE_OK,
        AWAIT_TUNE_OK,
        AWAIT_OPEN,
        OPEN
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(AMQPConnection_0_8Impl.class);

    private static final String BROKER_DEBUG_BINARY_DATA_LENGTH = "broker.debug.binaryDataLength";
    private static final int DEFAULT_DEBUG_BINARY_DATA_LENGTH = 80;

    private final AtomicBoolean _stateChanged = new AtomicBoolean();
    private final AtomicReference<Action<ProtocolEngine>> _workListener = new AtomicReference<>();

    private final Object _channelAddRemoveLock = new Object();
    private final Map<Integer, AMQChannel> _channelMap = new ConcurrentHashMap<>();

    private volatile ConnectionState _state = ConnectionState.INIT;

    /**
     * The channels that the latest call to {@link ProtocolEngine#received(QpidByteBuffer)} applied to.
     * Used so we know which channels we need to call {@link AMQChannel#receivedComplete()}
     * on after handling the frames.
     */
    private final Set<AMQChannel> _channelsForCurrentMessage = Collections.newSetFromMap(new ConcurrentHashMap<AMQChannel, Boolean>());

    private final ServerDecoder _decoder;

    private volatile SaslNegotiator _saslNegotiator;

    private volatile int _maxNoOfChannels;

    private volatile ProtocolVersion _protocolVersion;
    private volatile MethodRegistry _methodRegistry;

    private final Queue<Action<? super AMQPConnection_0_8Impl>> _asyncTaskList =
            new ConcurrentLinkedQueue<>();

    private final Map<Integer, Long> _closingChannelsList = new ConcurrentHashMap<>();
    private volatile ProtocolOutputConverter _protocolOutputConverter;

    private final Object _reference = new Object();

    private volatile int _maxFrameSize;
    private final AtomicBoolean _orderlyClose = new AtomicBoolean(false);

    private final ByteBufferSender _sender;

    private volatile boolean _deferFlush;
    /** Guarded by _channelAddRemoveLock */
    private boolean _blocking;

    private volatile boolean _closeWhenNoRoute;
    private volatile boolean _compressionSupported;

    /**
     * QPID-6744 - Older queue clients (<=0.32) set the nowait flag false on the queue.delete method and then
     * incorrectly await regardless.  If we detect an old Qpid client, we send the queue.delete-ok response regardless
     * of the queue.delete flag request made by the client.
     */
    private volatile boolean _sendQueueDeleteOkRegardless;
    private final Pattern _sendQueueDeleteOkRegardlessClientVerRegexp;

    private volatile int _currentClassId;
    private volatile int _currentMethodId;
    private final int _binaryDataLimit;
    private volatile boolean _transportBlockedForWriting;
    private volatile SubjectAuthenticationResult _successfulAuthenticationResult;

    private final Set<AMQPSession<?,?>> _sessionsWithWork =
            Collections.newSetFromMap(new ConcurrentHashMap<AMQPSession<?,?>, Boolean>());

    private volatile int _heartBeatDelay;
    private volatile String _closeCause;
    private volatile int _closeCauseCode;


    public AMQPConnection_0_8Impl(Broker<?> broker,
                                  ServerNetworkConnection network,
                                  AmqpPort<?> port,
                                  Transport transport,
                                  Protocol protocol,
                                  long connectionId,
                                  AggregateTicker aggregateTicker)
    {
        super(broker, network, port, transport, protocol, connectionId, aggregateTicker);


        _maxNoOfChannels = port.getSessionCountLimit();
        _decoder = new BrokerDecoder(this);
        _binaryDataLimit = getBroker().getContextKeys(false).contains(BROKER_DEBUG_BINARY_DATA_LENGTH)
                ? getBroker().getContextValue(Integer.class, BROKER_DEBUG_BINARY_DATA_LENGTH)
                : DEFAULT_DEBUG_BINARY_DATA_LENGTH;
        String sendQueueDeleteOkRegardlessRegexp = getBroker().getContextKeys(false).contains(Broker.SEND_QUEUE_DELETE_OK_REGARDLESS_CLIENT_VER_REGEXP)
                ? getBroker().getContextValue(String.class, Broker.SEND_QUEUE_DELETE_OK_REGARDLESS_CLIENT_VER_REGEXP): "";
        _sendQueueDeleteOkRegardlessClientVerRegexp = Pattern.compile(sendQueueDeleteOkRegardlessRegexp);

        _sender = network.getSender();
        _closeWhenNoRoute = port.getCloseWhenNoRoute();
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
            for (AMQChannel channel : _channelMap.values())
            {
                channel.transportStateChanged();
            }
        }
    }

    public void setMaxFrameSize(int frameMax)
    {
        _maxFrameSize = frameMax;
        _decoder.setMaxFrameSize(frameMax);
    }

    public long getMaxFrameSize()
    {
        return _maxFrameSize;
    }

    private int getDefaultMaxFrameSize()
    {
        Broker<?> broker = getBroker();

        // QPID-6784 : Some old clients send payload with size equals to max frame size
        // we want to fit those frames into the network buffer
        return broker.getNetworkBufferSize() - AMQFrame.getFrameOverhead();
    }

    @Override
    public boolean isClosing()
    {
        return _orderlyClose.get();
    }

    @Override
    public ClientDeliveryMethod createDeliveryMethod(int channelId)
    {
        return new WriteDeliverMethod(channelId);
    }

    @Override
    protected void onReceive(final QpidByteBuffer msg)
    {
        try
        {
            _decoder.decodeBuffer(msg);
            receivedCompleteAllChannels();
        }
        catch (AMQFrameDecodingException | IOException | AMQPInvalidClassException
                | IllegalArgumentException | IllegalStateException | BufferUnderflowException e)
        {
            LOGGER.warn("Unexpected exception", e);
            throw new ConnectionScopedRuntimeException(e);
        }
    }

    private void receivedCompleteAllChannels()
    {
        RuntimeException exception = null;

        for (AMQChannel channel : _channelsForCurrentMessage)
        {
            try
            {
                channel.receivedComplete();
            }
            catch(RuntimeException exceptionForThisChannel)
            {
                if(exception == null)
                {
                    exception = exceptionForThisChannel;
                }
                LOGGER.info("Error informing channel that receiving is complete. Channel: " + channel,
                              exceptionForThisChannel);
            }
        }

        _channelsForCurrentMessage.clear();

        if(exception != null)
        {
            throw exception;
        }
    }


    void channelRequiresSync(final AMQChannel amqChannel)
    {
        _channelsForCurrentMessage.add(amqChannel);
    }

    private synchronized void protocolInitiationReceived(ProtocolInitiation pi)
    {
        // this ensures the codec never checks for a PI message again
        _decoder.setExpectProtocolInitiation(false);
        try
        {
            ProtocolVersion pv = pi.checkVersion(); // Fails if not correct
            setProtocolVersion(pv);

            StringBuilder mechanismBuilder = new StringBuilder();
            for(String mechanismName : getPort().getAuthenticationProvider().getAvailableMechanisms(getTransport().isSecure()))
            {
                if(mechanismBuilder.length() != 0)
                {
                    mechanismBuilder.append(' ');
                }
                mechanismBuilder.append(mechanismName);
            }
            String mechanisms = mechanismBuilder.toString();

            String locales = "en_US";

            Map<String,Object> props = Collections.emptyMap();
            for(ConnectionPropertyEnricher enricher : getPort().getConnectionPropertyEnrichers())
            {
                props = enricher.addConnectionProperties(this, props);
            }

            FieldTable serverProperties = FieldTable.convertToFieldTable(props);

            AMQMethodBody responseBody = getMethodRegistry().createConnectionStartBody((short) getProtocolMajorVersion(),
                                                                                       (short) pv.getActualMinorVersion(),
                                                                                       serverProperties,
                                                                                       mechanisms.getBytes(US_ASCII),
                                                                                       locales.getBytes(US_ASCII));
            writeFrame(responseBody.generateFrame(0));
            _state = ConnectionState.AWAIT_START_OK;

            _sender.flush();

        }
        catch (QpidException e)
        {
            LOGGER.debug("Received unsupported protocol initiation for protocol version: {} ", getProtocolVersion());

            writeFrame(new ProtocolInitiation(ProtocolVersion.getLatestSupportedVersion()));
            _sender.flush();
        }
    }

    @Override
    public synchronized void writeFrame(AMQDataBlock frame)
    {
        if(LOGGER.isDebugEnabled())
        {
            LOGGER.debug("SEND: " + frame);
        }

        frame.writePayload(_sender);


        updateLastWriteTime();

        if(!_deferFlush)
        {
            _sender.flush();
        }
    }

    public AMQChannel getChannel(int channelId)
    {
        final AMQChannel channel = _channelMap.get(channelId);
        if ((channel == null) || channel.isClosing())
        {
            return null;
        }
        else
        {
            return channel;
        }
    }

    @Override
    public boolean channelAwaitingClosure(int channelId)
    {
        return ignoreAllButCloseOk() || (!_closingChannelsList.isEmpty()
                                         && _closingChannelsList.containsKey(channelId));
    }

    private void addChannel(AMQChannel channel)
    {
        synchronized (_channelAddRemoveLock)
        {
            _channelMap.put(channel.getChannelId(), channel);
            if(_blocking)
            {
                channel.block();
            }
        }
    }

    private void removeChannel(int channelId)
    {
        synchronized (_channelAddRemoveLock)
        {
            _channelMap.remove(channelId);
        }
    }


    @Override
    public void closeChannel(AMQChannel channel)
    {
        closeChannel(channel, 0, null, false);
    }

    @Override
    public void closeChannelAndWriteFrame(AMQChannel channel, int cause, String message)
    {
        writeFrame(new AMQFrame(channel.getChannelId(),
                                getMethodRegistry().createChannelCloseBody(cause,
                                                                           AMQShortString.validValueOf(message),
                                                                           _currentClassId,
                                                                           _currentMethodId)));
        closeChannel(channel, cause, message, true);
    }

    public void closeChannel(int channelId, int cause, String message)
    {
        final AMQChannel channel = getChannel(channelId);
        if (channel == null)
        {
            throw new IllegalArgumentException("Unknown channel id");
        }
        closeChannel(channel, cause, message, true);
    }

    void closeChannel(AMQChannel channel, int cause, String message, boolean mark)
    {
        int channelId = channel.getChannelId();
        try
        {
            channel.close(cause, message);
            if(mark)
            {
                markChannelAwaitingCloseOk(channelId);
            }
        }
        finally
        {
            removeChannel(channelId);
        }
    }


    @Override
    public void closeChannelOk(int channelId)
    {
        _closingChannelsList.remove(channelId);
    }

    private void markChannelAwaitingCloseOk(int channelId)
    {
        _closingChannelsList.put(channelId, System.currentTimeMillis());
    }

    private void closeAllChannels()
    {
        try
        {
            RuntimeException firstException = null;
            for (AMQChannel channel : getSessionModels())
            {
                try
                {
                    channel.close(_closeCauseCode, _closeCause);
                }
                catch (RuntimeException re)
                {
                    if (!(re instanceof ConnectionScopedRuntimeException))
                    {
                        LOGGER.error("Unexpected exception closing channel", re);
                    }
                    firstException = re;
                }
            }

            if (firstException != null)
            {
                throw firstException;
            }
        }
        finally
        {
            synchronized (_channelAddRemoveLock)
            {
                _channelMap.clear();
            }
        }
    }

    private void completeAndCloseAllChannels()
    {
        try
        {
            receivedCompleteAllChannels();
        }
        finally
        {
            closeAllChannels();
        }
    }

    @Override
    public void sendConnectionClose(int errorCode,
                                    String message, int channelId)
    {
        sendConnectionClose(channelId, new AMQFrame(0, new ConnectionCloseBody(getProtocolVersion(), errorCode, AMQShortString.validValueOf(message), _currentClassId, _currentMethodId)));
    }

    private void sendConnectionClose(int channelId, AMQFrame frame)
    {
        if (_orderlyClose.compareAndSet(false, true))
        {
            try
            {
                markChannelAwaitingCloseOk(channelId);
                completeAndCloseAllChannels();
            }
            finally
            {
                try
                {
                    writeFrame(frame);
                }
                finally
                {
                    final long timeoutTime = System.currentTimeMillis()
                                             + getContextValue(Long.class, Connection.CLOSE_RESPONSE_TIMEOUT);

                    getAggregateTicker().addTicker(new ConnectionClosingTicker(timeoutTime, getNetwork()));
                    // trigger a wakeup to ensure the ticker will be taken into account
                    notifyWork();
                }
            }
        }
    }

    public void closeNetworkConnection()
    {
        getNetwork().close();
    }

    @Override
    public boolean isSendQueueDeleteOkRegardless()
    {
        return _sendQueueDeleteOkRegardless;
    }

    void setSendQueueDeleteOkRegardless(boolean sendQueueDeleteOkRegardless)
    {
        _sendQueueDeleteOkRegardless = sendQueueDeleteOkRegardless;
    }

    private void setClientProperties(FieldTable clientProperties)
    {
        if (clientProperties != null)
        {
            Object closeWhenNoRoute = clientProperties.get(ConnectionStartProperties.QPID_CLOSE_WHEN_NO_ROUTE);
            if (closeWhenNoRoute != null)
            {
                _closeWhenNoRoute = Boolean.parseBoolean(String.valueOf(closeWhenNoRoute));
                LOGGER.debug("Client set closeWhenNoRoute={} for connection {}", _closeWhenNoRoute, this);
            }
            Object compressionSupported = clientProperties.get(ConnectionStartProperties.QPID_MESSAGE_COMPRESSION_SUPPORTED);
            if (compressionSupported != null)
            {
                _compressionSupported = Boolean.parseBoolean(String.valueOf(compressionSupported));
                LOGGER.debug("Client set compressionSupported={} for connection {}", _compressionSupported, this);
            }

            String clientId = Objects.toString(clientProperties.get(ConnectionStartProperties.CLIENT_ID_0_8), null);
            String clientVersion = Objects.toString(clientProperties.get(ConnectionStartProperties.VERSION_0_8), null);
            String clientProduct = Objects.toString(clientProperties.get(ConnectionStartProperties.PRODUCT), null);
            String remoteProcessPid = Objects.toString(clientProperties.get(ConnectionStartProperties.PID), null);

            boolean mightBeQpidClient = clientProduct != null &&
                                        (clientProduct.toLowerCase().contains("qpid") || clientProduct.toLowerCase() .equals("unknown"));
            boolean sendQueueDeleteOkRegardless = mightBeQpidClient &&(clientVersion == null || _sendQueueDeleteOkRegardlessClientVerRegexp
                    .matcher(clientVersion).matches());

            setSendQueueDeleteOkRegardless(sendQueueDeleteOkRegardless);
            if (sendQueueDeleteOkRegardless)
            {
                LOGGER.debug("Peer is an older Qpid client, queue delete-ok response will be sent"
                              + " regardless for connection {}", this);
            }

            setClientVersion(clientVersion);
            setClientProduct(clientProduct);
            setRemoteProcessPid(remoteProcessPid);
            setClientId(clientId == null ? UUID.randomUUID().toString() : clientId);
        }
    }

    private void setProtocolVersion(ProtocolVersion pv)
    {
        // TODO MultiVersionProtocolEngine takes responsibility for making the ProtocolVersion determination.
        // These steps could be moved to construction.
        _protocolVersion = pv;
        _methodRegistry = new MethodRegistry(_protocolVersion);
        _protocolOutputConverter = new ProtocolOutputConverterImpl(this);
    }

    public byte getProtocolMajorVersion()
    {
        return _protocolVersion.getMajorVersion();
    }

    @Override
    public ProtocolVersion getProtocolVersion()
    {
        return _protocolVersion;
    }

    public byte getProtocolMinorVersion()
    {
        return _protocolVersion.getMinorVersion();
    }

    public MethodRegistry getRegistry()
    {
        return getMethodRegistry();
    }

    @Override
    public ProtocolOutputConverter getProtocolOutputConverter()
    {
        return _protocolOutputConverter;
    }

    @Override
    public MethodRegistry getMethodRegistry()
    {
        return _methodRegistry;
    }

    @Override
    public void closed()
    {
        try
        {
            try
            {
                if (!_orderlyClose.get())
                {
                    completeAndCloseAllChannels();
                }
            }
            finally
            {
                performDeleteTasks();

                final NamedAddressSpace virtualHost = getAddressSpace();
                if (virtualHost != null)
                {
                    virtualHost.deregisterConnection(this);
                }

            }
        }
        catch (ConnectionScopedRuntimeException | TransportException e)
        {
            LOGGER.error("Could not close protocol engine", e);
        }
        finally
        {
            markTransportClosed();
        }
    }

    @Override
    protected boolean isOrderlyClose()
    {
        return _orderlyClose.get();
    }

    @Override
    protected String getCloseCause()
    {
        if (_closeCause == null)
        {
            return null;
        }
        return _closeCauseCode + " - " + _closeCause;
    }

    @Override
    public void encryptedTransport()
    {
    }

    @Override
    public void readerIdle()
    {
        AccessController.doPrivileged(new PrivilegedAction<Object>()
        {
            @Override
            public Object run()
            {
                getEventLogger().message(ConnectionMessages.IDLE_CLOSE("Current connection state: " + _state, true));
                getNetwork().close();
                return null;
            }
        }, getAccessControllerContext());
    }

    @Override
    public synchronized void writerIdle()
    {
        writeFrame(HeartbeatBody.FRAME);
    }

    @Override
    public int getSessionCountLimit()
    {
        return _maxNoOfChannels;
    }

    @Override
    public int getHeartbeatDelay()
    {
        return _heartBeatDelay;
    }

    public String getAddress()
    {
        return String.valueOf(getNetwork().getRemoteAddress());
    }

    @Override
    public void closeSessionAsync(final AMQPSession<?,?> session, final CloseReason reason, final String message)
    {
        final int cause;
        switch (reason)
        {
            case MANAGEMENT:
                cause = ErrorCodes.CONNECTION_FORCED;
                break;
            case TRANSACTION_TIMEOUT:
                cause = ErrorCodes.RESOURCE_ERROR;
                break;
            default:
                cause = ErrorCodes.INTERNAL_ERROR;
        }
        addAsyncTask(new Action<AMQPConnection_0_8Impl>()
        {

            @Override
            public void performAction(final AMQPConnection_0_8Impl object)
            {
                int channelId = session.getChannelId();
                closeChannel(channelId, cause, message);

                MethodRegistry methodRegistry = getMethodRegistry();
                ChannelCloseBody responseBody =
                        methodRegistry.createChannelCloseBody(
                                cause,
                                AMQShortString.validValueOf(message),
                                0, 0);

                writeFrame(responseBody.generateFrame(channelId));
            }
        });

    }

    @Override
    public void sendConnectionCloseAsync(final CloseReason reason, final String description)
    {
        stopConnection();
        final int cause;
        switch(reason)
        {
            case MANAGEMENT:
                cause = ErrorCodes.CONNECTION_FORCED;
                break;
            case TRANSACTION_TIMEOUT:
                cause = ErrorCodes.RESOURCE_ERROR;
                break;
            default:
                cause = ErrorCodes.INTERNAL_ERROR;
        }
        _closeCauseCode = cause;
        _closeCause = description;
        Action<AMQPConnection_0_8Impl> action = new Action<AMQPConnection_0_8Impl>()
        {
            @Override
            public void performAction(final AMQPConnection_0_8Impl object)
            {
                AMQConnectionException e = new AMQConnectionException(cause, description, 0, 0,
                                                                      getMethodRegistry(),
                                                                      null);
                sendConnectionClose(0, e.getCloseFrame());
            }
        };
        addAsyncTask(action);
    }

    @Override
    protected void addAsyncTask(final Action<? super AMQPConnection_0_8Impl> action)
    {
        _asyncTaskList.add(action);
        notifyWork();
    }

    @Override
    public void block()
    {
        synchronized (_channelAddRemoveLock)
        {
            if(!_blocking)
            {
                _blocking = true;
                for(AMQChannel channel : _channelMap.values())
                {
                    channel.block();
                }
            }
        }
    }

    @Override
    public void unblock()
    {
        synchronized (_channelAddRemoveLock)
        {
            if(_blocking)
            {
                _blocking = false;
                for(AMQChannel channel : _channelMap.values())
                {
                    channel.unblock();
                }
            }
        }
    }

    @Override
    public Collection<? extends AMQChannel> getSessionModels()
    {
        return Collections.unmodifiableCollection(_channelMap.values());
    }

    @Override
    public String getRemoteContainerName()
    {
        return getClientId();
    }


    @Override
    public void setDeferFlush(boolean deferFlush)
    {
        _deferFlush = deferFlush;
    }

    @Override
    public boolean hasSessionWithName(final byte[] name)
    {
        return false;
    }

    @Override
    public Iterator<ServerTransaction> getOpenTransactions()
    {
        return _channelMap.values()
                          .stream()
                          .filter(channel -> channel.getTransaction() instanceof LocalTransaction)
                          .map(AMQChannel::getTransaction)
                          .iterator();
    }

    @Override
    public void receiveChannelOpen(final int channelId)
    {
        if(LOGGER.isDebugEnabled())
        {
            LOGGER.debug("RECV[" + channelId + "] ChannelOpen");
        }
        assertState(ConnectionState.OPEN);

        // Protect the broker against out of order frame request.
        final NamedAddressSpace virtualHost = getAddressSpace();
        if (virtualHost == null)
        {
            sendConnectionClose(ErrorCodes.COMMAND_INVALID,
                                "Virtualhost has not yet been set. ConnectionOpen has not been called.", channelId);
        }
        else if(getChannel(channelId) != null || channelAwaitingClosure(channelId))
        {
            sendConnectionClose(ErrorCodes.CHANNEL_ERROR, "Channel " + channelId + " already exists", channelId);
        }
        else if(channelId > getSessionCountLimit())
        {
            sendConnectionClose(ErrorCodes.CHANNEL_ERROR,
                                "Channel " + channelId + " cannot be created as the max allowed channel id is "
                                + getSessionCountLimit(),
                                channelId);
        }
        else
        {
            LOGGER.debug("Connecting to: {}", virtualHost.getName());

            final AMQChannel channel = new AMQChannel(this, channelId, virtualHost.getMessageStore());
            channel.create();

            addChannel(channel);

            ChannelOpenOkBody response;


            response = getMethodRegistry().createChannelOpenOkBody();


            writeFrame(response.generateFrame(channelId));
        }
    }

    void assertState(final ConnectionState requiredState)
    {
        if(_state != requiredState)
        {
            String replyText = "Command Invalid, expected " + requiredState + " but was " + _state;
            sendConnectionClose(ErrorCodes.COMMAND_INVALID, replyText, 0);
            throw new ConnectionScopedRuntimeException(replyText);
        }
    }

    @Override
    public void receiveConnectionOpen(AMQShortString virtualHostName,
                                      AMQShortString capabilities,
                                      boolean insist)
    {
        if(LOGGER.isDebugEnabled())
        {
            LOGGER.debug("RECV ConnectionOpen[" +" virtualHost: " + virtualHostName + " capabilities: " + capabilities + " insist: " + insist + " ]");
        }

        assertState(ConnectionState.AWAIT_OPEN);

        String virtualHostStr = AMQShortString.toString(virtualHostName);
        if ((virtualHostStr != null) && virtualHostStr.charAt(0) == '/')
        {
            virtualHostStr = virtualHostStr.substring(1);
        }

        NamedAddressSpace addressSpace = ((AmqpPort)getPort()).getAddressSpace(virtualHostStr);

        if (addressSpace == null)
        {
            sendConnectionClose(ErrorCodes.NOT_FOUND,
                    "Unknown virtual host: '" + virtualHostName + "'", 0);

        }
        else
        {
            // Check virtualhost access
            if (!addressSpace.isActive())
            {
                String redirectHost = addressSpace.getRedirectHost(getPort());
                if(redirectHost != null)
                {
                    sendConnectionClose(0, new AMQFrame(0, new ConnectionRedirectBody(getProtocolVersion(), AMQShortString.valueOf(redirectHost), null)));
                }
                else
                {
                    sendConnectionClose(ErrorCodes.CONNECTION_FORCED,
                            "Virtual host '" + addressSpace.getName() + "' is not active", 0);
                }

            }
            else
            {
                try
                {
                    addressSpace.registerConnection(this, new NoopConnectionEstablishmentPolicy());
                    setAddressSpace(addressSpace);

                    if(addressSpace.authoriseCreateConnection(this))
                    {
                        MethodRegistry methodRegistry = getMethodRegistry();
                        AMQMethodBody responseBody = methodRegistry.createConnectionOpenOkBody(virtualHostName);

                        writeFrame(responseBody.generateFrame(0));
                        _state = ConnectionState.OPEN;
                    }
                    else
                    {
                        sendConnectionClose(ErrorCodes.ACCESS_REFUSED, "Connection refused", 0);
                    }
                }
                catch (AccessControlException | VirtualHostUnavailableException e)
                {
                    sendConnectionClose(ErrorCodes.ACCESS_REFUSED, e.getMessage(), 0);
                }
            }
        }
    }

    @Override
    public void receiveConnectionClose(final int replyCode,
                                       final AMQShortString replyText,
                                       final int classId,
                                       final int methodId)
    {
        if(LOGGER.isDebugEnabled())
        {
            LOGGER.debug("RECV ConnectionClose[" +" replyCode: " + replyCode + " replyText: " + replyText + " classId: " + classId + " methodId: " + methodId + " ]");
        }

        try
        {
            if (_orderlyClose.compareAndSet(false, true))
            {
                completeAndCloseAllChannels();
            }

            MethodRegistry methodRegistry = getMethodRegistry();
            ConnectionCloseOkBody responseBody = methodRegistry.createConnectionCloseOkBody();
            writeFrame(responseBody.generateFrame(0));
        }
        catch (Exception e)
        {
            LOGGER.error("Error closing connection for " + getRemoteAddressString(), e);
        }
        finally
        {
            closeNetworkConnection();
        }
    }

    @Override
    public void receiveConnectionCloseOk()
    {
        if(LOGGER.isDebugEnabled())
        {
            LOGGER.debug("RECV ConnectionCloseOk");
        }

        closeNetworkConnection();
    }

    @Override
    public void receiveConnectionSecureOk(final byte[] response)
    {
        if(LOGGER.isDebugEnabled())
        {
            LOGGER.debug("RECV ConnectionSecureOk[ response: ******** ] ");
        }

        assertState(ConnectionState.AWAIT_SECURE_OK);

        processSaslResponse(response, getSubjectCreator());
    }


    private void disposeSaslNegotiator()
    {
        _saslNegotiator.dispose();
        _saslNegotiator = null;
    }

    @Override
    public void receiveConnectionStartOk(final FieldTable clientProperties,
                                         final AMQShortString mechanism,
                                         final byte[] response,
                                         final AMQShortString locale)
    {
        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("RECV ConnectionStartOk["
                          + " clientProperties: "
                          + clientProperties
                          + " mechanism: "
                          + mechanism
                          + " response: ********"
                          + " locale: "
                          + locale
                          + " ]");
        }

        assertState(ConnectionState.AWAIT_START_OK);

        LOGGER.debug("SASL Mechanism selected: {} Locale : {}", mechanism, locale);

        if (mechanism == null || mechanism.length() == 0)
        {
            sendConnectionClose(ErrorCodes.CONNECTION_FORCED, "No Sasl mechanism was specified", 0);
            return;
        }

        SubjectCreator subjectCreator = getSubjectCreator();
        _saslNegotiator = subjectCreator.createSaslNegotiator(String.valueOf(mechanism), this);
        if (_saslNegotiator == null)
        {
            sendConnectionClose(ErrorCodes.CONNECTION_FORCED, "No SaslServer could be created for mechanism: " + mechanism, 0);
        }
        else
        {
            setClientProperties(clientProperties);
            processSaslResponse(response, subjectCreator);
        }
    }

    private void processSaslResponse(final byte[] response, final SubjectCreator subjectCreator)
    {
        MethodRegistry methodRegistry = getMethodRegistry();
        SubjectAuthenticationResult authResult = _successfulAuthenticationResult;
        byte[] challenge = null;
        if (authResult == null)
        {
            authResult = subjectCreator.authenticate(_saslNegotiator, response);
            challenge = authResult.getChallenge();
        }

        switch (authResult.getStatus())
        {
            case ERROR:
                Exception cause = authResult.getCause();

                LOGGER.debug("Authentication failed: {}", (cause == null ? "" : cause.getMessage()));

                sendConnectionClose(ErrorCodes.NOT_ALLOWED, "Authentication failed", 0);

                disposeSaslNegotiator();
                break;

            case SUCCESS:
                _successfulAuthenticationResult = authResult;
                if (challenge == null || challenge.length == 0)
                {
                    LOGGER.debug("Connected as: {}", authResult.getSubject());
                    setSubject(authResult.getSubject());

                    int frameMax = getDefaultMaxFrameSize();

                    if (frameMax <= 0)
                    {
                        frameMax = Integer.MAX_VALUE;
                    }

                    ConnectionTuneBody tuneBody =
                            methodRegistry.createConnectionTuneBody(getPort().getSessionCountLimit(),
                                                                    frameMax,
                                                                    getPort().getHeartbeatDelay());
                    writeFrame(tuneBody.generateFrame(0));
                    _state = ConnectionState.AWAIT_TUNE_OK;
                    disposeSaslNegotiator();
                }
                else
                {
                    continueSaslNegotiation(challenge);
                }
                break;
            case CONTINUE:
                continueSaslNegotiation(challenge);
                break;
        }
    }

    private void continueSaslNegotiation(final byte[] challenge)
    {
        ConnectionSecureBody secureBody = getMethodRegistry().createConnectionSecureBody(challenge);
        writeFrame(secureBody.generateFrame(0));

        _state = ConnectionState.AWAIT_SECURE_OK;
    }

    @Override
    public void receiveConnectionTuneOk(final int channelMax, final long frameMax, final int heartbeat)
    {
        if(LOGGER.isDebugEnabled())
        {
            LOGGER.debug("RECV ConnectionTuneOk[" +" channelMax: " + channelMax + " frameMax: " + frameMax + " heartbeat: " + heartbeat + " ]");
        }

        assertState(ConnectionState.AWAIT_TUNE_OK);

        if (heartbeat > 0)
        {
            _heartBeatDelay = heartbeat;
            long writerDelay = 1000L * heartbeat;
            long readerDelay = 1000L * getContextValue(Integer.class, AMQPConnection_0_8.PROPERTY_HEARTBEAT_TIMEOUT_FACTOR) * heartbeat;
            initialiseHeartbeating(writerDelay, readerDelay);
        }

        int brokerFrameMax = getDefaultMaxFrameSize();
        if (brokerFrameMax <= 0)
        {
            brokerFrameMax = Integer.MAX_VALUE;
        }

        if (frameMax > (long) brokerFrameMax)
        {
            sendConnectionClose(ErrorCodes.SYNTAX_ERROR,
                    "Attempt to set max frame size to " + frameMax
                            + " greater than the broker will allow: "
                            + brokerFrameMax, 0);
        }
        else if (frameMax > 0 && frameMax < AMQDecoder.FRAME_MIN_SIZE)
        {
            sendConnectionClose(ErrorCodes.SYNTAX_ERROR,
                                "Attempt to set max frame size to " + frameMax
                                + " which is smaller than the specification defined minimum: "
                                + AMQDecoder.FRAME_MIN_SIZE, 0);
        }
        else
        {
            int calculatedFrameMax = frameMax == 0 ? brokerFrameMax : (int) frameMax;
            setMaxFrameSize(calculatedFrameMax);

            //0 means no implied limit, except that forced by protocol limitations (0xFFFF)
            int value = ((channelMax == 0) || (channelMax > 0xFFFF))
                                               ? 0xFFFF
                                               : channelMax;
            _maxNoOfChannels = value;
        }
        _state = ConnectionState.AWAIT_OPEN;

    }

    @Override
    public int getBinaryDataLimit()
    {
        return _binaryDataLimit;
    }

    public final class WriteDeliverMethod
            implements ClientDeliveryMethod
    {
        private final int _channelId;

        public WriteDeliverMethod(int channelId)
        {
            _channelId = channelId;
        }

        @Override
        public long deliverToClient(final ConsumerTarget_0_8 target, final AMQMessage message,
                                    final InstanceProperties props, final long deliveryTag)
        {
            long size = _protocolOutputConverter.writeDeliver(message,
                                                  props,
                                                  _channelId,
                                                  deliveryTag,
                                                  target.getConsumerTag());
            registerMessageDelivered(size);
            if (target.getChannel().isTransactional())
            {
                registerTransactedMessageDelivered();
            }
            return size;
        }

    }

    @Override
    public Object getReference()
    {
        return _reference;
    }

    @Override
    public boolean isCloseWhenNoRoute()
    {
        return _closeWhenNoRoute;
    }

    public boolean isCompressionSupported()
    {
        return _compressionSupported && getBroker().isMessageCompressionEnabled();
    }

    private SubjectCreator getSubjectCreator()
    {
        return getPort().getSubjectCreator(getTransport().isSecure(), getNetwork().getSelectedHost());
    }

    @Override
    public ServerChannelMethodProcessor getChannelMethodProcessor(final int channelId)
    {
        assertState(ConnectionState.OPEN);

        ServerChannelMethodProcessor channelMethodProcessor = getChannel(channelId);
        if(channelMethodProcessor == null)
        {
            channelMethodProcessor = (ServerChannelMethodProcessor) Proxy.newProxyInstance(ServerMethodDispatcher.class.getClassLoader(),
                                                            new Class[] { ServerChannelMethodProcessor.class }, new InvocationHandler()
                    {
                        @Override
                        public Object invoke(final Object proxy, final Method method, final Object[] args)
                                throws Throwable
                        {
                            if (method.getName().equals("receiveChannelCloseOk") && channelAwaitingClosure(channelId))
                            {
                                closeChannelOk(channelId);
                            }
                            else if(method.getName().startsWith("receive"))
                            {
                                sendConnectionClose(ErrorCodes.CHANNEL_ERROR,
                                        "Unknown channel id: " + channelId, channelId);
                            }
                            else if(method.getName().equals("ignoreAllButCloseOk"))
                            {
                                return channelAwaitingClosure(channelId);
                            }
                            return null;
                        }
                    });
        }
        return channelMethodProcessor;
    }

    @Override
    public void receiveHeartbeat()
    {
        if(LOGGER.isDebugEnabled())
        {
            LOGGER.debug("RECV Heartbeat");
        }

        // No op
    }

    @Override
    public void receiveProtocolHeader(final ProtocolInitiation protocolInitiation)
    {

        if(LOGGER.isDebugEnabled())
        {
            LOGGER.debug("RECV ProtocolHeader [" + protocolInitiation + " ]");
        }

        protocolInitiationReceived(protocolInitiation);
    }

    @Override
    public void setCurrentMethod(final int classId, final int methodId)
    {
        _currentClassId = classId;
        _currentMethodId = methodId;
    }

    @Override
    public boolean ignoreAllButCloseOk()
    {
        return isClosing();
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
    public Iterator<Runnable> processPendingIterator()
    {
        if (!isIOThread())
        {
            return Collections.emptyIterator();
        }
        return new ProcessPendingIterator();
    }

    @Override
    protected boolean isOpeningInProgress()
    {
        switch (_state)
        {
            case INIT:
            case AWAIT_START_OK:
            case AWAIT_SECURE_OK:
            case AWAIT_TUNE_OK:
            case AWAIT_OPEN:
                return true;
            case OPEN:
                return false;
            default:
                throw new IllegalStateException("Unsupported state " + _state);
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
            return (!_sessionsWithWork.isEmpty() && !isClosing() && !isConnectionStopped()) || !_asyncTaskList.isEmpty();
        }

        @Override
        public Runnable next()
        {
            if(!_sessionsWithWork.isEmpty())
            {
                if(isClosing() || isConnectionStopped())
                {
                    final Action<? super AMQPConnection_0_8Impl> asyncAction = _asyncTaskList.poll();
                    if(asyncAction != null)
                    {
                        return new Runnable()
                        {
                            @Override
                            public void run()
                            {
                                asyncAction.performAction(AMQPConnection_0_8Impl.this);
                            }
                        };
                    }
                    else
                    {
                        // in case the connection was marked as closing between a call to hasNext() and
                        // a subsequent call to next()
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
                    final AMQPSession<?,?> session = _sessionIterator.next();
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
                final Action<? super AMQPConnection_0_8Impl> asyncAction = _asyncTaskList.poll();
                return new Runnable()
                {
                    @Override
                    public void run()
                    {
                        asyncAction.performAction(AMQPConnection_0_8Impl.this);
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
}
