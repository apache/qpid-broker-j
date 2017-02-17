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
package org.apache.qpid.server.protocol.v0_10;

import static org.apache.qpid.server.protocol.v0_10.ServerConnection.State.CLOSED;
import static org.apache.qpid.server.protocol.v0_10.ServerConnection.State.CLOSING;
import static org.apache.qpid.server.protocol.v0_10.ServerConnection.State.NEW;
import static org.apache.qpid.server.protocol.v0_10.ServerConnection.State.OPEN;

import java.net.SocketAddress;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.security.auth.Subject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.protocol.ConnectionClosingTicker;
import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.session.AMQPSession;
import org.apache.qpid.server.transport.*;
import org.apache.qpid.server.transport.network.NetworkConnection;
import org.apache.qpid.server.transport.util.Waiter;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.util.ServerScopedRuntimeException;

public class ServerConnection extends ConnectionInvoker implements ProtocolEventReceiver, ProtocolEventSender
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerConnection.class);
    private final Broker<?> _broker;

    private final long _connectionId;
    private final Object _reference = new Object();
    private final AmqpPort<?> _port;
    private final AtomicLong _lastIoTime = new AtomicLong();
    final private Map<Binary,ServerSession> sessions = new HashMap<Binary,ServerSession>();
    final private Map<Integer,ServerSession> channels = new ConcurrentHashMap<Integer,ServerSession>();
    final private Object lock = new Object();
    private final AtomicBoolean connectionLost = new AtomicBoolean(false);
    private boolean _blocking;
    private final Transport _transport;

    private final Queue<Action<? super ServerConnection>> _asyncTaskList =
            new ConcurrentLinkedQueue<>();

    private final AMQPConnection_0_10 _amqpConnection;
    private boolean _ignoreFutureInput;
    private boolean _ignoreAllButConnectionCloseOk;
    private NetworkConnection _networkConnection;
    private FrameSizeObserver _frameSizeObserver;
    private ServerConnectionDelegate delegate;
    private ProtocolEventSender sender;
    private State state = NEW;
    private long timeout = 60000;  // TODO server side close does not require this
    private ConnectionException error = null;
    private int channelMax = 1;
    private String locale;
    private SocketAddress _remoteAddress;
    private SocketAddress _localAddress;

    public ServerConnection(final long connectionId,
                            Broker<?> broker,
                            final AmqpPort<?> port,
                            final Transport transport,
                            final AMQPConnection_0_10 serverProtocolEngine)
    {
        _connectionId = connectionId;
        _broker = broker;

        _port = port;
        _transport = transport;
        _amqpConnection = serverProtocolEngine;
    }

    public Object getReference()
    {
        return _reference;
    }

    public Broker<?> getBroker()
    {
        return _broker;
    }

    @Override
    protected void invoke(Method method)
    {
        invokeSuper(method);
        if (method instanceof ConnectionClose)
        {
            _ignoreAllButConnectionCloseOk = true;
        }
    }

    private void invokeSuper(Method method)
    {
        method.setChannel(0);
        send(method);
        if (!method.isBatch())
        {
            flush();
        }
    }


    EventLogger getEventLogger()
    {
        return _amqpConnection.getEventLogger();
    }

    protected void setState(State state)
    {
        setStateSuper(state);

        if(state == State.CLOSING)
        {
            long timeoutTime = System.currentTimeMillis() + getAmqpConnection().getContextValue(Long.class, org.apache.qpid.server.model.Connection.CLOSE_RESPONSE_TIMEOUT);

            getAmqpConnection().getAggregateTicker().addTicker(new ConnectionClosingTicker(timeoutTime, (ServerNetworkConnection) getNetworkConnection()));

            // trigger a wakeup to ensure the ticker will be taken into account
            getAmqpConnection().notifyWork();
        }
    }

    private void setStateSuper(State state)
    {
        synchronized (lock)
        {
            this.state = state;
            lock.notifyAll();
        }
    }


    public ServerConnectionDelegate getConnectionDelegate()
    {
        return (ServerConnectionDelegate) getConnectionDelegateSuper();
    }

    private ServerConnectionDelegate getConnectionDelegateSuper()
    {
        return delegate;
    }

    public AMQPConnection_0_10 getAmqpConnection()
    {
        return _amqpConnection;
    }

    public NamedAddressSpace getAddressSpace()
    {
        return _amqpConnection.getAddressSpace();
    }

    public void setVirtualHost(NamedAddressSpace addressSpace)
    {
        _amqpConnection.setAddressSpace(addressSpace);
    }

    public AmqpPort<?> getPort()
    {
        return _port;
    }

    public Transport getTransport()
    {
        return _transport;
    }

    public void closeSessionAsync(final ServerSession session, final AMQPConnection.CloseReason reason, final String message)
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
        addAsyncTask(new Action<ServerConnection>()
        {

            @Override
            public void performAction(final ServerConnection conn)
            {
                if(!session.isClosing())
                {
                    ExecutionException ex = new ExecutionException();
                    ExecutionErrorCode code = ExecutionErrorCode.INTERNAL_ERROR;
                    try
                    {
                        code = ExecutionErrorCode.get(cause);
                    }
                    catch (IllegalArgumentException iae)
                    {
                        // Ignore, already set to INTERNAL_ERROR
                    }
                    ex.setErrorCode(code);
                    ex.setDescription(message);
                    session.invoke(ex);

                    session.close(cause, message);
                }
            }
        });

    }

    @Override
    public void exception(final Throwable t)
    {
        try
        {
            exceptionSuper(t);
        }
        finally
        {
            if(t instanceof Error)
            {
                throw (Error) t;
            }
            if(t instanceof ServerScopedRuntimeException)
            {
                throw (ServerScopedRuntimeException) t;
            }
        }
    }

    private void exceptionSuper(Throwable t)
    {
        exception(new ConnectionException(t));
    }


    @Override
    public void received(final ProtocolEvent event)
    {
        _lastIoTime.set(System.currentTimeMillis());
        AccessControlContext context;
        if (event.isConnectionControl())
        {
            context = _amqpConnection.getAccessControllerContext();
        }
        else
        {
            ServerSession channel = (ServerSession) getSession(event.getChannel());
            if (channel != null)
            {
                context = channel.getAccessControllerContext();
            }
            else
            {
                context = _amqpConnection.getAccessControllerContext();
            }
        }

        if(!_ignoreAllButConnectionCloseOk || (event instanceof ConnectionCloseOk))
        {
            AccessController.doPrivileged(new PrivilegedAction<Void>()
            {
                @Override
                public Void run()
                {
                    receivedSuper(event);
                    return null;
                }
            }, context);
        }
    }

    private void receivedSuper(ProtocolEvent event)
    {
        if(LOGGER.isDebugEnabled())
        {
            LOGGER.debug("RECV: [{}] {}", this, String.valueOf(event));
        }
        event.delegate(this, delegate);
    }


    void sendConnectionCloseAsync(final ConnectionCloseCode replyCode, final String message)
    {
        addAsyncTask(new Action<ServerConnection>()
        {
            @Override
            public void performAction(final ServerConnection object)
            {
                if(!isClosing())
                {
                    markAllSessionsClosed();

                    setState(CLOSING);
                    sendConnectionClose(replyCode, message);
                }
            }
        });
    }

    protected void performDeleteTasks()
    {
        _amqpConnection.performDeleteTasks();
    }

    public synchronized void block()
    {
        if(!_blocking)
        {
            _blocking = true;
            for(ServerSession ssn : getSessionModels())
            {
                ssn.block();
            }
        }
    }

    public synchronized void unblock()
    {
        if(_blocking)
        {
            _blocking = false;
            for(ServerSession ssn : getSessionModels())
            {
                ssn.unblock();
            }
        }
    }

    public synchronized void registerSession(final ServerSession ssn)
    {
        registerSessionSuper(ssn);
        if(_blocking)
        {
            ((ServerSession)ssn).block();
        }
    }

    private void registerSessionSuper(ServerSession ssn)
    {
        synchronized (lock)
        {
            sessions.put(ssn.getName(),ssn);
        }
    }

    public Collection<? extends ServerSession> getSessionModels()
    {
        return Collections.unmodifiableCollection(getChannels());
    }

    protected Collection<ServerSession> getChannels()
    {
        return  (Collection<ServerSession>) getChannelsSuper();
    }

    private Collection<ServerSession> getChannelsSuper()
    {
        return new ArrayList<>(channels.values());
    }


    public void setAuthorizedSubject(final Subject authorizedSubject)
    {
        _amqpConnection.setSubject(authorizedSubject);
    }

    public Principal getAuthorizedPrincipal()
    {
        return _amqpConnection.getAuthorizedPrincipal();
    }

    public long getConnectionId()
    {
        return _connectionId;
    }

    @Override
    public void closed()
    {
        try
        {
            performDeleteTasks();
            closedSuper();
        }
        finally
        {
            NamedAddressSpace addressSpace = getAddressSpace();
            if(addressSpace != null)
            {
                addressSpace.deregisterConnection(_amqpConnection);
            }
        }

    }

    private void closedSuper()
    {
        if (state == OPEN)
        {
            exception(new ConnectionException("connection aborted"));
        }

        LOGGER.debug("connection closed: {}", this);

        synchronized (lock)
        {
            List<ServerSession> values = new ArrayList<ServerSession>(channels.values());
            for (ServerSession ssn : values)
            {
                ssn.closed();
            }

            try
            {
                sender.close();
            }
            catch(Exception e)
            {
                // ignore.
            }
            sender = null;
            setState(CLOSED);
        }
    }

    private void markAllSessionsClosed()
    {
        for (ServerSession ssn :  getChannels())
        {
            final ServerSession session = (ServerSession) ssn;
            ((ServerSession) ssn).setClose(true);
            session.closed();
        }
    }

    public void receivedComplete()
    {
        for (ServerSession ssn : getChannels())
        {
            ((ServerSession)ssn).receivedComplete();
        }
    }

    @Override
    public void send(ProtocolEvent event)
    {
        _lastIoTime.set(System.currentTimeMillis());
        sendSuper(event);
    }

    private void sendSuper(ProtocolEvent event)
    {
        if(LOGGER.isDebugEnabled())
        {
            LOGGER.debug("SEND: [{}] {}", this, String.valueOf(event));
        }
        ProtocolEventSender s = sender;
        if (s == null)
        {
            throw new ConnectionException("connection closed");
        }
        s.send(event);
    }


    public String getRemoteContainerName()
    {
        return getConnectionDelegate().getClientId();
    }


    public long getSessionCountLimit()
    {
        return getChannelMax();
    }


    void addAsyncTask(final Action<? super ServerConnection> action)
    {
        _asyncTaskList.add(action);
        getAmqpConnection().notifyWork();
    }

    public int getMessageCompressionThreshold()
    {
        return _amqpConnection.getMessageCompressionThreshold();
    }

    public int getMaxMessageSize()
    {
        return (int)Math.min(_amqpConnection.getMaxMessageSize(), (long)Integer.MAX_VALUE);
    }

    public void transportStateChanged()
    {
        for (ServerSession ssn : getSessionModels())
        {
            ssn.getModelObject().transportStateChanged();
        }
    }

    public Iterator<Runnable> processPendingIterator(final Set<AMQPSession<?,?>> sessionsWithWork)
    {
        return new ProcessPendingIterator(sessionsWithWork);
    }

    public void setConnectionDelegate(ServerConnectionDelegate delegate)
    {
        this.delegate = delegate;
    }

    public ProtocolEventSender getSender()
    {
        return sender;
    }

    public void setSender(ProtocolEventSender sender)
    {
        this.sender = sender;
    }

    protected void setLocale(String locale)
    {
        this.locale = locale;
    }

    String getLocale()
    {
        return locale;
    }

    public void removeSession(ServerSession ssn)
    {
        synchronized (lock)
        {
            sessions.remove(ssn.getName());
        }
    }

    @Override
    public void flush()
    {
        if(LOGGER.isDebugEnabled())
        {
            LOGGER.debug("FLUSH: [{}]", this);
        }
        final ProtocolEventSender theSender = sender;
        if(theSender != null)
        {
            theSender.flush();
        }
    }

    public void dispatch(Method method)
    {
        int channel = method.getChannel();
        ServerSession ssn = getSession(channel);
        if(ssn != null)
        {
            ssn.received(method);
        }
        else
        {
            /*
             * A peer receiving any other control on a detached transport MUST discard it and
             * send a session.detached with the "not-attached" reason code.
             */
            if(LOGGER.isDebugEnabled())
            {
                LOGGER.debug("Control received on unattached channel : {}", channel);
            }
            invokeSessionDetached(channel, SessionDetachCode.NOT_ATTACHED);
        }
    }

    public int getChannelMax()
    {
        return channelMax;
    }

    protected void setChannelMax(int max)
    {
        channelMax = max;
    }

    private int map(ServerSession ssn)
    {
        synchronized (lock)
        {
            //For a negotiated channelMax N, there are channels 0 to N-1 available.
            for (int i = 0; i < getChannelMax(); i++)
            {
                if (!channels.containsKey(i))
                {
                    map(ssn, i);
                    return i;
                }
            }

            throw new RuntimeException("no more channels available");
        }
    }

    protected void map(ServerSession ssn, int channel)
    {
        synchronized (lock)
        {
            channels.put(channel, ssn);
            ssn.setChannel(channel);
        }
    }

    void unmap(ServerSession ssn)
    {
        synchronized (lock)
        {
            channels.remove(ssn.getChannel());
        }
    }

    public ServerSession getSession(int channel)
    {
        synchronized (lock)
        {
            return channels.get(channel);
        }
    }

    public void resume()
    {
        synchronized (lock)
        {
            for (ServerSession ssn : sessions.values())
            {
                map(ssn);
                ssn.resume();
            }

            setState(OPEN);
        }
    }

    public void exception(ConnectionException e)
    {
        connectionLost.set(true);
        synchronized (lock)
        {
            switch (state)
            {
            case OPENING:
            case CLOSING:
                error = e;
                lock.notifyAll();
                return;
            }
        }
    }

    public void closeCode(ConnectionClose close)
    {
        synchronized (lock)
        {
            ConnectionCloseCode code = close.getReplyCode();
            if (code != ConnectionCloseCode.NORMAL)
            {
                exception(new ConnectionException(close));
            }
        }
    }

    @Override
    public void close()
    {
        close(ConnectionCloseCode.NORMAL, null);
    }

    protected void sendConnectionClose(ConnectionCloseCode replyCode, String replyText, Option... _options)
    {
        connectionClose(replyCode, replyText, _options);
    }

    public void close(ConnectionCloseCode replyCode, String replyText, Option ... _options)
    {
        synchronized (lock)
        {
            switch (state)
            {
            case OPEN:
                state = CLOSING;
                connectionClose(replyCode, replyText, _options);
                Waiter w = new Waiter(lock, timeout);
                while (w.hasTime() && state == CLOSING && error == null)
                {
                    w.await();
                }

                if (error != null)
                {
                    close(replyCode, replyText, _options);
                    throw new ConnectionException(error);
                }

                switch (state)
                {
                case CLOSING:
                    close(replyCode, replyText, _options);
                    throw new ConnectionException("close() timed out");
                case CLOSED:
                    break;
                default:
                    throw new IllegalStateException(String.valueOf(state));
                }
                break;
            case CLOSED:
                break;
            default:
                if (sender != null)
                {
                    sender.close();
                    w = new Waiter(lock, timeout);
                    while (w.hasTime() && sender != null && error == null)
                    {
                        w.await();
                    }

                    if (error != null)
                    {
                        throw new ConnectionException(error);
                    }

                    if (sender != null)
                    {
                        throw new ConnectionException("close() timed out");
                    }
                }
                break;
            }
        }
    }

    @Override
    public String toString()
    {
        return String.format("conn:%x", System.identityHashCode(this));
    }

    protected boolean isConnectionLost()
    {
        return connectionLost.get();
    }

    public boolean hasSessionWithName(final byte[] name)
    {
        return sessions.containsKey(new Binary(name));
    }

    public SocketAddress getRemoteSocketAddress()
    {
        return _remoteAddress;
    }

    public SocketAddress getLocalAddress()
    {
        return _localAddress;
    }

    protected void setRemoteAddress(SocketAddress remoteAddress)
    {
        _remoteAddress = remoteAddress;
    }

    protected void setLocalAddress(SocketAddress localAddress)
    {
        _localAddress = localAddress;
    }

    private void invokeSessionDetached(int channel, SessionDetachCode sessionDetachCode)
    {
        SessionDetached sessionDetached = new SessionDetached();
        sessionDetached.setChannel(channel);
        sessionDetached.setCode(sessionDetachCode);
        invoke(sessionDetached);
    }

    protected void doHeartBeat()
    {
        connectionHeartbeat();
    }

    public void setNetworkConnection(NetworkConnection network)
    {
        _networkConnection = network;
    }

    public NetworkConnection getNetworkConnection()
    {
        return _networkConnection;
    }

    public void setMaxFrameSize(final int maxFrameSize)
    {
        if(_frameSizeObserver != null)
        {
            _frameSizeObserver.setMaxFrameSize(maxFrameSize);
        }
    }

    public void addFrameSizeObserver(final FrameSizeObserver frameSizeObserver)
    {
        if(_frameSizeObserver == null)
        {
            _frameSizeObserver = frameSizeObserver;
        }
        else
        {
            final FrameSizeObserver currentObserver = _frameSizeObserver;
            _frameSizeObserver = new FrameSizeObserver()
                                    {
                                        @Override
                                        public void setMaxFrameSize(final int frameSize)
                                        {
                                            currentObserver.setMaxFrameSize(frameSize);
                                            frameSizeObserver.setMaxFrameSize(frameSize);
                                        }
                                    };
        }
    }

    public boolean isClosing()
    {
        synchronized (lock)
        {
            return state == CLOSING || state == CLOSED;
        }
    }

    protected void sendConnectionSecure(byte[] challenge, Option ... options)
    {
        super.connectionSecure(challenge, options);
    }

    protected void sendConnectionTune(int channelMax, int maxFrameSize, int heartbeatMin, int heartbeatMax, Option ... options)
    {
        super.connectionTune(channelMax, maxFrameSize, heartbeatMin, heartbeatMax, options);
    }

    protected void sendConnectionStart(final Map<String, Object> clientProperties,
                                       final List<Object> mechanisms,
                                       final List<Object> locales, final Option... options)
    {
        super.connectionStart(clientProperties, mechanisms, locales, options);
    }

    public enum State { NEW, CLOSED, OPENING, OPEN, CLOSING, CLOSE_RCVD, RESUMING }

    private class ProcessPendingIterator implements Iterator<Runnable>
    {
        private final Collection<AMQPSession<?,?>> _sessionsWithPending;
        private Iterator<? extends AMQPSession<?,?>> _sessionIterator;
        private ProcessPendingIterator(final Set<AMQPSession<?,?>> sessionsWithWork)
        {
            _sessionsWithPending = sessionsWithWork;
            _sessionIterator = _sessionsWithPending.iterator();
        }

        @Override
        public boolean hasNext()
        {
            return (!_sessionsWithPending.isEmpty() && !isClosing() && !_amqpConnection.isConnectionStopped())
                   || !_asyncTaskList.isEmpty();
        }

        @Override
        public Runnable next()
        {
            if(!_sessionsWithPending.isEmpty())
            {
                if(isClosing() || _amqpConnection.isConnectionStopped())
                {
                    final Action<? super ServerConnection> asyncAction = _asyncTaskList.poll();
                    if(asyncAction != null)
                    {
                        return new Runnable()
                        {
                            @Override
                            public void run()
                            {
                                asyncAction.performAction(ServerConnection.this);
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
                        _sessionIterator = _sessionsWithPending.iterator();
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
                                _sessionsWithPending.add(session);
                            }
                        }
                    };
                }
            }
            else if(!_asyncTaskList.isEmpty())
            {
                final Action<? super ServerConnection> asyncAction = _asyncTaskList.poll();
                return new Runnable()
                {
                    @Override
                    public void run()
                    {
                        asyncAction.performAction(ServerConnection.this);
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

    public void closeAndIgnoreFutureInput()
    {
        _ignoreFutureInput = true;
        getSender().close();
    }

    public boolean isIgnoreFutureInput()
    {
        return _ignoreFutureInput;
    }

}
