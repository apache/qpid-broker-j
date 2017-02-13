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
package org.apache.qpid.server.transport;

import static org.apache.qpid.server.transport.Connection.State.CLOSED;
import static org.apache.qpid.server.transport.Connection.State.CLOSING;
import static org.apache.qpid.server.transport.Connection.State.NEW;
import static org.apache.qpid.server.transport.Connection.State.OPEN;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.transport.network.NetworkConnection;
import org.apache.qpid.server.transport.util.Waiter;


/**
 * Connection
 *
 * @author Rafael H. Schloming
 *
 * TODO the channels map should probably be replaced with something
 * more efficient, e.g. an array or a map implementation that can use
 * short instead of Short
 */

public class Connection extends ConnectionInvoker
    implements ProtocolEventReceiver, ProtocolEventSender
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Connection.class);

    private long _lastSendTime;
    private long _lastReadTime;
    private NetworkConnection _networkConnection;
    private FrameSizeObserver _frameSizeObserver;

    public enum State { NEW, CLOSED, OPENING, OPEN, CLOSING, CLOSE_RCVD, RESUMING }

    private ConnectionDelegate delegate;
    private ProtocolEventSender sender;

    final private Map<Binary,Session> sessions = new HashMap<Binary,Session>();
    final private Map<Integer,Session> channels = new ConcurrentHashMap<Integer,Session>();

    private State state = NEW;
    final private Object lock = new Object();
    private long timeout = 60000;  // TODO server side close does not require this
    private ConnectionException error = null;

    private int channelMax = 1;
    private String locale;

    private final AtomicBoolean connectionLost = new AtomicBoolean(false);

    private SocketAddress _remoteAddress;
    private SocketAddress _localAddress;

    public Connection() {}

    public void setConnectionDelegate(ConnectionDelegate delegate)
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

    protected void setState(State state)
    {
        synchronized (lock)
        {
            this.state = state;
            lock.notifyAll();
        }
    }

    protected void setLocale(String locale)
    {
        this.locale = locale;
    }

    String getLocale()
    {
        return locale;
    }

    public void registerSession(Session ssn)
    {
        synchronized (lock)
        {
            sessions.put(ssn.getName(),ssn);
        }
    }

    public void removeSession(Session ssn)
    {
        synchronized (lock)
        {
            sessions.remove(ssn.getName());
        }
    }

    public ConnectionDelegate getConnectionDelegate()
    {
        return delegate;
    }

    public void received(ProtocolEvent event)
    {
        _lastReadTime = System.currentTimeMillis();
        if(LOGGER.isDebugEnabled())
        {
            LOGGER.debug("RECV: [{}] {}", this, String.valueOf(event));
        }
        event.delegate(this, delegate);
    }

    public void send(ProtocolEvent event)
    {
        _lastSendTime = System.currentTimeMillis();
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

    protected void invoke(Method method)
    {
        method.setChannel(0);
        send(method);
        if (!method.isBatch())
        {
            flush();
        }
    }

    public void dispatch(Method method)
    {
        int channel = method.getChannel();
        Session ssn = getSession(channel);
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

    private int map(Session ssn)
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

    protected void map(Session ssn, int channel)
    {
        synchronized (lock)
        {
            channels.put(channel, ssn);
            ssn.setChannel(channel);
        }
    }

    void unmap(Session ssn)
    {
        synchronized (lock)
        {
            channels.remove(ssn.getChannel());
        }
    }

    public Session getSession(int channel)
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
            for (Session ssn : sessions.values())
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

    public void exception(Throwable t)
    {
        exception(new ConnectionException(t));
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

    public void closed()
    {
        if (state == OPEN)
        {
            exception(new ConnectionException("connection aborted"));
        }

        LOGGER.debug("connection closed: {}", this);

        synchronized (lock)
        {
            List<Session> values = new ArrayList<Session>(channels.values());
            for (Session ssn : values)
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

    public void close()
    {
        close(ConnectionCloseCode.NORMAL, null);
    }


    protected void sendConnectionClose(ConnectionCloseCode replyCode, String replyText, Option ... _options)
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

    protected Collection<? extends Session> getChannels()
    {
        return new ArrayList<>(channels.values());
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

}
