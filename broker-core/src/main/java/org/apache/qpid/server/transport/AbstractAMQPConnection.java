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

import java.net.SocketAddress;
import java.security.AccessController;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.security.auth.Subject;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.protocol.AMQConstant;
import org.apache.qpid.server.connection.ConnectionPrincipal;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.messages.ConnectionMessages;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.Session;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.StateTransition;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.adapter.SessionAdapter;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.protocol.AMQSessionModel;
import org.apache.qpid.server.stats.StatisticsCounter;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.virtualhost.VirtualHostImpl;
import org.apache.qpid.transport.network.AggregateTicker;
import org.apache.qpid.transport.network.NetworkConnection;
import org.apache.qpid.transport.network.Ticker;

public abstract class AbstractAMQPConnection<C extends AbstractAMQPConnection<C>>
        extends AbstractConfiguredObject<C>
        implements ProtocolEngine, AMQPConnection<C>

{
    private static final Logger _logger = LoggerFactory.getLogger(AbstractAMQPConnection.class);

    private final Broker<?> _broker;
    private final NetworkConnection _network;
    private final AmqpPort<?> _port;
    private final Transport _transport;
    private final long _connectionId;
    private final AggregateTicker _aggregateTicker;
    private final Subject _subject = new Subject();
    private final List<Action<? super C>> _connectionCloseTaskList =
            new CopyOnWriteArrayList<>();

    private String _clientProduct;
    private String _clientVersion;
    private String _remoteProcessPid;
    private String _clientId;

    private volatile boolean _stopped;
    private final StatisticsCounter _messagesDelivered, _dataDelivered, _messagesReceived, _dataReceived;
    private final SettableFuture<Void> _transportClosedFuture = SettableFuture.create();
    private final SettableFuture<Void> _modelClosedFuture = SettableFuture.create();
    private final AtomicBoolean _modelClosing = new AtomicBoolean();
    private volatile long _lastReadTime;
    private volatile long _lastWriteTime;

    public AbstractAMQPConnection(Broker<?> broker,
                                  NetworkConnection network,
                                  AmqpPort<?> port,
                                  Transport transport,
                                  long connectionId,
                                  final AggregateTicker aggregateTicker)
    {
        super(parentsMap(port),createAttributes(connectionId, network));

        _broker = broker;
        _network = network;
        _port = port;
        _transport = transport;
        _connectionId = connectionId;
        _aggregateTicker = aggregateTicker;
        _subject.getPrincipals().add(new ConnectionPrincipal(this));
        _messagesDelivered = new StatisticsCounter("messages-delivered-" + getConnectionId());
        _dataDelivered = new StatisticsCounter("data-delivered-" + getConnectionId());
        _messagesReceived = new StatisticsCounter("messages-received-" + getConnectionId());
        _dataReceived = new StatisticsCounter("data-received-" + getConnectionId());

        _transportClosedFuture.addListener(
                new Runnable()
                {
                    @Override
                    public void run()
                    {
                        try
                        {
                            deleted();
                            setState(State.DELETED);
                        }
                        finally
                        {
                            _modelClosedFuture.set(null);
                        }
                    }
                }, getTaskExecutor().getExecutor());

        setState(State.ACTIVE);
    }

    private static Map<String, Object> createAttributes(long connectionId, NetworkConnection network)
    {
        Map<String,Object> attributes = new HashMap<>();
        attributes.put(ID, UUID.randomUUID());
        attributes.put(NAME, "[" + connectionId + "] " + String.valueOf(network.getRemoteAddress()).replaceAll("/", ""));
        attributes.put(DURABLE, false);
        return attributes;
    }

    @Override
    protected void onOpen()
    {
        super.onOpen();
        long maxAuthDelay = _port.getContextValue(Long.class, Port.CONNECTION_MAXIMUM_AUTHENTICATION_DELAY);
        _aggregateTicker.addTicker(new SlowConnectionOpenTicker(maxAuthDelay));
        _lastReadTime = _lastWriteTime = getCreatedTime();

    }

    public final Broker<?> getBroker()
    {
        return _broker;
    }

    public final NetworkConnection getNetwork()
    {
        return _network;
    }

    public final AmqpPort<?> getPort()
    {
        return _port;
    }

    public final Transport getTransport()
    {
        return _transport;
    }

    @Override
    public final AggregateTicker getAggregateTicker()
    {
        return _aggregateTicker;
    }

    public final long getLastIoTime()
    {
        return Math.max(getLastReadTime(), getLastWriteTime());
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

    public final long getConnectionId()
    {
        return _connectionId;
    }

    public final StatisticsCounter getMessageDeliveryStatistics()
    {
        return _messagesDelivered;
    }

    public String getRemoteAddressString()
    {
        return String.valueOf(_network.getRemoteAddress());
    }

    public final void stopConnection()
    {
        _stopped = true;
    }

    public final ProtocolEngine getProtocolEngine()
    {
        return this;
    }

    public boolean isConnectionStopped()
    {
        return _stopped;
    }

    public final String getVirtualHostName()
    {
        return getVirtualHost() == null ? null : getVirtualHost().getName();
    }

    public String getClientVersion()
    {
        return _clientVersion;
    }

    public String getRemoteProcessPid()
    {
        return _remoteProcessPid;
    }

    public void setScheduler(final NetworkConnectionScheduler networkConnectionScheduler)
    {
        ((NonBlockingConnection)_network).changeScheduler(networkConnectionScheduler);
    }

    public String getClientProduct()
    {
        return _clientProduct;
    }

    public void addDeleteTask(final Action<? super C> task)
    {
        _connectionCloseTaskList.add(task);
    }

    public void removeDeleteTask(final Action<? super C> task)
    {
        _connectionCloseTaskList.remove(task);
    }


    protected void performDeleteTasks()
    {
        if(runningAsSubject())
        {
            for (Action<? super C> task : _connectionCloseTaskList)
            {
                task.performAction((C)this);
            }
        }
        else
        {
            runAsSubject(new PrivilegedAction<Object>()
            {
                @Override
                public Object run()
                {
                    performDeleteTasks();
                    return null;
                }
            });
        }
    }

    public String getClientId()
    {
        return _clientId;
    }

    public final StatisticsCounter getDataReceiptStatistics()
    {
        return _dataReceived;
    }

    public final StatisticsCounter getDataDeliveryStatistics()
    {
        return _dataDelivered;
    }

    public final SocketAddress getRemoteSocketAddress()
    {
        return _network.getRemoteAddress();
    }

    public void registerMessageDelivered(long messageSize)
    {
        _messagesDelivered.registerEvent(1L);
        _dataDelivered.registerEvent(messageSize);
        ((VirtualHostImpl<?,?,?>)getVirtualHost()).registerMessageDelivered(messageSize);
    }

    public void registerMessageReceived(long messageSize, long timestamp)
    {
        _messagesReceived.registerEvent(1L, timestamp);
        _dataReceived.registerEvent(messageSize, timestamp);
        ((VirtualHostImpl<?,?,?>)getVirtualHost()).registerMessageReceived(messageSize, timestamp);
    }

    public final void resetStatistics()
    {
        _messagesDelivered.reset();
        _dataDelivered.reset();
        _messagesReceived.reset();
        _dataReceived.reset();
    }

    public final StatisticsCounter getMessageReceiptStatistics()
    {
        return _messagesReceived;
    }

    public void setClientProduct(final String clientProduct)
    {
        _clientProduct = clientProduct;
    }

    public void setClientVersion(final String clientVersion)
    {
        _clientVersion = clientVersion;
    }

    public void setRemoteProcessPid(final String remoteProcessPid)
    {
        _remoteProcessPid = remoteProcessPid;
    }

    public void setClientId(final String clientId)
    {
        _clientId = clientId;
    }

    private <T> T runAsSubject(PrivilegedAction<T> action)
    {
        return Subject.doAs(_subject, action);
    }

    private boolean runningAsSubject()
    {
        return _subject.equals(Subject.getSubject(AccessController.getContext()));
    }

    @Override
    public Subject getSubject()
    {
        return _subject;
    }

    public void sessionAdded(final AMQSessionModel<?> session)
    {
        SessionAdapter adapter = new SessionAdapter(this, session);
        adapter.create();
        childAdded(adapter);

    }

    public void sessionRemoved(final AMQSessionModel<?> session)
    {
    }

    public void virtualHostAssociated()
    {
        getVirtualHost().registerConnection(this);
    }

    @Override
    public boolean isIncoming()
    {
        return true;
    }

    @Override
    public String getLocalAddress()
    {
        return null;
    }

    @Override
    public String getPrincipal()
    {
        final Principal authorizedPrincipal = getAuthorizedPrincipal();
        return authorizedPrincipal == null ? null : authorizedPrincipal.getName();
    }

    @Override
    public String getRemoteAddress()
    {
        return getRemoteAddressString();
    }

    @Override
    public String getRemoteProcessName()
    {
        return null;
    }

    public Collection<Session> getSessions()
    {
        return getChildren(Session.class);
    }

    @SuppressWarnings("unused")
    @StateTransition( currentState = State.ACTIVE, desiredState = State.DELETED)
    private ListenableFuture<Void> doDelete()
    {
        return closeAsyncIfNotAlreadyClosing();
    }

    @Override
    protected ListenableFuture<Void> beforeClose()
    {
        return closeAsyncIfNotAlreadyClosing();
    }

    private ListenableFuture<Void> closeAsyncIfNotAlreadyClosing()
    {
        if (_modelClosing.compareAndSet(false, true))
        {
            sendConnectionCloseAsync(AMQConstant.CONNECTION_FORCED, "Connection closed by external action");
        }
        return _modelClosedFuture;
    }

    @Override
    public <C extends ConfiguredObject> ListenableFuture<C> addChildAsync(Class<C> childClass, Map<String, Object> attributes, ConfiguredObject... otherParents)
    {
        if(childClass == Session.class)
        {
            throw new IllegalStateException();
        }
        else
        {
            throw new IllegalArgumentException("Cannot create a child of class " + childClass.getSimpleName());
        }

    }

    @Override
    public long getBytesIn()
    {
        return getDataReceiptStatistics().getTotal();
    }

    @Override
    public long getBytesOut()
    {
        return getDataDeliveryStatistics().getTotal();
    }

    @Override
    public long getMessagesIn()
    {
        return getMessageReceiptStatistics().getTotal();
    }

    @Override
    public long getMessagesOut()
    {
        return getMessageDeliveryStatistics().getTotal();
    }

    public abstract List<? extends AMQSessionModel<?>> getSessionModels();

    @Override
    public int getSessionCount()
    {
        return getSessionModels().size();
    }

    @Override
    public AbstractAMQPConnection<?> getUnderlyingConnection()
    {
        return this;
    }

    protected void markTransportClosed()
    {
        _transportClosedFuture.set(null);
    }

    protected abstract EventLogger getEventLogger();

    private class SlowConnectionOpenTicker implements Ticker
    {
        private final long _allowedTime;

        public SlowConnectionOpenTicker(long timeoutTime)
        {
            _allowedTime = timeoutTime;
        }

        @Override
        public int getTimeToNextTick(final long currentTime)
        {
            final int timeToNextTick = (int) (getCreatedTime() + _allowedTime - currentTime);
            return timeToNextTick;
        }

        @Override
        public int tick(final long currentTime)
        {
            int nextTick = getTimeToNextTick(currentTime);
            if(nextTick <= 0)
            {
                if (getAuthorizedPrincipal() == null)
                {
                    _logger.warn("Connection has taken more than {} ms to establish identity.  Closing as possible DoS.",
                                 _allowedTime);
                    getEventLogger().message(ConnectionMessages.IDLE_CLOSE());
                    _network.close();
                }
                else
                {
                    _aggregateTicker.removeTicker(this);
                }
            }
            return nextTick;
        }
    }

}
