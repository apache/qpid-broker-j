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

package org.apache.qpid.server.consumer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Type;
import java.net.SocketAddress;
import java.security.AccessControlException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.util.concurrent.ListenableFuture;

import org.apache.qpid.protocol.AMQConstant;
import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.filter.FilterManager;
import org.apache.qpid.server.filter.Filterable;
import org.apache.qpid.server.filter.MessageFilter;
import org.apache.qpid.server.logging.LogSubject;
import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.ConfigurationChangeListener;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObjectFactory;
import org.apache.qpid.server.model.ConfiguredObjectFactoryImpl;
import org.apache.qpid.server.model.Consumer;
import org.apache.qpid.server.model.LifetimePolicy;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.Session;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.protocol.AMQSessionModel;
import org.apache.qpid.server.protocol.ConsumerListener;
import org.apache.qpid.server.store.ConfiguredObjectRecord;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.transport.AbstractAMQPConnection;
import org.apache.qpid.server.queue.AMQQueue;
import org.apache.qpid.server.transport.NetworkConnectionScheduler;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.util.StateChangeListener;
import org.apache.qpid.transport.network.Ticker;

public class MockConsumer implements ConsumerTarget
{

    private final List<String> _messageIds;
    private boolean _closed = false;
    private String tag = "mocktag";
    private AMQQueue queue = null;
    private StateChangeListener<ConsumerTarget, State> _listener = null;
    private State _state = State.ACTIVE;
    private ArrayList<MessageInstance> messages = new ArrayList<MessageInstance>();
    private final Lock _stateChangeLock = new ReentrantLock();

    private boolean _isActive = true;

    public MockConsumer()
    {
        _messageIds = null;
    }

    public MockConsumer(List<String> messageIds)
    {
        _messageIds = messageIds;
    }

    public boolean close()
    {
        _closed = true;
        if (_listener != null)
        {
            _listener.stateChanged(this, _state, State.CLOSED);
        }
        _state = State.CLOSED;
        return true;
    }

    public String getName()
    {
        return tag;
    }

    public FilterManager getFilters()
    {
        if(_messageIds != null)
        {
            FilterManager filters = new FilterManager();
            MessageFilter filter = new MessageFilter()
            {
                @Override
                public String getName()
                {
                    return "";
                }

                @Override
                public boolean startAtTail()
                {
                    return false;
                }

                @Override
                public boolean matches(final Filterable message)
                {
                    final String messageId = message.getMessageHeader().getMessageId();
                    return _messageIds.contains(messageId);
                }
            };
            filters.add(filter.getName(), filter);
            return filters;
        }
        else
        {
            return null;
        }
    }

    public long getUnacknowledgedBytes()
    {
        return 0;  // TODO - Implement
    }

    public long getUnacknowledgedMessages()
    {
        return 0;  // TODO - Implement
    }

    public AMQQueue getQueue()
    {
        return queue;
    }

    public AMQSessionModel getSessionModel()
    {
        return new MockSessionModel();
    }

    public boolean isActive()
    {
        return _isActive ;
    }



    public boolean isClosed()
    {
        return _closed;
    }


    public boolean isSuspended()
    {
        return false;
    }

    public void queueDeleted()
    {
    }

    public void restoreCredit(ServerMessage message)
    {
    }

    public long send(final ConsumerImpl consumer, MessageInstance entry, boolean batch)
    {
        long size = entry.getMessage().getSize();
        if (messages.contains(entry))
        {
            entry.setRedelivered();
        }
        messages.add(entry);
        return size;
    }

    @Override
    public boolean hasMessagesToSend()
    {
        return false;
    }

    @Override
    public void sendNextMessage()
    {

    }

    public void flushBatched()
    {

    }

    @Override
    public void acquisitionRemoved(final MessageInstance node)
    {

    }

    public State getState()
    {
        return _state;
    }

    @Override
    public void consumerAdded(final ConsumerImpl sub)
    {
    }

    @Override
    public void consumerRemoved(final ConsumerImpl sub)
    {
       close();
    }

    @Override
    public void notifyCurrentState()
    {

    }

    public void setState(State state)
    {
        State oldState = _state;
        _state = state;
        if(_listener != null)
        {
            _listener.stateChanged(this, oldState, state);
        }
    }

    @Override
    public void addStateListener(final StateChangeListener<ConsumerTarget, State> listener)
    {
        _listener = listener;
    }

    @Override
    public void removeStateChangeListener(final StateChangeListener<ConsumerTarget, State> listener)
    {
        if(_listener == listener)
        {
            _listener = null;
        }
    }

    @Override
    public boolean processPending()
    {
        return false;
    }

    @Override
    public boolean hasPendingWork()
    {
        return false;
    }

    public ArrayList<MessageInstance> getMessages()
    {
        return messages;
    }


    public void queueEmpty()
    {
    }

    @Override
    public boolean allocateCredit(final ServerMessage msg)
    {
        return true;
    }

    public void setActive(final boolean isActive)
    {
        _isActive = isActive;
    }


    public final boolean trySendLock()
    {
        return _stateChangeLock.tryLock();
    }

    public final void getSendLock()
    {
        _stateChangeLock.lock();
    }

    public final void releaseSendLock()
    {
        _stateChangeLock.unlock();
    }


    private static class MockSessionModel implements AMQSessionModel<MockSessionModel>
    {
        private final UUID _id = UUID.randomUUID();
        private Session _modelObject;

        private MockSessionModel()
        {
            _modelObject = mock(Session.class);
            when(_modelObject.getCategoryClass()).thenReturn(Session.class);
            ConfiguredObjectFactory factory = new ConfiguredObjectFactoryImpl(BrokerModel.getInstance());
            when(_modelObject.getObjectFactory()).thenReturn(factory);
            when(_modelObject.getModel()).thenReturn(factory.getModel());
            TaskExecutor taskExecutor = CurrentThreadTaskExecutor.newStartedInstance();
            when(_modelObject.getTaskExecutor()).thenReturn(taskExecutor);
            when(_modelObject.getChildExecutor()).thenReturn(taskExecutor);
        }

        @Override
        public UUID getId()
        {
            return _id;
        }

        @Override
        public AMQPConnection<?> getAMQPConnection()
        {
            return null;
        }

        @Override
        public String getClientID()
        {
            return null;
        }

        @Override
        public void close()
        {
        }

        @Override
        public LogSubject getLogSubject()
        {
            return null;
        }

        @Override
        public void checkTransactionStatus(long openWarn, long openClose,
                long idleWarn, long idleClose)
        {
        }

        @Override
        public void block(AMQQueue queue)
        {
        }

        @Override
        public void unblock(AMQQueue queue)
        {
        }

        @Override
        public void block()
        {
        }

        @Override
        public void unblock()
        {
        }

        @Override
        public boolean getBlocking()
        {
            return false;
        }

        @Override
        public Object getConnectionReference()
        {
            return this;
        }

        @Override
        public int getUnacknowledgedMessageCount()
        {
            return 0;
        }

        @Override
        public Long getTxnCount()
        {
            return null;
        }

        @Override
        public Long getTxnStart()
        {
            return null;
        }

        @Override
        public Long getTxnCommits()
        {
            return null;
        }

        @Override
        public Long getTxnRejects()
        {
            return null;
        }

        @Override
        public int getChannelId()
        {
            return 0;
        }

        @Override
        public int getConsumerCount()
        {
            return 0;
        }

        @Override
        public Collection<Consumer<?>> getConsumers()
        {
            return null;
        }

        @Override
        public void addConsumerListener(final ConsumerListener listener)
        {

        }

        @Override
        public void setModelObject(final Session session)
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
            return 0;
        }

        @Override
        public long getTransactionUpdateTime()
        {
            return 0;
        }

        @Override
        public void removeConsumerListener(final ConsumerListener listener)
        {

        }

        @Override
        public void close(AMQConstant cause, String message)
        {
        }

        @Override
        public void addDeleteTask(final Action task)
        {

        }

        @Override
        public void removeDeleteTask(final Action task)
        {

        }


        @Override
        public void transportStateChanged()
        {

        }

        @Override
        public boolean processPending()
        {
            return false;
        }

        @Override
        public void addTicker(final Ticker ticker)
        {

        }

        @Override
        public void removeTicker(final Ticker ticker)
        {

        }

        @Override
        public int compareTo(final AMQSessionModel o)
        {
            return 0;
        }
    }

    private static class MockConnectionModel implements AMQPConnection<MockConnectionModel>
    {

        @Override
        public void registerMessageReceived(long messageSize, long timestamp)
        {
        }

        @Override
        public void registerMessageDelivered(long messageSize)
        {
        }

        @Override
        public void closeAsync(AMQConstant cause, String message)
        {
        }

        @Override
        public void closeSessionAsync(AMQSessionModel<?> session, AMQConstant cause,
                                      String message)
        {
        }

        @Override
        public long getConnectionId()
        {
            return 0;
        }

        @Override
        public void block()
        {
        }

        @Override
        public void unblock()
        {

        }

        @Override
        public String getRemoteAddressString()
        {
            return "remoteAddress:1234";
        }

        public SocketAddress getRemoteSocketAddress()
        {
            return null;
        }

        @Override
        public String getClientId()
        {
            return null;
        }

        @Override
        public String getRemoteContainerName()
        {
            return null;
        }

        @Override
        public void notifyWork()
        {

        }

        @Override
        public boolean isMessageAssignmentSuspended()
        {
            return false;
        }

        @Override
        public boolean hasSessionWithName(final byte[] name)
        {
            return false;
        }

        @Override
        public void setScheduler(final NetworkConnectionScheduler networkConnectionScheduler)
        {

        }

        @Override
        public String getClientVersion()
        {
            return null;
        }

        @Override
        public boolean isIncoming()
        {
            return false;
        }

        @Override
        public String getLocalAddress()
        {
            return null;
        }

        @Override
        public String getPrincipal()
        {
            return null;
        }

        @Override
        public String getRemoteAddress()
        {
            return null;
        }

        @Override
        public String getRemoteProcessName()
        {
            return null;
        }

        @Override
        public String getRemoteProcessPid()
        {
            return null;
        }

        @Override
        public long getSessionCountLimit()
        {
            return 0;
        }

        @Override
        public Principal getAuthorizedPrincipal()
        {
            return null;
        }

        @Override
        public AmqpPort<?> getPort()
        {
            return null;
        }

        @Override
        public long getBytesIn()
        {
            return 0;
        }

        @Override
        public long getBytesOut()
        {
            return 0;
        }

        @Override
        public long getMessagesIn()
        {
            return 0;
        }

        @Override
        public long getMessagesOut()
        {
            return 0;
        }

        @Override
        public long getLastIoTime()
        {
            return 0;
        }

        @Override
        public int getSessionCount()
        {
            return 0;
        }

        @Override
        public Collection<Session> getSessions()
        {
            return null;
        }

        @Override
        public AbstractAMQPConnection<?> getUnderlyingConnection()
        {
            return null;
        }

        @Override
        public Transport getTransport()
        {
            return null;
        }

        @Override
        public boolean isConnectionStopped()
        {
            return false;
        }

        @Override
        public String getVirtualHostName()
        {
            return null;
        }

        @Override
        public VirtualHost<?, ?, ?> getVirtualHost()
        {
            return null;
        }

        @Override
        public void addDeleteTask(final Action task)
        {

        }

        @Override
        public void removeDeleteTask(final Action task)
        {

        }


        @Override
        public UUID getId()
        {
            return null;
        }

        @Override
        public String getName()
        {
            return null;
        }

        @Override
        public String getDescription()
        {
            return null;
        }

        @Override
        public String getType()
        {
            return null;
        }

        @Override
        public Map<String, String> getContext()
        {
            return null;
        }

        @Override
        public <T> T getContextValue(final Class<T> clazz, final String propertyName)
        {
            return null;
        }

        @Override
        public <T> T getContextValue(final Class<T> clazz, final Type t, final String propertyName)
        {
            return null;
        }

        @Override
        public Set<String> getContextKeys(final boolean excludeSystem)
        {
            return null;
        }

        @Override
        public String getLastUpdatedBy()
        {
            return null;
        }

        @Override
        public long getLastUpdatedTime()
        {
            return 0;
        }

        @Override
        public String getCreatedBy()
        {
            return null;
        }

        @Override
        public long getCreatedTime()
        {
            return 0;
        }

        @Override
        public org.apache.qpid.server.model.State getDesiredState()
        {
            return null;
        }

        @Override
        public org.apache.qpid.server.model.State getState()
        {
            return null;
        }

        @Override
        public void addChangeListener(final ConfigurationChangeListener listener)
        {

        }

        @Override
        public boolean removeChangeListener(final ConfigurationChangeListener listener)
        {
            return false;
        }

        @Override
        public <T extends ConfiguredObject> T getParent(final Class<T> clazz)
        {
            return null;
        }

        @Override
        public boolean isDurable()
        {
            return false;
        }

        @Override
        public LifetimePolicy getLifetimePolicy()
        {
            return null;
        }

        @Override
        public Collection<String> getAttributeNames()
        {
            return null;
        }

        @Override
        public Object getAttribute(final String name)
        {
            return null;
        }

        @Override
        public Map<String, Object> getActualAttributes()
        {
            return null;
        }

        @Override
        public Object setAttribute(final String name, final Object expected, final Object desired)
                throws IllegalStateException, AccessControlException, IllegalArgumentException
        {
            return null;
        }

        @Override
        public Map<String, Number> getStatistics()
        {
            return null;
        }

        @Override
        public <C extends ConfiguredObject> Collection<C> getChildren(final Class<C> clazz)
        {
            return null;
        }

        @Override
        public <C extends ConfiguredObject> C getChildById(final Class<C> clazz, final UUID id)
        {
            return null;
        }

        @Override
        public <C extends ConfiguredObject> C getChildByName(final Class<C> clazz, final String name)
        {
            return null;
        }

        @Override
        public <C extends ConfiguredObject> C createChild(final Class<C> childClass,
                                                          final Map<String, Object> attributes,
                                                          final ConfiguredObject... otherParents)
        {
            return null;
        }

        @Override
        public <C extends ConfiguredObject> ListenableFuture<C> createChildAsync(final Class<C> childClass,
                                                                                 final Map<String, Object> attributes,
                                                                                 final ConfiguredObject... otherParents)
        {
            return null;
        }

        @Override
        public void setAttributes(final Map<String, Object> attributes)
                throws IllegalStateException, AccessControlException, IllegalArgumentException
        {

        }

        @Override
        public ListenableFuture<Void> setAttributesAsync(final Map<String, Object> attributes)
                throws IllegalStateException, AccessControlException, IllegalArgumentException
        {
            return null;
        }

        @Override
        public Class<? extends ConfiguredObject> getCategoryClass()
        {
            return null;
        }

        @Override
        public Class<? extends ConfiguredObject> getTypeClass()
        {
            return null;
        }

        @Override
        public boolean managesChildStorage()
        {
            return false;
        }

        @Override
        public <C extends ConfiguredObject<C>> C findConfiguredObject(final Class<C> clazz, final String name)
        {
            return null;
        }

        @Override
        public ConfiguredObjectRecord asObjectRecord()
        {
            return null;
        }

        @Override
        public void open()
        {

        }

        @Override
        public ListenableFuture<Void> openAsync()
        {
            return null;
        }

        @Override
        public void close()
        {

        }

        @Override
        public ListenableFuture<Void> closeAsync()
        {
            return null;
        }

        @Override
        public ListenableFuture<Void> deleteAsync()
        {
            return null;
        }

        @Override
        public TaskExecutor getTaskExecutor()
        {
            return null;
        }

        @Override
        public TaskExecutor getChildExecutor()
        {
            return null;
        }

        @Override
        public ConfiguredObjectFactory getObjectFactory()
        {
            return null;
        }

        @Override
        public Model getModel()
        {
            return null;
        }

        @Override
        public void delete()
        {

        }

        @Override
        public void decryptSecrets()
        {

        }
    }
}
