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

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.logging.LogSubject;
import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.message.MessageInstanceConsumer;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.ConfiguredObjectFactory;
import org.apache.qpid.server.model.ConfiguredObjectFactoryImpl;
import org.apache.qpid.server.model.Consumer;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.Session;
import org.apache.qpid.server.protocol.AMQSessionModel;
import org.apache.qpid.server.protocol.ConsumerListener;
import org.apache.qpid.server.message.MessageContainer;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.transport.network.Ticker;

public class TestConsumerTarget implements ConsumerTarget
{

    private boolean _closed = false;
    private String tag = "mocktag";
    private Queue<?> queue = null;
    private State _state = State.OPEN;
    private ArrayList<MessageInstance> _messages = new ArrayList<MessageInstance>();

    private boolean _isActive = true;
    private MessageInstanceConsumer _consumer;
    private MockSessionModel _sessionModel = new MockSessionModel();
    private boolean _notifyDesired;

    public boolean close()
    {
        _closed = true;
        _state = State.CLOSED;
        updateNotifyWorkDesired();
        return true;
    }

    public String getName()
    {
        return tag;
    }

    public long getUnacknowledgedBytes()
    {
        return 0;
    }

    public long getUnacknowledgedMessages()
    {
        return 0;
    }

    public Queue<?> getQueue()
    {
        return queue;
    }

    public AMQSessionModel getSessionModel()
    {
        return _sessionModel;
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

    public void restoreCredit(ServerMessage message)
    {
    }

    public long send(final MessageInstanceConsumer consumer, MessageInstance entry, boolean batch)
    {
        long size = entry.getMessage().getSize();
        if (_messages.contains(entry))
        {
            entry.setRedelivered();
        }
        _messages.add(entry);
        return size;
    }

    @Override
    public boolean sendNextMessage()
    {
        return false;
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
    public String getTargetAddress()
    {
        return getName();
    }

    @Override
    public void consumerAdded(final MessageInstanceConsumer sub)
    {
        _consumer = sub;
    }

    @Override
    public ListenableFuture<Void> consumerRemoved(final MessageInstanceConsumer sub)
    {
       close();
        return Futures.immediateFuture(null);
    }

    public void setState(State state)
    {
        _state = state;
        updateNotifyWorkDesired();
    }

    @Override
    public boolean processPending()
    {
        MessageContainer messageContainer = _consumer.pullMessage();
        if (messageContainer == null)
        {
            return false;
        }

        send(_consumer, messageContainer.getMessageInstance(), false);
        return true;
    }

    public ArrayList<MessageInstance> getMessages()
    {
        return _messages;
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


    @Override
    public boolean isMultiQueue()
    {
        return false;
    }

    @Override
    public void notifyWork()
    {

    }

    @Override
    public boolean isNotifyWorkDesired()
    {
        return _state == State.OPEN;
    }

    @Override
    public void updateNotifyWorkDesired()
    {
        if (isNotifyWorkDesired() != _notifyDesired && _consumer != null)
        {
            _consumer.setNotifyWorkDesired(isNotifyWorkDesired());
            _notifyDesired = isNotifyWorkDesired();
        }
    }

    private static class MockSessionModel implements AMQSessionModel<MockSessionModel>
    {
        private final UUID _id = UUID.randomUUID();
        private Session _modelObject;
        private AMQPConnection<?> _connection = mock(AMQPConnection.class);

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
            return _connection;
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
        public void doTimeoutAction(final String reason)
        {
        }

        @Override
        public void block(Queue<?> queue)
        {
        }

        @Override
        public void unblock(Queue<?> queue)
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
        public long getTxnStart()
        {
            return 0L;
        }

        @Override
        public long getTxnCommits()
        {
            return 0L;
        }

        @Override
        public long getTxnRejects()
        {
            return 0L;
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
        public void notifyWork(final ConsumerTarget target)
        {
            _connection.notifyWork(this);
        }

        @Override
        public int compareTo(final AMQSessionModel o)
        {
            return 0;
        }
    }

}
