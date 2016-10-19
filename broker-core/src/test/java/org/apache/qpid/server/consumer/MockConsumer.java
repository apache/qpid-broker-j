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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.qpid.protocol.AMQConstant;
import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.logging.LogSubject;
import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.ConfiguredObjectFactory;
import org.apache.qpid.server.model.ConfiguredObjectFactoryImpl;
import org.apache.qpid.server.model.Consumer;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.Session;
import org.apache.qpid.server.protocol.AMQSessionModel;
import org.apache.qpid.server.protocol.ConsumerListener;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.util.StateChangeListener;
import org.apache.qpid.transport.network.Ticker;

public class MockConsumer implements ConsumerTarget
{

    private final List<String> _messageIds;
    private boolean _closed = false;
    private String tag = "mocktag";
    private Queue<?> queue = null;
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
    public boolean hasCredit()
    {
        return _state == State.ACTIVE;
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

    @Override
    public boolean isPullOnly()
    {
        return false;
    }

    @Override
    public boolean isMultiQueue()
    {
        return false;
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
        public void notifyConsumerTargetCurrentStates()
        {

        }

        @Override
        public void ensureConsumersNoticedStateChange()
        {

        }

        @Override
        public int compareTo(final AMQSessionModel o)
        {
            return 0;
        }
    }

}
