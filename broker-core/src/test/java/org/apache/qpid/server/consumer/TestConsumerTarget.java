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

import java.security.Principal;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;

import org.apache.qpid.server.message.MessageContainer;
import org.apache.qpid.server.message.MessageInstance;
import org.apache.qpid.server.message.MessageInstanceConsumer;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.session.AMQPSession;
import org.apache.qpid.server.transport.AMQPConnection;

@SuppressWarnings({"rawtypes", "unchecked"})
public class TestConsumerTarget implements ConsumerTarget<TestConsumerTarget>
{
    private final Queue<?> queue = null;
    private final ArrayList<MessageInstance> _messages = new ArrayList<>();
    private final AMQPSession<?, ?> _sessionModel = mock(AMQPSession.class);

    private boolean _closed = false;
    private boolean _isActive = true;
    private boolean _notifyDesired;
    private State _state = State.OPEN;
    private MessageInstanceConsumer<?> _consumer;

    public TestConsumerTarget()
    {
        when(_sessionModel.getName()).thenReturn("mock session");

        final Principal principal = mock(Principal.class);
        when(principal.getName()).thenReturn("mock principal");

        final AMQPConnection amqpConnection = mock(AMQPConnection.class);
        when(amqpConnection.getAuthorizedPrincipal()).thenReturn(principal);
        when(amqpConnection.getName()).thenReturn("mock connection");
        when(amqpConnection.getRemoteContainerName()).thenReturn("mock container");

        when(_sessionModel.getChannelId()).thenReturn(0);
        when(_sessionModel.getAMQPConnection()).thenReturn(amqpConnection);
    }

    @Override
    public boolean close()
    {
        _closed = true;
        _state = State.CLOSED;
        updateNotifyWorkDesired();
        return true;
    }

    @Override
    public void queueDeleted(final Queue queue, final MessageInstanceConsumer sub)
    {
        consumerRemoved(sub);
    }

    public String getName()
    {
        return "mocktag";
    }

    @Override
    public long getUnacknowledgedBytes()
    {
        return 0;
    }

    @Override
    public long getUnacknowledgedMessages()
    {
        return 0;
    }

    @Override
    public void resetStatistics()
    {

    }

    public Queue<?> getQueue()
    {
        return queue;
    }

    @Override
    public AMQPSession getSession()
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

    @Override
    public boolean isSuspended()
    {
        return false;
    }

    @Override
    public void restoreCredit(ServerMessage message)
    {

    }

    @Override
    public void send(final MessageInstanceConsumer consumer, MessageInstance entry, boolean batch)
    {
        if (_messages.contains(entry))
        {
            entry.setRedelivered();
        }
        _messages.add(entry);
    }

    @Override
    public boolean sendNextMessage()
    {
        return false;
    }

    @Override
    public void flushBatched()
    {

    }

    @Override
    public void acquisitionRemoved(final MessageInstance node)
    {

    }

    @Override
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
    public CompletableFuture<Void> consumerRemoved(final MessageInstanceConsumer sub)
    {
        close();
        return CompletableFuture.completedFuture(null);
    }

    public void setState(State state)
    {
        _state = state;
        updateNotifyWorkDesired();
    }

    @Override
    public boolean processPending()
    {
        final MessageContainer messageContainer = _consumer.pullMessage();
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

    @Override
    public void noMessagesAvailable()
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
}
