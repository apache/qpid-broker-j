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

import java.security.AccessControlContext;
import java.util.List;

import javax.security.auth.Subject;

import org.apache.qpid.server.logging.LogSubject;
import org.apache.qpid.server.model.Connection;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.protocol.PublishAuthorisationCache;
import org.apache.qpid.server.session.AbstractAMQPSession;
import org.apache.qpid.server.util.Action;

public class Session_0_10 extends AbstractAMQPSession<Session_0_10, ConsumerTarget_0_10>
        implements LogSubject, org.apache.qpid.server.util.Deletable<Session_0_10>
{
    private final AMQPConnection_0_10 _connection;
    private final ServerSession _serverSession;

    protected Session_0_10(final Connection<?> parent, final int sessionId, final ServerSession serverSession)
    {
        super(parent, sessionId);
        _connection = (AMQPConnection_0_10) parent;
        _serverSession = serverSession;
    }

    @Override
    public String toLogString()
    {
        return _serverSession.toLogString();
    }

    @Override
    public void block(final Queue<?> queue)
    {
        _serverSession.block(queue);
    }

    @Override
    public void unblock(final Queue<?> queue)
    {
        _serverSession.unblock(queue);
    }

    @Override
    public void block()
    {
        _serverSession.block();
    }

    @Override
    public void unblock()
    {
        _serverSession.unblock();
    }

    @Override
    public Object getConnectionReference()
    {
        return _serverSession.getConnectionReference();
    }

    @Override
    public void transportStateChanged()
    {
        for(ConsumerTarget_0_10 consumerTarget : _serverSession.getSubscriptions())
        {
            consumerTarget.transportStateChanged();
        }
        if (!_consumersWithPendingWork.isEmpty() && !getAMQPConnection().isTransportBlockedForWriting())
        {
            getAMQPConnection().notifyWork(this);
        }
    }

    @Override
    protected void updateBlockedStateIfNecessary()
    {
        _serverSession.updateBlockedStateIfNecesssary();
    }

    @Override
    public boolean getBlocking()
    {
        return _serverSession.getBlocking();
    }

    @Override
    public int getUnacknowledgedMessageCount()
    {
        return _serverSession.getUnacknowledgedMessageCount();
    }

    @Override
    public long getTransactionUpdateTimeLong()
    {
        return _serverSession.getTransactionUpdateTimeLong();
    }

    @Override
    public long getTransactionStartTimeLong()
    {
        return _serverSession.getTransactionStartTimeLong();
    }

    public AMQPConnection_0_10 getConnection()
    {
        return _connection;
    }

    public Subject getSubject()
    {
        return _subject;
    }

    AccessControlContext getAccessControllerContext()
    {
        return _accessControllerContext;
    }

    PublishAuthorisationCache getPublishAuthCache()
    {
        return _publishAuthCache;
    }

    List<Action<? super Session_0_10>> getTaskList()
    {
        return _taskList;
    }

    @Override
    public boolean isClosing()
    {
        return _serverSession.isClosing();
    }

    ServerSession getServerSession()
    {
        return _serverSession;
    }
}
