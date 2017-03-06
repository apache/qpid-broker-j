/*
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
 */

package org.apache.qpid.server.protocol.v1_0;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.protocol.v1_0.type.BaseSource;
import org.apache.qpid.server.protocol.v1_0.type.BaseTarget;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Target;
import org.apache.qpid.server.protocol.v1_0.type.messaging.TerminusDurability;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;

public abstract class AbstractLink<T extends LinkEndpoint<?>> implements Link_1_0
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractLink.class);

    protected final String _linkName;
    protected T _linkEndpoint;
    protected volatile BaseSource _source;
    protected volatile BaseTarget _target;

    public AbstractLink(final String linkName)
    {
        _linkName = linkName;
    }

    @Override
    public final ListenableFuture<T> attach(final Session_1_0 session, final Attach attach)
    {
        final ListenableFuture<T> future;
        try
        {
            boolean isAttachingLocalTerminusNull = (attach.getRole() == Role.SENDER ? attach.getTarget() == null : attach.getSource() == null);
            boolean isLocalTerminusNull = (attach.getRole() == Role.SENDER ? getTarget() == null : getSource() == null);

            if (isAttachingLocalTerminusNull)
            {
                future = recoverLink(session, attach);
            }
            else if (isLocalTerminusNull)
            {
                future = establishLink(session, attach);
            }
            else if (_linkEndpoint != null && _linkEndpoint.getSession() != null && !session.equals(_linkEndpoint.getSession()))
            {
                future = stealLink(session, attach);
            }
            else if (attach.getUnsettled() != null)
            {
                future = resumeLink(session, attach);
            }
            else
            {
                future = reattachLink(session, attach);
            }
        }
        catch (Throwable t)
        {
            return Futures.immediateFailedFuture(t);
        }
        AbstractConfiguredObject.addFutureCallback(future, new FutureCallback<T>()
        {
            @Override
            public void onSuccess(final T result)
            {
                _linkEndpoint = result;
            }

            @Override
            public void onFailure(final Throwable t)
            {
            }
        }, MoreExecutors.directExecutor());
        return future;
    }

    protected abstract ListenableFuture<T> recoverLink(final Session_1_0 session, final Attach attach);

    protected abstract ListenableFuture<T> establishLink(final Session_1_0 session, final Attach attach);

    protected abstract ListenableFuture<T> stealLink(final Session_1_0 session, final Attach attach);

    protected abstract ListenableFuture<T> resumeLink(final Session_1_0 session, final Attach attach);

    protected abstract ListenableFuture<T> reattachLink(final Session_1_0 session, final Attach attach);

    @Override
    public void linkClosed()
    {
        discardEndpoint();
    }

    @Override
    public void discardEndpoint()
    {
        _linkEndpoint = null;
    }

    @Override
    public final String getName()
    {
        return _linkName;
    }

    @Override
    public BaseSource getSource()
    {
        return _source;
    }

    @Override
    public BaseTarget getTarget()
    {
        return _target;
    }

    TerminusDurability getLocalTerminusDurability()
    {
        if (_linkEndpoint.getRole() == Role.SENDER)
        {
            return ((Source) getSource()).getDurable();
        }
        else if (getTarget() instanceof Target)
        {
            return ((Target) getTarget()).getDurable();
        }
        else
        {
            return TerminusDurability.NONE;
        }
    }
}
