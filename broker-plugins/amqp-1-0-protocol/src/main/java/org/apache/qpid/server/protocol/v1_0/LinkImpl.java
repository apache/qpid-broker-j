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

import java.util.concurrent.ExecutionException;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.BaseSource;
import org.apache.qpid.server.protocol.v1_0.type.BaseTarget;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Target;
import org.apache.qpid.server.protocol.v1_0.type.messaging.TerminusDurability;
import org.apache.qpid.server.protocol.v1_0.type.transaction.Coordinator;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Error;
import org.apache.qpid.server.protocol.v1_0.type.transport.LinkError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;

public class LinkImpl<S extends BaseSource, T extends BaseTarget> implements Link_1_0<S, T>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LinkImpl.class);

    private final String _remoteContainerId;
    private final String _linkName;
    private final Role _role;
    private final LinkRegistry _linkRegistry;

    private volatile LinkEndpoint<S, T> _linkEndpoint;
    private volatile S _source;
    private volatile T _target;

    public LinkImpl(final String remoteContainerId, final String linkName, final Role role, final LinkRegistry linkRegistry)
    {
        _remoteContainerId = remoteContainerId;
        _linkName = linkName;
        _role = role;
        _linkRegistry = linkRegistry;
    }

    public LinkImpl(final LinkDefinition<S, T> linkDefinition, final LinkRegistry linkRegistry)
    {
        this(linkDefinition.getRemoteContainerId(), linkDefinition.getName(), linkDefinition.getRole(), linkRegistry);
        setTermini(linkDefinition.getSource(), linkDefinition.getTarget());
    }

    @Override
    public final ListenableFuture<? extends LinkEndpoint<S, T>> attach(final Session_1_0 session, final Attach attach)
    {
        try
        {
            if (_role == attach.getRole())
            {
                throw new AmqpErrorException(new Error(AmqpError.ILLEGAL_STATE, "Cannot switch SendingLink to ReceivingLink and vice versa"));
            }


            if (_linkEndpoint != null && !session.equals(_linkEndpoint.getSession()))
            {
                return stealLink(session, attach);
            }
            else
            {
                if (_linkEndpoint == null)
                {
                    _linkEndpoint = createLinkEndpoint(session, attach);
                }

                _linkEndpoint.receiveAttach(attach);
                _linkRegistry.linkChanged(this);
                return Futures.immediateFuture(_linkEndpoint);
            }
        }
        catch (Exception e)
        {
            LOGGER.debug("Error attaching link", e);
            return rejectLink(session, e);
        }
    }

    private synchronized ListenableFuture<LinkEndpoint<S, T>> stealLink(final Session_1_0 session, final Attach attach)
    {
        final SettableFuture<LinkEndpoint<S, T>> returnFuture = SettableFuture.create();
        _linkEndpoint.getSession().doOnIOThreadAsync(
                () ->
                {
                    _linkEndpoint.close(new Error(LinkError.STOLEN,
                                                  String.format("Link is being stolen by connection '%s'",
                                                                session.getConnection())));
                    try
                    {
                        returnFuture.set(attach(session, attach).get());
                    }
                    catch (InterruptedException e)
                    {
                        returnFuture.setException(e);
                        Thread.currentThread().interrupt();
                    }
                    catch (ExecutionException e)
                    {
                        returnFuture.setException(e.getCause());
                    }
                });
        return returnFuture;
    }

    private LinkEndpoint<S, T> createLinkEndpoint(final Session_1_0 session, final Attach attach)
    {
        final LinkEndpoint<S, T> linkEndpoint;
        if (_role == Role.SENDER)
        {
            linkEndpoint = (LinkEndpoint<S, T>) new SendingLinkEndpoint(session, (LinkImpl<Source, Target>) this);
        }
        else if (attach.getTarget() instanceof Coordinator)
        {
            linkEndpoint = (LinkEndpoint<S, T>) new TxnCoordinatorReceivingLinkEndpoint(session, (LinkImpl<Source, Coordinator>) this);
        }
        else
        {
            linkEndpoint = (LinkEndpoint<S, T>) new StandardReceivingLinkEndpoint(session, (LinkImpl<Source, Target>) this);
        }
        return linkEndpoint;
    }

    private ListenableFuture<? extends LinkEndpoint<S, T>> rejectLink(final Session_1_0 session, Throwable t)
    {
        if (t instanceof AmqpErrorException)
        {
            _linkEndpoint = new ErrantLinkEndpoint<>(this, session, ((AmqpErrorException) t).getError());
        }
        else
        {
            _linkEndpoint = new ErrantLinkEndpoint<>(this, session, new Error(AmqpError.INTERNAL_ERROR, t.getMessage()));
        }
        return Futures.immediateFuture(_linkEndpoint);
    }


    @Override
    public void linkClosed()
    {
        discardEndpoint();
        _linkRegistry.linkClosed(this);
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
    public Role getRole()
    {
        return _role;
    }

    @Override
    public S getSource()
    {
        return _source;
    }

    @Override
    public void setSource(S source)
    {
        setTermini(source, _target);
    }

    @Override
    public T getTarget()
    {
        return _target;
    }

    @Override
    public void setTarget(T target)
    {
        setTermini(_source, target);
    }

    @Override
    public void setTermini(S source, T target)
    {
        _source = source;
        _target = target;
    }

    @Override
    public TerminusDurability getHighestSupportedTerminusDurability()
    {
        return _linkRegistry.getHighestSupportedTerminusDurability();
    }

    @Override
    public String getRemoteContainerId()
    {
        return _remoteContainerId;
    }
}
