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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.protocol.LinkModel;
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
import org.apache.qpid.server.util.Action;

public class LinkImpl<S extends BaseSource, T extends BaseTarget> implements Link_1_0<S, T>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LinkImpl.class);

    private final String _remoteContainerId;
    private final String _linkName;
    private final Role _role;
    private final LinkRegistry<S, T> _linkRegistry;
    private final Queue<ThiefInformation> _thiefQueue = new LinkedList<>();

    private volatile LinkEndpoint<S, T> _linkEndpoint;
    private volatile S _source;
    private volatile T _target;
    private boolean _stealingInProgress;
    private final Queue<Action<? super Link_1_0<S, T>>> _deleteTasks = new ConcurrentLinkedQueue<>();

    public LinkImpl(final String remoteContainerId,
                    final String linkName,
                    final Role role,
                    final LinkRegistry<S, T> linkRegistry)
    {
        _remoteContainerId = remoteContainerId;
        _linkName = linkName;
        _role = role;
        _linkRegistry = linkRegistry;
    }

    public LinkImpl(final LinkDefinition<S, T> linkDefinition, final LinkRegistry<S, T> linkRegistry)
    {
        this(linkDefinition.getRemoteContainerId(), linkDefinition.getName(), linkDefinition.getRole(), linkRegistry);
        setTermini(linkDefinition.getSource(), linkDefinition.getTarget());
    }

    @Override
    public final synchronized ListenableFuture<? extends LinkEndpoint<S, T>> attach(final Session_1_0 session, final Attach attach)
    {
        try
        {
            if (_role == attach.getRole())
            {
                throw new AmqpErrorException(new Error(AmqpError.ILLEGAL_STATE, "Cannot switch SendingLink to ReceivingLink and vice versa"));
            }

            if (_linkEndpoint != null && !session.equals(_linkEndpoint.getSession()))
            {
                SettableFuture<LinkEndpoint<S, T>> future = SettableFuture.create();
                _thiefQueue.add(new ThiefInformation(session, attach, future));
                startLinkStealingIfNecessary();
                return future;
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

    @Override
    public synchronized void linkClosed()
    {
        Iterator<Action<? super Link_1_0<S, T>>> iterator = _deleteTasks.iterator();
        while (iterator.hasNext())
        {
            final Action<? super Link_1_0<S, T>> deleteTask = iterator.next();
            deleteTask.performAction(this);
            iterator.remove();
        }
        discardEndpoint();
        _linkRegistry.linkClosed(this);
    }

    @Override
    public synchronized void discardEndpoint()
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

    private LinkEndpoint<S, T> createLinkEndpoint(final Session_1_0 session, final Attach attach)
    {
        final LinkEndpoint<S, T> linkEndpoint;
        if (_role == Role.SENDER)
        {
            linkEndpoint = (LinkEndpoint<S, T>) new SendingLinkEndpoint(session, (LinkImpl<Source, Target>) this);
        }
        else if (attach.getTarget() instanceof Coordinator)
        {
            linkEndpoint = (LinkEndpoint<S, T>) new TxnCoordinatorReceivingLinkEndpoint(session,
                                                                                        (LinkImpl<Source, Coordinator>) this);
        }
        else
        {
            linkEndpoint =
                    (LinkEndpoint<S, T>) new StandardReceivingLinkEndpoint(session, (LinkImpl<Source, Target>) this);
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
            _linkEndpoint =
                    new ErrantLinkEndpoint<>(this, session, new Error(AmqpError.INTERNAL_ERROR, t.getMessage()));
        }
        return Futures.immediateFuture(_linkEndpoint);
    }

    private void startLinkStealingIfNecessary()
    {
        if (!_stealingInProgress)
        {
            _stealingInProgress = true;
            stealLink();
        }
    }

    private synchronized void stealLink()
    {
        ThiefInformation thiefInformation;
        if ((thiefInformation = _thiefQueue.poll()) != null)
        {
            AbstractConfiguredObject.addFutureCallback(doStealLink(thiefInformation.getSession(),
                                                                   thiefInformation.getAttach()),
                                                       new FutureCallback<LinkEndpoint<S, T>>()
                                                       {
                                                           @Override
                                                           public void onSuccess(final LinkEndpoint<S, T> result)
                                                           {
                                                               thiefInformation.getFuture().set(result);
                                                               stealLink();
                                                           }

                                                           @Override
                                                           public void onFailure(final Throwable t)
                                                           {
                                                               thiefInformation.getFuture().setException(t);
                                                               stealLink();
                                                           }
                                                       }, MoreExecutors.directExecutor());
        }
        else
        {
            _stealingInProgress = false;
        }
    }

    private ListenableFuture<LinkEndpoint<S, T>> doStealLink(final Session_1_0 session, final Attach attach)
    {
        final SettableFuture<LinkEndpoint<S, T>> returnFuture = SettableFuture.create();
        final LinkEndpoint<S, T> linkEndpoint = _linkEndpoint;

        // check whether linkEndpoint has been closed in the mean time
        if (linkEndpoint != null)
        {
            linkEndpoint.getSession().doOnIOThreadAsync(
                    () ->
                    {
                        // check whether linkEndpoint has been closed in the mean time
                        LinkEndpoint<S, T> endpoint = _linkEndpoint;
                        if (endpoint != null)
                        {
                            endpoint.close(new Error(LinkError.STOLEN,
                                                          String.format("Link is being stolen by connection '%s'",
                                                                        session.getConnection())));
                        }
                        doLinkStealAndHandleExceptions(session, attach, returnFuture);
                    });
        }
        else
        {
            doLinkStealAndHandleExceptions(session, attach, returnFuture);
        }

        return returnFuture;
    }

    private void doLinkStealAndHandleExceptions(final Session_1_0 session,
                                                final Attach attach,
                                                final SettableFuture<LinkEndpoint<S, T>> returnFuture)
    {
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
    }

    @Override
    public void addDeleteTask(final Action<? super LinkModel> task)
    {
        _deleteTasks.add(task);
    }

    @Override
    public void removeDeleteTask(final Action<? super LinkModel> task)
    {
        _deleteTasks.remove(task);
    }


    private class ThiefInformation
    {
        private final Session_1_0 _session;
        private final Attach _attach;
        private final SettableFuture<LinkEndpoint<S, T>> _future;

        ThiefInformation(final Session_1_0 session,
                         final Attach attach,
                         final SettableFuture<LinkEndpoint<S, T>> future)
        {
            _session = session;
            _attach = attach;
            _future = future;
        }

        Session_1_0 getSession()
        {
            return _session;
        }

        Attach getAttach()
        {
            return _attach;
        }

        SettableFuture<LinkEndpoint<S, T>> getFuture()
        {
            return _future;
        }
    }
}
