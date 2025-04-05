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
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.ConfiguredObject;
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
import org.apache.qpid.server.security.access.Operation;
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
    public final synchronized CompletableFuture<? extends LinkEndpoint<S, T>> attach(final Session_1_0 session, final Attach attach)
    {
        try
        {
            if (_role == attach.getRole())
            {
                throw new AmqpErrorException(new Error(AmqpError.ILLEGAL_STATE, "Cannot switch SendingLink to ReceivingLink and vice versa"));
            }

            if (_linkEndpoint != null && !session.equals(_linkEndpoint.getSession()))
            {
                if (!Objects.equals(_linkEndpoint.getSession().getConnection().getPrincipal(),
                        session.getConnection().getPrincipal()))
                {
                    final Operation operation = attach.getRole() == Role.SENDER
                            ? Operation.PERFORM_ACTION("publish")
                            : Operation.PERFORM_ACTION("consume");
                    final ConfiguredObject<?> targetObject = _linkEndpoint instanceof SendingLinkEndpoint
                            ? (ConfiguredObject<?>) ((SendingLinkEndpoint) _linkEndpoint).getDestination().getMessageSource()
                            : (ConfiguredObject<?>) session.getReceivingDestination(this, (Target) getTarget()).getMessageDestination();
                    targetObject.authorise(operation);
                }
                final CompletableFuture<LinkEndpoint<S, T>> future = new CompletableFuture<>();
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
                return CompletableFuture.completedFuture(_linkEndpoint);
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

    private CompletableFuture<? extends LinkEndpoint<S, T>> rejectLink(final Session_1_0 session, Throwable t)
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
        return CompletableFuture.completedFuture(_linkEndpoint);
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
            doStealLink(thiefInformation.getSession(), thiefInformation.getAttach())
                    .whenComplete((result, error) ->
                    {
                        if (error != null)
                        {
                            thiefInformation.getFuture().completeExceptionally(error);
                            stealLink();
                        }
                        else
                        {
                            thiefInformation.getFuture().complete(result);
                            stealLink();
                        }
                    });
        }
        else
        {
            _stealingInProgress = false;
        }
    }

    private CompletableFuture<LinkEndpoint<S, T>> doStealLink(final Session_1_0 session, final Attach attach)
    {
        final CompletableFuture<LinkEndpoint<S, T>> returnFuture = new CompletableFuture<>();
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
                                                final CompletableFuture<LinkEndpoint<S, T>> returnFuture)
    {
        try
        {
            returnFuture.complete(attach(session, attach).get());
        }
        catch (InterruptedException e)
        {
            returnFuture.completeExceptionally(e);
            Thread.currentThread().interrupt();
        }
        catch (ExecutionException e)
        {
            returnFuture.completeExceptionally(e.getCause());
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
        private final CompletableFuture<LinkEndpoint<S, T>> _future;

        ThiefInformation(final Session_1_0 session,
                         final Attach attach,
                         final CompletableFuture<LinkEndpoint<S, T>> future)
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

        CompletableFuture<LinkEndpoint<S, T>> getFuture()
        {
            return _future;
        }
    }
}
