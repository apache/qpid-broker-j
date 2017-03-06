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
package org.apache.qpid.server.protocol.v1_0;

import java.util.Collections;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;

public class SendingLink_1_0 extends AbstractLink<SendingLinkEndpoint>
{
    public SendingLink_1_0(final String linkName)
    {
        super(linkName);
    }

    @Override
    protected ListenableFuture<SendingLinkEndpoint> stealLink(final Session_1_0 session, final Attach attach)
    {
        throw new UnsupportedOperationException("Link stealing is not implemented yet.");
        /*
        final SettableFuture<SendingLinkEndpoint> returnFuture = SettableFuture.create();
        _linkEndpoint.getSession().doOnIOThreadAsync(new Runnable()
        {
            @Override
            public void run()
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
            }
        });
        return returnFuture;
        */
    }

    @Override
    protected ListenableFuture<SendingLinkEndpoint> reattachLink(final Session_1_0 session, final Attach attach)
    {
        if (_linkEndpoint == null)
        {
            _linkEndpoint = new SendingLinkEndpoint(this);
        }

        _source = new Source();
        _target = attach.getTarget();

        try
        {
            _linkEndpoint.associateSession(session);

            Source attachSource = (Source) attach.getSource();
            final SendingDestination destination = session.getSendingDestination(attach.getName(), attachSource);
            ((Source) getSource()).setAddress(attachSource.getAddress());
            ((Source) getSource()).setDynamic(attachSource.getDynamic());
            ((Source) getSource()).setDurable(attachSource.getDurable());
            ((Source) getSource()).setExpiryPolicy(attachSource.getExpiryPolicy());
            ((Source) getSource()).setDistributionMode(attachSource.getDistributionMode());
            ((Source) getSource()).setFilter(attachSource.getFilter());
            ((Source) getSource()).setCapabilities(destination.getCapabilities());
            _linkEndpoint.prepareConsumerOptionsAndFilters(destination);
            _linkEndpoint.attachReceived(attach);

            if (destination instanceof ExchangeDestination)
            {
                ExchangeDestination exchangeDestination = (ExchangeDestination) destination;
                exchangeDestination.getQueue()
                                   .setAttributes(Collections.<String, Object>singletonMap(Queue.DESIRED_STATE,
                                                                                           State.ACTIVE));
            }
        }
        catch (AmqpErrorException e)
        {
            rejectLink(session, attach);
        }

        return Futures.immediateFuture(_linkEndpoint);
    }

    @Override
    protected ListenableFuture<SendingLinkEndpoint> resumeLink(final Session_1_0 session, final Attach attach)
    {
        if (getSource() == null)
        {
            throw new IllegalStateException("Terminus should be set when resuming a Link.");
        }
        if (attach.getSource() == null)
        {
            throw new IllegalStateException("Attach.getSource should not be null when resuming a Link. That would be recovering the Link.");
        }

        Source newSource = (Source) attach.getSource();
        Source oldSource = (Source) getSource();

        try
        {
            if (_linkEndpoint == null)
            {
                _linkEndpoint = new SendingLinkEndpoint(this);

                final SendingDestination destination = session.getSendingDestination(getName(), oldSource);
                _linkEndpoint.prepareConsumerOptionsAndFilters(destination);
            }

            if (_linkEndpoint.getDestination() instanceof ExchangeDestination
                && !Boolean.TRUE.equals(newSource.getDynamic()))
            {
                final SendingDestination newDestination =
                        session.getSendingDestination(_linkEndpoint.getLinkName(), newSource);
                if (session.updateSourceForSubscription(_linkEndpoint, newSource, newDestination))
                {
                    _linkEndpoint.setDestination(newDestination);
                }
            }

            _linkEndpoint.associateSession(session);
            _linkEndpoint.attachReceived(attach);

            _linkEndpoint.setLocalUnsettled(_linkEndpoint.getUnsettledOutcomeMap());
        }
        catch (AmqpErrorException e)
        {
            rejectLink(session, attach);
        }
        return Futures.immediateFuture(_linkEndpoint);
    }

    @Override
    protected ListenableFuture<SendingLinkEndpoint> recoverLink(final Session_1_0 session, final Attach attach)
    {
        if (_source == null)
        {
            return rejectLink(session, attach);
        }

        _target = attach.getTarget();

        try
        {
            if (_linkEndpoint == null)
            {
                _linkEndpoint = new SendingLinkEndpoint(this);

                final SendingDestination destination = session.getSendingDestination(getName(), (Source) _source);
                _linkEndpoint.prepareConsumerOptionsAndFilters(destination);
            }

            _linkEndpoint.associateSession(session);
            _linkEndpoint.attachReceived(attach);

            _linkEndpoint.setLocalUnsettled(_linkEndpoint.getUnsettledOutcomeMap());
        }
        catch (AmqpErrorException e)
        {
            rejectLink(session, attach);
        }

        return Futures.immediateFuture(_linkEndpoint);
    }

    @Override
    protected ListenableFuture<SendingLinkEndpoint> establishLink(final Session_1_0 session, final Attach attach)
    {
        if (_linkEndpoint != null || getSource() != null)
        {
            throw new IllegalStateException("LinkEndpoint and Source should be null when establishing a Link.");
        }

        return reattachLink(session, attach);
    }

    private ListenableFuture<SendingLinkEndpoint> rejectLink(final Session_1_0 session, final Attach attach)
    {
        _linkEndpoint = new SendingLinkEndpoint(this);
        _linkEndpoint.associateSession(session);
        _source = null;
        return Futures.immediateFuture(_linkEndpoint);
    }
}
