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

import java.util.Arrays;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import org.apache.qpid.server.protocol.v1_0.messaging.SectionDecoderImpl;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Target;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;

public class StandardReceivingLink_1_0 extends AbstractLink<StandardReceivingLinkEndpoint> implements ReceivingLink_1_0
{
    public StandardReceivingLink_1_0(final String linkName)
    {
        super(linkName);
    }

    @Override
    protected ListenableFuture<StandardReceivingLinkEndpoint> stealLink(final Session_1_0 session, final Attach attach)
    {
        throw new UnsupportedOperationException("Link stealing not implemented yet");
        /*
        final SettableFuture<StandardReceivingLinkEndpoint> returnFuture = SettableFuture.create();
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
    protected ListenableFuture<StandardReceivingLinkEndpoint> reattachLink(final Session_1_0 session,
                                                                           final Attach attach)
    {
        if (_linkEndpoint == null)
        {
            SectionDecoderImpl sectionDecoder = new SectionDecoderImpl(session.getConnection()
                                                                              .getDescribedTypeRegistry()
                                                                              .getSectionDecoderRegistry());
            _linkEndpoint = new StandardReceivingLinkEndpoint(this, sectionDecoder);
        }

        _target = new Target();
        _source = attach.getSource();

        try
        {
            _linkEndpoint.associateSession(session);
            _linkEndpoint.attachReceived(attach);

            Target attachTarget = (Target) attach.getTarget();
            final ReceivingDestination destination = session.getReceivingDestination(attachTarget);
            ((Target) _target).setAddress(attachTarget.getAddress());
            ((Target) _target).setDynamic(attachTarget.getDynamic());
            ((Target) _target).setCapabilities(destination.getCapabilities());
            _linkEndpoint.setCapabilities(Arrays.asList(destination.getCapabilities()));
            _linkEndpoint.setDestination(destination);
        }
        catch (AmqpErrorException e)
        {
            rejectLink(session, attach);
        }

        return Futures.immediateFuture(_linkEndpoint);    }

    @Override
    protected ListenableFuture<StandardReceivingLinkEndpoint> resumeLink(final Session_1_0 session, final Attach attach)
    {
        if (getTarget() == null)
        {
            throw new IllegalStateException("Terminus should be set when resuming a Link.");
        }
        if (attach.getTarget() == null)
        {
            throw new IllegalStateException("Attach.getTarget should not be null when resuming a Link. That would be recovering the Link.");
        }

        _source = attach.getSource();

        try
        {

            if (_linkEndpoint == null)
            {
                SectionDecoderImpl sectionDecoder = new SectionDecoderImpl(session.getConnection()
                                                                                  .getDescribedTypeRegistry()
                                                                                  .getSectionDecoderRegistry());
                _linkEndpoint = new StandardReceivingLinkEndpoint(this, sectionDecoder);

                final ReceivingDestination destination = session.getReceivingDestination((Target) _target);
                _linkEndpoint.setDestination(destination);
                ((Target) _target).setCapabilities(destination.getCapabilities());
                _linkEndpoint.setCapabilities(Arrays.asList(destination.getCapabilities()));
                _linkEndpoint.setDestination(destination);
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
    protected ListenableFuture<StandardReceivingLinkEndpoint> recoverLink(final Session_1_0 session,
                                                                          final Attach attach)
    {
        if (_target == null)
        {
            return rejectLink(session, attach);
        }

        _source = attach.getSource();

        try
        {
            if (_linkEndpoint == null)
            {
                SectionDecoderImpl sectionDecoder = new SectionDecoderImpl(session.getConnection()
                                                                                  .getDescribedTypeRegistry()
                                                                                  .getSectionDecoderRegistry());
                _linkEndpoint = new StandardReceivingLinkEndpoint(this, sectionDecoder);

                final ReceivingDestination destination = session.getReceivingDestination((Target) _target);
                ((Target) _target).setCapabilities(destination.getCapabilities());
                _linkEndpoint.setCapabilities(Arrays.asList(destination.getCapabilities()));
                _linkEndpoint.setDestination(destination);
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
    protected ListenableFuture<StandardReceivingLinkEndpoint> establishLink(final Session_1_0 session,
                                                                            final Attach attach)
    {
        if (_linkEndpoint != null || getTarget() != null)
        {
            throw new IllegalStateException("LinkEndpoint and Target should be null when establishing a Link.");
        }

        return reattachLink(session, attach);
    }

    private ListenableFuture<StandardReceivingLinkEndpoint> rejectLink(final Session_1_0 session,
                                                                       final Attach attach)
    {
        SectionDecoderImpl sectionDecoder =
                new SectionDecoderImpl(session.getConnection().getDescribedTypeRegistry().getSectionDecoderRegistry());
        _linkEndpoint = new StandardReceivingLinkEndpoint(this, sectionDecoder);
        _linkEndpoint.associateSession(session);
        _target = null;
        return Futures.immediateFuture(_linkEndpoint);
    }
}
