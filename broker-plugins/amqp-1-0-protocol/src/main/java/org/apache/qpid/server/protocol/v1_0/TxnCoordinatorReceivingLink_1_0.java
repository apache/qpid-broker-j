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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import org.apache.qpid.server.protocol.v1_0.messaging.SectionDecoder;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionDecoderImpl;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.transaction.Coordinator;
import org.apache.qpid.server.protocol.v1_0.type.transaction.TxnCapability;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;

public class TxnCoordinatorReceivingLink_1_0 extends AbstractLink<TxnCoordinatorReceivingLinkEndpoint> implements ReceivingLink_1_0
{
    public TxnCoordinatorReceivingLink_1_0(final String linkName)
    {
        super(linkName);
    }

    @Override
    protected ListenableFuture<TxnCoordinatorReceivingLinkEndpoint> stealLink(final Session_1_0 session,
                                                                              final Attach attach)
    {
        return rejectLink(session, attach);
    }

    @Override
    protected ListenableFuture<TxnCoordinatorReceivingLinkEndpoint> reattachLink(final Session_1_0 session,
                                                                                 final Attach attach)
    {
        return rejectLink(session, attach);
    }

    @Override
    protected ListenableFuture<TxnCoordinatorReceivingLinkEndpoint> resumeLink(final Session_1_0 session,
                                                                               final Attach attach)
    {
        return rejectLink(session, attach);
    }

    @Override
    protected ListenableFuture<TxnCoordinatorReceivingLinkEndpoint> recoverLink(final Session_1_0 session,
                                                                                final Attach attach)
    {
        return rejectLink(session, attach);
    }

    @Override
    protected ListenableFuture<TxnCoordinatorReceivingLinkEndpoint> establishLink(final Session_1_0 session,
                                                                                  final Attach attach)
    {
        if (_linkEndpoint != null || getTarget() != null)
        {
            throw new IllegalStateException("LinkEndpoint and Target should be null when establishing a Link.");
        }

        _target = new Coordinator();
        ((Coordinator) _target).setCapabilities(TxnCapability.LOCAL_TXN,
                                                TxnCapability.MULTI_SSNS_PER_TXN,
                                                TxnCapability.MULTI_TXNS_PER_SSN);
        _source = attach.getSource();

        try
        {
            SectionDecoder sectionDecoder = new SectionDecoderImpl(session.getConnection()
                                                                          .getDescribedTypeRegistry()
                                                                          .getSectionDecoderRegistry());
            _linkEndpoint = new TxnCoordinatorReceivingLinkEndpoint(this, sectionDecoder);
            _linkEndpoint.associateSession(session);
            _linkEndpoint.attachReceived(attach);
        }
        catch (AmqpErrorException e)
        {
            rejectLink(session, attach);
        }

        return Futures.immediateFuture(_linkEndpoint);
    }

    private ListenableFuture<TxnCoordinatorReceivingLinkEndpoint> rejectLink(final Session_1_0 session,
                                                                             final Attach attach)
    {
        SectionDecoder sectionDecoder = new SectionDecoderImpl(session.getConnection()
                                                                      .getDescribedTypeRegistry()
                                                                      .getSectionDecoderRegistry());
        _linkEndpoint = new TxnCoordinatorReceivingLinkEndpoint(this, sectionDecoder);
        _linkEndpoint.associateSession(session);
        _target = null;
        return Futures.immediateFuture(_linkEndpoint);
    }
}
