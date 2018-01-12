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

import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.BaseSource;
import org.apache.qpid.server.protocol.v1_0.type.BaseTarget;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.DeliveryState;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Detach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Error;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.ReceiverSettleMode;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.server.protocol.v1_0.type.transport.SenderSettleMode;

public class ErrantLinkEndpoint<S extends BaseSource, T extends BaseTarget> implements LinkEndpoint<S, T>
{
    private final Link_1_0<S, T> _link;
    private final Session_1_0 _session;
    private final Error _error;
    private UnsignedInteger _localHandle;

    ErrantLinkEndpoint(Link_1_0<S, T> link, Session_1_0 session, Error error)
    {
        _link = link;
        _session = session;
        _error = error;
    }

    @Override
    public Role getRole()
    {
        return _link.getRole();
    }

    @Override
    public S getSource()
    {
        return null;
    }

    @Override
    public T getTarget()
    {
        return null;
    }

    @Override
    public Session_1_0 getSession()
    {
        return _session;
    }

    @Override
    public UnsignedInteger getLocalHandle()
    {
        return _localHandle;
    }

    @Override
    public void setLocalHandle(final UnsignedInteger localHandle)
    {
        _localHandle = localHandle;
    }

    @Override
    public void sendAttach()
    {
        Attach attachToSend = new Attach();
        attachToSend.setName(_link.getName());
        attachToSend.setRole(getRole());
        attachToSend.setHandle(getLocalHandle());
        attachToSend.setSource(getSource());
        attachToSend.setTarget(getTarget());
        _session.sendAttach(attachToSend);
    }

    @Override
    public void receiveAttach(final Attach attach) throws AmqpErrorException
    {
        throw new UnsupportedOperationException("This Link is errant");
    }

    @Override
    public void destroy()
    {
        setLocalHandle(null);
        _link.discardEndpoint();
    }

    public void closeWithError()
    {
        close(_error);
    }

    @Override
    public void close(final Error error)
    {
        Detach detach = new Detach();
        detach.setHandle(_localHandle);
        detach.setClosed(true);
        detach.setError(error);
        _session.sendDetach(detach);
        _session.dissociateEndpoint(this);
        destroy();
        _link.linkClosed();
    }

    @Override
    public SenderSettleMode getSendingSettlementMode()
    {
        return null;
    }

    @Override
    public ReceiverSettleMode getReceivingSettlementMode()
    {
        return null;
    }

    @Override
    public void remoteDetached(final Detach detach)
    {
        // ignore
    }

    @Override
    public void receiveDeliveryState(final Binary deliveryTag, final DeliveryState state, final Boolean settled)
    {

    }

    @Override
    public void receiveFlow(final Flow flow)
    {
        throw new UnsupportedOperationException("This Link is errant");
    }

    @Override
    public void sendFlow()
    {
        throw new UnsupportedOperationException("This Link is errant");
    }

    @Override
    public void flowStateChanged()
    {
        throw new UnsupportedOperationException("This Link is errant");
    }

    @Override
    public void start()
    {
        throw new UnsupportedOperationException("This Link is errant");
    }

    @Override
    public void setStopped(final boolean stopped)
    {
        throw new UnsupportedOperationException("This Link is errant");
    }

    @Override
    public void receiveComplete()
    {
    }
}
