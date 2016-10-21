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
package org.apache.qpid.server.federation;

import java.security.AccessControlContext;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.AbstractConfigurationChangeListener;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.RemoteHost;
import org.apache.qpid.server.model.RemoteHostAddress;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.StateTransition;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.virtualhost.HouseKeepingTask;

@ManagedObject( category = false, type = RemoteHost.REMOTE_HOST_TYPE )
class RemoteHostImpl extends AbstractConfiguredObject<RemoteHostImpl> implements RemoteHost<RemoteHostImpl>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteHostImpl.class);

    private final VirtualHost<?> _virtualHost;
    @ManagedAttributeField
    private int _retryPeriod;

    @ManagedAttributeField
    private boolean _redirectFollowed;

    @ManagedAttributeField
    private Collection<String> _routableAddresses;

    private final AccessControlContext _createConnectionContext;
    private final CreateConnectionTask _createConnectionTask;

    enum ConnectionState
    {
        DISCONNECTED,
        CONNECTING,
        CONNECTED,
        STOPPED
    }

    private ConnectionState _connectionState = ConnectionState.STOPPED;

    @ManagedObjectFactoryConstructor
    public RemoteHostImpl(Map<String, Object> attributes, VirtualHost<?> virtualHost)
    {
        super(parentsMap(virtualHost), attributes);
        _virtualHost = virtualHost;
        _createConnectionContext =
                getSystemTaskControllerContext("Create connection " + getName(), _virtualHost.getPrincipal());
        _createConnectionTask = new CreateConnectionTask();
    }

    @Override
    public int getRetryPeriod()
    {
        return _retryPeriod;
    }

    @Override
    public boolean isRedirectFollowed()
    {
        return _redirectFollowed;
    }

    @Override
    public Collection<String> getRoutableAddresses()
    {
        return _routableAddresses;
    }

    @StateTransition(currentState = {State.UNINITIALIZED, State.ERRORED, State.STOPPED}, desiredState = State.ACTIVE)
    private ListenableFuture<Void> onActivate()
    {
        setState(State.ACTIVE);
        _failoverIterator = null;
        if (_virtualHost.getState() == State.ACTIVE)
        {
            _createConnectionTask.scheduleNow();
        }
        else if (_virtualHost.getDesiredState() == State.ACTIVE)
        {
            _virtualHost.addChangeListener(new AbstractConfigurationChangeListener()
            {
                @Override
                public void stateChanged(final ConfiguredObject<?> object, final State oldState, final State newState)
                {
                    if (newState == State.ACTIVE)
                    {

                        _createConnectionTask.scheduleNow();

                        _virtualHost.removeChangeListener(this);
                    }
                    else if (object.getDesiredState() != State.ACTIVE)
                    {
                        _virtualHost.removeChangeListener(this);
                    }
                }
            });
        }
        return Futures.immediateFuture(null);
    }

    private Iterator<RemoteHostAddress> _failoverIterator;


    private void setConnectionState(ConnectionState connectionState)
    {
        _connectionState = connectionState;
    }

    private synchronized void makeConnection()
    {
        LOGGER.debug("makeConnection called with state: {}, connectionState: {}", getState(), _connectionState);
        if(getState() == State.ACTIVE && !EnumSet.of(ConnectionState.CONNECTED, ConnectionState.CONNECTING).contains(_connectionState))
        {
            if (_failoverIterator == null || !_failoverIterator.hasNext())
            {
                _failoverIterator = getChildren(RemoteHostAddress.class).iterator();
            }
            if (_failoverIterator.hasNext())
            {
                RemoteHostAddress<?> address = _failoverIterator.next();
                setConnectionState(ConnectionState.CONNECTING);
                boolean connected = _virtualHost.makeConnection(address, new Action<Boolean>()
                {

                    @Override
                    public void performAction(final Boolean wasConnected)
                    {
                        setConnectionState(ConnectionState.DISCONNECTED);
                        if (wasConnected)
                        {
                            _failoverIterator = null;
                            _createConnectionTask.scheduleNow();
                        }
                        else if (_failoverIterator.hasNext())
                        {
                            _createConnectionTask.scheduleNow();
                        }
                        else
                        {
                            _createConnectionTask.schedule(1000L * _retryPeriod);
                        }
                    }
                });

                if (connected)
                {
                    setConnectionState(ConnectionState.CONNECTED);
                }
                else
                {
                    setConnectionState(ConnectionState.DISCONNECTED);
                    if (_failoverIterator.hasNext())
                    {
                        _createConnectionTask.scheduleNow();
                    }
                    else
                    {
                        _createConnectionTask.schedule(1000L * _retryPeriod);
                    }
                }
            }
        }

    }

    private class CreateConnectionTask extends HouseKeepingTask
    {

        private final AtomicBoolean _scheduled = new AtomicBoolean();

        public CreateConnectionTask()
        {
            super("Create connection: " + RemoteHostImpl.this.getName(), _virtualHost, _createConnectionContext);
        }

        @Override
        public void execute()
        {
            _scheduled.set(false);
            makeConnection();
        }

        public void schedule(long delay)
        {
            if(_scheduled.compareAndSet(false, true))
            {
                _virtualHost.scheduleTask(delay, this);
            }
        }

        public void scheduleNow()
        {
            schedule(0L);
        }
    }
}
