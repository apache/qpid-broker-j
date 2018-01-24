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
package org.apache.qpid.server.security.access;

import java.util.Map;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.EventLoggerProvider;
import org.apache.qpid.server.logging.messages.AccessControlMessages;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.CommonAccessControlProvider;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Container;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.StateTransition;


public abstract class AbstractAccessControlProvider<X extends AbstractAccessControlProvider<X,Y,T>, Y extends CommonAccessControlProvider<Y>, T extends EventLoggerProvider & ConfiguredObject<?>>
        extends AbstractConfiguredObject<X> implements EventLoggerProvider, CommonAccessControlProvider<Y>
{
    private final EventLogger _eventLogger;
    @ManagedAttributeField
    private int _priority;


    public AbstractAccessControlProvider(Map<String, Object> attributes, T parent)
    {
        super(parent, attributes);

        _eventLogger = parent.getEventLogger();
        _eventLogger.message(AccessControlMessages.CREATE(getName()));


    }

    @Override
    public EventLogger getEventLogger()
    {
        return _eventLogger;
    }

    @Override
    public final int getPriority()
    {
        return _priority;
    }

    @Override
    public int compareTo(final Y o)
    {
        return ACCESS_CONTROL_PROVIDER_COMPARATOR.compare((Y)this, o);
    }

    @StateTransition(currentState = State.UNINITIALIZED, desiredState = State.QUIESCED)
    @SuppressWarnings("unused")
    private ListenableFuture<Void> startQuiesced()
    {
        setState(State.QUIESCED);
        return Futures.immediateFuture(null);
    }

    @Override
    protected ListenableFuture<Void> onDelete()
    {
        getEventLogger().message(AccessControlMessages.DELETE(getName()));
        return super.onDelete();
    }

    @Override
    protected void logOperation(final String operation)
    {
        getAncestor(Container.class).getEventLogger().message(AccessControlMessages.OPERATION(operation));
    }
}
