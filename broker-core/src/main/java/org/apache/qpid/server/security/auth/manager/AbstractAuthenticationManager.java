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
package org.apache.qpid.server.security.auth.manager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.messages.AuthenticationProviderMessages;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Container;
import org.apache.qpid.server.model.IntegrityViolationException;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.StateTransition;
import org.apache.qpid.server.model.SystemConfig;

public abstract class AbstractAuthenticationManager<T extends AbstractAuthenticationManager<T>>
    extends AbstractConfiguredObject<T>
    implements AuthenticationProvider<T>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractAuthenticationManager.class);

    private final Container<?> _container;
    private final EventLogger _eventLogger;

    @ManagedAttributeField
    private List<String> _secureOnlyMechanisms;

    @ManagedAttributeField
    private List<String> _disabledMechanisms;


    protected AbstractAuthenticationManager(final Map<String, Object> attributes, final Container<?> container)
    {
        super(container, attributes);
        _container = container;
        _eventLogger = _container.getEventLogger();
        _eventLogger.message(AuthenticationProviderMessages.CREATE(getName()));
    }

    @Override
    public void onValidate()
    {
        super.onValidate();

        if(!isDurable())
        {
            throw new IllegalArgumentException(getClass().getSimpleName() + " must be durable");
        }
    }

    @Override
    public List<String> getAvailableMechanisms(boolean secure)
    {
        List<String> mechanisms = getMechanisms();
        Set<String> filter = getDisabledMechanisms() != null
                ? new HashSet<>(getDisabledMechanisms())
                : new HashSet<>() ;
        if(!secure)
        {
            filter.addAll(getSecureOnlyMechanisms());
        }
        if (!filter.isEmpty())
        {
            mechanisms = new ArrayList<>(mechanisms);
            mechanisms.removeAll(filter);
        }
        return mechanisms;
    }


    @StateTransition( currentState = State.UNINITIALIZED, desiredState = State.QUIESCED )
    protected ListenableFuture<Void> startQuiesced()
    {
        setState(State.QUIESCED);
        return Futures.immediateFuture(null);
    }

    @StateTransition( currentState = { State.UNINITIALIZED, State.QUIESCED, State.QUIESCED }, desiredState = State.ACTIVE )
    protected ListenableFuture<Void> activate()
    {
        try
        {
            setState(State.ACTIVE);
        }
        catch(RuntimeException e)
        {
            setState(State.ERRORED);
            if (getAncestor(SystemConfig.class).isManagementMode())
            {
                LOGGER.warn("Failed to activate authentication provider: " + getName(), e);
            }
            else
            {
                throw e;
            }
        }
        return Futures.immediateFuture(null);
    }

    @Override
    protected ListenableFuture<Void> onDelete()
    {
        _eventLogger.message(AuthenticationProviderMessages.DELETE(getName()));
        return super.onDelete();
    }

    @Override
    public final List<String> getSecureOnlyMechanisms()
    {
        return _secureOnlyMechanisms;
    }

    @Override
    public final List<String> getDisabledMechanisms()
    {
        return _disabledMechanisms;
    }

    @Override
    protected void logOperation(final String operation)
    {
        _container.getEventLogger().message(AuthenticationProviderMessages.OPERATION(operation));
    }

    @Override
    public EventLogger getEventLogger()
    {
        return _eventLogger;
    }
}
