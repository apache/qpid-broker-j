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
import java.util.List;
import java.util.Map;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.messages.AuthenticationProviderMessages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Container;
import org.apache.qpid.server.model.GroupProvider;
import org.apache.qpid.server.model.IntegrityViolationException;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.StateTransition;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.model.port.AbstractPortWithAuthProvider;
import org.apache.qpid.server.security.SubjectCreator;

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
        super(parentsMap(container), attributes);
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
    public SubjectCreator getSubjectCreator(final boolean secure)
    {
        Collection children = _container.getChildren(GroupProvider.class);
        return new SubjectCreator(this, children, secure);
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

    @StateTransition( currentState = { State.ACTIVE, State.QUIESCED, State.ERRORED}, desiredState = State.DELETED)
    protected ListenableFuture<Void> doDelete()
    {

        String providerName = getName();

        // verify that provider is not in use
        Collection<Port> ports = new ArrayList<>(_container.getChildren(Port.class));
        for (Port<?> port : ports)
        {
            if(port instanceof AbstractPortWithAuthProvider
               && ((AbstractPortWithAuthProvider<?>)port).getAuthenticationProvider() == this)
            {
                throw new IntegrityViolationException("Authentication provider '" + providerName + "' is set on port " + port.getName());
            }
        }

        return performDelete();
    }

    private ListenableFuture<Void> performDelete()
    {
        final SettableFuture<Void> futureResult = SettableFuture.create();
        addFutureCallback(closeAsync(), new FutureCallback<Void>()
        {
            @Override
            public void onSuccess(final Void result)
            {
                try
                {
                    tidyUp();
                    futureResult.set(null);
                }
                catch (Exception e)
                {
                    futureResult.setException(e);
                }
            }

            @Override
            public void onFailure(final Throwable t)
            {
                try
                {
                    tidyUp();
                }
                finally
                {
                    futureResult.setException(t);
                }
            }

            private void tidyUp()
            {
                deleted();
                setState(State.DELETED);
                _eventLogger.message(AuthenticationProviderMessages.DELETE(getName()));
            }
        }, getTaskExecutor());

        return futureResult;
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
}
