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
package org.apache.qpid.server.user.connection.limits.plugins;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.StateTransition;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.security.limit.ConnectionLimitProvider;
import org.apache.qpid.server.security.limit.ConnectionLimiter;
import org.apache.qpid.server.user.connection.limits.config.RuleSetCreator;
import org.apache.qpid.server.util.urlstreamhandler.data.Handler;

public abstract class AbstractConnectionLimitProvider<X extends AbstractConnectionLimitProvider<X>>
        extends AbstractConfiguredObject<X> implements ConnectionLimitProvider<X>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractConnectionLimitProvider.class);

    private static final String FAILED_CREATE_NEW_PROVIDER = "Failed to create a new connection limit provider";

    private final AtomicReference<RuleSetCreator> _creator = new AtomicReference<>(null);

    static
    {
        Handler.register();
    }

    abstract RuleSetCreator newRuleSetCreator();

    public AbstractConnectionLimitProvider(ConfiguredObject<?> parent, Map<String, Object> attributes)
    {
        super(parent, attributes);
    }

    @Override
    public ConnectionLimiter getConnectionLimiter()
    {
        return Optional.ofNullable(_creator.get())
                .<ConnectionLimiter>map(provider -> provider.getLimiter(getName()))
                .orElseGet(ConnectionLimiter::noLimits);
    }

    @Override
    public void onValidate()
    {
        super.onValidate();
        if (!isDurable())
        {
            throw new IllegalArgumentException(getClass().getSimpleName() + " must be durable");
        }
    }

    @Override
    protected void validateOnCreate()
    {
        try
        {
            if (_creator.get() == null)
            {
                _creator.compareAndSet(null, newRuleSetCreator());
            }
        }
        catch (RuntimeException e)
        {
            throw new IllegalConfigurationException(FAILED_CREATE_NEW_PROVIDER, e);
        }
    }

    @StateTransition(currentState = {State.UNINITIALIZED, State.QUIESCED, State.ERRORED}, desiredState = State.ACTIVE)
    @SuppressWarnings("unused")
    CompletableFuture<Void> activate()
    {
        final boolean isManagementMode = getModel().getAncestor(SystemConfig.class, this).isManagementMode();
        final RuleSetCreator creator;
        if (State.ERRORED == getState())
        {
            creator = null;
            _creator.set(null);
        }
        else
        {
            creator = _creator.get();
        }
        try
        {
            if (creator == null)
            {
                _creator.compareAndSet(null, newRuleSetCreator());
            }
            setState(isManagementMode ? State.QUIESCED : State.ACTIVE);
        }
        catch (RuntimeException e)
        {
            LOGGER.debug(String.format(
                    "Connection limit provider '%s' can not be activated because of the error: ", getName()), e);
            setState(State.ERRORED);
            if (isManagementMode)
            {
                LOGGER.warn(String.format("Failed to activate connection limit provider: %s", getName()));
            }
            else
            {
                throw e;
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @StateTransition(currentState = {
            State.UNINITIALIZED, State.QUIESCED, State.ACTIVE, State.STOPPED, State.DELETED, State.UNAVAILABLE},
            desiredState = State.ERRORED)
    @SuppressWarnings("unused")
    CompletableFuture<Void> error()
    {
        _creator.set(null);
        setState(State.ERRORED);
        return CompletableFuture.completedFuture(null);
    }

    @StateTransition(currentState = State.UNINITIALIZED, desiredState = State.QUIESCED)
    @SuppressWarnings("unused")
    private CompletableFuture<Void> startQuiesced()
    {
        setState(State.QUIESCED);
        return CompletableFuture.completedFuture(null);
    }

    protected void forceNewRuleSetCreator()
    {
        try
        {
            _creator.set(newRuleSetCreator());
        }
        catch (RuntimeException e)
        {
            _creator.set(null);
            throw new IllegalConfigurationException(FAILED_CREATE_NEW_PROVIDER, e);
        }
    }

    protected RuleSetCreator creator()
    {
        return _creator.get();
    }
}
