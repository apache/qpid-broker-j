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
package org.apache.qpid.server.security.access.plugins;

import java.util.Map;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.logging.EventLoggerProvider;
import org.apache.qpid.server.model.CommonAccessControlProvider;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.StateTransition;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.security.AccessControl;
import org.apache.qpid.server.security.access.AbstractAccessControlProvider;
import org.apache.qpid.server.security.access.config.RuleBasedAccessControl;
import org.apache.qpid.server.util.urlstreamhandler.data.Handler;

abstract class AbstractLegacyAccessControlProvider<X extends AbstractLegacyAccessControlProvider<X,T,Y>, T extends EventLoggerProvider & ConfiguredObject<?>, Y extends CommonAccessControlProvider<Y>>
        extends AbstractAccessControlProvider<X, Y, T> implements EventLoggerProvider
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractLegacyAccessControlProvider.class);

    static
    {
        Handler.register();
    }

    private volatile RuleBasedAccessControl _accessControl;


    AbstractLegacyAccessControlProvider(Map<String, Object> attributes, T parent)
    {
        super(attributes, parent);

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
    protected void validateOnCreate()
    {
        try
        {
            createRuleBasedAccessController();
        }
        catch(RuntimeException e)
        {
            throw new IllegalConfigurationException(e.getMessage(), e);
        }

    }

    abstract protected RuleBasedAccessControl createRuleBasedAccessController();

    @StateTransition(currentState = {State.UNINITIALIZED, State.QUIESCED, State.ERRORED}, desiredState = State.ACTIVE)
    @SuppressWarnings("unused")
    private ListenableFuture<Void> activate()
    {

        final boolean isManagementMode = getModel().getAncestor(SystemConfig.class, this).isManagementMode();
        try
        {
            recreateAccessController();
            setState(isManagementMode ? State.QUIESCED : State.ACTIVE);
        }
        catch (RuntimeException e)
        {
            setState(State.ERRORED);
            if (isManagementMode)
            {
                LOGGER.warn("Failed to activate ACL provider: " + getName(), e);
            }
            else
            {
                throw e;
            }
        }
        return Futures.immediateFuture(null);
    }


    protected final void recreateAccessController()
    {
        _accessControl = createRuleBasedAccessController();
    }


    @Override
    public AccessControl<?> getController()
    {
        return _accessControl;
    }
}
