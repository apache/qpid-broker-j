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
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.messages.AccessControlMessages;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.StateTransition;
import org.apache.qpid.server.security.AccessControl;
import org.apache.qpid.server.security.access.config.AclFileParser;
import org.apache.qpid.server.security.access.config.RuleBasedAccessControl;
import org.apache.qpid.server.util.urlstreamhandler.data.Handler;

public abstract class AbstractRuleBasedAccessControlProvider<X extends AbstractRuleBasedAccessControlProvider<X>>
        extends AbstractConfiguredObject<X>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRuleBasedAccessControlProvider.class);

    static
    {
        Handler.register();
    }

    private volatile RuleBasedAccessControl _accessControl;
    private final Broker _broker;
    private final EventLogger _eventLogger;

    public AbstractRuleBasedAccessControlProvider(Map<String, Object> attributes, Broker broker)
    {
        super(parentsMap(broker), attributes);


        _broker = broker;
        _eventLogger = _broker.getEventLogger();
        _eventLogger.message(AccessControlMessages.CREATE(getName()));
    }

    protected final Broker getBroker()
    {
        return _broker;
    }

    protected final EventLogger getEventLogger()
    {
        return _eventLogger;
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


    @Override
    protected void onOpen()
    {
        super.onOpen();
    }


    @StateTransition(currentState = {State.UNINITIALIZED, State.QUIESCED, State.ERRORED}, desiredState = State.ACTIVE)
    @SuppressWarnings("unused")
    private ListenableFuture<Void> activate()
    {

        try
        {
            recreateAccessController();
            setState(_broker.isManagementMode() ? State.QUIESCED : State.ACTIVE);
        }
        catch (RuntimeException e)
        {
            setState(State.ERRORED);
            if (_broker.isManagementMode())
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
    protected void onClose()
    {
        super.onClose();

    }

    @StateTransition(currentState = State.UNINITIALIZED, desiredState = State.QUIESCED)
    @SuppressWarnings("unused")
    private ListenableFuture<Void> startQuiesced()
    {
        setState(State.QUIESCED);
        return Futures.immediateFuture(null);
    }

    @StateTransition(currentState = {State.ACTIVE, State.QUIESCED, State.ERRORED}, desiredState = State.DELETED)
    @SuppressWarnings("unused")
    private ListenableFuture<Void> doDelete()
    {
        return doAfterAlways(closeAsync(),
                new Runnable()
                {
                    @Override
                    public void run()
                    {
                        setState(State.DELETED);
                        deleted();
                        _eventLogger.message(AccessControlMessages.DELETE(getName()));
                    }
                });
    }

    public final AccessControl getAccessControl()
    {
        return _accessControl;
    }
}
