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
package org.apache.qpid.server.security;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.Outcome;
import org.apache.qpid.server.logging.messages.AccessControlMessages;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.AccessControlProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.StateTransition;

public class AllowAllAccessControlProviderImpl extends AbstractConfiguredObject<AllowAllAccessControlProviderImpl> implements AllowAllAccessControlProvider<AllowAllAccessControlProviderImpl>
{
    private final Broker _broker;
    @ManagedAttributeField
    private int _priority;

    @ManagedObjectFactoryConstructor
    public AllowAllAccessControlProviderImpl(Map<String, Object> attributes, Broker broker)
    {
        super((ConfiguredObject<?>) broker, attributes);
        _broker = broker;
    }

    @Override
    public int getPriority()
    {
        return _priority;
    }

    @Override
    public AccessControl getController()
    {
        return AccessControl.ALWAYS_ALLOWED;
    }

    @Override
    protected void logOperation(final String operation)
    {
        getEventLogger().message(AccessControlMessages.OPERATION(operation));
    }

    private EventLogger getEventLogger()
    {
        return _broker.getEventLogger();
    }

    @StateTransition(currentState = {State.UNINITIALIZED, State.QUIESCED, State.ERRORED}, desiredState = State.ACTIVE)
    @SuppressWarnings("unused")
    private CompletableFuture<Void> activate()
    {

        setState(_broker.isManagementMode() ? State.QUIESCED : State.ACTIVE);
        return CompletableFuture.completedFuture(null);
    }


    @StateTransition(currentState = State.UNINITIALIZED, desiredState = State.QUIESCED)
    @SuppressWarnings("unused")
    private CompletableFuture<Void> startQuiesced()
    {
        setState(State.QUIESCED);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public int compareTo(final AccessControlProvider<?> o)
    {
        return ACCESS_CONTROL_PROVIDER_COMPARATOR.compare(this, o);
    }

    @Override
    protected void logCreated(final Map<String, Object> attributes,
                              final Outcome outcome)
    {
        getEventLogger().message(AccessControlMessages.CREATE(getName(),
                                                              String.valueOf(outcome),
                                                              attributesAsString(attributes)));
    }

    @Override
    protected void logRecovered(final Outcome outcome)
    {
        getEventLogger().message(AccessControlMessages.OPEN(getName(), String.valueOf(outcome)));
    }

    @Override
    protected void logDeleted(final Outcome outcome)
    {
        getEventLogger().message(AccessControlMessages.DELETE(getName(), String.valueOf(outcome)));
    }


    @Override
    protected void logUpdated(final Map<String, Object> attributes, final Outcome outcome)
    {
        getEventLogger().message(AccessControlMessages.UPDATE(getName(), outcome.name(), attributesAsString(attributes)));
    }
}
