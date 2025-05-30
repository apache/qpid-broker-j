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

package org.apache.qpid.server.model.testmodels.hierarchy;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.StateTransition;

public class TestAbstractEngineImpl<X extends TestAbstractEngineImpl<X>> extends AbstractConfiguredObject<X> implements TestEngine<X>
{
    public static final int TEST_TEMPERATURE = 50;
    @ManagedAttributeField
    private CompletableFuture<Void> _beforeCloseFuture = CompletableFuture.completedFuture(null);

    @ManagedAttributeField
    private Object _stateChangeFuture = CompletableFuture.completedFuture(null);

    @ManagedAttributeField
    private RuntimeException _stateChangeException;

    public TestAbstractEngineImpl(final ConfiguredObject<?> parent,
                                  final Map<String, Object> attributes)
    {
        super(parent, attributes);
    }

    @Override
    public Object getBeforeCloseFuture()
    {
        return _beforeCloseFuture;
    }

    @Override
    public void setBeforeCloseFuture(final CompletableFuture<Void> completableFuture)
    {
        _beforeCloseFuture = completableFuture;
    }

    @Override
    public Object getStateChangeFuture()
    {
        return _stateChangeFuture;
    }

    @Override
    public void setStateChangeFuture(final CompletableFuture<Void> completableFuture)
    {
        _stateChangeFuture = completableFuture;
    }

    @Override
    public Object getStateChangeException()
    {
        return _stateChangeException;
    }

    @Override
    public void setStateChangeException(final RuntimeException exception)
    {
        _stateChangeException = exception;
    }

    @Override
    protected CompletableFuture<Void> beforeClose()
    {
        return _beforeCloseFuture;
    }

    @Override
    protected void logOperation(final String operation)
    {

    }

    @StateTransition(currentState = {State.UNINITIALIZED, State.ERRORED}, desiredState = State.ACTIVE)
    @SuppressWarnings({"unchecked", "unused"})
    private CompletableFuture<Void> onActivate()
    {
        final RuntimeException stateChangeException = _stateChangeException;
        if (stateChangeException != null)
        {
            throw stateChangeException;
        }
        setState(State.ACTIVE);
        return (CompletableFuture<Void>) _stateChangeFuture;
    }

    @Override
    public int getTemperature()
    {
        return TEST_TEMPERATURE;
    }
}
