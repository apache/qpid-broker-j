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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.StateTransition;

public class TestAbstractInstrumentPanelImpl<X extends TestAbstractInstrumentPanelImpl<X>> extends AbstractConfiguredObject<X>
        implements TestInstrumentPanel<X>
{

    protected TestAbstractInstrumentPanelImpl(final TestCar<?> parent,
                                              final Map<String, Object> attributes)
    {
        super(parent, attributes);
    }

    @Override
    protected void logOperation(final String operation)
    {

    }

    @StateTransition(currentState = {State.UNINITIALIZED, State.ERRORED}, desiredState = State.ACTIVE)
    private ListenableFuture<Void> onActivate()
    {
        setState(State.ACTIVE);
        return Futures.immediateFuture(null);
    }
}
