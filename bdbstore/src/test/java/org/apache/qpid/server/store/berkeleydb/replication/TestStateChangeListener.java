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
package org.apache.qpid.server.store.berkeleydb.replication;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.SettableFuture;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.rep.StateChangeEvent;
import com.sleepycat.je.rep.StateChangeListener;

class TestStateChangeListener implements StateChangeListener
{
    private final AtomicReference<State> _expectedState = new AtomicReference<>();
    private final AtomicReference<SettableFuture<Void>> _currentFuture = new AtomicReference<>(SettableFuture.create());
    private final AtomicReference<State> _currentState = new AtomicReference<>();

    @Override
    public void stateChange(StateChangeEvent stateChangeEvent) throws RuntimeException
    {
        final State newState = stateChangeEvent.getState();
        _currentState.set(newState);
        if (_expectedState.get() == newState)
        {
            _currentFuture.get().set(null);
        }
    }

    public boolean awaitForStateChange(State desiredState, long timeout, TimeUnit timeUnit) throws Exception
    {
        _expectedState.set(desiredState);
        if (desiredState == _currentState.get())
        {
            return true;
        }
        else
        {
            final SettableFuture<Void> future = SettableFuture.create();
            _currentFuture.set(future);
            if (desiredState == _currentState.get())
            {
                future.set(null);
            }

            try
            {
                future.get(timeout, timeUnit);
                return true;
            }
            catch (TimeoutException e)
            {
                return false;
            }
        }
    }

    public State getCurrentActualState()
    {
        return _currentState.get();
    }
}