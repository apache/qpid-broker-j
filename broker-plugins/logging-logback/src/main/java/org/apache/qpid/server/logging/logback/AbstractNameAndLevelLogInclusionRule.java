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
package org.apache.qpid.server.logging.logback;

import java.util.Map;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.Filter;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import org.apache.qpid.server.logging.LogLevel;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.StateTransition;

public abstract class AbstractNameAndLevelLogInclusionRule<X extends AbstractNameAndLevelLogInclusionRule<X>> extends AbstractConfiguredObject<X>
{
    @ManagedAttributeField
    private String _loggerName;
    @ManagedAttributeField(afterSet = "logLevelAfterSet")
    private LogLevel _level;

    private LoggerNameAndLevelFilter _filter;

    protected AbstractNameAndLevelLogInclusionRule(final ConfiguredObject<?> parent,
                                                   Map<String, Object> attributes)
    {
        super(parent, attributes);
    }

    @Override
    protected void postResolve()
    {
        super.postResolve();
        _filter = new LoggerNameAndLevelFilter(getLoggerName(), Level.toLevel(getLevel().name()));
    }


    public String getLoggerName()
    {
        return _loggerName;
    }

    public LogLevel getLevel()
    {
        return _level;
    }

    @SuppressWarnings("unused")
    private void logLevelAfterSet()
    {
        if (_filter != null)
        {
            _filter.setLevel(Level.toLevel(getLevel().name()));
            QpidLoggerTurboFilter.filterChangedOnRootContext(_filter);
        }
    }

    public Filter<ILoggingEvent> asFilter()
    {
        return _filter;
    }

    @Override
    protected ListenableFuture<Void> onDelete()
    {
        QpidLoggerTurboFilter.filterRemovedFromRootContext(_filter);
        return super.onDelete();
    }

    @StateTransition( currentState = { State.ERRORED, State.UNINITIALIZED }, desiredState = State.ACTIVE )
    private ListenableFuture<Void> doActivate()
    {
        setState(State.ACTIVE);
        QpidLoggerTurboFilter.filterAddedToRootContext(_filter);
        return Futures.immediateFuture(null);
    }

}
