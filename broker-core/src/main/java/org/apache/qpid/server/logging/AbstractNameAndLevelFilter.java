package org.apache.qpid.server.logging;

import java.util.Map;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.Filter;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.StateTransition;

public abstract class AbstractNameAndLevelFilter<X extends AbstractNameAndLevelFilter<X>> extends AbstractConfiguredObject<X>
{
    @ManagedAttributeField
    private String _loggerName;
    @ManagedAttributeField(afterSet = "logLevelAfterSet")
    private LogLevel _level;

    private LoggerNameAndLevelFilter _filter;

    protected AbstractNameAndLevelFilter(final Map<Class<? extends ConfiguredObject>, ConfiguredObject<?>> parents,
                                         Map<String, Object> attributes)
    {
        super(parents, attributes);
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

    @StateTransition( currentState = { State.ACTIVE, State.ERRORED, State.UNINITIALIZED }, desiredState = State.DELETED )
    private ListenableFuture<Void> doDelete()
    {
        return doAfterAlways(closeAsync(), new Runnable()
        {
            @Override
            public void run()
            {
                deleted();
                QpidLoggerTurboFilter.filterRemovedFromRootContext(_filter);
                setState(State.DELETED);

            }
        });
    }

    @StateTransition( currentState = { State.ERRORED, State.UNINITIALIZED }, desiredState = State.ACTIVE )
    private ListenableFuture<Void> doActivate()
    {
        setState(State.ACTIVE);
        QpidLoggerTurboFilter.filterAddedToRootContext(_filter);
        return Futures.immediateFuture(null);
    }
}
