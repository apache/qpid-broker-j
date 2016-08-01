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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.turbo.TurboFilter;
import ch.qos.logback.core.spi.FilterReply;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

public final class QpidLoggerTurboFilter extends TurboFilter
{
    private final CopyOnWriteArrayList<EffectiveLevelFilter> _filters = new CopyOnWriteArrayList<>();
    private final AtomicReference<ConcurrentMap<Logger, Integer>> _effectiveLevels =
            new AtomicReference<>();
    private volatile int _minimumFilterLevel = Level.OFF.levelInt;
    /** Ensures updates to _minimumLogLevel are single threaded */
    private final Object _minimumFilterLevelUpdateLock = new Object();

    public QpidLoggerTurboFilter()
    {
        resetCacheAndSetMinimumLevel();
    }

    @Override
    public FilterReply decide(final Marker marker,
                              final Logger logger,
                              final Level level,
                              final String format,
                              final Object[] params,
                              final Throwable t)
    {
        if (level.levelInt  < _minimumFilterLevel)
        {
            // Optimisation - no filters accept an event with this level
            return FilterReply.DENY;
        }

        final ConcurrentMap<Logger, Integer> effectiveLevels = _effectiveLevels.get();
        Integer effectiveLoggerLevel = effectiveLevels.get(logger);
        if(effectiveLoggerLevel == null)
        {
            effectiveLoggerLevel = Level.OFF.levelInt;
            for (EffectiveLevelFilter filter : _filters)
            {
                Integer loggerLevel = filter.getEffectiveLevel(logger).levelInt;
                if (effectiveLoggerLevel >= loggerLevel)
                {
                    effectiveLoggerLevel = loggerLevel;
                }
            }
            effectiveLevels.putIfAbsent(logger, effectiveLoggerLevel);
        }

        return level.levelInt >= effectiveLoggerLevel ? FilterReply.ACCEPT : FilterReply.DENY;
    }

    public void filterAdded(EffectiveLevelFilter filter)
    {
        if(_filters.addIfAbsent(filter))
        {
            resetCacheAndSetMinimumLevel();
        }
    }

    public void filterRemoved(EffectiveLevelFilter filter)
    {
        if(_filters.remove(filter))
        {
            resetCacheAndSetMinimumLevel();
        }
    }

    public void filterChanged(EffectiveLevelFilter filter)
    {
        if(_filters.contains(filter))
        {
            resetCacheAndSetMinimumLevel();
        }
    }

    private void resetCacheAndSetMinimumLevel()
    {
        // Disable the 'decide' optimisation until the new minimum is established
        _minimumFilterLevel = Level.ALL.levelInt;
        _effectiveLevels.set(new ConcurrentHashMap<Logger, Integer>());
        synchronized (_minimumFilterLevelUpdateLock)
        {
            int newMinimumLogLevel = Level.OFF.levelInt;
            for (EffectiveLevelFilter filter : _filters)
            {
                newMinimumLogLevel = Math.min(filter.getLevel().levelInt, newMinimumLogLevel);
            }
            _minimumFilterLevel = newMinimumLogLevel;
        }
    }

    @Override
    public boolean equals(final Object o)
    {
        return (o != null && getClass() == o.getClass());
    }

    @Override
    public int hashCode()
    {
        return 0;
    }

    public static QpidLoggerTurboFilter installIfNecessary(LoggerContext loggerContext)
    {
        QpidLoggerTurboFilter filter = new QpidLoggerTurboFilter();
        if(!loggerContext.getTurboFilterList().addIfAbsent(filter))
        {
            for(TurboFilter candidate : loggerContext.getTurboFilterList())
            {
                if(candidate instanceof QpidLoggerTurboFilter)
                {
                    filter = (QpidLoggerTurboFilter)candidate;
                    break;
                }
            }
        }
        return filter;
    }


    public static QpidLoggerTurboFilter installIfNecessaryToRootContext()
    {
        return installIfNecessary(getRootContext());
    }

    public static void uninstallFromRootContext()
    {
        uninstall(getRootContext());
    }

    public static void uninstall(LoggerContext context)
    {
        context.getTurboFilterList().remove(new QpidLoggerTurboFilter());
    }

    private static LoggerContext getRootContext()
    {
        final Logger rootLogger = (Logger) (LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME));
        return rootLogger.getLoggerContext();
    }

    public static void filterAdded(EffectiveLevelFilter filter, LoggerContext context)
    {
        QpidLoggerTurboFilter turboFilter = installIfNecessary(context);
        turboFilter.filterAdded(filter);
    }

    public static void filterRemoved(EffectiveLevelFilter filter, LoggerContext context)
    {
        QpidLoggerTurboFilter turboFilter = installIfNecessary(context);
        turboFilter.filterRemoved(filter);
    }

    private static void filterChanged(final EffectiveLevelFilter filter, final LoggerContext context)
    {
        QpidLoggerTurboFilter turboFilter = installIfNecessary(context);
        turboFilter.filterChanged(filter);
    }

    public static void filterAddedToRootContext(EffectiveLevelFilter filter)
    {
        filterAdded(filter, getRootContext());
    }

    public static void filterRemovedFromRootContext(EffectiveLevelFilter filter)
    {
        filterRemoved(filter, getRootContext());
    }

    public static void filterChangedOnRootContext(final EffectiveLevelFilter filter)
    {
        filterChanged(filter, getRootContext());
    }

}
