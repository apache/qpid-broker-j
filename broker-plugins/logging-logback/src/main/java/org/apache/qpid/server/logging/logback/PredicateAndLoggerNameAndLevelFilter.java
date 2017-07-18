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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;

class PredicateAndLoggerNameAndLevelFilter extends LoggerNameAndLevelFilter
{
    private final Filter<ILoggingEvent> _filter;
    private final Predicate _predicate;

    interface Predicate
    {
        boolean evaluate(final ILoggingEvent event);
    }

    PredicateAndLoggerNameAndLevelFilter(String loggerName, Level level, final Predicate predicate)
    {
        super(loggerName, level);
        _filter = createFilter(loggerName);
        _predicate = predicate;
    }

    @Override
    public FilterReply decide(ILoggingEvent event)
    {
        return _filter.decide(event);
    }

    @Override
    protected Filter<ILoggingEvent> createFilter(final String loggerName)
    {
        final Filter<ILoggingEvent> filter = super.createFilter(loggerName);
        return new Filter<ILoggingEvent>()
        {
            @Override
            public FilterReply decide(final ILoggingEvent event)
            {
                final FilterReply result = filter.decide(event);
                if (result == FilterReply.ACCEPT)
                {
                    if (_predicate.evaluate(event))
                    {
                        return FilterReply.ACCEPT;
                    }
                    else
                    {
                        return FilterReply.NEUTRAL;
                    }
                }
                else
                {
                    return result;
                }
            }
        };
    }
}
