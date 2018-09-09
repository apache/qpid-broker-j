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

import static ch.qos.logback.classic.Level.ERROR_INT;
import static ch.qos.logback.classic.Level.WARN_INT;
import static ch.qos.logback.core.spi.FilterReply.ACCEPT;
import static ch.qos.logback.core.spi.FilterReply.DENY;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;

public class CompositeFilter extends Filter<ILoggingEvent>
{
    private List<Filter<ILoggingEvent>> _filterList = new CopyOnWriteArrayList<>();
    private final AtomicLong _warnCount = new AtomicLong();
    private final AtomicLong _errorCount = new AtomicLong();

    public void addLogInclusionRule(LogBackLogInclusionRule logInclusionRule)
    {
        Filter<ILoggingEvent> f = logInclusionRule.asFilter();
        f.setName(logInclusionRule.getName());
        _filterList.add(f);
    }

    public void removeLogInclusionRule(LogBackLogInclusionRule logInclusionRule)
    {
        Iterator<Filter<ILoggingEvent>> it = _filterList.iterator();
        while(it.hasNext())
        {
            Filter f = it.next();
            if (f.getName().equals(logInclusionRule.getName()))
            {
                _filterList.remove(f);
                break;
            }
        }
    }

    @Override
    public FilterReply decide(ILoggingEvent event)
    {
        FilterReply reply = DENY;
        for(Filter<ILoggingEvent> filter : _filterList)
        {
            FilterReply filterReply = filter.decide(event);
            if (filterReply == DENY)
            {
                reply = filterReply;
                break;
            }
            if (filterReply == ACCEPT)
            {
                reply = filterReply;
            }
        }
        if(reply == ACCEPT)
        {
            switch(event.getLevel().toInt())
            {
                case WARN_INT:
                    _warnCount.incrementAndGet();
                    break;
                case ERROR_INT:
                    _errorCount.incrementAndGet();
                    break;
                default:
                    // do nothing
            }
            return ACCEPT;
        }
        return DENY;
    }

    public long getErrorCount()
    {
        return _errorCount.get();
    }

    public long getWarnCount()
    {
        return _warnCount.get();
    }
}
