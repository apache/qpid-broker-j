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
package org.apache.qpid.server.logging;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;
import org.apache.qpid.server.model.BrokerLoggerFilter;

public class CompositeFilter extends Filter<ILoggingEvent>
{
    private List<Filter<ILoggingEvent>> _filterList = new CopyOnWriteArrayList<>();

    public void addFilter(BrokerLoggerFilter filter)
    {
        Filter f = filter.asFilter();
        f.setName(filter.getName());
        _filterList.add(f);
    }

    public void addFilters(Collection<BrokerLoggerFilter> filters)
    {
        for(BrokerLoggerFilter<?> filter : filters)
        {
            addFilter(filter);
        }
    }

    public void removeFilter(BrokerLoggerFilter filter)
    {
        Iterator<Filter<ILoggingEvent>> it = _filterList.iterator();
        while(it.hasNext())
        {
            Filter f = it.next();
            if (f.getName().equals(filter.getName()))
            {
                _filterList.remove(f);
                break;
            }
        }
    }

    @Override
    public FilterReply decide(ILoggingEvent event)
    {
        for(Filter filter : _filterList)
        {
            FilterReply reply = filter.decide(event);
            if (reply == FilterReply.DENY || reply == FilterReply.ACCEPT)
            {
                return reply;
            }
        }
        return FilterReply.DENY;
    }
}
