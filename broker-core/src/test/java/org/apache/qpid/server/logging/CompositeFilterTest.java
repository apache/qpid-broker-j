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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;
import org.apache.qpid.test.utils.QpidTestCase;

public class CompositeFilterTest extends QpidTestCase
{
    public void testDecideWithNoFilterAdded()
    {
        CompositeFilter compositeFilter = new CompositeFilter();
        FilterReply reply = compositeFilter.decide(mock(ILoggingEvent.class));
        assertEquals("Unexpected reply with no filter added", FilterReply.DENY, reply);
    }

    public void testDecideWithAcceptFilter()
    {
        CompositeFilter compositeFilter = new CompositeFilter();

        compositeFilter.addFilter(createFilter(FilterReply.ACCEPT));

        FilterReply reply = compositeFilter.decide(mock(ILoggingEvent.class));
        assertEquals("Unexpected reply with ACCEPT filter added", FilterReply.ACCEPT, reply);
    }

    public void testDecideWithNeutralFilter()
    {
        CompositeFilter compositeFilter = new CompositeFilter();

        compositeFilter.addFilter(createFilter(FilterReply.NEUTRAL));

        FilterReply reply = compositeFilter.decide(mock(ILoggingEvent.class));
        assertEquals("Unexpected reply with NEUTRAL filter added", FilterReply.DENY, reply);
    }

    public void testDecideWithFilterChain()
    {
        CompositeFilter compositeFilter = new CompositeFilter();

        LoggerFilter brokerFilterNeutral = createFilter(FilterReply.NEUTRAL);
        compositeFilter.addFilter(brokerFilterNeutral);

        LoggerFilter brokerFilterDeny = createFilter(FilterReply.DENY);
        compositeFilter.addFilter(brokerFilterDeny);

        LoggerFilter brokerFilterAccept = createFilter(FilterReply.ACCEPT);
        compositeFilter.addFilter(brokerFilterAccept);

        FilterReply reply = compositeFilter.decide(mock(ILoggingEvent.class));
        assertEquals("Unexpected reply", FilterReply.DENY, reply);

        verify(brokerFilterNeutral.asFilter()).decide(any(ILoggingEvent.class));
        verify(brokerFilterDeny.asFilter()).decide(any(ILoggingEvent.class));
        verify(brokerFilterAccept.asFilter(), never()).decide(any(ILoggingEvent.class));
    }

    public void testRemoveFilterFromChain()
    {
        CompositeFilter compositeFilter = new CompositeFilter();

        LoggerFilter brokerFilterNeutral = createFilter(FilterReply.NEUTRAL, "neutral");
        compositeFilter.addFilter(brokerFilterNeutral);

        LoggerFilter brokerFilterDeny = createFilter(FilterReply.DENY, "deny");
        compositeFilter.addFilter(brokerFilterDeny);

        LoggerFilter brokerFilterAccept = createFilter(FilterReply.ACCEPT, "accept");
        compositeFilter.addFilter(brokerFilterAccept);

        FilterReply reply = compositeFilter.decide(mock(ILoggingEvent.class));
        assertEquals("Unexpected reply", FilterReply.DENY, reply);

        compositeFilter.removeFilter(brokerFilterDeny);

        FilterReply reply2 = compositeFilter.decide(mock(ILoggingEvent.class));
        assertEquals("Unexpected reply", FilterReply.ACCEPT, reply2);

        verify(brokerFilterNeutral.asFilter(), times(2)).decide(any(ILoggingEvent.class));
        verify(brokerFilterDeny.asFilter()).decide(any(ILoggingEvent.class));
        verify(brokerFilterAccept.asFilter()).decide(any(ILoggingEvent.class));
    }

    public void testAddFilter()
    {
        CompositeFilter compositeFilter = new CompositeFilter();

        LoggerFilter brokerFilter = createFilter(FilterReply.ACCEPT, "accept");
        compositeFilter.addFilter(brokerFilter);

        verify(brokerFilter.asFilter()).setName("accept");
    }

    public void testAddFilters()
    {
        CompositeFilter compositeFilter = new CompositeFilter();

        LoggerFilter brokerFilterNeutral = createFilter(FilterReply.NEUTRAL, "neutral");
        LoggerFilter brokerFilterAccept = createFilter(FilterReply.ACCEPT, "accept");
        LoggerFilter brokerFilterDeny = createFilter(FilterReply.DENY, "deny");

        compositeFilter.addFilters(Arrays.asList(brokerFilterNeutral, brokerFilterAccept, brokerFilterDeny));

        FilterReply reply = compositeFilter.decide(mock(ILoggingEvent.class));
        assertEquals("Unexpected reply", FilterReply.ACCEPT, reply);

        verify(brokerFilterNeutral.asFilter()).decide(any(ILoggingEvent.class));
        verify(brokerFilterAccept.asFilter()).decide(any(ILoggingEvent.class));
        verify(brokerFilterDeny.asFilter(), never()).decide(any(ILoggingEvent.class));
    }

    private LoggerFilter createFilter(FilterReply decision)
    {
        return createFilter(decision, "UNNAMED");
    }

    private LoggerFilter createFilter(final FilterReply decision, String name)
    {
        LoggerFilter brokerFilter = mock(LoggerFilter.class);
        when(brokerFilter.getName()).thenReturn(name);
        Filter filter = mock(Filter.class);
        when(filter.getName()).thenReturn(name);
        when(filter.decide(any(ILoggingEvent.class))).thenReturn(decision);
        when(brokerFilter.asFilter()).thenReturn(filter);
        return brokerFilter;
    }
}
