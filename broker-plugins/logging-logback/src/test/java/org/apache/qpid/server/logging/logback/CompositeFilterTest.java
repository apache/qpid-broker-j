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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;
import org.junit.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class CompositeFilterTest extends UnitTestBase
{
    @Test
    public void testDecideWithNoRule()
    {
        CompositeFilter compositeFilter = new CompositeFilter();
        FilterReply reply = compositeFilter.decide(mock(ILoggingEvent.class));
        assertEquals("Unexpected reply with no rule added", FilterReply.DENY, reply);
    }

    @Test
    public void testDecideWithAcceptRule()
    {
        CompositeFilter compositeFilter = new CompositeFilter();

        compositeFilter.addLogInclusionRule(createRule(FilterReply.ACCEPT));

        final ILoggingEvent loggingEvent = mock(ILoggingEvent.class);
        when(loggingEvent.getLevel()).thenReturn(Level.ERROR);
        FilterReply reply = compositeFilter.decide(loggingEvent);
        assertEquals("Unexpected reply with ACCEPT rule added", FilterReply.ACCEPT, reply);
    }

    @Test
    public void testDecideWithNeutralRule()
    {
        CompositeFilter compositeFilter = new CompositeFilter();

        compositeFilter.addLogInclusionRule(createRule(FilterReply.NEUTRAL));

        final ILoggingEvent loggingEvent = mock(ILoggingEvent.class);
        when(loggingEvent.getLevel()).thenReturn(Level.ERROR);
        FilterReply reply = compositeFilter.decide(loggingEvent);
        assertEquals("Unexpected reply with NEUTRAL rule added", FilterReply.DENY, reply);
    }

    @Test
    public void testDecideWithMultipleRules()
    {
        CompositeFilter compositeFilter = new CompositeFilter();

        LogBackLogInclusionRule neutral = createRule(FilterReply.NEUTRAL);
        compositeFilter.addLogInclusionRule(neutral);

        LogBackLogInclusionRule accept = createRule(FilterReply.ACCEPT);
        compositeFilter.addLogInclusionRule(accept);

        LogBackLogInclusionRule deny = createRule(FilterReply.DENY);
        compositeFilter.addLogInclusionRule(deny);

        FilterReply reply = compositeFilter.decide(mock(ILoggingEvent.class));
        assertEquals("Unexpected reply", FilterReply.DENY, reply);

        verify(neutral.asFilter()).decide(any(ILoggingEvent.class));
        verify(deny.asFilter()).decide(any(ILoggingEvent.class));
        verify(accept.asFilter()).decide(any(ILoggingEvent.class));
    }

    @Test
    public void testRemoveLogInclusionRule()
    {
        CompositeFilter compositeFilter = new CompositeFilter();

        LogBackLogInclusionRule neutral = createRule(FilterReply.NEUTRAL, "neutral");
        compositeFilter.addLogInclusionRule(neutral);

        LogBackLogInclusionRule deny = createRule(FilterReply.DENY, "deny");
        compositeFilter.addLogInclusionRule(deny);

        LogBackLogInclusionRule accept = createRule(FilterReply.ACCEPT, "accept");
        compositeFilter.addLogInclusionRule(accept);

        FilterReply reply = compositeFilter.decide(mock(ILoggingEvent.class));
        assertEquals("Unexpected reply", FilterReply.DENY, reply);

        compositeFilter.removeLogInclusionRule(deny);

        final ILoggingEvent loggingEvent = mock(ILoggingEvent.class);
        when(loggingEvent.getLevel()).thenReturn(Level.ERROR);
        FilterReply reply2 = compositeFilter.decide(loggingEvent);
        assertEquals("Unexpected reply", FilterReply.ACCEPT, reply2);

        verify(neutral.asFilter(), times(2)).decide(any(ILoggingEvent.class));
        verify(deny.asFilter()).decide(any(ILoggingEvent.class));
        verify(accept.asFilter()).decide(any(ILoggingEvent.class));
    }

    @Test
    public void testAddLogInclusionRule()
    {
        CompositeFilter compositeFilter = new CompositeFilter();

        LogBackLogInclusionRule rule = createRule(FilterReply.ACCEPT, "accept");
        compositeFilter.addLogInclusionRule(rule);

        verify(rule.asFilter()).setName("accept");
    }

    private LogBackLogInclusionRule createRule(FilterReply decision)
    {
        return createRule(decision, "UNNAMED");
    }

    private LogBackLogInclusionRule createRule(final FilterReply decision, String name)
    {
        LogBackLogInclusionRule rule = mock(LogBackLogInclusionRule.class);
        when(rule.getName()).thenReturn(name);
        Filter filter = mock(Filter.class);
        when(filter.getName()).thenReturn(name);
        when(filter.decide(any(ILoggingEvent.class))).thenReturn(decision);
        when(rule.asFilter()).thenReturn(filter);
        return rule;
    }
}
