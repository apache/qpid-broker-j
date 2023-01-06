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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;

import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class CompositeFilterTest extends UnitTestBase
{
    @Test
    public void testDecideWithNoRule()
    {
        final CompositeFilter compositeFilter = new CompositeFilter();
        final FilterReply reply = compositeFilter.decide(mock(ILoggingEvent.class));
        assertEquals(FilterReply.DENY, reply, "Unexpected reply with no rule added");
    }

    @Test
    public void testDecideWithAcceptRule()
    {
        final CompositeFilter compositeFilter = new CompositeFilter();

        compositeFilter.addLogInclusionRule(createRule(FilterReply.ACCEPT));

        final ILoggingEvent loggingEvent = mock(ILoggingEvent.class);
        when(loggingEvent.getLevel()).thenReturn(Level.ERROR);
        final FilterReply reply = compositeFilter.decide(loggingEvent);
        assertEquals(FilterReply.ACCEPT, reply, "Unexpected reply with ACCEPT rule added");
    }

    @Test
    public void testDecideWithNeutralRule()
    {
        final CompositeFilter compositeFilter = new CompositeFilter();

        compositeFilter.addLogInclusionRule(createRule(FilterReply.NEUTRAL));

        final ILoggingEvent loggingEvent = mock(ILoggingEvent.class);
        when(loggingEvent.getLevel()).thenReturn(Level.ERROR);
        final FilterReply reply = compositeFilter.decide(loggingEvent);
        assertEquals(FilterReply.DENY, reply, "Unexpected reply with NEUTRAL rule added");
    }

    @Test
    public void testDecideWithMultipleRules()
    {
        final CompositeFilter compositeFilter = new CompositeFilter();

        final LogBackLogInclusionRule neutral = createRule(FilterReply.NEUTRAL);
        compositeFilter.addLogInclusionRule(neutral);

        final LogBackLogInclusionRule accept = createRule(FilterReply.ACCEPT);
        compositeFilter.addLogInclusionRule(accept);

        final LogBackLogInclusionRule deny = createRule(FilterReply.DENY);
        compositeFilter.addLogInclusionRule(deny);

        final FilterReply reply = compositeFilter.decide(mock(ILoggingEvent.class));
        assertEquals(FilterReply.DENY, reply, "Unexpected reply");

        verify(neutral.asFilter()).decide(any(ILoggingEvent.class));
        verify(deny.asFilter()).decide(any(ILoggingEvent.class));
        verify(accept.asFilter()).decide(any(ILoggingEvent.class));
    }

    @Test
    public void testRemoveLogInclusionRule()
    {
        final CompositeFilter compositeFilter = new CompositeFilter();

        final LogBackLogInclusionRule neutral = createRule(FilterReply.NEUTRAL, "neutral");
        compositeFilter.addLogInclusionRule(neutral);

        final LogBackLogInclusionRule deny = createRule(FilterReply.DENY, "deny");
        compositeFilter.addLogInclusionRule(deny);

        final LogBackLogInclusionRule accept = createRule(FilterReply.ACCEPT, "accept");
        compositeFilter.addLogInclusionRule(accept);

        final FilterReply reply = compositeFilter.decide(mock(ILoggingEvent.class));
        assertEquals(FilterReply.DENY, reply, "Unexpected reply");

        compositeFilter.removeLogInclusionRule(deny);

        final ILoggingEvent loggingEvent = mock(ILoggingEvent.class);
        when(loggingEvent.getLevel()).thenReturn(Level.ERROR);
        final FilterReply reply2 = compositeFilter.decide(loggingEvent);
        assertEquals(FilterReply.ACCEPT, reply2, "Unexpected reply");

        verify(neutral.asFilter(), times(2)).decide(any(ILoggingEvent.class));
        verify(deny.asFilter()).decide(any(ILoggingEvent.class));
        verify(accept.asFilter()).decide(any(ILoggingEvent.class));
    }

    @Test
    public void testAddLogInclusionRule()
    {
        final CompositeFilter compositeFilter = new CompositeFilter();

        final LogBackLogInclusionRule rule = createRule(FilterReply.ACCEPT, "accept");
        compositeFilter.addLogInclusionRule(rule);

        verify(rule.asFilter()).setName("accept");
    }

    private LogBackLogInclusionRule createRule(final FilterReply decision)
    {
        return createRule(decision, "UNNAMED");
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private LogBackLogInclusionRule createRule(final FilterReply decision, final String name)
    {
        final LogBackLogInclusionRule rule = mock(LogBackLogInclusionRule.class);
        when(rule.getName()).thenReturn(name);
        final Filter filter = mock(Filter.class);
        when(filter.getName()).thenReturn(name);
        when(filter.decide(any(ILoggingEvent.class))).thenReturn(decision);
        when(rule.asFilter()).thenReturn(filter);
        return rule;
    }
}
