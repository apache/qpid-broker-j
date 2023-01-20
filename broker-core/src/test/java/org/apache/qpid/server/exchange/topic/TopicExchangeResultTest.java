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
package org.apache.qpid.server.exchange.topic;

import static org.apache.qpid.server.model.Binding.BINDING_ARGUMENT_REPLACEMENT_ROUTING_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.exchange.AbstractExchange;
import org.apache.qpid.server.filter.FilterManager;
import org.apache.qpid.server.filter.Filterable;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.test.utils.UnitTestBase;

public class TopicExchangeResultTest extends UnitTestBase
{
    @Test
    public void processMessageForUnfilteredDestinations()
    {
        final TopicExchangeResult result = new TopicExchangeResult();

        final MessageDestination unfilteredDestination1 = mock(MessageDestination.class);
        result.addUnfilteredDestination(unfilteredDestination1);
        result.addBinding(new AbstractExchange.BindingIdentifier("key1", unfilteredDestination1), Map.of());

        final MessageDestination unfilteredDestination2 = mock(MessageDestination.class);
        result.addUnfilteredDestination(unfilteredDestination2);
        result.addBinding(new AbstractExchange.BindingIdentifier("key2", unfilteredDestination2),
                Map.of(BINDING_ARGUMENT_REPLACEMENT_ROUTING_KEY, "replacement"));

        final Filterable msg = mock(Filterable.class);
        final Map<MessageDestination, Set<String>> matches = new HashMap<>();

        result.processMessage(msg, matches, "test");

        assertTrue(matches.containsKey(unfilteredDestination1), "Unfiltered destination is not found ");
        assertEquals(Set.of("test"), matches.get(unfilteredDestination1));
        assertTrue(matches.containsKey(unfilteredDestination2), "Replacement key destination is not found ");
        assertEquals(Set.of("replacement"), matches.get(unfilteredDestination2));
    }


    @Test
    public void processMessageForFilteredDestinations()
    {
        final TopicExchangeResult result = new TopicExchangeResult();
        final MessageDestination matchingFilteredDestination = mock(MessageDestination.class);
        final FilterManager matchingFilter = mock(FilterManager.class);
        result.addFilteredDestination(matchingFilteredDestination, matchingFilter);
        result.addBinding(new AbstractExchange.BindingIdentifier("key1", matchingFilteredDestination),
                Map.of());
        result.addBinding(new AbstractExchange.BindingIdentifier("key3", matchingFilteredDestination),
                Map.of(BINDING_ARGUMENT_REPLACEMENT_ROUTING_KEY, "replacement"));

        final MessageDestination notMatchingFilteredDestination = mock(MessageDestination.class);
        final FilterManager nonMatchingFilter = mock(FilterManager.class);
        result.addFilteredDestination(notMatchingFilteredDestination, nonMatchingFilter);
        result.addBinding(new AbstractExchange.BindingIdentifier("key2", notMatchingFilteredDestination),
                Map.of());

        final Filterable msg = mock(Filterable.class);
        when(matchingFilter.allAllow(msg)).thenReturn(true);
        when(nonMatchingFilter.allAllow(msg)).thenReturn(false);

        final Map<MessageDestination, Set<String>> matches = new HashMap<>();
        result.processMessage(msg, matches, "test");

        assertTrue(matches.containsKey(matchingFilteredDestination), "Matched destination is not found ");
        assertEquals(Set.of("replacement"), matches.get(matchingFilteredDestination));
        assertFalse(matches.containsKey(notMatchingFilteredDestination), "Unfiltered destination is not found ");
    }
}
