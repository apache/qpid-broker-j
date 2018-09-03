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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import org.apache.qpid.server.exchange.AbstractExchange;
import org.apache.qpid.server.filter.FilterManager;
import org.apache.qpid.server.filter.Filterable;
import org.apache.qpid.server.message.MessageDestination;

public class TopicExchangeResultTest
{

    @Test
    public void processMessageForUnfilteredDestinations()
    {
        final TopicExchangeResult result = new TopicExchangeResult();

        final MessageDestination unfilteredDestination1 = mock(MessageDestination.class);
        result.addUnfilteredDestination(unfilteredDestination1);
        result.addBinding(new AbstractExchange.BindingIdentifier("key1", unfilteredDestination1),
                          Collections.emptyMap());

        final MessageDestination unfilteredDestination2 = mock(MessageDestination.class);
        result.addUnfilteredDestination(unfilteredDestination2);
        result.addBinding(new AbstractExchange.BindingIdentifier("key2", unfilteredDestination2),
                          Collections.singletonMap(BINDING_ARGUMENT_REPLACEMENT_ROUTING_KEY, "replacement"));

        final Filterable msg = mock(Filterable.class);
        final Map<MessageDestination, Set<String>> matches = new HashMap<>();

        result.processMessage(msg, matches, "test");

        assertTrue("Unfiltered destination is not found ", matches.containsKey(unfilteredDestination1));
        assertEquals(Collections.singleton("test"), matches.get(unfilteredDestination1));
        assertTrue("Replacement key destination is not found ", matches.containsKey(unfilteredDestination2));
        assertEquals(Collections.singleton("replacement"), matches.get(unfilteredDestination2));
    }


    @Test
    public void processMessageForFilteredDestinations()
    {
        final TopicExchangeResult result = new TopicExchangeResult();

        final MessageDestination matchingFilteredDestination = mock(MessageDestination.class);
        final FilterManager matchingFilter = mock(FilterManager.class);
        result.addFilteredDestination(matchingFilteredDestination, matchingFilter);
        result.addBinding(new AbstractExchange.BindingIdentifier("key1", matchingFilteredDestination),
                          Collections.emptyMap());
        result.addBinding(new AbstractExchange.BindingIdentifier("key3", matchingFilteredDestination),
                          Collections.singletonMap(BINDING_ARGUMENT_REPLACEMENT_ROUTING_KEY, "replacement"));

        final MessageDestination notMatchingFilteredDestination = mock(MessageDestination.class);
        final FilterManager nonMatchingFilter = mock(FilterManager.class);
        result.addFilteredDestination(notMatchingFilteredDestination, nonMatchingFilter);
        result.addBinding(new AbstractExchange.BindingIdentifier("key2", notMatchingFilteredDestination),
                          Collections.emptyMap());

        final Filterable msg = mock(Filterable.class);
        when(matchingFilter.allAllow(msg)).thenReturn(true);
        when(nonMatchingFilter.allAllow(msg)).thenReturn(false);

        final Map<MessageDestination, Set<String>> matches = new HashMap<>();
        result.processMessage(msg, matches, "test");

        assertTrue("Matched destination is not found ", matches.containsKey(matchingFilteredDestination));
        assertEquals(Collections.singleton("replacement"), matches.get(matchingFilteredDestination));
        assertFalse("Unfiltered destination is not found ", matches.containsKey(notMatchingFilteredDestination));
    }

}
