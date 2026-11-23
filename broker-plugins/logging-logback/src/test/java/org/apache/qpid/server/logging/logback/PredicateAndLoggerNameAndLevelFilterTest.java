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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.Set;

import javax.security.auth.Subject;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import org.apache.qpid.server.connection.ConnectionPrincipal;
import org.apache.qpid.server.model.preferences.GenericPrincipal;
import org.apache.qpid.server.security.SubjectExecutionContext;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.test.utils.UnitTestBase;

class PredicateAndLoggerNameAndLevelFilterTest extends UnitTestBase
{
    private static final String LOGGER_NAME = "test.logger";

    @ParameterizedTest
    @CsvSource({"INFO, INFO, true, ACCEPT", "INFO, INFO, false, NEUTRAL", "ERROR, INFO, true, DENY",
                "ERROR, INFO, false, NEUTRAL", "ERROR, ERROR, true, ACCEPT", "ERROR, ERROR, false, NEUTRAL",
                "OFF, ERROR, true, DENY", "OFF, ERROR, false, NEUTRAL"})
    void testExactNameDecisionAppliesOnlyToMatchingPredicate(final String threshold,
                                                             final String eventLevel,
                                                             final boolean matches,
                                                             final FilterReply expected)
    {
        final PredicateAndLoggerNameAndLevelFilter.Predicate predicate = mock();
        final ILoggingEvent event = event(Level.toLevel(eventLevel));
        when(predicate.evaluate(event)).thenReturn(matches);
        final PredicateAndLoggerNameAndLevelFilter filter =
                new PredicateAndLoggerNameAndLevelFilter(LOGGER_NAME, Level.toLevel(threshold), predicate);

        assertEquals(expected, filter.decide(event));
        verify(predicate).evaluate(event);
    }

    @ParameterizedTest
    @CsvSource({"test.*, true, ACCEPT", "test.*, false, NEUTRAL", "ROOT, true, ACCEPT", "ROOT, false, NEUTRAL",
                "'', true, ACCEPT", "'', false, NEUTRAL", ", true, ACCEPT", ", false, NEUTRAL"})
    void testWildcardAcceptanceRequiresMatchingPredicate(final String loggerName,
                                                         final boolean matches,
                                                         final FilterReply expected)
    {
        final PredicateAndLoggerNameAndLevelFilter filter =
                new PredicateAndLoggerNameAndLevelFilter(loggerName, Level.INFO, event -> matches);

        assertEquals(expected, filter.decide(event(Level.INFO)));
    }

    @ParameterizedTest
    @CsvSource({"unrelated.logger, ERROR", "unrelated.*, INFO", "test.*, ERROR", "test.*, OFF", "ROOT, ERROR",
                "'', ERROR", ", ERROR"})
    void testNeutralNameOrLevelDecisionSkipsPredicate(final String loggerName, final String threshold)
    {
        final PredicateAndLoggerNameAndLevelFilter.Predicate predicate = mock();
        final PredicateAndLoggerNameAndLevelFilter filter =
                new PredicateAndLoggerNameAndLevelFilter(loggerName, Level.toLevel(threshold), predicate);

        assertEquals(FilterReply.NEUTRAL, filter.decide(event(Level.INFO)));
        verifyNoInteractions(predicate);
    }

    @ParameterizedTest
    @CsvSource({"ERROR, true", "ERROR, false", "OFF, true", "OFF, false"})
    void testScopedVetoWithWildcardAcceptance(final String threshold, final boolean scopedRuleFirst)
    {
        final ConnectionAndUserPredicate predicate = new ConnectionAndUserPredicate();
        predicate.setUsernamePattern("alice@.*");
        predicate.setConnectionNamePattern("client:1234");
        predicate.setRemoteContainerIdPattern("container");
        final PredicateAndLoggerNameAndLevelFilter scopedFilter =
                new PredicateAndLoggerNameAndLevelFilter(LOGGER_NAME, Level.toLevel(threshold), predicate);
        final LogBackLogInclusionRule scopedRule = rule(scopedFilter);
        final LogBackLogInclusionRule wildcardRule = rule(new LoggerNameAndLevelFilter("test.*", Level.DEBUG));
        final CompositeFilter destination = new CompositeFilter();
        destination.addLogInclusionRule(scopedRuleFirst ? scopedRule : wildcardRule);
        destination.addLogInclusionRule(scopedRuleFirst ? wildcardRule : scopedRule);
        final ILoggingEvent event = event(Level.INFO);

        assertDecision(destination, event, subject("alice", "client:1234", "container"), FilterReply.DENY);
        assertDecision(destination, event, subject("bob", "client:1234", "container"), FilterReply.ACCEPT);
        assertDecision(destination, event, subject("alice", "client:5678", "container"), FilterReply.ACCEPT);
        assertDecision(destination, event, subject("alice", "client:1234", "other"), FilterReply.ACCEPT);
        assertDecision(destination, event, new Subject(), FilterReply.ACCEPT);
        assertDecision(destination, event, null, FilterReply.ACCEPT);

        final CompositeFilter scopedOnlyDestination = new CompositeFilter();
        scopedOnlyDestination.addLogInclusionRule(scopedRule);
        assertDecision(scopedOnlyDestination, event, subject("bob", "client:1234", "container"), FilterReply.DENY);
        assertDecision(scopedOnlyDestination, event, null, FilterReply.DENY);
        assertDecision(scopedOnlyDestination, event(Level.ERROR), subject("alice", "client:1234", "container"),
                "OFF".equals(threshold) ? FilterReply.DENY : FilterReply.ACCEPT);
        assertDecision(scopedOnlyDestination, event(Level.ERROR), subject("bob", "client:1234", "container"),
                FilterReply.DENY);
    }

    private ILoggingEvent event(final Level level)
    {
        final LoggingEvent event = new LoggingEvent();
        event.setLoggerName(LOGGER_NAME);
        event.setLevel(level);
        return event;
    }

    private LogBackLogInclusionRule rule(final Filter<ILoggingEvent> filter)
    {
        final LogBackLogInclusionRule rule = mock();
        when(rule.asFilter()).thenReturn(filter);
        return rule;
    }

    private Subject subject(final String username, final String connectionName, final String remoteContainer)
    {
        final AMQPConnection<?> connection = mock();
        when(connection.getRemoteAddressString()).thenReturn(connectionName);
        when(connection.getRemoteContainerName()).thenReturn(remoteContainer);
        final GenericPrincipal principal = new GenericPrincipal(username + "@test('auth')");
        final AuthenticatedPrincipal user = new AuthenticatedPrincipal(principal);
        return new Subject(true, Set.of(user, new ConnectionPrincipal(connection)), Set.of(), Set.of());
    }

    private void assertDecision(final CompositeFilter filter,
                                final ILoggingEvent event,
                                final Subject subject,
                                final FilterReply expected)
    {
        SubjectExecutionContext.withSubject(subject, () -> assertEquals(expected, filter.decide(event)));
    }
}
