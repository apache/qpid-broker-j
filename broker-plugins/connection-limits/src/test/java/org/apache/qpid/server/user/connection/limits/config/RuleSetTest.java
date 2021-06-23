/*
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
 */
package org.apache.qpid.server.user.connection.limits.config;

import java.security.Principal;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import javax.security.auth.Subject;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;
import org.apache.qpid.server.security.auth.TestPrincipalUtils;
import org.apache.qpid.server.security.limit.ConnectionLimitException;
import org.apache.qpid.server.security.limit.ConnectionSlot;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.user.connection.limits.config.RuleSet.Builder;
import org.apache.qpid.test.utils.UnitTestBase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RuleSetTest extends UnitTestBase
{
    private static final String TEST_USER = "user";

    private static final String OTHER_USER = "other";

    private static final String TEST_GROUP1 = "group1";
    private static final String TEST_GROUP2 = "group2";

    private static final String OTHER_GROUP = "anotherGroup";

    private static final String TEST_PORT = "amqp";
    private static final String LIMITER_NAME = "Limiter";

    private EventLogger _eventLogger;

    private AmqpPort<?> _port;

    private Subject _subject;

    private Principal _principal;

    @Before
    public void setUp()
    {
        _eventLogger = Mockito.mock(EventLogger.class);

        _subject = TestPrincipalUtils.createTestSubject(TEST_USER, TEST_GROUP1, TEST_GROUP2);
        for (Principal principal : _subject.getPrincipals())
        {
            if (principal instanceof AuthenticatedPrincipal)
            {
                _principal = principal;
            }
        }
        _port = Mockito.mock(AmqpPort.class);
        Mockito.doReturn(TEST_PORT).when(_port).getName();
    }

    @Test
    public void testFrequencyLimit_multiplePeriods()
    {
        final Duration frequencyPeriod1 = Duration.ofSeconds(1L);
        final Duration frequencyPeriod2 = Duration.ofSeconds(2L);

        final Builder builder = RuleSet.newBuilder(LIMITER_NAME, Duration.ofMinutes(1L));
        builder.addRule(Rule.newNonBlockingRule(TEST_PORT, TEST_USER, null, 3, frequencyPeriod1));
        builder.addRule(Rule.newNonBlockingRule(TEST_PORT, TEST_USER, null, 2, frequencyPeriod2));
        builder.addRule(Rule.newBlockingRule(RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS));

        final RuleSet ruleSet = builder.build();
        assertNotNull(ruleSet);

        final Instant registrationStart = Instant.now();
        try
        {
            ruleSet.register(newConnection()).free();
            ruleSet.register(newConnection()).free();
        }
        catch (ConnectionLimitException e)
        {
            fail("An exception is not expected here");
        }
        final Instant registrationEnd = Instant.now();

        Instant before = Instant.now();
        do
        {
            try
            {
                before = Instant.now();
                ruleSet.register(newConnection()).free();
                assertTrue(Duration.between(registrationStart, Instant.now()).compareTo(frequencyPeriod2) >= 0);
                break;
            }
            catch (ConnectionLimitException e)
            {
                assertTrue(Duration.between(registrationEnd, before).compareTo(frequencyPeriod2) <= 0);
            }
        }
        while (Duration.between(registrationEnd, Instant.now()).compareTo(Duration.ofSeconds(3L)) < 0);
    }

    @Test
    public void testGroupConnectionFrequencyLimit_Concurrency()
    {
        for (final Integer countLimit : Arrays.asList(57, 176, null))
        {
            for (final int threadCount : new int[]{7, 17, 27})
            {
                testGroupConnectionFrequencyLimit_Concurrency(countLimit, threadCount);
            }
        }
    }

    private void testGroupConnectionFrequencyLimit_Concurrency(Integer countLimit, int threadCount)
    {
        if (countLimit != null)
        {
            if (countLimit < 3)
            {
                countLimit = 3;
            }
            if (countLimit < threadCount)
            {
                countLimit = threadCount;
            }
        }
        final Duration frequencyPeriod = Duration.ofDays(3650L);
        final Builder builder = RuleSet.newBuilder(LIMITER_NAME, frequencyPeriod);
        builder.addRule(Rule.newNonBlockingRule(TEST_PORT, TEST_GROUP2, countLimit, 1000, frequencyPeriod));
        builder.addRule(Rule.newNonBlockingRule(TEST_PORT, TEST_GROUP1, null, 2, frequencyPeriod));
        builder.addRule(Rule.newNonBlockingRule(TEST_PORT, OTHER_USER, 1, 1, frequencyPeriod));
        builder.addRule(Rule.newNonBlockingRule(RulePredicates.ALL_PORTS, OTHER_GROUP, 1, 1, frequencyPeriod));
        builder.addRule(Rule.newBlockingRule(RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS));

        testParallelThreads(threadCount, builder.build(), true);
    }

    @Test
    public void testGroupConnectionCountLimit_Concurrency()
    {
        for (final Duration duration : Arrays.asList(Duration.ofDays(2L), null))
        {
            for (final Integer frequencyLimit : Arrays.asList(200, 77, null))
            {
                for (final int threadCount : new int[]{7, 11, 21})
                {
                    testGroupConnectionCountLimit_Concurrency(duration, frequencyLimit, threadCount);
                }
            }
        }
    }

    private void testGroupConnectionCountLimit_Concurrency(Duration frequencyPeriod, Integer frequencyLimit, int threadCount)
    {
        if (frequencyLimit != null)
        {
            if (frequencyLimit < 3)
            {
                frequencyLimit = 3;
            }
            if (frequencyLimit < threadCount)
            {
                frequencyLimit = threadCount;
            }
        }
        final Builder builder = RuleSet.newBuilder(LIMITER_NAME, frequencyPeriod);
        builder.addRule(Rule.newNonBlockingRule(TEST_PORT, TEST_GROUP2, 1000, frequencyLimit, frequencyPeriod));
        builder.addRule(Rule.newNonBlockingRule(TEST_PORT, TEST_GROUP1, 2, null, frequencyPeriod));
        builder.addRule(Rule.newNonBlockingRule(TEST_PORT, OTHER_USER, 1, 1, frequencyPeriod));
        builder.addRule(Rule.newNonBlockingRule(RulePredicates.ALL_PORTS, OTHER_GROUP, 1, 1, frequencyPeriod));
        builder.addRule(Rule.newBlockingRule(RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS));

        testParallelThreads(threadCount, builder.build(), false);
    }

    @Test
    public void testUserConnectionFrequencyLimit_Concurrency()
    {
        for (final Integer countLimit : Arrays.asList(200, 77, null))
        {
            for (int threadCount : new int[]{7, 12, 27})
            {
                testUserConnectionFrequencyLimit_Concurrency(countLimit, threadCount);
            }
        }
    }

    private void testUserConnectionFrequencyLimit_Concurrency(Integer countLimit, int threadCount)
    {
        if (countLimit != null)
        {
            if (countLimit < 3)
            {
                countLimit = 3;
            }
            if (countLimit < threadCount)
            {
                countLimit = threadCount;
            }
        }
        final Duration frequencyPeriod = Duration.ofDays(3650L);
        final Builder builder = RuleSet.newBuilder(LIMITER_NAME, frequencyPeriod);
        builder.addRule(Rule.newNonBlockingRule(TEST_PORT, TEST_USER, countLimit, 2, frequencyPeriod));
        builder.addRule(Rule.newNonBlockingRule(TEST_PORT, OTHER_USER, 1, 1, frequencyPeriod));
        builder.addRule(Rule.newNonBlockingRule(RulePredicates.ALL_PORTS, OTHER_GROUP, 1, 1, frequencyPeriod));
        builder.addRule(Rule.newBlockingRule(RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS));

        testParallelThreads(threadCount, builder.build(), true);
    }

    @Test
    public void testUserConnectionCountLimit_Concurrency()
    {
        for (final Duration duration : Arrays.asList(Duration.ofMinutes(10L), Duration.ofHours(1L), null))
        {
            for (final Integer connectionFrequency : Arrays.asList(200, 787, null))
            {
                for (final int threadCount : new int[]{5, 10, 20})
                {
                    testUserConnectionCountLimit_Concurrency(duration, connectionFrequency, threadCount);
                }
            }
        }
    }

    private void testUserConnectionCountLimit_Concurrency(Duration duration, Integer connectionFrequency, int threadCount)
    {
        if (connectionFrequency != null)
        {
            if (connectionFrequency < 3)
            {
                connectionFrequency = 3;
            }
            if (connectionFrequency < threadCount)
            {
                connectionFrequency = threadCount;
            }
        }

        final Builder builder = RuleSet.newBuilder(LIMITER_NAME, duration);
        builder.addRule(Rule.newNonBlockingRule(RulePredicates.ALL_PORTS, TEST_USER, 2, null, duration));
        builder.addRule(Rule.newNonBlockingRule(TEST_PORT, TEST_USER, 2, connectionFrequency, duration));
        builder.addRule(Rule.newNonBlockingRule(TEST_PORT, OTHER_USER, 1, 1, duration));
        builder.addRule(Rule.newNonBlockingRule(RulePredicates.ALL_PORTS, OTHER_GROUP, 1, 1, duration));
        builder.addRule(Rule.newBlockingRule(RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS));

        testParallelThreads(threadCount, builder.build(), false);
    }

    private void testParallelThreads(int threadCount, RuleSet ruleSet, boolean frequencyTest)
    {
        assertNotNull(ruleSet);

        final AtomicReference<ConnectionSlot> connection1 = new AtomicReference<>();
        final AtomicReference<ConnectionLimitException> exception1 = new AtomicReference<>();

        final Thread thread1 = new Thread(() ->
        {
            try
            {
                connection1.set(ruleSet.register(newConnection()));
            }
            catch (ConnectionLimitException e)
            {
                exception1.set(e);
            }
        });

        final AtomicReference<ConnectionSlot> connection2 = new AtomicReference<>();
        final AtomicReference<ConnectionLimitException> exception2 = new AtomicReference<>();
        final Thread thread2 = new Thread(() ->
        {
            try
            {
                connection2.set(ruleSet.register(newConnection()));
            }
            catch (ConnectionLimitException e)
            {
                exception2.set(e);
            }
        });
        try
        {
            thread1.start();
            thread2.start();
            thread1.join(300000L);
            thread2.join(300000L);
        }
        catch (InterruptedException e)
        {
            thread1.interrupt();
            thread2.interrupt();
            return;
        }
        assertNotNull(connection1.get());
        assertNull(exception1.get());
        assertNotNull(connection2.get());
        assertNull(exception2.get());

        int positive = runRegistration(ruleSet, threadCount);
        if (positive < 0)
        {
            return;
        }
        assertEquals(0, positive);

        final Thread deThread1 = new Thread(() -> connection1.get().free());
        final Thread deThread2 = new Thread(() -> connection2.get().free());
        try
        {
            deThread1.start();
            deThread2.start();
            deThread1.join(300000L);
            deThread2.join(300000L);
        }
        catch (InterruptedException e)
        {
            deThread1.interrupt();
            deThread2.interrupt();
            return;
        }

        positive = runRegistration(ruleSet, threadCount);
        if (positive < 0)
        {
            return;
        }
        if (frequencyTest)
        {
            assertEquals(0, positive);
        }
        else
        {
            assertEquals(2, positive);
        }
    }

    private int runRegistration(RuleSet ruleSet, int threadCount)
    {
        final AtomicInteger positive = new AtomicInteger(threadCount);
        final Thread[] threads = new Thread[threadCount];

        for (int i = 0; i < threads.length; i++)
        {
            threads[i] = new Thread(() ->
            {
                try
                {
                    ruleSet.register(newConnection());
                }
                catch (ConnectionLimitException e)
                {
                    positive.decrementAndGet();
                }
            });
        }

        try
        {
            Arrays.stream(threads).forEach(Thread::start);
            for (final Thread thread : threads)
            {
                thread.join(300000L);
            }
        }
        catch (InterruptedException e)
        {
            Arrays.stream(threads).forEach(Thread::interrupt);
            return -1;
        }
        return positive.get();
    }

    @Test
    public void testUserConnectionCountLimit()
    {
        for (final Duration duration : Arrays.asList(Duration.ofMinutes(11L), Duration.ofDays(1L), null))
        {
            for (final Integer frequencyLimit : Arrays.asList(211, null, 45))
            {
                testUserConnectionCountLimit(duration, frequencyLimit);
            }
        }
    }

    private void testUserConnectionCountLimit(Duration duration, Integer frequencyLimit)
    {
        if (frequencyLimit != null && frequencyLimit < 3)
        {
            frequencyLimit = 3;
        }
        final Builder builder = RuleSet.newBuilder(LIMITER_NAME, duration);
        builder.addRule(Rule.newNonBlockingRule(
                RulePredicates.ALL_PORTS, TEST_USER, 2, null, duration));
        builder.addRule(Rule.newNonBlockingRule(TEST_PORT, TEST_USER, 2, frequencyLimit, duration));
        builder.addRule(Rule.newNonBlockingRule(TEST_PORT, OTHER_USER, 1, 1, duration));
        builder.addRule(Rule.newNonBlockingRule(RulePredicates.ALL_PORTS, OTHER_GROUP, 1, 1, duration));
        builder.addRule(Rule.newBlockingRule(RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS));

        testConnectionCountLimit2(builder.build());
        testConnectionCountLimit2(builder.logAllMessages(true).build());
        testConnectionCountLimit2(builder.logAllMessages(false).build());
    }

    @Test
    public void testGroupConnectionCountLimit()
    {
        for (final Duration duration : Arrays.asList(Duration.ofMinutes(11L), Duration.ofDays(2L), null))
        {
            for (final Integer frequencyLimit : Arrays.asList(217, null, 47))
            {
                testGroupConnectionCountLimit(duration, frequencyLimit);
            }
        }
    }

    private void testGroupConnectionCountLimit(Duration duration, Integer frequencyLimit)
    {
        if (frequencyLimit != null && frequencyLimit < 3)
        {
            frequencyLimit = 3;
        }
        final Builder builder = RuleSet.newBuilder(LIMITER_NAME, duration);
        builder.addRule(Rule.newNonBlockingRule(TEST_PORT, TEST_GROUP2, 1000, frequencyLimit, duration));
        builder.addRule(Rule.newNonBlockingRule(TEST_PORT, TEST_GROUP1, 2, null, duration));
        builder.addRule(Rule.newNonBlockingRule(TEST_PORT, OTHER_USER, 1, 1, duration));
        builder.addRule(Rule.newNonBlockingRule(RulePredicates.ALL_PORTS, OTHER_GROUP, 1, 1, duration));
        builder.addRule(Rule.newBlockingRule(RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS));

        testConnectionCountLimit2(builder.build());
        testConnectionCountLimit2(builder.logAllMessages(true).build());
        testConnectionCountLimit2(builder.logAllMessages(false).build());
    }

    @Test
    public void testDefaultConnectionCountLimit()
    {
        for (final Duration duration : Arrays.asList(Duration.ofMinutes(11L), Duration.ofDays(12L), null))
        {
            for (final Integer frequencyLimit : Arrays.asList(117, null, 147))
            {
                testDefaultConnectionCountLimit(duration, frequencyLimit);
            }
        }
    }

    private void testDefaultConnectionCountLimit(Duration duration, Integer frequencyLimit)
    {
        if (frequencyLimit != null && frequencyLimit < 3)
        {
            frequencyLimit = 3;
        }
        final Builder builder = RuleSet.newBuilder(LIMITER_NAME, duration);
        builder.addRule(Rule.newNonBlockingRule(RulePredicates.ALL_PORTS, TEST_USER, 2, frequencyLimit, duration));
        builder.addRule(Rule.newNonBlockingRule(TEST_PORT, OTHER_USER, 1, 1, duration));
        builder.addRule(Rule.newNonBlockingRule(RulePredicates.ALL_PORTS, OTHER_GROUP, 1, 1, duration));
        builder.addRule(Rule.newNonBlockingRule(RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS, 2, null, duration));

        testConnectionCountLimit2(builder.build());
        testConnectionCountLimit2(builder.logAllMessages(true).build());
        testConnectionCountLimit2(builder.logAllMessages(false).build());
    }

    private void testConnectionCountLimit2(RuleSet ruleSet)
    {
        assertNotNull(ruleSet);

        ConnectionSlot connection1 = null;
        ConnectionSlot connection2 = null;
        try
        {
            connection1 = ruleSet.register(newConnection());
            connection2 = ruleSet.register(newConnection());
        }
        catch (ConnectionLimitException e)
        {
            fail("No exception is expected: " + e.getMessage());
        }
        assertNotNull(connection1);
        assertNotNull(connection2);

        try
        {
            ruleSet.register(newConnection());
            fail("An exception is expected");
        }
        catch (ConnectionLimitException e)
        {
            assertEquals("User user breaks connection count limit 2 on port amqp", e.getMessage());
        }

        connection1.free();
        ConnectionSlot connection3 = null;
        try
        {
            connection3 = ruleSet.register(newConnection());
        }
        catch (ConnectionLimitException e)
        {
            fail("No exception is expected: " + e.getMessage());
        }
        assertNotNull(connection3);

        connection2.free();
        connection3.free();
    }

    @Test
    public void testBlockedUser()
    {
        for (final Duration duration : Arrays.asList(Duration.ofMinutes(11L), Duration.ofDays(1L), null))
        {
            testBlockedUser(duration);
        }
    }

    private void testBlockedUser(Duration duration)
    {
        final Builder builder = RuleSet.newBuilder(LIMITER_NAME, duration);
        builder.addRule(Rule.newBlockingRule(TEST_PORT, TEST_USER));
        builder.addRule(Rule.newNonBlockingRule(TEST_PORT, OTHER_USER, 1000, 1000, duration));
        builder.addRule(Rule.newNonBlockingRule(RulePredicates.ALL_PORTS, OTHER_GROUP, 1000, 1000, duration));
        builder.addRule(Rule.newNonBlockingRule(RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS, 1000, 1000, duration));

        testBlocked(builder.build());
        testBlocked(builder.logAllMessages(true).build());
        testBlocked(builder.logAllMessages(false).build());
    }

    @Test
    public void testBlockedGroup()
    {
        for (final Duration duration : Arrays.asList(Duration.ofMinutes(11L), Duration.ofDays(1L), null))
        {
            testBlockedGroup(duration);
        }
    }

    private void testBlockedGroup(Duration duration)
    {
        final Builder builder = RuleSet.newBuilder(LIMITER_NAME, duration);
        builder.addRule(Rule.newNonBlockingRule(TEST_PORT, TEST_GROUP1, 10000, null, duration));
        builder.addRule(Rule.newBlockingRule(TEST_PORT, TEST_GROUP2));
        builder.addRule(Rule.newNonBlockingRule(TEST_PORT, OTHER_USER, 1000, 1000, duration));
        builder.addRule(Rule.newNonBlockingRule(RulePredicates.ALL_PORTS, OTHER_GROUP, 1000, 1000, duration));
        builder.addRule(Rule.newNonBlockingRule(RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS, 10000, 10000, duration));

        testBlocked(builder.build());
        testBlocked(builder.logAllMessages(true).build());
        testBlocked(builder.logAllMessages(false).build());
    }

    @Test
    public void testBlockedByDefault()
    {
        for (final Duration duration : Arrays.asList(Duration.ofMinutes(11L), Duration.ofDays(1L), null))
        {
            testBlockedByDefault(duration);
        }
    }

    private void testBlockedByDefault(Duration duration)
    {
        final Builder builder = RuleSet.newBuilder(LIMITER_NAME, duration);
        builder.addRule(Rule.newBlockingRule(TEST_PORT, RulePredicates.ALL_USERS));
        builder.addRule(Rule.newNonBlockingRule(RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS, 10000, 10000, duration));
        builder.addRule(Rule.newNonBlockingRule(TEST_PORT, OTHER_USER, 1000, 1000, duration));
        builder.addRule(Rule.newNonBlockingRule(RulePredicates.ALL_PORTS, OTHER_GROUP, 1000, 1000, duration));

        testBlocked(builder.build());
        testBlocked(builder.logAllMessages(true).build());
        testBlocked(builder.logAllMessages(false).build());
    }

    private void testBlocked(RuleSet ruleSet)
    {
        assertNotNull(ruleSet);

        ConnectionSlot connection = null;
        try
        {
            connection = ruleSet.register(newConnection());
            fail("An exception is expected");
        }
        catch (ConnectionLimitException e)
        {
            assertEquals("User user is blocked on port amqp", e.getMessage());
        }
        assertNull(connection);
    }

    @Test
    public void testUserConnectionFrequencyLimit()
    {
        for (final Integer countLimit : Arrays.asList(300, 200, null))
        {
            testUserConnectionFrequencyLimit(countLimit);
        }
    }

    private void testUserConnectionFrequencyLimit(Integer countLimit)
    {
        if (countLimit != null && countLimit < 3)
        {
            countLimit = 3;
        }
        final Duration frequencyPeriod = Duration.ofDays(3650L);
        final Builder builder = RuleSet.newBuilder(LIMITER_NAME, frequencyPeriod);
        builder.addRule(Rule.newNonBlockingRule(TEST_PORT, TEST_USER, countLimit, 2, frequencyPeriod));
        builder.addRule(Rule.newNonBlockingRule(TEST_PORT, OTHER_USER, 1, 1, frequencyPeriod));
        builder.addRule(Rule.newNonBlockingRule(RulePredicates.ALL_PORTS, OTHER_GROUP, 1, 1, frequencyPeriod));
        builder.addRule(Rule.newBlockingRule(RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS));

        testConnectionFrequencyLimit2(builder.build());
        testConnectionFrequencyLimit2(builder.logAllMessages(true).build());
        testConnectionFrequencyLimit2(builder.logAllMessages(false).build());
    }

    @Test
    public void testGroupConnectionFrequencyLimit()
    {
        for (final Integer countLimit : Arrays.asList(300, 200, null))
        {
            testGroupConnectionFrequencyLimit(countLimit);
        }
    }

    private void testGroupConnectionFrequencyLimit(Integer countLimit)
    {
        if (countLimit != null && countLimit < 3)
        {
            countLimit = 3;
        }
        final Duration frequencyPeriod = Duration.ofDays(3650L);
        final Builder builder = RuleSet.newBuilder(LIMITER_NAME, frequencyPeriod);
        builder.addRule(Rule.newNonBlockingRule(TEST_PORT, TEST_GROUP2, countLimit, 1000, frequencyPeriod));
        builder.addRule(Rule.newNonBlockingRule(TEST_PORT, TEST_GROUP1, null, 2, frequencyPeriod));
        builder.addRule(Rule.newNonBlockingRule(TEST_PORT, OTHER_USER, 1, 1, frequencyPeriod));
        builder.addRule(Rule.newNonBlockingRule(RulePredicates.ALL_PORTS, OTHER_GROUP, 1, 1, frequencyPeriod));
        builder.addRule(Rule.newBlockingRule(RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS));

        final RuleSet ruleSet = builder.build();
        testConnectionFrequencyLimit2(ruleSet);
    }

    @Test
    public void testDefaultConnectionFrequencyLimit()
    {
        for (final Integer countLimit : Arrays.asList(300, 200, null))
        {
            testDefaultConnectionFrequencyLimit(countLimit);
        }
    }

    private void testDefaultConnectionFrequencyLimit(Integer countLimit)
    {
        if (countLimit != null && countLimit < 3)
        {
            countLimit = 3;
        }
        final Duration frequencyPeriod = Duration.ofDays(3650L);
        final Builder builder = RuleSet.newBuilder(LIMITER_NAME, frequencyPeriod);
        builder.addRule(Rule.newNonBlockingRule(TEST_PORT, RulePredicates.ALL_USERS, null, 2, frequencyPeriod));
        builder.addRule(Rule.newNonBlockingRule(RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS, countLimit, 2, frequencyPeriod));
        builder.addRule(Rule.newNonBlockingRule(TEST_PORT, OTHER_USER, 1, 1, frequencyPeriod));
        builder.addRule(Rule.newNonBlockingRule(RulePredicates.ALL_PORTS, OTHER_GROUP, 1, 1, frequencyPeriod));

        final RuleSet ruleSet = builder.build();
        testConnectionFrequencyLimit2(ruleSet);
    }

    private void testConnectionFrequencyLimit2(RuleSet ruleSet)
    {
        assertNotNull(ruleSet);

        ConnectionSlot connection1 = null;
        ConnectionSlot connection2 = null;

        try
        {
            connection1 = ruleSet.register(newConnection());
            connection2 = ruleSet.register(newConnection());
        }
        catch (ConnectionLimitException e)
        {
            fail("An exception is not expected");
        }
        assertNotNull(connection1);
        assertNotNull(connection2);

        try
        {
            ruleSet.register(newConnection());
            fail("An exception is expected here");
        }
        catch (ConnectionLimitException e)
        {
            assertTrue(Pattern.matches("User user breaks connection frequency limit 2 per \\d+ s on port amqp", e.getMessage()));
        }

        connection1.free();
        connection2.free();

        try
        {
            ruleSet.register(newConnection());
            fail("An exception is expected here");
        }
        catch (ConnectionLimitException e)
        {
            assertTrue(Pattern.matches("User user breaks connection frequency limit 2 per \\d+ s on port amqp", e.getMessage()));
        }
    }

    @Test
    public void testNoLimits()
    {
        for (final Duration duration : Arrays.asList(null, Duration.ofNanos(1L), Duration.ofMillis(1L), Duration.ofMinutes(1L), Duration.ofDays(1L)))
        {
            testNoLimits(duration);
        }
    }

    private void testNoLimits(Duration duration)
    {
        final Builder builder = RuleSet.newBuilder(LIMITER_NAME, duration);
        builder.addRule(Rule.newBlockingRule(TEST_PORT, OTHER_USER));
        builder.addRule(Rule.newBlockingRule(RulePredicates.ALL_PORTS, OTHER_GROUP));

        final RuleSet ruleSet = builder.build();
        assertNotNull(ruleSet);

        ConnectionSlot connection1 = null;
        ConnectionSlot connection2 = null;
        ConnectionSlot connection3 = null;

        try
        {
            connection1 = ruleSet.register(newConnection());
            connection2 = ruleSet.register(newConnection());
            connection3 = ruleSet.register(newConnection());
        }
        catch (ConnectionLimitException e)
        {
            fail("An exception is not expected here");
        }
        assertNotNull(connection1);
        assertNotNull(connection2);
        assertNotNull(connection3);

        connection1.free();
        connection2.free();
        connection3.free();
    }

    @Test
    public void testRegisterNullUser()
    {
        for (final Duration duration : Arrays.asList(null, Duration.ofMinutes(1L), Duration.ofDays(1L)))
        {
            testRegisterNullUser(duration);
        }
    }

    private void testRegisterNullUser(Duration duration)
    {
        final Builder builder = RuleSet.newBuilder(LIMITER_NAME, duration);
        builder.addRule(Rule.newBlockingRule(RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS));

        final RuleSet ruleSet = builder.build();
        assertNotNull(ruleSet);

        final AMQPConnection<?> connection = Mockito.mock(AMQPConnection.class);
        Mockito.doReturn(_port).when(connection).getPort();
        Mockito.doReturn(_subject).when(connection).getSubject();
        Mockito.doReturn(_eventLogger).when(connection).getEventLogger();

        try
        {
            ruleSet.register(connection);
            fail("An exception is expected");
        }
        catch (ConnectionLimitException e)
        {
            assertEquals("Unauthorized connection is forbidden", e.getMessage());
        }
    }

    @Test
    public void testRegisterNullSubject()
    {
        for (final Duration duration : Arrays.asList(null, Duration.ofMinutes(1L), Duration.ofDays(1L)))
        {
            testRegisterNullSubject(duration);
        }
    }

    private void testRegisterNullSubject(Duration duration)
    {
        final Builder builder = RuleSet.newBuilder(LIMITER_NAME, duration);
        builder.addRule(Rule.newNonBlockingRule(RulePredicates.ALL_PORTS, TEST_GROUP1, 1000, 1000, duration));
        builder.addRule(Rule.newNonBlockingRule(RulePredicates.ALL_PORTS, TEST_GROUP2, 1000, 1000, duration));
        builder.addRule(Rule.newBlockingRule(RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS));

        final RuleSet ruleSet = builder.build();
        assertNotNull(ruleSet);

        final AMQPConnection<?> connection = Mockito.mock(AMQPConnection.class);
        Mockito.doReturn(_port).when(connection).getPort();
        Mockito.doReturn(_principal).when(connection).getAuthorizedPrincipal();
        Mockito.doReturn(_eventLogger).when(connection).getEventLogger();

        try
        {
            ruleSet.register(connection);
            fail("An exception is expected");
        }
        catch (ConnectionLimitException e)
        {
            assertEquals("User user is blocked on port amqp", e.getMessage());
        }
    }

    private AMQPConnection<?> newConnection()
    {
        final AMQPConnection<?> connection = Mockito.mock(AMQPConnection.class);
        Mockito.doReturn(_port).when(connection).getPort();
        Mockito.doReturn(_subject).when(connection).getSubject();
        Mockito.doReturn(_principal).when(connection).getAuthorizedPrincipal();
        Mockito.doReturn(_eventLogger).when(connection).getEventLogger();
        return connection;
    }

    @Test
    public void testBuilder_AddNull()
    {
        final Builder builder = RuleSet.newBuilder(LIMITER_NAME, Duration.ofMillis(2L));
        builder.addRule(null);
        builder.addRules(null);

        final RuleSet ruleSet = builder.build();

        assertNotNull(ruleSet);

        final ConnectionSlot connection1 = ruleSet.register(newConnection());
        assertNotNull(connection1);

        final ConnectionSlot connection2 = ruleSet.register(newConnection());
        assertNotNull(connection2);

        connection1.free();
        connection2.free();
    }

    @Test
    public void testAppend_CountLimit()
    {
        for (final Duration duration : Arrays.asList(Duration.ofMinutes(11L), Duration.ofDays(1L), null))
        {
            for (final Integer frequencyLimit : Arrays.asList(211, null, 45))
            {
                testAppend_CountLimit(duration, frequencyLimit);
            }
        }
    }

    private void testAppend_CountLimit(Duration duration, Integer frequencyLimit)
    {
        if (frequencyLimit != null && frequencyLimit < 3)
        {
            frequencyLimit = 3;
        }
        final Builder builder = RuleSet.newBuilder(LIMITER_NAME, duration);
        builder.addRule(Rule.newNonBlockingRule(RulePredicates.ALL_PORTS, TEST_USER, 200, 20000, duration));
        builder.addRule(Rule.newNonBlockingRule(TEST_PORT, OTHER_USER, 1, 1, duration));
        builder.addRule(Rule.newBlockingRule(RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS));

        final Builder secondaryBuilder = RuleSet.newBuilder(LIMITER_NAME, duration);
        secondaryBuilder.addRule(Rule.newNonBlockingRule(TEST_PORT, TEST_USER, 2, frequencyLimit, duration));
        secondaryBuilder.addRule(Rule.newBlockingRule(RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS));

        testConnectionCountLimit2((RuleSet) builder.build().append(secondaryBuilder.build()));
    }

    @Test
    public void testAppend_FrequencyLimit()
    {
        for (final Integer countLimit : Arrays.asList(300, 200, null))
        {
            testAppend_FrequencyLimit(countLimit);
        }
    }

    private void testAppend_FrequencyLimit(Integer countLimit)
    {
        if (countLimit != null && countLimit < 3)
        {
            countLimit = 3;
        }

        final Duration frequencyPeriod = Duration.ofDays(3650L);
        final Builder builder = RuleSet.newBuilder(LIMITER_NAME, frequencyPeriod);
        builder.addRule(Rule.newNonBlockingRule(RulePredicates.ALL_PORTS, TEST_USER, 200, 20000, frequencyPeriod));
        builder.addRule(Rule.newNonBlockingRule(TEST_PORT, OTHER_USER, 1, 1, frequencyPeriod));
        builder.addRule(Rule.newBlockingRule(RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS));

        final Builder secondaryBuilder = RuleSet.newBuilder(LIMITER_NAME, frequencyPeriod);
        secondaryBuilder.addRule(Rule.newNonBlockingRule(TEST_PORT, TEST_USER, countLimit, 2, frequencyPeriod));
        secondaryBuilder.addRule(Rule.newBlockingRule(RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS));

        testConnectionFrequencyLimit2((RuleSet) builder.build().append(secondaryBuilder.build()));
    }

    @Test
    public void testAppend_BlockedUser()
    {
        for (final Duration duration : Arrays.asList(Duration.ofMinutes(11L), Duration.ofDays(1L), null))
        {
            testAppend_BlockedUser(duration);
        }
    }

    private void testAppend_BlockedUser(Duration duration)
    {
        final Builder builder = RuleSet.newBuilder(LIMITER_NAME, duration);
        builder.addRule(Rule.newNonBlockingRule(TEST_PORT, TEST_USER, 1000, 1000, duration));

        final Builder secondaryBuilder = RuleSet.newBuilder(LIMITER_NAME, Duration.ofDays(1L));
        secondaryBuilder.addRule(Rule.newBlockingRule(RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS));

        testBlocked((RuleSet) builder.build().append(secondaryBuilder.build()));
    }

    @Test
    public void testName()
    {
        final RuleSet ruleSet = RuleSet.newBuilder(LIMITER_NAME, Duration.ofMinutes(1L)).build();
        assertEquals(LIMITER_NAME, ruleSet.toString());
    }

    @Test
    public void testInvalidRuleWithoutPeriod()
    {
        RuleSet.Builder builder = RuleSet.newBuilder(LIMITER_NAME, Duration.ofMinutes(1L));
        final NonBlockingRule rule = Rule.newNonBlockingRule(
                RulePredicates.ALL_PORTS, RulePredicates.ALL_USERS, 2, 2, null);
        try
        {
            builder.addRule(rule);
            fail("An exception is expected, the rule is not valid.");
        }
        catch (IllegalArgumentException e)
        {
            assertNotNull(e.getMessage());
        }

        try
        {
            builder.addRules(Collections.singletonList(rule));
            fail("An exception is expected, the rule is not valid.");
        }
        catch (IllegalArgumentException e)
        {
            assertNotNull(e.getMessage());
        }
    }
}