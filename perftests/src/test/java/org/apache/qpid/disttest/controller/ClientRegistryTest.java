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
package org.apache.qpid.disttest.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.disttest.DistributedTestException;
import org.apache.qpid.test.utils.UnitTestBase;

public class ClientRegistryTest extends UnitTestBase
{
    private static final String CLIENT1_REGISTERED_NAME = "CLIENT1_REGISTERED_NAME";
    private static final String CLIENT2_REGISTERED_NAME = "CLIENT2_REGISTERED_NAME";
    private static final String CLIENT3_REGISTERED_NAME = "CLIENT3_REGISTERED_NAME";

    private static final long IDLE_TIMEOUT = 600;
    private static final long REAL_IDLE_TIMEOUT = 1000;
    private static final long REAL_TIMEOUT_TOLERANCE_MILLIS = 10;
    private static final long TEST_TIMEOUT_SECONDS = 10;

    private Queue<TimedEvent> _events;
    private List<Long> _requestedWaits;
    private long _currentTimeMillis;
    private ClientRegistry _clientRegistry;

    @BeforeEach
    public void setUp()
    {
        _currentTimeMillis = 0;
        _events = new PriorityQueue<>(Comparator.comparingLong(TimedEvent::timeMillis));
        _requestedWaits = new ArrayList<>();
        _clientRegistry = new ClientRegistry(this::waitForNextEvent)
        {
            @Override
            long currentTimeMillis()
            {
                return _currentTimeMillis;
            }
        };
    }

    @Test
    public void testRegisterClient()
    {
        assertEquals(0, _clientRegistry.getClients().size());

        _clientRegistry.registerClient(CLIENT1_REGISTERED_NAME);
        assertEquals(1, _clientRegistry.getClients().size());
    }

    @Test
    public void testRejectsDuplicateClientNames()
    {
        _clientRegistry.registerClient(CLIENT1_REGISTERED_NAME);
        assertThrows(DistributedTestException.class, () ->
                _clientRegistry.registerClient(CLIENT1_REGISTERED_NAME));
    }

    @Test
    public void testAwaitTimeoutWhenNoClientsRegister()
    {
        assertEquals(1, _clientRegistry.awaitClients(1, IDLE_TIMEOUT));
        assertEquals(List.of(IDLE_TIMEOUT), _requestedWaits);
        assertEquals(IDLE_TIMEOUT, _currentTimeMillis);
    }

    @Test
    public void testAwaitOneClientWhenClientNotRegistered() throws Exception
    {
        final ClientRegistry clientRegistry = new ClientRegistry();
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try
        {
            final Future<Integer> result = executor.submit(() ->
            {
                final long start = System.nanoTime();
                final int numberOfClientsAbsent = clientRegistry.awaitClients(1, REAL_IDLE_TIMEOUT);
                final long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                assertTrue(elapsed >= REAL_IDLE_TIMEOUT - REAL_TIMEOUT_TOLERANCE_MILLIS,
                           "Registry returned before the timeout: " + elapsed);
                return numberOfClientsAbsent;
            });
            assertEquals(1, result.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS).intValue());
        }
        finally
        {
            shutdownExecutor(executor);
        }
    }

    @Test
    public void testAwaitOneClientWhenClientAlreadyRegistered()
    {
        _clientRegistry.registerClient(CLIENT1_REGISTERED_NAME);

        final int numberOfClientsAbsent = _clientRegistry.awaitClients(1, IDLE_TIMEOUT);
        assertEquals(0, numberOfClientsAbsent);
        assertTrue(_requestedWaits.isEmpty(), "Already registered clients must not wait");
    }

    @Test
    public void testAwaitTwoClientsWhenClientRegistersWhilstWaiting() throws Exception
    {
        final CountDownLatch started = new CountDownLatch(1);
        final AtomicReference<Thread> waiter = new AtomicReference<>();
        final ClientRegistry clientRegistry = new ClientRegistry();
        clientRegistry.registerClient(CLIENT1_REGISTERED_NAME);

        final ExecutorService executor = Executors.newFixedThreadPool(2);
        try
        {
            final Future<Integer> result = executor.submit(() ->
            {
                waiter.set(Thread.currentThread());
                started.countDown();
                return clientRegistry.awaitClients(2, TimeUnit.MINUTES.toMillis(1));
            });
            assertTrue(started.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS), "Registry waiter did not start");
            awaitTimedWait(waiter.get());
            final Future<?> registration = executor.submit(() ->
                    clientRegistry.registerClient(CLIENT2_REGISTERED_NAME));
            registration.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            assertEquals(0, result.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS).intValue());
        }
        finally
        {
            shutdownExecutor(executor);
        }
    }

    @Test
    public void testAwaitTimeoutForPromptRegistrations()
    {
        registerClientAt(CLIENT1_REGISTERED_NAME, 300);
        _events.add(new TimedEvent(700, () ->
        {
            // Wake without a registration.
        }));
        registerClientAt(CLIENT2_REGISTERED_NAME, 800);
        _events.add(new TimedEvent(1000, () ->
        {
            // Force a deadline check after the second registration.
        }));
        registerClientAt(CLIENT3_REGISTERED_NAME, 1300);

        assertEquals(0, _clientRegistry.awaitClients(3, IDLE_TIMEOUT));
        assertEquals(1300, _currentTimeMillis, "Each registration must reset the idle timeout");
    }

    @Test
    public void testAwaitTimeoutForWhenThirdRegistrationIsLate()
    {
        registerClientAt(CLIENT1_REGISTERED_NAME, 300);
        registerClientAt(CLIENT2_REGISTERED_NAME, 400);
        registerClientAt(CLIENT3_REGISTERED_NAME, 1500);

        assertEquals(1, _clientRegistry.awaitClients(3, IDLE_TIMEOUT));
        assertEquals(1000, _currentTimeMillis);
        assertEquals(2, _clientRegistry.getClients().size());
    }

    @Test
    public void testAwaitTimeoutWhenSecondAndThirdRegistrationsAreLate()
    {
        registerClientAt(CLIENT1_REGISTERED_NAME, 300);
        registerClientAt(CLIENT2_REGISTERED_NAME, 1500);
        registerClientAt(CLIENT3_REGISTERED_NAME, 1500);

        assertEquals(2, _clientRegistry.awaitClients(3, IDLE_TIMEOUT));
        assertEquals(900, _currentTimeMillis);
        assertEquals(1, _clientRegistry.getClients().size());
    }

    @Test
    public void testAwaitTimeoutAfterSpuriousWakeups()
    {
        _events.add(new TimedEvent(300, () ->
        {
            // Wake without a registration.
        }));
        _events.add(new TimedEvent(650, () ->
        {
            // Wake without a registration.
        }));
        registerClientAt(CLIENT1_REGISTERED_NAME, 700);

        assertEquals(1, _clientRegistry.awaitClients(1, IDLE_TIMEOUT));
        assertTrue(_currentTimeMillis >= IDLE_TIMEOUT, "An early wakeup must not end the wait");
        assertTrue(_clientRegistry.getClients().isEmpty(), "The late client must remain unregistered");
    }

    private void waitForNextEvent(final Object lock, final long idleTimeout)
    {
        assertTrue(Thread.holdsLock(lock), "Registry must hold the monitor before waiting");
        assertTrue(idleTimeout > 0, "Wait must have a positive timeout");
        assertTrue(_currentTimeMillis < 10 * IDLE_TIMEOUT, "Registry exceeded the simulated time limit");
        _requestedWaits.add(idleTimeout);
        final long expiry = _currentTimeMillis + idleTimeout;
        _currentTimeMillis = _events.isEmpty() ? expiry : Math.min(expiry, _events.peek().timeMillis());
        while (!_events.isEmpty() && _events.peek().timeMillis() <= _currentTimeMillis)
        {
            _events.remove().action().run();
        }
    }

    private void registerClientAt(final String clientName, final long timeMillis)
    {
        _events.add(new TimedEvent(timeMillis, () -> _clientRegistry.registerClient(clientName)));
    }

    private void awaitTimedWait(final Thread waiter) throws InterruptedException
    {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TEST_TIMEOUT_SECONDS);
        while (waiter.getState() != Thread.State.TIMED_WAITING)
        {
            assertTrue(System.nanoTime() < deadline, "Registry did not enter a timed wait");
            TimeUnit.MILLISECONDS.sleep(1);
        }
    }

    private void shutdownExecutor(final ExecutorService executor) throws InterruptedException
    {
        executor.shutdownNow();
        assertTrue(executor.awaitTermination(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS),
                "Registry workers did not terminate");
    }

    private record TimedEvent(long timeMillis, Runnable action)
    {
    }
}
