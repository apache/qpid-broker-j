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
package org.apache.qpid.server.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.qpid.server.transport.ServerNetworkConnection;
import org.apache.qpid.test.utils.UnitTestBase;

class ConnectionClosingTickerTest extends UnitTestBase
{
    private final AtomicLong _nanoTime = new AtomicLong();
    private final ServerNetworkConnection _network = mock(ServerNetworkConnection.class);

    @BeforeEach
    void setUp()
    {
        _nanoTime.set(0);
        reset(_network);
    }

    @Test
    void expiresAtDeadlineAndRoundsUpUntilThen()
    {
        final ConnectionClosingTicker ticker = new ConnectionClosingTicker(1000, _network, _nanoTime::get);
        assertEquals(1000, ticker.tick(0));
        _nanoTime.set(TimeUnit.MILLISECONDS.toNanos(999) + 1);
        assertEquals(1, ticker.tick(0));
        verify(_network, never()).abort();

        _nanoTime.set(TimeUnit.MILLISECONDS.toNanos(1000));
        assertEquals(0, ticker.tick(0));
        verify(_network).abort();
        verify(_network, never()).close();
    }

    @Test
    void wallClockChangesAndWorkerSchedulingDoNotExtendDeadline()
    {
        when(_network.getScheduledTime()).thenReturn(1L);
        final ConnectionClosingTicker ticker = new ConnectionClosingTicker(1000, _network, _nanoTime::get);
        _nanoTime.set(TimeUnit.MILLISECONDS.toNanos(500));
        assertEquals(500, ticker.getTimeToNextTick(Long.MIN_VALUE));
        assertEquals(500, ticker.getTimeToNextTick(Long.MAX_VALUE));
        _nanoTime.set(TimeUnit.MILLISECONDS.toNanos(1000));
        assertEquals(0, ticker.tick(0));
        verify(_network).abort();
    }

    @Test
    void nanoTimeWrapDoesNotExtendDeadline()
    {
        final long start = Long.MAX_VALUE - TimeUnit.MILLISECONDS.toNanos(500);
        _nanoTime.set(start);
        final ConnectionClosingTicker ticker = new ConnectionClosingTicker(1000, _network, _nanoTime::get);
        _nanoTime.set(start + TimeUnit.MILLISECONDS.toNanos(750));
        assertEquals(250, ticker.tick(0));
        _nanoTime.set(start + TimeUnit.MILLISECONDS.toNanos(1000));
        assertEquals(0, ticker.tick(0));
        verify(_network).abort();
    }

    @ParameterizedTest
    @ValueSource(longs = {0, -1, Long.MIN_VALUE})
    void nonPositiveTimeoutExpiresImmediately(final long timeout)
    {
        final ConnectionClosingTicker ticker = new ConnectionClosingTicker(timeout, _network, _nanoTime::get);
        assertEquals(0, ticker.tick(0));
        verify(_network).abort();
    }

    @ParameterizedTest
    @ValueSource(longs = {Integer.MAX_VALUE, (long) Integer.MAX_VALUE + 1, Long.MAX_VALUE})
    void largeTimeoutDoesNotOverflowToImmediateExpiry(final long timeout)
    {
        final ConnectionClosingTicker ticker = new ConnectionClosingTicker(timeout, _network, _nanoTime::get);
        assertEquals(Integer.MAX_VALUE, ticker.tick(0));
        verify(_network, never()).abort();
    }
}
