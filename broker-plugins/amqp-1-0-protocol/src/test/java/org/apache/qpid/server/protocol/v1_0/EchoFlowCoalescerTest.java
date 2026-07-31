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

package org.apache.qpid.server.protocol.v1_0;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

class EchoFlowCoalescerTest extends UnitTestBase
{
    @Test
    void requestEchoSendsImmediatelyWhenNoRecentFlow()
    {
        final EchoFlowTestSupport.FakeClock clock = new EchoFlowTestSupport.FakeClock();
        final EchoFlowTestSupport.FakeScheduler scheduler = new EchoFlowTestSupport.FakeScheduler();
        final AtomicInteger sendCount = new AtomicInteger();
        final EchoFlowCoalescer coalescer = createCoalescer(clock, scheduler, sendCount);

        coalescer.requestEcho();

        assertEquals(1, sendCount.get(), "First echo should be sent immediately");
        assertEquals(0, scheduler.getScheduledTaskCount(), "No delayed wake-up should be scheduled");
    }

    @Test
    void repeatedEchoesWithinIntervalCoalesceToOneDelayedSend()
    {
        final EchoFlowTestSupport.FakeClock clock = new EchoFlowTestSupport.FakeClock();
        final EchoFlowTestSupport.FakeScheduler scheduler = new EchoFlowTestSupport.FakeScheduler();
        final AtomicInteger sendCount = new AtomicInteger();
        final EchoFlowCoalescer coalescer = createCoalescer(clock, scheduler, sendCount);

        coalescer.requestEcho();
        coalescer.requestEcho();
        coalescer.requestEcho();

        assertEquals(1, sendCount.get(), "Only the first echo should be sent immediately");
        assertEquals(1, scheduler.getScheduledTaskCount(), "Further echoes should coalesce to one delayed send");

        clock.advanceMillis(10);
        scheduler.runNext();

        assertEquals(2, sendCount.get(), "The pending echo should be sent once after the interval");
        assertEquals(0, scheduler.getScheduledTaskCount(), "The delayed wake-up should clear after firing");
    }

    @Test
    void negativeClockValueStillCoalescesRepeatedEchoes()
    {
        final EchoFlowTestSupport.FakeClock clock = new EchoFlowTestSupport.FakeClock();
        clock.setTimeNs(-5_000_000L);
        final EchoFlowTestSupport.FakeScheduler scheduler = new EchoFlowTestSupport.FakeScheduler();
        final AtomicInteger sendCount = new AtomicInteger();
        final EchoFlowCoalescer coalescer = createCoalescer(clock, scheduler, sendCount);

        coalescer.requestEcho();

        assertEquals(1, sendCount.get(), "First echo should be sent immediately with a negative clock value");
        assertEquals(0, scheduler.getScheduledTaskCount(), "No delayed wake-up should be scheduled for the first echo");

        coalescer.requestEcho();

        assertEquals(1, sendCount.get(), "Second echo should be coalesced while still inside the interval");
        assertEquals(1, scheduler.getScheduledTaskCount(), "Second echo should schedule one delayed wake-up");

        clock.advanceMillis(10);
        scheduler.runNext();

        assertEquals(2, sendCount.get(), "The pending echo should be sent once after the interval");
        assertEquals(0, scheduler.getScheduledTaskCount(), "The delayed wake-up should clear after firing");
    }

    @Test
    void markFlowSentClearsPendingEcho()
    {
        final EchoFlowTestSupport.FakeClock clock = new EchoFlowTestSupport.FakeClock();
        final EchoFlowTestSupport.FakeScheduler scheduler = new EchoFlowTestSupport.FakeScheduler();
        final AtomicInteger sendCount = new AtomicInteger();
        final EchoFlowCoalescer coalescer = createCoalescer(clock, scheduler, sendCount);

        coalescer.requestEcho();
        coalescer.requestEcho();
        coalescer.markFlowSent();

        assertEquals(1, sendCount.get(), "The pending echo should be satisfied by the real flow send");
        assertEquals(0, scheduler.getScheduledTaskCount(), "Satisfied echoes must cancel the delayed wake-up");
    }

    @Test
    void cancelRemovesPendingWakeUp()
    {
        final EchoFlowTestSupport.FakeClock clock = new EchoFlowTestSupport.FakeClock();
        final EchoFlowTestSupport.FakeScheduler scheduler = new EchoFlowTestSupport.FakeScheduler();
        final AtomicInteger sendCount = new AtomicInteger();
        final EchoFlowCoalescer coalescer = createCoalescer(clock, scheduler, sendCount);

        coalescer.requestEcho();
        coalescer.requestEcho();
        coalescer.cancel();

        assertEquals(1, sendCount.get(), "Cancel should not send the pending echo");
        assertEquals(0, scheduler.getScheduledTaskCount(), "Cancel must remove the delayed wake-up");
    }

    @Test
    void separateCoalescersRemainIndependent()
    {
        final EchoFlowTestSupport.FakeClock clock = new EchoFlowTestSupport.FakeClock();
        final EchoFlowTestSupport.FakeScheduler scheduler = new EchoFlowTestSupport.FakeScheduler();
        final AtomicInteger sendCount = new AtomicInteger();
        final EchoFlowCoalescer first = createCoalescer(clock, scheduler, sendCount);
        final EchoFlowCoalescer second = createCoalescer(clock, scheduler, sendCount);

        first.requestEcho();
        first.requestEcho();
        second.requestEcho();

        assertEquals(2, sendCount.get(), "A second scope should still send immediately");
        assertEquals(1, scheduler.getScheduledTaskCount(),
                "Only the scope with a pending echo should have a delayed wake-up");
    }

    private EchoFlowCoalescer createCoalescer(final EchoFlowTestSupport.FakeClock clock,
                                              final EchoFlowTestSupport.FakeScheduler scheduler,
                                              final AtomicInteger sendCount)
    {
        final EchoFlowCoalescer[] coalescerHolder = new EchoFlowCoalescer[1];
        coalescerHolder[0] = new EchoFlowCoalescer(10L, clock, scheduler, EchoFlowTestSupport.DIRECT_EXECUTOR, () ->
        {
            sendCount.incrementAndGet();
            coalescerHolder[0].markFlowSent();
        });
        return coalescerHolder[0];
    }
}
