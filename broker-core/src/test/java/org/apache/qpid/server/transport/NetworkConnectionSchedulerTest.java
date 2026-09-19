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
package org.apache.qpid.server.transport;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class NetworkConnectionSchedulerTest extends UnitTestBase
{
    private static final int SELECTOR_COUNT = 1;
    private static final int PROCESSING_THREAD_COUNT = 2;
    private static final int POOL_SIZE = SELECTOR_COUNT + PROCESSING_THREAD_COUNT;
    private static final long TIMEOUT_SECONDS = 10L;

    private NetworkConnectionScheduler _scheduler;

    @Test
    public void testProcessingCapacityRestoredAfterConcurrentTaskFailures() throws Exception
    {
        final CyclicBarrier failureBarrier = new CyclicBarrier(PROCESSING_THREAD_COUNT);
        final CountDownLatch failureTasksStarted = new CountDownLatch(PROCESSING_THREAD_COUNT);
        final CountDownLatch workerFailures = new CountDownLatch(PROCESSING_THREAD_COUNT);
        final ThreadFactory threadFactory = runnable ->
        {
            final Thread thread = new Thread(runnable, getTestName());
            thread.setDaemon(true);
            thread.setUncaughtExceptionHandler((ignored, failure) -> workerFailures.countDown());
            return thread;
        };

        _scheduler = new NetworkConnectionScheduler(getTestName(), SELECTOR_COUNT, POOL_SIZE, 1L, threadFactory);
        _scheduler.start();

        _scheduler.schedule(createFailingConnection(new NegativeArraySizeException("test failure"), failureBarrier,
                failureTasksStarted));
        _scheduler.schedule(createFailingConnection(new StackOverflowError("test failure"), failureBarrier,
                failureTasksStarted));

        assertTrue(failureTasksStarted.await(TIMEOUT_SECONDS, TimeUnit.SECONDS),
                   "Concurrent failure tasks did not start");
        assertTrue(workerFailures.await(TIMEOUT_SECONDS, TimeUnit.SECONDS),
                   "Selector processing tasks did not terminate as expected");

        final CyclicBarrier successfulWorkBarrier = new CyclicBarrier(PROCESSING_THREAD_COUNT);
        final CountDownLatch successfulWorkCompleted = new CountDownLatch(PROCESSING_THREAD_COUNT);
        for (int i = 0; i < PROCESSING_THREAD_COUNT; i++)
        {
            _scheduler.schedule(createSuccessfulConnection(successfulWorkBarrier, successfulWorkCompleted));
        }

        assertTrue(successfulWorkCompleted.await(TIMEOUT_SECONDS, TimeUnit.SECONDS),
                "Configured selector processing capacity was not restored");
    }

    private NonBlockingConnection createFailingConnection(final Throwable failure,
                                                          final CyclicBarrier failureBarrier,
                                                          final CountDownLatch failureTasksStarted)
    {
        final NonBlockingConnection connection = createConnection();
        doAnswer(invocation ->
        {
            failureTasksStarted.countDown();
            await(failureBarrier);
            throw failure;
        }).when(connection).doWork();
        return connection;
    }

    private NonBlockingConnection createSuccessfulConnection(final CyclicBarrier successfulWorkBarrier,
                                                             final CountDownLatch successfulWorkCompleted)
    {
        final NonBlockingConnection connection = createConnection();
        doAnswer(invocation ->
        {
            await(successfulWorkBarrier);
            successfulWorkCompleted.countDown();
            return true;
        }).when(connection).doWork();
        return connection;
    }

    private NonBlockingConnection createConnection()
    {
        final NonBlockingConnection connection = mock(NonBlockingConnection.class);
        when(connection.setScheduled()).thenReturn(true);
        when(connection.getThreadName()).thenReturn(getTestName());
        when(connection.getScheduler()).thenReturn(_scheduler);
        return connection;
    }

    private static void await(final CyclicBarrier barrier)
    {
        try
        {
            barrier.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while coordinating selector processing tasks", e);
        }
        catch (BrokenBarrierException | TimeoutException e)
        {
            throw new AssertionError("Failed to coordinate selector processing tasks", e);
        }
    }

    @AfterEach
    public void tearDown()
    {
        if (_scheduler != null)
        {
            _scheduler.close();
        }
    }
}
