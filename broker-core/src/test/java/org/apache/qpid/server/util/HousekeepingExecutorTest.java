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

package org.apache.qpid.server.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.ThreadFactory;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.pool.SubjectExecutionContextThreadFactory;
import org.apache.qpid.test.utils.UnitTestBase;

public class HousekeepingExecutorTest extends UnitTestBase
{
    @Test
    public void afterExecuteInvokesUncaughtExceptionHandler() throws Exception
    {
        final AtomicReference<Throwable> captured = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        final Thread.UncaughtExceptionHandler handler = (thread, throwable) ->
        {
            captured.set(throwable);
            latch.countDown();
        };

        final HousekeepingExecutor executor = new HousekeepingExecutor(getTestName(), 1, null);
        try
        {
            final ThreadFactory baseFactory =
                    new SubjectExecutionContextThreadFactory(getTestName(), null);
            final ThreadFactory factory = runnable ->
            {
                final Thread thread = baseFactory.newThread(runnable);
                thread.setUncaughtExceptionHandler(handler);
                return thread;
            };
            executor.setThreadFactory(QpidByteBuffer.createQpidByteBufferTrackingThreadFactory(factory));

            final Future<?> future = executor.submit(() -> { throw new RuntimeException("boom"); });
            try
            {
                future.get(3, TimeUnit.SECONDS);
            }
            catch (ExecutionException e)
            {
                // expected
            }

            assertTrue(latch.await(3, TimeUnit.SECONDS), "Uncaught exception handler not invoked");
            assertEquals("boom", captured.get().getMessage(), "Unexpected exception message");
        }
        finally
        {
            executor.shutdownNow();
        }
    }
}
