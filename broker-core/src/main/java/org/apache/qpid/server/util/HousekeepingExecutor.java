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

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import javax.security.auth.Subject;

import com.google.common.util.concurrent.UncheckedExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.pool.SuppressingInheritedAccessControlContextThreadFactory;

public class HousekeepingExecutor extends ScheduledThreadPoolExecutor
{

    private static final Logger LOGGER = LoggerFactory.getLogger(HousekeepingExecutor.class);

    public HousekeepingExecutor(final String threadPrefix, final int threadCount, final Subject subject)
    {
        super(threadCount, QpidByteBuffer.createQpidByteBufferTrackingThreadFactory(createThreadFactory(threadPrefix, threadCount, subject)));

    }

    private static SuppressingInheritedAccessControlContextThreadFactory createThreadFactory(String threadPrefix, int threadCount, Subject subject)
    {
        return new SuppressingInheritedAccessControlContextThreadFactory(threadPrefix, subject);
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t)
    {
        super.afterExecute(r, t);

        if (t == null && r instanceof Future<?>)
        {
            Future future = (Future<?>) r;
            try
            {
                if (future.isDone())
                {
                    Object result = future.get();
                }
            }
            catch (CancellationException ce)
            {
                LOGGER.debug("Housekeeping task got cancelled");
                // Ignore cancellation of task
            }
            catch (ExecutionException | UncheckedExecutionException ee)
            {
                t = ee.getCause();
            }
            catch (InterruptedException ie)
            {
                Thread.currentThread().interrupt(); // ignore/reset
            }
            catch (Throwable t1)
            {
                t = t1;
            }
        }
        if (t != null)
        {
            LOGGER.error("Housekeeping task threw an exception:", t);

            final Thread.UncaughtExceptionHandler uncaughtExceptionHandler = Thread.getDefaultUncaughtExceptionHandler();
            if (uncaughtExceptionHandler != null)
            {
                uncaughtExceptionHandler.uncaughtException(Thread.currentThread(), t);
            }
            else
            {
                Runtime.getRuntime().halt(1);
            }
        }
    }
}
