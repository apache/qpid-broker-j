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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.function.LongSupplier;

final class EchoFlowTestSupport
{
    static final Executor DIRECT_EXECUTOR = Runnable::run;

    private EchoFlowTestSupport()
    {
    }

    static final class FakeClock implements LongSupplier
    {
        private long _timeNs;

        @Override
        public long getAsLong()
        {
            return _timeNs;
        }

        void advanceMillis(final long millis)
        {
            _timeNs += millis * 1_000_000L;
        }

        void setTimeNs(final long timeNs)
        {
            _timeNs = timeNs;
        }
    }

    static final class FakeScheduler implements EchoFlowCoalescer.Scheduler
    {
        private final List<FakeTask> _tasks = new ArrayList<>();

        @Override
        public EchoFlowCoalescer.Cancellable schedule(final int delayMillis, final Runnable task)
        {
            final FakeTask scheduledTask = new FakeTask(delayMillis, task);
            _tasks.add(scheduledTask);
            return scheduledTask;
        }

        int getScheduledTaskCount()
        {
            int count = 0;
            for (final FakeTask task : _tasks)
            {
                if (!task.isCancelled())
                {
                    count++;
                }
            }
            return count;
        }

        void runNext()
        {
            for (final FakeTask task : _tasks)
            {
                if (!task.isCancelled())
                {
                    task.run();
                    return;
                }
            }
            throw new IllegalStateException("No scheduled tasks to run");
        }

        private static final class FakeTask implements EchoFlowCoalescer.Cancellable
        {
            private final int _delayMillis;
            private final Runnable _task;

            private boolean _cancelled;

            private FakeTask(final int delayMillis, final Runnable task)
            {
                _delayMillis = delayMillis;
                _task = task;
            }

            @SuppressWarnings("unused")
            private int getDelayMillis()
            {
                return _delayMillis;
            }

            @Override
            public void cancel()
            {
                _cancelled = true;
            }

            private boolean isCancelled()
            {
                return _cancelled;
            }

            private void run()
            {
                _cancelled = true;
                _task.run();
            }
        }
    }
}
