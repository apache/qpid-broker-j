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

import java.util.concurrent.ThreadFactory;

public final class DaemonThreadFactory implements ThreadFactory
{
    private final String _threadName;
    private final Thread.UncaughtExceptionHandler _uncaughtExceptionHandler;
    private final ThreadGroup _threadGroup;

    public DaemonThreadFactory(final String threadName)
    {
        _threadName = threadName;
        _uncaughtExceptionHandler = null;
        _threadGroup = null;
    }

    public DaemonThreadFactory(final String threadName, final Thread.UncaughtExceptionHandler uncaughtExceptionHandler)
    {
        _threadName = threadName;
        _uncaughtExceptionHandler = uncaughtExceptionHandler;
        _threadGroup = null;
    }

    public DaemonThreadFactory(final String threadName, final Thread.UncaughtExceptionHandler uncaughtExceptionHandler, final ThreadGroup threadGroup)
    {
        _threadName = threadName;
        _uncaughtExceptionHandler = uncaughtExceptionHandler;
        _threadGroup = threadGroup;
    }

    @Override
    public Thread newThread(final Runnable runnable)
    {
        final Thread thread = _threadGroup == null ? new Thread(runnable, _threadName) : new Thread(_threadGroup, runnable, _threadName);
        thread.setDaemon(true);
        if (_uncaughtExceptionHandler != null)
        {
            thread.setUncaughtExceptionHandler(_uncaughtExceptionHandler);
        }
        return thread;
    }
}