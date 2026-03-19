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
package org.apache.qpid.server.pool;

import javax.security.auth.Subject;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.qpid.server.security.SubjectExecutionContext;

/**
 * {@link ThreadFactory} that creates threads running with the configured {@link Subject}.
 * <br>
 * It delegates thread creation to {@link Executors} default thread factory.
 */
public class SubjectExecutionContextThreadFactory implements ThreadFactory
{
    private final ThreadFactory _defaultThreadFactory = Executors.defaultThreadFactory();
    private final String _threadNamePrefix;
    private final AtomicInteger _threadId = new AtomicInteger();
    private final Subject _subject;

    public SubjectExecutionContextThreadFactory(final String threadNamePrefix, final Subject subject)
    {
        _threadNamePrefix = threadNamePrefix;
        _subject = subject;
    }

    @Override
    public Thread newThread(final Runnable runnable)
    {
        final Thread thread = _defaultThreadFactory.newThread(() ->
                SubjectExecutionContext.withSubject(_subject, runnable));
        if (_threadNamePrefix != null)
        {
            thread.setName(_threadNamePrefix + "-" + _threadId.getAndIncrement());
        }
        return thread;
    }
}
