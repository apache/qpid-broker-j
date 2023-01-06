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
package org.apache.qpid.test.utils;


import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Method;

import org.junit.jupiter.api.extension.ExtensionContext;

public class TestUtils
{
    public static String dumpThreads()
    {
        final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        final ThreadInfo[] threadInfos = threadMXBean.dumpAllThreads(true, true);
        final StringBuilder dump = new StringBuilder();
        dump.append(String.format("%n"));
        for (final ThreadInfo threadInfo : threadInfos)
        {
            dump.append(threadInfo);
        }

        final long[] deadLocks = threadMXBean.findDeadlockedThreads();
        if (deadLocks != null && deadLocks.length > 0)
        {
            final ThreadInfo[] deadlockedThreads = threadMXBean.getThreadInfo(deadLocks);
            dump.append(String.format("%n"));
            dump.append("Deadlock is detected!");
            dump.append(String.format("%n"));
            for (final ThreadInfo threadInfo : deadlockedThreads)
            {
                dump.append(threadInfo);
            }
        }
        return dump.toString();
    }

    public static Class<?> getTestClass(final ExtensionContext extensionContext)
    {
        return extensionContext.getTestClass()
                .orElseThrow(() -> new RuntimeException("Failed to resolve test class"));
    }

    public static Method getTestMethod(final ExtensionContext extensionContext)
    {
        return extensionContext.getTestMethod()
                .orElseThrow(() -> new RuntimeException("Failed to resolve test method"));
    }
}
