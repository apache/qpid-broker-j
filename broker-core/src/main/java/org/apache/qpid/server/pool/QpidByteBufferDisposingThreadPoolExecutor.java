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
 *
 */

package org.apache.qpid.server.pool;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;

public class QpidByteBufferDisposingThreadPoolExecutor extends ThreadPoolExecutor
{
    private final Map<Thread, QpidByteBuffer> _cachedBufferMap = new ConcurrentHashMap<>();

    public QpidByteBufferDisposingThreadPoolExecutor(final int corePoolSize,
                                                     final int maximumPoolSize,
                                                     final long keepAliveTime,
                                                     final TimeUnit unit,
                                                     final BlockingQueue<Runnable> workQueue)
    {
        this(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, Executors.defaultThreadFactory());
    }

    public QpidByteBufferDisposingThreadPoolExecutor(final int corePoolSize,
                                                     final int maximumPoolSize,
                                                     final long keepAliveTime,
                                                     final TimeUnit unit,
                                                     final BlockingQueue<Runnable> workQueue,
                                                     final ThreadFactory factory)
    {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, factory);
    }

    @Override
    protected void afterExecute(final Runnable r, final Throwable t)
    {
        super.afterExecute(r, t);
        final QpidByteBuffer cachedThreadLocalBuffer = QpidByteBuffer.getCachedThreadLocalBuffer();
        if (cachedThreadLocalBuffer != null)
        {
            _cachedBufferMap.put(Thread.currentThread(), cachedThreadLocalBuffer);
        }
        else
        {
            _cachedBufferMap.remove(Thread.currentThread());
        }
    }

    @Override
    protected void terminated()
    {
        super.terminated();
        for (QpidByteBuffer qpidByteBuffer : _cachedBufferMap.values())
        {
            qpidByteBuffer.dispose();
        }
        _cachedBufferMap.clear();
    }
}
