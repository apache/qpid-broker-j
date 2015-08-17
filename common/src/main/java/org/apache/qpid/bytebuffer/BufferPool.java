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
package org.apache.qpid.bytebuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

class BufferPool
{
    private static final AtomicIntegerFieldUpdater<BufferPool> MAX_SIZE = AtomicIntegerFieldUpdater.newUpdater(BufferPool.class, "_maxSize");
    @SuppressWarnings("unused")
    private volatile int _maxSize;
    private final ConcurrentLinkedQueue<ByteBuffer> _pooledBuffers = new ConcurrentLinkedQueue<>();

    BufferPool(final int maxSize)
    {
        _maxSize = maxSize;
    }

    ByteBuffer getBuffer()
    {
        return _pooledBuffers.poll();
    }

    void returnBuffer(ByteBuffer buf)
    {
        buf.clear();
        if(_pooledBuffers.size() < MAX_SIZE.get(this))
        {
            _pooledBuffers.add(buf);
        }
    }

    void ensureSize(final int maxPoolSize)
    {
        int currentSize;
        while((currentSize = MAX_SIZE.get(this))<maxPoolSize && !MAX_SIZE.compareAndSet(this, currentSize, maxPoolSize));
    }

    public int getSize()
    {
        return MAX_SIZE.get(this);
    }
}
