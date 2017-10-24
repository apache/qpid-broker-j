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
package org.apache.qpid.server.bytebuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;

class PooledByteBufferRef implements ByteBufferRef
{
    private static final AtomicIntegerFieldUpdater<PooledByteBufferRef> REF_COUNT_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(PooledByteBufferRef.class, "_refCount");
    private static final AtomicIntegerFieldUpdater<PooledByteBufferRef> CLAIMED_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(PooledByteBufferRef.class, "_claimed");
    private static final AtomicInteger ACTIVE_BUFFERS = new AtomicInteger();
    private static final AtomicLong DISPOSAL_COUNTER = new AtomicLong();
    private final ByteBuffer _buffer;

    @SuppressWarnings("unused")
    private volatile int _refCount;

    @SuppressWarnings("unused")
    private volatile int _claimed;

    PooledByteBufferRef(final ByteBuffer buffer)
    {
        if (buffer == null)
        {
            throw new NullPointerException();
        }
        _buffer = buffer;
        ACTIVE_BUFFERS.incrementAndGet();
    }

    @Override
    public void incrementRef(final int capacity)
    {
        if(REF_COUNT_UPDATER.get(this) >= 0)
        {
            CLAIMED_UPDATER.addAndGet(this, capacity);
            REF_COUNT_UPDATER.incrementAndGet(this);
        }
    }

    @Override
    public void decrementRef(final int capacity)
    {
        CLAIMED_UPDATER.addAndGet(this, -capacity);
        DISPOSAL_COUNTER.incrementAndGet();
        if(REF_COUNT_UPDATER.get(this) > 0 && REF_COUNT_UPDATER.decrementAndGet(this) == 0)
        {
            QpidByteBuffer.returnToPool(_buffer);
            ACTIVE_BUFFERS.decrementAndGet();
        }
    }

    @Override
    public ByteBuffer getBuffer()
    {
        return _buffer.duplicate();
    }

    @Override
    public boolean isSparse(final double minimumSparsityFraction)
    {
        return minimumSparsityFraction > (double) CLAIMED_UPDATER.get(this) / (double) _buffer.capacity();
    }

    static int getActiveBufferCount()
    {
        return ACTIVE_BUFFERS.get();
    }

    static long getDisposalCounter()
    {
        return DISPOSAL_COUNTER.get();
    }
}
