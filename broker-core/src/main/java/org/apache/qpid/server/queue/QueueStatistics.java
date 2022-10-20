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
package org.apache.qpid.server.queue;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

final class QueueStatistics
{
    private final AtomicInteger _queueCount = new AtomicInteger();
    private final AtomicLong _queueSize = new AtomicLong();

    private final AtomicInteger _unackedCount = new AtomicInteger();
    private final AtomicLong _unackedSize = new AtomicLong();

    private final AtomicInteger _availableCount = new AtomicInteger();
    private final AtomicLong _availableSize = new AtomicLong();

    private final AtomicLong _dequeueCount = new AtomicLong();
    private final AtomicLong _dequeueSize = new AtomicLong();

    private final AtomicLong _enqueueCount = new AtomicLong();
    private final AtomicLong _enqueueSize = new AtomicLong();

    private final AtomicLong _persistentEnqueueCount = new AtomicLong();
    private final AtomicLong _persistentEnqueueSize = new AtomicLong();

    private final AtomicLong _persistentDequeueCount = new AtomicLong();
    private final AtomicLong _persistentDequeueSize = new AtomicLong();

    private final AtomicInteger _queueCountHwm = new AtomicInteger();
    private final AtomicLong _queueSizeHwm = new AtomicLong();

    private final AtomicInteger _availableCountHwm = new AtomicInteger();
    private final AtomicLong _availableSizeHwm = new AtomicLong();

    private final AtomicInteger _expiredCount = new AtomicInteger();
    private final AtomicLong _expiredSize = new AtomicLong();
    private final AtomicInteger _malformedCount = new AtomicInteger();
    private final AtomicLong _malformedSize = new AtomicLong();

    public int getQueueCount()
    {
        return _queueCount.get();
    }

    public long getQueueSize()
    {
        return _queueSize.get();
    }

    public int getUnackedCount()
    {
        return _unackedCount.get();
    }

    public long getUnackedSize()
    {
        return _unackedSize.get();
    }

    public int getAvailableCount()
    {
        return _availableCount.get();
    }

    public long getAvailableSize()
    {
        return _availableSize.get();
    }

    public long getEnqueueCount()
    {
        return _enqueueCount.get();
    }

    public long getEnqueueSize()
    {
        return _enqueueSize.get();
    }

    public long getDequeueCount()
    {
        return _dequeueCount.get();
    }

    public long getDequeueSize()
    {
        return _dequeueSize.get();
    }

    public long getPersistentEnqueueCount()
    {
        return _persistentEnqueueCount.get();
    }

    public long getPersistentEnqueueSize()
    {
        return _persistentEnqueueSize.get();
    }

    public long getPersistentDequeueCount()
    {
        return _persistentDequeueCount.get();
    }

    public long getPersistentDequeueSize()
    {
        return _persistentDequeueSize.get();
    }

    public int getQueueCountHwm()
    {
        return _queueCountHwm.get();
    }

    public long getQueueSizeHwm()
    {
        return _queueSizeHwm.get();
    }

    public int getAvailableCountHwm()
    {
        return _availableCountHwm.get();
    }

    public long getAvailableSizeHwm()
    {
        return _availableSizeHwm.get();
    }

    public int getExpiredCount()
    {
        return _expiredCount.get();
    }

    public long getExpiredSize()
    {
        return _expiredSize.get();
    }

    public int getMalformedCount()
    {
        return _malformedCount.get();
    }

    public long getMalformedSize()
    {
        return _malformedSize.get();
    }

    void addToQueue(long size)
    {
        int count = _queueCount.incrementAndGet();
        long queueSize = _queueSize.addAndGet(size);
        int hwm;
        while((hwm = _queueCountHwm.get()) < count)
        {
            _queueCountHwm.compareAndSet(hwm, count);
        }
        long sizeHwm;
        while((sizeHwm = _queueSizeHwm.get()) < queueSize)
        {
            _queueSizeHwm.compareAndSet(sizeHwm, queueSize);
        }
    }

    void removeFromQueue(long size)
    {
        _queueCount.decrementAndGet();
        _queueSize.addAndGet(-size);
    }

    void addToAvailable(long size)
    {
        int count = _availableCount.incrementAndGet();
        long availableSize = _availableSize.addAndGet(size);
        int hwm;
        while((hwm = _availableCountHwm.get()) < count)
        {
            _availableCountHwm.compareAndSet(hwm, count);
        }
        long sizeHwm;
        while((sizeHwm = _availableSizeHwm.get()) < availableSize)
        {
            _availableSizeHwm.compareAndSet(sizeHwm, availableSize);
        }
    }

    void removeFromAvailable(long size)
    {
        _availableCount.decrementAndGet();
        _availableSize.addAndGet(-size);
    }

    void addToUnacknowledged(long size)
    {
        _unackedCount.incrementAndGet();
        _unackedSize.addAndGet(size);
    }

    void removeFromUnacknowledged(long size)
    {
        _unackedCount.decrementAndGet();
        _unackedSize.addAndGet(-size);
    }

    void addToEnqueued(long size)
    {
        _enqueueCount.incrementAndGet();
        _enqueueSize.addAndGet(size);
    }

    void addToDequeued(long size)
    {
        _dequeueCount.incrementAndGet();
        _dequeueSize.addAndGet(size);
    }

    void addToPersistentEnqueued(long size)
    {
        _persistentEnqueueCount.incrementAndGet();
        _persistentEnqueueSize.addAndGet(size);
    }

    void addToPersistentDequeued(long size)
    {
        _persistentDequeueCount.incrementAndGet();
        _persistentDequeueSize.addAndGet(size);
    }

    void addToExpired(final long size)
    {
        _expiredCount.incrementAndGet();
        _expiredSize.addAndGet(size);
    }

    void addToMalformed(final long size)
    {
        _malformedCount.incrementAndGet();
        _malformedSize.addAndGet(size);
    }

    void reset()
    {
        _queueCountHwm.set(_queueCount.get());
        // _queueCount shouldn't be reset

        _queueSizeHwm.set(_queueSize.get());
        // _queueSize shouldn't be reset

        // _unackedCount shouldn't be reset
        // _unackedSize shouldn't be reset

        _availableCountHwm.set(_availableCount.get());
        // _availableCount shouldn't be reset

        _availableSizeHwm.set(_availableSize.get());
        // _availableSize shouldn't be reset

        _dequeueCount.set(0L);
        _dequeueSize.set(0L);

        _enqueueCount.set(0L);
        _enqueueSize.set(0L);

        _persistentEnqueueCount.set(0L);
        _persistentEnqueueSize.set(0L);

        _persistentDequeueCount.set(0L);
        _persistentDequeueSize.set(0L);

        _expiredCount.set(0);
        _expiredSize.set(0L);

        _malformedCount.set(0);
        _malformedSize.set(0L);
    }
}
