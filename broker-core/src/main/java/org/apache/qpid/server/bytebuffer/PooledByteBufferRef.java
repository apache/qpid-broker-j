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
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

class PooledByteBufferRef implements ByteBufferRef
{
    private static final AtomicIntegerFieldUpdater<PooledByteBufferRef> REF_COUNT = AtomicIntegerFieldUpdater.newUpdater(PooledByteBufferRef.class, "_refCount");

    private final ByteBuffer _buffer;
    private volatile int _refCount;

    PooledByteBufferRef(final ByteBuffer buffer)
    {
        _buffer = buffer;
    }

    @Override
    public void incrementRef()
    {

        if(REF_COUNT.get(this) >= 0)
        {
            REF_COUNT.incrementAndGet(this);
        }
    }

    @Override
    public void decrementRef()
    {
        if(REF_COUNT.get(this) > 0 && REF_COUNT.decrementAndGet(this) == 0)
        {
            QpidByteBuffer.returnToPool(_buffer);
        }
    }

    @Override
    public ByteBuffer getBuffer()
    {
        return _buffer.duplicate();
    }

    @Override
    public void removeFromPool()
    {
        REF_COUNT.set(this, Integer.MIN_VALUE/2);
    }


}
