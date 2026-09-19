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
package org.apache.qpid.server.protocol.v0_10;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_10.transport.AbstractDecoder;

final class ServerDecoder extends AbstractDecoder
{
    private final QpidByteBuffer _underlying;

    ServerDecoder(final QpidByteBuffer in)
    {
        this(in, DEFAULT_MAX_ZERO_WIDTH_ARRAY_ELEMENTS, DEFAULT_MAX_NESTED_OBJECTS);
    }

    ServerDecoder(final QpidByteBuffer in,
                  final int maxZeroWidthArrayElements,
                  final int maxNestedObjects)
    {
        super(maxZeroWidthArrayElements, maxNestedObjects);
        _underlying = in;
    }

    @Override
    protected byte doGet()
    {
        return _underlying.get();
    }

    @Override
    protected void doGet(final byte[] bytes)
    {
        _underlying.get(bytes);
    }

    @Override
    protected int underlyingRemaining()
    {
        return _underlying.remaining();
    }

    @Override
    public boolean hasRemaining()
    {
        return remaining() != 0;
    }

    @Override
    public short readUint8()
    {
        checkAvailable(Byte.BYTES);
        final short value = _underlying.getUnsignedByte();
        recordBytesRead(Byte.BYTES);
        return value;
    }

    @Override
    public int readUint16()
    {
        checkAvailable(Short.BYTES);
        final int value = _underlying.getUnsignedShort();
        recordBytesRead(Short.BYTES);
        return value;
    }

    @Override
    public long readUint32()
    {
        checkAvailable(Integer.BYTES);
        final long value = _underlying.getUnsignedInt();
        recordBytesRead(Integer.BYTES);
        return value;
    }

    @Override
    public long readUint64()
    {
        checkAvailable(Long.BYTES);
        final long value = _underlying.getLong();
        recordBytesRead(Long.BYTES);
        return value;
    }

    @Override
    public byte[] readBin128()
    {
        final byte[] result = new byte[16];
        get(result);
        return result;
    }

    @Override
    public byte[] readBytes(final int howManyBytes)
    {
        return readByteArray(howManyBytes);
    }

    @Override
    public double readDouble()
    {
        checkAvailable(Double.BYTES);
        final double value = _underlying.getDouble();
        recordBytesRead(Double.BYTES);
        return value;
    }

    @Override
    public float readFloat()
    {
        checkAvailable(Float.BYTES);
        final float value = _underlying.getFloat();
        recordBytesRead(Float.BYTES);
        return value;
    }

    @Override
    public short readInt16()
    {
        checkAvailable(Short.BYTES);
        final short value = _underlying.getShort();
        recordBytesRead(Short.BYTES);
        return value;
    }

    @Override
    public int readInt32()
    {
        checkAvailable(Integer.BYTES);
        final int value = _underlying.getInt();
        recordBytesRead(Integer.BYTES);
        return value;
    }

    @Override
    public byte readInt8()
    {
        checkAvailable(Byte.BYTES);
        final byte value = _underlying.get();
        recordBytesRead(Byte.BYTES);
        return value;
    }

    @Override
    public byte[] readRemainingBytes()
    {
        final byte[] result = new byte[remaining()];
        get(result);
        return result;
    }

    @Override
    public long readInt64()
    {
        checkAvailable(Long.BYTES);
        final long value = _underlying.getLong();
        recordBytesRead(Long.BYTES);
        return value;
    }
}
