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
package org.apache.qpid.server.protocol.v0_10.transport;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Byte Buffer Decoder.
 * Decoder concrete implementor using a backing byte buffer for decoding data.
 *
 * @author Rafael H. Schloming
 */
public final class BBDecoder extends AbstractDecoder
{
    private ByteBuffer in;

    public BBDecoder()
    {
    }

    public BBDecoder(final int maxZeroWidthArrayElements, final int maxNestedObjects)
    {
        super(maxZeroWidthArrayElements, maxNestedObjects);
    }

    public void init(final ByteBuffer in)
    {
        this.in = in;
        this.in.order(ByteOrder.BIG_ENDIAN);
        resetDecoderState();
    }

    public void releaseBuffer()
    {
        in = null;
    }

    @Override
    protected byte doGet()
    {
        return in.get();
    }

    @Override
    protected void doGet(final byte[] bytes)
    {
        in.get(bytes);
    }

    @Override
    protected int underlyingRemaining()
    {
        return in.remaining();
    }

    @Override
    protected Binary get(final int size)
    {
        if (in.hasArray())
        {
            checkAvailable(size);
            final byte[] bytes = in.array();
            final Binary bin = new Binary(bytes, in.arrayOffset() + in.position(), size);
            in.position(in.position() + size);
            recordBytesRead(size);
            return bin;
        }
        else
        {
            return super.get(size);
        }
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
        final short value = (short) (0xFF & in.get());
        recordBytesRead(Byte.BYTES);
        return value;
    }

    @Override
    public int readUint16()
    {
        checkAvailable(Short.BYTES);
        final int value = 0xFFFF & in.getShort();
        recordBytesRead(Short.BYTES);
        return value;
    }

    @Override
    public long readUint32()
    {
        checkAvailable(Integer.BYTES);
        final long value = 0xFFFFFFFFL & in.getInt();
        recordBytesRead(Integer.BYTES);
        return value;
    }

    @Override
    public long readUint64()
    {
        checkAvailable(Long.BYTES);
        final long value = in.getLong();
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
        final double value = in.getDouble();
        recordBytesRead(Double.BYTES);
        return value;
    }

    @Override
    public float readFloat()
    {
        checkAvailable(Float.BYTES);
        final float value = in.getFloat();
        recordBytesRead(Float.BYTES);
        return value;
    }

    @Override
    public short readInt16()
    {
        checkAvailable(Short.BYTES);
        final short value = in.getShort();
        recordBytesRead(Short.BYTES);
        return value;
    }

    @Override
    public int readInt32()
    {
        checkAvailable(Integer.BYTES);
        final int value = in.getInt();
        recordBytesRead(Integer.BYTES);
        return value;
    }

    @Override
    public byte readInt8()
    {
        checkAvailable(Byte.BYTES);
        final byte value = in.get();
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
        final long value = in.getLong();
        recordBytesRead(Long.BYTES);
        return value;
    }
}
