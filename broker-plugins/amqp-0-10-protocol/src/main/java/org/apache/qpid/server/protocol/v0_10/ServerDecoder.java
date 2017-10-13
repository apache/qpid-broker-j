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

    ServerDecoder(QpidByteBuffer in)
    {
        _underlying = in;
    }

    @Override
    protected byte doGet()
    {
        return _underlying.get();
    }

    @Override
    protected void doGet(byte[] bytes)
    {
        _underlying.get(bytes);
    }

    @Override
    public boolean hasRemaining()
    {
        return _underlying.remaining() != 0;
    }

    @Override
    public short readUint8()
    {
        return _underlying.getUnsignedByte();
    }

    @Override
    public int readUint16()
    {
        return _underlying.getUnsignedShort();
    }

    @Override
    public long readUint32()
    {
        return _underlying.getUnsignedInt();
    }

    @Override
    public long readUint64()
    {
        return _underlying.getLong();
    }

	@Override
    public byte[] readBin128()
	{
		byte[] result = new byte[16];
		get(result);
		return result;
	}
	
	@Override
    public byte[] readBytes(int howManyBytes)
	{
		byte[] result = new byte[howManyBytes];
		get(result);
		return result;
	}
	
	@Override
    public double readDouble()
	{
        return _underlying.getDouble();
	}

	@Override
    public float readFloat()
	{
        return _underlying.getFloat();
	}

	@Override
    public short readInt16()
	{
        return _underlying.getShort();
	}

	@Override
    public int readInt32()
	{
        return _underlying.getInt();
	}

	@Override
    public byte readInt8()
	{
        return _underlying.get();
	}

	@Override
    public byte[] readRemainingBytes()
	{
        byte[] result = new byte[_underlying.remaining()];
        get(result);
        return result;
    }

	@Override
    public long readInt64()
	{
        return _underlying.getLong();
	}
}
