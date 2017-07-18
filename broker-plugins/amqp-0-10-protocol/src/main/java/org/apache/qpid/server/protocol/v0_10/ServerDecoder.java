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

import java.util.List;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_10.transport.AbstractDecoder;

final class ServerDecoder extends AbstractDecoder
{
    private final List<QpidByteBuffer> _underlying;
    private int _bufferIndex;

    ServerDecoder(List<QpidByteBuffer> in)
    {
        _underlying = in;
        _bufferIndex = 0;
    }

    private void advanceIfNecessary()
    {
        while(!getCurrentBuffer().hasRemaining() && _bufferIndex != _underlying.size()-1)
        {
            _bufferIndex++;
        }
    }

    private QpidByteBuffer getBuffer(int size)
    {
        advanceIfNecessary();
        final QpidByteBuffer currentBuffer = getCurrentBuffer();
        if(currentBuffer.remaining()>= size)
        {
            return currentBuffer;
        }
        else
        {
            return readAsQpidByteBuffer(size);
        }
    }

    private QpidByteBuffer readAsQpidByteBuffer(int len)
    {
        QpidByteBuffer currentBuffer = getCurrentBuffer();
        if(currentBuffer.remaining()>=len)
        {
            QpidByteBuffer buf = currentBuffer.slice();
            buf.limit(len);
            currentBuffer.position(currentBuffer.position()+len);
            return buf;
        }
        else
        {
            QpidByteBuffer dest = QpidByteBuffer.allocate(len);
            while(dest.hasRemaining() && available()>0)
            {
                advanceIfNecessary();
                currentBuffer = getCurrentBuffer();
                final int remaining = dest.remaining();
                if(currentBuffer.remaining()>= remaining)
                {
                    QpidByteBuffer buf = currentBuffer.slice();
                    buf.limit(remaining);
                    currentBuffer.position(currentBuffer.position()+remaining);
                    dest.put(buf);
                    buf.dispose();
                }
                else
                {
                    dest.put(currentBuffer);
                }
            }

            dest.flip();
            return dest;
        }
    }

    private int available()
    {
        int remaining = 0;
        for(int i = _bufferIndex; i < _underlying.size(); i++)
        {
            remaining += _underlying.get(i).remaining();
        }
        return remaining;
    }


    private QpidByteBuffer getCurrentBuffer()
    {
        return _underlying.get(_bufferIndex);
    }

    @Override
    protected byte doGet()
    {
        return getBuffer(1).get();
    }

    @Override
    protected void doGet(byte[] bytes)
    {
        getBuffer(bytes.length).get(bytes);
    }

    @Override
    public boolean hasRemaining()
    {
        return available() != 0;
    }

    @Override
    public short readUint8()
    {
        return (short) (0xFF & getBuffer(1).get());
    }

    @Override
    public int readUint16()
    {
        return 0xFFFF & getBuffer(2).getShort();
    }

    @Override
    public long readUint32()
    {
        return 0xFFFFFFFFL & getBuffer(4).getInt();
    }

    @Override
    public long readUint64()
    {
        return getBuffer(8).getLong();
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
		return getBuffer(8).getDouble();
	}

	@Override
    public float readFloat()
	{
		return getBuffer(4).getFloat();
	}

	@Override
    public short readInt16()
	{
		return getBuffer(2).getShort();
	}

	@Override
    public int readInt32()
	{
		return getBuffer(4).getInt();
	}

	@Override
    public byte readInt8()
	{
		return getBuffer(1).get();
	}

	@Override
    public byte[] readRemainingBytes()
	{
      byte[] result = new byte[available()];
      get(result);
      return result;		
	}

	@Override
    public long readInt64()
	{
		return getBuffer(8).getLong();
	}
}
