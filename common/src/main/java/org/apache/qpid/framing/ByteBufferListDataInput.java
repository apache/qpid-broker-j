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
package org.apache.qpid.framing;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.qpid.codec.MarkableDataInput;

public class ByteBufferListDataInput implements ExtendedDataInput, MarkableDataInput
{
    private final List<ByteBuffer> _underlying;
    private int _bufferIndex;
    private int _mark;

    public ByteBufferListDataInput(List<ByteBuffer> underlying)
    {
        _underlying = underlying;
    }

    public void readFully(byte[] b)
    {
        final ByteBuffer currentBuffer = getCurrentBuffer();
        if(currentBuffer.remaining()>b.length)
        {
            currentBuffer.get(b);
        }
        else
        {
            ByteBuffer buf = readAsByteBuffer(b.length);
            buf.get(b);
        }
    }

    public void readFully(byte[] b, int off, int len)
    {
        final ByteBuffer currentBuffer = getCurrentBuffer();
        if(currentBuffer.remaining()>len)
        {
            currentBuffer.get(b, off, len);
        }
        else
        {
            ByteBuffer buf = readAsByteBuffer(len);
            buf.get(b, off, len);
        }
    }

    @Override
    public ByteBuffer readAsByteBuffer(int len)
    {
        ByteBuffer currentBuffer = getCurrentBuffer();
        if(currentBuffer.remaining()>=len)
        {
            ByteBuffer buf = currentBuffer.slice();
            buf.limit(len);
            currentBuffer.position(currentBuffer.position()+len);
            return buf;
        }
        else
        {
            ByteBuffer dest = currentBuffer.isDirect() ? ByteBuffer.allocateDirect(len) : ByteBuffer.allocate(len);
            while(dest.hasRemaining() && available()>0)
            {
                advanceIfNecessary();
                currentBuffer = getCurrentBuffer();
                final int remaining = dest.remaining();
                if(currentBuffer.remaining()>= remaining)
                {
                    ByteBuffer buf = currentBuffer.slice();
                    buf.limit(remaining);
                    currentBuffer.position(currentBuffer.position()+remaining);
                    dest.put(buf);
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

    public int skipBytes(int n)
    {
        final ByteBuffer currentBuffer = getCurrentBuffer();
        if(currentBuffer.remaining()>n)
        {
            currentBuffer.position(currentBuffer.position()+n);
        }
        else
        {
            n -= currentBuffer.remaining();
            currentBuffer.position(currentBuffer.limit());
            if(_bufferIndex != _underlying.size()-1)
            {
                _bufferIndex++;
                skipBytes(n);
            }
        }
        return position();
    }

    private ByteBuffer getCurrentBuffer()
    {
        return _underlying.get(_bufferIndex);
    }

    public boolean readBoolean()
    {
        advanceIfNecessary();
        return getCurrentBuffer().get() != 0;
    }

    private void advanceIfNecessary()
    {
        while(!getCurrentBuffer().hasRemaining() && _bufferIndex != _underlying.size()-1)
        {
            _bufferIndex++;
        }
    }

    public byte readByte()
    {
        advanceIfNecessary();
        return getCurrentBuffer().get();
    }

    public int readUnsignedByte()
    {
        advanceIfNecessary();
        return ((int)getCurrentBuffer().get()) & 0xFF;
    }

    public short readShort()
    {
        return getBuffer(2).getShort();
    }

    private ByteBuffer getBuffer(int size)
    {
        advanceIfNecessary();
        final ByteBuffer currentBuffer = getCurrentBuffer();
        if(currentBuffer.remaining()>= size)
        {
            return currentBuffer;
        }
        else
        {
            return readAsByteBuffer(size);
        }
    }

    public int readUnsignedShort()
    {
        return ((int)getBuffer(2).getShort()) & 0xffff;
    }

    public char readChar()
    {
        return (char) getBuffer(2).getChar();
    }

    public int readInt()
    {
        return getBuffer(4).getInt();
    }

    public long readLong()
    {
        return getBuffer(8).getLong();
    }

    public float readFloat()
    {
        return getBuffer(4).getFloat();
    }

    public double readDouble()
    {
        return getBuffer(8).getDouble();
    }

    public AMQShortString readAMQShortString()
    {
        advanceIfNecessary();
        final ByteBuffer currentBuffer = getCurrentBuffer();
        int size = ((int) currentBuffer.get(currentBuffer.position())) & 0xff;
        return AMQShortString.readAMQShortString(getBuffer(size + 1));
    }

    public String readLine()
    {
        throw new UnsupportedOperationException();
    }

    public String readUTF()
    {
        throw new UnsupportedOperationException();
    }

    public int available()
    {
        int remaining = 0;
        for(int i = _bufferIndex; i < _underlying.size(); i++)
        {
            remaining += _underlying.get(i).remaining();
        }
        return remaining;
    }


    public long skip(long i)
    {
        skipBytes((int)i);
        return i;
    }

    public int read(byte[] b)
    {
        readFully(b);
        return b.length;
    }

    public int position()
    {
        int position = 0;
        for(int i = 0; i < _bufferIndex; i++)
        {
            position += _underlying.get(i).limit();
        }
        position += getCurrentBuffer().position();
        return position;
    }

    public void position(int position)
    {
        int offset = 0;
        boolean beforePos = true;
        for(int i = 0; i < _underlying.size(); i++)
        {
            final ByteBuffer buffer = _underlying.get(i);
            if(beforePos)
            {
                if (position - offset <= buffer.limit())
                {
                    buffer.position(position - offset);
                    _bufferIndex = i;
                    beforePos = false;
                }
                else
                {
                    offset += buffer.limit();
                }
            }
            else
            {
                buffer.position(0);
            }
        }
    }

    public int length()
    {
        int length = 0;
        for(ByteBuffer buf : _underlying)
        {
            length+= buf.limit();
        }
        return length;
    }


    public void mark(int readAhead)
    {
        _mark = position();
    }

    public void reset()
    {
        position(_mark);
    }
}
