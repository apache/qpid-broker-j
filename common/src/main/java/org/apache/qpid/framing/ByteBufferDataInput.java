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

import org.apache.qpid.codec.MarkableDataInput;

public class ByteBufferDataInput implements ExtendedDataInput, MarkableDataInput
{
    private final ByteBuffer _underlying;
    private int _mark;

    public ByteBufferDataInput(ByteBuffer underlying)
    {
        _underlying = underlying.slice();
    }

    public void readFully(byte[] b)
    {
        _underlying.get(b);
    }

    public void readFully(byte[] b, int off, int len)
    {
        _underlying.get(b,0, len);
    }

    public ByteBuffer readAsByteBuffer(int len)
    {
        ByteBuffer buf = _underlying.slice();
        buf.limit(len);
        skipBytes(len);
        return buf;
    }

    public int skipBytes(int n)
    {
        _underlying.position(_underlying.position()+n);
        return _underlying.position();
    }

    public boolean readBoolean()
    {
        return _underlying.get() != 0;
    }

    public byte readByte()
    {
        return _underlying.get();
    }

    public int readUnsignedByte()
    {
        return ((int)_underlying.get()) & 0xFF;
    }

    public short readShort()
    {
        return _underlying.getShort();
    }

    public int readUnsignedShort()
    {
        return ((int)_underlying.getShort()) & 0xffff;
    }

    public char readChar()
    {
        return (char) _underlying.getChar();
    }

    public int readInt()
    {
        return _underlying.getInt();
    }

    public long readLong()
    {
        return _underlying.getLong();
    }

    public float readFloat()
    {
        return _underlying.getFloat();
    }

    public double readDouble()
    {
        return _underlying.getDouble();
    }

    public AMQShortString readAMQShortString()
    {
        return AMQShortString.readAMQShortString(_underlying);
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
        return _underlying.remaining();
    }


    public long skip(long i)
    {
        _underlying.position(_underlying.position()+(int)i);
        return i;
    }

    public int read(byte[] b)
    {
        readFully(b);
        return b.length;
    }

    public int position()
    {
        return _underlying.position();
    }

    public void position(int position)
    {
        _underlying.position(position);
    }

    public int length()
    {
        return _underlying.limit();
    }


    public void mark(int readAhead)
    {
        _mark = position();
    }

    public void reset()
    {
        _underlying.position(_mark);
    }
}
