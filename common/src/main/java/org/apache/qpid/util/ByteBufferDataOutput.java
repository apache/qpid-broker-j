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
package org.apache.qpid.util;

import java.io.DataOutput;
import java.nio.ByteBuffer;

public class ByteBufferDataOutput implements DataOutput
{
    private final ByteBuffer _buf;

    public ByteBufferDataOutput(ByteBuffer buf)
    {
        _buf = buf;
    }

    public void write(int b)
    {
        _buf.put((byte)b);
    }

    public void write(byte[] b)
    {
       _buf.put(b);
    }


    public void write(byte[] b, int off, int len)
    {
        _buf.put(b, off, len);

    }

    public void writeBoolean(boolean v)
    {
        _buf.put( v ? (byte) 1 : (byte) 0);
    }

    public void writeByte(int v)
    {
        _buf.put((byte) v);
    }

    public void writeShort(int v)
    {
        _buf.putShort((short)v);
    }

    public void writeChar(int v)
    {
        _buf.put((byte) (v >>> 8));
        _buf.put((byte) v);
    }

    public void writeInt(int v)
    {
        _buf.putInt(v);
    }

    public void writeLong(long v)
    {
        _buf.putLong(v);
    }

    public void writeFloat(float v)
    {
        writeInt(Float.floatToIntBits(v));
    }

    public void writeDouble(double v)
    {
        writeLong(Double.doubleToLongBits(v));
    }

    public void writeBytes(String s)
    {
        throw new UnsupportedOperationException("writeBytes(String s) not supported");
    }

    public void writeChars(String s)
    {
        int len = s.length();
        for (int i = 0 ; i < len ; i++)
        {
            int v = s.charAt(i);
            _buf.put((byte) (v >>> 8));
            _buf.put((byte) v);
        }
    }

    public void writeUTF(String s)
    {
        int strlen = s.length();

        int pos = _buf.position();
        _buf.position(pos+2);


        for (int i = 0; i < strlen; i++)
        {
            int c = s.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F))
            {
                c = s.charAt(i);
                _buf.put((byte) c);

            }
            else if (c > 0x07FF)
            {
                _buf.put((byte) (0xE0 | ((c >> 12) & 0x0F)));
                _buf.put((byte) (0x80 | ((c >>  6) & 0x3F)));
                _buf.put((byte) (0x80 | (c & 0x3F)));
            }
            else
            {
                _buf.put((byte) (0xC0 | ((c >>  6) & 0x1F)));
                _buf.put((byte) (0x80 | (c & 0x3F)));
            }
        }

        int len = _buf.position() - (pos + 2);

        _buf.put(pos++, (byte) (len >>> 8));
        _buf.put(pos, (byte) len);
    }

}
