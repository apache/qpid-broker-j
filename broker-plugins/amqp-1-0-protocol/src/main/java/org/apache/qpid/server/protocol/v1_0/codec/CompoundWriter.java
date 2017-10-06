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

package org.apache.qpid.server.protocol.v1_0.codec;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;

public abstract class CompoundWriter<V> implements ValueWriter<V>
{
    private int _length = -1;
    private final Registry _registry;

    public CompoundWriter(final Registry registry)
    {
        _registry = registry;
    }

    public CompoundWriter(final Registry registry, V object)
    {
        _registry = registry;
    }


    @Override
    public final int getEncodedSize()
    {
        int encodedSize = 1; // byte for the count
        if(_length == -1)
        {
            while (hasNext())
            {
                encodedSize += _registry.getValueWriter(next()).getEncodedSize();
            }
            if(encodedSize > 255)
            {
                encodedSize += 3;  // we'll need four bytes for the count, to match the length
            }
            _length = encodedSize;
        }
        else
        {
            encodedSize = _length;
        }
        if(encodedSize>255)
        {
            encodedSize+=5; // 1 byte constructor, 4 bytes length
        }
        else
        {
            encodedSize+=2; // 1 byte constructor, 1 byte length
        }
        return encodedSize;
    }

    @Override
    public final void writeToBuffer(QpidByteBuffer buffer)
    {
        if(_length == -1)
        {
            getEncodedSize();
        }
        writeToBuffer(buffer, _length>255);
    }

    private void writeToBuffer(QpidByteBuffer buffer, boolean large)
    {
        reset();
        final int count = getCount();

        buffer.put(large ? getFourOctetEncodingCode() : getSingleOctetEncodingCode());
        if(large)
        {
            buffer.putInt(_length);
            buffer.putInt(count);
        }
        else
        {
            buffer.put((byte) (_length));
            buffer.put((byte) count);
        }

        while(hasNext())
        {
            Object val = next();
             _registry.getValueWriter(val).writeToBuffer(buffer);
        }

    }


    protected abstract byte getFourOctetEncodingCode();

    protected abstract byte getSingleOctetEncodingCode();

    public Registry getRegistry()
    {
        return _registry;
    }

    protected abstract int getCount();

    protected abstract boolean hasNext();

    protected abstract Object next();

    protected abstract void reset();
}
